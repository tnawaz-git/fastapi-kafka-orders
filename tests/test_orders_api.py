"""POST /orders publishing behavior, with a fake producer instead of Kafka."""

import asyncio

import httpx
import pytest
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI

from app.config import Settings
from app.routes import orders
from app.store import OrderStore

ORDER = {"customer_email": "a@b.co", "sku": "SKU-MOUSE", "quantity": 1}


class FakeProducer:
    """send_and_wait waits for `release`, then succeeds or raises `error`."""

    def __init__(self, error: Exception | None = None, delay: float = 0):
        self.error = error
        self.delay = delay
        self.release = asyncio.Event()
        self.sent = 0
        if delay == 0:
            self.release.set()

    async def send_and_wait(self, topic, value=None, key=None):
        await self.release.wait()
        if self.error:
            raise self.error
        self.sent += 1


@pytest.fixture
async def store(tmp_path):
    store = OrderStore(str(tmp_path / "orders.db"))
    await store.open()
    yield store
    await store.close()


def make_client(store: OrderStore, producer: FakeProducer) -> httpx.AsyncClient:
    app = FastAPI()
    app.include_router(orders.router)
    app.state.store = store
    app.state.producer = producer
    app.state.settings = Settings(publish_timeout_seconds=0.2)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_published_order_is_accepted(store):
    producer = FakeProducer()
    async with make_client(store, producer) as client:
        response = await client.post("/orders", json=ORDER)

    assert response.status_code == 202
    assert producer.sent == 1
    assert len(await store.list()) == 1


async def test_kafka_error_returns_503_and_removes_order(store):
    async with make_client(store, FakeProducer(error=KafkaConnectionError())) as client:
        response = await client.post("/orders", json=ORDER)

    assert response.status_code == 503
    assert await store.list() == []


async def test_slow_kafka_answers_quickly_and_delivers_later(store):
    producer = FakeProducer(delay=1)
    async with make_client(store, producer) as client:
        response = await asyncio.wait_for(client.post("/orders", json=ORDER), timeout=2)

    assert response.status_code == 202
    assert response.json()["status"] == "PENDING"

    producer.release.set()
    await asyncio.gather(*orders._pending_deliveries)
    assert producer.sent == 1
    assert (await store.list())[0].status.value == "PENDING"  # left for the inventory worker


async def test_slow_kafka_that_finally_fails_rejects_the_order(store):
    producer = FakeProducer(error=KafkaConnectionError(), delay=1)
    async with make_client(store, producer) as client:
        response = await client.post("/orders", json=ORDER)

    assert response.status_code == 202
    producer.release.set()
    await asyncio.gather(*orders._pending_deliveries)

    [order] = await store.list()
    assert order.status.value == "REJECTED"
    assert "Could not be queued" in order.reason
