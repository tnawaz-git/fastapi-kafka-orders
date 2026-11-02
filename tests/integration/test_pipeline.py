import asyncio

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.kafka_client import ensure_topics

pytestmark = pytest.mark.integration


async def wait_for_final_status(client, order_id: str, timeout: float = 30) -> dict:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        order = (await client.get(f"/orders/{order_id}")).json()
        if order["status"] != "PENDING":
            return order
        await asyncio.sleep(0.2)
    raise AssertionError(f"Order {order_id} still PENDING after {timeout}s")


@pytest.mark.parametrize(
    ("sku", "quantity", "expected_status", "reason_part"),
    [
        ("SKU-KEYBOARD", 2, "CONFIRMED", None),
        ("SKU-KEYBOARD", 50, "REJECTED", "exceeds the limit"),
        ("SKU-TOASTER", 1, "REJECTED", "Unknown SKU"),
    ],
)
async def test_order_flows_through_kafka(client, sku, quantity, expected_status, reason_part):
    response = await client.post("/orders", json={"customer_email": "jane@example.com", "sku": sku, "quantity": quantity})

    assert response.status_code == 202
    placed = response.json()
    assert placed["status"] == "PENDING"
    assert response.headers["location"] == f"/orders/{placed['id']}"

    order = await wait_for_final_status(client, placed["id"])
    assert order["status"] == expected_status
    if reason_part:
        assert reason_part in order["reason"]


@pytest.mark.settings(simulated_failure_rate=1.0)
async def test_order_that_keeps_failing_ends_up_failed(client):
    response = await client.post("/orders", json={"customer_email": "a@b.co", "sku": "SKU-MOUSE", "quantity": 1})

    order = await wait_for_final_status(client, response.json()["id"])
    assert order["status"] == "FAILED"
    assert "Simulated inventory database timeout" in order["reason"]


async def test_invalid_request_is_rejected_by_api(client):
    response = await client.post("/orders", json={"customer_email": "not-an-email", "sku": "SKU-MOUSE", "quantity": 0})

    assert response.status_code == 422


async def test_list_and_404(client):
    await client.post("/orders", json={"customer_email": "a@b.co", "sku": "SKU-MOUSE", "quantity": 1})

    assert len((await client.get("/orders")).json()) == 1
    assert (await client.get("/orders/00000000-0000-0000-0000-000000000000")).status_code == 404


async def test_health(client):
    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json()["checks"] == {"kafka": "ok", "database": "ok", "status_consumer": "ok"}


async def test_poison_message_lands_in_dlq(settings, inventory):
    await ensure_topics(settings)

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        await producer.send_and_wait(settings.topic_orders_created, value=b"this is not json", key=b"poison")
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(
        settings.topic_orders_dlq,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"{settings.inventory_group_id}-dlq-reader",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        msg = await asyncio.wait_for(consumer.getone(), timeout=30)
    finally:
        await consumer.stop()

    headers = dict(msg.headers)
    assert msg.value == b"this is not json"
    assert msg.key == b"poison"
    assert headers["dlq.source.topic"] == settings.topic_orders_created.encode()
    assert headers["dlq.attempts"] == b"1"  # invalid JSON is permanent: no retries
    assert headers["dlq.consumer.group"] == settings.inventory_group_id.encode()
