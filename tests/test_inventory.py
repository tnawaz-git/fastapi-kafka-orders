from uuid import uuid4

import pytest

from app.config import Settings
from app.kafka_client import TransientError
from app.schemas import OrderCreatedEvent, OrderResultEvent, OrderStatus
from tests.test_retry import FakeMessage, FakeProducer
from workers.inventory_worker import evaluate, make_on_give_up


def event(sku: str, quantity: int) -> OrderCreatedEvent:
    return OrderCreatedEvent(order_id=uuid4(), customer_email="a@b.co", sku=sku, quantity=quantity)


@pytest.mark.parametrize(
    ("sku", "quantity", "expected", "reason_part"),
    [
        ("SKU-KEYBOARD", 1, OrderStatus.CONFIRMED, None),
        ("SKU-MOUSE", 10, OrderStatus.CONFIRMED, None),
        ("SKU-MOUSE", 11, OrderStatus.REJECTED, "exceeds the limit"),
        ("SKU-UNKNOWN", 1, OrderStatus.REJECTED, "Unknown SKU"),
    ],
)
def test_evaluate(sku, quantity, expected, reason_part):
    status, reason = evaluate(event(sku, quantity), max_quantity=10)

    assert status is expected
    if reason_part is None:
        assert reason is None
    else:
        assert reason_part in reason


async def test_give_up_publishes_failed_result():
    settings = Settings()
    producer = FakeProducer()
    created = event("SKU-MOUSE", 1)
    msg = FakeMessage(value=created.model_dump_json().encode(), key=b"order-key")

    await make_on_give_up(settings, producer)(msg, TransientError("db timeout"))

    [sent] = producer.sent
    assert sent["topic"] == settings.topic_orders_failed
    assert sent["key"] == b"order-key"
    result = OrderResultEvent.model_validate_json(sent["value"])
    assert result.order_id == created.order_id
    assert result.status is OrderStatus.FAILED
    assert "db timeout" in result.reason


async def test_give_up_ignores_unparseable_message():
    producer = FakeProducer()

    await make_on_give_up(Settings(), producer)(FakeMessage(value=b"not json"), ValueError("bad"))

    assert producer.sent == []
