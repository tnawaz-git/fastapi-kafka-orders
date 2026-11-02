from uuid import uuid4

import pytest

from app.schemas import OrderCreatedEvent, OrderStatus
from workers.inventory_worker import evaluate


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
