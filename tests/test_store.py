from uuid import uuid4

import pytest

from app.schemas import OrderStatus
from app.store import OrderStore


@pytest.fixture
async def store(tmp_path):
    store = OrderStore(str(tmp_path / "orders.db"))
    await store.open()
    yield store
    await store.close()


async def test_new_order_is_pending(store):
    order_id = uuid4()
    await store.create(order_id, "a@b.co", "SKU-MOUSE", 2)

    order = await store.get(order_id)

    assert order.status is OrderStatus.PENDING
    assert order.quantity == 2


async def test_apply_result_is_idempotent(store):
    order_id = uuid4()
    await store.create(order_id, "a@b.co", "SKU-MOUSE", 2)

    assert await store.apply_result(order_id, OrderStatus.CONFIRMED, None) is True
    # A redelivered (or conflicting) result must not change a final status.
    assert await store.apply_result(order_id, OrderStatus.REJECTED, "late") is False

    order = await store.get(order_id)
    assert order.status is OrderStatus.CONFIRMED
    assert order.reason is None


async def test_apply_result_for_unknown_order(store):
    assert await store.apply_result(uuid4(), OrderStatus.CONFIRMED, None) is False


async def test_list_filters_by_status(store):
    confirmed, pending = uuid4(), uuid4()
    await store.create(confirmed, "a@b.co", "SKU-MOUSE", 1)
    await store.create(pending, "a@b.co", "SKU-MOUSE", 1)
    await store.apply_result(confirmed, OrderStatus.CONFIRMED, None)

    assert [o.id for o in await store.list(OrderStatus.PENDING)] == [pending]
    assert len(await store.list()) == 2


async def test_get_missing_order(store):
    assert await store.get(uuid4()) is None
