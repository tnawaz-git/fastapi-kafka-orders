import asyncio
import logging
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from app.config import Settings
from app.kafka_client import to_bytes
from app.schemas import Order, OrderCreate, OrderCreatedEvent, OrderStatus
from app.store import OrderStore

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/orders", tags=["orders"])


def get_store(request: Request) -> OrderStore:
    return request.app.state.store


def get_producer(request: Request) -> AIOKafkaProducer:
    return request.app.state.producer


def get_app_settings(request: Request) -> Settings:
    return request.app.state.settings


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=Order)
async def place_order(
    body: OrderCreate,
    response: Response,
    store: OrderStore = Depends(get_store),
    producer: AIOKafkaProducer = Depends(get_producer),
    settings: Settings = Depends(get_app_settings),
) -> Order:
    """Accept an order and hand it to Kafka. Processing happens asynchronously; poll GET /orders/{id}."""
    order_id = uuid4()
    # Save first, so a fast worker's result can never arrive for an order we don't know yet.
    order = await store.create(order_id, body.customer_email, body.sku, body.quantity)

    event = OrderCreatedEvent(order_id=order_id, **body.model_dump())
    # Keying by order_id sends every event for one order to the same partition, in order.
    delivery = asyncio.ensure_future(
        producer.send_and_wait(settings.topic_orders_created, value=to_bytes(event), key=str(order_id).encode())
    )
    try:
        await asyncio.wait_for(asyncio.shield(delivery), timeout=settings.publish_timeout_seconds)
    except KafkaError as exc:
        logger.exception("Could not publish order %s", order_id)
        await store.delete(order_id)
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Order queue unavailable, try again") from exc
    except TimeoutError:
        # The producer keeps the message buffered and delivers it once the broker is reachable,
        # so don't make the client wait (and retry into a duplicate order): accept it now.
        logger.warning("Kafka is slow to acknowledge order %s; finishing delivery in the background", order_id)
        task = asyncio.create_task(_finish_delivery(delivery, order_id, store))
        _pending_deliveries.add(task)
        task.add_done_callback(_pending_deliveries.discard)

    response.headers["Location"] = f"/orders/{order_id}"
    return order


# Deliveries still in flight after their request returned; held here so they aren't garbage-collected.
_pending_deliveries: set[asyncio.Task] = set()


async def _finish_delivery(delivery: asyncio.Future, order_id: UUID, store: OrderStore) -> None:
    try:
        await delivery
        logger.info("Order %s reached Kafka after a delay", order_id)
    except Exception:
        logger.exception("Order %s never reached Kafka", order_id)
        await store.apply_result(order_id, OrderStatus.REJECTED, "Could not be queued for processing, please try again")


@router.get("/{order_id}", response_model=Order)
async def get_order(order_id: UUID, store: OrderStore = Depends(get_store)) -> Order:
    order = await store.get(order_id)
    if order is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Order not found")
    return order


@router.get("", response_model=list[Order])
async def list_orders(
    status_filter: OrderStatus | None = Query(None, alias="status"),
    limit: int = Query(50, ge=1, le=500),
    store: OrderStore = Depends(get_store),
) -> list[Order]:
    return await store.list(status_filter, limit)
