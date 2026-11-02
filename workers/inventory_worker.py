"""Reserves stock for new orders and publishes the outcome.

orders.created -> orders.processed (CONFIRMED) or orders.failed (REJECTED)
"""

import asyncio
import logging
import random

from aiokafka import AIOKafkaProducer

from app.config import Settings, configure_logging, get_settings
from app.kafka_client import GiveUpHandler, Handler, Message, PermanentError, TransientError, to_bytes
from app.schemas import OrderCreatedEvent, OrderResultEvent, OrderStatus
from workers.runtime import main, run_worker

logger = logging.getLogger("inventory-worker")

# A stand-in for a real inventory database.
CATALOG = {
    "SKU-KEYBOARD": "Mechanical keyboard",
    "SKU-MOUSE": "Wireless mouse",
    "SKU-MONITOR": "27-inch monitor",
    "SKU-HEADSET": "USB headset",
}


def evaluate(event: OrderCreatedEvent, max_quantity: int) -> tuple[OrderStatus, str | None]:
    """Decide whether an order can be fulfilled."""
    if event.sku not in CATALOG:
        return OrderStatus.REJECTED, f"Unknown SKU {event.sku}"
    if event.quantity > max_quantity:
        return OrderStatus.REJECTED, f"Quantity {event.quantity} exceeds the limit of {max_quantity} per order"
    return OrderStatus.CONFIRMED, None


def make_handler(settings: Settings, producer: AIOKafkaProducer) -> Handler:
    async def handle(msg: Message) -> None:
        if msg.value is None:
            raise PermanentError("Empty message")
        event = OrderCreatedEvent.model_validate_json(msg.value)

        if random.random() < settings.simulated_failure_rate:
            raise TransientError("Simulated inventory database timeout")
        await asyncio.sleep(settings.inventory_processing_delay_seconds)

        status, reason = evaluate(event, settings.max_quantity_per_order)
        result = OrderResultEvent(
            order_id=event.order_id,
            customer_email=event.customer_email,
            status=status,
            reason=reason,
        )
        topic = settings.topic_orders_processed if status is OrderStatus.CONFIRMED else settings.topic_orders_failed
        await producer.send_and_wait(topic, value=to_bytes(result), key=msg.key)
        logger.info(
            "Order %s %s (partition %d)%s",
            event.order_id, status.value, msg.partition, f": {reason}" if reason else "",
        )

    return handle


def make_on_give_up(settings: Settings, producer: AIOKafkaProducer) -> GiveUpHandler:
    """Once an order event is in the DLQ, publish a FAILED result so the order doesn't sit in PENDING forever."""

    async def on_give_up(msg: Message, error: Exception) -> None:
        try:
            event = OrderCreatedEvent.model_validate_json(msg.value or b"")
        except ValueError:
            return  # unparseable, so there is no order to update
        result = OrderResultEvent(
            order_id=event.order_id,
            customer_email=event.customer_email,
            status=OrderStatus.FAILED,
            reason=f"Could not be processed ({type(error).__name__}: {error})",
        )
        await producer.send_and_wait(settings.topic_orders_failed, value=to_bytes(result), key=msg.key)
        logger.error("Order %s FAILED and was parked in %s", event.order_id, settings.topic_orders_dlq)

    return on_give_up


async def run(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    await run_worker(
        settings,
        name="inventory",
        group_id=settings.inventory_group_id,
        topics=[settings.topic_orders_created],
        make_handler=lambda producer: make_handler(settings, producer),
        make_on_give_up=lambda producer: make_on_give_up(settings, producer),
    )


if __name__ == "__main__":
    configure_logging(get_settings().log_level)
    main(run)
