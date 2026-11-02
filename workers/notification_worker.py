"""Sends (logs) customer emails for every stage of an order.

It reads orders.created in its own consumer group, so it gets every message the inventory
worker gets: that's Kafka fan-out.
"""

import logging

from aiokafka import AIOKafkaProducer

from app.config import Settings, configure_logging, get_settings
from app.kafka_client import Handler, Message, PermanentError
from app.schemas import OrderCreatedEvent, OrderResultEvent, OrderStatus
from workers.runtime import main, run_worker

logger = logging.getLogger("notification-worker")


def make_handler(settings: Settings, producer: AIOKafkaProducer) -> Handler:
    async def handle(msg: Message) -> None:
        if msg.value is None:
            raise PermanentError("Empty message")

        if msg.topic == settings.topic_orders_created:
            created = OrderCreatedEvent.model_validate_json(msg.value)
            send_email(created.customer_email, f"We received your order {created.order_id}")
            return

        result = OrderResultEvent.model_validate_json(msg.value)
        if result.status is OrderStatus.CONFIRMED:
            send_email(result.customer_email, f"Your order {result.order_id} is confirmed")
        else:
            send_email(result.customer_email, f"Sorry, order {result.order_id} was rejected: {result.reason}")

    return handle


def send_email(to: str, subject: str) -> None:
    # A real service would call an email provider here.
    logger.info("EMAIL to=%s subject=%r", to, subject)


async def run(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    await run_worker(
        settings,
        name="notification",
        group_id=settings.notification_group_id,
        topics=[settings.topic_orders_created, settings.topic_orders_processed, settings.topic_orders_failed],
        make_handler=lambda producer: make_handler(settings, producer),
    )


if __name__ == "__main__":
    configure_logging(get_settings().log_level)
    main(run)
