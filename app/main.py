import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator

from fastapi import FastAPI

from app.config import Settings, configure_logging, get_settings
from app.kafka_client import Message, create_producer, ensure_topics, retry_async, run_consumer
from app.routes import health, orders
from app.schemas import OrderResultEvent
from app.store import OrderStore

logger = logging.getLogger(__name__)


def make_status_handler(store: OrderStore):
    async def handle(msg: Message) -> None:
        result = OrderResultEvent.model_validate_json(msg.value or b"")
        if await store.apply_result(result.order_id, result.status, result.reason):
            logger.info("Order %s is now %s", result.order_id, result.status.value)
        else:
            logger.info("Ignored result for order %s (unknown or already final)", result.order_id)

    return handle


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        store = OrderStore(settings.database_path)
        await store.open()
        await retry_async(lambda: ensure_topics(settings))

        producer = create_producer(settings)
        await retry_async(producer.start)

        status_consumer = asyncio.create_task(
            run_consumer(
                settings,
                name="api-status",
                group_id=settings.status_group_id,
                topics=[settings.topic_orders_processed, settings.topic_orders_failed],
                handler=make_status_handler(store),
                producer=producer,
            ),
            name="status-consumer",
        )

        app.state.settings = settings
        app.state.store = store
        app.state.producer = producer
        app.state.status_consumer = status_consumer
        try:
            yield
        finally:
            status_consumer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await status_consumer
            await producer.stop()
            await store.close()

    configure_logging(settings.log_level)
    app = FastAPI(
        title="Order Service",
        description="Places orders and tracks their status. Processing happens asynchronously through Kafka.",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(orders.router)
    app.include_router(health.router)
    return app


app = create_app()
