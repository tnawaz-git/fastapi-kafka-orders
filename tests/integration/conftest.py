import asyncio
import contextlib
import os
import uuid

import httpx
import pytest
from aiokafka.admin import AIOKafkaAdminClient
from asgi_lifespan import LifespanManager

from app.config import Settings
from app.main import create_app
from workers import inventory_worker


@pytest.fixture(scope="session")
def kafka_bootstrap():
    """A broker to test against: TEST_KAFKA_BOOTSTRAP if set, otherwise a throwaway Testcontainers Kafka."""
    if url := os.environ.get("TEST_KAFKA_BOOTSTRAP"):
        yield url
        return

    try:
        from testcontainers.kafka import KafkaContainer

        container = KafkaContainer().start()
    except Exception as exc:
        pytest.skip(f"No Kafka available (start Docker or set TEST_KAFKA_BOOTSTRAP): {exc}")

    try:
        yield container.get_bootstrap_server()
    finally:
        container.stop()


@pytest.fixture
async def settings(kafka_bootstrap, tmp_path, request):
    """Per-test settings. Override fields with @pytest.mark.settings(field=value)."""
    # Unique topic and group names keep tests apart from each other and from any running workers.
    run = f"test-{uuid.uuid4().hex[:8]}"
    values = dict(
        kafka_bootstrap_servers=kafka_bootstrap,
        topic_orders_created=f"{run}.orders.created",
        topic_orders_processed=f"{run}.orders.processed",
        topic_orders_failed=f"{run}.orders.failed",
        topic_orders_dlq=f"{run}.orders.dlq",
        status_group_id=f"{run}-status",
        inventory_group_id=f"{run}-inventory",
        notification_group_id=f"{run}-notification",
        database_path=str(tmp_path / "orders.db"),
        max_attempts=2,
        retry_backoff_seconds=0.1,
        simulated_failure_rate=0,
        inventory_processing_delay_seconds=0,
    )
    if marker := request.node.get_closest_marker("settings"):
        values.update(marker.kwargs)
    settings = Settings(**values)
    yield settings

    # Runs after the client and worker fixtures have stopped. Deleting the topics also drops the
    # test groups' offsets, so the (now empty) groups disappear from the broker as well.
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        await admin.delete_topics([t for t in settings.all_topics if t in existing])
    finally:
        await admin.close()


@pytest.fixture
async def inventory(settings):
    task = asyncio.create_task(inventory_worker.run(settings))
    yield task
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


@pytest.fixture
async def client(settings, inventory):
    app = create_app(settings)
    async with LifespanManager(app, startup_timeout=60, shutdown_timeout=30) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
