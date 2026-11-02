import asyncio
import contextlib
import logging
import signal
from collections.abc import Callable

from aiokafka import AIOKafkaProducer

from app.config import Settings
from app.kafka_client import Handler, create_producer, ensure_topics, retry_async, run_consumer

logger = logging.getLogger(__name__)


async def run_worker(
    settings: Settings,
    *,
    name: str,
    group_id: str,
    topics: list[str],
    make_handler: Callable[[AIOKafkaProducer], Handler],
) -> None:
    """Start a producer, then consume until cancelled. make_handler gets the producer for output events."""
    await retry_async(lambda: ensure_topics(settings))
    producer = create_producer(settings)
    try:
        await retry_async(producer.start)
        await run_consumer(
            settings,
            name=name,
            group_id=group_id,
            topics=topics,
            handler=make_handler(producer),
            producer=producer,
        )
    finally:
        await producer.stop()


def main(worker: Callable[[], asyncio.Future]) -> None:
    """Run a worker coroutine and stop it cleanly on SIGTERM (docker stop) or Ctrl+C."""

    async def runner() -> None:
        task = asyncio.ensure_future(worker())
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            # Not supported on Windows; Ctrl+C still raises KeyboardInterrupt there.
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, task.cancel)
        with contextlib.suppress(asyncio.CancelledError):
            await task
        logger.info("Worker stopped")

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(runner())
