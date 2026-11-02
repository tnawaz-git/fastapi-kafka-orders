"""Kafka helpers shared by the API and the workers.

Delivery semantics: consumers run with auto-commit off and commit an offset only after the
message was handled (or parked in the DLQ). That gives at-least-once processing, so every
handler must be idempotent.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError, TopicAlreadyExistsError
from pydantic import BaseModel

from app.config import Settings

logger = logging.getLogger(__name__)


class PermanentError(Exception):
    """The message can never succeed (bad payload, broken invariant): skip retries, go to the DLQ."""


class TransientError(Exception):
    """A temporary failure (e.g. a downstream service timed out): worth retrying."""


# Pydantic's ValidationError and json's JSONDecodeError are both ValueErrors, so a payload
# that cannot be parsed is treated as permanent.
PERMANENT_ERRORS = (PermanentError, ValueError)


class Message(Protocol):
    """The parts of aiokafka's ConsumerRecord the helpers use (lets tests pass simple fakes)."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: Any


Handler = Callable[[Message], Awaitable[None]]
# Called after a message was sent to the DLQ, e.g. to tell others the work won't happen.
GiveUpHandler = Callable[[Message, Exception], Awaitable[None]]


def to_bytes(event: BaseModel) -> bytes:
    return event.model_dump_json().encode()


def create_producer(settings: Settings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        enable_idempotence=True,
    )


def create_consumer(settings: Settings, group_id: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


async def retry_async(fn: Callable[[], Awaitable[Any]], *, attempts: int = 30, delay: float = 2.0) -> Any:
    """Retry fn on connection errors; used at startup while the broker may still be booting."""
    for attempt in range(1, attempts + 1):
        try:
            return await fn()
        except (KafkaConnectionError, OSError) as exc:
            if attempt == attempts:
                raise
            logger.warning("Kafka not reachable (%s), retrying in %.0fs (%d/%d)", exc, delay, attempt, attempts)
            await asyncio.sleep(delay)


async def ensure_topics(settings: Settings) -> None:
    """Create any missing topics. Safe to call from several services at once."""
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        missing = [
            NewTopic(
                name=name,
                num_partitions=settings.topic_partitions,
                replication_factor=settings.topic_replication_factor,
            )
            for name in settings.all_topics
            if name not in existing
        ]
        if missing:
            try:
                await admin.create_topics(missing)
                logger.info("Created topics: %s", ", ".join(t.name for t in missing))
            except TopicAlreadyExistsError:
                pass  # another service won the race
    finally:
        await admin.close()


class LoggingRebalanceListener(ConsumerRebalanceListener):
    """Logs partition moves, so scaling a worker shows the group rebalancing."""

    def __init__(self, name: str):
        self.name = name

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        if revoked:
            logger.info("[%s] partitions revoked: %s", self.name, _fmt(revoked))

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        logger.info("[%s] partitions assigned: %s", self.name, _fmt(assigned) or "none")


def _fmt(partitions: set[TopicPartition]) -> str:
    return ", ".join(f"{tp.topic}[{tp.partition}]" for tp in sorted(partitions))


async def send_to_dlq(
    producer: AIOKafkaProducer,
    dlq_topic: str,
    msg: Message,
    error: Exception,
    *,
    attempts: int,
    group_id: str,
) -> None:
    """Park the original message, untouched, with headers describing why it failed."""
    headers = list(msg.headers or []) + [
        ("dlq.error", f"{type(error).__name__}: {error}"[:1000].encode()),
        ("dlq.source.topic", msg.topic.encode()),
        ("dlq.source.partition", str(msg.partition).encode()),
        ("dlq.source.offset", str(msg.offset).encode()),
        ("dlq.attempts", str(attempts).encode()),
        ("dlq.consumer.group", group_id.encode()),
    ]
    await producer.send_and_wait(dlq_topic, value=msg.value, key=msg.key, headers=headers)
    logger.error(
        "Sent %s[%d]@%d to %s after %d attempt(s): %s",
        msg.topic, msg.partition, msg.offset, dlq_topic, attempts, error,
    )


async def handle_with_retry(
    msg: Message,
    handler: Handler,
    *,
    producer: AIOKafkaProducer,
    settings: Settings,
    group_id: str,
    on_give_up: GiveUpHandler | None = None,
) -> None:
    """Run handler with exponential backoff. Permanent errors and exhausted retries go to the DLQ."""

    async def give_up(exc: Exception, attempts: int) -> None:
        await send_to_dlq(producer, settings.topic_orders_dlq, msg, exc, attempts=attempts, group_id=group_id)
        if on_give_up is not None:
            try:
                await on_give_up(msg, exc)
            except Exception:
                # The message is already safe in the DLQ; don't let this stop the consumer.
                logger.exception("on_give_up failed for %s[%d]@%d", msg.topic, msg.partition, msg.offset)

    for attempt in range(1, settings.max_attempts + 1):
        try:
            await handler(msg)
            return
        except PERMANENT_ERRORS as exc:
            await give_up(exc, attempt)
            return
        except Exception as exc:
            if attempt == settings.max_attempts:
                await give_up(exc, attempt)
                return
            delay = settings.retry_backoff_seconds * 2 ** (attempt - 1)
            logger.warning(
                "Attempt %d/%d failed for %s[%d]@%d (%s), retrying in %.1fs",
                attempt, settings.max_attempts, msg.topic, msg.partition, msg.offset, exc, delay,
            )
            await asyncio.sleep(delay)


async def run_consumer(
    settings: Settings,
    *,
    name: str,
    group_id: str,
    topics: list[str],
    handler: Handler,
    producer: AIOKafkaProducer,
    on_give_up: GiveUpHandler | None = None,
) -> None:
    """Consume topics until cancelled, committing each offset only after its message is done."""
    consumer = create_consumer(settings, group_id)
    consumer.subscribe(topics, listener=LoggingRebalanceListener(name))
    # start() is inside the try so a cancellation during startup still closes the consumer.
    try:
        await retry_async(consumer.start)
        logger.info("[%s] consuming %s as group '%s'", name, ", ".join(topics), group_id)
        async for msg in consumer:
            await handle_with_retry(
                msg, handler, producer=producer, settings=settings, group_id=group_id, on_give_up=on_give_up
            )
            await consumer.commit({TopicPartition(msg.topic, msg.partition): msg.offset + 1})
    finally:
        await consumer.stop()
