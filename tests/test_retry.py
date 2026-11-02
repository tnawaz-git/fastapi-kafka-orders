from dataclasses import dataclass, field

from app.config import Settings
from app.kafka_client import PermanentError, handle_with_retry
from app.schemas import OrderCreatedEvent

SETTINGS = Settings(max_attempts=3, retry_backoff_seconds=0)


@dataclass
class FakeMessage:
    topic: str = "orders.created"
    partition: int = 2
    offset: int = 41
    key: bytes | None = b"order-1"
    value: bytes | None = b"{}"
    headers: tuple = ()


@dataclass
class FakeProducer:
    sent: list[dict] = field(default_factory=list)

    async def send_and_wait(self, topic, value=None, key=None, headers=None):
        self.sent.append({"topic": topic, "value": value, "key": key, "headers": dict(headers or [])})


def failing_handler(failures: int, error: Exception):
    calls = {"count": 0}

    async def handler(msg):
        calls["count"] += 1
        if calls["count"] <= failures:
            raise error

    return handler, calls


async def run(handler, producer, on_give_up=None):
    await handle_with_retry(
        FakeMessage(), handler, producer=producer, settings=SETTINGS, group_id="test-group", on_give_up=on_give_up
    )


def recorder():
    calls = []

    async def on_give_up(msg, error):
        calls.append(error)

    return on_give_up, calls


async def test_on_give_up_runs_after_dlq():
    producer = FakeProducer()
    handler, _ = failing_handler(failures=99, error=RuntimeError("db down"))
    on_give_up, calls = recorder()

    await run(handler, producer, on_give_up)

    assert len(producer.sent) == 1
    assert [str(e) for e in calls] == ["db down"]


async def test_on_give_up_not_called_on_success():
    handler, _ = failing_handler(failures=1, error=RuntimeError("blip"))
    on_give_up, calls = recorder()

    await run(handler, FakeProducer(), on_give_up)

    assert calls == []


async def test_failing_on_give_up_does_not_break_the_consumer():
    producer = FakeProducer()
    handler, _ = failing_handler(failures=99, error=PermanentError("bad"))

    async def broken(msg, error):
        raise RuntimeError("boom")

    await run(handler, producer, broken)  # must not raise

    assert len(producer.sent) == 1


async def test_transient_failure_is_retried_until_success():
    producer = FakeProducer()
    handler, calls = failing_handler(failures=2, error=RuntimeError("db timeout"))

    await run(handler, producer)

    assert calls["count"] == 3
    assert producer.sent == []


async def test_exhausted_retries_go_to_dlq_with_metadata():
    producer = FakeProducer()
    handler, calls = failing_handler(failures=99, error=RuntimeError("db down"))

    await run(handler, producer)

    assert calls["count"] == SETTINGS.max_attempts
    [dlq] = producer.sent
    assert dlq["topic"] == SETTINGS.topic_orders_dlq
    assert dlq["value"] == b"{}"
    assert dlq["key"] == b"order-1"
    assert dlq["headers"]["dlq.attempts"] == b"3"
    assert dlq["headers"]["dlq.source.topic"] == b"orders.created"
    assert dlq["headers"]["dlq.source.partition"] == b"2"
    assert dlq["headers"]["dlq.source.offset"] == b"41"
    assert dlq["headers"]["dlq.consumer.group"] == b"test-group"
    assert b"db down" in dlq["headers"]["dlq.error"]


async def test_permanent_error_skips_retries():
    producer = FakeProducer()
    handler, calls = failing_handler(failures=99, error=PermanentError("bad order"))

    await run(handler, producer)

    assert calls["count"] == 1
    assert producer.sent[0]["headers"]["dlq.attempts"] == b"1"


async def test_invalid_payload_is_permanent():
    producer = FakeProducer()
    calls = {"count": 0}

    async def handler(msg):
        calls["count"] += 1
        OrderCreatedEvent.model_validate_json(b"not json")

    await run(handler, producer)

    assert calls["count"] == 1
    assert b"ValidationError" in producer.sent[0]["headers"]["dlq.error"]
