import logging
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All settings can be overridden with environment variables (e.g. KAFKA_BOOTSTRAP_SERVERS)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"

    topic_orders_created: str = "orders.created"
    topic_orders_processed: str = "orders.processed"
    topic_orders_failed: str = "orders.failed"
    topic_orders_dlq: str = "orders.dlq"
    topic_partitions: int = 3
    topic_replication_factor: int = 1

    status_group_id: str = "order-api-status"
    inventory_group_id: str = "inventory-service"
    notification_group_id: str = "notification-service"

    database_path: str = "data/orders.db"

    # How long POST /orders waits for Kafka to acknowledge a new order before answering anyway.
    publish_timeout_seconds: float = 5.0

    # Consumer retry policy: a message gets max_attempts tries, then goes to the DLQ.
    max_attempts: int = 3
    retry_backoff_seconds: float = 0.5

    # Inventory simulation.
    max_quantity_per_order: int = 10
    simulated_failure_rate: float = 0.0
    inventory_processing_delay_seconds: float = 0.5

    log_level: str = "INFO"

    @property
    def all_topics(self) -> list[str]:
        return [
            self.topic_orders_created,
            self.topic_orders_processed,
            self.topic_orders_failed,
            self.topic_orders_dlq,
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
