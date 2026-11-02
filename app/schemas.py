from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"


# --- API models ---------------------------------------------------------------


class OrderCreate(BaseModel):
    customer_email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", examples=["jane@example.com"])
    sku: str = Field(min_length=1, max_length=64, examples=["SKU-KEYBOARD"])
    quantity: int = Field(gt=0, examples=[2])


class Order(BaseModel):
    id: UUID
    customer_email: str
    sku: str
    quantity: int
    status: OrderStatus
    reason: str | None = None
    created_at: datetime
    updated_at: datetime


# --- Kafka events -------------------------------------------------------------


class OrderCreatedEvent(BaseModel):
    """Published to orders.created when the API accepts an order."""

    event_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    customer_email: str
    sku: str
    quantity: int
    created_at: datetime = Field(default_factory=utcnow)


class OrderResultEvent(BaseModel):
    """Published to orders.processed (CONFIRMED) or orders.failed (REJECTED) by the inventory worker."""

    event_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    customer_email: str
    status: OrderStatus
    reason: str | None = None
    processed_at: datetime = Field(default_factory=utcnow)
