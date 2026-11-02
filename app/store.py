from pathlib import Path
from uuid import UUID

import aiosqlite

from app.schemas import Order, OrderStatus, utcnow

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id             TEXT PRIMARY KEY,
    customer_email TEXT NOT NULL,
    sku            TEXT NOT NULL,
    quantity       INTEGER NOT NULL,
    status         TEXT NOT NULL,
    reason         TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_orders_status ON orders (status);
"""


class OrderStore:
    """Order status store owned by the API. One shared aiosqlite connection per process."""

    def __init__(self, path: str):
        self.path = path
        self._db: aiosqlite.Connection | None = None

    async def open(self) -> None:
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.executescript(SCHEMA)
        await self._db.commit()

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("OrderStore is not open")
        return self._db

    async def ping(self) -> None:
        await self.db.execute("SELECT 1")

    async def create(self, order_id: UUID, customer_email: str, sku: str, quantity: int) -> Order:
        now = utcnow()
        order = Order(
            id=order_id,
            customer_email=customer_email,
            sku=sku,
            quantity=quantity,
            status=OrderStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        await self.db.execute(
            "INSERT INTO orders (id, customer_email, sku, quantity, status, reason, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(order.id), order.customer_email, order.sku, order.quantity,
                order.status.value, None, now.isoformat(), now.isoformat(),
            ),
        )
        await self.db.commit()
        return order

    async def delete(self, order_id: UUID) -> None:
        await self.db.execute("DELETE FROM orders WHERE id = ?", (str(order_id),))
        await self.db.commit()

    async def get(self, order_id: UUID) -> Order | None:
        async with self.db.execute("SELECT * FROM orders WHERE id = ?", (str(order_id),)) as cursor:
            row = await cursor.fetchone()
        return Order(**dict(row)) if row else None

    async def list(self, status: OrderStatus | None = None, limit: int = 50) -> list[Order]:
        query, params = "SELECT * FROM orders", []
        if status is not None:
            query += " WHERE status = ?"
            params.append(status.value)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        async with self.db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        return [Order(**dict(row)) for row in rows]

    async def apply_result(self, order_id: UUID, status: OrderStatus, reason: str | None) -> bool:
        """Move a PENDING order to its final status.

        Returns False when nothing changed, which happens when Kafka redelivers a result
        we already applied. That keeps the update idempotent.
        """
        cursor = await self.db.execute(
            "UPDATE orders SET status = ?, reason = ?, updated_at = ? WHERE id = ? AND status = ?",
            (status.value, reason, utcnow().isoformat(), str(order_id), OrderStatus.PENDING.value),
        )
        await self.db.commit()
        return cursor.rowcount > 0
