"""Demo traffic for the order pipeline.

    python -m scripts.demo orders --count 20   # place random orders and wait for their results
    python -m scripts.demo poison              # publish an unparseable message to watch the DLQ

Inside Docker: docker compose exec api python -m scripts.demo orders --count 20
"""

import argparse
import asyncio
import json
import os
import random
import time
import urllib.request

from aiokafka import AIOKafkaProducer

from app.config import get_settings

API_URL = os.environ.get("API_URL", "http://localhost:8000")
SKUS = ["SKU-KEYBOARD", "SKU-MOUSE", "SKU-MONITOR", "SKU-HEADSET", "SKU-TOASTER"]


def request(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{API_URL}{path}", data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def place_orders(count: int, timeout: float) -> None:
    ids = []
    for i in range(count):
        order = request("POST", "/orders", {
            "customer_email": f"customer{i}@example.com",
            "sku": random.choice(SKUS),
            "quantity": random.choice([1, 2, 3, 5, 12]),
        })
        ids.append(order["id"])
        print(f"placed  {order['id']}  {order['sku']:<13} x{order['quantity']:<3} {order['status']}")

    print(f"\nWaiting up to {timeout:.0f}s for results...\n")
    pending, deadline = set(ids), time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for order_id in list(pending):
            order = request("GET", f"/orders/{order_id}")
            if order["status"] != "PENDING":
                pending.discard(order_id)
                print(f"{order['status']:<9} {order_id}  {order.get('reason') or ''}")
        time.sleep(0.5)
    if pending:
        print(f"\n{len(pending)} order(s) still PENDING (is the inventory worker running?)")


async def send_poison() -> None:
    settings = get_settings()
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        await producer.send_and_wait(settings.topic_orders_created, value=b"{definitely not json", key=b"poison")
    finally:
        await producer.stop()
    print(f"Sent a bad message to {settings.topic_orders_created}.")
    print(f"Each consumer group parks it in {settings.topic_orders_dlq}: see Kafka UI at http://localhost:8080")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    orders = sub.add_parser("orders", help="place random orders")
    orders.add_argument("--count", type=int, default=10)
    orders.add_argument("--timeout", type=float, default=60)
    sub.add_parser("poison", help="send an unparseable message")
    args = parser.parse_args()

    if args.command == "orders":
        place_orders(args.count, args.timeout)
    else:
        asyncio.run(send_poison())


if __name__ == "__main__":
    main()
