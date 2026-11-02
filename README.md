# FastAPI + Kafka order pipeline

A small event-driven system: a FastAPI service accepts orders and publishes them to Kafka.
Independent workers process them, and the API tracks each order's status in SQLite.

```
Client ──POST /orders──▶ API ──▶ orders.created ──┬──▶ inventory-worker ──▶ orders.processed / orders.failed
                          │                        │                                │
                          │                        └──▶ notification-worker ◀───────┤ (emails for every stage)
                          │                                                         │
                          └── status consumer (inside the API) ◀────────────────────┘
                                  updates SQLite: PENDING → CONFIRMED / REJECTED

Anything that can't be processed ──▶ orders.dlq
```

## Run it

Needs Docker Desktop.

```bash
docker compose up -d --build
```

| What | Where |
|---|---|
| API docs (Swagger) | http://localhost:8000/docs |
| Health check | http://localhost:8000/health |
| Kafka UI (topics, messages, consumer groups) | http://localhost:8080 |
| Kafka from your machine | `localhost:9092` |

Place an order and check on it:

```bash
curl -X POST localhost:8000/orders -H "Content-Type: application/json" -d '{"customer_email":"jane@example.com","sku":"SKU-KEYBOARD","quantity":2}'
curl localhost:8000/orders/<id from the response>
```

Known SKUs are `SKU-KEYBOARD`, `SKU-MOUSE`, `SKU-MONITOR` and `SKU-HEADSET`. Unknown SKUs and quantities over 10 are rejected.

Stop it with `docker compose down`. Add `-v` to also delete the SQLite database. Kafka's data isn't kept on a volume, so it starts empty every time.

## Things to try

**Generate traffic** (random orders, some of them rejected):

```bash
docker compose exec api python -m scripts.demo orders --count 20
```

**Watch retries.** The inventory worker fails 20% of attempts on purpose (`SIMULATED_FAILURE_RATE`):

```bash
docker compose logs -f inventory-worker
```

**Dead-letter queue.** Send a message that can't be parsed:

```bash
docker compose exec api python -m scripts.demo poison
```

Every consumer group that reads `orders.created` (inventory and notification) parks it in `orders.dlq`.
Open the message in Kafka UI: its headers say which topic, partition, offset and group it came from, and the error.

**Scale and watch partitions rebalance.** `orders.created` has 3 partitions, so up to 3 inventory workers share the work:

```bash
docker compose up -d --scale inventory-worker=3 inventory-worker
docker compose logs inventory-worker | grep "partitions"
```

A 4th worker would get no partition and sit idle until another one stops.

## How it works

- **Ordering per order.** Messages are keyed by `order_id`, so all events for one order go to the same partition and stay in order.
- **At-least-once delivery.** Consumers turn auto-commit off and commit an offset only after the message is handled or sent to the DLQ ([app/kafka_client.py](app/kafka_client.py)). A crash means the message is redelivered, never lost. Handlers are idempotent: the API only moves an order out of `PENDING` once ([app/store.py](app/store.py)).
- **Retries.** Errors are retried with exponential backoff (`MAX_ATTEMPTS`, `RETRY_BACKOFF_SECONDS`). Payloads that can't be parsed skip retries and go straight to the DLQ.
- **Safe publishing.** The API saves the order *before* publishing, so a result can't arrive for an unknown order. If publishing fails, it removes the row and returns `503`. The producer uses `acks=all` with idempotence on.
- **Topics** are created on startup by whichever service starts first ([`ensure_topics`](app/kafka_client.py)). Broker auto-creation is off.

## Project layout

```
app/
  main.py            FastAPI app; starts the producer and the status consumer on startup
  config.py          Settings, all overridable through environment variables
  schemas.py         API models and Kafka event models
  kafka_client.py    Producer/consumer setup, retries, DLQ, manual commits, rebalance logging
  store.py           SQLite order store
  routes/            /orders and /health
workers/
  inventory_worker.py     orders.created → orders.processed / orders.failed
  notification_worker.py  logs a customer email for each event
  runtime.py              shared worker startup and shutdown
scripts/demo.py      traffic and poison-message generator
tests/               unit tests and Kafka integration tests
```

## Tests

Against the Kafka in docker compose (no local Python needed):

```bash
docker compose --profile test run --rm --build tests
```

With local Python 3.12, integration tests start their own throwaway Kafka through Testcontainers:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements-dev.txt
pytest                      # everything
pytest -m "not integration" # unit tests only, no Docker needed
```

Integration tests use unique topic and group names for each test, so they never interfere with the running services.

## Running outside Docker

With Kafka from compose running (`docker compose up -d kafka`) and the venv above:

```bash
uvicorn app.main:app --reload
python -m workers.inventory_worker
python -m workers.notification_worker
```

They connect to `localhost:9092` by default. Settings can also be set in a `.env` file.
