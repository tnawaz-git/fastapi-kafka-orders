import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    """Report whether the broker, the database, and the status consumer are all working."""
    state = request.app.state
    checks: dict[str, str] = {}

    try:
        # Asks the broker for fresh cluster metadata, so a dead broker fails here.
        await asyncio.wait_for(state.producer.client.fetch_all_metadata(), timeout=5)
        checks["kafka"] = "ok"
    except Exception as exc:
        checks["kafka"] = f"error: {type(exc).__name__}: {exc}"

    try:
        await state.store.ping()
        checks["database"] = "ok"
    except Exception as exc:
        checks["database"] = f"error: {type(exc).__name__}: {exc}"

    task: asyncio.Task = state.status_consumer
    checks["status_consumer"] = "ok" if not task.done() else "error: stopped"

    healthy = all(value == "ok" for value in checks.values())
    return JSONResponse(
        {"status": "ok" if healthy else "degraded", "checks": checks},
        status_code=200 if healthy else 503,
    )
