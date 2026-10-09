"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

PUSH_INTERVAL = 0.5  # seconds between cache checks
HEARTBEAT_INTERVAL = 15.0  # comment line when nothing changed, keeps proxies from closing


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Build a router exposing GET /api/stream/prices bound to `price_cache`."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


def format_snapshot(price_cache: PriceCache) -> str:
    """One SSE event holding every cached price, keyed by ticker."""
    data = {ticker: update.to_dict() for ticker, update in price_cache.get_all().items()}
    return f"data: {json.dumps(data)}\n\n"


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = PUSH_INTERVAL,
    heartbeat: float = HEARTBEAT_INTERVAL,
) -> AsyncGenerator[str, None]:
    """Yield a full snapshot whenever the cache version changes.

    Every event is the complete set of tracked tickers, so a ticker missing
    from an event has been removed. An empty cache sends `data: {}`.
    """
    yield "retry: 1000\n\n"  # EventSource reconnects 1s after a drop
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)
    last_version = -1
    last_sent = time.monotonic()
    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                last_sent = time.monotonic()
                yield format_snapshot(price_cache)
            elif time.monotonic() - last_sent >= heartbeat:
                last_sent = time.monotonic()
                yield ": ping\n\n"
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    logger.info("SSE client disconnected: %s", client)
