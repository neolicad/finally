"""SSE stream tests."""

import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.market.cache import PriceCache
from app.market.stream import _generate_events, create_stream_router, format_snapshot


class FakeRequest:
    """Stands in for starlette's Request: disconnects after `polls` checks."""

    def __init__(self, polls: int) -> None:
        self.client = None
        self._polls = polls

    async def is_disconnected(self) -> bool:
        self._polls -= 1
        return self._polls < 0


def _events(chunks: list[str]) -> list[dict]:
    return [json.loads(c.removeprefix("data: ")) for c in chunks if c.startswith("data: ")]


class TestStream:
    async def test_retry_then_snapshot(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        chunks = [c async for c in _generate_events(cache, FakeRequest(1), interval=0)]
        assert chunks[0] == "retry: 1000\n\n"
        assert _events(chunks)[0]["AAPL"]["price"] == 190.0

    async def test_unchanged_cache_sends_heartbeat_not_data(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        chunks = [
            c async for c in _generate_events(cache, FakeRequest(3), interval=0, heartbeat=0)
        ]
        assert len(_events(chunks)) == 1
        assert ": ping\n\n" in chunks

    async def test_new_price_sends_new_event(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        gen = _generate_events(cache, FakeRequest(3), interval=0)
        chunks = [await anext(gen), await anext(gen)]
        cache.update("AAPL", 191.0)
        chunks += [c async for c in gen]
        prices = [e["AAPL"]["price"] for e in _events(chunks)]
        assert prices == [190.0, 191.0]

    async def test_removal_is_streamed(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        gen = _generate_events(cache, FakeRequest(2), interval=0)
        chunks = [await anext(gen), await anext(gen)]
        cache.remove("AAPL")
        chunks += [c async for c in gen]
        assert _events(chunks)[-1] == {}

    async def test_empty_cache_sends_empty_object(self):
        chunks = [c async for c in _generate_events(PriceCache(), FakeRequest(1), interval=0)]
        assert _events(chunks) == [{}]


def test_format_snapshot_contains_full_payload():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    line = format_snapshot(cache)
    assert line.startswith("data: ") and line.endswith("\n\n")
    payload = json.loads(line.removeprefix("data: "))
    assert payload["AAPL"]["direction"] == "flat"
    assert set(payload["AAPL"]) >= {"price", "previous_price", "day_change_percent"}


def test_router_factory_is_reusable():
    a = create_stream_router(PriceCache())
    b = create_stream_router(PriceCache())
    assert len(a.routes) == len(b.routes) == 1


def test_endpoint_headers_and_route():
    app = FastAPI()
    router = create_stream_router(PriceCache())
    app.include_router(router)
    assert [r.path for r in router.routes] == ["/api/stream/prices"]
