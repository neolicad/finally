"""Behaviour every MarketDataSource must share, run against both implementations."""

import asyncio
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


def _fake_massive_client() -> MagicMock:
    """Answers any snapshot request with a price for each requested ticker."""
    client = MagicMock()
    client.get_snapshot_all.side_effect = lambda market_type, tickers: [
        TickerSnapshot.from_dict({"ticker": t, "lastTrade": {"p": 100.0, "t": 1}}) for t in tickers
    ]
    return client


@pytest_asyncio.fixture(params=["simulator", "massive"])
async def source_and_cache(request):
    cache = PriceCache()
    if request.param == "simulator":
        source = SimulatorDataSource(cache, update_interval=0.01)
    else:
        source = MassiveDataSource("key", cache, poll_interval=0.01, client=_fake_massive_client())
    yield source, cache
    await source.stop()


class TestContract:
    async def test_start_fills_cache_before_returning(self, source_and_cache):
        source, cache = source_and_cache
        await source.start(["AAPL", "googl"])
        assert sorted(source.get_tickers()) == ["AAPL", "GOOGL"]
        assert cache.get_price("AAPL") is not None
        assert cache.get_price("GOOGL") is not None

    async def test_add_and_remove(self, source_and_cache):
        source, cache = source_and_cache
        await source.start(["AAPL"])
        await source.add_ticker("pypl")
        await source.add_ticker("PYPL")  # idempotent
        assert source.get_tickers().count("PYPL") == 1
        assert cache.get_price("PYPL") is not None  # priced immediately
        await source.remove_ticker("PYPL")
        await source.remove_ticker("PYPL")  # idempotent
        assert "PYPL" not in source.get_tickers()
        assert "PYPL" not in cache

    async def test_invalid_ticker_rejected(self, source_and_cache):
        source, _ = source_and_cache
        await source.start([])
        with pytest.raises(ValueError):
            await source.add_ticker("not a ticker")

    async def test_stop_is_idempotent_and_final(self, source_and_cache):
        source, cache = source_and_cache
        await source.start(["AAPL"])
        await source.stop()
        await source.stop()
        version = cache.version
        await asyncio.sleep(0.05)
        assert cache.version == version  # no writes after stop()
