"""MassiveDataSource tests using real massive model objects (not MagicMock)."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from massive.exceptions import BadResponse
from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource, parse_snapshot

NS = 1_707_580_800_000_000_000  # 2024-02-10T16:00:00Z in nanoseconds


def snapshot(ticker: str, price: float | None, prev_close: float = 100.0) -> TickerSnapshot:
    """A real TickerSnapshot built from the JSON shape in MASSIVE_API.md section 2.1."""
    payload = {"ticker": ticker, "prevDay": {"c": prev_close}, "updated": NS}
    if price is not None:
        payload["lastTrade"] = {"p": price, "s": 100, "x": 4, "t": NS}
    return TickerSnapshot.from_dict(payload)


def make_source(tickers, client=None) -> tuple[MassiveDataSource, PriceCache]:
    cache = PriceCache()
    source = MassiveDataSource("key", cache, poll_interval=60.0, client=client or MagicMock())
    source._tickers = list(tickers)
    return source, cache


class TestParseSnapshot:
    def test_last_trade_price_and_ns_timestamp(self):
        assert parse_snapshot(snapshot("AAPL", 190.5, 188.0)) == (
            "AAPL", 190.5, 1707580800.0, 188.0,
        )

    def test_falls_back_to_day_close(self):
        snap = TickerSnapshot.from_dict({"ticker": "AAPL", "day": {"c": 191.0}, "updated": NS})
        assert parse_snapshot(snap) == ("AAPL", 191.0, 1707580800.0, None)

    def test_no_price_returns_none(self):
        assert parse_snapshot(snapshot("AAPL", None)) is None


class TestPolling:
    async def test_poll_writes_price_timestamp_and_reference(self):
        source, cache = make_source(["AAPL", "GOOGL"])
        source._client.get_snapshot_all.return_value = [
            snapshot("AAPL", 190.5, 188.0),
            snapshot("GOOGL", 175.25),
        ]
        await source._poll_once()
        aapl = cache.get("AAPL")
        assert aapl.price == 190.5
        assert aapl.timestamp == 1707580800.0
        assert aapl.reference_price == 188.0
        assert cache.get_price("GOOGL") == 175.25

    async def test_snapshot_without_price_is_skipped(self):
        source, cache = make_source(["AAPL", "NEW"])
        source._client.get_snapshot_all.return_value = [
            snapshot("AAPL", 190.5),
            snapshot("NEW", None),
        ]
        await source._poll_once()
        assert cache.get_price("AAPL") == 190.5
        assert "NEW" not in cache

    async def test_ticker_removed_mid_fetch_is_not_rewritten(self):
        source, cache = make_source(["AAPL", "TSLA"])

        def fetch(*_args, **_kwargs):
            source._tickers.remove("TSLA")  # simulates remove_ticker during the HTTP call
            return [snapshot("AAPL", 190.0), snapshot("TSLA", 250.0)]

        source._client.get_snapshot_all.side_effect = fetch
        await source._poll_once()
        assert "TSLA" not in cache

    @pytest.mark.parametrize("error", [BadResponse('{"status":"ERROR"}'), TimeoutError("slow")])
    async def test_errors_are_swallowed(self, error):
        source, cache = make_source(["AAPL"])
        source._client.get_snapshot_all.side_effect = error
        await source._poll_once()  # must not raise
        assert len(cache) == 0
        assert source.mode == "snapshot"

    async def test_repeated_error_is_logged_once_at_error(self, caplog):
        source, _ = make_source(["AAPL"])
        source._client.get_snapshot_all.side_effect = TimeoutError("slow")
        with caplog.at_level("DEBUG", logger="app.market.massive_client"):
            await source._poll_once()
            await source._poll_once()
        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(errors) == 1

    async def test_recovery_clears_error_state(self):
        source, cache = make_source(["AAPL"])
        source._client.get_snapshot_all.side_effect = TimeoutError("slow")
        await source._poll_once()
        assert source._last_error is not None
        source._client.get_snapshot_all.side_effect = None
        source._client.get_snapshot_all.return_value = [snapshot("AAPL", 190.0)]
        await source._poll_once()
        assert source._last_error is None
        assert cache.get_price("AAPL") == 190.0

    async def test_empty_ticker_list_makes_no_call(self):
        source, _ = make_source([])
        await source._poll_once()
        source._client.get_snapshot_all.assert_not_called()

    async def test_403_switches_to_eod_mode(self):
        source, cache = make_source(["AAPL", "MSFT"])
        source._client.get_snapshot_all.side_effect = BadResponse(
            '{"status":"NOT_AUTHORIZED","message":"You are not entitled to this data."}'
        )
        source._client.get_grouped_daily_aggs.side_effect = [
            [],  # yesterday was a holiday
            [
                SimpleNamespace(ticker="AAPL", open=180.0, close=185.0, timestamp=1_707_523_200_000),
                SimpleNamespace(ticker="ZZZZ", open=1.0, close=2.0, timestamp=1_707_523_200_000),
            ],
        ]
        await source._poll_once()
        assert source.mode == "eod"
        aapl = cache.get("AAPL")
        assert (aapl.price, aapl.reference_price, aapl.timestamp) == (185.0, 180.0, 1707523200.0)
        assert "MSFT" not in cache and "ZZZZ" not in cache

        version = cache.version
        await source._poll_once()  # unchanged EOD bar: no rewrite, no extra API call
        assert cache.version == version
        assert source._client.get_grouped_daily_aggs.call_count == 2

    async def test_eod_with_no_data_in_lookback_leaves_cache_empty(self):
        source, cache = make_source(["AAPL"])
        source._mode = "eod"
        source._client.get_grouped_daily_aggs.return_value = []
        await source._poll_once()
        assert len(cache) == 0
        assert source._client.get_grouped_daily_aggs.call_count == 5


class TestTickerManagement:
    async def test_add_ticker_fetches_immediately(self):
        source, cache = make_source([])
        source._client.get_snapshot_all.return_value = [snapshot("PYPL", 61.2)]
        await source.add_ticker("pypl")
        assert source.get_tickers() == ["PYPL"]
        assert cache.get_price("PYPL") == 61.2

    async def test_add_ticker_failure_still_tracks(self):
        source, cache = make_source([])
        source._client.get_snapshot_all.side_effect = TimeoutError("slow")
        await source.add_ticker("PYPL")
        assert source.get_tickers() == ["PYPL"]
        assert "PYPL" not in cache

    async def test_add_existing_ticker_is_noop(self):
        source, _ = make_source(["AAPL"])
        await source.add_ticker("aapl")
        assert source.get_tickers() == ["AAPL"]
        source._client.get_snapshot_all.assert_not_called()

    async def test_add_ticker_in_eod_mode_uses_cached_bars(self):
        source, cache = make_source([])
        source._mode = "eod"
        source._eod_bars = {"PYPL": (61.0, 60.0, 1707523200.0)}
        await source.add_ticker("PYPL")
        assert cache.get_price("PYPL") == 61.0
        source._client.get_snapshot_all.assert_not_called()

    async def test_remove_ticker_clears_cache(self):
        source, cache = make_source(["AAPL"])
        cache.update("AAPL", 190.0)
        await source.remove_ticker("aapl")
        assert source.get_tickers() == []
        assert "AAPL" not in cache

    async def test_invalid_ticker_rejected(self):
        source, _ = make_source([])
        with pytest.raises(ValueError):
            await source.add_ticker("bad ticker!")

    async def test_start_and_stop(self):
        client = MagicMock()
        client.get_snapshot_all.return_value = [snapshot("AAPL", 190.0)]
        cache = PriceCache()
        source = MassiveDataSource("key", cache, poll_interval=60.0, client=client)
        await source.start(["aapl", "AAPL"])
        assert source.get_tickers() == ["AAPL"]
        assert cache.get_price("AAPL") == 190.0  # filled before start() returned
        await source.stop()
        await source.stop()  # idempotent

    async def test_double_start_raises(self):
        client = MagicMock()
        client.get_snapshot_all.return_value = []
        source = MassiveDataSource("key", PriceCache(), poll_interval=60.0, client=client)
        await source.start(["AAPL"])
        try:
            with pytest.raises(RuntimeError):
                await source.start(["AAPL"])
        finally:
            await source.stop()
