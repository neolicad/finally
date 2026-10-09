"""Massive (formerly Polygon.io) REST API source for real market data."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, timedelta
from typing import Literal

from massive import RESTClient
from massive.exceptions import BadResponse
from massive.rest.models import SnapshotMarketType, TickerSnapshot

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker

logger = logging.getLogger(__name__)

Mode = Literal["snapshot", "eod"]


def _is_not_authorized(exc: Exception) -> bool:
    """True when Massive says our plan does not include the endpoint (HTTP 403)."""
    text = str(exc)
    return "NOT_AUTHORIZED" in text or "not entitled" in text.lower()


def parse_snapshot(snap: TickerSnapshot) -> tuple[str, float, float | None, float | None] | None:
    """Turn one TickerSnapshot into (ticker, price, timestamp_s, reference_price).

    Returns None when the snapshot carries no usable price.
    price:     last trade, falling back to today's close-so-far
    timestamp: last trade SIP time (ns) -> seconds, falling back to `updated` (ns)
    reference: previous day's close, used for the daily change %
    """
    trade, day, prev = snap.last_trade, snap.day, snap.prev_day
    price = trade.price if trade and trade.price else (day.close if day and day.close else None)
    if not snap.ticker or not price:
        return None
    ns = (trade.sip_timestamp if trade else None) or snap.updated
    timestamp = ns / 1e9 if ns else None
    reference = prev.close if prev and prev.close else None
    return snap.ticker, float(price), timestamp, reference


class MassiveDataSource(MarketDataSource):
    """Polls Massive and writes prices into the PriceCache.

    snapshot mode (paid plans): one get_snapshot_all() call per poll covers
        every tracked ticker; price = last trade.
    eod mode (free plan): snapshots return 403, so we switch automatically to
        get_grouped_daily_aggs() (one call, every US stock, end of day) and
        refresh it every `eod_refresh_interval` seconds. Prices are static.
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
        eod_refresh_interval: float = 900.0,
        client: RESTClient | None = None,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._eod_interval = eod_refresh_interval
        self._client = client
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._mode: Mode = "snapshot"
        self._eod_bars: dict[str, tuple[float, float, float]] = {}  # ticker -> (close, open, ts)
        self._eod_fetched_at = 0.0
        self._last_error: str | None = None

    @property
    def mode(self) -> Mode:
        return self._mode

    # --- MarketDataSource ---

    async def start(self, tickers: list[str]) -> None:
        if self._task is not None:
            raise RuntimeError("MassiveDataSource already started")
        if self._client is None:
            self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        await self._poll_once()  # fill the cache before returning
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval, %s mode",
            len(self._tickers), self._interval, self._mode,
        )

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker in self._tickers:
            return
        self._tickers.append(ticker)
        # Price the new ticker now rather than up to one poll interval later,
        # so a chat "buy NEWTICKER" can execute straight away.
        if self._mode == "eod":
            self._write_eod(ticker)
        elif self._client is not None:
            await self._fetch_one(ticker)
        logger.info("Massive: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Polling ---

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """One cycle. Never raises: errors are logged and retried next interval."""
        if not self._tickers or self._client is None:
            return
        try:
            if self._mode == "snapshot":
                await self._poll_snapshots()
            else:
                await self._poll_eod()
            if self._last_error is not None:
                logger.info("Massive poll recovered")
                self._last_error = None
        except BadResponse as exc:
            if self._mode == "snapshot" and _is_not_authorized(exc):
                logger.warning(
                    "Massive plan has no snapshot access (free plan?). "
                    "Switching to end-of-day prices; they will not move intraday."
                )
                self._mode = "eod"
                await self._poll_once()
                return
            self._log_error(exc)
        except Exception as exc:  # network errors, timeouts, bad payloads
            self._log_error(exc)

    async def _poll_snapshots(self) -> None:
        tickers = tuple(self._tickers)  # the worker thread gets an immutable copy
        snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        self._write_snapshots(snapshots)

    async def _fetch_one(self, ticker: str) -> None:
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots, (ticker,))
            self._write_snapshots(snapshots)
        except Exception as exc:
            logger.warning("Massive: immediate fetch for %s failed: %s", ticker, exc)

    def _write_snapshots(self, snapshots: list[TickerSnapshot]) -> None:
        tracked = set(self._tickers)  # re-read: a ticker may have been removed mid-fetch
        written = 0
        for snap in snapshots:
            parsed = parse_snapshot(snap)
            if parsed is None:
                logger.debug("Massive: no usable price for %s", getattr(snap, "ticker", "?"))
                continue
            ticker, price, timestamp, reference = parsed
            if ticker not in tracked:
                continue
            self._cache.update(ticker, price, timestamp=timestamp, reference_price=reference)
            written += 1
        logger.debug("Massive poll: %d/%d tickers priced", written, len(tracked))

    async def _poll_eod(self) -> None:
        if time.monotonic() - self._eod_fetched_at >= self._eod_interval or not self._eod_bars:
            self._eod_bars = await asyncio.to_thread(self._fetch_latest_eod)
            self._eod_fetched_at = time.monotonic()
        for ticker in self._tickers:
            self._write_eod(ticker)

    def _write_eod(self, ticker: str) -> None:
        bar = self._eod_bars.get(ticker)
        if bar is None:
            return
        close, open_, ts = bar
        # Only write when the bar changed, so SSE does not resend a static price.
        current = self._cache.get(ticker)
        if current is None or current.timestamp != ts:
            self._cache.update(ticker, close, timestamp=ts, reference_price=open_)

    def _log_error(self, exc: Exception) -> None:
        message = f"{type(exc).__name__}: {exc}"
        if message != self._last_error:  # log each distinct error once, not every poll
            logger.error("Massive poll failed: %s", message)
            self._last_error = message
        else:
            logger.debug("Massive poll failed again: %s", message)

    # --- Blocking client calls (run in a worker thread) ---

    def _fetch_snapshots(self, tickers: tuple[str, ...]) -> list[TickerSnapshot]:
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=list(tickers),
        )

    def _fetch_latest_eod(self, lookback_days: int = 5) -> dict[str, tuple[float, float, float]]:
        """{ticker: (close, open, timestamp_s)} for the latest trading day with data.

        Starts from yesterday (today's bar is not final). At most `lookback_days`
        calls; a long weekend plus a holiday needs 4, inside the free 5/min.
        """
        day = date.today()
        for _ in range(lookback_days):
            day -= timedelta(days=1)
            bars = self._client.get_grouped_daily_aggs(day.isoformat(), adjusted=True)
            if bars:
                return {
                    b.ticker: (b.close, b.open, b.timestamp / 1000.0)  # ms -> s
                    for b in bars
                    if b.ticker and b.close
                }
        return {}
