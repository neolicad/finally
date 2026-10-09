"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    A source pushes prices into a shared PriceCache on its own schedule.
    Nothing downstream asks the source for a price; it reads the cache.

    Contract every implementation honours:
      * start() fills the cache for the initial tickers *before* returning
        (best effort for Massive: an API failure leaves the cache empty but
        start() still returns and the loop keeps retrying).
      * A failing update cycle is logged; the background task never dies.
      * Ticker symbols are normalized with normalize_ticker() on the way in.
      * add_ticker / remove_ticker are idempotent.
      * stop() is idempotent; after it returns, the source never writes again.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Seed the cache for `tickers` and launch the background task. Call once."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task. Safe to call more than once."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and drop it from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers."""
