"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writer: exactly one MarketDataSource (simulator or Massive poller).
    Readers: SSE stream, portfolio valuation, trade execution, LLM context.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0  # bumped on every write (update or remove)

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,
    ) -> PriceUpdate:
        """Record a new price and return the resulting PriceUpdate.

        previous_price comes from the cached entry; the first update for a
        ticker has previous_price == price. reference_price is kept from the
        cached entry unless a new one is passed; the first update falls back to
        the price itself.
        """
        with self._lock:
            prev = self._prices.get(ticker)
            if reference_price is None:
                reference_price = prev.reference_price if prev else price
            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(prev.price if prev else price, 2),
                timestamp=time.time() if timestamp is None else timestamp,
                reference_price=round(reference_price, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow copy of every cached price."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        """Drop a ticker. Bumps the version so SSE clients see it disappear."""
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
