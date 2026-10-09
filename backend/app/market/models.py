"""Data models for market data."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

# 1-5 letters, optionally followed by a class suffix such as ".B" (BRK.B)
_TICKER_RE = re.compile(r"^[A-Z]{1,5}(\.[A-Z]{1,2})?$")


def normalize_ticker(ticker: str) -> str:
    """Uppercase and strip a ticker symbol. Raises ValueError if it is not a valid symbol."""
    t = ticker.strip().upper()
    if not _TICKER_RE.match(t):
        raise ValueError(f"Invalid ticker symbol: {ticker!r}")
    return t


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    # Baseline for the "daily change" column: previous close (Massive) or the
    # first price seen since process start (simulator).
    reference_price: float | None = None

    @property
    def change(self) -> float:
        """Absolute change since the previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percent change since the previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' compared with the previous update."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    @property
    def day_change_percent(self) -> float:
        """Percent change against reference_price (the watchlist's daily change %)."""
        ref = self.reference_price
        if not ref:
            return 0.0
        return round((self.price - ref) / ref * 100, 4)

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "reference_price": self.reference_price,
            "day_change_percent": self.day_change_percent,
        }
