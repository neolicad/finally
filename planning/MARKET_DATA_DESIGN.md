# Market Data Backend: Detailed Design

This document is the implementation spec for FinAlly's market data subsystem: the unified API, the GBM simulator, the Massive (Polygon.io) client, the price cache, and the SSE stream. It gives complete code for each module in `backend/app/market/`, the tests that pin the behaviour down, and how the rest of the backend and the frontend use it.

It builds on, and where they disagree replaces:

- `PLAN.md` §6 (requirements)
- `MARKET_INTERFACE.md` (the interface contract)
- `MARKET_SIMULATOR.md` (the GBM model)
- `MASSIVE_API.md` (Massive REST API and Python client reference)

**Status.** A first version of every module already exists in `backend/app/market/` with 73 passing tests. This design keeps its shape and public API and fixes the defects listed in §2. All code below has been run against the existing test suite plus the new tests in §14 (96 passing, `ruff check` clean) with `massive` 2.2.0 installed.

---

## Contents

1. [Architecture](#1-architecture)
2. [Changes from the current code](#2-changes-from-the-current-code)
3. [Data model: `models.py`](#3-data-model-modelspy)
4. [Price cache: `cache.py`](#4-price-cache-cachepy)
5. [Unified interface: `interface.py`](#5-unified-interface-interfacepy)
6. [Seed data: `seed_prices.py`](#6-seed-data-seed_pricespy)
7. [Simulator: `simulator.py`](#7-simulator-simulatorpy)
8. [Massive client: `massive_client.py`](#8-massive-client-massive_clientpy)
9. [Factory: `factory.py`](#9-factory-factorypy)
10. [SSE stream: `stream.py`](#10-sse-stream-streampy)
11. [Package exports: `__init__.py`](#11-package-exports-__init__py)
12. [Using it from the rest of the backend](#12-using-it-from-the-rest-of-the-backend)
13. [Frontend contract](#13-frontend-contract)
14. [Tests](#14-tests)
15. [Configuration and operations](#15-configuration-and-operations)
16. [Implementation checklist](#16-implementation-checklist)

---

## 1. Architecture

```
                    create_market_data_source(cache)
                        │  MASSIVE_API_KEY set?
              ┌─────────┴──────────┐
             yes                   no
              │                     │
     MassiveDataSource       SimulatorDataSource
     ├ snapshot mode (paid)  └ GBMSimulator.step() every 0.5s
     │  get_snapshot_all
     │  every 15s
     └ eod mode (free, auto)
        get_grouped_daily_aggs
        every 15 min
              │                     │
              └────── writes ───────┘
                        ▼
                   PriceCache   in-memory, Lock-guarded, versioned
                        │ reads
       ┌────────────────┼─────────────────┬────────────────┐
  SSE /api/stream   GET /api/portfolio   trade execution   LLM chat context
  (0.5s, on change)  /api/watchlist
```

### Module layout

```
backend/app/market/
├── __init__.py        public exports
├── models.py          PriceUpdate, normalize_ticker
├── cache.py           PriceCache
├── interface.py       MarketDataSource (ABC)
├── seed_prices.py     constants for the simulator
├── simulator.py       GBMSimulator (math) + SimulatorDataSource (asyncio adapter)
├── massive_client.py  MassiveDataSource, parse_snapshot
├── factory.py         create_market_data_source
└── stream.py          create_stream_router (SSE)
```

### Rules

1. **Push, not pull.** A source writes into `PriceCache` on its own schedule. Request handlers only read the cache, so they are fast and never wait on the network.
2. **One writer, many readers.** One source per process, created in the FastAPI lifespan and stopped at shutdown.
3. **The app owns the ticker set; the source tracks it.** The tracked set is the union of the watchlist and open positions (PLAN.md §6). The app calls `add_ticker` / `remove_ticker` as that set changes (§12.3).
4. **Every ticker is normalized on entry** with `normalize_ticker()` (uppercase, stripped, validated).

### Concurrency model

- Everything runs on the single asyncio event loop except Massive HTTP calls, which go through `asyncio.to_thread` because the `massive` client is synchronous (urllib3).
- `PriceCache` uses a `threading.Lock`, so it is safe whichever thread writes.
- The Massive worker thread receives an immutable tuple of tickers, never the live list, so `add_ticker` / `remove_ticker` on the loop cannot race with an in-flight request. After the fetch returns, results are filtered against the *current* ticker set so a ticker removed mid-fetch is not written back (§8.4).

---

## 2. Changes from the current code

| # | Area | Defect in current code | Fix in this design |
|---|---|---|---|
| 1 | Massive | Reads `snap.last_trade.timestamp / 1000`. `LastTrade` has no `timestamp`; the field is `sip_timestamp` in **nanoseconds**. Every live snapshot raises `AttributeError` and the cache is never filled. Tests miss it because `MagicMock` invents attributes. | `parse_snapshot()` uses `sip_timestamp / 1e9`, falls back to `updated`. Tests build real `TickerSnapshot.from_dict(...)` objects. |
| 2 | Massive | A free API key gets HTTP 403 on snapshots every 15s forever, and prices never appear. | On 403 `NOT_AUTHORIZED`, switch to **eod mode**: `get_grouped_daily_aggs` (free plan, one call for all US stocks), refreshed every 15 min. |
| 3 | Massive | A ticker added to the watchlist has no price for up to 15s, so an LLM "buy PYPL" for a new ticker fails with "no price". | `add_ticker` fetches that ticker immediately (snapshot mode) or writes it from the cached EOD bars (eod mode). |
| 4 | Massive | The worker thread reads `self._tickers` while the loop may reassign it; a ticker removed during a fetch is written back to the cache. | Thread gets a tuple copy; results are filtered against the current set. |
| 5 | Massive | Same error logged at ERROR every poll. | Each distinct error is logged once; repeats go to DEBUG; recovery is logged. |
| 6 | Stream | `router` is module-level, so calling `create_stream_router()` twice registers the route twice on the same router. | Router created inside the factory. |
| 7 | Stream | Empty cache sends nothing, so when the last ticker is removed the client keeps showing it. | Always send on version change, including `data: {}`. |
| 8 | Stream | No heartbeat. With Massive (15s polls, or static EOD prices) the connection is idle and proxies may close it. | `: ping` comment line after 15s without data. |
| 9 | Cache | `remove()` does not bump `version`, so SSE does not push the removal. | `remove()` bumps `version` when it removed something. |
| 10 | Cache | `timestamp or time.time()` treats `0.0` as missing. | `time.time() if timestamp is None else timestamp`. |
| 11 | Cache / model | Only tick-to-tick change exists. PLAN.md §10 wants a **daily change %** in the watchlist. | `PriceUpdate.reference_price` + `day_change_percent`. Massive: previous close (snapshot) or day open (eod). Simulator: first price since process start. |
| 12 | Simulator | Tickers are not normalized; `start(["aapl"])` tracks a different symbol from `"AAPL"`; duplicates are allowed. | `normalize_ticker` + de-duplication in `start` / `add_ticker` / `remove_ticker`. Invalid symbols raise `ValueError`. |
| 13 | Both | Calling `start()` twice silently leaks a second background task. | Raises `RuntimeError`. |
| 14 | Factory | Poll interval is hard-coded. PLAN.md §6 asks for a configurable one. | Optional `MASSIVE_POLL_INTERVAL` env var (default 15s, minimum 1s). |

The public API (`PriceUpdate`, `PriceCache`, `MarketDataSource`, `create_market_data_source`, `create_stream_router`) keeps every existing name and signature. The only additions are optional parameters and new fields.

---

## 3. Data model: `models.py`

`PriceUpdate` is the one value type that flows from a source, through the cache, to every consumer. It is frozen, so a reader can hold one without locking.

| Field / property | Meaning |
|---|---|
| `price` | Latest price, rounded to cents |
| `previous_price` | Price at the previous cache write for this ticker (drives the flash colour) |
| `timestamp` | Unix seconds of the price (exchange time for Massive, wall clock for the simulator) |
| `reference_price` | Baseline for the daily change column |
| `change`, `change_percent`, `direction` | Tick-to-tick, from `previous_price` |
| `day_change_percent` | From `reference_price` |

`normalize_ticker()` lives here too, because both sources and the API routes need it and it has no dependencies. The regex accepts 1–5 letters plus an optional class suffix (`BRK.B`).

```python
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
```

Example:

```python
>>> u = PriceUpdate("AAPL", price=190.42, previous_price=190.00, reference_price=188.00)
>>> u.direction, u.change, u.change_percent, u.day_change_percent
('up', 0.42, 0.2211, 1.2872)
>>> normalize_ticker(" brk.b ")
'BRK.B'
>>> normalize_ticker("not a ticker")
ValueError: Invalid ticker symbol: 'not a ticker'
```

---

## 4. Price cache: `cache.py`

The cache is the only shared state. Its rules:

- `update()` computes `previous_price` from the cached entry. The first write for a ticker has `previous_price == price` (direction `flat`).
- `reference_price` is sticky: once set, it carries over until a source passes a new one. That lets the simulator set it implicitly (first price) and Massive set it explicitly (previous close) through the same method.
- Prices are rounded to 2 decimals on the way in. The simulator keeps full precision internally (§7), so rounding never accumulates.
- `version` bumps on every write *and* every successful removal. The SSE loop compares versions instead of diffing prices.

```python
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
```

Example:

```python
cache = PriceCache()
cache.update("AAPL", 190.00)                         # first write: flat, reference 190.00
u = cache.update("AAPL", 190.42)
u.previous_price, u.direction, u.reference_price     # (190.0, 'up', 190.0)
cache.update("AAPL", 191.10, reference_price=188.0)  # Massive passes previous close
cache.get_price("MSFT")                              # None: not tracked / not priced yet
```

---

## 5. Unified interface: `interface.py`

Both sources implement this ABC. Nothing outside `app/market/` imports a concrete source class; it calls the factory and talks to `MarketDataSource`.

```python
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
```

Lifecycle:

```python
cache = PriceCache()
source = create_market_data_source(cache)      # unstarted
await source.start(["AAPL", "GOOGL"])          # cache now has AAPL, GOOGL
await source.add_ticker("pypl")                # tracked as PYPL, priced immediately
await source.remove_ticker("GOOGL")            # gone from source and cache
source.get_tickers()                           # ['AAPL', 'PYPL']
await source.stop()                            # no more writes; safe to call again
```

---

## 6. Seed data: `seed_prices.py`

Unchanged from the current code. Constants only, so the simulator's parameters can be tuned without touching logic.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

---

## 7. Simulator: `simulator.py`

### 7.1 Model

Each tick (0.5s) moves every price by geometric Brownian motion:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
dt      = 0.5 / (252 · 6.5 · 3600) ≈ 8.48e-8      (one tick as a fraction of a trading year)
```

- `exp(...)` keeps prices positive and makes moves proportional to price.
- AAPL (`σ = 0.22`): one tick has a standard deviation of about 0.0064% (≈ $0.012). Over an hour that compounds to about 0.5%, which looks like a calm real stock.
- `Z` is a vector of *correlated* standard normals: draw `z ~ N(0, I)`, then `Z = L·z` where `L = cholesky(C)`. Correlations (`_pairwise_correlation`): tech/tech 0.6, finance/finance 0.5, anything with TSLA 0.3, everything else 0.3. With a constant baseline of 0.3 and higher constant blocks, `C` is always positive definite, so `np.linalg.cholesky` never fails, including for any number of unknown tickers.
- `L` is rebuilt only when the ticker set changes (O(n²), trivial for n < 50), never per tick.
- **Events:** after the GBM step, each ticker has probability `event_probability` (0.001) of a ±2–5% jump. With 10 tickers at 2 ticks/s that is about one event every 50s across the watchlist.
- Unknown tickers (e.g. added through chat) start at a random $50–$300 with `DEFAULT_PARAMS`.

**Tuning for demos:** pass a larger `dt` (e.g. `GBMSimulator.DEFAULT_DT * 10`) to `SimulatorDataSource(dt=...)`. That scales every ticker's volatility together and keeps their relative behaviour, which editing individual `sigma` values would not.

### 7.2 Code structure

Two classes, one job each:

- `GBMSimulator`: pure, synchronous math. No asyncio, no cache. Testable by seeding `np.random` and `random` and calling `step()`.
- `SimulatorDataSource`: the `MarketDataSource` adapter. Owns the asyncio task and writes to the cache.

`GBMSimulator._prices` holds full-precision floats; only the values returned from `step()` / `get_price()` are rounded.

### 7.3 Code

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Correlated Geometric Brownian Motion. Pure math: no asyncio, no I/O.

        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
    """

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # one 500ms tick, ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._sqrt_dt = math.sqrt(dt)
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}  # full precision, never rounded
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance every ticker one tick. Returns {ticker: price rounded to cents}."""
        n = len(self._tickers)
        if n == 0:
            return {}
        z = np.random.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z

        out: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * self._sqrt_dt * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:
                shock = random.choice([-1, 1]) * random.uniform(0.02, 0.05)
                self._prices[ticker] *= 1 + shock
                logger.debug("Random event on %s: %+.1f%%", ticker, shock * 100)

            out[ticker] = round(self._prices[ticker], 2)
        return out

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        price = self._prices.get(ticker)
        return round(price, 2) if price is not None else None

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        tech, finance = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource adapter: drives GBMSimulator from an asyncio task."""

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
        dt: float = GBMSimulator.DEFAULT_DT,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._dt = dt
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        if self._task is not None:
            raise RuntimeError("SimulatorDataSource already started")
        normalized = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        self._sim = GBMSimulator(normalized, dt=self._dt, event_probability=self._event_prob)
        for ticker in normalized:
            self._seed_cache(ticker)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(normalized))

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim is None or ticker in self._sim.get_tickers():
            return
        self._sim.add_ticker(ticker)
        self._seed_cache(ticker)  # price visible before the next tick
        logger.info("Simulator: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim is not None:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    def _seed_cache(self, ticker: str) -> None:
        price = self._sim.get_price(ticker) if self._sim else None
        if price is not None:
            self._cache.update(ticker=ticker, price=price)

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim is not None:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

### 7.4 Behaviour notes

- `start()` writes every seed price to the cache before creating the task, so the first SSE event already has the full watchlist.
- `add_ticker()` writes the new ticker's seed price immediately, so it can be traded straight away.
- The loop catches every exception from a step, logs it with a traceback, and keeps going. Only cancellation (from `stop()`) ends it.
- The loop uses a plain `asyncio.sleep(interval)`, so the real period is `interval + step time` (microseconds). Drift does not matter here.

---

## 8. Massive client: `massive_client.py`

### 8.1 Two modes, chosen automatically

| | snapshot mode | eod mode |
|---|---|---|
| When | Default. Paid plans (Starter and up). | After the first HTTP 403 `NOT_AUTHORIZED` from the snapshot endpoint (free plan). |
| Endpoint | `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=…` via `get_snapshot_all` | `GET /v2/aggs/grouped/locale/us/market/stocks/{date}` via `get_grouped_daily_aggs` |
| Calls | 1 per poll for all tickers | 1 per refresh (up to 5 when walking back over weekends/holidays) |
| Cadence | `poll_interval` (15s default) | Re-fetch every `eod_refresh_interval` (15 min); the 15s loop just writes cached bars for new tickers |
| `price` | `last_trade.price`, else `day.close` | That day's `close` |
| `timestamp` | `last_trade.sip_timestamp` (ns) / 1e9, else `updated` (ns) / 1e9 | bar `timestamp` (ms) / 1000 |
| `reference_price` | `prev_day.close` → watchlist shows today's change | That day's `open` → shows that day's move |
| Prices move? | Yes, every poll (15-min delayed on Starter, real-time on Advanced) | No, static until the next trading day |

The switch happens once and is logged at WARNING. It is one-way for the life of the process: a plan upgrade needs a restart.

The eod walk-back starts at *yesterday*, because today's grouped bar is incomplete or empty during the session. The worst realistic case (Tuesday after a Monday holiday) needs 4 calls: Mon, Sun, Sat, Fri. That fits inside the free plan's 5 requests/min.

### 8.2 Snapshot parsing

`parse_snapshot()` is a free function so it can be tested without a source object. It uses the real `massive` model attribute names, verified against `massive` 2.2.0:

| JSON | Python attribute | Unit |
|---|---|---|
| `lastTrade.p` | `snap.last_trade.price` | dollars |
| `lastTrade.t` | `snap.last_trade.sip_timestamp` | **nanoseconds** |
| `day.c` | `snap.day.close` | dollars |
| `prevDay.c` | `snap.prev_day.close` | dollars |
| `updated` | `snap.updated` | nanoseconds |
| grouped bar `t` | `GroupedDailyAgg.timestamp` | **milliseconds** |

Any of `last_trade`, `day`, `prev_day` can be `None` (for example a new listing with no trades yet, or between midnight and 4am ET when snapshot data is reset). A snapshot with no usable price is skipped, not an error.

Sample input and output:

```python
snap = TickerSnapshot.from_dict({
    "ticker": "AAPL",
    "prevDay":  {"c": 188.00},
    "lastTrade": {"p": 190.42, "s": 100, "x": 4, "t": 1791523200500000000},
    "updated": 1791523200500000000,
})
parse_snapshot(snap)   # ('AAPL', 190.42, 1791523200.5, 188.0)
```

### 8.3 Error handling

The `massive` client already retries transient failures itself (`retries=3`, urllib3 backoff). It raises `massive.exceptions.BadResponse(body_text)` for any non-200 response; the exception carries the response body, not a status code, so the 403 check looks for `NOT_AUTHORIZED` / `not entitled` in the text.

| Failure | Behaviour |
|---|---|
| 403 `NOT_AUTHORIZED` in snapshot mode | Switch to eod mode and poll again immediately |
| 401 bad key, 429 rate limit, 5xx, network error, timeout | Log once at ERROR (repeats at DEBUG), keep the last cached prices, retry next interval |
| Malformed or price-less snapshot | Skip that ticker (DEBUG log) |
| Immediate fetch in `add_ticker` fails | WARNING; the ticker is still tracked and priced on the next poll |

`_poll_once()` never raises, so `start()` returns even when the API is down; the cache is simply empty until a poll succeeds.

### 8.4 The removed-mid-fetch race

```
loop:   _poll_snapshots ── to_thread(fetch, ("AAPL","TSLA")) ───────────────► _write_snapshots
loop:                           remove_ticker("TSLA") → cache.remove("TSLA")
```

Without care, `_write_snapshots` would put TSLA back in the cache after the user removed it, and it would stay there forever (no later poll would remove it). `_write_snapshots` re-reads `self._tickers` after the fetch and drops anything no longer tracked.

### 8.5 Code

```python
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
```

The optional `client` constructor argument lets tests inject a fake `RESTClient` without patching private methods.

---

## 9. Factory: `factory.py`

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)

DEFAULT_MASSIVE_POLL_INTERVAL = 15.0  # safe for the free plan's 5 requests/min


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Return an *unstarted* source chosen from the environment.

    MASSIVE_API_KEY set and non-blank -> MassiveDataSource
    otherwise                         -> SimulatorDataSource
    MASSIVE_POLL_INTERVAL (optional, seconds) tunes the Massive poll rate.
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if not api_key:
        logger.info("Market data source: GBM simulator")
        return SimulatorDataSource(price_cache=price_cache)

    interval = _float_env("MASSIVE_POLL_INTERVAL", DEFAULT_MASSIVE_POLL_INTERVAL)
    logger.info("Market data source: Massive API (poll every %.1fs)", interval)
    return MassiveDataSource(api_key=api_key, price_cache=price_cache, poll_interval=interval)


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Ignoring non-numeric %s=%r; using %.1f", name, raw, default)
        return default
    return max(value, 1.0)
```

| `MASSIVE_API_KEY` | `MASSIVE_POLL_INTERVAL` | Result |
|---|---|---|
| unset, empty, or whitespace | ignored | `SimulatorDataSource` |
| any other value | unset | `MassiveDataSource`, 15s |
| any other value | `5` | `MassiveDataSource`, 5s (paid plans) |
| any other value | `abc` | 15s, with a warning |
| any other value | `0.2` | clamped to 1s |

---

## 10. SSE stream: `stream.py`

### 10.1 Wire format

`GET /api/stream/prices`, `Content-Type: text/event-stream`. The stream begins with a `retry` directive, then sends one unnamed `data:` event (so `EventSource.onmessage` receives it) each time the cache version changes, checked every 0.5s:

```
retry: 1000

data: {"AAPL": {"ticker": "AAPL", "price": 190.42, "previous_price": 190.0, "timestamp": 1791523200.5, "change": 0.42, "change_percent": 0.2211, "direction": "up", "reference_price": 190.0, "day_change_percent": 0.2211}, "TSLA": {"ticker": "TSLA", "price": 249.1, "previous_price": 250.0, "timestamp": 1791523200.5, "change": -0.9, "change_percent": -0.36, "direction": "down", "reference_price": 250.0, "day_change_percent": -0.36}}

: ping

```

- Each event is the **complete** set of tracked tickers, keyed by symbol. A ticker missing from an event has been removed. A fresh client gets full state from its first event, so reconnects need no replay logic and no `Last-Event-ID`.
- `data: {}` means nothing is tracked.
- `: ping` is an SSE comment, sent after 15s without data. `EventSource` ignores it; it only keeps proxies and load balancers from closing an idle connection (Massive eod mode can be idle for hours).
- Size: about 230 bytes per ticker, so 20 tickers at 2 events/s is about 9 KB/s. Sending deltas is not worth the extra client logic at this scale.

### 10.2 Code

```python
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
    """One SSE event holding every cached price: data: {"AAPL": {...}, ...}"""
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
```

### 10.3 Behaviour notes

- Disconnects are detected through `request.is_disconnected()` on each iteration (within 0.5s), and through `CancelledError` when uvicorn tears down the response.
- Each connected client has its own generator, all reading the same cache. There is no per-client state beyond `last_version`.
- `X-Accel-Buffering: no` stops nginx (and similar proxies) from buffering events if the container is deployed behind one.

---

## 11. Package exports: `__init__.py`

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate               - Immutable price snapshot dataclass
    PriceCache                - Thread-safe in-memory price store
    MarketDataSource          - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router      - FastAPI router factory for the SSE endpoint
    normalize_ticker          - Uppercase + validate a ticker symbol
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate, normalize_ticker
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
    "normalize_ticker",
]
```

---

## 12. Using it from the rest of the backend

These are the integration points the backend agent writes outside `app/market/`. Function and module names here are suggestions; the behaviour is what matters.

### 12.1 Lifespan

```python
# backend/app/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db import init_db, tracked_tickers
from app.market import PriceCache, create_market_data_source, create_stream_router

price_cache = PriceCache()
market = create_market_data_source(price_cache)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()                                   # lazy schema + seed (PLAN.md §7)
    await market.start(tracked_tickers())       # cache is filled before we serve requests
    app.state.price_cache = price_cache
    app.state.market = market
    yield
    await market.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
# app.include_router(portfolio_router), watchlist_router, chat_router ...
# Mount the static frontend LAST so /api/* routes win:
# app.mount("/", StaticFiles(directory="static", html=True), name="static")
```

### 12.2 Tracked tickers

```python
# backend/app/db.py
def tracked_tickers(user_id: str = "default") -> list[str]:
    """Sorted union of watchlist and open positions (PLAN.md §6)."""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT ticker FROM watchlist WHERE user_id = ?
            UNION
            SELECT ticker FROM positions WHERE user_id = ? AND quantity > 0
            ORDER BY ticker
            """,
            (user_id, user_id),
        ).fetchall()
    return [r[0] for r in rows]
```

### 12.3 Keeping the source in sync

One helper decides whether a ticker should be tracked. Call it after every watchlist change and every trade, from both the REST routes and the chat flow:

```python
# backend/app/services/market_sync.py
from app.db import is_held, is_watched


async def sync_ticker(market: MarketDataSource, ticker: str) -> None:
    """Track `ticker` if it is watched or held; stop tracking it otherwise."""
    if is_watched(ticker) or is_held(ticker):
        await market.add_ticker(ticker)      # idempotent
    else:
        await market.remove_ticker(ticker)   # idempotent, also clears the cache
```

| Event | Effect of `sync_ticker` |
|---|---|
| Watchlist add | tracked (no-op if already held) |
| Watchlist remove, still held | stays tracked, so the position keeps its live price |
| Watchlist remove, not held | untracked, removed from the cache and the next SSE event |
| Buy (LLM auto-adds to watchlist first) | tracked |
| Sell that closes the position, still watched | stays tracked |
| Sell that closes the position, not watched | untracked |

### 12.4 Watchlist route

```python
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.market import normalize_ticker


class AddTicker(BaseModel):
    ticker: str


@router.post("/api/watchlist", status_code=201)
async def add_to_watchlist(body: AddTicker, request: Request):
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    db_add_watchlist(ticker)                               # INSERT OR IGNORE
    await sync_ticker(request.app.state.market, ticker)
    update = request.app.state.price_cache.get(ticker)
    return {"ticker": ticker, "price": update.price if update else None}


@router.delete("/api/watchlist/{ticker}", status_code=204)
async def remove_from_watchlist(ticker: str, request: Request):
    ticker = normalize_ticker(ticker)
    db_remove_watchlist(ticker)
    await sync_ticker(request.app.state.market, ticker)
```

With Massive, `add_ticker` fetches immediately, so a `None` price after the add means Massive has no data for that symbol (unknown or delisted). Returning `price: null` and letting the UI show "—" is enough; rejecting the add is an option if the backend agent prefers it. The simulator invents a price for any valid symbol.

### 12.5 Trade execution

```python
async def execute_trade(ticker: str, side: str, quantity: float, market, cache) -> dict:
    ticker = normalize_ticker(ticker)
    if side == "buy":
        await market.add_ticker(ticker)          # make sure it is priced (chat may buy unseen tickers)
    try:
        price = cache.get_price(ticker)
        if price is None:
            raise TradeError(f"No price available for {ticker}")
        # ... validate cash / shares, update positions + trades, record snapshot (PLAN.md §7) ...
        return {"ticker": ticker, "side": side, "quantity": quantity, "price": price}
    finally:
        # Untrack if a sell closed an unwatched position, or a buy of an unwatched ticker failed.
        await sync_ticker(market, ticker)
```

The fill price is the cache price at the moment of the trade. Under Massive it can be up to one poll interval old; that is accepted for a simulated portfolio.

### 12.6 Portfolio valuation and LLM context

```python
def portfolio_value(cash: float, positions: list[Position], cache: PriceCache) -> dict:
    rows, total = [], cash
    for p in positions:
        price = cache.get_price(p.ticker) or p.avg_cost      # fall back to cost if not priced yet
        value = p.quantity * price
        total += value
        rows.append({
            "ticker": p.ticker,
            "quantity": p.quantity,
            "avg_cost": p.avg_cost,
            "current_price": price,
            "market_value": round(value, 2),
            "unrealized_pnl": round((price - p.avg_cost) * p.quantity, 2),
            "pnl_percent": round((price / p.avg_cost - 1) * 100, 2) if p.avg_cost else 0.0,
        })
    return {"cash": round(cash, 2), "positions": rows, "total_value": round(total, 2)}
```

The 30-second `portfolio_snapshots` task and the chat prompt builder both call this same function, and the chat context lists the watchlist with `cache.get(t).to_dict()` for each ticker.

---

## 13. Frontend contract

### 13.1 Types

```ts
// frontend/src/lib/types.ts
export type Direction = "up" | "down" | "flat";

export interface PriceUpdate {
  ticker: string;
  price: number;
  previous_price: number;
  timestamp: number;            // Unix seconds
  change: number;               // since previous update
  change_percent: number;       // since previous update
  direction: Direction;         // drives the green/red flash
  reference_price: number | null;
  day_change_percent: number;   // watchlist "daily change %"
}

export type PriceSnapshot = Record<string, PriceUpdate>;
```

### 13.2 Connecting

```ts
// frontend/src/hooks/usePriceStream.ts
import { useEffect, useRef, useState } from "react";
import type { PriceSnapshot } from "@/lib/types";

export type ConnectionStatus = "connected" | "reconnecting" | "disconnected";
const MAX_POINTS = 600; // ~5 min of sparkline at 2 events/s

export function usePriceStream() {
  const [prices, setPrices] = useState<PriceSnapshot>({});
  const [status, setStatus] = useState<ConnectionStatus>("reconnecting");
  const history = useRef<Record<string, { t: number; p: number }[]>>({});

  useEffect(() => {
    const es = new EventSource("/api/stream/prices");
    es.onopen = () => setStatus("connected");
    es.onerror = () =>
      setStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
    es.onmessage = (e) => {
      const snap: PriceSnapshot = JSON.parse(e.data);
      for (const [ticker, u] of Object.entries(snap)) {
        const series = (history.current[ticker] ??= []);
        if (series.at(-1)?.t !== u.timestamp) series.push({ t: u.timestamp, p: u.price });
        if (series.length > MAX_POINTS) series.shift();
      }
      for (const ticker of Object.keys(history.current)) {
        if (!(ticker in snap)) delete history.current[ticker];   // removed upstream
      }
      setPrices(snap);  // full snapshot: replace, don't merge
    };
    return () => es.close();
  }, []);

  return { prices, status, history: history.current };
}
```

- **Replace, don't merge.** Each event is complete; a missing key means the ticker was removed.
- **Flash:** apply the flash class when `direction !== "flat"` and `timestamp` changed since the last render for that ticker.
- **Sparklines:** append only when `timestamp` changes. Under Massive the same price can arrive in several events.
- **Reconnects** are automatic (`retry: 1000`). The first event after reconnect is the full state.

---

## 14. Tests

Existing tests in `backend/tests/market/` (models, cache, simulator, simulator source, factory) keep passing unchanged. Replace `test_massive.py` and add three files.

### 14.1 `tests/market/test_massive.py` (replaces the MagicMock version)

Every snapshot is a real `TickerSnapshot`, so a wrong attribute name fails the test instead of being invented by `MagicMock`.

```python
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
    """A real TickerSnapshot built from the JSON shape in MASSIVE_API.md §2.1."""
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
        assert parse_snapshot(snapshot("AAPL", 190.5, 188.0)) == ("AAPL", 190.5, 1707580800.0, 188.0)

    def test_falls_back_to_day_close(self):
        snap = TickerSnapshot.from_dict({"ticker": "AAPL", "day": {"c": 191.0}, "updated": NS})
        assert parse_snapshot(snap) == ("AAPL", 191.0, 1707580800.0, None)

    def test_no_price_returns_none(self):
        assert parse_snapshot(snapshot("AAPL", None)) is None


@pytest.mark.asyncio
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


@pytest.mark.asyncio
class TestTickerManagement:
    async def test_add_ticker_fetches_immediately(self):
        source, cache = make_source([])
        source._client.get_snapshot_all.return_value = [snapshot("PYPL", 61.2)]
        await source.add_ticker("pypl")
        assert source.get_tickers() == ["PYPL"]
        assert cache.get_price("PYPL") == 61.2

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
```

### 14.2 `tests/market/test_contract.py`: both sources, one contract

```python
"""Behaviour every MarketDataSource must share, run against both implementations."""

import asyncio
from unittest.mock import MagicMock

import pytest
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


@pytest.fixture(params=["simulator", "massive"])
async def source_and_cache(request):
    cache = PriceCache()
    if request.param == "simulator":
        source = SimulatorDataSource(cache, update_interval=0.01)
    else:
        source = MassiveDataSource("key", cache, poll_interval=0.01, client=_fake_massive_client())
    yield source, cache
    await source.stop()


@pytest.mark.asyncio
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
```

### 14.3 `tests/market/test_stream.py`

```python
"""SSE stream tests."""

import json

import pytest

from app.market.cache import PriceCache
from app.market.stream import _generate_events, create_stream_router


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


@pytest.mark.asyncio
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

    async def test_removal_is_streamed(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        gen = _generate_events(cache, FakeRequest(2), interval=0)
        chunks = [await anext(gen), await anext(gen)]
        cache.remove("AAPL")
        chunks += [c async for c in gen]
        assert _events(chunks)[-1] == {}


def test_router_factory_is_reusable():
    a = create_stream_router(PriceCache())
    b = create_stream_router(PriceCache())
    assert len(a.routes) == len(b.routes) == 1
```

### 14.4 `tests/market/test_cache_reference.py`

```python
"""PriceCache: reference price, version on remove, timestamp handling."""

import pytest

from app.market.cache import PriceCache
from app.market.models import normalize_ticker


def test_reference_defaults_to_first_price_and_is_kept():
    cache = PriceCache()
    cache.update("AAPL", 200.0)
    update = cache.update("AAPL", 210.0)
    assert update.reference_price == 200.0
    assert update.day_change_percent == 5.0


def test_explicit_reference_overrides():
    cache = PriceCache()
    cache.update("AAPL", 200.0)
    assert cache.update("AAPL", 210.0, reference_price=205.0).reference_price == 205.0


def test_remove_bumps_version_only_when_present():
    cache = PriceCache()
    cache.update("AAPL", 200.0)
    v = cache.version
    cache.remove("AAPL")
    assert cache.version == v + 1
    cache.remove("AAPL")
    assert cache.version == v + 1


def test_zero_timestamp_is_respected():
    assert PriceCache().update("AAPL", 1.0, timestamp=0.0).timestamp == 0.0


@pytest.mark.parametrize("raw,expected", [(" aapl ", "AAPL"), ("brk.b", "BRK.B")])
def test_normalize_ticker(raw, expected):
    assert normalize_ticker(raw) == expected


@pytest.mark.parametrize("raw", ["", "TOOLONG", "AA PL", "12", "$AAPL"])
def test_normalize_ticker_rejects(raw):
    with pytest.raises(ValueError):
        normalize_ticker(raw)
```

### 14.5 Running

```bash
cd backend
uv run --extra dev pytest tests/market -v
uv run --extra dev ruff check app/ tests/
```

### 14.6 Manual check against the live API

There is no automated test against the real Massive API (it needs a key and costs requests). Before relying on Massive, run this once:

```bash
cd backend
MASSIVE_API_KEY=... uv run python -c "
import asyncio, logging
from app.market import PriceCache, create_market_data_source
logging.basicConfig(level=logging.INFO)
async def main():
    cache = PriceCache()
    src = create_market_data_source(cache)
    await src.start(['AAPL', 'MSFT'])
    print(src.mode, {t: u.to_dict() for t, u in cache.get_all().items()})
    await src.stop()
asyncio.run(main())
"
```

A paid key prints `snapshot` and two prices; a free key logs the eod-mode warning and prints `eod` with the last close.

---

## 15. Configuration and operations

### 15.1 Environment variables

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | unset | Unset/blank: simulator. Set: Massive. |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between snapshot polls. Only read when the key is set. Paid plans can use `2`–`5`. **New: add to `.env.example`.** |

### 15.2 Timing at a glance

| Component | Period |
|---|---|
| Simulator tick | 0.5s |
| Massive snapshot poll | 15s (configurable) |
| Massive eod refresh | 15 min |
| SSE cache check | 0.5s, sends only on change |
| SSE heartbeat | after 15s idle |
| `EventSource` reconnect | 1s |

### 15.3 What the user sees by plan

| Setup | Experience |
|---|---|
| No key | Lively simulated prices, 2 updates/s, occasional 2–5% events. **Recommended.** |
| Massive free | Static end-of-day prices; watchlist daily change = that day's open→close. Trading works. |
| Massive Starter / Developer | Prices move every poll, 15 minutes delayed. |
| Massive Advanced+ | Real-time last trades at the poll interval. |

### 15.4 Logging

| Logger | Level | Message |
|---|---|---|
| `app.market.factory` | INFO | Which source was chosen |
| `app.market.simulator` | INFO / DEBUG | Start/stop, add/remove; random events at DEBUG |
| `app.market.massive_client` | WARNING | Switched to eod mode |
| `app.market.massive_client` | ERROR (once per distinct error) | Poll failures |
| `app.market.stream` | INFO | Client connect / disconnect |

---

## 16. Implementation checklist

In order, each step leaving the suite green:

1. `models.py`: add `normalize_ticker`, `reference_price`, `day_change_percent`, extend `to_dict`.
2. `cache.py`: `reference_price` parameter, `timestamp is None`, version bump on `remove`, locked `version` read.
3. `interface.py`: updated contract docstring (no signature changes).
4. `simulator.py`: normalize + de-duplicate tickers, `start()` guard, `dt` parameter, `_seed_cache` helper, cached `sqrt(dt)`.
5. `massive_client.py`: `parse_snapshot` (`sip_timestamp` ns), eod mode, immediate fetch in `add_ticker`, tuple copy + post-fetch filter, error de-duplication, injectable `client`, `start()` guard.
6. `factory.py`: `MASSIVE_POLL_INTERVAL`.
7. `stream.py`: router inside the factory, always send on version change, heartbeat, `format_snapshot` helper.
8. `__init__.py`: export `normalize_ticker`.
9. Tests: replace `test_massive.py`; add `test_contract.py`, `test_stream.py`, `test_cache_reference.py`.
10. `.env.example` and `backend/CLAUDE.md`: document `MASSIVE_POLL_INTERVAL`, eod mode, and the new `PriceUpdate` fields.
11. Run the manual live check (§14.6) once if a Massive key is available.
