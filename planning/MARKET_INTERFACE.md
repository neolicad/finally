# Market Data Interface

This is the one Python API that FinAlly uses for stock prices. Every price comes from a `MarketDataSource`. When `MASSIVE_API_KEY` is set, that source is the Massive REST API. Otherwise it is the built-in simulator. Nothing downstream (SSE, portfolio valuation, trade execution, the LLM context) can tell which one is running.

Code: `backend/app/market/`. Massive API details are in `MASSIVE_API.md`, and the simulator is in `MARKET_SIMULATOR.md`.

## 1. Design

```
             create_market_data_source(cache)
                 │  MASSIVE_API_KEY set?
         ┌───────┴────────┐
        yes               no
         │                 │
 MassiveDataSource   SimulatorDataSource
  (REST poll 15s)     (GBM step 500ms)
         │                 │
         └──── writes ─────┘
                 ▼
            PriceCache  (in-memory, thread-safe, versioned)
                 │ reads
     ┌───────────┼──────────────┬──────────────┐
  SSE stream   /api/portfolio  trade exec   chat context
```

Rules:

1. **Push, not pull.** A source writes into `PriceCache` on its own schedule. Consumers never ask the source for a price. They read the cache. This keeps request handlers fast, and slow API calls never block a trade.
2. **One writer, many readers.** Exactly one source runs per process. It is created at app startup and stopped at shutdown.
3. **The source owns the ticker set.** The app tells the source which tickers to track: watchlist tickers plus tickers with an open position (PLAN.md §6).

## 2. Public API

```python
from app.market import (
    PriceUpdate, PriceCache, MarketDataSource,
    create_market_data_source, create_stream_router,
)
```

### 2.1 `PriceUpdate` (`models.py`)

This is the immutable price snapshot type that every source produces.

```python
@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float          # Unix seconds

    change: float             # property: price - previous_price
    change_percent: float     # property
    direction: str            # property: "up" | "down" | "flat"
    def to_dict(self) -> dict # JSON / SSE payload
```

### 2.2 `PriceCache` (`cache.py`)

```python
class PriceCache:
    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate
    def get(self, ticker: str) -> PriceUpdate | None
    def get_price(self, ticker: str) -> float | None
    def get_all(self) -> dict[str, PriceUpdate]
    def remove(self, ticker: str) -> None
    @property
    def version(self) -> int      # bumps on every update; SSE uses it to skip unchanged ticks
```

- `update()` works out `previous_price` from the value already in the cache. The first update for a ticker has `previous_price == price` (direction `flat`).
- Prices are rounded to 2 decimal places.
- A `threading.Lock` guards the cache, so a source may write to it from a worker thread.

### 2.3 `MarketDataSource` (`interface.py`)

```python
class MarketDataSource(ABC):
    async def start(self, tickers: list[str]) -> None     # seed cache, launch background task
    async def stop(self) -> None                          # cancel task; idempotent
    async def add_ticker(self, ticker: str) -> None       # no-op if present
    async def remove_ticker(self, ticker: str) -> None    # also removes from cache
    def get_tickers(self) -> list[str]
```

What every implementation must do:

- `start()` puts initial data in the cache **before** it returns, so the first SSE event is not empty.
- A failed update cycle is logged, and the loop keeps running. The background task never dies because of a bad tick or an HTTP error.
- Ticker symbols are uppercased.

### 2.4 `create_market_data_source(cache)` (`factory.py`)

```python
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    return SimulatorDataSource(price_cache=price_cache)
```

It returns an **unstarted** source.

### 2.5 `create_stream_router(cache)` (`stream.py`)

This returns a FastAPI `APIRouter` with `GET /api/stream/prices`. Every 500ms it sends one SSE `data:` event holding `{ticker: PriceUpdate.to_dict()}` for all cached tickers, but only when `cache.version` has changed. It starts with `retry: 1000` so `EventSource` reconnects after 1s.

## 3. Implementations

### 3.1 `SimulatorDataSource` (default)

This wraps `GBMSimulator`. An asyncio task calls `step()` every 0.5s and writes each price to the cache. `add_ticker` seeds the cache straight away, so a new ticker shows a price before the next tick. See `MARKET_SIMULATOR.md`.

### 3.2 `MassiveDataSource` (`MASSIVE_API_KEY` set)

- Makes one `get_snapshot_all(STOCKS, tickers=[...])` call per poll, which covers every tracked ticker.
- `poll_interval` defaults to 15s, which is safe for the free plan's 5 req/min. Paid plans can go down to 2–5s.
- The client is synchronous, so each call runs through `asyncio.to_thread`.
- `start()` polls once right away, then loops.
- `add_ticker` does not fetch anything. The new ticker gets its price on the next poll (up to 15s later).
- It writes `last_trade.price` to the cache, with the timestamp converted to seconds.

#### Required fix in `massive_client.py`

Research for `MASSIVE_API.md` turned up a bug in the current `_poll_once`:

```python
timestamp = snap.last_trade.timestamp / 1000.0     # current code: wrong
```

The real `massive` `LastTrade` model has **no `timestamp` attribute**. The field is `sip_timestamp`, and it is in **nanoseconds**. Against the live API, every snapshot raises `AttributeError`, gets logged as "Skipping snapshot", and the cache is never filled. The unit tests do not catch this because `MagicMock` invents any attribute you ask for. The fix:

```python
trade = snap.last_trade
self._cache.update(ticker=snap.ticker, price=trade.price, timestamp=trade.sip_timestamp / 1e9)
```

The tests should also build real `TickerSnapshot.from_dict({...})` objects (use the sample JSON in `MASSIVE_API.md` §2.1) instead of `MagicMock`, so the tests fail if an attribute name is wrong.

#### Free-plan limitation

Snapshot endpoints need a paid Massive plan. With a free key, each poll logs a 403 and prices never show up. For this project that is acceptable and should be stated in the README: *a free key gives no data, so use the simulator or a Starter+ plan*. If free-plan support is needed later, add an EOD mode that calls `get_grouped_daily_aggs` (one call, all tickers) and writes each ticker's close once. The interface does not change.

## 4. How the app uses it

### 4.1 Lifecycle (FastAPI lifespan)

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.market import PriceCache, create_market_data_source, create_stream_router

price_cache = PriceCache()
market = create_market_data_source(price_cache)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await market.start(tracked_tickers())       # watchlist ∪ open positions, from the DB
    yield
    await market.stop()

app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
```

`tracked_tickers()` is a small DB helper owned by the backend. It returns the sorted union of `watchlist.ticker` and `positions.ticker`.

### 4.2 Keeping the ticker set in sync

| Event | Call |
|---|---|
| Watchlist add | `await market.add_ticker(t)` |
| Watchlist remove | `await market.remove_ticker(t)` **only if there is no open position in `t`** |
| Buy of a ticker not being tracked | `await market.add_ticker(t)` (PLAN.md §9 also auto-adds it to the watchlist) |
| Sell that closes a position | `await market.remove_ticker(t)` **only if `t` is not on the watchlist** |

Put this rule in one helper so routes and the chat flow do not each reimplement it:

```python
async def sync_ticker(t: str) -> None:
    """Track t if it is watched or held, stop tracking it otherwise."""
    if is_watched(t) or is_held(t):
        await market.add_ticker(t)
    else:
        await market.remove_ticker(t)
```

### 4.3 Reading prices

```python
price = price_cache.get_price("AAPL")          # trade execution / valuation
if price is None:
    raise HTTPException(400, "No price available for AAPL yet")
```

A ticker whose price is not in the cache yet cannot be traded. With the simulator this cannot happen after `add_ticker`. With Massive it can, for up to one poll interval.

## 5. Testing

- **Contract tests** run against both implementations: `start` fills the cache, `add_ticker` / `remove_ticker` update `get_tickers()` and the cache, and `stop` is idempotent.
- **Simulator tests** are deterministic when you seed `np.random` and `random`.
- **Massive tests** patch `_fetch_snapshots` to return real `TickerSnapshot` models, and cover a 403 or 429 exception, which must be logged without crashing.
- **Factory test**: `MASSIVE_API_KEY` unset, empty, or whitespace gives the simulator. Any other value gives Massive.
