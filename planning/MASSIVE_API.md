# Massive API Reference (formerly Polygon.io)

Reference for the parts of the Massive REST API that FinAlly uses: current prices and end-of-day prices for many tickers. Polygon.io rebranded as Massive in 2025. The API paths and response shapes did not change. The base URL is now `https://api.massive.com`, and the Python package is `massive`, which replaces `polygon-api-client`.

Researched September 2026 against massive.com/docs and `github.com/massive-com/client-python`.

## 1. Basics

| Item | Value |
|---|---|
| Base URL | `https://api.massive.com` |
| Auth | `Authorization: Bearer <KEY>` header (the Python client does this), or `?apiKey=<KEY>` query param |
| Python package | `massive` (`uv add massive`) |
| Env var the client reads | `MASSIVE_API_KEY` (used when `RESTClient()` gets no key) |
| Client defaults | `connect_timeout=10.0`, `read_timeout=10.0`, `retries=3`, `pagination=True` |

### Plans and what they mean for us

| Plan | Rate limit | Data recency | Snapshot endpoints |
|---|---|---|---|
| Basic (free) | 5 requests/min | End of day | **Not included** |
| Starter / Developer | Unlimited | 15-minute delayed | Included |
| Advanced / Business | Unlimited | Real-time | Included |

Two consequences for FinAlly:

1. The **snapshot** endpoints, which give one call for many tickers with the latest price, need a paid plan. A free key gets an authorization error (HTTP 403 / `NOT_AUTHORIZED`) from them.
2. On the free plan the useful endpoints are **Previous Day Bar** and **Daily Market Summary (grouped daily)**. Both return end-of-day data, and 5 calls/min allows at most one poll every 12s. We use 15s.

Snapshot data is cleared at about 12am–3:30am ET and fills again as exchanges report, starting around 4am ET. Before the market opens, `day` can be empty while `prevDay` and `lastTrade` still hold data.

## 2. Endpoints

### 2.1 Full Market Snapshot: many tickers, latest price (main endpoint)

```
GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,TSLA
```

| Param | Notes |
|---|---|
| `tickers` | Comma-separated and case-sensitive. If empty, returns **all** ~10k tickers. |
| `include_otc` | Default `false` |

It costs one request however many tickers you ask for, so it is the right endpoint for polling a watchlist.

Response (one element of `tickers[]`; the single-ticker endpoint below returns the same object under `ticker`):

```json
{
  "ticker": "AAPL",
  "todaysChange": 0.98,
  "todaysChangePerc": 0.82,
  "updated": 1605195918306274000,
  "day":     {"o": 119.62, "h": 120.53, "l": 118.81, "c": 120.4229, "v": 28727868, "vw": 119.725},
  "prevDay": {"o": 117.19, "h": 119.63, "l": 116.44, "c": 119.49,   "v": 110597265, "vw": 118.4998},
  "min":     {"o": 120.435, "h": 120.468, "l": 120.37, "c": 120.4201, "v": 270796, "vw": 120.4129,
              "av": 28724441, "n": 762, "t": 1684428720000},
  "lastTrade": {"p": 120.47, "s": 236, "x": 10, "c": [14, 41], "i": "4046", "t": 1605195918306274000},
  "lastQuote": {"p": 120.46, "s": 8, "P": 120.47, "S": 4, "t": 1605195918507251700}
}
```

Field meanings:

- `lastTrade.p`: last trade price. **This is the "current price" we use.**
- `lastTrade.t`: SIP timestamp in **nanoseconds** since the Unix epoch
- `lastQuote.p` / `lastQuote.P`: bid and ask; `s` / `S` are the bid and ask sizes
- `day`: today's OHLCV so far; `prevDay`: yesterday's full-day OHLCV
- `min`: the most recent minute bar; its `t` is in **milliseconds**
- `todaysChange` / `todaysChangePerc`: change against `prevDay.c`
- `updated`: last update time in nanoseconds

### 2.2 Single Ticker Snapshot

```
GET /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}
```

Returns the same object as above under the key `ticker`. Useful for checking a symbol is valid before adding it to the watchlist.

### 2.3 Unified Snapshot (v3, multi-asset)

```
GET /v3/snapshot?ticker.any_of=AAPL,MSFT&limit=250
```

Takes at most 250 tickers per call and is paginated with `next_url`. It covers stocks, options, FX, crypto and indices, with fields `last_trade`, `last_quote`, `session` (open/high/low/close/volume/change), and `market_status`. It needs the same plans as 2.1. We do not need it because 2.1 is enough for stocks.

### 2.4 Daily Market Summary / Grouped Daily: end of day for all tickers

```
GET /v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true
```

Returns one bar for **every** US stock on `date` (`YYYY-MM-DD`) in a single call, on **all plans including free**. Filter it to the watchlist on the client side.

```json
{
  "status": "OK", "adjusted": true, "resultsCount": 10938,
  "results": [
    {"T": "AAPL", "o": 115.55, "h": 117.59, "l": 114.13, "c": 115.97,
     "v": 131704427, "vw": 116.3058, "t": 1605042000000, "n": 802310}
  ]
}
```

`t` is in milliseconds (the start of the day window). Days with no trading, such as weekends and holidays, return `resultsCount: 0`.

### 2.5 Previous Day Bar

```
GET /v2/aggs/ticker/{ticker}/prev?adjusted=true
```

One ticker's previous close (`results[0].c`). Available on all plans. Each ticker costs one request, so on the free plan it only suits small watchlists.

### 2.6 Daily Ticker Summary (open/close for a date)

```
GET /v1/open-close/{ticker}/{date}?adjusted=true
```

Returns `open`, `high`, `low`, `close`, `volume`, `preMarket`, `afterHours`, and `symbol` for one ticker on one date.

### 2.7 Other endpoints we do not use

- `GET /v2/last/trade/{ticker}` and `GET /v2/last/nbbo/{ticker}`: last trade or quote for **one** ticker per call.
- `GET /v2/aggs/ticker/{ticker}/range/{mult}/{timespan}/{from}/{to}`: historical bars. This could seed a chart history later.
- WebSocket streaming (`wss://socket.massive.com/stocks`): push-based, but PLAN.md picks REST polling for simplicity.

## 3. Python client (`massive`)

### 3.1 Setup

```python
from massive import RESTClient

client = RESTClient()                    # reads MASSIVE_API_KEY from the environment
client = RESTClient(api_key="...")       # or pass it explicitly
```

The client is **synchronous** (it uses urllib3). From asyncio code, call it through `asyncio.to_thread(...)`.

### 3.2 Latest prices for many tickers (paid plans)

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "MSFT", "TSLA"],    # the client joins a list with commas
)
for snap in snapshots:
    trade = snap.last_trade
    price = trade.price
    ts_seconds = trade.sip_timestamp / 1e9   # nanoseconds -> seconds
    print(snap.ticker, price, ts_seconds, snap.todays_change_percent)
```

Model attribute names (the Python client maps the short JSON keys to these):

| Model | JSON key -> attribute |
|---|---|
| `TickerSnapshot` | `ticker`, `day`, `prevDay`->`prev_day`, `min`, `lastTrade`->`last_trade`, `lastQuote`->`last_quote`, `todaysChange`->`todays_change`, `todaysChangePerc`->`todays_change_percent`, `updated` |
| `LastTrade` | `p`->`price`, `s`->`size`, `x`->`exchange`, `t`->`sip_timestamp`, `y`->`participant_timestamp`, `c`->`conditions` |
| `Agg` (`day` / `prev_day`) | `o`/`h`/`l`/`c`/`v`/`vw` -> `open`/`high`/`low`/`close`/`volume`/`vwap`, `t`->`timestamp` |

> **Gotcha:** `LastTrade` has **no `timestamp` attribute**. It is `sip_timestamp`, and it is in **nanoseconds**. Any attribute can be optional (`None`), for example `last_trade` for a ticker that has not traded yet.

### 3.3 One ticker

```python
snap = client.get_snapshot_ticker(SnapshotMarketType.STOCKS, "AAPL")
print(snap.last_trade.price, snap.prev_day.close)
```

### 3.4 End-of-day prices for many tickers (all plans, free included)

```python
from datetime import date, timedelta

def latest_closes(client: RESTClient, tickers: set[str], lookback_days: int = 5) -> dict[str, float]:
    """Return {ticker: close} from the most recent trading day with data."""
    day = date.today()
    for _ in range(lookback_days):
        day -= timedelta(days=1)
        bars = client.get_grouped_daily_aggs(day.isoformat(), adjusted=True)
        if bars:
            return {b.ticker: b.close for b in bars if b.ticker in tickers}
    return {}
```

`get_grouped_daily_aggs` returns `list[GroupedDailyAgg]` with attributes `ticker, open, high, low, close, volume, vwap, timestamp, transactions`.

### 3.5 Previous close for one ticker

```python
prev = client.get_previous_close_agg("AAPL")   # list[PreviousCloseAgg]
print(prev[0].close)
```

### 3.6 Open/close on a date

```python
oc = client.get_daily_open_close_agg("AAPL", "2026-09-18", adjusted=True)
print(oc.open, oc.close, oc.after_hours)
```

### 3.7 Errors

The client raises `massive.exceptions.BadResponse` for non-2xx responses and retries transient failures by itself (`retries=3`). Errors you will see:

| HTTP | Cause | What to do |
|---|---|---|
| 401 | Bad or missing key | Log it; the key is misconfigured |
| 403 `NOT_AUTHORIZED` | Your plan does not include the endpoint (for example, snapshots on the free plan) | Log it; use a free endpoint instead |
| 429 | Over the rate limit (free: 5/min) | Wait for the next poll interval |

## 4. Raw HTTP example (no client)

```bash
curl -H "Authorization: Bearer $MASSIVE_API_KEY" \
  "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT"
```

## 5. What FinAlly uses

- **Paid key:** poll `get_snapshot_all(STOCKS, tickers=watchlist)` every 2–15s and use `last_trade.price` as the price.
- **Free key:** snapshots are not available. The simplest working option is `get_grouped_daily_aggs` (one call, EOD closes), which is enough to value the portfolio but produces a static price.
- The design is in `MARKET_INTERFACE.md`.

## Sources

- https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot
- https://massive.com/docs/rest/stocks/snapshots/single-ticker-snapshot
- https://massive.com/docs/rest/stocks/snapshots/unified-snapshot
- https://massive.com/docs/rest/stocks/aggregates/daily-market-summary
- https://massive.com/docs/rest/stocks/aggregates/previous-day-bar
- https://massive.com/docs/rest/stocks/aggregates/daily-ticker-summary
- https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis
- https://github.com/massive-com/client-python (`massive/rest/snapshot.py`, `models/snapshot.py`, `models/trades.py`, `models/aggs.py`)
