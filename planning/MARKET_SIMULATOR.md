# Market Simulator

This is the default price source when `MASSIVE_API_KEY` is not set. It produces realistic, correlated, live-looking stock prices with no network access. It implements `MarketDataSource` (see `MARKET_INTERFACE.md`).

Code: `backend/app/market/simulator.py` and `backend/app/market/seed_prices.py`.

## 1. Goals

- Prices look real: they move continuously, never go negative, and move by a realistic amount for each stock.
- Stocks move together the way real sectors do. Tech tends to rise and fall as a group.
- Something interesting happens now and then, such as a sudden 2–5% jump, so the demo has visible action.
- It is cheap: one tick for about 10–50 tickers takes microseconds.
- It is fully in-process. There are no dependencies apart from numpy.

## 2. Model: Geometric Brownian Motion

Each tick moves every price by:

```
S(t+dt) = S(t) * exp( (mu - sigma²/2)·dt + sigma·√dt·Z )
```

| Symbol | Meaning | Source |
|---|---|---|
| `mu` | Annual drift (expected return) | `TICKER_PARAMS` |
| `sigma` | Annual volatility | `TICKER_PARAMS` |
| `dt` | Tick length as a fraction of a trading year | `0.5 / (252 · 6.5 · 3600)` ≈ `8.48e-8` |
| `Z` | Correlated standard normal draw | Cholesky (see §3) |

Why GBM: it is the standard textbook model. Multiplying by `exp(...)` means prices can never go below zero, and moves scale with the price, so a $800 stock moves more dollars than a $190 one.

How big the moves are: for AAPL (`sigma=0.22`), one tick has a standard deviation of `0.22·√8.48e-8 ≈ 0.0064%`, about $0.012. Over an hour (7,200 ticks) that grows to about 0.5%, which looks like a real, fairly calm stock.

Tuning: to make the demo livelier, multiply `dt` (for example `DEFAULT_DT * 10`) instead of changing each ticker's `sigma`. That keeps the relative volatility between tickers the same.

## 3. Correlated moves

Every tick draws `n` independent normals and correlates them with the Cholesky factor `L` of the correlation matrix `C`:

```python
z = np.random.standard_normal(n)
z_corr = L @ z            # L = np.linalg.cholesky(C)
```

How pairs are correlated (`_pairwise_correlation`):

| Pair | rho |
|---|---|
| Both tech (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| Both finance (JPM, V) | 0.5 |
| Either one is TSLA | 0.3 (it moves on its own story) |
| Any other pair, including unknown tickers | 0.3 |

With one constant correlation inside each group and a smaller one everywhere else, `C` stays positive definite, so the Cholesky step always succeeds. `L` is rebuilt (O(n²) plus the Cholesky cost, trivial for n < 50) only when tickers are added or removed. It is never rebuilt on a tick.

## 4. Random events

After the GBM step, each ticker gets a shock with probability `event_probability` (default `0.001`):

```python
if random.random() < event_prob:
    price *= 1 + random.choice([-1, 1]) * random.uniform(0.02, 0.05)
```

With 10 tickers at 2 ticks/s, there is about one event every 50 seconds across the watchlist. That is enough to catch the eye without looking artificial.

## 5. Seed data (`seed_prices.py`)

| Ticker | Seed $ | sigma | mu | Note |
|---|---|---|---|---|
| AAPL | 190 | 0.22 | 0.05 | |
| GOOGL | 175 | 0.25 | 0.05 | |
| MSFT | 420 | 0.20 | 0.05 | |
| AMZN | 185 | 0.28 | 0.05 | |
| TSLA | 250 | 0.50 | 0.03 | high vol |
| NVDA | 800 | 0.40 | 0.08 | high vol, strong drift |
| META | 500 | 0.30 | 0.05 | |
| JPM | 195 | 0.18 | 0.04 | bank, low vol |
| V | 280 | 0.17 | 0.04 | payments, low vol |
| NFLX | 600 | 0.35 | 0.05 | |

A ticker that is not in the table, for example one added through chat, starts at a random price between $50 and $300 with `DEFAULT_PARAMS = {sigma: 0.25, mu: 0.05}`. It also gets the cross-group correlation of 0.3 with everything else.

Prices start from the seed values on every process start. The simulator does not persist state, and it does not need to.

## 6. Code structure

There are two classes, and each has one job:

```
simulator.py
├── GBMSimulator            pure math, synchronous, no I/O, no asyncio
│   ├── __init__(tickers, dt, event_probability)
│   ├── step() -> dict[str, float]      advance every ticker by one tick
│   ├── add_ticker(t) / remove_ticker(t) rebuild Cholesky
│   ├── get_price(t) / get_tickers()
│   ├── _add_ticker_internal(t)         seed price and params, no rebuild
│   ├── _rebuild_cholesky()
│   └── _pairwise_correlation(t1, t2)   static
│
└── SimulatorDataSource(MarketDataSource)   adapter: asyncio + PriceCache
    ├── start(tickers)   build GBMSimulator, seed the cache, create_task(_run_loop)
    ├── stop()           cancel the task
    ├── add_ticker(t)    sim.add_ticker, then write the seed price to the cache right away
    ├── remove_ticker(t) sim.remove_ticker and cache.remove
    └── _run_loop()      forever: step() -> cache.update(...) -> sleep(0.5)

seed_prices.py          constants only: SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS,
                        CORRELATION_GROUPS, *_CORR
```

Why the split: `GBMSimulator` can be tested with no event loop or cache. Seed the RNGs, call `step()`, and check the numbers. `SimulatorDataSource` holds only lifecycle and wiring.

### Core loop

```python
async def _run_loop(self) -> None:
    while True:
        try:
            for ticker, price in self._sim.step().items():
                self._cache.update(ticker=ticker, price=price)
        except Exception:
            logger.exception("Simulator step failed")
        await asyncio.sleep(self._interval)
```

A bad tick is logged and the loop keeps going, so the price stream never stops quietly.

### Hot path: `step()`

```python
def step(self) -> dict[str, float]:
    z = np.random.standard_normal(len(self._tickers))
    if self._cholesky is not None:
        z = self._cholesky @ z
    out = {}
    for i, t in enumerate(self._tickers):
        mu, sigma = self._params[t]["mu"], self._params[t]["sigma"]
        self._prices[t] *= math.exp((mu - 0.5 * sigma**2) * self._dt
                                    + sigma * math.sqrt(self._dt) * z[i])
        if random.random() < self._event_prob:
            self._prices[t] *= 1 + random.choice([-1, 1]) * random.uniform(0.02, 0.05)
        out[t] = round(self._prices[t], 2)
    return out
```

The full-precision price is kept inside the simulator. Only the value written out is rounded, so rounding errors never build up.

## 7. Testing (`tests/market/test_simulator*.py`)

- Prices stay positive after many steps, including at high sigma.
- With `sigma = 0` and no events, the price follows `S·exp(mu·dt)` exactly (checks the drift term).
- Statistics: over many steps with a seeded RNG, the mean and variance of the log returns match `(mu - sigma²/2)·dt` and `sigma²·dt`.
- Correlation: the sample correlation of the log returns of two tech tickers is about 0.6, and a tech/finance pair is about 0.3.
- Events: `event_probability=1.0` gives a move of 2–5% on every tick, and `0.0` gives none.
- Add or remove a ticker: the Cholesky matrix is rebuilt at the right size, and an unknown ticker gets `DEFAULT_PARAMS`.
- `SimulatorDataSource`: `start` seeds the cache, the cache `version` goes up over time, `remove_ticker` clears the cache entry, and `stop` is idempotent.
