"""GBMSimulator and SimulatorDataSource tests."""

import asyncio
import math
import random

import numpy as np
import pytest

from app.market.cache import PriceCache
from app.market.seed_prices import DEFAULT_PARAMS, SEED_PRICES
from app.market.simulator import GBMSimulator, SimulatorDataSource

DEFAULTS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"]


@pytest.fixture(autouse=True)
def seeded():
    np.random.seed(1234)
    random.seed(1234)


class TestGBMSimulator:
    def test_initial_prices_are_seeds(self):
        sim = GBMSimulator(DEFAULTS)
        for t in DEFAULTS:
            assert sim.get_price(t) == SEED_PRICES[t]

    def test_step_returns_all_tickers_positive(self):
        sim = GBMSimulator(DEFAULTS)
        out = sim.step()
        assert set(out) == set(DEFAULTS)
        assert all(p > 0 for p in out.values())

    def test_empty_step(self):
        assert GBMSimulator([]).step() == {}

    def test_prices_stay_positive_high_vol(self):
        sim = GBMSimulator(["TSLA"], dt=GBMSimulator.DEFAULT_DT * 1000, event_probability=0.05)
        for _ in range(2000):
            assert sim.step()["TSLA"] > 0

    def test_zero_sigma_follows_drift(self):
        sim = GBMSimulator(["AAPL"], event_probability=0.0)
        sim._params["AAPL"] = {"sigma": 0.0, "mu": 0.10}
        n = 1000
        for _ in range(n):
            sim.step()
        expected = SEED_PRICES["AAPL"] * math.exp(0.10 * sim._dt * n)
        assert sim._prices["AAPL"] == pytest.approx(expected, rel=1e-12)

    def test_log_return_statistics(self):
        dt = GBMSimulator.DEFAULT_DT * 10_000  # amplify so stats are measurable
        sim = GBMSimulator(["AAPL"], dt=dt, event_probability=0.0)
        mu, sigma = sim._params["AAPL"]["mu"], sim._params["AAPL"]["sigma"]
        prev = sim._prices["AAPL"]
        rets = []
        for _ in range(20_000):
            sim.step()
            cur = sim._prices["AAPL"]
            rets.append(math.log(cur / prev))
            prev = cur
        rets = np.array(rets)
        assert rets.mean() == pytest.approx((mu - sigma**2 / 2) * dt, abs=4 * sigma * math.sqrt(dt) / math.sqrt(len(rets)))
        assert rets.var() == pytest.approx(sigma**2 * dt, rel=0.05)

    def _corr(self, a: str, b: str, n: int = 20_000) -> float:
        sim = GBMSimulator([a, b], event_probability=0.0)
        ra, rb = [], []
        pa, pb = sim._prices[a], sim._prices[b]
        for _ in range(n):
            sim.step()
            ra.append(math.log(sim._prices[a] / pa))
            rb.append(math.log(sim._prices[b] / pb))
            pa, pb = sim._prices[a], sim._prices[b]
        return float(np.corrcoef(ra, rb)[0, 1])

    def test_tech_pair_correlation(self):
        assert self._corr("AAPL", "MSFT") == pytest.approx(0.6, abs=0.05)

    def test_tech_finance_correlation(self):
        assert self._corr("AAPL", "JPM") == pytest.approx(0.3, abs=0.05)

    def test_events_every_tick(self):
        sim = GBMSimulator(["AAPL"], event_probability=1.0)
        prev = sim._prices["AAPL"]
        for _ in range(50):
            sim.step()
            move = abs(sim._prices["AAPL"] / prev - 1)
            assert 0.019 < move < 0.051
            prev = sim._prices["AAPL"]

    def test_no_events_when_probability_zero(self):
        sim = GBMSimulator(["AAPL"], event_probability=0.0)
        prev = sim._prices["AAPL"]
        for _ in range(500):
            sim.step()
            assert abs(sim._prices["AAPL"] / prev - 1) < 0.01
            prev = sim._prices["AAPL"]

    def test_add_ticker_rebuilds_cholesky(self):
        sim = GBMSimulator(["AAPL"])
        assert sim._cholesky is None
        sim.add_ticker("MSFT")
        assert sim._cholesky.shape == (2, 2)
        sim.add_ticker("JPM")
        assert sim._cholesky.shape == (3, 3)

    def test_add_ticker_idempotent(self):
        sim = GBMSimulator(["AAPL"])
        sim.add_ticker("AAPL")
        assert sim.get_tickers() == ["AAPL"]

    def test_remove_ticker(self):
        sim = GBMSimulator(["AAPL", "MSFT", "JPM"])
        sim.remove_ticker("MSFT")
        assert sim.get_tickers() == ["AAPL", "JPM"]
        assert sim._cholesky.shape == (2, 2)
        assert sim.get_price("MSFT") is None
        sim.remove_ticker("MSFT")  # no-op
        sim.remove_ticker("AAPL")
        assert sim._cholesky is None

    def test_unknown_ticker_uses_defaults(self):
        sim = GBMSimulator(["ZZZ"])
        assert 50 <= sim.get_price("ZZZ") <= 300
        assert sim._params["ZZZ"] == DEFAULT_PARAMS

    def test_cholesky_succeeds_with_many_unknown_tickers(self):
        sim = GBMSimulator([f"A{chr(65 + i)}" for i in range(26)] + DEFAULTS)
        assert sim._cholesky is not None
        assert len(sim.step()) == 36

    def test_pairwise_correlation_table(self):
        c = GBMSimulator._pairwise_correlation
        assert c("AAPL", "MSFT") == 0.6
        assert c("JPM", "V") == 0.5
        assert c("TSLA", "AAPL") == 0.3
        assert c("AAPL", "JPM") == 0.3
        assert c("ZZZ", "YYY") == 0.3


class TestSimulatorDataSource:
    async def test_start_seeds_cache(self):
        cache = PriceCache()
        src = SimulatorDataSource(cache, update_interval=0.01)
        await src.start(DEFAULTS)
        try:
            assert len(cache) == 10
            assert cache.get_price("AAPL") == SEED_PRICES["AAPL"]
        finally:
            await src.stop()

    async def test_version_increases_over_time(self):
        cache = PriceCache()
        src = SimulatorDataSource(cache, update_interval=0.01)
        await src.start(["AAPL"])
        try:
            v = cache.version
            await asyncio.sleep(0.1)
            assert cache.version > v
        finally:
            await src.stop()

    async def test_add_and_remove(self):
        cache = PriceCache()
        src = SimulatorDataSource(cache, update_interval=0.01)
        await src.start(["AAPL"])
        try:
            await src.add_ticker("pypl")
            assert "PYPL" in cache
            assert src.get_tickers() == ["AAPL", "PYPL"]
            await src.remove_ticker("PYPL")
            assert "PYPL" not in cache
            assert src.get_tickers() == ["AAPL"]
        finally:
            await src.stop()

    async def test_start_normalizes_and_dedupes(self):
        cache = PriceCache()
        src = SimulatorDataSource(cache, update_interval=0.01)
        await src.start(["aapl", "AAPL", " msft "])
        try:
            assert src.get_tickers() == ["AAPL", "MSFT"]
        finally:
            await src.stop()

    async def test_double_start_raises(self):
        src = SimulatorDataSource(PriceCache(), update_interval=0.01)
        await src.start(["AAPL"])
        try:
            with pytest.raises(RuntimeError):
                await src.start(["AAPL"])
        finally:
            await src.stop()

    async def test_stop_idempotent(self):
        src = SimulatorDataSource(PriceCache(), update_interval=0.01)
        await src.start(["AAPL"])
        await src.stop()
        await src.stop()

    async def test_stop_before_start(self):
        await SimulatorDataSource(PriceCache()).stop()

    async def test_loop_survives_step_exception(self):
        cache = PriceCache()
        src = SimulatorDataSource(cache, update_interval=0.01)
        await src.start(["AAPL"])
        try:
            calls = {"n": 0}
            real_step = src._sim.step

            def flaky():
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RuntimeError("boom")
                return real_step()

            src._sim.step = flaky
            await asyncio.sleep(0.1)
            assert calls["n"] > 1
        finally:
            await src.stop()

    async def test_operations_before_start_are_safe(self):
        src = SimulatorDataSource(PriceCache())
        await src.add_ticker("AAPL")
        assert src.get_tickers() == []
