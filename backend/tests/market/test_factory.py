"""Factory selection tests."""

import pytest

from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("MASSIVE_POLL_INTERVAL", raising=False)


@pytest.mark.parametrize("value", [None, "", "   "])
def test_simulator_when_key_missing_or_blank(monkeypatch, value):
    if value is not None:
        monkeypatch.setenv("MASSIVE_API_KEY", value)
    assert isinstance(create_market_data_source(PriceCache()), SimulatorDataSource)


def test_massive_when_key_set(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "abc")
    src = create_market_data_source(PriceCache())
    assert isinstance(src, MassiveDataSource)
    assert src._interval == 15.0


@pytest.mark.parametrize("raw,expected", [("5", 5.0), ("abc", 15.0), ("0.2", 1.0), ("", 15.0)])
def test_poll_interval(monkeypatch, raw, expected):
    monkeypatch.setenv("MASSIVE_API_KEY", "abc")
    monkeypatch.setenv("MASSIVE_POLL_INTERVAL", raw)
    assert create_market_data_source(PriceCache())._interval == expected


def test_returns_unstarted_source(monkeypatch):
    src = create_market_data_source(PriceCache())
    assert src.get_tickers() == []
