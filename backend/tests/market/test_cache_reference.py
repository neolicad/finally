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
