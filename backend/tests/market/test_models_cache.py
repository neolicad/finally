"""PriceUpdate and PriceCache basics."""

import threading

from app.market.cache import PriceCache
from app.market.models import PriceUpdate


class TestPriceUpdate:
    def test_up(self):
        u = PriceUpdate("AAPL", price=190.42, previous_price=190.00, reference_price=188.0)
        assert u.direction == "up"
        assert u.change == 0.42
        assert u.change_percent == 0.2211
        assert u.day_change_percent == 1.2872

    def test_down(self):
        u = PriceUpdate("AAPL", price=189.0, previous_price=190.0)
        assert u.direction == "down"
        assert u.change == -1.0

    def test_flat(self):
        u = PriceUpdate("AAPL", price=190.0, previous_price=190.0)
        assert u.direction == "flat"
        assert u.change == 0

    def test_zero_previous_price(self):
        assert PriceUpdate("X", price=1.0, previous_price=0).change_percent == 0.0

    def test_no_reference(self):
        assert PriceUpdate("X", price=1.0, previous_price=1.0).day_change_percent == 0.0

    def test_to_dict_keys(self):
        d = PriceUpdate("AAPL", 190.0, 189.0, timestamp=1.0, reference_price=188.0).to_dict()
        assert d == {
            "ticker": "AAPL",
            "price": 190.0,
            "previous_price": 189.0,
            "timestamp": 1.0,
            "change": 1.0,
            "change_percent": 0.5291,
            "direction": "up",
            "reference_price": 188.0,
            "day_change_percent": 1.0638,
        }


class TestPriceCache:
    def test_first_update_is_flat(self):
        u = PriceCache().update("AAPL", 190.0)
        assert u.previous_price == 190.0
        assert u.direction == "flat"

    def test_previous_price_tracked(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        u = cache.update("AAPL", 191.0)
        assert u.previous_price == 190.0
        assert u.direction == "up"

    def test_rounding(self):
        assert PriceCache().update("AAPL", 190.12345).price == 190.12

    def test_get_missing(self):
        cache = PriceCache()
        assert cache.get("NOPE") is None
        assert cache.get_price("NOPE") is None

    def test_get_all_is_copy(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        snap = cache.get_all()
        snap.clear()
        assert "AAPL" in cache
        assert len(cache) == 1

    def test_version_increments(self):
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.0)
        cache.update("AAPL", 191.0)
        assert cache.version == v0 + 2

    def test_thread_safety(self):
        cache = PriceCache()

        def writer(n):
            for i in range(200):
                cache.update(f"T{n}", 100.0 + i)

        threads = [threading.Thread(target=writer, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(cache) == 8
        assert cache.version == 8 * 200
