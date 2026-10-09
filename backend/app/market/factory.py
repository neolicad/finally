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
