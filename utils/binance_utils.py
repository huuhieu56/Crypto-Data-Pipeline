# =============================================================================
# Binance API Utilities - Crypto Data Pipeline
# =============================================================================
# Cac function dung chung de goi Binance API voi retry va rate-limiting.
# Tat ca module can goi API phai dung cac ham o day.
# =============================================================================

from __future__ import annotations

import time

import requests

from config.config import (
    BINANCE_ENDPOINTS,
    API_TIMEOUT,
    API_SLEEP,
)
from utils.logger import get_logger
from utils.exceptions import APIRequestError

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Core request helper
# ---------------------------------------------------------------------------

_DEFAULT_MAX_RETRIES = 3
_RETRY_BACKOFF = 2  # seconds, doubles after each retry


def make_request(
    url: str,
    params: dict | None = None,
    timeout: int | None = None,
    max_retries: int = _DEFAULT_MAX_RETRIES,
) -> dict | list:
    """Goi GET request voi retry va exponential backoff.

    Args:
        url: URL endpoint.
        params: Query parameters.
        timeout: Timeout tinh bang giay (mac dinh tu config).
        max_retries: So lan thu lai khi gap loi.

    Returns:
        Parsed JSON response.

    Raises:
        APIRequestError: Sau khi het so lan retry.
    """
    timeout = timeout or API_TIMEOUT
    last_exc: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            logger.warning(
                "HTTP %s from %s (attempt %d/%d)",
                status, url, attempt, max_retries,
            )
            # 429 Too Many Requests — wait longer
            if status == 429:
                wait = _RETRY_BACKOFF * attempt * 2
                logger.warning("Rate limited — waiting %ds", wait)
                time.sleep(wait)
            last_exc = exc
        except requests.ConnectionError as exc:
            logger.warning(
                "Connection error for %s (attempt %d/%d): %s",
                url, attempt, max_retries, exc,
            )
            last_exc = exc
        except requests.Timeout as exc:
            logger.warning(
                "Timeout for %s (attempt %d/%d)",
                url, attempt, max_retries,
            )
            last_exc = exc
        except Exception as exc:
            last_exc = exc
            logger.error("Unexpected error calling %s: %s", url, exc)
            break

        if attempt < max_retries:
            wait = _RETRY_BACKOFF * attempt
            time.sleep(wait)

    raise APIRequestError(
        endpoint=url,
        detail=str(last_exc),
    )


def make_request_raw(
    url: str,
    params: dict | None = None,
    timeout: int | None = None,
    max_retries: int = _DEFAULT_MAX_RETRIES,
) -> bytes:
    """Like make_request but returns raw bytes (for zip downloads)."""
    timeout = timeout or API_TIMEOUT
    last_exc: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.content
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            logger.warning(
                "Download error %s (attempt %d/%d): %s",
                url, attempt, max_retries, exc,
            )
            last_exc = exc
            # 404 Not Found — resource does not exist, retrying is pointless
            if status == 404:
                break
            if attempt < max_retries:
                time.sleep(_RETRY_BACKOFF * attempt)
        except Exception as exc:
            logger.warning(
                "Download error %s (attempt %d/%d): %s",
                url, attempt, max_retries, exc,
            )
            last_exc = exc
            if attempt < max_retries:
                time.sleep(_RETRY_BACKOFF * attempt)

    raise APIRequestError(endpoint=url, detail=str(last_exc))


# ---------------------------------------------------------------------------
# Endpoint wrappers
# ---------------------------------------------------------------------------

def get_klines(symbol: str, interval: str = "1m", **kwargs) -> list:
    """Fetch candlestick data from /api/v3/klines."""
    params = {"symbol": symbol, "interval": interval, **kwargs}
    return make_request(BINANCE_ENDPOINTS["klines"], params=params)


def get_ticker_24h() -> list[dict]:
    """Fetch 24h ticker stats for all symbols."""
    return make_request(BINANCE_ENDPOINTS["ticker_24h"])


def get_book_ticker() -> list[dict]:
    """Fetch best bid/ask for all symbols."""
    return make_request(BINANCE_ENDPOINTS["book_ticker"])


def get_order_book(symbol: str, limit: int = 100) -> dict:
    """Fetch order book depth for a symbol."""
    params = {"symbol": symbol, "limit": limit}
    return make_request(BINANCE_ENDPOINTS["order_book"], params=params)


def sleep_between_requests() -> None:
    """Sleep between requests to avoid rate limit."""
    time.sleep(API_SLEEP)


# ---------------------------------------------------------------------------
# Klines constants & parsing
# ---------------------------------------------------------------------------

# Binance klines: 12 columns, drop "ignore"
KLINES_RAW_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]
KLINES_COLUMNS = [c for c in KLINES_RAW_COLUMNS if c != "ignore"]
NUMERIC_COLUMNS = [
    "open", "high", "low", "close", "volume",
    "quote_volume", "taker_buy_base", "taker_buy_quote",
]


def parse_klines_df(raw_data: list[list], symbol: str):
    """Parse raw klines list into a clean DataFrame."""
    import pandas as pd

    df = pd.DataFrame(raw_data, columns=KLINES_RAW_COLUMNS)
    df = df.drop(columns=["ignore"])

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    df["symbol"] = symbol
    return df


# ---------------------------------------------------------------------------
# Data Vision (bulk download 1 month)
# ---------------------------------------------------------------------------

def download_klines_month(symbol: str, year: int, month: int):
    """Download 1-month klines ZIP from Data Vision. Returns DataFrame or None."""
    import io
    import zipfile
    import pandas as pd
    from config.config import BINANCE_DATA_VISION_URL

    url = BINANCE_DATA_VISION_URL.format(symbol=symbol, year=year, month=month)

    try:
        content = make_request_raw(url, timeout=60)
    except Exception as exc:
        logger.warning(
            "Data Vision download failed for %s %d-%02d: %s",
            symbol, year, month, exc,
        )
        return None

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        with z.open(z.namelist()[0]) as f:
            df = pd.read_csv(
                f, header=None, usecols=range(11), names=KLINES_COLUMNS,
            )

    raw_ts = df["open_time"].astype("int64")
    divisor = 1000 if raw_ts.iloc[0] > 1e15 else 1

    df["open_time"] = pd.to_datetime(raw_ts // divisor, unit="ms")
    df["close_time"] = pd.to_datetime(
        df["close_time"].astype("int64") // divisor, unit="ms",
    )
    df["symbol"] = symbol
    return df


# ---------------------------------------------------------------------------
# REST API — paginated klines fetch
# ---------------------------------------------------------------------------

def fetch_klines_paginated(
    symbol: str,
    start_time: int,
    end_time: int,
    limit: int | None = None,
) -> list:
    """Paginate /klines API from start_time to end_time. Returns list of DataFrames."""
    from config.config import API_LIMIT as _DEFAULT_LIMIT

    limit = limit or _DEFAULT_LIMIT
    frames = []
    cursor = start_time + 60_000

    while True:
        params = {"startTime": cursor, "endTime": end_time, "limit": limit}

        try:
            data = get_klines(symbol, **params)
        except Exception as exc:
            logger.error("API error for %s: %s", symbol, exc)
            break

        if not data:
            break

        frames.append(parse_klines_df(data, symbol))

        if len(data) < limit:
            break

        cursor = int(data[-1][0]) + 60_000
        sleep_between_requests()

    return frames
