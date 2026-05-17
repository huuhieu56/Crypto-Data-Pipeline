"""Binance API utilities for the Crypto Data Pipeline.

Provides HTTP request helpers, endpoint wrappers, klines parsing,
Data Vision bulk downloads, and paginated REST API fetching.
"""

from __future__ import annotations

import io
import time
import zipfile

import pandas as pd
import requests

from config.config import (
    BINANCE_ENDPOINTS,
    BINANCE_DATA_VISION_URL,
    API_LIMIT,
    API_TIMEOUT,
    API_SLEEP,
    _KLINES_RAW_COLUMNS,
    RAW_KLINES_COLUMNS,
    NUMERIC_COLUMNS,
)
from utils.logger import get_logger
from utils.exceptions import APIRequestError

logger = get_logger(__name__)

# --- Core Request Helper -----------------------------------------------------

_DEFAULT_MAX_RETRIES = 3
_RETRY_BACKOFF = 2  # seconds, doubles after each retry


def _request_with_retry(
    url: str,
    params: dict | None = None,
    timeout: int | None = None,
    max_retries: int = _DEFAULT_MAX_RETRIES,
) -> requests.Response:
    """HTTP GET with retry, exponential backoff, 429/404 handling."""
    timeout = timeout or API_TIMEOUT
    last_exc: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            logger.warning("HTTP %s from %s (%d/%d)", status, url, attempt, max_retries)
            last_exc = exc
            if status == 404:
                break
            if status == 429:
                wait = _RETRY_BACKOFF * attempt * 2
                logger.warning("Rate limited — waiting %ds", wait)
                time.sleep(wait)
                continue
        except (requests.ConnectionError, requests.Timeout) as exc:
            logger.warning("%s for %s (%d/%d)", type(exc).__name__, url, attempt, max_retries)
            last_exc = exc
        except Exception as exc:
            logger.error("Unexpected error calling %s: %s", url, exc)
            last_exc = exc
            break

        if attempt < max_retries:
            time.sleep(_RETRY_BACKOFF * attempt)

    raise APIRequestError(endpoint=url, detail=str(last_exc))


def make_request(
    url: str,
    params: dict | None = None,
    timeout: int | None = None,
    max_retries: int = _DEFAULT_MAX_RETRIES,
) -> dict | list:
    """GET → JSON with retry."""
    return _request_with_retry(url, params, timeout, max_retries).json()


def make_request_raw(
    url: str,
    params: dict | None = None,
    timeout: int | None = None,
    max_retries: int = _DEFAULT_MAX_RETRIES,
) -> bytes:
    """GET → raw bytes with retry (for ZIP downloads)."""
    return _request_with_retry(url, params, timeout, max_retries).content


# --- Endpoint Wrappers -------------------------------------------------------

def get_klines(symbol: str, interval: str = "1m", **kwargs) -> list:
    """Fetch candlestick data from /api/v3/klines."""
    params = {"symbol": symbol, "interval": interval, **kwargs}
    return make_request(BINANCE_ENDPOINTS["klines"], params=params)


def get_ticker_24h() -> list[dict]:
    """Fetch 24h ticker stats for all symbols."""
    return make_request(BINANCE_ENDPOINTS["ticker_24h"])


def get_order_book(symbol: str, limit: int = 100) -> dict:
    """Fetch order book depth for a symbol."""
    params = {"symbol": symbol, "limit": limit}
    return make_request(BINANCE_ENDPOINTS["order_book"], params=params)


def sleep_between_requests() -> None:
    """Sleep between requests to avoid rate limit."""
    time.sleep(API_SLEEP)


# --- Klines Parsing ----------------------------------------------------------


def parse_klines_df(raw_data: list[list], symbol: str):
    """Parse raw klines list into a DataFrame. Keeps epoch ms timestamps as-is."""
    df = pd.DataFrame(raw_data, columns=_KLINES_RAW_COLUMNS)
    df = df.drop(columns=["ignore"])
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["symbol"] = symbol
    return df


# --- Data Vision (monthly bulk download) ------------------------------------

def download_klines_month(symbol: str, year: int, month: int):
    """Download 1-month klines ZIP from Data Vision. Returns DataFrame or None."""
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
        names = z.namelist()
        if not names:
            logger.warning(
                "Empty ZIP for %s %d-%02d", symbol, year, month,
            )
            return None
        with z.open(names[0]) as f:
            df = pd.read_csv(
                f, header=None, usecols=range(11), names=RAW_KLINES_COLUMNS,
            )

    raw_ts = df["open_time"].astype("int64")
    # Data Vision may use microsecond (>1e15) or millisecond timestamps
    if raw_ts.iloc[0] > 1e15:
        df["open_time"] = raw_ts // 1000
        df["close_time"] = df["close_time"].astype("int64") // 1000
    df["symbol"] = symbol
    return df


# --- REST API (paginated klines fetch) --------------------------------------

def fetch_klines_paginated(
    symbol: str,
    start_time: int,
    end_time: int,
    limit: int | None = None,
) -> pd.DataFrame | None:
    """Paginate /klines API from start_time to end_time.

    Collects raw API rows first, then parses into a single DataFrame
    at the end — avoids creating N intermediate DataFrames (1 per page).

    Returns DataFrame or None if no data.
    """
    limit = limit or API_LIMIT
    all_raw: list[list] = []
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

        all_raw.extend(data)

        if len(data) < limit:
            break

        cursor = int(data[-1][0]) + 60_000
        sleep_between_requests()

    if not all_raw:
        return None
    return parse_klines_df(all_raw, symbol)
