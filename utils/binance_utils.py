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
_RETRY_BACKOFF = 2  # seconds, nhan doi sau moi lan retry


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
            # 429 Too Many Requests — doi lau hon
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
    """Giong make_request nhung tra ve raw bytes (dung cho download zip)."""
    timeout = timeout or API_TIMEOUT
    last_exc: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.content
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
    """Lay du lieu nen tu /api/v3/klines."""
    params = {"symbol": symbol, "interval": interval, **kwargs}
    return make_request(BINANCE_ENDPOINTS["klines"], params=params)


def get_ticker_24h() -> list[dict]:
    """Lay thong ke 24h cho tat ca symbols."""
    return make_request(BINANCE_ENDPOINTS["ticker_24h"])


def get_book_ticker() -> list[dict]:
    """Lay best bid/ask cho tat ca symbols."""
    return make_request(BINANCE_ENDPOINTS["book_ticker"])


def get_order_book(symbol: str, limit: int = 100) -> dict:
    """Lay order book depth cho 1 symbol."""
    params = {"symbol": symbol, "limit": limit}
    return make_request(BINANCE_ENDPOINTS["order_book"], params=params)


def sleep_between_requests() -> None:
    """Doi giua cac request de tranh rate limit."""
    time.sleep(API_SLEEP)
