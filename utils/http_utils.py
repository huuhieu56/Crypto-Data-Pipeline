"""Generic HTTP utilities with retry and exponential backoff."""

from __future__ import annotations

import time

import requests

from utils.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_TIMEOUT = 30
_DEFAULT_MAX_RETRIES = 3
_RETRY_BACKOFF = 2  # seconds, multiplied by attempt number


def http_get_with_retry(
    url: str,
    params: dict | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
    max_retries: int = _DEFAULT_MAX_RETRIES,
) -> requests.Response:
    """HTTP GET with exponential backoff retry on connection/timeout errors.

    Retries on ConnectionError and Timeout. HTTP errors (4xx, 5xx) are
    raised immediately via raise_for_status() — callers that need
    status-specific handling should catch HTTPError themselves.
    """
    last_exc: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout) as exc:
            logger.warning(
                "%s for %s (%d/%d)", type(exc).__name__, url, attempt, max_retries,
            )
            last_exc = exc
        except requests.HTTPError:
            raise
        except Exception as exc:
            logger.error("Unexpected error calling %s: %s", url, exc)
            raise

        if attempt < max_retries:
            time.sleep(_RETRY_BACKOFF * attempt)

    raise requests.ConnectionError(
        f"Request to {url} failed after {max_retries} retries: {last_exc}"
    )
