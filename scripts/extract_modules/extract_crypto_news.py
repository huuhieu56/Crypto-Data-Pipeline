"""Extract crypto news from GNews API.

Uses GNews search endpoint to fetch crypto/binance news articles.
Stores raw Parquet to MinIO every 15 minutes.

Extract stage only: no transformation or loading to ClickHouse.

GNews free tier: 100 requests/day, max 10 articles/request.
At 15-min intervals = 96 calls/day → fits within limit.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pandas as pd
import requests

from config.config import (
    GNEWS_API_KEY,
    GNEWS_MIN_DESC_LENGTH,
    GNEWS_RELEVANT_KEYWORDS,
    GNEWS_SEARCH_QUERIES,
    GNEWS_SPAM_TITLE_KEYWORDS,
    MINIO_CONFIG,
)
from utils.exceptions import ExtractError
from utils.http_utils import http_get_with_retry
from utils.logger import get_logger
from utils.storage import append_to_partition

logger = get_logger(__name__)
BUCKET_RAW = MINIO_CONFIG["bucket_raw"]

GNEWS_SEARCH_URL = "https://gnews.io/api/v4/search"


def _fetch_articles(query: str, max_articles: int = 10) -> list[dict]:
    """Fetch articles from GNews search endpoint."""
    if not GNEWS_API_KEY:
        raise ExtractError(
            "GNews API key not configured. "
            "Set GNEWS_API_KEY in .env (get free key at https://gnews.io/register)"
        )

    params = {
        "q": query,
        "lang": "en",
        "max": max_articles,
        "apikey": GNEWS_API_KEY,
    }

    try:
        resp = http_get_with_retry(GNEWS_SEARCH_URL, params=params, timeout=30)
    except requests.RequestException as exc:
        raise ExtractError(f"GNews API request failed: {exc}") from exc

    data = resp.json()
    return data.get("articles", [])


def _parse_articles(articles: list[dict], query: str) -> list[dict]:
    """Parse GNews articles into flat records for DataFrame."""
    now = datetime.now(timezone.utc)
    records = []

    for article in articles:
        source = article.get("source", {})
        published = article.get("publishedAt", "")

        # Parse publishedAt (ISO 8601: "2025-09-30T19:38:25Z")
        try:
            published_dt = datetime.fromisoformat(
                published.replace("Z", "+00:00"),
            )
        except (ValueError, AttributeError):
            published_dt = now

        records.append({
            "article_id": _make_article_id(article),
            "title": (article.get("title") or "")[:500],
            "description": (article.get("description") or "")[:1000],
            "content": (article.get("content") or "")[:2000],
            "url": (article.get("url") or "")[:500],
            "image_url": (article.get("image") or "")[:500],
            "source_name": (source.get("name") or "")[:200],
            "source_url": (source.get("url") or "")[:500],
            "published_at": published_dt,
            "search_query": query,
            "extracted_at": now,
        })

    return records


def _make_article_id(article: dict) -> str:
    """Generate a stable ID for dedup from URL (most unique field)."""
    import hashlib

    url = article.get("url", "")
    return hashlib.md5(url.encode()).hexdigest()


# Pre-compile patterns for filter performance
_SPAM_PATTERN = re.compile(
    "|".join(re.escape(kw) for kw in GNEWS_SPAM_TITLE_KEYWORDS),
    re.IGNORECASE,
)
_RELEVANCE_PATTERN = re.compile(
    r"\b(?:" + "|".join(re.escape(kw) for kw in GNEWS_RELEVANT_KEYWORDS) + r")\b",
    re.IGNORECASE,
)


def _filter_articles(records: list[dict]) -> list[dict]:
    """Filter out spam, irrelevant, and low-quality articles.

    Rules:
        1. Description too short (< GNEWS_MIN_DESC_LENGTH) → skip
        2. Title contains spam keywords (presale, airdrop, ...) → skip
        3. Title+description don't mention any tracked coin/keyword → skip
    """
    filtered = []

    for rec in records:
        title = rec.get("title", "")
        desc = rec.get("description", "")
        text = f"{title} {desc}"

        # Rule 1: minimum description length
        if len(desc) < GNEWS_MIN_DESC_LENGTH:
            logger.debug("Filtered (short desc): %s", title[:60])
            continue

        # Rule 2: spam keywords in title
        if _SPAM_PATTERN.search(title):
            logger.debug("Filtered (spam title): %s", title[:60])
            continue

        # Rule 3: relevance — must mention at least one tracked keyword
        if not _RELEVANCE_PATTERN.search(text):
            logger.debug("Filtered (not relevant): %s", title[:60])
            continue

        filtered.append(rec)

    skipped = len(records) - len(filtered)
    if skipped:
        logger.info("Quality filter: %d/%d articles kept (%d skipped)",
                    len(filtered), len(records), skipped)

    return filtered


def extract_crypto_news(
    queries: list[str] | None = None,
) -> dict[str, int]:
    """Extract crypto news articles from GNews API.

    Args:
        queries: Search queries. Defaults to GNEWS_SEARCH_QUERIES.
                 Each query costs 1 API request.

    Returns:
        Dict with counts: {"articles": N, "queries": M}.
    """
    queries = queries or GNEWS_SEARCH_QUERIES
    total_articles = 0

    for query in queries:
        try:
            raw_articles = _fetch_articles(query, max_articles=10)

            if not raw_articles:
                logger.info("Query '%s': no articles found", query)
                continue

            records = _parse_articles(raw_articles, query)
            records = _filter_articles(records)

            if records:
                df = pd.DataFrame(records)
                append_to_partition(
                    BUCKET_RAW, "crypto_news", "gnews",
                    df, dedup_col="article_id",
                )
                total_articles += len(records)
                logger.info(
                    "Query '%s': %d articles extracted",
                    query, len(records),
                )

        except ExtractError:
            raise
        except Exception as exc:
            logger.error("Failed to extract query '%s': %s", query, exc)
            continue

    logger.info(
        "=== GNews Extract finished: %d articles from %d queries ===",
        total_articles, len(queries),
    )
    return {"articles": total_articles, "queries": len(queries)}
