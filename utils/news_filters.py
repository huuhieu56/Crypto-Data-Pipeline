"""Shared news quality filters for extract and transform stages.

Filters:
    1. Minimum description length
    2. Spam keywords in title
    3. Relevance — must mention at least one tracked crypto keyword
"""

from __future__ import annotations

import re

from config.config import (
    GNEWS_MIN_DESC_LENGTH,
    GNEWS_RELEVANT_KEYWORDS,
    GNEWS_SPAM_TITLE_KEYWORDS,
)
from utils.logger import get_logger

logger = get_logger(__name__)

# Pre-compiled patterns
_SPAM_PATTERN = re.compile(
    "|".join(re.escape(kw) for kw in GNEWS_SPAM_TITLE_KEYWORDS),
    re.IGNORECASE,
)
_RELEVANCE_PATTERN = re.compile(
    r"\b(?:" + "|".join(re.escape(kw) for kw in GNEWS_RELEVANT_KEYWORDS) + r")\b",
    re.IGNORECASE,
)


def filter_articles(records: list[dict]) -> list[dict]:
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

        if len(desc) < GNEWS_MIN_DESC_LENGTH:
            logger.debug("Filtered (short desc): %s", title[:60])
            continue

        if _SPAM_PATTERN.search(title):
            logger.debug("Filtered (spam title): %s", title[:60])
            continue

        if not _RELEVANCE_PATTERN.search(text):
            logger.debug("Filtered (not relevant): %s", title[:60])
            continue

        filtered.append(rec)

    skipped = len(records) - len(filtered)
    if skipped:
        logger.info("Quality filter: %d/%d articles kept (%d skipped)",
                    len(filtered), len(records), skipped)

    return filtered


def filter_dataframe(df, title_col: str = "title", desc_col: str = "description"):
    """Filter a DataFrame of articles by the same quality rules.

    Args:
        df: DataFrame with at least title and description columns.
        title_col: Name of the title column.
        desc_col: Name of the description column.

    Returns:
        Filtered DataFrame.
    """
    import pandas as pd

    before = len(df)

    # Rule 1: minimum description length
    desc = df[desc_col].fillna("")
    mask = desc.str.len() >= GNEWS_MIN_DESC_LENGTH

    # Rule 2: spam keywords in title
    title = df[title_col].fillna("")
    mask &= ~title.str.contains(_SPAM_PATTERN, regex=True, na=False)

    # Rule 3: relevance
    text = title + " " + desc
    mask &= text.str.contains(_RELEVANCE_PATTERN, regex=True, na=False)

    filtered = df[mask]
    skipped = before - len(filtered)
    if skipped:
        logger.info("Quality filter: %d/%d articles kept (%d skipped)",
                    len(filtered), before, skipped)
    return filtered
