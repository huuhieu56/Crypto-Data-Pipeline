"""Update actuals — backfill realized prices into prediction rows.

Workflow:
    1. Find predictions missing actual_close where target_time has passed
    2. Query actual close prices (1-min candles) from the klines table
    3. Update actual_close and error_pct in the predictions table

Schedule: runs at the end of hourly_inference DAG.

Usage:
    python scripts/update_actuals.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from utils.db_utils import get_engine
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def update_actuals(engine) -> int:
    """Update actual_close and error_pct for past predictions.

    Uses a single SQL UPDATE ... FROM join between predictions and klines:
      - Matches on (symbol, target_time = timestamp)
      - Only updates rows where actual_close IS NULL and target_time <= NOW()
      - error_pct = ABS(actual - predicted) / actual * 100

    Returns number of rows updated.
    """
    update_sql = text("""
        UPDATE predictions p
        SET
            actual_close = k.close,
            error_pct    = CASE
                WHEN k.close > 0
                THEN ABS(k.close - p.predicted_close) / k.close * 100
                ELSE NULL
            END
        FROM klines k
        WHERE p.symbol      = k.symbol
          AND p.target_time  = k.timestamp
          AND p.actual_close IS NULL
          AND p.target_time  <= NOW()
    """)

    with engine.connect() as conn:
        result = conn.execute(update_sql)
        updated = result.rowcount
        conn.commit()

    logger.info("Updated %d predictions with actual prices", updated)
    return updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    engine = get_engine()
    updated = update_actuals(engine)
    logger.info("=== Update actuals complete: %d rows ===", updated)


if __name__ == "__main__":
    main()
