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

from utils.db_utils import ch_command, ch_query_df
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def update_actuals() -> int:
    """Update actual_close and error_pct for past predictions.

    Uses ClickHouse ALTER TABLE ... UPDATE with a subquery join:
      - Matches on (symbol, target_time = timestamp)
      - Only updates rows where actual_close IS NULL and target_time <= now()
      - error_pct = ABS(actual - predicted) / actual * 100

    Returns number of rows updated.
    """
    # First, find how many rows need updating
    count_result = ch_query_df("""
        SELECT count() AS cnt
        FROM predictions p
        WHERE p.actual_close IS NULL
          AND p.target_time <= now()
          AND EXISTS (
              SELECT 1 FROM klines k
              WHERE k.symbol = p.symbol AND k.timestamp = p.target_time
          )
    """)
    to_update = int(count_result["cnt"].iloc[0]) if not count_result.empty else 0

    if to_update == 0:
        logger.info("No predictions to update")
        return 0

    # ClickHouse ALTER TABLE UPDATE
    ch_command("""
        ALTER TABLE predictions
        UPDATE
            actual_close = (
                SELECT k.close FROM klines k
                WHERE k.symbol = predictions.symbol
                  AND k.timestamp = predictions.target_time
                LIMIT 1
            ),
            error_pct = (
                SELECT
                    CASE WHEN k.close > 0
                         THEN abs(k.close - predictions.predicted_close) / k.close * 100
                         ELSE NULL
                    END
                FROM klines k
                WHERE k.symbol = predictions.symbol
                  AND k.timestamp = predictions.target_time
                LIMIT 1
            )
        WHERE actual_close IS NULL
          AND target_time <= now()
          AND EXISTS (
              SELECT 1 FROM klines k
              WHERE k.symbol = predictions.symbol
                AND k.timestamp = predictions.target_time
          )
    """)

    logger.info("Updated %d predictions with actual prices", to_update)
    return to_update


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    updated = update_actuals()
    logger.info("=== Update actuals complete: %d rows ===", updated)


if __name__ == "__main__":
    main()
