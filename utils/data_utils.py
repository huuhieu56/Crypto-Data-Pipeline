# =============================================================================
# Data Utilities - Crypto Data Pipeline
# =============================================================================
# Các functions hỗ trợ xử lý dữ liệu
# =============================================================================

from __future__ import annotations

import pandas as pd

from config.config import RAW_DATA_DIR
from utils.logger import get_logger

logger = get_logger(__name__)


def get_last_timestamp(symbol: str) -> int | None:
    """Get last timestamp (in milliseconds) of a symbol's CSV data."""
    csv_path = RAW_DATA_DIR / f"{symbol}.csv"
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, usecols=["open_time"])
        if df.empty:
            return None
        last = pd.to_datetime(df["open_time"].iloc[-1])
        return int(last.timestamp() * 1000)
    except Exception as exc:
        logger.error("Cannot read last timestamp from %s: %s", csv_path.name, exc)
        return None


# TODO: Implement normalize_data()
# - Min-Max normalization cho features

# TODO: Implement denormalize_data()
# - Reverse normalization cho predictions

# TODO: Implement create_sequences()
# - Tạo input/output sequences cho LSTM

# TODO: Implement validate_data()
# - Check missing values, duplicates
