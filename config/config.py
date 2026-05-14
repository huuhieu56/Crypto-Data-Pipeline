"""Central configuration for the Crypto Data Pipeline.

All modules MUST import settings from this file.
Values are read from environment variables (.env) with sensible defaults.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# --- Project Paths -----------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# --- Database (ClickHouse) ---------------------------------------------------

CH_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
    "user": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "crypto123"),
    "database": os.getenv("CLICKHOUSE_DB", "crypto_db"),
}

# --- Binance API -------------------------------------------------------------

BINANCE_BASE_URL = "https://api.binance.com/api/v3"
BINANCE_DATA_VISION_URL = (
    "https://data.binance.vision/data/spot/monthly/klines"
    "/{symbol}/1m/{symbol}-1m-{year}-{month:02d}.zip"
)

BINANCE_ENDPOINTS = {
    "klines": f"{BINANCE_BASE_URL}/klines",
    "ticker_24h": f"{BINANCE_BASE_URL}/ticker/24hr",
    "book_ticker": f"{BINANCE_BASE_URL}/ticker/bookTicker",
    "order_book": f"{BINANCE_BASE_URL}/depth",
}

# Rate limiting & request settings
API_LIMIT = 1000          # max records per klines request
ORDER_BOOK_LIMIT = 100    # depth levels for order book
API_TIMEOUT = 30          # seconds
API_SLEEP = 0.1           # seconds between API calls
MONTHS_BACK = int(os.getenv("ETL_MONTHS_BACK", "36"))  # historical window in months

# --- MinIO Object Storage ----------------------------------------------------

MINIO_CONFIG = {
    "enabled": os.getenv("USE_MINIO", "true").lower() == "true",
    "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
    "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
    "secure": os.getenv("MINIO_SECURE", "false").lower() == "true",
    "bucket_raw": os.getenv("MINIO_BUCKET_RAW") or os.getenv("MINIO_RAW_BUCKET", "crypto-raw"),
    "bucket_processed": os.getenv("MINIO_BUCKET_PROCESSED") or os.getenv("MINIO_PROCESSED_BUCKET", "crypto-processed"),
}

# --- Parallelism (ThreadPoolExecutor) ----------------------------------------

PARALLELISM = {
    "bulk_download_workers": 6,     # concurrent month downloads per symbol
    "bulk_symbol_workers": 4,       # concurrent symbols in extract_bulk
    "klines_max_workers": 8,        # concurrent REST API klines fetches
    "orderbook_max_workers": 8,     # concurrent order book fetches
    "transform_max_workers": 8,     # concurrent symbol indicator calculations
}

# --- Pipeline Tuning ---------------------------------------------------------

INDICATOR_CONTEXT_ROWS = 120        # warm-up rows for indicator calculation
PARTITION_MONTH_FORMAT = "%Y-%m"    # monthly partition key format

# --- Klines Schema -----------------------------------------------------------

_KLINES_RAW_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]
RAW_KLINES_COLUMNS = [c for c in _KLINES_RAW_COLUMNS if c != "ignore"]
NUMERIC_COLUMNS = [
    "open", "high", "low", "close", "volume",
    "quote_volume", "taker_buy_base", "taker_buy_quote",
]

# --- Feature Columns (shared by Transform & Load) ----------------------------

KLINES_COLUMNS = [
    "symbol", "timestamp", "open", "high", "low", "close",
    "volume", "quote_volume", "trades", "rsi_14", "macd", "macd_signal",
]
