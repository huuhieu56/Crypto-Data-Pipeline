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

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
SQL_DIR = PROJECT_ROOT / "sql"
MODELS_DIR = PROJECT_ROOT / "models"

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)

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
MONTHS_BACK = 36          # 3-year historical window

# --- MinIO Object Storage ----------------------------------------------------

MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
    "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
    "secure": os.getenv("MINIO_SECURE", "false").lower() == "true",
    "bucket_raw": os.getenv("MINIO_BUCKET_RAW", "crypto-raw"),
    "bucket_processed": os.getenv("MINIO_BUCKET_PROCESSED", "crypto-processed"),
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
GAP_THRESHOLD_DAYS = 30             # pre_extract: gap → backfill threshold
GAP_WARNING_DAYS = 1                # pre_extract: gap warning threshold
PARTITION_DATE_FORMAT = "%Y-%m-%d"  # daily partition key format

# --- Model Hyperparameters (LSTM) --------------------------------------------

MODEL_CONFIG = {
    "input_window": 120,            # 120 candles (2h lookback)
    "output_window": 60,            # 60 candles (1h prediction)
    "features": 7,                  # open, high, low, close, volume, rsi_14, macd
    "hidden_size": 128,
    "num_layers": 2,
    "dropout": 0.2,
    "learning_rate": 0.001,
    "epochs": 50,
    "batch_size": 32,
    "early_stopping_patience": 10,
    "train_ratio": 0.70,
    "val_ratio": 0.15,
    "test_ratio": 0.15,
    "n_candles_to_load": 50000,
    "default_train_symbols": ["BTCUSDT"],
    # Log-return prediction & directional loss
    "predict_returns": True,
    "directional_loss_weight": 0.3,
    "grad_clip_max_norm": 1.0,
}

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

FEATURE_COLUMNS = ["open", "high", "low", "close", "volume", "rsi_14", "macd"]

# Column rename mapping: raw field names -> database field names
KLINES_RENAME_MAP = {
    "open_time": "timestamp",
    "RSI": "rsi_14",
    "MACD": "macd",
    "MACD_signal": "macd_signal",
}
