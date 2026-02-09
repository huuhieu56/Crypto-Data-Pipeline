# =============================================================================
# Configuration - Crypto Data Pipeline
# =============================================================================
# File chua TAT CA cau hinh cho he thong.
# Tat ca cac module khac PHAI import tu day, KHONG duoc hardcode.
#
# Uu tien doc tu bien moi truong (.env) -> fallback ve gia tri mac dinh.
# =============================================================================

import os
from pathlib import Path

from dotenv import load_dotenv

# =============================================================================
# Project Paths & Environment
# =============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
SQL_DIR = PROJECT_ROOT / "sql"
MODELS_DIR = PROJECT_ROOT / "models"

# Đảm bảo thư mục tồn tại
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# Database Configuration
# =============================================================================
DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER", "crypto123az"),
    "password": os.getenv("POSTGRES_PASSWORD", "crypto123"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "crypto_db"),
}

DB_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

JDBC_URL = (
    f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}"
    f"/{DB_CONFIG['dbname']}"
)

JDBC_PROPERTIES = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver",
    "batchsize": "10000",
}

# =============================================================================
# Binance API Configuration
# =============================================================================
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

# Rate limit & request settings
API_LIMIT = 1000          # Max records per klines request
ORDER_BOOK_LIMIT = 100    # Depth levels for order book
API_TIMEOUT = 30          # Seconds
API_SLEEP = 0.1           # Seconds between API calls
MONTHS_BACK = 36          # Lịch sử 3 năm

# =============================================================================
# Spark Configuration
# =============================================================================
SPARK_CONFIG = {
    "driver_memory": os.getenv("SPARK_DRIVER_MEMORY", "6g"),
    "jdbc_package": "org.postgresql:postgresql:42.6.0",
    "arrow_enabled": "true",
}

# =============================================================================
# Model Hyperparameters (LSTM)
# =============================================================================
MODEL_CONFIG = {
    "input_window": 60,     # Số nến quá khứ
    "output_window": 60,    # Số nến dự báo
    "features": 7,          # open, high, low, close, volume, rsi_14, macd
    "hidden_size": 128,
    "num_layers": 2,
    "dropout": 0.2,
    "learning_rate": 0.001,
    "epochs": 50,
    "batch_size": 64,
    "early_stopping_patience": 5,
    "train_ratio": 0.70,
    "val_ratio": 0.15,
    "test_ratio": 0.15,
}

# =============================================================================
# Feature columns — dùng chung cho Transform & Load
# =============================================================================
KLINES_COLUMNS = [
    "symbol", "timestamp", "open", "high", "low", "close",
    "volume", "quote_volume", "trades", "rsi_14", "macd", "macd_signal",
]

FEATURE_COLUMNS = ["open", "high", "low", "close", "volume", "rsi_14", "macd"]

# Column rename mapping: raw → DB
KLINES_RENAME_MAP = {
    "open_time": "timestamp",
    "RSI": "rsi_14",
    "MACD": "macd",
    "MACD_signal": "macd_signal",
}
