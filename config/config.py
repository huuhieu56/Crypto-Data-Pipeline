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
    "order_book": f"{BINANCE_BASE_URL}/depth",
}

# Rate limiting & request settings
API_LIMIT = 1000          # max records per klines request
ORDER_BOOK_LIMIT = 100    # depth levels for order book
OBI_DEPTH_PCT = 0.005   # ±0.5% around mid price for OBI calculation
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

# Internal S3 endpoint for ClickHouse to reach MinIO (Docker network)
CLICKHOUSE_S3_ENDPOINT = os.getenv(
    "CH_S3_ENDPOINT",
    f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
)

# --- Pipeline Tuning ---------------------------------------------------------

INDICATOR_CONTEXT_ROWS = 120        # warm-up rows for indicator calculation
PARTITION_MONTH_FORMAT = "%Y-%m"    # monthly partition key format

# --- Binance API → DB column mapping (camelCase → snake_case) ----------------

BINANCE_COLUMN_MAP = {
    # ticker_24h endpoint (includes bid/ask)
    "priceChange": "price_change",
    "priceChangePercent": "price_change_pct",
    "highPrice": "high_24h",
    "lowPrice": "low_24h",
    "volume": "volume_24h",
    "quoteVolume": "quote_volume_24h",
    "count": "trade_count",
    "bidPrice": "bid_price",
    "askPrice": "ask_price",
}

# --- Klines Schema -----------------------------------------------------------

_KLINES_RAW_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trade_count",
    "taker_buy_base", "taker_buy_quote", "ignore",
]
RAW_KLINES_COLUMNS = [c for c in _KLINES_RAW_COLUMNS if c != "ignore"]
NUMERIC_COLUMNS = [
    "open", "high", "low", "close", "volume",
    "quote_volume", "taker_buy_base", "taker_buy_quote",
]

# --- GNews API ----------------------------------------------------------------

GNEWS_API_KEY = os.getenv("GNEWS_API_KEY", "")

# Search queries — each query costs 1 API request per run.
# Free tier: 100 requests/day. At 15-min intervals = 96 calls/day.
# → Use 1 query per run to stay within limit.
# Covers top coins from config/symbols.py via OR boolean.
# GNews API limits query to ~200 URL-encoded chars.
# Use short names/tickers; multi-word names are too expensive.
GNEWS_SEARCH_QUERIES = [
    "crypto OR BTC OR ETH OR SOL OR BNB OR XRP OR DOGE OR ADA"
    " OR TRX OR LINK OR AVAX OR DOT OR LTC OR UNI OR ATOM OR APT",
]

# --- Quality Filters (applied after fetch) -----------------------------------

# Minimum description length — skip articles with thin/empty descriptions
GNEWS_MIN_DESC_LENGTH = 50

# Spam keywords in title — articles promoting shitcoins, airdrops, giveaways
GNEWS_SPAM_TITLE_KEYWORDS = [
    "presale", "airdrop", "giveaway", "free coins", "buy now",
    "100x", "1000x", "moonshot", "guaranteed", "act now",
    "pepeto", "shiba 2.0", "next dogecoin",
]

# Tracked coin names for relevance check — article must mention at least one.
# Only use UNAMBIGUOUS terms (avoid "avalanche" = NHL team, "dot" = common word).
GNEWS_RELEVANT_KEYWORDS = [
    "crypto", "cryptocurrency", "blockchain", "defi", "nft",
    # Layer 1
    "bitcoin", "btc", "ethereum", "eth", "binance", "bnb",
    "solana", "sol", "xrp", "ripple", "cardano", "ada",
    "tron", "trx", "avalanche", "avax", "polkadot", "dot",
    "toncoin", "ton", "near", "aptos", "apt", "sui",
    "cosmos", "atom", "vechain", "vet",
    "internet computer", "icp", "ethereum classic", "etc",
    "algorand", "algo", "tezos", "xtz", "stacks", "stx",
    "hedera", "hbar", "bitcoin cash", "bch", "neo",
    # Layer 2
    "arbitrum", "arb", "optimism", "op",
    "immutable", "imx", "polygon", "matic",
    # DeFi / Infra
    "chainlink", "link", "uniswap", "uni", "aave",
    "litecoin", "ltc", "filecoin", "fil",
    "the graph", "grt", "render",
    "thorchain", "rune", "injective", "inj",
    "theta", "theta network", "arweave",
    # Meme / Gaming / Metaverse
    "dogecoin", "doge", "shiba", "shib", "pepe", "dogwifhat", "wif",
    "axie infinity", "axs", "the sandbox", "sand",
    "decentraland", "mana", "stellar", "xlm",
    # Generic crypto terms
    "trading crypto", "exchange crypto", "market cap",
    "altcoin", "stablecoin", "web3", "token",
]
