"""Configuration for LLM chat assistant."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


# Provider: gemini | openai (ignored when LLM_BASE_URL is set)
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini").lower()
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "gemini-2.5-flash-lite")

# Generation params
TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.3"))
MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "65536"))
TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "30"))

# Market context
LLM_DAILY_CANDLES = int(os.getenv("LLM_DAILY_CANDLES", "30"))

# Chat settings
CHAT_MAX_HISTORY = int(os.getenv("CHAT_MAX_HISTORY", "10"))

# Retry
MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))
RETRY_DELAY = float(os.getenv("LLM_RETRY_DELAY", "2"))

# LangSmith (auto-enabled when env vars are set)
LANGSMITH_TRACING = os.getenv("LANGCHAIN_TRACING_V2", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Timeframe-aware market data configuration
# ---------------------------------------------------------------------------

TIMEFRAME_CONFIG = {
    "short": {
        "label": "Short-term (hours → days)",
        "candle_group_by": "toStartOfHour(open_time)",
        "candle_lookback_days": 20,
        "candle_limit": 480,
        "candle_ts_format": "%Y-%m-%d %H:00",
        "ticker_group_by": "toStartOfHour(snapshot_time)",
        "ticker_lookback_days": 3,
        "ob_group_by": "toStartOfHour(timestamp)",
        "ob_lookback_days": 3,
        "ob_mode": "trend",
        "news_lookback_days": 3,
        "llm_guidance": (
            "Focus on hourly momentum, volume spikes, order book "
            "imbalance shifts, and intraday support/resistance levels."
        ),
    },
    "medium": {
        "label": "Medium-term (weeks → month)",
        "candle_group_by": "toStartOfInterval(open_time, INTERVAL 4 HOUR)",
        "candle_lookback_days": 90,
        "candle_limit": 540,
        "candle_ts_format": "%Y-%m-%d %H:00",
        "ticker_group_by": "toDate(snapshot_time)",
        "ticker_lookback_days": 30,
        "ob_group_by": "toDate(timestamp)",
        "ob_lookback_days": 30,
        "ob_mode": "trend",
        "news_lookback_days": 14,
        "llm_guidance": (
            "Focus on daily trend direction, volume trend changes, "
            "MACD crossovers, and key price levels."
        ),
    },
    "long": {
        "label": "Long-term (months → 1 year)",
        "candle_group_by": "toDate(open_time)",
        "candle_lookback_days": 730,
        "candle_limit": 730,
        "candle_ts_format": "%Y-%m-%d",
        "ticker_group_by": "toStartOfWeek(snapshot_time)",
        "ticker_lookback_days": 730,
        "ob_mode": "summary_30d",
        "news_lookback_days": 30,
        "llm_guidance": (
            "Focus on macro trend, long-term support/resistance, "
            "volume cycles, and structural market shifts."
        ),
    },
    "very_long": {
        "label": "Very long-term (1 → 3+ years)",
        "candle_group_by": "toDate(open_time)",
        "candle_lookback_days": 1095,
        "candle_limit": 1095,
        "candle_ts_format": "%Y-%m-%d",
        "ticker_group_by": "toStartOfMonth(snapshot_time)",
        "ticker_lookback_days": 1095,
        "ob_mode": "latest_only",
        "news_lookback_days": 90,
        "llm_guidance": (
            "Focus on multi-year cycles, halvings, macro adoption "
            "trends, and historical price ranges."
        ),
    },
}
