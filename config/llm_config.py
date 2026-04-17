"""Configuration for LLM advisory signal generation."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


# Provider: gemini | openai
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini").lower()

# API keys
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Model names
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4-nano")

# Generation params
TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.1"))
MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "220"))
TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "15"))

# Signal / prompt settings
CANDLE_WINDOW_MINUTES = int(os.getenv("CANDLE_WINDOW_MINUTES", "360"))
LLM_DAILY_CANDLES = int(os.getenv("LLM_DAILY_CANDLES", "30"))
RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "30"))

# Batch / retry
BATCH_SIZE = int(os.getenv("LLM_BATCH_SIZE", "10"))
MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))
RETRY_DELAY = float(os.getenv("LLM_RETRY_DELAY", "2"))
