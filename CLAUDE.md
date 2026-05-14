# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

End-to-end crypto data pipeline that collects 1-minute candle data from Binance for 50 coins, computes technical indicators (RSI-14, MACD) via Spark, loads into ClickHouse, and exposes an LLM-powered chat assistant via LangGraph/FastAPI. Grafana provides dashboards with an embedded chatbox.

## Commands

### Start/Stop Infrastructure
```bash
docker compose up -d          # Start all services (ClickHouse, MinIO, Airflow, Grafana, Chat API)
docker compose down            # Stop all services
docker compose logs -f <svc>   # Tail logs for a specific service
```

### Testing
```bash
# Unit tests (offline, no API key needed)
pytest tests/test_market_queries.py tests/test_nodes.py tests/test_chat_api.py tests/test_extract.py tests/test_transform.py -v

# AI evaluation tests (requires LLM_API_KEY in .env, marked with @pytest.mark.llm)
pytest tests/test_ai_eval.py -v

# All tests
pytest -v
```

### Running ETL Manually
The ETL runs automatically every minute via Airflow DAG `minutely_etl`. For manual runs:
```bash
# From project root with venv activated
python scripts/extract.py    # Fetch from Binance API
python scripts/transform.py  # Compute RSI/MACD via Spark
python scripts/load.py       # Load into ClickHouse
```

## Architecture

### ETL Pipeline Flow
```
Binance REST API ──extract──▶ Parquet files (data/raw/) ──transform──▶ Parquet (data/processed/) ──load──▶ ClickHouse
```
- **Extract** (`scripts/extract.py`): Fetches 1-min klines, 24h ticker, order book snapshots. First run auto-bootstraps 3 years of historical data from Binance Data Vision; subsequent runs are incremental (~50 rows/min).
- **Transform** (`scripts/transform.py`): PySpark computes RSI(14) and MACD(12/26/9) on 1-min candles.
- **Load** (`scripts/load.py`): Writes to ClickHouse via `clickhouse-connect`. Supports `--only klines|ticker|orderbook|symbols` flags for selective loading.

### Airflow DAG (`airflow/dags/minutely_etl.py`)
Three independent extract→load chains running every minute:
- `extract_klines → transform → load_klines`
- `extract_ticker → load_ticker`
- `extract_order_book → load_order_book`
Plus `load_symbols` (dimension table, no dependency).

### Chat API (`services/chat_api/`)
FastAPI service with LangGraph workflow:
- **Graph** (`graph.py`): `load_history → agent ↔ tools → save_history`
- **Nodes** (`nodes.py`): LLM agent with tool-calling loop; tools query ClickHouse for market data
- **Market queries** (`market_queries.py`): ClickHouse SQL for candles, ticker, order book — timeframe-aware (short/medium/long/very_long)
- **LLM providers**: Supports Gemini (native), OpenAI, DeepSeek, Groq, Mistral via `LLM_BASE_URL` override

### ClickHouse Schema (`sql/schema.sql`)
Star schema with `ReplacingMergeTree` engines, partitioned by month:
- **Dimension**: `symbols` (50 coins)
- **Facts**: `klines` (1-min candles + indicators), `ticker_24h`, `order_book_snapshot`
- **Chat**: `chat_history` (session-based, `MergeTree`)

### Configuration
- `config/config.py`: Central config — ClickHouse, Binance API, MinIO, parallelism settings, column schemas. All modules import from here.
- `config/llm_config.py`: LLM provider/model/params, timeframe configs (short/medium/long/very_long) with ClickHouse aggregation queries and LLM guidance per timeframe.
- `config/symbols.py`: `SYMBOL_REGISTRY` is the single source of truth for the 50 tracked coins. Derived views: `SYMBOLS`, `SYMBOLS_STATUS`, `BREAK_DATES`.

### Utilities (`utils/`)
- `db_utils.py`: ClickHouse client helpers (`get_ch_client`, `ch_query_df`, insert functions)
- `binance_utils.py`: Binance API wrappers with retry/rate-limit handling
- `llm_utils.py`: LLM API callers for DeepSeek, Gemini, OpenAI
- `storage.py`: MinIO object storage utilities
- `data_utils.py`: Timestamp/partition key helpers
- `logger.py`: Logging configuration
- `exceptions.py`: Custom exceptions per layer (E/T/L/LLM)

## Key Conventions

- All Python imports use the project root as base (`config.xxx`, `scripts.xxx`, `utils.xxx`).
- The Chat API service has its own `requirements.txt` (in `services/chat_api/`) separate from the project root.
- `.env` is gitignored — copy from `.env.example` and fill in `LLM_API_KEY` at minimum.
- Vietnamese comments and docstrings are common throughout the codebase.
- Airflow mounts the entire project at `/opt/project` inside Docker.
