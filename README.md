# Crypto Data Pipeline - Big Data & LLM Chat Assistant

## Mô tả dự án

Hệ thống End-to-End Data Pipeline thu thập, xử lý và phân tích thị trường cryptocurrency
sử dụng ClickHouse SQL, Apache Airflow, ClickHouse, MinIO, LLM (DeepSeek/Gemini/OpenAI) và Grafana.

- **Extract mỗi phút**: Gọi Binance REST API lấy nến 1-min, ticker 24h, order book cho 50 coins
- **Transform**: ClickHouse SQL tính RSI(14), MACD(12/26/9) trên 1-min candles; Python tính OBI, spread, bid/ask walls từ order book snapshot
- **Load**: Ghi vào ClickHouse (clickhouse-connect)
- **LLM Chat Assistant**: AI chatbot với LangGraph workflow, DeepSeek reasoning model, tool calling tự động (candles, volume, order book), timeframe-aware analysis
- **Visualize**: Grafana dashboard real-time + embedded chatbox

## Cấu trúc thư mục

```
Crypto-Data-Pipeline/
├── .env                                # Biến môi trường
├── .env.example                        # Mẫu biến môi trường
├── README.md                           # Tài liệu dự án
├── requirements.txt                    # Python dependencies
├── docker-compose.yml                  # Docker services config
├── pytest.ini                          # Pytest config (markers)
│
├── config/                             # Cấu hình hệ thống
│   ├── config.py                       # Main config (paths, DB, API, MinIO, ClickHouse)
│   ├── llm_config.py                   # LLM config (provider, base_url, model, timeframes)
│   └── symbols.py                      # Danh sách 50 coins (single source of truth)
│
├── scripts/                            # ETL scripts (auto-bootstrap + incremental)
│   ├── extract.py                      # Entry point/orchestrator cho extract
│   ├── extract_modules/                # Logic extract theo klines, ticker, order book
│   ├── transform.py                    # ClickHouse SQL (RSI, MACD) → MinIO processed
│   └── load.py                         # Ghi vào ClickHouse (clickhouse-connect + s3)
│
├── services/                           # Microservices
│   └── chat_api/                       # LLM Chat Assistant backend
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── main.py                     # FastAPI endpoints (chat, history, health)
│       ├── graph.py                    # LangGraph workflow definition
│       ├── nodes.py                    # LangGraph nodes (agent, tools, load/save history)
│       ├── market_queries.py           # Market data queries (candles, ticker, orderbook)
│       └── chat_ui.html                # Chat interface (dark theme, iframe-ready)
│
├── airflow/                            # Apache Airflow
│   └── dags/
│       └── minutely_etl.py            # DAG Mini-batch ETL mỗi phút (* * * * *)
│
├── sql/                                # Database schemas & queries
│   ├── schema.sql                      # ClickHouse schema (Star Schema: 1 dim + 3 fact)
│   ├── transform_klines.sql            # ClickHouse SQL transform (RSI + MACD)
│   ├── queries.sql                     # Query mẫu cho Grafana
│   └── init_db.sql                     # Tạo database thủ công (ngoài Docker)
│
├── tests/                              # Test suites
│   ├── test_market_queries.py          # Unit tests: market data formatting & queries
│   ├── test_nodes.py                   # Unit tests: LangGraph nodes, LLM factory, tools
│   ├── test_chat_api.py                # Unit tests: FastAPI endpoints
│   ├── test_ai_eval.py                 # Integration tests: LLM evaluation (live API)
│   ├── test_extract.py                 # Unit tests: extract pipeline
│   └── test_load.py                    # Unit tests: load + transform pipeline
│
├── utils/                              # Utility functions
│   ├── binance_utils.py                # Binance API wrappers (retry, rate limit, parse)
│   ├── db_utils.py                     # ClickHouse client, insert/query helpers
│   ├── data_utils.py                   # Data helpers (timestamps, partition keys, dates)
│   ├── llm_utils.py                    # LLM API callers (DeepSeek, Gemini, OpenAI)
│   ├── storage.py                      # MinIO object storage utilities
│   ├── exceptions.py                   # Custom exceptions theo layer (E/T/L/LLM)
│   └── logger.py                       # Logging configuration
│
├── grafana/                            # Grafana configs
│   ├── dashboards/                     # Dashboard JSON definitions
│   └── provisioning/                   # Datasource & dashboard provisioning
│
├── data/                               # Data Lake (local cache, synced to MinIO)
│   ├── raw/                            # Dữ liệu thô: klines/CSV, ticker/Parquet, order_book/Parquet
│   └── processed/                      # Dữ liệu đã xử lý: klines/Parquet (có RSI + MACD)
│
└── docs/
    └── ProjectOverview.md              # Tài liệu chi tiết dự án
```

## Công nghệ sử dụng

| Layer | Công nghệ | Mục đích |
|-------|-----------|----------|
| **Extract** | Python + Binance API | Thu thập dữ liệu nến 1-min |
| **Transform** | ClickHouse SQL | Tính RSI(14), MACD(12/26/9) (in-DB) |
| **Load** | ClickHouse | Data Warehouse (columnar, fast analytics) |
| **Store** | MinIO (S3-compatible) | Object Storage cho Data Lake |
| **Orchestrate** | Apache Airflow | Tự động hóa ETL jobs |
| **Chat** | LangGraph + DeepSeek/Gemini/OpenAI | AI Reasoning Chat Assistant |
| **Visualize** | Grafana | Dashboard real-time + embedded chatbox |
| **Testing** | pytest | Unit tests + LLM integration eval |

## Khởi chạy

```bash
# 1. Copy và cấu hình biến môi trường
cp .env.example .env

# 2. Khởi chạy toàn bộ hệ thống
docker compose up -d 

# 3. Truy cập các services:
#    Grafana (+ AI Chatbox):  http://localhost:3000
#    Chat API:                http://localhost:8501/chat-ui?symbol=BTCUSDT
#    Airflow UI:              http://localhost:8080
#    MinIO Console:           http://localhost:9001
```

ETL pipeline sau đó chạy tự động qua Airflow DAG (`minutely_etl`) (mở Airflow UI và trigger DAG) — không cần chạy script thủ công.

## Testing

```bash
# Cài dependencies cho testing
pip install -r requirements.txt

# Chạy toàn bộ unit tests (offline, không cần API key)
pytest tests/test_market_queries.py tests/test_nodes.py tests/test_chat_api.py -v

# Chạy AI evaluation tests (cần LLM_API_KEY trong .env)
pytest tests/test_ai_eval.py -v

# Chạy tất cả tests
pytest -v
```

| Test Suite | File | Mô tả |
|---|---|---|
| Market Queries | `test_market_queries.py` | Format candles, ticker, orderbook, SQL queries |
| LangGraph Nodes | `test_nodes.py` | LLM factory, router, load/save history, tools |
| Chat API | `test_chat_api.py` | FastAPI endpoints (chat, history, health) |
| AI Evaluation | `test_ai_eval.py` | LLM reasoning: tool calling, timeframe, language |

## Tài liệu chi tiết

Xem [docs/ProjectOverview.md](docs/ProjectOverview.md)
