# Crypto Data Pipeline - Big Data & LLM Chat Assistant

## Mô tả dự án

Hệ thống End-to-End Data Pipeline thu thập, xử lý và phân tích thị trường cryptocurrency
sử dụng Apache Spark, Apache Airflow, ClickHouse, MinIO, LLM (Gemini/OpenAI) và Grafana.

- **Extract mỗi phút**: Gọi Binance REST API lấy nến 1-min, ticker 24h, order book cho 50 coins
- **Transform**: Spark tính RSI(14), MACD(12/26/9) trên 1-min candles
- **Load**: Ghi vào ClickHouse (clickhouse-connect)
- **LLM Chat Assistant**: Chatbox tương tác trên Grafana, tự động gắn 30 nến daily context
- **Visualize**: Grafana dashboard real-time

## Cấu trúc thư mục

```
Crypto-Data-Pipeline/
├── .env                                # Biến môi trường
├── .env.example                        # Mẫu biến môi trường
├── .gitignore                          # Git ignore rules
├── README.md                           # Tài liệu dự án
├── requirements.txt                    # Python dependencies
├── docker-compose.yml                  # Docker services config
│
├── config/                             # Cấu hình hệ thống
│   ├── __init__.py
│   ├── config.py                       # Main config (paths, DB, API, MinIO, Spark)
│   ├── llm_config.py                   # LLM config (provider, model, chat params)
│   └── symbols.py                      # Danh sách 50 coins (single source of truth)
│
├── scripts/                            # Scripts chính ETL
│   ├── __init__.py
│   ├── pre_extract.py                  # Self-healing gap detection + recovery
│   ├── extract.py                      # Thu thập từ Binance API & Data Vision
│   ├── transform.py                    # Xử lý với Spark (RSI, MACD trên 1-min)
│   └── load.py                         # Ghi vào ClickHouse (clickhouse-connect)
│
├── services/                           # Microservices
│   └── chat_api/                       # LLM Chat Assistant backend
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── main.py                     # FastAPI (GET /chat-ui, POST /api/chat)
│       └── chat_ui.html                # Chat interface (dark theme, iframe-ready)
│
├── airflow/                            # Apache Airflow
│   ├── __init__.py
│   └── dags/
│       ├── __init__.py
│       └── minutely_etl.py            # DAG Mini-batch ETL mỗi phút (* * * * *)
│
├── sql/                                # Database schemas & queries
│   ├── schema.sql                      # ClickHouse schema (Star Schema: 1 dim + 3 fact)
│   ├── queries.sql                     # Query mẫu cho Grafana
│   └── init_db.sql                     # Tạo database thủ công (ngoài Docker)
│
├── models/                             # Placeholder (reserved for future use)
│   └── .gitkeep
│
├── data/                               # Data Lake (local cache, synced to MinIO)
│   ├── raw/                            # Dữ liệu thô (CSV/Parquet)
│   └── processed/                      # Dữ liệu đã xử lý (Parquet)
│
├── grafana/                            # Grafana configs
│   ├── dashboards/                     # Dashboard JSON definitions
│   └── provisioning/                   # Datasource & dashboard provisioning
│       ├── datasources/
│       └── dashboards/
│
├── notebooks/                          # Jupyter notebooks cho EDA
│
├── tests/                              # Unit tests
│   ├── __init__.py
│   ├── test_extract.py                 # Tests cho extract pipeline
│   └── test_transform.py              # Tests cho transform pipeline
│
├── utils/                              # Utility functions
│   ├── __init__.py
│   ├── binance_utils.py                # Binance API wrappers (retry, rate limit, parse)
│   ├── db_utils.py                     # ClickHouse client, insert/query helpers
│   ├── data_utils.py                   # Data helpers (timestamps, partition keys, dates)
│   ├── llm_utils.py                    # LLM API callers (Gemini, OpenAI), chat mode
│   ├── storage.py                      # MinIO object storage utilities
│   ├── exceptions.py                   # Custom exceptions theo layer (E/T/L/LLM)
│   └── logger.py                       # Logging configuration
│
└── docs/
    └── ProjectOverview.md              # Tài liệu chi tiết dự án
```

## Công nghệ sử dụng

| Layer | Công nghệ | Mục đích |
|-------|-----------|----------|
| **Extract** | Python + Binance API | Thu thập dữ liệu nến 1-min |
| **Transform** | Apache Spark | Tính RSI(14), MACD(12/26/9) |
| **Load** | ClickHouse | Data Warehouse (columnar, fast analytics) |
| **Store** | MinIO (S3-compatible) | Object Storage cho Data Lake |
| **Orchestrate** | Apache Airflow | Tự động hóa ETL jobs |
| **Chat** | LLM (Gemini / OpenAI) + FastAPI | AI Chat Assistant với market context |
| **Visualize** | Grafana | Dashboard real-time + embedded chatbox |

## Khởi chạy

```bash
# 1. Start Docker services (ClickHouse, MinIO, Airflow, Chat API, Grafana)
docker compose up -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Run pre-extract (self-healing: detect gaps, bulk download)
python scripts/pre_extract.py

# 4. Run initial ETL
python scripts/extract.py
python scripts/transform.py
python scripts/load.py

# 5. Truy cập các services:
#    Grafana (+ AI Chatbox):  http://localhost:3000
#    Chat API:                http://localhost:8501/chat-ui?symbol=BTCUSDT
#    Airflow UI:              http://localhost:8080
#    MinIO Console:           http://localhost:9001
```

## Tài liệu chi tiết

Xem [docs/ProjectOverview.md](docs/ProjectOverview.md)