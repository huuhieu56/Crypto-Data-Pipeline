# Crypto Data Pipeline - Big Data & LLM Advisor

## Mô tả dự án

Hệ thống End-to-End Data Pipeline thu thập, xử lý và phân tích thị trường cryptocurrency
sử dụng Apache Spark, Apache Airflow, ClickHouse, MinIO, LLM (Gemini/OpenAI) và Grafana.

- **Extract hàng ngày**: Gọi Binance REST API + Data Vision lấy nến 1-min cho 50 coins
- **Transform**: Spark tính RSI(14), MACD(12/26/9) trên 1-min candles
- **Load**: Ghi vào ClickHouse (clickhouse-connect)
- **LLM Advisory mỗi giờ**: Phân tích 30 nến daily + snapshot → BUY/SELL/HOLD signals
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
│   ├── llm_config.py                   # LLM config (provider, model, params)
│   └── symbols.py                      # Danh sách 50 coins (single source of truth)
│
├── scripts/                            # Scripts chính ETL & LLM
│   ├── __init__.py
│   ├── pre_extract.py                  # Self-healing gap detection + recovery
│   ├── extract.py                      # Thu thập từ Binance API & Data Vision
│   ├── transform.py                    # Xử lý với Spark (RSI, MACD trên 1-min)
│   ├── load.py                         # Ghi vào ClickHouse (clickhouse-connect)
│   └── llm_signal.py                   # Sinh tín hiệu BUY/SELL/HOLD từ LLM
│
├── airflow/                            # Apache Airflow
│   ├── __init__.py
│   └── dags/
│       ├── __init__.py
│       ├── daily_etl.py                # DAG ETL klines + ticker hàng ngày (0 2 * * *)
│       ├── daily_snapshot.py           # DAG ticker_24h + order_book (0 0 * * *)
│       └── hourly_inference.py         # DAG LLM advisory signals mỗi giờ (0 * * * *)
│
├── sql/                                # Database schemas & queries
│   ├── schema.sql                      # ClickHouse schema (Star Schema: 1 dim + 4 fact)
│   ├── queries.sql                     # Query mẫu cho Grafana
│   └── migrate_remove_predictions_clickhouse.sql  # Migration: predictions → llm_signals
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
│   ├── llm_utils.py                    # LLM API callers (Gemini, OpenAI), JSON parser
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
| **Orchestrate** | Apache Airflow | Tự động hóa ETL + LLM jobs |
| **Advise** | LLM (Gemini / OpenAI) | Tín hiệu BUY/SELL/HOLD |
| **Visualize** | Grafana | Dashboard real-time |

## Khởi chạy

```bash
# 1. Start Docker services (ClickHouse, MinIO, Airflow, Grafana)
docker compose up -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Run pre-extract (self-healing: detect gaps, bulk download)
python scripts/pre_extract.py

# 4. Run initial ETL
python scripts/extract.py
python scripts/transform.py
python scripts/load.py

# 5. Run LLM advisory signals (manual test)
python scripts/llm_signal.py --dry-run

# 6. Airflow tự động: ETL hàng ngày, LLM advisory mỗi giờ
# Truy cập Airflow UI: http://localhost:8080
# Truy cập Grafana:    http://localhost:3000
# Truy cập MinIO:      http://localhost:9001
```

## Tài liệu chi tiết

Xem [docs/ProjectOverview.md](docs/ProjectOverview.md)