# Crypto Data Pipeline - Big Data & Deep Learning (LSTM)

## Mô tả dự án

Hệ thống End-to-End Data Pipeline thu thập, xử lý và dự báo giá cryptocurrency
sử dụng Apache Spark, Apache Airflow, PostgreSQL, PyTorch LSTM và Grafana.

- **Extract mỗi phút**: Gọi Binance REST API lấy nến 1-min cho 50 coins
- **Transform**: Spark tính RSI(14), MACD(12/26/9) trên 1-min candles
- **Inference mỗi giờ**: LSTM dùng 360 nến (6h) → dự báo 60 nến tới (1h)
- **Retrain hàng tuần**: Train lại model trên toàn bộ dữ liệu lịch sử

## Cấu trúc thư mục

```
Crypto-Data-Pipeline/
├── .env                                # Biến môi trường (không commit)
├── .env.example                        # Mẫu biến môi trường
├── .gitignore                          # Git ignore rules
├── README.md                           # Tài liệu dự án
├── requirements.txt                    # Python dependencies
├── docker-compose.yml                  # Docker services config
│
├── config/                             # Cấu hình hệ thống
│   ├── __init__.py
│   ├── config.py                       # Main configuration (paths, DB, API, Spark, Model)
│   └── symbols.py                      # Danh sách 50 coins
│
├── scripts/                            # Scripts chính ETL & ML
│   ├── __init__.py
│   ├── pre_extract.py                  # Self-healing gap detection + recovery (1 lần setup)
│   ├── extract.py                      # Thu thập từ Binance API & Data Vision
│   ├── transform.py                    # Xử lý với Spark (RSI, MACD trên 1-min)
│   ├── load.py                         # Ghi vào PostgreSQL (Spark JDBC upsert)
│   ├── train.py                        # Huấn luyện LSTM (360→60, 1-min candles)
│   ├── inference.py                    # Chạy dự báo mỗi giờ (360→60)
│   └── update_actuals.py               # Cập nhật giá thực tế
│
├── airflow/                            # Apache Airflow
│   ├── __init__.py
│   └── dags/
│       ├── __init__.py
│       ├── minutely_extract.py         # DAG ETL klines mỗi phút (* * * * *)
│       ├── daily_snapshot.py           # DAG ticker_24h + order_book (0 0 * * *)
│       ├── weekly_retrain.py           # DAG train lại model (Chủ Nhật)
│       └── hourly_inference.py         # DAG dự báo mỗi giờ (0 * * * *)
│
├── sql/                                # Database schemas & queries
│   ├── init_db.sql                     # Khởi tạo database
│   ├── schema.sql                      # Tạo bảng (Star Schema: 1 dim + 4 fact)
│   └── queries.sql                     # Query mẫu cho Grafana
│
├── models/                             # Model definitions & trained weights
│   ├── .gitkeep
│   └── model.py                        # LSTM model definition (PyTorch)
│
├── data/                               # Data Lake
│   ├── raw/.gitkeep                    # Dữ liệu thô (CSV)
│   └── processed/.gitkeep              # Dữ liệu đã xử lý (Parquet)
│
├── grafana/                            # Grafana configs
│   ├── dashboards/
│   │   └── .gitkeep
│   └── provisioning/
│       ├── datasources/
│       │   └── datasources.yml         # PostgreSQL datasource
│       └── dashboards/
│           └── dashboards.yml          # Dashboard provisioning
│
├── notebooks/.gitkeep                  # Jupyter notebooks cho EDA
│
├── tests/                              # Unit tests
│   ├── __init__.py
│   ├── test_extract.py                 # Tests cho extract pipeline
│   ├── test_transform.py               # Tests cho transform pipeline
│   └── test_model.py                   # Tests cho LSTM model
│
├── utils/                              # Utility functions
│   ├── __init__.py
│   ├── binance_utils.py                # Binance API wrappers (retry, rate limit, parse)
│   ├── db_utils.py                     # Database utilities (SQLAlchemy, Spark JDBC, upsert)
│   ├── data_utils.py                   # Data helpers (timestamps, merge CSV, date utils)
│   ├── exceptions.py                   # Custom exceptions theo layer (E/T/L)
│   └── logger.py                       # Logging configuration
│
└── docs/
    └── ProjectOverview.md              # Tài liệu chi tiết dự án
```

## Công nghệ sử dụng

- **Extract**: Python + Binance REST API (mỗi phút)
- **Transform**: Apache Spark (RSI, MACD trên 1-min candles)
- **Load**: PostgreSQL (Spark JDBC upsert)
- **Orchestrate**: Apache Airflow (minutely + hourly + weekly)
- **Train**: PyTorch LSTM (360 nến → 60 nến)
- **Visualize**: Grafana

## Khởi chạy

```bash
# 1. Start Docker services
docker compose up -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Run pre-extract (self-healing: detect gaps, bulk download 3 năm)
python scripts/pre_extract.py

# 4. Run initial ETL (nặng lần đầu: 78M rows)
python scripts/extract.py
python scripts/transform.py
python scripts/load.py

# 5. Train model (cần đủ dữ liệu)
python scripts/train.py

# 6. Airflow tự động: extract mỗi phút, inference mỗi giờ
# Truy cập Airflow UI: http://localhost:8080
```

## Tài liệu chi tiết

Xem [docs/ProjectOverview.md](docs/ProjectOverview.md)