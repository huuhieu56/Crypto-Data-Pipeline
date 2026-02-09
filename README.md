# Crypto Data Pipeline - Big Data & Deep Learning (LSTM)

## Mô tả dự án

Hệ thống End-to-End Data Pipeline thu thập, xử lý và dự báo giá cryptocurrency
sử dụng Apache Spark, Apache Airflow, PostgreSQL, PyTorch LSTM và Grafana.

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
│   ├── extract.py                      # Thu thập từ Binance API & Data Vision
│   ├── transform.py                    # Xử lý với Spark (RSI, MACD)
│   ├── load.py                         # Ghi vào PostgreSQL (Pandas + Spark JDBC)
│   ├── train.py                        # Huấn luyện LSTM
│   ├── inference.py                    # Chạy dự báo
│   └── update_actuals.py               # Cập nhật giá thực tế
│
├── airflow/                            # Apache Airflow
│   ├── __init__.py
│   └── dags/
│       ├── __init__.py
│       ├── daily_etl.py                # DAG ETL hàng ngày (02:00 AM)
│       ├── weekly_retrain.py           # DAG train lại model (Chủ Nhật)
│       └── hourly_inference.py         # DAG dự báo mỗi giờ
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
│   ├── binance_utils.py                # Binance API wrappers (retry, rate limit)
│   ├── db_utils.py                     # Database utilities (SQLAlchemy, Spark JDBC)
│   ├── data_utils.py                   # Data processing utilities (TODO)
│   ├── exceptions.py                   # Custom exceptions theo layer (E/T/L)
│   └── logger.py                       # Logging configuration
│
└── docs/
    └── ProjectOverview.md              # Tài liệu chi tiết dự án
```

## Công nghệ sử dụng

- **Extract**: Python + Binance API
- **Transform**: Apache Spark
- **Load**: PostgreSQL
- **Orchestrate**: Apache Airflow
- **Train**: PyTorch LSTM
- **Visualize**: Grafana

## Khởi chạy

```bash
# 1. Start Docker services
docker compose up -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Run initial ETL
python scripts/extract.py
python scripts/transform.py
python scripts/load.py
```
