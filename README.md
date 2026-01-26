# Crypto Data Pipeline - Big Data & Deep Learning (LSTM)

## Mô tả dự án

Hệ thống End-to-End Data Pipeline thu thập, xử lý và dự báo giá cryptocurrency
sử dụng Apache Spark, Apache Airflow, PostgreSQL, PyTorch LSTM và Grafana.

## Cấu trúc thư mục

```
Crypto-Data-Pipeline/
├── README.md                           # Tài liệu dự án
├── .gitignore                          # Git ignore rules
├── requirements.txt                    # Python dependencies
├── docker-compose.yml                  # Docker services config
│
├── config/                             # Cấu hình hệ thống
│   ├── __init__.py
│   ├── config.py                       # Main configuration
│   └── symbols.py                      # Danh sách 50 coins
│
├── scripts/                            # Scripts chính ETL & ML
│   ├── __init__.py
│   ├── extract.py                      # Thu thập từ Binance API
│   ├── transform.py                    # Xử lý với Spark
│   ├── load.py                         # Ghi vào PostgreSQL
│   ├── train.py                        # Huấn luyện LSTM
│   ├── inference.py                    # Chạy dự báo
│   └── update_actuals.py               # Cập nhật giá thực tế
│
├── airflow/                            # Apache Airflow
│   ├── __init__.py
│   └── dags/
│       ├── __init__.py
│       ├── daily_etl.py                # DAG ETL hàng ngày
│       ├── weekly_retrain.py           # DAG train lại model
│       └── hourly_inference.py         # DAG dự báo mỗi giờ
│
├── sql/                                # Database schemas
│   ├── init_db.sql                     # Khởi tạo database
│   ├── schema.sql                      # Tạo bảng (Star Schema)
│   └── queries.sql                     # Query mẫu cho Grafana
│
├── models/                             # Trained models
│   ├── .gitkeep
│   └── model.py                        # LSTM model definition
│
├── data/                               # Data Lake
│   ├── raw/.gitkeep                    # Dữ liệu thô (CSV)
│   └── processed/.gitkeep              # Dữ liệu đã xử lý (Parquet)
│
├── grafana/                            # Grafana configs
│   ├── dashboards/.gitkeep
│   └── provisioning/
│       ├── datasources/datasources.yml
│       └── dashboards/dashboards.yml
│
├── notebooks/.gitkeep                  # Jupyter notebooks cho EDA
│
├── tests/                              # Unit tests
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_model.py
│
├── utils/                              # Utility functions
│   ├── __init__.py
│   ├── db_utils.py                     # Database utilities
│   ├── binance_utils.py                # Binance API utilities
│   ├── data_utils.py                   # Data processing utilities
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
docker-compose up -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Run initial ETL
python scripts/extract.py
python scripts/transform.py
python scripts/load.py
```
