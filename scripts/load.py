# =============================================================================
# Load Script - Ghi dữ liệu vào PostgreSQL
# =============================================================================
# Chức năng:
#   1. Load symbols: Ghi thông tin coins vào bảng symbols
#   2. Load klines: Ghi dữ liệu nến + features vào bảng klines
#   3. Load ticker_24h: Ghi thống kê 24h vào bảng ticker_24h
#
# Input:
#   - data/raw/symbols.json
#   - data/processed/features.parquet
#   - data/raw/ticker_24h.csv
#
# Output: PostgreSQL tables (symbols, klines, ticker_24h)
# Lưu ý:
#   - Data Types: Các cột giá (price) và spread của các coin như SHIB, PEPE 
#     có giá trị cực nhỏ (e.g., 6.42e-06). 
#         => BẮT BUỘC dùng kiểu dữ liệu NUMERIC hoặc DECIMAL(20, 10) trong Postgres.
#         => TRÁNH dùng FLOAT/REAL nếu cần độ chính xác tuyệt đối cho tiền tệ.
#   - Đảm bảo khi chạy lại script không bị trùng lặp dữ liệu (Duplicate).
#     Sử dụng logic "UPSERT" (INSERT ON CONFLICT).
#   - Performance: Với bảng klines (~78M records), cần đánh Index cho cột 
#     (symbol, timestamp) để tối ưu tốc độ Query cho Grafana.
#
# Sử dụng:
#   python scripts/load.py
# =============================================================================

# TODO: Import libraries (psycopg2, sqlalchemy, pandas)

# TODO: Implement get_db_connection()
# - Tạo connection đến PostgreSQL

# TODO: Implement load_symbols()
# - Đọc symbols.json
# - Insert/Update vào bảng symbols

# TODO: Implement load_klines()
# - Đọc features.parquet
# - Append vào bảng klines

# TODO: Implement load_ticker_24h()
# - Đọc ticker_24h.csv
# - Note: Ép kiểu dữ liệu (Data Type Casting) rõ ràng trước khi ghi vào DB để tránh Postgres hiểu nhầm scientific notation (e.g., 4e-07) là string.
# - Append vào bảng ticker_24h

# TODO: Implement main()




# =============================================================================
# Load Script - Ghi dữ liệu vào PostgreSQL
# =============================================================================
# =============================================================================
# Load Script - Ghi dữ liệu vào PostgreSQL (Sử dụng Spark)
# =============================================================================

import os
import sys
import json
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

# 1. CẤU HÌNH (CONFIGURATION)

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
SQL_DIR = PROJECT_ROOT / "sql"

# Cấu hình Database
DB_CONFIG = {
    "user": "crypto123az",
    "password": "crypto123",
    "host": "localhost",
    "port": "5432",
    "dbname": "crypto_db"
}
# URL cho SQLAlchemy 
DB_URL = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

# URL cho Spark JDBC 
JDBC_URL = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"



def init_spark(app_name="CryptoLoad"):
    """
    Khởi tạo SparkSession có kèm JDBC Driver để ghi vào Postgres.
    Lưu ý: Bạn cần tải file jar 'postgresql-42.x.x.jar' nếu chưa có, 
    nhưng để đơn giản ta sẽ dùng gói maven có sẵn.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def get_engine():
    try:
        return create_engine(DB_URL)
    except Exception as e:
        print(f"Lỗi kết nối Database: {e}")
        sys.exit(1)

def init_db(engine):
    """Chạy file sql/schema.sql"""
    schema_path = SQL_DIR / "schema.sql"
    if not schema_path.exists(): return
    
    print("--- Init Database Schema ---")
    with open(schema_path, "r") as f:
        sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print("✅ Schema initialized.")

def upsert_method(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert
    
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    table_name = table.table.name
    
    if table_name == 'symbols':
        index_elements = ['symbol']
    elif table_name == 'ticker_24h':
        index_elements = ['symbol', 'snapshot_time']
    elif table_name == 'order_book_snapshot':
        index_elements = ['symbol', 'timestamp'] 
    else:
        index_elements = [c.name for c in table.table.primary_key.columns]

    if not index_elements:
        raise ValueError(f"Table {table_name} has no primary key defined for upsert!")

    stmt = insert(table.table).values(data)
    stmt = stmt.on_conflict_do_nothing(index_elements=index_elements)
    conn.execute(stmt)

def load_symbols_pandas(engine):
    """File nhỏ (JSON) -> Dùng Pandas cho tiện"""
    json_path = RAW_DATA_DIR / "symbols.json"
    if not json_path.exists(): return

    print(f"Loading Symbols from {json_path.name}...")
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, dict) and "symbols" in data:
            df = pd.DataFrame(data["symbols"])
        else:
            df = pd.DataFrame(data)

       
        rename_map = {
            'base asset': 'base_asset', 'quote asset': 'quote_asset',
            'baseAsset': 'base_asset', 'quoteAsset': 'quote_asset'
        }
        df = df.rename(columns=rename_map)
        valid_cols = ['symbol', 'base_asset', 'quote_asset', 'status']
        df = df[[c for c in valid_cols if c in df.columns]]
        
        df.to_sql('symbols', engine, if_exists='append', index=False, method=upsert_method)
        print(f"✅ Loaded {len(df)} symbols.")
    except Exception as e:
        print(f"⚠️ Error loading symbols: {e}")

def load_klines_spark(spark):
    """
    File LỚN (Parquet) -> Dùng SPARK để đọc và ghi vào DB
    """
    parquet_path = str(PROCESSED_DATA_DIR / "features.parquet")
    print(f"Loading Klines from {parquet_path} using Spark...")
    
    try:
        # 1. Đọc Parquet bằng Spark
        df = spark.read.parquet(parquet_path)
        
        # 2. Định dạng tên giống với column bên postgre
        df = df.withColumnRenamed("open_time", "timestamp") \
               .withColumnRenamed("RSI", "rsi_14") \
               .withColumnRenamed("MACD", "macd") \
               .withColumnRenamed("MACD_signal", "macd_signal")
        
        # 3. Chọn đúng các cột cần thiết
        target_cols = [
            'symbol', 'timestamp', 'open', 'high', 'low', 'close', 
            'volume', 'quote_volume', 'trades', 'rsi_14', 'macd', 'macd_signal'
        ]
        # Lọc cột (chỉ lấy những cột có trong df)
        existing_cols = [c for c in target_cols if c in df.columns]
        df_final = df.select(*existing_cols)
        
        print(f"Start writing {df_final.count():,} records to PostgreSQL via JDBC...")
        db_properties = {
            "user": DB_CONFIG['user'],
            "password": DB_CONFIG['password'],
            "driver": "org.postgresql.Driver",
            "batchsize": "10000" # Tăng tốc độ ghi
        }
        
        # Ghi vào bảng klines
        # mode="append": Thêm vào đuôi. Nếu trùng PK -> Exception.
        df_final.write.jdbc(
            url=JDBC_URL,
            table="klines",
            mode="append", 
            properties=db_properties
        )
        print(" Klines loaded successfully via Spark!")
        
    except Exception as e:
        # Lỗi thường gặp: Duplicate Key.
        if "duplicate key value" in str(e).lower():
            print("Warning: Một số dữ liệu đã tồn tại (Duplicate Key). Spark đã dừng ghi.")
        else:
            print(f"Error loading klines with Spark: {e}")

def load_ticker_pandas(engine):
    csv_path = RAW_DATA_DIR / "ticker_24h.csv"
    if csv_path.exists():
        print(f"Loading Ticker form {csv_path.name}...")
        df = pd.read_csv(csv_path)
        # Ép kiểu số
        for col in ['price_change', 'high_24h', 'low_24h', 'bid_price', 'ask_price']:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
            
        df.to_sql('ticker_24h', engine, if_exists='append', index=False, method=upsert_method)
        print(f"Loaded {len(df)} tickers.")

def load_orderbook_pandas(engine):
    csv_path = RAW_DATA_DIR / "order_book_snapshot.csv"
    if csv_path.exists():
        print(f"Loading Orderbook form {csv_path.name}...")
        df = pd.read_csv(csv_path)
        if 'timestamp' in df.columns: df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.to_sql('order_book_snapshot', engine, if_exists='append', index=False, method=upsert_method)
        print(f"✅ Loaded {len(df)} orderbooks.")

# =============================================================================
# 4. MAIN
# =============================================================================
def main():
    # 1. Init DB Structure (Dùng SQLAlchemy engine)
    engine = get_engine()
    init_db(engine)
    
    # 2. Load các bảng nhỏ bằng Pandas 
    load_symbols_pandas(engine)
    load_ticker_pandas(engine)
    load_orderbook_pandas(engine)
    
    # 3. Load bảng lớn bằng Spark
    spark = init_spark()
    try:
        load_klines_spark(spark)
    finally:
        spark.stop()
    
    print("\n All processes finished!")

if __name__ == "__main__":
    main()