# =============================================================================
# Transform Script - Xử lý dữ liệu với Apache Spark
# =============================================================================
# Chức năng:
#   1. Đọc dữ liệu raw CSV từ Data Lake
#   2. Tính toán chỉ số kỹ thuật: RSI(14), MACD, MACD Signal
#   3. Xử lý missing values (forward fill)
#   4. Lưu kết quả dạng Parquet
#
# Input: data/raw/*.csv
# Output: data/processed/features.parquet
#
# Sử dụng:
#   python scripts/transform.py
# =============================================================================

# TODO: Import PySpark libraries

# TODO: Implement init_spark()
# - Tạo SparkSession với cấu hình phù hợp

# TODO: Implement calculate_rsi()
# - Tính RSI(14) sử dụng Window Function
# - RSI = 100 - 100/(1 + RS)

# TODO: Implement calculate_macd()
# - Tính MACD = EMA(12) - EMA(26)
# - Tính MACD Signal = EMA(9) của MACD

# TODO: Implement transform_data()
# - Đọc tất cả CSV files
# - Gọi calculate_rsi(), calculate_macd()
# - Xử lý missing values
# - Lưu Parquet

# TODO: Implement main()

# =============================================================================
# Transform Script - Fix lỗi __file__ & Update Schema chuẩn (Có cột trades)
# =============================================================================
import os
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import numpy as np

# =============================================================================
# 1. Configuration & Path
# =============================================================================
# Sử dụng __file__ (2 dấu gạch dưới) để lấy đường dẫn script hiện tại
# Giả định cấu trúc folder: /project_root/scripts/transform.py
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent 

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# 2. Schema Definitions
# =============================================================================

# tao schema dau vao giong voi raw data
INPUT_SCHEMA = StructType([
    StructField("open_time", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("close_time", TimestampType(), True),
    StructField("quote_volume", DoubleType(), True),
    StructField("trades", LongType(), True),         
    StructField("taker_buy_base", DoubleType(), True),
    StructField("taker_buy_quote", DoubleType(), True),
    StructField("symbol", StringType(), True)
])

# Schema đầu ra (Input cũ + Các chỉ báo mới bạn yêu cầu)
OUTPUT_SCHEMA = INPUT_SCHEMA \
    .add("RSI", DoubleType(), True) \
    .add("MACD", DoubleType(), True) \
    .add("MACD_signal", DoubleType(), True)

# Khoi tao spark seasion la diem entry point tao data frame

def init_spark(app_name="CryptoTransform"):
    """Khởi tạo SparkSession tối ưu cho xử lý cục bộ với Arrow."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()

# Hàm tính toán chỉ báo bằng Pandas 
def calculate_indicators_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    # 1. Sort để đảm bảo tính toán đúng thứ tự thời gian
    pdf = pdf.sort_values("open_time")
    
    close = pdf["close"]
    
    # --- Tính RSI (14) ---
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    
    loss = loss.replace(0, np.nan)
    rs = gain / loss
    
    # Công thức RSI
    pdf["RSI"] = 100 - (100 / (1 + rs))
    
    # --- Tính MACD (12, 26, 9) ---
    # adjust=False là tiêu chuẩn cho phân tích kỹ thuật crypto
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    
    pdf["MACD"] = ema12 - ema26
    pdf["MACD_signal"] = pdf["MACD"].ewm(span=9, adjust=False).mean()
    
    # ---Fill dư liệu cho những dòng bị NaN
    pdf = pdf.ffill().bfill()
    
    # Fill 0 cho trường hợp dữ liệu quá ngắn không tính được indicator
    pdf = pdf.fillna(0)
    
    return pdf

def transform_data(spark: SparkSession):
    print(f"--- Processing data from: {RAW_DATA_DIR} ---")
    
    # Đọc tất cả file CSV trong thư mục raw (*.csv)
    raw_path = str(RAW_DATA_DIR / "*.csv")
    
    try:
        df = spark.read.csv(raw_path, header=True, schema=INPUT_SCHEMA)
    except Exception:
        print(f"ERROR: Không tìm thấy file csv nào tại {raw_path}")
        return None
    
    # Lọc bỏ dữ liệu Null
    df = df.filter(F.col("open_time").isNotNull())
    
    print("Calculating RSI & MACD using Pandas UDF...")
    
    # Apply Pandas UDF theo từng Symbol
    processed_df = df.groupBy("symbol").applyInPandas(
        calculate_indicators_pandas, 
        schema=OUTPUT_SCHEMA
    )
    
    # Lưu Parquet 
    output_path = str(PROCESSED_DATA_DIR / "features.parquet")
    print(f"Saving to: {output_path}")
    
    processed_df.write \
        .mode("overwrite") \
        .partitionBy("symbol") \
        .parquet(output_path)
        
    return output_path


if __name__ == "__main__":
    spark = init_spark()
    try:
        out_file = transform_data(spark)
        
        if out_file:
            # Verify kết quả
            print("\n--- Preview Result ---")
            res = spark.read.parquet(out_file)
            
            # Chọn các cột quan trọng để hiển thị kiểm tra
            display_cols = ["open_time", "symbol", "close", "trades", "RSI", "MACD", "MACD_signal"]
            res.select(display_cols).show(5)
            
            print(f"Total records: {res.count():,}")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()