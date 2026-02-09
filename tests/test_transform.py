# =============================================================================
# Test Transform Module
# =============================================================================
# Unit tests cho scripts/transform.py
#
# Chạy: pytest tests/test_transform.py -v
# =============================================================================

# TODO: Import pytest và transform module

# TODO: Test calculate_rsi()
# - Test với sample data
# - Verify RSI range [0, 100]
# - Test edge cases (all gains, all losses)

# TODO: Test calculate_macd()
# - Test với sample data
# - Verify MACD và Signal values

# TODO: Test transform_data()
# - Test full pipeline với small dataset
# - Verify output schema


import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession

# ====================================================
# 1. TỰ ĐỘNG TÌM ĐƯỜNG DẪN GỐC (Fix lỗi FileNotFound)
# ====================================================
# Lấy đường dẫn tuyệt đối của file test.py hiện tại
CURRENT_FILE = Path(__file__).resolve()

# Tìm thư mục gốc dự án (Project Root)
# test.py nằm trong 'tests/', nên cần lùi lại 1 cấp (.parent) để ra 'tests'
# rồi lùi thêm 1 cấp nữa (.parent) để ra thư mục gốc dự án
PROJECT_ROOT = CURRENT_FILE.parent.parent

# Tạo đường dẫn chính xác tới file parquet
PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "features.parquet"

print(f"--- DEBUG INFO ---")
print(f"Project Root: {PROJECT_ROOT}")
print(f"Data Path:    {PARQUET_PATH}")

# ====================================================
# 2. KHỞI TẠO SPARK (Fix lỗi Crash)
# ====================================================
spark = SparkSession.builder \
    .appName("TestParquet") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

try:
    # Kiểm tra xem file có tồn tại không trước khi đọc
    if not PARQUET_PATH.exists():
        print(f"\n[ERROR] Không tìm thấy file tại: {PARQUET_PATH}")
        print("Gợi ý: Hãy chạy script 'transform.py' trước để tạo dữ liệu.")
    else:
        # Đọc file parquet (chuyển Path thành string)
        df = spark.read.parquet(str(PARQUET_PATH))
        
        print("\n--- SCHEMA ---")
        df.printSchema()
        
        print("\n--- SAMPLE DATA ---")
        # Hiển thị 5 dòng, bao gồm các cột quan trọngpy
        df.show(5, vertical=True, truncate=False)
        
        print(f"Total records: {df.count():,}")

except Exception as e:
    print(f"Lỗi: {e}")
finally:
    spark.stop()