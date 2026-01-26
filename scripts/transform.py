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
