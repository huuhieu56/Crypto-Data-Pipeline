# =============================================================================
# Update Actuals Script - Cập nhật giá thực tế và tính error
# =============================================================================
# Chức năng:
#   1. Lấy predictions từ 1 giờ trước chưa có actual_close
#   2. Query giá thực tế từ bảng klines
#   3. Cập nhật actual_close và error_pct vào bảng predictions
#
# Timing:
#   - Chạy sau inference 1 giờ để có đủ 60 nến thực tế
#   - Schedule bởi Airflow hourly_inference DAG
#
# Sử dụng:
#   python scripts/update_actuals.py
# =============================================================================

# TODO: Import libraries

# TODO: Implement get_pending_predictions()
# - Query predictions có actual_close IS NULL
# - Filter những predictions đã qua target_time

# TODO: Implement get_actual_prices()
# - Query close prices từ bảng klines
# - Match với target_time của predictions

# TODO: Implement update_predictions()
# - UPDATE actual_close và error_pct
# - error_pct = ABS(actual - predicted) / actual * 100

# TODO: Implement main()
