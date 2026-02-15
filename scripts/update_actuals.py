# =============================================================================
# Update Actuals Script - Cập nhật giá thực tế và tính error
# =============================================================================
# Chức năng:
#   1. Lấy predictions chưa có actual_close mà target_time đã qua
#   2. Query giá close thực tế (nến 1-min) từ bảng klines
#   3. Cập nhật actual_close và error_pct vào bảng predictions
#
# Timing:
#   - Chạy cuối hourly_inference DAG (sau save_predictions)
#   - Cập nhật các dự báo cũ có target_time <= now() (phút đã qua)
#
# Sử dụng:
#   python scripts/update_actuals.py
# =============================================================================

# TODO: Import libraries

# TODO: Implement get_pending_predictions()
# - Query predictions có actual_close IS NULL
# - Filter: target_time <= NOW() (phút đã qua)

# TODO: Implement get_actual_prices()
# - Query close từ bảng klines
# - Match với target_time (timestamp) của predictions

# TODO: Implement update_predictions()
# - UPDATE actual_close và error_pct
# - error_pct = ABS(actual - predicted) / actual * 100

# TODO: Implement main()
