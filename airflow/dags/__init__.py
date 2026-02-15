# =============================================================================
# Airflow DAGs Package - Crypto Data Pipeline
# =============================================================================
# Chứa các DAG files:
#   - minutely_extract.py: ETL klines mỗi phút (* * * * *)
#   - daily_snapshot.py: Ticker 24h + Order Book (0 0 * * *)
#   - weekly_retrain.py: Train lại model (03:00 AM Chủ Nhật)
#   - hourly_inference.py: Dự báo mỗi giờ (0 * * * *)
# =============================================================================
