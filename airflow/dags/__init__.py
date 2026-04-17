# =============================================================================
# Airflow DAGs Package - Crypto Data Pipeline
# =============================================================================
# Chứa các DAG files:
#   - minutely_extract.py: ETL klines mỗi phút (* * * * *)
#   - daily_snapshot.py: Ticker 24h + Order Book (0 0 * * *)
#   - daily_etl.py: ETL gia tăng hằng ngày
#   - hourly_inference.py: Sinh tín hiệu LLM mỗi giờ (0 * * * *)
# =============================================================================
