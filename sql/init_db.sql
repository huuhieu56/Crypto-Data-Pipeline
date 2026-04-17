-- =============================================================================
-- Initialize Database - Crypto Data Warehouse (ClickHouse)
-- =============================================================================
-- ClickHouse tự tạo database qua schema.sql được mount vào Docker container.
-- File này chỉ dùng khi cần tạo database thủ công bên ngoài Docker.
--
-- Chạy qua ClickHouse client:
--   clickhouse-client --query "$(cat sql/init_db.sql)"
--
-- Hoặc qua HTTP API:
--   curl http://localhost:8123 --data-binary @sql/init_db.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS crypto_db;