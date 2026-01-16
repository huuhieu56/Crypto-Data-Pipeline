"""Configuration settings for Cinema 360 ETL pipeline.

This module loads configuration from environment variables.
NEVER hardcode secrets or paths directly in code.
"""
import os
from typing import Optional


def get_env(key: str, default: Optional[str] = None) -> str:
    """Get environment variable with optional default.

    Args:
        key: The environment variable name.
        default: Default value if not found.

    Returns:
        The environment variable value.

    Raises:
        ValueError: If the variable is not set and no default provided.
    """
    value = os.environ.get(key, default)
    if value is None:
        raise ValueError(f"Environment variable '{key}' is not set.")
    return value


# --- HDFS Configuration ---
HDFS_NAMENODE_HOST: str = get_env("HDFS_NAMENODE_HOST", "namenode")
HDFS_NAMENODE_PORT: str = get_env("HDFS_NAMENODE_PORT", "9000")
HDFS_BASE_URL: str = f"hdfs://{HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}"

HDFS_RAW_IMDB_PATH: str = "/data/raw/imdb"
HDFS_RAW_TMDB_PATH: str = "/data/raw/tmdb"
HDFS_RAW_INTERNAL_PATH: str = "/data/raw/internal"
HDFS_PROCESSED_PATH: str = "/data/processed"

# --- PostgreSQL Configuration ---
POSTGRES_HOST: str = get_env("POSTGRES_HOST", "postgres")
POSTGRES_PORT: str = get_env("POSTGRES_PORT", "5432")
POSTGRES_DB: str = get_env("POSTGRES_DB", "cinema360")
POSTGRES_USER: str = get_env("POSTGRES_USER", "cinema360_user")
POSTGRES_PASSWORD: str = get_env("POSTGRES_PASSWORD", "")  # Must be set in .env
POSTGRES_JDBC_URL: str = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# --- MySQL (Internal Logs) Configuration ---
MYSQL_HOST: str = get_env("MYSQL_HOST", "mysql")
MYSQL_PORT: str = get_env("MYSQL_PORT", "3306")
MYSQL_DB: str = get_env("MYSQL_DB", "internal_logs")
MYSQL_USER: str = get_env("MYSQL_USER", "root")
MYSQL_PASSWORD: str = get_env("MYSQL_PASSWORD", "")

# --- TMDB API Configuration ---
TMDB_API_KEY: str = get_env("TMDB_API_KEY", "")
TMDB_API_BASE_URL: str = "https://api.themoviedb.org/3"
TMDB_DAILY_EXPORT_URL: str = "https://files.tmdb.org/p/exports"
TMDB_RATE_LIMIT_RPS: int = 50  # Requests per second

# --- Spark Configuration ---
SPARK_MASTER: str = get_env("SPARK_MASTER", "spark://spark-master:7077")
SPARK_APP_NAME: str = "Cinema360_ETL"
