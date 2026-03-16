"""Logging configuration for the Crypto Data Pipeline.

Usage in any module:
    from utils.logger import get_logger
    logger = get_logger(__name__)
"""

import logging
import sys
from pathlib import Path


_LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_CONFIGURED = False


def setup_logging(
    level: str = "INFO",
    log_file: str | None = None,
) -> None:
    """Configure logging once for the entire process.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: If provided, logs are also written to this file path.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(console)

    # File handler (optional)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
        root.addHandler(file_handler)

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a logger instance for the given module.

    Automatically calls setup_logging() with defaults if not yet configured.
    """
    if not _CONFIGURED:
        import os
        setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
    return logging.getLogger(name)
