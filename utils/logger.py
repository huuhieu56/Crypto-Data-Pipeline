# =============================================================================
# Logger - Crypto Data Pipeline
# =============================================================================
# Cau hinh logging chuan cho toan bo project.
# Moi module goi: from utils.logger import get_logger
#                  logger = get_logger(__name__)
# =============================================================================

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
    """Cau hinh logging mot lan duy nhat cho toan bo process.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Neu truyen path, se ghi them log ra file.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # --- Console handler ---
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(console)

    # --- File handler (optional) ---
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
        root.addHandler(file_handler)

    # Giam log cua thu vien ben thu ba
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Tra ve logger instance cho module.

    Neu setup_logging() chua duoc goi, se tu dong goi voi gia tri mac dinh.
    """
    if not _CONFIGURED:
        import os
        setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
    return logging.getLogger(name)
