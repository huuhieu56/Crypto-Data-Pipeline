# =============================================================================
# Custom Exceptions - Crypto Data Pipeline
# =============================================================================
# Dinh nghia cac exception rieng de phan biet loi theo tang (layer).
# Moi module raise exception cu the thay vi Exception chung.
# =============================================================================


class PipelineError(Exception):
    """Base exception cho toan bo pipeline."""


# ---------------------------------------------------------------------------
# Extract layer
# ---------------------------------------------------------------------------
class ExtractError(PipelineError):
    """Loi xay ra trong qua trinh thu thap du lieu tu Binance."""


class APIRequestError(ExtractError):
    """Loi HTTP khi goi Binance API."""

    def __init__(self, endpoint: str, status_code: int | None = None, detail: str = ""):
        self.endpoint = endpoint
        self.status_code = status_code
        self.detail = detail
        msg = f"API request failed: {endpoint}"
        if status_code:
            msg += f" (HTTP {status_code})"
        if detail:
            msg += f" - {detail}"
        super().__init__(msg)


# ---------------------------------------------------------------------------
# Transform layer
# ---------------------------------------------------------------------------
class TransformError(PipelineError):
    """Loi xay ra trong qua trinh Transform voi Spark."""


# ---------------------------------------------------------------------------
# Load layer
# ---------------------------------------------------------------------------
class LoadError(PipelineError):
    """Loi xay ra trong qua trinh Load vao PostgreSQL."""


class DatabaseConnectionError(LoadError):
    """Khong ket noi duoc den PostgreSQL."""


class SchemaInitError(LoadError):
    """Loi khi khoi tao schema."""


# ---------------------------------------------------------------------------
# Model / Inference layer
# ---------------------------------------------------------------------------
class ModelError(PipelineError):
    """Loi lien quan den LSTM model."""


class ModelNotFoundError(ModelError):
    """Khong tim thay file model .pth."""
