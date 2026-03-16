"""Custom exception hierarchy for the Crypto Data Pipeline.

Each pipeline layer raises its own exception type for precise error handling.
All exceptions inherit from PipelineError.
"""


class PipelineError(Exception):
    """Base exception for the entire pipeline."""


# --- Extract Layer -----------------------------------------------------------

class ExtractError(PipelineError):
    """Error during data collection from Binance."""


class APIRequestError(ExtractError):
    """HTTP error when calling the Binance API."""

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


# --- Transform Layer ---------------------------------------------------------

class TransformError(PipelineError):
    """Error during Spark/Pandas transformation."""


# --- Load Layer --------------------------------------------------------------

class LoadError(PipelineError):
    """Error during data loading into PostgreSQL."""


class DatabaseConnectionError(LoadError):
    """Failed to connect to PostgreSQL."""


class SchemaInitError(LoadError):
    """Error during database schema initialization."""


# --- Model / Inference Layer -------------------------------------------------

class ModelError(PipelineError):
    """Error related to the LSTM model."""


class ModelNotFoundError(ModelError):
    """Model weights file (.pth) not found."""
