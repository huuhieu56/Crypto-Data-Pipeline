# =============================================================================
# Test Model Module (Updated for Phase 1 improvements)
# =============================================================================
# Unit tests cho models/model.py, utils/data_utils.py (ML helpers)
#
# Chạy: pytest tests/test_model.py -v
# =============================================================================

from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import torch

from models.model import LSTMModel, DirectionalLoss, save_model, load_model
from utils.data_utils import (
    create_sequences,
    normalize_data,
    denormalize_data,
    validate_data,
    compute_log_returns,
    returns_to_price,
)
from config.config import MODEL_CONFIG


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def model():
    """Create a default LSTMModel."""
    return LSTMModel()


@pytest.fixture
def dummy_df():
    """Create a dummy DataFrame mimicking klines features."""
    np.random.seed(42)
    n = 800  # >= input_window(120) + output_window(60) + buffer
    base_price = 70000
    # Generate realistic-ish price data
    returns = np.random.normal(0, 0.001, n)
    close = base_price * np.exp(np.cumsum(returns))
    return pd.DataFrame({
        "open": close * (1 + np.random.normal(0, 0.0005, n)),
        "high": close * (1 + np.abs(np.random.normal(0, 0.001, n))),
        "low": close * (1 - np.abs(np.random.normal(0, 0.001, n))),
        "close": close,
        "volume": np.random.uniform(1e6, 1e8, n),
        "rsi_14": np.random.uniform(20, 80, n),
        "macd": np.random.uniform(-5, 5, n),
    })


# ---------------------------------------------------------------------------
# Test LSTMModel
# ---------------------------------------------------------------------------

class TestLSTMModel:
    """Tests for the LSTM model architecture."""

    def test_model_init(self, model):
        """Model initializes with correct attributes."""
        assert model.hidden_size == MODEL_CONFIG["hidden_size"]
        assert model.num_layers == MODEL_CONFIG["num_layers"]
        assert model.fc.out_features == MODEL_CONFIG["output_window"]

    def test_forward_shape(self, model):
        """Forward pass produces correct output shape."""
        batch_size = 4
        x = torch.randn(batch_size, MODEL_CONFIG["input_window"], MODEL_CONFIG["features"])
        y = model(x)
        assert y.shape == (batch_size, MODEL_CONFIG["output_window"])

    def test_forward_single_sample(self, model):
        """Forward pass works with batch_size=1."""
        x = torch.randn(1, MODEL_CONFIG["input_window"], MODEL_CONFIG["features"])
        y = model(x)
        assert y.shape == (1, MODEL_CONFIG["output_window"])

    def test_parameter_count(self, model):
        """Model has a reasonable number of parameters."""
        total = sum(p.numel() for p in model.parameters())
        assert total > 0
        # 2-layer LSTM(7→128) + FC(128→60) — ~200K-500K params
        assert 100_000 < total < 1_000_000

    def test_custom_config(self):
        """Model accepts custom hyperparameters."""
        model = LSTMModel(
            input_size=5, hidden_size=64,
            num_layers=1, output_size=30, dropout=0.1,
        )
        x = torch.randn(2, 100, 5)
        y = model(x)
        assert y.shape == (2, 30)


# ---------------------------------------------------------------------------
# Test DirectionalLoss (Phase 1)
# ---------------------------------------------------------------------------

class TestDirectionalLoss:
    """Tests for the custom DirectionalLoss."""

    def test_same_direction_no_penalty(self):
        """No directional penalty when predictions match target direction."""
        loss_fn = DirectionalLoss(directional_weight=1.0)
        # Both increasing
        pred = torch.tensor([[1.0, 2.0, 3.0, 4.0]])
        target = torch.tensor([[1.0, 2.0, 3.0, 4.0]])
        loss = loss_fn(pred, target)
        # MSE = 0, dir_penalty = 0
        assert loss.item() == pytest.approx(0.0, abs=1e-6)

    def test_wrong_direction_has_penalty(self):
        """Directional penalty kicks in when directions mismatch."""
        loss_fn = DirectionalLoss(directional_weight=1.0)
        # pred goes up, target goes down
        pred = torch.tensor([[1.0, 2.0, 3.0, 4.0]])
        target = torch.tensor([[4.0, 3.0, 2.0, 1.0]])
        loss_with_dir = loss_fn(pred, target)

        # Compare with pure MSE
        mse_only = DirectionalLoss(directional_weight=0.0)
        loss_without_dir = mse_only(pred, target)

        assert loss_with_dir.item() > loss_without_dir.item()

    def test_zero_weight_equals_mse(self):
        """With weight=0, DirectionalLoss equals MSE."""
        loss_fn = DirectionalLoss(directional_weight=0.0)
        mse_fn = torch.nn.MSELoss()
        pred = torch.randn(4, 60)
        target = torch.randn(4, 60)
        assert loss_fn(pred, target).item() == pytest.approx(
            mse_fn(pred, target).item(), abs=1e-6,
        )

    def test_single_step_no_crash(self):
        """Does not crash with single timestep output."""
        loss_fn = DirectionalLoss(directional_weight=0.5)
        pred = torch.tensor([[1.0]])
        target = torch.tensor([[2.0]])
        loss = loss_fn(pred, target)
        assert loss.item() > 0


# ---------------------------------------------------------------------------
# Test Save / Load
# ---------------------------------------------------------------------------

class TestSaveLoad:
    """Tests for model checkpointing."""

    def test_save_load_roundtrip(self, model, dummy_df):
        """Save and load model, verify predictions match."""
        feature_cols = list(dummy_df.columns)
        _, scaler = normalize_data(dummy_df, feature_cols)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test_model.pth"
            save_model(model, scaler, path, metadata={"version": "test"})

            loaded_model, loaded_scaler, metadata = load_model(path, device="cpu")

        # Verify metadata
        assert metadata["version"] == "test"

        # Verify predictions match
        x = torch.randn(2, MODEL_CONFIG["input_window"], MODEL_CONFIG["features"])
        model.eval()
        loaded_model.eval()

        with torch.no_grad():
            orig_pred = model(x)
            loaded_pred = loaded_model(x)

        assert torch.allclose(orig_pred, loaded_pred, atol=1e-6)

    def test_load_nonexistent_raises(self):
        """Loading a nonexistent model raises ModelNotFoundError."""
        from utils.exceptions import ModelNotFoundError
        with pytest.raises(ModelNotFoundError):
            load_model("/nonexistent/path/model.pth")


# ---------------------------------------------------------------------------
# Test Data Utils (ML helpers)
# ---------------------------------------------------------------------------

class TestDataUtils:
    """Tests for ML data preparation functions."""

    def test_create_sequences_shape(self, dummy_df):
        """Sequences have correct shapes."""
        feature_cols = list(dummy_df.columns)
        input_w, output_w = 50, 10
        X, y = create_sequences(
            dummy_df, input_w, output_w,
            feature_cols=feature_cols, target_col="close",
        )
        expected_samples = len(dummy_df) - input_w - output_w + 1
        assert X.shape == (expected_samples, input_w, len(feature_cols))
        assert y.shape == (expected_samples, output_w)

    def test_create_sequences_insufficient_data(self, dummy_df):
        """Raises ValueError when data is too short."""
        with pytest.raises(ValueError, match="Not enough data"):
            create_sequences(dummy_df, 700, 200, feature_cols=list(dummy_df.columns))

    def test_normalize_denormalize_roundtrip(self, dummy_df):
        """Normalize → denormalize recovers original values (StandardScaler)."""
        feature_cols = list(dummy_df.columns)
        scaled_df, scaler = normalize_data(dummy_df, feature_cols)

        # StandardScaler: values should roughly center around 0
        assert scaled_df[feature_cols].mean().abs().max() < 0.1

        # Round-trip for 'close' column
        close_idx = feature_cols.index("close")
        original = dummy_df["close"].values
        scaled = scaled_df["close"].values
        recovered = denormalize_data(scaled, scaler, close_idx)

        np.testing.assert_allclose(recovered, original, rtol=1e-5)

    def test_normalize_with_pretrained_scaler(self, dummy_df):
        """Inference-mode normalize uses pre-fitted scaler."""
        feature_cols = list(dummy_df.columns)
        _, scaler = normalize_data(dummy_df, feature_cols)

        # Second normalize with same scaler
        new_df = dummy_df.iloc[:10].copy()
        scaled_new, _ = normalize_data(new_df, feature_cols, scaler=scaler)
        assert len(scaled_new) == 10

    def test_validate_data_drops_nulls(self):
        """validate_data drops rows with null values."""
        df = pd.DataFrame({
            "a": [1.0, 2.0, None, 4.0],
            "b": [5.0, None, 7.0, 8.0],
        })
        clean = validate_data(df, ["a", "b"])
        assert len(clean) == 2  # only rows 0 and 3 survive

    def test_validate_data_missing_column(self):
        """validate_data raises on missing required columns."""
        df = pd.DataFrame({"a": [1, 2]})
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_data(df, ["a", "nonexistent"])

    def test_validate_data_drops_duplicates(self):
        """validate_data removes duplicate rows."""
        df = pd.DataFrame({"a": [1, 1, 2], "b": [3, 3, 4]})
        clean = validate_data(df, ["a", "b"])
        assert len(clean) == 2


# ---------------------------------------------------------------------------
# Test Log-Returns (Phase 1)
# ---------------------------------------------------------------------------

class TestLogReturns:
    """Tests for log-returns computation and price reconstruction."""

    def test_compute_log_returns_shape(self, dummy_df):
        """Log-returns drops 1 row."""
        result = compute_log_returns(dummy_df)
        assert len(result) == len(dummy_df) - 1

    def test_compute_log_returns_values(self):
        """Log-returns are correct: log(p_t / p_{t-1})."""
        df = pd.DataFrame({
            "close": [100.0, 110.0, 105.0],
            "volume": [1000, 2000, 3000],
        })
        result = compute_log_returns(df, price_cols=["close"])
        expected = [np.log(110.0 / 100.0), np.log(105.0 / 110.0)]
        np.testing.assert_allclose(result["close"].values, expected, rtol=1e-6)
        # Volume should be unchanged
        np.testing.assert_array_equal(result["volume"].values, [2000, 3000])

    def test_returns_to_price_roundtrip(self):
        """Convert prices → log-returns → prices recovers originals."""
        prices = np.array([100.0, 105.0, 103.0, 108.0, 110.0])
        log_returns = np.diff(np.log(prices))
        recovered = returns_to_price(log_returns, last_price=prices[0])
        np.testing.assert_allclose(recovered, prices[1:], rtol=1e-6)

    def test_returns_to_price_single_step(self):
        """Works with a single return value."""
        log_ret = np.array([0.01])  # ~1% increase
        result = returns_to_price(log_ret, last_price=100.0)
        expected = 100.0 * np.exp(0.01)
        np.testing.assert_allclose(result, [expected], rtol=1e-6)
