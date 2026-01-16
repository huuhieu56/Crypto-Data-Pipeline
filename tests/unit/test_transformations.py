"""Unit tests for transformation functions."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Fixture for local Spark session (testing only)
@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("Cinema360_Tests")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestCleaning:
    """Tests for cleaning transformations."""

    def test_filter_adult_content(self, spark):
        """Test that adult content is filtered out."""
        # TODO: Implement test
        pass

    def test_filter_missing_year(self, spark):
        """Test that rows with missing year are removed."""
        # TODO: Implement test
        pass


class TestMetrics:
    """Tests for business metrics calculation."""

    def test_calculate_profit(self, spark):
        """Test profit calculation."""
        # TODO: Implement test
        pass

    def test_calculate_roi_division_by_zero(self, spark):
        """Test ROI handles zero budget gracefully."""
        # TODO: Implement test
        pass
