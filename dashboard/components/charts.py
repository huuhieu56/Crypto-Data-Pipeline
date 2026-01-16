"""Reusable Chart Components.

Uses Plotly for interactive visualizations.
"""
import plotly.express as px
import plotly.graph_objects as go
from typing import Any
import pandas as pd


def create_scatter_budget_revenue(df: pd.DataFrame) -> go.Figure:
    """Create Budget vs Revenue scatter plot.

    Args:
        df: DataFrame with 'budget' and 'revenue' columns.

    Returns:
        Plotly Figure object.
    """
    fig = px.scatter(
        df,
        x="budget",
        y="revenue",
        title="Budget vs Revenue Correlation",
        labels={"budget": "Budget ($)", "revenue": "Revenue ($)"},
        hover_data=["title"] if "title" in df.columns else None,
    )
    fig.update_layout(template="plotly_dark")
    return fig


def create_line_genre_trends(df: pd.DataFrame) -> go.Figure:
    """Create genre trends line chart.

    Args:
        df: DataFrame with 'year', 'genre', 'count' columns.

    Returns:
        Plotly Figure object.
    """
    fig = px.line(
        df,
        x="year",
        y="count",
        color="genre",
        title="Genre Trends Over Time",
        labels={"year": "Year", "count": "Number of Movies"},
    )
    fig.update_layout(template="plotly_dark")
    return fig
