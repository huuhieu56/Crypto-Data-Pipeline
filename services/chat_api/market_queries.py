"""ClickHouse market data queries — dynamic resolution per timeframe.

All query functions accept a timeframe config dict from TIMEFRAME_CONFIG
and return DataFrames or dicts ready for prompt formatting.
"""

from __future__ import annotations

import pandas as pd

from utils.db_utils import new_ch_client, ch_query_df_params
from utils.logger import get_logger

logger = get_logger(__name__)


def _query_df(query: str) -> pd.DataFrame:
    """Execute a query using a fresh ClickHouse client (async-safe)."""
    client = new_ch_client()
    try:
        return client.query_df(query)
    finally:
        client.close()


def _esc(value: str) -> str:
    """SQL-escape single quotes."""
    return value.replace("'", "''")


# ---------------------------------------------------------------------------
# Klines (OHLCV + indicators)
# ---------------------------------------------------------------------------

def fetch_candles(symbol: str, config: dict) -> pd.DataFrame:
    """Fetch aggregated candles from ClickHouse based on timeframe config."""
    group_by = config["candle_group_by"]
    lookback = int(config["candle_lookback_days"])
    limit = int(config["candle_limit"])

    q = (
        f"SELECT {group_by} AS ts, "
        "argMin(open, open_time) AS open, "
        "max(high) AS high, "
        "min(low) AS low, "
        "argMax(close, open_time) AS close, "
        "sum(volume) AS volume, "
        "argMax(rsi_14, open_time) AS rsi_14, "
        "argMax(macd, open_time) AS macd, "
        "argMax(macd_signal, open_time) AS macd_signal "
        "FROM klines FINAL "
        "WHERE symbol = {symbol:String} "
        f"AND open_time >= now() - INTERVAL {lookback} DAY "
        f"GROUP BY ts ORDER BY ts ASC "
        f"LIMIT {limit}"
    )
    return ch_query_df_params(q, {"symbol": symbol})


# ---------------------------------------------------------------------------
# Ticker 24h (volume, spread, price change trends)
# ---------------------------------------------------------------------------

def fetch_ticker_trend(symbol: str, config: dict) -> pd.DataFrame:
    """Fetch aggregated ticker_24h trend from ClickHouse."""
    group_by = config["ticker_group_by"]
    lookback = int(config["ticker_lookback_days"])

    q = (
        f"SELECT {group_by} AS ts, "
        "avg(price_change_pct) AS avg_price_change_pct, "
        "avg(volume_24h) AS avg_volume_24h, "
        "avg(spread_pct) AS avg_spread_pct, "
        "avg(trade_count) AS avg_trade_count "
        "FROM ticker_24h FINAL "
        "WHERE symbol = {symbol:String} "
        f"AND snapshot_time >= now() - INTERVAL {lookback} DAY "
        "GROUP BY ts ORDER BY ts ASC"
    )
    return ch_query_df_params(q, {"symbol": symbol})


def fetch_latest_ticker(symbol: str) -> dict:
    """Fetch the latest ticker_24h snapshot for scalar values."""
    q = (
        "SELECT price_change_pct, volume_24h, spread_pct "
        "FROM ticker_24h FINAL "
        "WHERE symbol = {symbol:String} "
        "ORDER BY snapshot_time DESC LIMIT 1"
    )
    df = ch_query_df_params(q, {"symbol": symbol})
    if df.empty:
        return {"price_change_pct": 0.0, "volume_24h": 0.0, "spread_pct": 0.0}

    row = df.iloc[0]
    return {
        "price_change_pct": float(row["price_change_pct"]) if pd.notna(row["price_change_pct"]) else 0.0,
        "volume_24h": float(row["volume_24h"]) if pd.notna(row["volume_24h"]) else 0.0,
        "spread_pct": float(row["spread_pct"]) if pd.notna(row["spread_pct"]) else 0.0,
    }


# ---------------------------------------------------------------------------
# Order Book Snapshot (buy/sell pressure)
# ---------------------------------------------------------------------------

def fetch_orderbook_data(symbol: str, config: dict) -> dict:
    """Fetch order book data — trend, summary, or latest-only per timeframe."""
    ob_mode = config.get("ob_mode", "trend")

    if ob_mode == "latest_only":
        return _fetch_ob_latest(symbol)
    if ob_mode == "summary_30d":
        return _fetch_ob_summary(symbol)
    return _fetch_ob_trend(symbol, config)


def _fetch_ob_latest(symbol: str) -> dict:
    q = (
        "SELECT imbalance, total_bid_volume, total_ask_volume "
        "FROM order_book_snapshot FINAL "
        "WHERE symbol = {symbol:String} "
        "ORDER BY timestamp DESC LIMIT 1"
    )
    df = ch_query_df_params(q, {"symbol": symbol})
    if df.empty:
        return {"mode": "latest_only", "latest_imbalance": 0.5}
    row = df.iloc[0]
    return {
        "mode": "latest_only",
        "latest_imbalance": _safe_float(row, "imbalance", 0.5),
        "bid_volume": _safe_float(row, "total_bid_volume", 0.0),
        "ask_volume": _safe_float(row, "total_ask_volume", 0.0),
    }


def _fetch_ob_summary(symbol: str) -> dict:
    q = (
        "SELECT "
        "avg(imbalance) AS avg_imbalance, "
        "min(imbalance) AS min_imbalance, "
        "max(imbalance) AS max_imbalance, "
        "argMax(imbalance, timestamp) AS latest_imbalance "
        "FROM order_book_snapshot FINAL "
        "WHERE symbol = {symbol:String} "
        "AND timestamp >= now() - INTERVAL 30 DAY"
    )
    df = ch_query_df_params(q, {"symbol": symbol})
    if df.empty:
        return {"mode": "summary_30d", "avg_imbalance": 0.5, "latest_imbalance": 0.5}
    row = df.iloc[0]
    return {
        "mode": "summary_30d",
        "avg_imbalance": _safe_float(row, "avg_imbalance", 0.5),
        "min_imbalance": _safe_float(row, "min_imbalance", 0.5),
        "max_imbalance": _safe_float(row, "max_imbalance", 0.5),
        "latest_imbalance": _safe_float(row, "latest_imbalance", 0.5),
    }


def _fetch_ob_trend(symbol: str, config: dict) -> dict:
    group_by = config["ob_group_by"]
    lookback = int(config["ob_lookback_days"])

    q = (
        f"SELECT {group_by} AS ts, "
        "avg(imbalance) AS avg_imbalance, "
        "avg(total_bid_volume) AS avg_bid_vol, "
        "avg(total_ask_volume) AS avg_ask_vol "
        "FROM order_book_snapshot FINAL "
        "WHERE symbol = {symbol:String} "
        f"AND timestamp >= now() - INTERVAL {lookback} DAY "
        "GROUP BY ts ORDER BY ts ASC"
    )
    trend_df = ch_query_df_params(q, {"symbol": symbol})

    # Also fetch latest snapshot
    latest = _fetch_ob_latest(symbol)

    return {
        "mode": "trend",
        "trend_df": trend_df,
        "latest_imbalance": latest.get("latest_imbalance", 0.5),
    }


# ---------------------------------------------------------------------------
# Signal computation — pre-interpreted signals from raw data
# ---------------------------------------------------------------------------

def compute_signals(df: pd.DataFrame) -> dict[str, str]:
    """Compute actionable signals from candle data.

    Returns a dict of signal_name -> human-readable interpretation.
    Requires columns: rsi_14, macd, macd_signal, high, low, volume.
    """
    if df.empty or len(df) < 2:
        return {}

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    signals: dict[str, str] = {}

    # RSI trend (not just level)
    rsi = _safe_float(latest, "rsi_14", 50.0)
    rsi_prev = _safe_float(prev, "rsi_14", 50.0)
    if rsi > 70:
        signals["rsi"] = f"OVERBOUGHT ({rsi:.1f}, {'rising' if rsi > rsi_prev else 'falling'})"
    elif rsi < 30:
        signals["rsi"] = f"OVERSOLD ({rsi:.1f}, {'falling' if rsi < rsi_prev else 'rising'})"
    else:
        signals["rsi"] = f"NEUTRAL ({rsi:.1f}, {'rising' if rsi > rsi_prev else 'falling'})"

    # MACD crossover
    macd = _safe_float(latest, "macd", 0.0)
    signal_val = _safe_float(latest, "macd_signal", 0.0)
    macd_prev = _safe_float(prev, "macd", 0.0)
    signal_prev = _safe_float(prev, "macd_signal", 0.0)
    if macd > signal_val and macd_prev <= signal_prev:
        signals["macd"] = "BULLISH CROSSOVER (just happened)"
    elif macd < signal_val and macd_prev >= signal_prev:
        signals["macd"] = "BEARISH CROSSOVER (just happened)"
    elif macd > signal_val:
        signals["macd"] = f"BULLISH (MACD above signal, gap: {macd - signal_val:.2f})"
    else:
        signals["macd"] = f"BEARISH (MACD below signal, gap: {signal_val - macd:.2f})"

    # Volume trend
    vol_avg = df["volume"].mean()
    vol_latest = _safe_float(latest, "volume", 0.0)
    if vol_avg > 0:
        vol_ratio = vol_latest / vol_avg
        signals["volume"] = f"{'ABOVE' if vol_ratio > 1 else 'BELOW'} average ({vol_ratio:.1f}x)"
    else:
        signals["volume"] = "NO DATA"

    # Price structure (higher highs / lower lows over last 5 bars)
    recent_highs = df["high"].tail(5)
    recent_lows = df["low"].tail(5)
    if recent_highs.is_monotonic_increasing:
        signals["structure"] = "HIGHER HIGHS (bullish structure)"
    elif recent_lows.is_monotonic_decreasing:
        signals["structure"] = "LOWER LOWS (bearish structure)"
    else:
        signals["structure"] = "MIXED (no clear trend structure)"

    return signals


def format_signals(signals: dict[str, str]) -> str:
    """Format computed signals into a text block."""
    if not signals:
        return ""
    lines = ["--- Signals ---"]
    for key in ("rsi", "macd", "volume", "structure"):
        if key in signals:
            lines.append(f"{key.upper()}: {signals[key]}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Formatters — convert DataFrames/dicts into prompt text
# ---------------------------------------------------------------------------

def format_candles(df: pd.DataFrame, ts_format: str) -> str:
    """Format candle DataFrame for the system prompt."""
    if df.empty:
        return "(No candle data available)"

    lines: list[str] = []
    for _, row in df.iterrows():
        ts = pd.Timestamp(row["ts"]).strftime(ts_format)
        rsi = _safe_float(row, "rsi_14", 50.0)
        line = (
            f"{ts} O:{float(row['open']):.4f} H:{float(row['high']):.4f} "
            f"L:{float(row['low']):.4f} C:{float(row['close']):.4f} "
            f"V:{float(row['volume']):,.0f} RSI:{rsi:.1f}"
        )
        lines.append(line)
    return "\n".join(lines)


def format_ticker_trend(df: pd.DataFrame) -> str:
    """Format ticker trend DataFrame for the system prompt."""
    if df.empty:
        return "(No ticker trend data available)"

    lines: list[str] = []
    for _, row in df.iterrows():
        ts = pd.Timestamp(row["ts"]).strftime("%Y-%m-%d")
        vol = _safe_float(row, "avg_volume_24h", 0.0)
        spread = _safe_float(row, "avg_spread_pct", 0.0)
        pchg = _safe_float(row, "avg_price_change_pct", 0.0)
        lines.append(
            f"{ts} Vol:{vol:,.0f} Spread:{spread:.4f}% PriceChg:{pchg:+.2f}%"
        )
    return "\n".join(lines)


def format_orderbook(data: dict) -> str:
    """Format order book data for the system prompt."""
    mode = data.get("mode", "latest_only")

    if mode == "latest_only":
        imb = data.get("latest_imbalance", 0.5)
        tag = _imbalance_tag(imb)
        return f"Current imbalance: {imb:.3f} ({tag})"

    if mode == "summary_30d":
        return (
            f"30-day order book summary:\n"
            f"  Avg imbalance: {data.get('avg_imbalance', 0.5):.3f}\n"
            f"  Min: {data.get('min_imbalance', 0.5):.3f} / "
            f"Max: {data.get('max_imbalance', 0.5):.3f}\n"
            f"  Latest: {data.get('latest_imbalance', 0.5):.3f} "
            f"({_imbalance_tag(data.get('latest_imbalance', 0.5))})"
        )

    # Trend mode
    trend_df = data.get("trend_df", pd.DataFrame())
    latest_imb = data.get("latest_imbalance", 0.5)
    lines = [f"Current imbalance: {latest_imb:.3f} ({_imbalance_tag(latest_imb)})"]

    if not trend_df.empty:
        lines.append("Order book trend:")
        for _, row in trend_df.iterrows():
            ts = pd.Timestamp(row["ts"]).strftime("%Y-%m-%d %H:00")
            imb = _safe_float(row, "avg_imbalance", 0.5)
            lines.append(f"  {ts} Imb:{imb:.3f} Bid:{_safe_float(row, 'avg_bid_vol', 0):,.0f} Ask:{_safe_float(row, 'avg_ask_vol', 0):,.0f}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(row, col: str, default: float = 0.0) -> float:
    """Safely extract a float from a DataFrame row."""
    val = row.get(col) if isinstance(row, dict) else row[col] if col in row.index else default
    return float(val) if pd.notna(val) else default


def _imbalance_tag(imbalance: float) -> str:
    if imbalance > 0.6:
        return "strong buy pressure"
    if imbalance < 0.4:
        return "strong sell pressure"
    return "balanced"
