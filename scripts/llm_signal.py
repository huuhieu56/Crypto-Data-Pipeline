"""Generate LLM advisory signals from latest market context (no predictions)."""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.llm_config import (
    BATCH_SIZE,
    LLM_DAILY_CANDLES,
    LLM_PROVIDER,
    RSI_OVERBOUGHT,
    RSI_OVERSOLD,
)
from config.symbols import SYMBOLS
from utils.db_utils import ch_command, ch_insert_df, ch_query_df
from utils.exceptions import LLMQuotaExceededError
from utils.llm_utils import get_llm_signal
from utils.logger import get_logger

logger = get_logger(__name__)


def _esc(value: str) -> str:
    return value.replace("'", "''")


def _fmt_dt(dt: datetime) -> str:
    return dt.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _fallback_hold_row(symbol: str, generated_at: datetime, reason: str = "LLM quota exceeded") -> dict:
    data_window_minutes = int(LLM_DAILY_CANDLES * 1440)
    return {
        "symbol": symbol,
        "generated_at": generated_at.replace(tzinfo=None),
        "signal": "HOLD",
        "confidence": 1,
        "reason": reason,
        "key_risk": "provider quota limit",
        "rsi_14": None,
        "macd_cross": "neutral",
        "ob_imbalance": None,
        "vol_change_pct": None,
        "price_change_pct": None,
        "data_window_minutes": data_window_minutes,
        "trend_6h": "SIDEWAYS",
        "trend_6h_pct": 0.0,
        "llm_provider": LLM_PROVIDER,
        "model_version": "daily30_fallback_quota_v1",
    }


def _fetch_daily_klines(symbol: str, limit_days: int) -> pd.DataFrame:
    q = (
        "SELECT "
        "  toDate(timestamp) AS day, "
        "  argMin(open, timestamp) AS open, "
        "  max(high) AS high, "
        "  min(low) AS low, "
        "  argMax(close, timestamp) AS close, "
        "  sum(volume) AS volume, "
        "  argMax(rsi_14, timestamp) AS rsi_14, "
        "  argMax(macd, timestamp) AS macd, "
        "  argMax(macd_signal, timestamp) AS macd_signal "
        "FROM klines "
        f"WHERE symbol = '{_esc(symbol)}' "
        "GROUP BY day "
        "ORDER BY day DESC "
        f"LIMIT {int(limit_days)}"
    )
    df = ch_query_df(q)
    if df.empty:
        return df
    df = df.iloc[::-1].reset_index(drop=True)
    df = df.rename(columns={"day": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def _fetch_ticker(symbol: str) -> tuple[float, float]:
    q = (
        "SELECT price_change_pct, volume_24h "
        "FROM ticker_24h "
        f"WHERE symbol = '{_esc(symbol)}' "
        "ORDER BY snapshot_time DESC LIMIT 2"
    )
    df = ch_query_df(q)
    if df.empty:
        return 0.0, 0.0

    latest_price_change = float(df["price_change_pct"].iloc[0]) if pd.notna(df["price_change_pct"].iloc[0]) else 0.0
    if len(df) < 2:
        return latest_price_change, 0.0

    curr_vol = float(df["volume_24h"].iloc[0]) if pd.notna(df["volume_24h"].iloc[0]) else 0.0
    prev_vol = float(df["volume_24h"].iloc[1]) if pd.notna(df["volume_24h"].iloc[1]) else 0.0
    vol_change = ((curr_vol - prev_vol) / prev_vol * 100.0) if prev_vol > 0 else 0.0
    return latest_price_change, vol_change


def _fetch_orderbook_imbalance(symbol: str) -> float:
    q = (
        "SELECT imbalance "
        "FROM order_book_snapshot "
        f"WHERE symbol = '{_esc(symbol)}' "
        "ORDER BY timestamp DESC LIMIT 1"
    )
    df = ch_query_df(q)
    if df.empty or pd.isna(df["imbalance"].iloc[0]):
        return 0.5
    return float(df["imbalance"].iloc[0])


def _build_prompt(symbol: str, daily: pd.DataFrame, pchg24h: float, vchg24h: float, ob_imbalance: float) -> tuple[str, dict]:
    """Build compact daily-candle prompt and structured snapshot for persistence."""
    if daily.empty:
        trend_6h = "SIDEWAYS"
        trend_6h_pct = 0.0
    else:
        first_close = float(daily["close"].iloc[0])
        last_close = float(daily["close"].iloc[-1])
        trend_6h_pct = ((last_close - first_close) / first_close * 100.0) if first_close > 0 else 0.0
        if trend_6h_pct > 1.0:
            trend_6h = "UPTREND"
        elif trend_6h_pct < -1.0:
            trend_6h = "DOWNTREND"
        else:
            trend_6h = "SIDEWAYS"

    latest = daily.iloc[-1]
    current_price = float(latest["close"])
    latest_rsi = float(latest["rsi_14"]) if pd.notna(latest["rsi_14"]) else 50.0
    latest_macd = float(latest["macd"]) if pd.notna(latest["macd"]) else 0.0
    latest_macd_signal = float(latest["macd_signal"]) if pd.notna(latest["macd_signal"]) else 0.0

    if latest_macd > latest_macd_signal:
        macd_cross = "bullish"
    elif latest_macd < latest_macd_signal:
        macd_cross = "bearish"
    else:
        macd_cross = "neutral"

    rsi_tag = "OVERBOUGHT" if latest_rsi > RSI_OVERBOUGHT else "OVERSOLD" if latest_rsi < RSI_OVERSOLD else "NEUTRAL"
    ob_tag = "strong buy" if ob_imbalance > 0.6 else "strong sell" if ob_imbalance < 0.4 else "balanced"

    def _fmt(df: pd.DataFrame) -> str:
        lines = []
        for _, row in df.iterrows():
            ts = pd.Timestamp(row["timestamp"]).strftime("%Y-%m-%d")
            rv = float(row["rsi_14"]) if pd.notna(row["rsi_14"]) else 50.0
            line = (
                f"{ts} O:{float(row['open']):.4f} H:{float(row['high']):.4f} "
                f"L:{float(row['low']):.4f} C:{float(row['close']):.4f} V:{float(row['volume']):,.0f} "
                f"RSI:{rv:.1f}"
            )
            lines.append(line)
        return "\n".join(lines)

    prompt = (
        f"You are a professional crypto analyst. Analyze {symbol} using exactly {LLM_DAILY_CANDLES} daily candles and return one advisory signal.\n\n"
        "DAILY CANDLES (oldest -> newest):\n"
        f"{_fmt(daily)}\n\n"
        "SNAPSHOT:\n"
        f"- Current price: {current_price:.4f}\n"
        f"- RSI(14): {latest_rsi:.1f} ({rsi_tag})\n"
        f"- MACD crossover: {macd_cross}\n"
        f"- Order book imbalance: {ob_imbalance:.3f} ({ob_tag})\n"
        f"- 24h price change: {pchg24h:+.2f}%\n"
        f"- 24h volume change: {vchg24h:+.2f}%\n\n"
        "Rules:\n"
        "1) Use only the provided daily candles + snapshot.\n"
        "2) Prioritize trend consistency and risk control.\n"
        "3) If signals conflict, return HOLD.\n\n"
        "Respond ONLY valid JSON:\n"
        '{"signal":"BUY|SELL|HOLD","confidence":1-5,"reason":"max 20 words","key_risk":"max 12 words"}'
    )

    snapshot = {
        "rsi_14": latest_rsi,
        "macd_cross": macd_cross,
        "ob_imbalance": ob_imbalance,
        "vol_change_pct": vchg24h,
        "price_change_pct": pchg24h,
        "trend_6h": trend_6h,
        "trend_6h_pct": round(trend_6h_pct, 4),
    }
    return prompt, snapshot


async def process_symbol(session: aiohttp.ClientSession, symbol: str, generated_at: datetime) -> dict | None:
    try:
        daily = _fetch_daily_klines(symbol, LLM_DAILY_CANDLES)
        if len(daily) < LLM_DAILY_CANDLES:
            logger.warning("[%s] not enough daily candles: %d/%d", symbol, len(daily), LLM_DAILY_CANDLES)
            return None

        pchg24h, vchg24h = _fetch_ticker(symbol)
        ob_imbalance = _fetch_orderbook_imbalance(symbol)
        prompt, snapshot = _build_prompt(symbol, daily, pchg24h, vchg24h, ob_imbalance)

        result = await get_llm_signal(session, symbol, prompt)
        if result is None:
            logger.error("[%s] LLM returned no valid response", symbol)
            return None

        return {
            "symbol": symbol,
            "generated_at": generated_at.replace(tzinfo=None),
            "signal": result["signal"],
            "confidence": int(result["confidence"]),
            "reason": result["reason"],
            "key_risk": result.get("key_risk", ""),
            "rsi_14": snapshot["rsi_14"],
            "macd_cross": snapshot["macd_cross"],
            "ob_imbalance": snapshot["ob_imbalance"],
            "vol_change_pct": snapshot["vol_change_pct"],
            "price_change_pct": snapshot["price_change_pct"],
            "data_window_minutes": int(LLM_DAILY_CANDLES * 1440),
            "trend_6h": snapshot["trend_6h"],
            "trend_6h_pct": snapshot["trend_6h_pct"],
            "llm_provider": LLM_PROVIDER,
            "model_version": f"daily{LLM_DAILY_CANDLES}_v1",
        }
    except LLMQuotaExceededError:
        raise
    except Exception as exc:
        logger.error("[%s] process failed: %s", symbol, exc)
        return None


def save_signals(rows: list[dict], generated_at: datetime, dry_run: bool = False) -> int:
    if not rows:
        return 0
    if dry_run:
        logger.info("Dry run: skip insert (%d rows)", len(rows))
        return len(rows)

    ch_command(
        "ALTER TABLE llm_signals "
        f"DELETE WHERE generated_at = toDateTime('{_fmt_dt(generated_at)}')"
    )
    out_df = pd.DataFrame(rows)
    table_cols_df = ch_query_df(
        "SELECT name FROM system.columns "
        "WHERE database = currentDatabase() AND table = 'llm_signals'"
    )
    table_cols = set(table_cols_df["name"].astype(str).tolist())
    insert_cols = [c for c in out_df.columns if c in table_cols]
    skipped_cols = [c for c in out_df.columns if c not in table_cols]
    if skipped_cols:
        logger.warning("llm_signals schema mismatch, skipping columns: %s", skipped_cols)
    if not insert_cols:
        logger.error("No compatible columns found for llm_signals insert")
        return 0

    inserted = ch_insert_df("llm_signals", out_df[insert_cols])
    return inserted


async def run_llm_signals(symbols: list[str], dry_run: bool = False) -> int:
    generated_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    logger.info(
        "LLM signal run: symbols=%d daily_candles=%d (~%d minutes)",
        len(symbols),
        LLM_DAILY_CANDLES,
        LLM_DAILY_CANDLES * 1440,
    )

    all_rows: list[dict] = []
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        stop_due_to_quota = False
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i : i + BATCH_SIZE]
            tasks = [process_symbol(session, symbol, generated_at) for symbol in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            ok_rows: list[dict] = []
            for symbol, item in zip(batch, batch_results):
                if isinstance(item, LLMQuotaExceededError):
                    stop_due_to_quota = True
                    ok_rows.append(_fallback_hold_row(symbol, generated_at))
                    continue
                if isinstance(item, Exception):
                    logger.error("Batch task failed: %s", item)
                    continue
                if item is not None:
                    ok_rows.append(item)

            all_rows.extend(ok_rows)
            logger.info(
                "Batch %d: %d/%d ok",
                (i // BATCH_SIZE) + 1,
                len(ok_rows),
                len(batch),
            )

            if stop_due_to_quota:
                logger.error(
                    "LLM quota exceeded. Stopping early after batch %d and using fallback HOLD for quota-hit symbols.",
                    (i // BATCH_SIZE) + 1,
                )
                break

    inserted = save_signals(all_rows, generated_at, dry_run=dry_run)
    buy = sum(1 for r in all_rows if r["signal"] == "BUY")
    sell = sum(1 for r in all_rows if r["signal"] == "SELL")
    hold = sum(1 for r in all_rows if r["signal"] == "HOLD")
    logger.info("Done: inserted=%d BUY=%d SELL=%d HOLD=%d", inserted, buy, sell, hold)
    return inserted


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate LLM advisory signals")
    p.add_argument("--symbols", nargs="+", default=SYMBOLS, help="Symbols to process")
    p.add_argument("--dry-run", action="store_true", help="Call LLM without DB insert")
    return p


def main() -> None:
    args = _build_parser().parse_args()
    asyncio.run(run_llm_signals(args.symbols, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
