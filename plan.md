# Crypto Data Pipeline — Improvement Plan

## Why this plan exists

The codebase review identified critical technical issues (OOM risks, thread-safety bugs, SQL injection, dead code, DRY violations) and product gaps (shallow indicator set, no derivatives data, no backtesting, chatbot analysis is surface-level). This plan keeps the existing infrastructure (MinIO, Spark, ClickHouse, Airflow, LangGraph) and focuses on fixes that actually move the needle — both hardening the code and deepening the analytical value.

---

## Phase 1: Fix Critical Bugs & Technical Debt

### 1.1 Fix Thread-Safety — ClickHouse Singleton

**File:** `utils/db_utils.py`

**Problem:** `_ch_client` is a module-level singleton shared across threads. `clickhouse-connect`'s Client is not thread-safe. The primary risk is in `transform.py`, where `_get_ch_context()` calls `ch_query_df(get_ch_client())` inside `ThreadPoolExecutor(max_workers=8)`. Multiple symbol workers query ClickHouse concurrently on the same connection object, which can cause `ProgrammingError`, corrupted results, or silent data loss.

**Why it matters:** In `transform.py`, up to 8 parallel workers call `_get_ch_context()` simultaneously — each sends a SELECT on the shared connection, and responses can get interleaved (worker A reads worker B's result set). Note: `extract.py` calls `get_last_timestamps()` once *before* spawning threads, and `load.py` is sequential — so the risk there is lower. The real danger is `transform.py`'s concurrent warm-up queries.

**Fix:**
```python
import threading

_local = threading.local()

def get_ch_client() -> Client:
    """Return a thread-local ClickHouse client."""
    if not hasattr(_local, "client"):
        try:
            _local.client = clickhouse_connect.get_client(
                host=CH_CONFIG["host"],
                port=CH_CONFIG["port"],
                username=CH_CONFIG["user"],
                password=CH_CONFIG["password"],
                database=CH_CONFIG["database"],
            )
            _local.client.query("SELECT 1")
            logger.info("ClickHouse connected: %s:%s/%s",
                CH_CONFIG["host"], CH_CONFIG["port"], CH_CONFIG["database"])
        except Exception as exc:
            raise DatabaseConnectionError(f"Cannot connect to ClickHouse: {exc}") from exc
    return _local.client
```

Each thread gets its own client. No shared state, no races.

---

### 1.2 Fix SQL Injection in Chat API

**Files:** `services/chat_api/main.py:117,147`, `services/chat_api/nodes.py:242`, `services/chat_api/market_queries.py` (all query functions)

**Problem:** User-controlled inputs are interpolated into SQL via f-strings throughout the chat API:
- `session_id` in `main.py:get_history`, `main.py:delete_history`, and `nodes.py:load_history` uses `_esc()` (single-quote doubling) which is inadequate — it only handles `'` but not other ClickHouse-specific injection vectors.
- `symbol` in all `market_queries.py` query functions (`fetch_candles`, `fetch_ticker_trend`, `fetch_latest_ticker`, `fetch_orderbook_data`) also uses `_esc(symbol)` with f-strings. The `symbol` parameter comes from `ChatRequest.symbol` — fully user-controlled.

**Why it matters:** Both `session_id` and `symbol` come from client-side input. While `_esc()` would stop naive payloads like `'; DROP TABLE ...` (it doubles the quote), it does not protect against all injection vectors. Parameterized queries eliminate the entire class of vulnerability. This is OWASP #1 — injection.

**Fix:** Use ClickHouse parameterized queries everywhere:
```python
# In market_queries.py, add a parameterized query helper:
def _query_df_params(query: str, params: dict) -> pd.DataFrame:
    client = new_ch_client()
    try:
        return client.query_df(query, parameters=params)
    finally:
        client.close()

# In nodes.py load_history:
q = (
    "SELECT role, content FROM chat_history "
    "WHERE session_id = {session_id:String} "
    "ORDER BY timestamp ASC"
)
df = _query_df_params(q, {"session_id": session_id})

# In market_queries.py fetch_candles (and all other query functions):
q = (
    f"SELECT {group_by} AS ts, ... "
    "FROM klines "
    "WHERE symbol = {symbol:String} "
    f"AND timestamp >= now() - INTERVAL {lookback} DAY "
    f"GROUP BY ts ORDER BY ts ASC LIMIT {limit}"
)
return _query_df_params(q, {"symbol": symbol})

# In main.py get_history / delete_history — same pattern.
```

---

### 1.3 Fix OOM Race Condition in `append_to_partition`

**File:** `utils/storage.py:155-196`

**Problem:** Two concurrent threads can read the same partition, each append their data, and one overwrites the other's upload. This is a classic read-modify-write race. During bootstrap, when 4 symbols download the same month concurrently, data silently disappears.

**Why it matters:** You won't notice this in testing (single-threaded) but in production with `ThreadPoolExecutor(max_workers=8)`, you'll get partition files with missing rows. The `ReplacingMergeTree` in ClickHouse can't help here — the data never reaches ClickHouse because it was clobbered in MinIO.

**Fix:** Add a per-key lock using a `threading.Lock` dictionary:
```python
import threading

_partition_locks: dict[str, threading.Lock] = {}
_locks_lock = threading.Lock()

def _get_lock(key: str) -> threading.Lock:
    with _locks_lock:
        if key not in _partition_locks:
            _partition_locks[key] = threading.Lock()
        return _partition_locks[key]

def append_to_partition(bucket, prefix, symbol, new_df, dedup_col, month_str=None):
    month_str = month_str or datetime.now(timezone.utc).strftime(PARTITION_MONTH_FORMAT)
    key = f"{prefix}/{symbol}/{month_str}.parquet"

    with _get_lock(key):
        # ... existing logic (download, concat, dedup, upload) ...
```

---

### 1.4 Fix Bootstrap Memory Accumulation

**File:** `scripts/extract.py:66-119`

**Problem:** `download_data_vision` accumulates all 36 monthly DataFrames in a `downloaded` dict before writing any. With 4 parallel symbol workers × 36 months × ~4MB/month, peak memory is ~576MB of raw data plus the Parquet serialization overhead.

**Why it matters:** On a t3.medium (4GB RAM) with Airflow + ClickHouse + MinIO also running, this can trigger OOM kills. The Airflow scheduler gets killed, the DAG fails mid-bootstrap, and you're left with partial data that's hard to recover from.

**Fix:** Write each month immediately after download instead of accumulating:
```python
def download_data_vision(symbol, months):
    sorted_months = sorted(months)
    if not sorted_months:
        return None

    total = 0
    max_workers = min(BULK_DOWNLOAD_WORKERS, len(sorted_months))

    def _download_and_write(ym):
        y, m = ym
        df = download_klines_month(symbol, y, m)
        if df is not None and not df.empty:
            month_str = f"{y}-{m:02d}"
            key = partition_key(symbol, month_str)
            df = df.sort_values("open_time")
            for c in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[c]):
                    df[c] = df[c].dt.as_unit("us")
            table = pa.Table.from_pandas(df, preserve_index=False)
            storage.upload_parquet(BUCKET_RAW, key, table)
            return len(df)
        return 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_download_and_write, ym): ym for ym in sorted_months}
        for future in as_completed(futures):
            try:
                total += future.result()
            except Exception as exc:
                ym = futures[future]
                logger.error("[%s] %d-%02d: %s", symbol, ym[0], ym[1], exc)

    logger.info("[%s] Bulk complete: %s records", symbol, f"{total:,}")
    return total if total > 0 else None
```

Peak memory drops from ~576MB to ~4MB (one month at a time).

---

### 1.5 Delete Dead Code — `utils/llm_utils.py`

**File:** `utils/llm_utils.py`

**Problem:** This file defines `_call_gemini` and `_call_openai` using raw `aiohttp` calls. Nobody imports `get_chat_response` — the chat API uses LangChain's `ChatDeepSeek`, `ChatOpenAI`, `ChatGoogleGenerativeAI` via `services/chat_api/nodes.py`. The file is dead code.

**Why it matters:** Dead code misleads developers. Someone reading the codebase will think there are two LLM integration paths and waste time understanding which one is active. It also inflates the dependency surface (`aiohttp` is imported but only used here).

**Fix:** Delete the file. Remove `aiohttp` from `requirements.txt` if nothing else uses it (it's only used in this dead file).

---

### 1.6 Extract DRY Loader Pattern

**File:** `scripts/load.py`

**Problem:** `load_klines`, `load_ticker`, `load_order_book` are ~50 lines each with identical structure: get watermarks → discover months → loop symbol×month → download → parse → filter → insert → delete source. Only the table name, bucket, prefix, and column set differ.

**Why it matters:** When you add a new table (e.g., funding rates in Phase 2), you'll copy-paste another 50-line function and introduce subtle bugs in the copy. Every loader already has slightly different error handling and type coercion — divergence will grow over time.

**Fix:**
```python
def _load_table(
    symbols: list[str],
    bucket: str,
    prefix: str,
    table_name: str,
    ts_col: str,
    target_cols: list[str],
    type_coercions: dict[str, str] | None = None,
    month_str: str | None = None,
) -> None:
    """Generic loader: MinIO Parquet → ClickHouse with watermark filtering."""
    symbols = symbols or SYMBOLS
    wm_map = get_table_watermarks(table_name, ts_col, symbols)
    total_inserted = 0
    errors = 0

    for symbol in symbols:
        months = [month_str] if month_str else _discover_months(bucket, prefix, symbol)
        for month in months:
            key = f"{prefix}/{symbol}/{month}.parquet"
            try:
                table = storage.download_parquet(bucket, key)
                df = table.to_pandas()
                del table
                if df.empty:
                    continue

                df[ts_col] = pd.to_datetime(df[ts_col])
                if type_coercions:
                    for col, dtype in type_coercions.items():
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                            if dtype == "uint32":
                                df[col] = df[col].fillna(0).astype("uint32")

                df = _filter_by_watermark(df, ts_col, wm_map.get(symbol))
                if df.empty:
                    continue

                df = df[[c for c in target_cols if c in df.columns]]
                total_inserted += ch_insert_df(table_name, df)

                try:
                    storage.remove_object(bucket, key)
                except Exception:
                    pass
            except Exception as exc:
                errors += 1
                logger.error("[Load] %s %s/%s: %s", table_name, symbol, month, exc)

    if total_inserted == 0 and errors > 0:
        raise LoadError(f"{table_name}: all {errors} partition(s) failed, 0 loaded")
    logger.info("Loaded %s NEW %s rows (%d errors)", f"{total_inserted:,}", table_name, errors)


# The three loaders become:
def load_klines(symbols=None, month_str=None):
    _load_table(symbols, BUCKET_PROCESSED, "features", "klines", "timestamp",
        ["symbol", "timestamp", "open", "high", "low", "close", "volume",
         "quote_volume", "trades", "rsi_14", "macd", "macd_signal"],
        {"trades": "uint32"}, month_str)

def load_ticker(symbols=None, month_str=None):
    _load_table(symbols, BUCKET_RAW, "ticker_24h", "ticker_24h", "snapshot_time",
        ["symbol", "snapshot_time", "price_change", "price_change_pct",
         "high_24h", "low_24h", "volume_24h", "quote_volume_24h",
         "trade_count", "bid_price", "ask_price", "spread_pct"],
        {"trade_count": "uint32"}, month_str)

def load_order_book(symbols=None, month_str=None):
    _load_table(symbols, BUCKET_RAW, "order_book", "order_book_snapshot", "timestamp",
        ["symbol", "timestamp", "total_bid_volume", "total_ask_volume", "imbalance"],
        month_str=month_str)
```

---

### 1.7 Move `init_schema()` Out of Hot Path

**File:** `scripts/load.py:282`

**Problem:** `init_schema()` runs `CREATE DATABASE` + 5× `CREATE TABLE IF NOT EXISTS` (6 DDL statements) on every load invocation. The Airflow DAG invokes `load.py` **three separate times** per minute (`--only klines`, `--only ticker`, `--only orderbook`), so `init_schema()` actually runs **3× per minute** = 18 unnecessary ClickHouse DDL round-trips every minute.

**Why it matters:** 18 DDL round-trips at ~5-10ms each = 90-180ms of wasted time per minute. More importantly, if ClickHouse is briefly unavailable at DAG start, the entire load fails even though the schema already exists.

**Fix:** Call `init_schema()` only in the Airflow init container. In `load.py`, remove the call and add a comment:
```python
# Schema is initialized by the Airflow init container (see docker-compose.yml airflow-init).
# If running manually for the first time: python -c "from utils.db_utils import init_schema; init_schema()"
```

---

### 1.8 Consolidate ClickHouse Access in Chat API

**Files:** `services/chat_api/main.py`

**Problem:** Inconsistent ClickHouse access patterns in the chat API:
- `market_queries.py` uses `new_ch_client()` (create, query, close) ✅ correct
- `nodes.py:load_history` uses `new_ch_client()` (create, query, close) ✅ already fixed
- `nodes.py:save_history` uses `new_ch_client()` (create, insert, close) ✅ correct
- `main.py:get_history` uses `ch_query_df()` — **singleton, blocks async event loop** ❌
- `main.py:delete_history` uses `get_ch_client()` — **singleton, blocks async event loop** ❌

**Why it matters:** The chat API is an async FastAPI app. `main.py:get_history` (line 120) calls `ch_query_df()` which uses the synchronous singleton client inside an `async def` endpoint — this blocks the event loop during the ClickHouse query. Same issue with `delete_history` (line 145) using `get_ch_client()`.

**Fix:** Replace all singleton usage in `main.py` with `new_ch_client()`. Use the parameterized helper from 1.2:
```python
# In main.py get_history:
from market_queries import _query_df_params

q = (
    "SELECT role, content FROM chat_history "
    "WHERE session_id = {session_id:String} "
    "ORDER BY timestamp ASC"
)
df = _query_df_params(q, {"session_id": session_id})

# In main.py delete_history:
client = new_ch_client()
try:
    client.command(
        "ALTER TABLE chat_history DELETE WHERE session_id = {session_id:String}",
        parameters={"session_id": session_id},
    )
finally:
    client.close()
```

Remove the `ch_query_df` and `get_ch_client` imports from `main.py`.

---

## Phase 2: Deepen Analytical Value

### 2.1 Add Derivatives Data — Funding Rates & Open Interest

**Why:** RSI and MACD on spot candles are the most basic indicators in existence. Every exchange shows them for free. They add almost zero analytical edge. What actually moves crypto prices is leverage — and leverage shows up in funding rates, open interest, and long/short ratios. A funding rate of -0.1% (shorts paying longs) combined with rising open interest is a classic short squeeze signal that RSI will never catch.

**Data sources (all free, no API key needed):**
- `GET /fapi/v1/fundingRate` — historical funding rates (every 8h)
- `GET /fapi/v1/openInterest` — current open interest per symbol
- `GET /futures/data/globalLongShortAccountRatio` — top trader long/short ratio
- `GET /futures/data/topLongShortPositionRatio` — top position holder ratio

**New ClickHouse tables:**
```sql
CREATE TABLE IF NOT EXISTS crypto_db.funding_rates (
    symbol String,
    funding_time DateTime,
    funding_rate Float64,
    mark_price Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(funding_time)
ORDER BY (symbol, funding_time);

CREATE TABLE IF NOT EXISTS crypto_db.open_interest (
    symbol String,
    timestamp DateTime,
    open_interest Float64,
    open_interest_value Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);
```

**ETL changes:**
- `extract.py`: Add `extract_funding_rates()` and `extract_open_interest()` functions
- `transform.py`: No transform needed — these are already clean numeric data
- `load.py`: Add `load_funding()` and `load_oi()` — use the generic `_load_table` from 1.6
- `airflow/dags/minutely_etl.py`: Add new extract→load tasks (no dependency on klines)

**Chat API changes:**
- Add new tools: `get_funding_rate(symbol, timeframe)`, `get_open_interest(symbol, timeframe)`
- Update `market_queries.py` with query/format functions
- LLM now sees: "BTC funding rate is -0.08% (shorts paying longs), open interest increased 12% in 24h — potential short squeeze setup"

---

### 2.2 Add Cross-Asset Analysis Capability

**Why:** Crypto doesn't move in isolation. BTC dominance (BTC market cap / total crypto market cap) determines whether altcoins outperform or underperform. A user asking "should I buy SOL?" needs to know: is this an altcoin season (BTC dominance falling) or a BTC-maxi environment (BTC dominance rising)? Your system tracks 50 coins but never compares them.

**Implementation:**
- Compute BTC dominance ratio from ticker data (already available — sum all market caps)
- Add a tool: `get_btc_dominance(timeframe)` that returns the trend
- Add a tool: `compare_performance(symbol1, symbol2, timeframe)` that returns relative performance
- These require no new data collection — just new SQL queries on existing ticker_24h and klines tables

**Example LLM output with this data:**
> "SOL is up 8% this week while BTC is up 3% — SOL is outperforming by 5%. BTC dominance has dropped from 54% to 52% this month, suggesting early altcoin rotation. However, SOL's funding rate is +0.05% (longs paying shorts), indicating crowded long positioning. Consider waiting for a funding rate reset before entering."

That's actual analysis. RSI 65 is not.

---

### 2.3 Add a Backtesting Module

**Why:** You have 3 years of historical data sitting in ClickHouse. This is your biggest asset and you're not using it at all. The chatbot can describe what happened but can't test what *would have* happened. A backtesting module lets users validate strategies before risking real money — this is where the historical pipeline justifies its entire existence.

**New file:** `scripts/backtest.py`

**Design:**
```python
def backtest(
    strategy_fn: Callable[[pd.DataFrame, int], str],  # (data_up_to_row_i, i) → "BUY"|"SELL"|"HOLD"
    symbol: str,
    start_date: str,
    end_date: str,
    initial_capital: float = 10000.0,
    position_size_pct: float = 0.1,  # 10% of capital per trade
) -> BacktestResult:
    """
    Run a strategy against historical klines data.
    
    Returns: BacktestResult with total_return, max_drawdown, sharpe_ratio,
    win_rate, total_trades, equity_curve.
    """
```

**Example usage:**
```python
# Simple RSI strategy
def rsi_strategy(data, i):
    if data.iloc[i]["rsi_14"] < 30:
        return "BUY"
    elif data.iloc[i]["rsi_14"] > 70:
        return "SELL"
    return "HOLD"

result = backtest(rsi_strategy, "BTCUSDT", "2023-01-01", "2024-12-31")
print(f"Total return: {result.total_return:.2%}")
print(f"Max drawdown: {result.max_drawdown:.2%}")
print(f"Win rate: {result.win_rate:.2%}")
```

**Chat API integration:**
- Add a tool: `run_backtest(symbol, strategy_name, start, end)`
- LLM can now say: "I tested the RSI oversold strategy on BTC from 2023-2024. It returned +12% with a max drawdown of -18% and a 43% win rate. The strategy works poorly in trending markets because RSI stays overbought during strong uptrends."

---

### 2.4 Enrich the System Prompt with Computed Signals

**Why:** The current system prompt tells the LLM to "base analysis ONLY on data returned by your tools." The tools return raw formatted text like `O:67500.0000 H:68200.0000 RSI:58.3`. The LLM has to interpret raw numbers — it doesn't know if RSI 58 is trending up or down, whether volume is above or below average, or what the price structure looks like.

**Fix:** Add computed signals to tool outputs. The tools should return pre-interpreted signals, not just raw data.

**In `market_queries.py`, add signal computation:**
```python
def compute_signals(df: pd.DataFrame) -> dict:
    """Compute actionable signals from candle data."""
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest
    
    signals = {}
    
    # RSI trend (not just level)
    rsi = latest["rsi_14"]
    rsi_prev = prev["rsi_14"]
    if rsi > 70:
        signals["rsi"] = f"OVERBOUGHT ({rsi:.1f}, {'rising' if rsi > rsi_prev else 'falling'})"
    elif rsi < 30:
        signals["rsi"] = f"OVERSOLD ({rsi:.1f}, {'falling' if rsi < rsi_prev else 'rising'})"
    else:
        signals["rsi"] = f"NEUTRAL ({rsi:.1f}, {'rising' if rsi > rsi_prev else 'falling'})"
    
    # MACD crossover
    macd = latest["macd"]
    signal = latest["macd_signal"]
    macd_prev = prev["macd"]
    signal_prev = prev["macd_signal"]
    if macd > signal and macd_prev <= signal_prev:
        signals["macd"] = "BULLISH CROSSOVER (just happened)"
    elif macd < signal and macd_prev >= signal_prev:
        signals["macd"] = "BEARISH CROSSOVER (just happened)"
    elif macd > signal:
        signals["macd"] = f"BULLISH (MACD above signal, gap: {macd - signal:.2f})"
    else:
        signals["macd"] = f"BEARISH (MACD below signal, gap: {signal - macd:.2f})"
    
    # Volume trend
    vol_avg = df["volume"].mean()
    vol_latest = latest["volume"]
    signals["volume"] = f"{'ABOVE' if vol_latest > vol_avg else 'BELOW'} average ({vol_latest/vol_avg:.1f}x)"
    
    # Price structure (higher highs / lower lows)
    recent_highs = df["high"].tail(5)
    recent_lows = df["low"].tail(5)
    if recent_highs.is_monotonic_increasing:
        signals["structure"] = "HIGHER HIGHS (bullish structure)"
    elif recent_lows.is_monotonic_decreasing:
        signals["structure"] = "LOWER LOWS (bearish structure)"
    else:
        signals["structure"] = "MIXED (no clear trend structure)"
    
    return signals
```

Append signals to tool output:
```
--- Signals ---
RSI: OVERBOUGHT (72.0, rising)
MACD: BULLISH (MACD above signal, gap: 130.4)
Volume: ABOVE average (1.8x)
Structure: HIGHER HIGHS (bullish structure)
```

The LLM can now write: "BTC shows bullish structure with higher highs, strong volume confirmation (1.8x average), and MACD in bullish territory. However, RSI is overbought at 72 and rising — consider waiting for a pullback to the 60-65 RSI zone before adding to position."

---

### 2.5 Add a LLM Tool for Strategy Backtest

**Why:** Users will ask "does RSI oversold strategy work on ETH?" The LLM should be able to answer with data, not opinion.

**New tool:**
```python
@tool
def backtest_strategy(symbol: str, strategy: str, start_date: str, end_date: str) -> str:
    """Run a backtest of a named strategy on historical data.
    
    Available strategies: rsi_oversold, macd_crossover, rsi_macd_combo.
    
    Args:
        symbol: Crypto trading pair (e.g. BTCUSDT).
        strategy: Strategy name.
        start_date: Start date (YYYY-MM-DD).
        end_date: End date (YYYY-MM-DD).
    """
    # ... run backtest, format results ...
```

---

## Phase 3: Structural Improvements

### 3.1 Separate Transform from ClickHouse Dependency

**File:** `scripts/transform.py`

**Problem:** `_process_symbol` queries ClickHouse for warm-up context (last 120 rows). This couples the transform step to the database — you can't test transform logic without ClickHouse running, and you can't run transform on a machine without ClickHouse access.

**Why it matters:** The transform step should be a pure function: input DataFrame → output DataFrame with indicators. The warm-up context is an optimization (avoids recalculating indicators from scratch), not a core requirement.

**Fix:** Extract the warm-up query into a separate function. Make `calculate_indicators` accept an optional context DataFrame:
```python
def calculate_indicators(
    raw_df: pd.DataFrame,
    context_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute RSI(14) and MACD(12,26,9).
    
    If context_df is provided, prepend it for indicator warm-up.
    Returns only rows from raw_df (context rows are dropped).
    """
    if context_df is not None and not context_df.empty:
        combined = pd.concat([context_df, raw_df], ignore_index=True)
    else:
        combined = raw_df.copy()
    
    combined = combined.sort_values("open_time").drop_duplicates(
        subset=["open_time"], keep="last"
    ).reset_index(drop=True)
    
    # ... RSI and MACD calculation ...
    
    # Keep only rows from raw_df
    if context_df is not None:
        cutoff = context_df["open_time"].max()
        combined = combined[combined["open_time"] > cutoff]
    
    return combined
```

Now `calculate_indicators` is testable without ClickHouse.

---

### 3.2 Move `_discover_months` to Shared Utility

**Files:** `scripts/load.py:89-97`, `scripts/transform.py:175-183`

**Problem:** Same function duplicated in two files. Both discover month partitions in MinIO by listing objects with a prefix.

**Fix:** Move to `utils/storage.py`:
```python
def discover_month_partitions(self, bucket: str, prefix: str, symbol: str) -> list[str]:
    """Find all month partitions for a symbol in MinIO."""
    keys = self.list_objects(bucket, prefix=f"{prefix}/{symbol}/")
    months = []
    for k in keys:
        if k.endswith(".parquet"):
            month = k.split("/")[-1].replace(".parquet", "")
            months.append(month)
    return sorted(months)
```

Both scripts call `storage.discover_month_partitions(...)`.

---

### 3.3 Add Retry to ClickHouse Inserts

**File:** `utils/db_utils.py:82-88`

**Problem:** `ch_insert_df` has no retry. During bulk loads, ClickHouse can reject inserts due to memory pressure or merge overhead. The caller (`load.py`) deletes the source Parquet after a "successful" insert — if the insert fails, data is lost.

**Fix:**
```python
def ch_insert_df(table: str, df: pd.DataFrame, max_retries: int = 3) -> int:
    """Insert a DataFrame into ClickHouse with retry."""
    if df.empty:
        return 0
    
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            client = get_ch_client()
            client.insert_df(table, df)
            return len(df)
        except Exception as exc:
            last_exc = exc
            logger.warning("Insert %s failed (%d/%d): %s", table, attempt, max_retries, exc)
            if attempt < max_retries:
                time.sleep(2 ** attempt)
    
    raise DatabaseConnectionError(f"Insert {table} failed after {max_retries} retries: {last_exc}")
```

---

### 3.4 Add TTL for Order Book Data

**File:** `sql/schema.sql`

**Problem:** Order book snapshots are stored indefinitely. A 1-minute snapshot from 6 months ago has zero analytical value — the order book has changed millions of times since then. Storing 3 years of order book data wastes disk and slows queries.

**Fix:**
```sql
CREATE TABLE IF NOT EXISTS crypto_db.order_book_snapshot (
    symbol String,
    timestamp DateTime,
    total_bid_volume Float64,
    total_ask_volume Float64,
    imbalance Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp)
TTL timestamp + INTERVAL 90 DAY;  -- Auto-delete after 90 days
```

Same for `ticker_24h` — keep 180 days, not forever.

---

## Implementation Order

| Priority | Task | Impact | Effort |
|----------|------|--------|--------|
| P0 | 1.1 Thread-safe ClickHouse client | Prevents data corruption in transform | 30 min |
| P0 | 1.2 SQL injection fix (session_id + symbol) | Security vulnerability | 1 hour |
| P0 | 1.3 Partition write race condition | Prevents data loss during bootstrap | 1 hour |
| P1 | 1.4 Bootstrap memory fix | Prevents OOM kills | 1 hour |
| P1 | 1.5 Delete dead code | Reduces confusion | 10 min |
| P1 | 1.6 DRY loader pattern | Reduces future bugs | 1 hour |
| P1 | 1.8 Fix main.py async blocking | Fixes event loop blocking | 30 min |
| P2 | 2.1 Add derivatives data (funding, OI) | 10x analytical value | 3 hours |
| P2 | 2.4 Enrich system prompt signals | Better LLM analysis | 2 hours |
| P2 | 3.1 Separate transform from CH | Testability | 1 hour |
| P2 | 3.2 Move discover_months to shared | DRY | 15 min |
| P2 | 3.3 Add retry to CH inserts | Reliability | 30 min |
| P2 | 3.4 Add TTL for order book | Storage efficiency | 15 min |
| P3 | 1.7 Move init_schema out of hot path | Saves 18 DDL/min | 15 min |
| P3 | 2.2 Cross-asset analysis | Better analysis | 2 hours |
| P3 | 2.3 Backtesting module | Justifies historical data | 4 hours |
| P3 | 2.5 Backtest LLM tool | End-to-end analysis | 2 hours |
