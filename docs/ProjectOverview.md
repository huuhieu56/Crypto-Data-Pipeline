# Hệ thống Data Pipeline Big Data & LLM Advisor - Tư vấn giao dịch Crypto

---

## 1. Tổng quan Dự án

### 1.1. Bối cảnh & Vấn đề

Thị trường cryptocurrency hoạt động 24/7 với khối lượng giao dịch khổng lồ. Việc phân tích và đưa ra quyết định giao dịch đòi hỏi:

- Xử lý dữ liệu lớn (hàng triệu records)
- Pipeline tự động, không can thiệp thủ công
- Hệ thống AI có khả năng phân tích đa chiều và đưa ra tín hiệu tư vấn (BUY/SELL/HOLD)

### 1.2. Mục tiêu Đồ án

Xây dựng hệ thống **End-to-End Data Pipeline** minh họa vai trò của Data Engineer + AI Engineer:

| Giai đoạn       | Công nghệ                 | Mục đích                                       |
| --------------- | ------------------------- | ---------------------------------------------- |
| **Extract**     | Python + Binance API      | Thu thập dữ liệu nến từ sàn giao dịch          |
| **Transform**   | Apache Spark              | Tính toán chỉ số kỹ thuật (RSI, MACD)          |
| **Load**        | ClickHouse                | Lưu trữ có cấu trúc, phục vụ analytics         |
| **Store**       | MinIO (S3-compatible)     | Object storage cho raw/processed data (Data Lake) |
| **Orchestrate** | Apache Airflow            | Tự động hóa và lập lịch các jobs               |
| **Advise**      | LLM (Gemini / OpenAI)    | Phân tích thị trường, sinh tín hiệu BUY/SELL/HOLD |
| **Visualize**   | Grafana                   | Dashboard theo dõi real-time                   |

### 1.3. Phạm vi & Giới hạn

| Thành phần        | Giá trị                   | Ghi chú                          |
| ----------------- | ------------------------- | -------------------------------- |
| Số lượng coin     | 50                        | Top vốn hóa, loại bỏ stablecoins |
| Khung thời gian   | Nến 1 phút                | Phù hợp phân tích ngắn hạn       |
| Dữ liệu lịch sử  | 3 năm (01/2023 - 01/2026) | Đủ để phân tích xu hướng          |
| Tín hiệu tư vấn  | BUY / SELL / HOLD          | Dựa trên 30 nến daily + snapshot |

---

## 2. Danh sách 50 Coins

> **Tiêu chí chọn:** Top 50 theo vốn hóa thị trường, loại bỏ stablecoins (USDT, USDC, DAI) vì không có biến động giá.

| #   | Symbol    | #   | Symbol   | #   | Symbol     | #   | Symbol    | #   | Symbol   |
| --- | --------- | --- | -------- | --- | ---------- | --- | --------- | --- | -------- |
| 1   | BTCUSDT   | 11  | AVAXUSDT | 21  | NEARUSDT   | 31  | INJUSDT   | 41  | FTMUSDT  |
| 2   | ETHUSDT   | 12  | TONUSDT  | 22  | APTUSDT    | 32  | IMXUSDT   | 42  | ALGOUSDT |
| 3   | BNBUSDT   | 13  | SHIBUSDT | 23  | ICPUSDT    | 33  | OPUSDT    | 43  | FLOWUSDT |
| 4   | SOLUSDT   | 14  | XLMUSDT  | 24  | ETCUSDT    | 34  | GRTUSDT   | 44  | XTZUSDT  |
| 5   | XRPUSDT   | 15  | BCHUSDT  | 25  | STXUSDT    | 35  | THETAUSDT | 45  | AXSUSDT  |
| 6   | DOGEUSDT  | 16  | DOTUSDT  | 26  | RENDERUSDT | 36  | FILUSDT   | 46  | SANDUSDT |
| 7   | ADAUSDT   | 17  | UNIUSDT  | 27  | CROUSDT    | 37  | ARUSDT    | 47  | MANAUSDT |
| 8   | TRXUSDT   | 18  | LTCUSDT  | 28  | ATOMUSDT   | 38  | MKRUSDT   | 48  | NEOUSDT  |
| 9   | LINKUSDT  | 19  | HBARUSDT | 29  | VETUSDT    | 39  | WIFUSDT   | 49  | EOSUSDT  |
| 10  | MATICUSDT | 20  | PEPEUSDT | 30  | ARBUSDT    | 40  | RUNEUSDT  | 50  | AAVEUSDT |

---

## 3. Ước tính Khối lượng Dữ liệu

### 3.1. Bảng `klines` (Fact Table chính)

```
Số nến = 50 coins × 3 năm × 365 ngày × 24 giờ × 60 phút
       = 50 × 3 × 525,600
       = 78,840,000 records

Kích thước = 78.84 triệu × 100 bytes/record ≈ 7.5 GB
```

### 3.2. Bảng `ticker_24h` (Daily snapshot)

```
Số records = 50 coins × 3 năm × 365 ngày
           = 50 × 3 × 365
           = 54,750 records

Kích thước = 54,750 × 150 bytes/record ≈ 8 MB
```

### 3.3. Bảng `llm_signals` (Hourly advisory - 1 lần/giờ)

```
Số records = 50 coins × 3 năm × 365 ngày × 24 lần/ngày
           = 50 × 3 × 365 × 24
           = 1,314,000 records

Kích thước = 1,314,000 × 200 bytes/record ≈ 250 MB
```

### 3.4. Tổng hợp theo bảng

| Bảng                  | Số records     | Kích thước   | Tần suất ghi     |
| --------------------- | -------------- | ------------ | ---------------- |
| `symbols`             | 50             | < 1 KB       | 1 lần (setup)    |
| `klines`              | 78,840,000     | ~7.5 GB      | Daily batch      |
| `ticker_24h`          | 54,750         | ~8 MB        | 50 recs/ngày      |
| `order_book_snapshot`  | 54,750         | ~5 MB        | 50 recs/ngày      |
| `llm_signals`         | 1,314,000      | ~250 MB      | 50 recs/giờ      |
| **Total**             | **~80.3 triệu** | **~7.8 GB** | -               |

### 3.5. Breakdown theo layer

| Layer                 | Format          | Kích thước | Ghi chú                               |
| --------------------- | --------------- | ---------- | ------------------------------------- |
| Raw (Data Lake)       | CSV → MinIO     | ~7.5 GB    | Dữ liệu gốc từ Binance               |
| Processed (Data Lake) | Parquet → MinIO | ~2 GB      | Nén tốt hơn CSV, có indicators        |
| Warehouse             | ClickHouse      | ~5 GB      | Bao gồm index + llm_signals           |

---

## 4. Kiến trúc Hệ thống

### 4.1. Tổng quan Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                 ORCHESTRATION                                    │
│                            ┌─────────────────────┐                              │
│                            │   Apache Airflow    │                              │
│                            │   (Scheduler)       │                              │
│                            └──────────┬──────────┘                              │
│                                       │ triggers                                │
├───────────────────────────────────────┼─────────────────────────────────────────┤
│                                       ▼                                         │
│  ┌─────────────┐    ┌─────────────────────────────────┐    ┌─────────────────┐ │
│  │   SOURCE    │    │           DATA LAKE             │    │ DATA WAREHOUSE  │ │
│  │             │    │  ┌───────────┐ ┌─────────────┐  │    │                 │ │
│  │  Binance    │───▶│  │  MinIO    │ │  MinIO      │  │───▶│   ClickHouse    │ │
│  │  API        │    │  │  Raw/CSV  │ │  Processed/ │  │    │                 │ │
│  │             │    │  │           │ │  Parquet    │  │    │                 │ │
│  └─────────────┘    │  └───────────┘ └─────────────┘  │    └────────┬────────┘ │
│                     │        │              ▲         │             │          │
│                     │        └──── Spark ───┘         │             │          │
│                     └─────────────────────────────────┘             │          │
├─────────────────────────────────────────────────────────────────────┼──────────┤
│                                                                     ▼          │
│  ┌─────────────────────────────────┐    ┌─────────────────────────────────┐   │
│  │        LLM ADVISOR LAYER        │    │         VISUALIZATION           │   │
│  │  ┌───────────┐ ┌─────────────┐  │    │                                 │   │
│  │  │  Gemini / │ │  Advisory   │  │    │         Grafana                 │   │
│  │  │  OpenAI   │ │  Signals    │  │    │         Dashboard               │   │
│  │  └───────────┘ └─────────────┘  │    │                                 │   │
│  └─────────────────────────────────┘    └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2. Data Lake vs Data Warehouse

| Khía cạnh         | Data Lake                             | Data Warehouse              |
| ----------------- | ------------------------------------- | --------------------------- |
| **Vị trí**        | MinIO buckets (`crypto-raw`, `crypto-processed`) | ClickHouse database  |
| **Format**        | CSV (raw), Parquet (processed)        | Bảng SQL có schema          |
| **Schema**        | Schema-on-read (linh hoạt)            | Schema-on-write (cố định)  |
| **Mục đích**      | Lưu trữ, batch processing             | Query nhanh, serving        |
| **Công cụ xử lý** | Apache Spark                          | SQL queries                 |
| **Ai dùng**       | Data Engineer, Data Scientist         | Analyst, Dashboard, LLM     |

### 4.3. Luồng dữ liệu chi tiết

```
[1] EXTRACT                    [2] TRANSFORM                 [3] LOAD
    Binance API                    Apache Spark                  ClickHouse
         │                              │                             │
         ▼                              ▼                             ▼
    ┌─────────┐                  ┌─────────────┐              ┌─────────────┐
    │  OHLCV  │      Spark       │  + RSI      │   clickhouse │   klines    │
    │  data   │  ──────────────▶ │  + MACD     │  ─────────▶  │   table     │
    │  (CSV)  │   Pandas UDF     │  (Parquet)  │   -connect   │             │
    └─────────┘                  └─────────────┘              └─────────────┘

    MinIO: crypto-raw/           MinIO: crypto-processed/       crypto_db
    BTCUSDT.csv                  features.parquet
```

---

## 5. Database Schema

### 5.1. Tổng quan - Star Schema (5 bảng)

```
                         ┌─────────────────────┐
                         │      symbols        │
                         │  (Dimension Table)  │
                         │                     │
                         │  symbol (PK)        │
                         │  base_asset         │
                         │  quote_asset        │
                         │  status             │
                         └──────────┬──────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐
│       klines         │ │     ticker_24h       │ │     llm_signals      │
│    (Fact Table)      │ │    (Fact Table)      │ │    (Fact Table)      │
│                      │ │                      │ │                      │
│  symbol (FK) ────────│─│── symbol (FK) ───────│─│── symbol (FK)        │
│  timestamp (PK)      │ │  snapshot_time (PK)  │ │  generated_at (PK)   │
│  open, high, low...  │ │  price_change_24h    │ │  signal, confidence  │
│  rsi, macd...        │ │  volume_24h...       │ │  reason, key_risk    │
└──────────────────────┘ └──────────────────────┘ └──────────────────────┘
                         ┌──────────────────────────┐
                         │   order_book_snapshot    │
                         │       (Fact Table)       │
                         │                          │
                         │  symbol (FK)             │
                         │  timestamp (PK)          │
                         │  total_bid_volume        │
                         │  total_ask_volume        │
                         │  imbalance               │
                         └──────────────────────────┘
```

> **Thiết kế Star Schema:** 1 Dimension table (`symbols`) + 4 Fact tables (`klines`, `ticker_24h`, `order_book_snapshot`, `llm_signals`)

### 5.2. Nguồn dữ liệu

| Bảng                  | Nguồn dữ liệu                                             | Mô tả                                    | Tần suất thu thập |
| --------------------- | ---------------------------------------------------------- | ---------------------------------------- | ----------------- |
| `symbols`             | Binance `/api/v3/exchangeInfo`                             | Thông tin trading pair                   | 1 lần (setup)     |
| `klines`              | Binance `/api/v3/klines`                                   | Dữ liệu nến (OHLCV) + indicators        | Daily (02:00 AM)  |
| `ticker_24h`          | Binance `/api/v3/ticker/24hr` + `/api/v3/ticker/bookTicker`| Thống kê 24h + best bid/ask + spread     | Daily             |
| `order_book_snapshot`  | Binance `/api/v3/depth`                                    | Snapshot order book để đo áp lực mua/bán | Daily             |
| `llm_signals`         | LLM API (Gemini / OpenAI)                                  | Tín hiệu tư vấn BUY/SELL/HOLD           | Mỗi giờ           |

### 5.3. Bảng `symbols` (Dimension Table)

> **Mục đích:** Lưu thông tin tĩnh về 50 coins, tránh lặp lại trong các Fact tables.

| Column        | Type     | Description                 | Nguồn                |
| ------------- | -------- | --------------------------- | -------------------- |
| `symbol`      | String   | Cặp giao dịch (PK)          | Binance exchangeInfo |
| `base_asset`  | String   | Coin gốc (BTC, ETH...)      | Binance exchangeInfo |
| `quote_asset` | String   | Coin định giá (USDT)        | Binance exchangeInfo |
| `status`      | String   | Trạng thái (TRADING, BREAK) | Binance exchangeInfo |
| `created_at`  | DateTime | Thời gian thêm vào hệ thống | System               |

**Primary Key (ORDER BY):** `symbol`

**Engine:** `ReplacingMergeTree()`

**Sample Data:**

```
| symbol   | base_asset | quote_asset | status  | created_at          |
|----------|------------|-------------|---------|---------------------|
| BTCUSDT  | BTC        | USDT        | TRADING | 2023-01-01 00:00:00 |
| ETHUSDT  | ETH        | USDT        | TRADING | 2023-01-01 00:00:00 |
```

### 5.4. Bảng `klines` (Fact Table)

> **Mục đích:** Lưu dữ liệu nến 1 phút và các chỉ số kỹ thuật đã tính toán.

| Column         | Type             | Description          | Nguồn           |
| -------------- | ---------------- | -------------------- | --------------- |
| `symbol`       | String           | → symbols.symbol     | Binance         |
| `timestamp`    | DateTime         | Thời gian mở nến     | Binance         |
| `open`         | Float64          | Giá mở cửa           | Binance         |
| `high`         | Float64          | Giá cao nhất         | Binance         |
| `low`          | Float64          | Giá thấp nhất        | Binance         |
| `close`        | Float64          | Giá đóng cửa         | Binance         |
| `volume`       | Float64          | Khối lượng giao dịch | Binance         |
| `quote_volume` | Float64          | Volume (USDT)        | Binance         |
| `trades`       | UInt32           | Số lượng trades      | Binance         |
| `rsi_14`       | Nullable(Float64)| RSI 14 periods       | Spark Transform |
| `macd`         | Nullable(Float64)| MACD line            | Spark Transform |
| `macd_signal`  | Nullable(Float64)| MACD signal line     | Spark Transform |

**ORDER BY:** `(symbol, timestamp)`

**PARTITION BY:** `toYYYYMM(timestamp)`

**Engine:** `ReplacingMergeTree()`

### 5.5. Bảng `ticker_24h` (Fact Table)

> **Mục đích:** Lưu snapshot thống kê 24h mỗi ngày, dùng để phân tích volume trend và market sentiment.

| Column             | Type    | Description                       | Nguồn               |
| ------------------ | ------- | --------------------------------- | ------------------- |
| `symbol`           | String  | → symbols.symbol                  | Binance ticker/24hr |
| `snapshot_time`    | DateTime| Thời điểm lấy snapshot            | System              |
| `price_change`     | Float64 | Chênh lệch giá 24h (absolute)     | Binance ticker/24hr |
| `price_change_pct` | Float64 | Chênh lệch giá 24h (%)            | Binance ticker/24hr |
| `high_24h`         | Float64 | Giá cao nhất trong 24h            | Binance ticker/24hr |
| `low_24h`          | Float64 | Giá thấp nhất trong 24h           | Binance ticker/24hr |
| `volume_24h`       | Float64 | Volume giao dịch 24h (base asset) | Binance ticker/24hr |
| `quote_volume_24h` | Float64 | Volume 24h (quote asset - USDT)   | Binance ticker/24hr |
| `trade_count`      | UInt32  | Số lượng trades trong 24h         | Binance ticker/24hr |
| `bid_price`        | Float64 | Best bid                          | Binance bookTicker  |
| `ask_price`        | Float64 | Best ask                          | Binance bookTicker  |
| `spread_pct`       | Float64 | (ask - bid) / ask \* 100          | Calculated          |

**ORDER BY:** `(symbol, snapshot_time)`

**Engine:** `ReplacingMergeTree()`

**Sample Data:**

```
| symbol  | snapshot_time       | price_change_pct | volume_24h    | trade_count |
|---------|---------------------|------------------|---------------|-------------|
| BTCUSDT | 2026-01-26 00:00:00 | 2.35             | 45,230.5 BTC  | 1,245,678   |
| ETHUSDT | 2026-01-26 00:00:00 | -1.20            | 312,450.2 ETH | 892,345     |
```

> **Ý nghĩa:** Dữ liệu này giúp phân tích market sentiment, detect unusual volume spikes, và là context đầu vào cho LLM advisory.

### 5.6. Bảng `llm_signals` (Fact Table)

> **Mục đích:** Lưu kết quả tín hiệu tư vấn từ LLM mỗi giờ, bao gồm tín hiệu, mức độ tin cậy, lý do, và snapshot thị trường tại thời điểm phân tích.

| Column              | Type                   | Description                                | Nguồn         |
| ------------------- | ---------------------- | ------------------------------------------ | ------------- |
| `symbol`            | String                 | → symbols.symbol                           | System        |
| `generated_at`      | DateTime               | Thời điểm chạy LLM (mỗi giờ)               | System        |
| `signal`            | LowCardinality(String) | Tín hiệu: BUY / SELL / HOLD              | LLM           |
| `confidence`        | UInt8                  | Mức tin cậy (1-5)                          | LLM           |
| `reason`            | String                 | Lý do tín hiệu (max 240 ký tự)            | LLM           |
| `key_risk`          | Nullable(String)       | Rủi ro chính (max 120 ký tự)              | LLM           |
| `rsi_14`            | Nullable(Float64)      | RSI tại thời điểm phân tích                | Snapshot      |
| `macd_cross`        | LowCardinality(String) | MACD crossover: bullish/bearish/neutral    | Calculated    |
| `ob_imbalance`      | Nullable(Float64)      | Order book imbalance (0-1)                 | Snapshot      |
| `vol_change_pct`    | Nullable(Float64)      | Thay đổi volume 24h (%)                    | Calculated    |
| `price_change_pct`  | Nullable(Float64)      | Thay đổi giá 24h (%)                       | Ticker        |
| `data_window_minutes`| UInt16                | Cửa sổ dữ liệu đầu vào (phút)             | Config        |
| `trend_6h`          | LowCardinality(String) | Xu hướng: UPTREND/DOWNTREND/SIDEWAYS      | Calculated    |
| `trend_6h_pct`      | Nullable(Float64)      | % thay đổi theo xu hướng                   | Calculated    |
| `llm_provider`      | LowCardinality(String) | Provider: gemini / openai                  | Config        |
| `model_version`     | String                 | Version (e.g. daily30_v1)                  | System        |
| `created_at`        | DateTime               | Thời gian ghi vào DB                       | System        |

**ORDER BY:** `(symbol, generated_at)`

**PARTITION BY:** `toYYYYMM(generated_at)`

**Engine:** `ReplacingMergeTree()`

**Sample Data:**

```
| symbol  | generated_at        | signal | confidence | reason                        | key_risk              |
|---------|---------------------|--------|------------|-------------------------------|-----------------------|
| BTCUSDT | 2026-01-26 14:00:00 | BUY    | 4          | RSI oversold, MACD bullish    | High volatility risk  |
| ETHUSDT | 2026-01-26 14:00:00 | HOLD   | 3          | Sideways trend, mixed signals | Breakout uncertainty  |
| SOLUSDT | 2026-01-26 14:00:00 | SELL   | 4          | RSI overbought, volume drop   | Sudden reversal risk  |
```

> **Ý nghĩa:** Tách signals ra bảng riêng giúp:
>
> - Theo dõi performance của LLM advisor theo thời gian
> - So sánh giữa các LLM providers (Gemini vs OpenAI)
> - Phân tích phân bố tín hiệu (BUY/SELL/HOLD ratio)
> - Dashboard hiển thị advisory real-time

### 5.7. Bảng `order_book_snapshot` (Fact Table)

> **Mục đích:** Theo dõi áp lực mua/bán theo snapshot order book.

| Column             | Type    | Description                         | Nguồn         |
| ------------------ | ------- | ----------------------------------- | ------------- |
| `symbol`           | String  | → symbols.symbol                    | Binance depth |
| `timestamp`        | DateTime| Thời điểm snapshot                  | System        |
| `total_bid_volume` | Float64 | Tổng khối lượng bid (top N levels)  | Calculated    |
| `total_ask_volume` | Float64 | Tổng khối lượng ask (top N levels)  | Calculated    |
| `imbalance`        | Float64 | total_bid / (total_bid + total_ask) | Calculated    |

**ORDER BY:** `(symbol, timestamp)`

**Engine:** `ReplacingMergeTree()`

> **Ý nghĩa:** `imbalance` > 0.5 → lực mua mạnh, < 0.5 → lực bán mạnh. Dữ liệu này là một trong các input cho LLM advisory.

### 5.8. Ví dụ SQL Queries (ClickHouse)

**Query 1:** Snapshot mới nhất theo symbol: close + ticker + advisory

```sql
SELECT
    s.symbol,
    s.base_asset,
    k.latest_close,
    t.price_change_pct AS change_24h_pct,
    l.signal,
    l.confidence,
    l.reason,
    l.generated_at
FROM symbols s
LEFT JOIN (
    SELECT symbol, argMax(close, timestamp) AS latest_close
    FROM klines GROUP BY symbol
) k ON s.symbol = k.symbol
LEFT JOIN (
    SELECT symbol, argMax(price_change_pct, snapshot_time) AS price_change_pct
    FROM ticker_24h GROUP BY symbol
) t ON s.symbol = t.symbol
LEFT JOIN (
    SELECT symbol, argMax(signal, generated_at) AS signal,
           argMax(confidence, generated_at) AS confidence,
           argMax(reason, generated_at) AS reason,
           max(generated_at) AS generated_at
    FROM llm_signals GROUP BY symbol
) l ON s.symbol = l.symbol
ORDER BY s.symbol;
```

**Query 2:** Phân bố tín hiệu theo giờ trong 7 ngày

```sql
SELECT
    toStartOfHour(generated_at) AS hour,
    countIf(signal = 'BUY') AS buy_count,
    countIf(signal = 'SELL') AS sell_count,
    countIf(signal = 'HOLD') AS hold_count,
    round(avg(confidence), 2) AS avg_confidence
FROM llm_signals
WHERE generated_at >= now() - INTERVAL 7 DAY
GROUP BY hour
ORDER BY hour DESC;
```

**Query 3:** KPI advisory tổng quan (7 ngày)

```sql
SELECT
    count() AS total_signals,
    round(avg(confidence), 2) AS avg_confidence,
    round(countIf(signal = 'BUY') * 100.0 / count(), 2) AS buy_ratio_pct,
    round(countIf(signal = 'SELL') * 100.0 / count(), 2) AS sell_ratio_pct,
    round(countIf(signal = 'HOLD') * 100.0 / count(), 2) AS hold_ratio_pct
FROM llm_signals
WHERE generated_at >= now() - INTERVAL 7 DAY;
```

### 5.9. Giải thích các chỉ số kỹ thuật

| Chỉ số          | Công thức          | Ý nghĩa                                         |
| --------------- | ------------------ | ----------------------------------------------- |
| **RSI (14)**    | 100 - 100/(1 + RS) | Đo momentum, > 70 = overbought, < 30 = oversold |
| **MACD**        | EMA(12) - EMA(26)  | Đo xu hướng và momentum                         |
| **MACD Signal** | EMA(9) của MACD    | Tín hiệu mua/bán khi MACD cắt Signal            |

---

## 6. ETL Pipeline

### 6.1. Tổng quan luồng dữ liệu (5 bảng)

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    EXTRACT                                               │
│                                                                                          │
│   ┌─────────────────┐   ┌─────────────────────┐   ┌─────────────────────────┐   ┌───────────────────┐ │
│   │ /exchangeInfo   │   │    /klines          │   │    /ticker/24hr         │   │    /depth         │ │
│   │ (1 lần setup)   │   │ (Daily: 50 coins)   │   │ (Daily: 50 coins)       │   │ (Daily)            │ │
│   └────────┬────────┘   └──────────┬──────────┘   └────────────┬────────────┘   └─────────┬─────────┘ │
│            │                       │                           │                           │           │
│            ▼                       ▼                           ▼                           ▼           │
│      symbols.json          MinIO: crypto-raw/           ticker_24h.csv           order_book_snapshot.csv │
└────────────┬───────────────────────┬───────────────────────────┬───────────────────────────┬───────────┘
             │                       │                           │
             │              ┌────────┴────────┐                  │
             │              │    TRANSFORM    │                  │
             │              │   (Spark)       │                  │
             │              │  + RSI, MACD    │                  │
             │              └────────┬────────┘                  │
             │                       │                           │
             │                       ▼                           │
             │           MinIO: crypto-processed/                │
             │              features.parquet                     │
             │                       │                           │
┌────────────┴───────────────────────┴───────────────────────────┴────────────────────────┐
│                                      LOAD                                                │
│                                                                                          │
│    ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌──────────────────────┐   ┌───────────────┐ │
│    │    symbols      │   │     klines      │   │   ticker_24h    │   │ order_book_snapshot │   │  llm_signals  │ │
│    │  (dim table)    │   │  (fact table)   │   │  (fact table)   │   │   (fact table)      │   │ (fact table)  │ │
│    └─────────────────┘   └─────────────────┘   └─────────────────┘   └──────────────────────┘   └───────────────┘ │
│                                                                                          │
│                              ClickHouse Database                                         │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2. Extract - Thu thập dữ liệu

#### 6.2.1. Extract Symbols (1 lần khi setup)

| Thuộc tính | Giá trị                 |
| ---------- | ----------------------- |
| API        | `/api/v3/exchangeInfo`  |
| Tần suất   | 1 lần (initial setup)   |
| Output     | `data/raw/symbols.json` |
| Ghi vào    | Bảng `symbols`          |

#### 6.2.2. Extract Klines (Daily batch)

| Thuộc tính | Giá trị                                                                |
| ---------- | ---------------------------------------------------------------------- |
| API        | `/api/v3/klines`                                                       |
| Tần suất   | Daily (02:00 AM)                                                       |
| Chiến lược | Bulk download từ Binance Data Vision (lịch sử), REST API (dữ liệu mới) |
| Output     | MinIO bucket `crypto-raw/{SYMBOL}.csv`                                 |
| Ghi vào    | Bảng `klines` (sau Transform)                                          |

> **Lưu ý:** Extract initial nặng (bulk 3 năm × 50 coins = 78M rows). Sau khi bulk xong, mỗi ngày chỉ lấy dữ liệu mới từ REST API. Transform + Load với Spark.

#### 6.2.3. Extract Ticker 24h + Best Bid/Ask (Daily)

| Thuộc tính | Giá trị                                            |
| ---------- | -------------------------------------------------- |
| API        | `/api/v3/ticker/24hr`, `/api/v3/ticker/bookTicker` |
| Tần suất   | Daily (cùng daily_etl DAG)                         |
| Đặc điểm   | 1 request lấy được tất cả 50 coins                 |
| Output     | `data/raw/ticker_24h.csv`                          |
| Ghi vào    | Bảng `ticker_24h`                                  |

> **Lưu ý:** API ticker/24hr và bookTicker trả về snapshot tại thời điểm gọi, không phải dữ liệu lịch sử. Mỗi ngày lưu 1 record/coin.

#### 6.2.4. Extract Order Book Snapshot (Daily)

| Thuộc tính | Giá trị                            |
| ---------- | ---------------------------------- |
| API        | `/api/v3/depth`                    |
| Tần suất   | Daily, cùng daily_snapshot DAG     |
| Đặc điểm   | Snapshot top N levels              |
| Output     | `data/raw/order_book_snapshot.csv` |
| Ghi vào    | Bảng `order_book_snapshot`         |

> **Lưu ý:** Lấy 1 snapshot/ngày để đơn giản hóa pipeline. Đủ để phân tích xu hướng áp lực mua/bán theo ngày mà không gây rate limit.

### 6.3. Transform - Xử lý với Spark

**Input:** CSV files từ Data Lake (MinIO `crypto-raw/`)

**Xử lý:**

1. Đọc tất cả CSV vào Spark DataFrame
2. Tính RSI(14) và MACD sử dụng Pandas UDF (`groupBy("symbol").applyInPandas`)
3. Xử lý missing values (forward fill, back fill, fillna)

**Output:** MinIO `crypto-processed/features.parquet`

> **Tại sao dùng Spark?** Dữ liệu 78 triệu records, Pandas sẽ chậm và tốn RAM. Spark xử lý distributed, tối ưu cho batch processing.

### 6.4. Load - Ghi vào ClickHouse

| Bảng                  | Input                   | Mode   | Ghi chú                              |
| --------------------- | ----------------------- | ------ | ------------------------------------- |
| `symbols`             | symbols.json            | Upsert | 1 lần setup, update nếu cần           |
| `klines`              | features.parquet        | Append | Dữ liệu mới daily (daily_etl)        |
| `ticker_24h`          | ticker_24h.csv          | Append | 50 records/ngày (daily_etl)           |
| `order_book_snapshot`  | order_book_snapshot.csv | Append | 50 records/ngày (daily_snapshot)      |
| `llm_signals`         | (từ LLM inference)      | Append | 50 records/giờ (hourly_inference)     |

---

## 7. Job Scheduling với Apache Airflow

### 7.1. Tại sao cần Airflow?

| Vấn đề khi chạy thủ công  | Giải pháp Airflow    |
| ------------------------- | -------------------- |
| Quên chạy ETL             | Tự động theo lịch    |
| Job A phải chờ job B xong | Dependencies rõ ràng |
| Không biết lỗi ở đâu      | Logs, alerts, retry  |
| Khó theo dõi tiến độ      | Web UI visualization |

### 7.2. Danh sách DAGs

| DAG                  | Schedule        | Mô tả                                                  |
| -------------------- | --------------- | ------------------------------------------------------- |
| `daily_etl`          | 0 2 * * *       | Pre-extract → Extract klines/ticker → Transform → Load  |
| `daily_snapshot`     | 0 0 * * *       | Ticker 24h + Order Book snapshot hàng ngày               |
| `hourly_inference`   | 0 * * * *       | Sinh tín hiệu LLM advisory (BUY/SELL/HOLD) mỗi giờ      |

### 7.3. DAG: daily_etl

```
Trigger: 02:00 AM mỗi ngày (0 2 * * *)
Timeout: 2 giờ
max_active_runs: 1 (tránh overlap)

Note: ETL nặng chỉ lần đầu (bulk 3 năm × 50 coins = 78M rows, dùng Spark).
      Sau đó mỗi ngày chỉ xử lý dữ liệu mới.

┌──────────────┐
│  pre_extract │
│              │
│ - Gap detect │
│ - Recovery   │
└──────┬───────┘
       │
       ├───────────────────────────┐
       ▼                           ▼
┌──────────────────┐     ┌──────────────────┐
│ extract_klines   │     │ extract_ticker   │
│                  │     │                  │
│ - GET /klines    │     │ - GET /ticker    │
│ - 50 coins       │     │ - GET /bookTicker│
└────────┬─────────┘     └────────┬─────────┘
         │                        │
         ▼                        ▼
┌──────────────────┐     ┌──────────────────┐
│    transform     │     │   load_ticker    │
│                  │     │                  │
│ - Spark job      │     │ - ClickHouse     │
│ - Calc RSI, MACD │     │ - ticker_24h     │
└────────┬─────────┘     └──────────────────┘
         │
         ▼
┌──────────────────┐
│  load_klines     │
│                  │
│ - ClickHouse     │
│ - klines table   │
└──────────────────┘
```

> **Dependencies:** `pre_extract >> [extract_klines, extract_ticker]`, `extract_klines >> transform >> load_klines`, `extract_ticker >> load_ticker`

### 7.4. DAG: daily_snapshot

```
Trigger: 00:00 AM mỗi ngày (0 0 * * *)
Timeout: 15 phút

┌────────────────────┐     ┌────────────────────┐
│  extract_ticker    │───▶│    load_ticker      │
│                    │     │                    │
│- GET /ticker/24hr  │     │ - ClickHouse       │
│- GET /bookTicker   │     │ - ticker_24h table │
│- Save CSV          │     │                    │
└────────────────────┘     └────────────────────┘
       ~3 giây                    ~5 giây

┌────────────────────┐     ┌────────────────────┐
│ extract_order_book │───▶│  load_order_book    │
│                    │     │                    │
│- GET /depth        │     │ - ClickHouse       │
│- 50 coins          │     │ - order_book table │
│- Save CSV          │     │                    │
└────────────────────┘     └────────────────────┘
       ~10 giây                   ~5 giây

(2 nhánh chạy song song)
```

> **Note:** Ticker 24h là rolling 24h snapshot, Order book là snapshot áp lực mua/bán — cả hai lấy 1 lần/ngày là đủ cho phân tích.

### 7.5. DAG: hourly_inference

```
Trigger: Đầu mỗi giờ (0 * * * *)
Timeout: 10 phút
max_active_runs: 1

┌───────────────────────────────────────────────────────────────────────────────┐
│                              llm_signal task                                   │
│                                                                                │
│  Cho mỗi symbol (batch 10):                                                   │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────────────┐ │
│  │ Fetch context    │───▶│  Call LLM API    │───▶│  Save to llm_signals    │ │
│  │                  │    │                  │    │                          │ │
│  │ - 30 daily klines│    │ - Gemini/OpenAI  │    │ - INSERT into ClickHouse │ │
│  │ - Ticker 24h     │    │ - Prompt + JSON  │    │ - 50 rows/giờ           │ │
│  │ - Order book     │    │ - BUY/SELL/HOLD  │    │                          │ │
│  │   imbalance      │    │                  │    │                          │ │
│  └──────────────────┘    └──────────────────┘    └──────────────────────────┘ │
│       ~2 giây                  ~5 giây                    ~1 giây             │
└───────────────────────────────────────────────────────────────────────────────┘
```

> **LLM Prompt Context:** Mỗi symbol nhận prompt bao gồm 30 nến daily (OHLCV + RSI), snapshot thị trường (RSI, MACD crossover, order book imbalance, price/volume change 24h), và trả về JSON `{signal, confidence, reason, key_risk}`.

> **Quota Handling:** Nếu LLM API bị rate limit (HTTP 429), hệ thống tự fallback sang tín hiệu HOLD với confidence=1 cho các symbol còn lại, đảm bảo pipeline không crash.

---

## 8. LLM Advisor — Tín hiệu tư vấn giao dịch Crypto

### 8.1. Thiết kế tổng quan

**Bài toán:** Phân tích dữ liệu thị trường đa chiều và sinh tín hiệu tư vấn BUY/SELL/HOLD cho 50 coins, mỗi giờ.

**Tại sao dùng LLM Advisor thay vì LSTM prediction?**

| Khía cạnh             | LSTM Prediction                      | LLM Advisor (được chọn)                |
| --------------------- | ------------------------------------ | --------------------------------------- |
| Output                | Giá close dự báo (con số)             | Signal BUY/SELL/HOLD + lý do            |
| Actionable            | Cần tự diễn giải giá                  | Signal rõ ràng, trader action ngay       |
| Interpretability      | Black-box                             | Có `reason` + `key_risk` giải thích      |
| Multi-factor          | Chỉ dùng OHLCV + RSI/MACD            | Kết hợp RSI, MACD, order book, volume   |
| Infrastructure        | Cần GPU training, `.pth` file         | Chỉ cần API key, không cần GPU           |
| Maintenance           | Cần retrain weekly                    | Model tự cập nhật bởi provider           |
| Cost                  | GPU training cost                     | API cost per request (rất thấp)          |

**Pipeline:** Klines daily (ClickHouse) + Ticker 24h + Order Book → Build Prompt → LLM API → Parse JSON → Save to `llm_signals`

### 8.2. Cấu hình LLM

| Parameter            | Value               | Giải thích                                       |
| -------------------- | ------------------- | ------------------------------------------------ |
| Daily candles        | 30                  | 30 nến daily (~1 tháng) làm context               |
| Providers            | Gemini / OpenAI     | Có thể switch qua env variable                    |
| Gemini model         | gemini-2.5-flash-lite| Nhanh, rẻ, đủ cho structured output              |
| OpenAI model         | gpt-5.4-nano        | Alternative provider                              |
| Temperature          | 0.1                 | Thấp → output ổn định, ít random                  |
| Max tokens           | 220                 | Đủ cho JSON response ngắn                         |
| Timeout              | 15s / request       | Tránh treo quá lâu                                |
| Batch size           | 10 symbols          | 10 symbols/batch, async concurrent                |
| Max retries          | 3                   | Retry với delay tăng dần (2s × attempt)            |

### 8.3. Context đầu vào cho LLM

| Dữ liệu             | Nguồn                | Chi tiết                                    |
| -------------------- | -------------------- | ------------------------------------------- |
| Daily candles        | Bảng `klines`        | 30 nến daily aggregate (O, H, L, C, V, RSI) |
| RSI(14)              | Bảng `klines`        | Kèm tag: OVERBOUGHT / OVERSOLD / NEUTRAL   |
| MACD crossover       | Bảng `klines`        | bullish / bearish / neutral                  |
| Order book imbalance | Bảng `order_book_snapshot` | strong buy / strong sell / balanced    |
| Price change 24h     | Bảng `ticker_24h`    | % thay đổi giá                              |
| Volume change 24h    | Bảng `ticker_24h`    | % thay đổi volume so với ngày trước          |

### 8.4. Output Schema

```json
{
    "signal": "BUY | SELL | HOLD",
    "confidence": 1-5,
    "reason": "max 20 words",
    "key_risk": "max 12 words"
}
```

### 8.5. Xử lý Quota & Fallback

| Tình huống                     | Hành vi                                                 |
| ------------------------------ | ------------------------------------------------------- |
| LLM trả kết quả hợp lệ        | Parse JSON, lưu vào `llm_signals`                       |
| LLM parse error                | Retry (max 3 lần), bỏ qua symbol nếu vẫn fail          |
| HTTP 429 (quota exceeded)      | Dừng batch, fallback HOLD (confidence=1) cho symbol đó, stop toàn bộ run |
| LLM trả response không hợp lệ | Retry, log warning                                      |
| Không đủ daily candles         | Skip symbol, log warning                                |

---

## 9. Infrastructure

### 9.1. Docker Services

| Service              | Image                            | Port       | Mục đích                    |
| -------------------- | -------------------------------- | ---------- | --------------------------- |
| ClickHouse           | clickhouse/clickhouse-server:24.3| 8123, 9100 | Data Warehouse              |
| MinIO                | minio/minio:latest               | 9000, 9001 | Object Storage (Data Lake)  |
| MinIO Init           | minio/mc:latest                  | -          | Tạo buckets lần đầu         |
| Airflow PostgreSQL   | postgres:15                      | -          | Airflow metadata DB         |
| Airflow Init         | apache/airflow:2.8.0             | -          | DB migrate + admin user     |
| Airflow Webserver    | apache/airflow:2.8.0             | 8080       | DAG UI                      |
| Airflow Scheduler    | apache/airflow:2.8.0             | -          | Chạy DAGs                   |
| Grafana              | grafana/grafana:11.4.0           | 3000       | Dashboard                   |

### 9.2. Native Services (chạy trên host)

| Component | Lý do không Docker                      |
| --------- | --------------------------------------- |
| PySpark   | Cần nhiều RAM, chạy native hiệu quả hơn |
| LLM API   | Chỉ cần HTTP call, không cần container   |

### 9.3. Cấu trúc thư mục

```
crypto-pipeline/
├── config/
│   ├── config.py               # Centralized config (paths, DB, API, Spark, MinIO)
│   ├── llm_config.py           # LLM config (provider, model, params)
│   └── symbols.py              # SYMBOL_REGISTRY (50 coins, single source of truth)
├── airflow/
│   └── dags/
│       ├── daily_etl.py        # ETL klines + ticker hàng ngày (0 2 * * *)
│       ├── daily_snapshot.py   # Ticker 24h + Order Book (0 0 * * *)
│       └── hourly_inference.py # LLM advisory signals mỗi giờ (0 * * * *)
├── data/
│   ├── raw/                    # Data Lake - Raw (CSV, synced to MinIO)
│   │   ├── BTCUSDT.csv
│   │   └── ...
│   └── processed/              # Data Lake - Processed (Parquet, synced to MinIO)
│       └── features.parquet
├── models/
│   └── .gitkeep                # Placeholder (không còn .pth files)
├── scripts/
│   ├── pre_extract.py          # Self-healing gap detection + recovery
│   ├── extract.py              # Data Vision bulk + REST API
│   ├── transform.py            # Spark: RSI-14, MACD-12/26/9 trên 1-min
│   ├── load.py                 # ClickHouse insert (clickhouse-connect)
│   └── llm_signal.py           # LLM advisory signal generation
├── sql/
│   ├── schema.sql              # ClickHouse schema (5 bảng)
│   ├── queries.sql             # Sample ClickHouse queries
│   ├── init_db.sql             # Database initialization
│   └── migrate_remove_predictions_clickhouse.sql  # Migration: predictions → llm_signals
├── utils/
│   ├── binance_utils.py        # API wrappers (retry, rate limit)
│   ├── db_utils.py             # ClickHouse client, insert/query helpers
│   ├── data_utils.py           # Timestamps, merge CSV, date utils
│   ├── llm_utils.py            # LLM API callers (Gemini, OpenAI), JSON parser
│   ├── storage.py              # MinIO object storage utilities
│   ├── exceptions.py           # Custom exceptions (E/T/L/LLM layers)
│   └── logger.py               # Logging config
├── grafana/
│   ├── provisioning/           # Grafana datasource provisioning
│   └── dashboards/             # Dashboard JSON definitions
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 10. Grafana Dashboard

### 10.1. Tổng quan Layout

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              CRYPTO INTELLIGENCE DASHBOARD                           │
├─────────────────────────────────┬───────────────────────────────────────────────────┤
│                                 │                                                   │
│   TOP 10 VOLUME 24H (Bar)       │           BTC PRICE CHART (Time Series)           │
│   ┌───────────────────────┐     │   ┌─────────────────────────────────────────────┐ │
│   │ BTC  ████████████ 45B │     │   │                    ___/\                    │ │
│   │ ETH  ████████ 32B     │     │   │              /\___/     \                   │ │
│   │ SOL  ██████ 18B       │     │   │         ___/            \___               │ │
│   │ ...                   │     │   │    ____/                                    │ │
│   └───────────────────────┘     │   └─────────────────────────────────────────────┘ │
│                                 │                                                   │
├─────────────────────────────────┼───────────────────────────────────────────────────┤
│                                 │                                                   │
│   TOP GAINERS 24H (Table)       │         LLM ADVISORY SIGNALS (Table)              │
│   ┌───────────────────────┐     │   ┌─────────────────────────────────────────────┐ │
│   │ PEPE  +15.2%          │     │   │ BTC  BUY  ★★★★☆  RSI oversold, MACD...    │ │
│   │ WIF   +12.8%          │     │   │ ETH  HOLD ★★★☆☆  Sideways trend...        │ │
│   │ RUNE  +8.5%           │     │   │ SOL  SELL ★★★★☆  RSI overbought...        │ │
│   └───────────────────────┘     │   └─────────────────────────────────────────────┘ │
│                                 │                                                   │
├─────────────────────────────────┼───────────────────────────────────────────────────┤
│                                 │                                                   │
│   TOP LOSERS 24H (Table)        │         SIGNAL DISTRIBUTION (Pie/Bar)             │
│   ┌───────────────────────┐     │   ┌──────────┬──────────┬──────────┬──────────┐  │
│   │ FTM   -8.3%           │     │   │   BUY    │   SELL   │   HOLD   │ Avg Conf │  │
│   │ ALGO  -6.2%           │     │   │   35%    │   25%    │   40%    │   3.2    │  │
│   │ MANA  -5.8%           │     │   └──────────┴──────────┴──────────┴──────────┘  │
│   └───────────────────────┘     │                                                   │
│                                 │                                                   │
├─────────────────────────────────┴───────────────────────────────────────────────────┤
│                          RSI HEATMAP (50 COINS)                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  BTC:68  ETH:55  BNB:42  SOL:71  XRP:38  DOGE:62  ADA:45  TRX:58  ...       │   │
│   │  ■ >70 Overbought   ■ 30-70 Neutral   ■ <30 Oversold                        │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 10.2. Chi tiết các Panels

#### Panel 1: Top 10 Volume 24h (Bar Chart)

| Thuộc tính  | Giá trị                |
| ----------- | ---------------------- |
| Loại        | Bar Chart (horizontal) |
| Data source | ClickHouse             |
| Refresh     | 1 giờ                  |

```sql
SELECT
    s.base_asset AS coin,
    t.quote_volume_24h / 1000000000 AS volume_billion_usd
FROM ticker_24h t
JOIN symbols s ON t.symbol = s.symbol
WHERE t.snapshot_time = (SELECT max(snapshot_time) FROM ticker_24h)
ORDER BY t.quote_volume_24h DESC
LIMIT 10;
```

#### Panel 2: Top Gainers 24h (Table)

| Thuộc tính | Giá trị                         |
| ---------- | ------------------------------- |
| Loại       | Table                           |
| Highlight  | Green background for positive % |

```sql
SELECT
    s.base_asset AS coin,
    t.price_change_pct AS change_24h,
    t.quote_volume_24h AS volume_usd
FROM ticker_24h t
JOIN symbols s ON t.symbol = s.symbol
WHERE t.snapshot_time = (SELECT max(snapshot_time) FROM ticker_24h)
  AND t.price_change_pct > 0
ORDER BY t.price_change_pct DESC
LIMIT 5;
```

#### Panel 3: Top Losers 24h (Table)

| Thuộc tính | Giá trị                       |
| ---------- | ----------------------------- |
| Loại       | Table                         |
| Highlight  | Red background for negative % |

```sql
SELECT
    s.base_asset AS coin,
    t.price_change_pct AS change_24h,
    t.quote_volume_24h AS volume_usd
FROM ticker_24h t
JOIN symbols s ON t.symbol = s.symbol
WHERE t.snapshot_time = (SELECT max(snapshot_time) FROM ticker_24h)
  AND t.price_change_pct < 0
ORDER BY t.price_change_pct ASC
LIMIT 5;
```

#### Panel 4: BTC Price Chart (Time Series)

| Thuộc tính | Giá trị      |
| ---------- | ------------ |
| Loại       | Time series  |
| Time range | Last 7 days  |

> **Lưu ý:** Dữ liệu klines cập nhật daily (daily_etl DAG). Chart hiển thị dữ liệu 1-min từ ClickHouse.

```sql
SELECT
    timestamp,
    close AS price
FROM klines
WHERE symbol = $symbol
  AND timestamp >= now() - INTERVAL 7 DAY
ORDER BY timestamp;
```

#### Panel 5: LLM Advisory Signals (Table)

| Thuộc tính | Giá trị                                     |
| ---------- | ------------------------------------------- |
| Loại       | Table (color-coded by signal)               |
| Legend     | BUY (green), SELL (red), HOLD (yellow)      |

```sql
SELECT
    s.base_asset AS coin,
    l.signal,
    l.confidence,
    l.reason,
    l.key_risk,
    l.trend_6h,
    l.generated_at
FROM llm_signals l
JOIN symbols s ON l.symbol = s.symbol
WHERE l.generated_at = (
    SELECT max(generated_at) FROM llm_signals
)
ORDER BY l.confidence DESC;
```

#### Panel 6: Signal Distribution (Stat Panels)

| Metric          | Query                                                                                           |
| --------------- | ----------------------------------------------------------------------------------------------- |
| BUY ratio       | `SELECT countIf(signal='BUY') * 100.0 / count() FROM llm_signals WHERE generated_at >= now() - INTERVAL 7 DAY` |
| SELL ratio      | `SELECT countIf(signal='SELL') * 100.0 / count() FROM llm_signals WHERE generated_at >= now() - INTERVAL 7 DAY` |
| HOLD ratio      | `SELECT countIf(signal='HOLD') * 100.0 / count() FROM llm_signals WHERE generated_at >= now() - INTERVAL 7 DAY` |
| Avg Confidence  | `SELECT round(avg(confidence), 2) FROM llm_signals WHERE generated_at >= now() - INTERVAL 7 DAY` |

#### Panel 7: RSI Heatmap (Table with color)

| Thuộc tính | Giá trị                               |
| ---------- | ------------------------------------- |
| Loại       | Table với color threshold             |
| Colors     | Red (>70), Green (<30), White (30-70) |

```sql
SELECT
    s.base_asset AS coin,
    argMax(k.rsi_14, k.timestamp) AS rsi_14
FROM klines k
JOIN symbols s ON k.symbol = s.symbol
GROUP BY s.base_asset
ORDER BY rsi_14 DESC;
```

### 10.3. Variables (Dropdown filters)

| Variable     | Query                                 | Mục đích                  |
| ------------ | ------------------------------------- | ------------------------- |
| `$symbol`    | `SELECT DISTINCT symbol FROM symbols` | Chọn coin để xem chi tiết |
| `$timerange` | `1h, 6h, 24h, 7d, 30d`                | Chọn khoảng thời gian     |

### 10.4. Alerts (Cảnh báo)

| Alert                 | Điều kiện                               | Notification |
| --------------------- | --------------------------------------- | ------------ |
| RSI Overbought        | RSI > 70 cho BTC hoặc ETH               | Slack/Email  |
| LLM SELL nhiều        | Hơn 50% signals là SELL trong 1h        | Slack        |
| Volume Spike          | Volume 24h tăng > 200% so với hôm trước | Slack        |

### 10.5. Refresh Rate

| Panel type        | Refresh interval | Giải thích                                       |
| ----------------- | ---------------- | ------------------------------------------------ |
| Price charts      | 5 phút           | Klines cập nhật daily, refresh vừa đủ            |
| Volume/Gainers    | 1 giờ            | Snapshot daily, thay đổi ít                       |
| LLM Signals       | 1 giờ            | Signals cập nhật mỗi giờ                         |
| RSI Heatmap       | 5 phút           | RSI trên 1-min, cập nhật theo klines             |

---

## 11. Lộ trình Thực hiện (4 Tuần)

### Tuần 1: Setup & Extract

| Ngày | Task                                                      | Deliverable                      |
| ---- | --------------------------------------------------------- | -------------------------------- |
| 1-2  | Setup Docker (ClickHouse, MinIO, Airflow, Grafana)        | docker-compose.yml chạy được     |
| 3-4  | Viết extract.py, download dữ liệu 3 năm                   | MinIO raw/ đầy đủ CSV            |
| 5-7  | Test, verify data integrity                               | Không thiếu nến, không duplicate |

### Tuần 2: Transform & Load

| Ngày | Task                              | Deliverable                     |
| ---- | --------------------------------- | ------------------------------- |
| 1-3  | Viết transform.py với Spark       | MinIO processed/features.parquet|
| 4-5  | Viết load.py, tạo bảng ClickHouse | Dữ liệu trong DB, query được    |
| 6-7  | Test full ETL pipeline            | E→T→L chạy end-to-end           |

### Tuần 3: Airflow & Automation

| Ngày | Task                                      | Deliverable                  |
| ---- | ----------------------------------------- | ---------------------------- |
| 1-2  | Setup Airflow, tạo daily_etl DAG         | DAG chạy được từ UI          |
| 3-4  | Tạo daily_snapshot, hourly_inference DAGs | Tất cả DAGs hoạt động        |
| 5-7  | Test scheduling, error handling           | Retry hoạt động, logs đầy đủ |

### Tuần 4: LLM Advisor & Dashboard

| Ngày | Task                                    | Deliverable                    |
| ---- | --------------------------------------- | ------------------------------ |
| 1-3  | Viết llm_signal.py, test LLM advisory  | Signals ghi đúng vào ClickHouse|
| 4-5  | Setup Grafana dashboard                 | Dashboard hiển thị đúng        |
| 6-7  | Test end-to-end, viết báo cáo           | Demo hoàn chỉnh                |

---

## 12. Checklist Trước Khi Demo

### Data Pipeline

- [ ] Extract: Download đủ 50 coins × 3 năm
- [ ] Transform: RSI, MACD tính đúng
- [ ] Load: Dữ liệu trong ClickHouse, query nhanh

### Automation

- [ ] Airflow Web UI accessible (port 8080)
- [ ] daily_etl DAG chạy thành công
- [ ] daily_snapshot DAG chạy thành công
- [ ] hourly_inference DAG chạy thành công

### LLM Advisor

- [ ] LLM API connected (Gemini/OpenAI)
- [ ] Signals generated (BUY/SELL/HOLD) cho 50 coins
- [ ] Quota fallback hoạt động (HOLD khi bị limit)
- [ ] Signals lưu đúng vào bảng `llm_signals`

### Visualization

- [ ] Grafana accessible (port 3000)
- [ ] Dashboard hiển thị giá
- [ ] Dashboard hiển thị LLM advisory signals
- [ ] Dashboard hiển thị signal distribution

---

## 13. Các Rủi ro & Giải pháp

| Rủi ro                    | Xác suất   | Tác động        | Giải pháp                                         |
| ------------------------- | ---------- | --------------- | ------------------------------------------------- |
| Binance rate limit        | Cao        | ETL fail        | Dùng Binance Data Vision cho historical data      |
| API lỗi tạm thời          | Trung bình | Mất data ngắn   | Retry với exponential backoff, skip 404 ngay       |
| Thiếu dữ liệu theo phút   | Trung bình | LLM context lỗi | Resample + forward fill hoặc linear interpolation |
| Downtime ngắn (< 30 ngày) | Thấp       | Gap dữ liệu     | Self-healing tự phát hiện và dùng REST API backfill |
| Downtime dài (≥ 30 ngày)  | Thấp       | Gap dữ liệu lớn | Self-healing dùng Data Vision bulk + REST API phần còn lại |
| Coin bị delist/migrate    | Thấp       | Dữ liệu bị cắt  | BREAK status + break_date giới hạn fetch tự động  |
| Spark out of memory       | Trung bình | Transform fail  | Tăng partition, xử lý theo batch nhỏ              |
| LLM quota exceeded        | Trung bình | Signals thiếu   | Fallback HOLD (confidence=1), stop early           |
| LLM response invalid      | Thấp       | Parse error      | Retry 3 lần, JSON extraction + validation          |
| ClickHouse slow query     | Thấp       | Dashboard lag   | Partition by month, ORDER BY tối ưu                |
| Airflow scheduler crash   | Thấp       | Jobs không chạy | Auto-restart với Docker, monitoring               |

### 13.1. Self-Healing Extract (`_pre_extract`)

Pipeline tự động phát hiện và phục hồi gap dữ liệu mỗi lần chạy `daily_etl`, không cần can thiệp thủ công.

#### Khái niệm chính — `target_end_time`

| Trạng thái symbol | Target          | Ý nghĩa                                        |
| ------------------ | --------------- | ----------------------------------------------- |
| **TRADING**        | `now()`         | Luôn fetch đến hiện tại                          |
| **BREAK**          | `break_date`    | Chỉ fetch đến ngày delist/migrate, sau đó dừng hẳn |

> Khi `last_timestamp >= target_end_time` → symbol **DONE**, không xử lý thêm.

#### Quy trình phân loại (Step 1)

```
Với mỗi symbol:
  ├── Không có CSV?
  │     ├── TRADING → bulk download từ Data Vision
  │     └── BREAK   → tạo placeholder (skip bulk vì hay 404)
  ├── last_ts >= target_end?
  │     └── DONE (up-to-date hoặc data complete)
  ├── Gap < 30 ngày?
  │     └── REST API (end_time = target_end)
  └── Gap ≥ 30 ngày?
        └── Data Vision backfill các tháng trọn vẹn + REST API phần còn lại
```

#### Chiến lược phục hồi (Step 2)

| Tình huống | Hành động | Ví dụ |
| --- | --- | --- |
| Không có CSV (TRADING) | Data Vision bulk N tháng → REST API fill phần còn lại | Symbol mới thêm vào hệ thống |
| Không có CSV (BREAK) | Tạo file CSV rỗng (placeholder) | Coin đã chết trước khi được track |
| Gap < 30 ngày | REST API paginate từ `last_ts` → `target_end` | Downtime ngắn, bảo trì server |
| Gap ≥ 30 ngày | Data Vision backfill tháng trọn vẹn + REST API tháng lẻ | Downtime dài, mất dữ liệu nhiều tháng |

#### Xử lý tháng trọn vẹn (`_get_months_between`)

Chỉ download những tháng **đã kết thúc hoàn toàn** từ Data Vision:

```
Ví dụ: last_ts = 15/01, target_end = 15/04
  → Tháng 1: bỏ qua (đang dở)  → REST API fill 15/01 → 31/01
  → Tháng 2: trọn vẹn          → Data Vision ZIP
  → Tháng 3: trọn vẹn          → Data Vision ZIP
  → Tháng 4: chưa hết          → REST API fill 01/04 → 15/04
```

#### Xử lý lỗi & cảnh báo

| Tình huống | Hành vi |
| --- | --- |
| Data Vision 404 | Skip ngay (không retry), fallback sang REST API |
| REST API trả rỗng | Log INFO, không crash |
| Gap vẫn còn sau cả 2 nguồn | Log WARNING với số ngày còn thiếu |
| BREAK coin đã đủ data | Đánh dấu DONE vĩnh viễn, không bao giờ gọi API lại |
