# Hệ thống Data Pipeline Big Data & Deep Learning (LSTM) - Dự báo giá Crypto

---

## 1. Tổng quan Dự án

### 1.1. Bối cảnh & Vấn đề

Thị trường cryptocurrency hoạt động 24/7 với khối lượng giao dịch khổng lồ. Việc phân tích và dự báo giá đòi hỏi:

- Xử lý dữ liệu lớn (hàng triệu records)
- Pipeline tự động, không can thiệp thủ công
- Model ML/DL có khả năng học patterns từ dữ liệu chuỗi thời gian

### 1.2. Mục tiêu Đồ án

Xây dựng hệ thống **End-to-End Data Pipeline** minh họa vai trò của Data Engineer + AI Engineer:

| Giai đoạn       | Công nghệ            | Mục đích                               |
| --------------- | -------------------- | -------------------------------------- |
| **Extract**     | Python + Binance API | Thu thập dữ liệu nến từ sàn giao dịch  |
| **Transform**   | Apache Spark         | Tính toán chỉ số kỹ thuật (RSI, MACD)  |
| **Load**        | PostgreSQL           | Lưu trữ có cấu trúc, phục vụ analytics |
| **Orchestrate** | Apache Airflow       | Tự động hóa và lập lịch các jobs       |
| **Train**       | PyTorch LSTM         | Huấn luyện model dự báo giá            |
| **Visualize**   | Grafana              | Dashboard theo dõi real-time           |

### 1.3. Phạm vi & Giới hạn

| Thành phần      | Giá trị                   | Ghi chú                          |
| --------------- | ------------------------- | -------------------------------- |
| Số lượng coin   | 50                        | Top vốn hóa, loại bỏ stablecoins |
| Khung thời gian | Nến 1 phút                | Phù hợp dự báo ngắn hạn          |
| Dữ liệu lịch sử | 3 năm (01/2023 - 01/2026) | Đủ để train model                |
| Dự báo          | 60 nến tiếp theo          | Dự báo 1 giờ tới (multi-step)    |

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

### 3.3. Bảng `predictions` (Hourly inference - 60 phút/lần)

```
Số records = 50 coins × 3 năm × 365 ngày × 24 lần/ngày × 60 predictions/lần
           = 50 × 3 × 365 × 24 × 60
           = 78,840,000 records

Kích thước = 78,840,000 × 80 bytes/record ≈ 6 GB
```

### 3.4. Tổng hợp theo bảng

| Bảng          | Số records     | Kích thước   | Tần suất ghi     |
| ------------- | -------------- | ------------ | ---------------- |
| `symbols`     | 50             | < 1 KB       | 1 lần (setup)    |
| `klines`      | 78,840,000     | ~7.5 GB      | 50 recs/phút       |
| `ticker_24h`  | 54,750         | ~8 MB        | 50 recs/ngày      |
| `predictions` | 78,840,000     | ~6 GB        | 3,000 recs/giờ    |
| **Total**     | **~157.7 triệu** | **~13.5 GB** | -               |

### 3.5. Breakdown theo layer

| Layer                 | Format     | Kích thước | Ghi chú                            |
| --------------------- | ---------- | ---------- | ---------------------------------- |
| Raw (Data Lake)       | CSV        | ~7.5 GB    | Dữ liệu gốc từ Binance             |
| Processed (Data Lake) | Parquet    | ~2 GB      | Nén tốt hơn CSV                    |
| Warehouse             | PostgreSQL | ~8 GB      | Bao gồm index + predictions (~30MB) |

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
│  │  Binance    │───▶│  │  Raw/     │ │ Processed/  │  │───▶│   PostgreSQL    │ │
│  │  API        │    │  │  (CSV)    │ │ (Parquet)   │  │    │                 │ │
│  │             │    │  └───────────┘ └─────────────┘  │    │                 │ │
│  └─────────────┘    │        │              ▲         │    └────────┬────────┘ │
│                     │        └──── Spark ───┘         │             │          │
│                     └─────────────────────────────────┘             │          │
├─────────────────────────────────────────────────────────────────────┼──────────┤
│                                                                     ▼          │
│  ┌─────────────────────────────────┐    ┌─────────────────────────────────┐   │
│  │           ML LAYER              │    │         VISUALIZATION           │   │
│  │  ┌───────────┐ ┌─────────────┐  │    │                                 │   │
│  │  │  PyTorch  │ │   Model     │  │    │         Grafana                 │   │
│  │  │  LSTM     │ │   (.pth)    │  │    │         Dashboard               │   │
│  │  └───────────┘ └─────────────┘  │    │                                 │   │
│  └─────────────────────────────────┘    └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2. Data Lake vs Data Warehouse

| Khía cạnh         | Data Lake                      | Data Warehouse            |
| ----------------- | ------------------------------ | ------------------------- |
| **Vị trí**        | `data/raw/`, `data/processed/` | PostgreSQL database       |
| **Format**        | CSV (raw), Parquet (processed) | Bảng SQL có schema        |
| **Schema**        | Schema-on-read (linh hoạt)     | Schema-on-write (cố định) |
| **Mục đích**      | Lưu trữ, batch processing      | Query nhanh, serving      |
| **Công cụ xử lý** | Apache Spark                   | SQL queries               |
| **Ai dùng**       | Data Engineer, Data Scientist  | Analyst, Dashboard, API   |

### 4.3. Luồng dữ liệu chi tiết

```
[1] EXTRACT                    [2] TRANSFORM                 [3] LOAD
    Binance API                    Apache Spark                  PostgreSQL
         │                              │                             │
         ▼                              ▼                             ▼
    ┌─────────┐                  ┌─────────────┐              ┌─────────────┐
    │  OHLCV  │      Spark       │  + RSI      │    JDBC      │   klines    │
    │  data   │  ──────────────▶ │  + MACD     │  ─────────▶  │   table     │
    │  (CSV)  │   Pandas UDF     │  (Parquet)  │              │             │
    └─────────┘                  └─────────────┘              └─────────────┘

    data/raw/                    data/processed/               crypto_dw
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
│       klines         │ │     ticker_24h       │ │     predictions      │
│    (Fact Table)      │ │    (Fact Table)      │ │    (Fact Table)      │
│                      │ │                      │ │                      │
│  symbol (FK) ────────│─│── symbol (FK) ───────│─│── symbol (FK)        │
│  timestamp (PK)      │ │  snapshot_time (PK)  │ │  predicted_at (PK)   │
│  open, high, low...  │ │  price_change_24h    │ │  target_time         │
│  rsi, macd...        │ │  volume_24h...       │ │  predicted_close     │
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

> **Thiết kế Star Schema:** 1 Dimension table (`symbols`) + 4 Fact tables (`klines`, `ticker_24h`, `order_book_snapshot`, `predictions`)

### 5.2. Nguồn dữ liệu từ Binance API

| Bảng                  | Binance API Endpoint                               | Mô tả                                    | Tần suất thu thập |
| --------------------- | -------------------------------------------------- | ---------------------------------------- | ----------------- |
| `symbols`             | `/api/v3/exchangeInfo`                             | Thông tin trading pair                   | 1 lần (setup)     |
| `klines`              | `/api/v3/klines`                                   | Dữ liệu nến (OHLCV)                      | Mỗi phút          |
| `ticker_24h`          | `/api/v3/ticker/24hr`, `/api/v3/ticker/bookTicker` | Thống kê 24h + best bid/ask + spread     | Daily             |
| `order_book_snapshot` | `/api/v3/depth`                                    | Snapshot order book để đo áp lực mua/bán | Daily             |
| `predictions`         | (Internal)                                         | Kết quả từ LSTM model                    | Mỗi giờ           |

### 5.3. Bảng `symbols` (Dimension Table)

> **Mục đích:** Lưu thông tin tĩnh về 50 coins, tránh lặp lại trong các Fact tables.

| Column        | Type        | Description                 | Nguồn                |
| ------------- | ----------- | --------------------------- | -------------------- |
| `symbol`      | VARCHAR(20) | Cặp giao dịch (PK)          | Binance exchangeInfo |
| `base_asset`  | VARCHAR(10) | Coin gốc (BTC, ETH...)      | Binance exchangeInfo |
| `quote_asset` | VARCHAR(10) | Coin định giá (USDT)        | Binance exchangeInfo |
| `status`      | VARCHAR(20) | Trạng thái (TRADING, BREAK) | Binance exchangeInfo |
| `created_at`  | TIMESTAMPTZ | Thời gian thêm vào hệ thống | System               |

**Primary Key:** `symbol`

**Sample Data:**

```
| symbol   | base_asset | quote_asset | status  | created_at          |
|----------|------------|-------------|---------|---------------------|
| BTCUSDT  | BTC        | USDT        | TRADING | 2023-01-01 00:00:00 |
| ETHUSDT  | ETH        | USDT        | TRADING | 2023-01-01 00:00:00 |
```

### 5.4. Bảng `klines` (Fact Table)

> **Mục đích:** Lưu dữ liệu nến 1 phút và các chỉ số kỹ thuật đã tính toán.

| Column        | Type        | Description          | Nguồn           |
| ------------- | ----------- | -------------------- | --------------- |
| `symbol`      | VARCHAR(20) | FK → symbols.symbol  | Binance         |
| `timestamp`   | TIMESTAMPTZ | Thời gian mở nến     | Binance         |
| `open`        | DOUBLE      | Giá mở cửa           | Binance         |
| `high`        | DOUBLE      | Giá cao nhất         | Binance         |
| `low`         | DOUBLE      | Giá thấp nhất        | Binance         |
| `close`       | DOUBLE      | Giá đóng cửa         | Binance         |
| `volume`      | DOUBLE      | Khối lượng giao dịch | Binance         |
| `rsi_14`      | DOUBLE      | RSI 14 periods       | Spark Transform |
| `macd`        | DOUBLE      | MACD line            | Spark Transform |
| `macd_signal` | DOUBLE      | MACD signal line     | Spark Transform |

**Primary Key:** `(symbol, timestamp)`

**Foreign Key:** `symbol` → `symbols.symbol`

**Indexes:**

- `idx_klines_symbol_time`: Tối ưu query theo symbol và time range

### 5.5. Bảng `ticker_24h` (Fact Table)

> **Mục đích:** Lưu snapshot thống kê 24h mỗi ngày, dùng để phân tích volume trend và market sentiment.

| Column             | Type        | Description                       | Nguồn               |
| ------------------ | ----------- | --------------------------------- | ------------------- |
| `symbol`           | VARCHAR(20) | FK → symbols.symbol               | Binance ticker/24hr |
| `snapshot_time`    | TIMESTAMPTZ | Thời điểm lấy snapshot            | System              |
| `price_change`     | DOUBLE      | Chênh lệch giá 24h (absolute)     | Binance ticker/24hr |
| `price_change_pct` | DOUBLE      | Chênh lệch giá 24h (%)            | Binance ticker/24hr |
| `high_24h`         | DOUBLE      | Giá cao nhất trong 24h            | Binance ticker/24hr |
| `low_24h`          | DOUBLE      | Giá thấp nhất trong 24h           | Binance ticker/24hr |
| `volume_24h`       | DOUBLE      | Volume giao dịch 24h (base asset) | Binance ticker/24hr |
| `quote_volume_24h` | DOUBLE      | Volume 24h (quote asset - USDT)   | Binance ticker/24hr |
| `trade_count`      | BIGINT      | Số lượng trades trong 24h         | Binance ticker/24hr |
| `bid_price`        | DOUBLE      | Best bid                          | Binance bookTicker  |
| `ask_price`        | DOUBLE      | Best ask                          | Binance bookTicker  |
| `spread_pct`       | DOUBLE      | (ask - bid) / ask \* 100          | Calculated          |

**Primary Key:** `(symbol, snapshot_time)`

**Foreign Key:** `symbol` → `symbols.symbol`

**Sample Data:**

```
| symbol  | snapshot_time       | price_change_pct | volume_24h    | trade_count |
|---------|---------------------|------------------|---------------|-------------|
| BTCUSDT | 2026-01-26 00:00:00 | 2.35             | 45,230.5 BTC  | 1,245,678   |
| ETHUSDT | 2026-01-26 00:00:00 | -1.20            | 312,450.2 ETH | 892,345     |
```

> **Ý nghĩa:** Dữ liệu này giúp phân tích market sentiment, detect unusual volume spikes, và có thể dùng làm features bổ sung cho model.

### 5.6. Bảng `predictions` (Fact Table)

> **Mục đích:** Lưu riêng kết quả dự báo 60 phút/lần (mỗi giờ), tách biệt khỏi dữ liệu thực tế để dễ đánh giá model performance.

| Column            | Type        | Description                       | Nguồn      |
| ----------------- | ----------- | --------------------------------- | ---------- |
| `symbol`          | VARCHAR(20) | FK → symbols.symbol               | System     |
| `predicted_at`    | TIMESTAMPTZ | Thời điểm chạy dự báo (mỗi giờ)    | System     |
| `step_index`      | INTEGER     | Phút dự báo thứ i (1-60)           | System     |
| `target_time`     | TIMESTAMPTZ | Phút được dự báo                  | System     |
| `predicted_close` | DOUBLE      | Giá close dự báo                  | LSTM Model |
| `model_version`   | VARCHAR(50) | Version của model sử dụng         | System     |
| `actual_close`    | DOUBLE      | Giá thực tế (cập nhật sau)        | Binance    |
| `error_pct`       | DOUBLE      | % sai số (tính sau khi có actual) | System     |

**Primary Key:** `(symbol, predicted_at, step_index)`

**Foreign Key:** `symbol` → `symbols.symbol`

**Sample Data:**

```
| symbol  | predicted_at        | step | target_time         | predicted | actual  | error_pct |
|---------|---------------------|------|---------------------|-----------|---------|-----------|
| BTCUSDT | 2026-01-26 14:00:00 |  1   | 2026-01-26 14:01:00 | 102,345.5 | 102,340 | 0.005%    |
| BTCUSDT | 2026-01-26 14:00:00 |  2   | 2026-01-26 14:02:00 | 102,348.2 | 102,355 | 0.007%    |
| ...     | ...                 | ...  | ...                 | ...       | ...     | ...       |
| BTCUSDT | 2026-01-26 14:00:00 | 60   | 2026-01-26 14:60:00 | 102,890.5 | 102,850 | 0.039%    |
```

> **Ý nghĩa:** Tách predictions ra bảng riêng giúp:
>
> - Theo dõi performance của model theo thời gian
> - So sánh giữa các model versions
> - Không làm "ô nhiễm" dữ liệu gốc trong bảng klines

### 5.7. Bảng `order_book_snapshot` (Fact Table)

> **Mục đích:** Theo dõi áp lực mua/bán theo snapshot order book.

| Column             | Type        | Description                         | Nguồn         |
| ------------------ | ----------- | ----------------------------------- | ------------- |
| `symbol`           | VARCHAR(20) | FK → symbols.symbol                 | Binance depth |
| `timestamp`        | TIMESTAMPTZ | Thời điểm snapshot                  | System        |
| `total_bid_volume` | DOUBLE      | Tổng khối lượng bid (top N levels)  | Calculated    |
| `total_ask_volume` | DOUBLE      | Tổng khối lượng ask (top N levels)  | Calculated    |
| `imbalance`        | DOUBLE      | total_bid / (total_bid + total_ask) | Calculated    |

**Primary Key:** `(symbol, timestamp)`

> **Ý nghĩa:** `imbalance` > 0.5 → lực mua mạnh, < 0.5 → lực bán mạnh.

### 5.8. Relationships Diagram

```sql
-- Foreign Key Constraints
ALTER TABLE klines ADD CONSTRAINT fk_klines_symbol
    FOREIGN KEY (symbol) REFERENCES symbols(symbol);

ALTER TABLE ticker_24h ADD CONSTRAINT fk_ticker_symbol
    FOREIGN KEY (symbol) REFERENCES symbols(symbol);

ALTER TABLE predictions ADD CONSTRAINT fk_predictions_symbol
    FOREIGN KEY (symbol) REFERENCES symbols(symbol);

ALTER TABLE order_book_snapshot ADD CONSTRAINT fk_orderbook_symbol
    FOREIGN KEY (symbol) REFERENCES symbols(symbol);
```

### 5.9. Ví dụ SQL Queries (JOIN các bảng)

**Query 1:** Lấy thông tin coin kèm giá hiện tại và dự báo

```sql
SELECT
    s.symbol,
    s.base_asset,
    k.close AS current_price,
    p.predicted_close,
    t.volume_24h,
    t.price_change_pct AS change_24h
FROM symbols s
JOIN klines k ON s.symbol = k.symbol
JOIN predictions p ON s.symbol = p.symbol
JOIN ticker_24h t ON s.symbol = t.symbol
WHERE k.timestamp = (SELECT MAX(timestamp) FROM klines WHERE symbol = s.symbol)
  AND t.snapshot_time = CURRENT_DATE;
```

**Query 2:** Đánh giá model accuracy theo symbol

```sql
SELECT
    s.base_asset,
    COUNT(*) AS total_predictions,
    AVG(p.error_pct) AS avg_error,
    MIN(p.error_pct) AS best_prediction,
    MAX(p.error_pct) AS worst_prediction
FROM predictions p
JOIN symbols s ON p.symbol = s.symbol
WHERE p.actual_close IS NOT NULL
GROUP BY s.base_asset
ORDER BY avg_error ASC;
```

### 5.10. Giải thích các chỉ số kỹ thuật

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
│   │ (1 lần setup)   │   │ (Mỗi phút: 50 coins)│   │ (Daily: 50 coins)       │   │ (Daily)            │ │
│   └────────┬────────┘   └──────────┬──────────┘   └────────────┬────────────┘   └─────────┬─────────┘ │
│            │                       │                           │                           │           │
│            ▼                       ▼                           ▼                           ▼           │
│      symbols.json            raw/{SYMBOL}.csv            ticker_24h.csv           order_book_snapshot.csv │
└────────────┬───────────────────────┬───────────────────────────┬───────────────────────────┬───────────┘
             │                       │                           │
             │              ┌────────┴────────┐                  │
             │              │    TRANSFORM    │                  │
             │              │   (Spark)       │                  │
             │              │  + RSI, MACD    │                  │
             │              └────────┬────────┘                  │
             │                       │                           │
             │                       ▼                           │
             │              features.parquet                     │
             │                       │                           │
┌────────────┴───────────────────────┴───────────────────────────┴────────────────────────┐
│                                      LOAD                                                │
│                                                                                          │
│    ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌──────────────────────┐   ┌───────────────┐ │
│    │    symbols      │   │     klines      │   │   ticker_24h    │   │ order_book_snapshot │   │  predictions  │ │
│    │  (dim table)    │   │  (fact table)   │   │  (fact table)   │   │   (fact table)      │   │ (fact table)  │ │
│    └─────────────────┘   └─────────────────┘   └─────────────────┘   └──────────────────────┘   └───────────────┘ │
│                                                                                          │
│                              PostgreSQL Database                                         │
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

#### 6.2.2. Extract Klines (Mỗi phút)

| Thuộc tính | Giá trị                                                                |
| ---------- | ---------------------------------------------------------------------- |
| API        | `/api/v3/klines`                                                       |
| Tần suất   | Mỗi phút (* * * * *)                                                    |
| Chiến lược | Bulk download từ Binance Data Vision (lịch sử), REST API (dữ liệu mới) |
| Output     | `data/raw/{SYMBOL}.csv`                                                |
| Ghi vào    | Bảng `klines` (sau Transform)                                          |

> **Lưu ý:** Extract initial nặng (bulk 3 năm × 50 coins = 78M rows). Sau khi bulk xong, mỗi phút chỉ lấy 1 nến/coin (50 API calls) → rất nhẹ. Transform + Load với Spark để giữ nhất quán kiến trúc.

#### 6.2.3. Extract Ticker 24h + Best Bid/Ask (Daily)

| Thuộc tính | Giá trị                                            |
| ---------- | -------------------------------------------------- |
| API        | `/api/v3/ticker/24hr`, `/api/v3/ticker/bookTicker` |
| Tần suất   | Daily (2:30 AM)                                    |
| Đặc điểm   | 1 request lấy được tất cả 50 coins                 |
| Output     | `data/raw/ticker_24h.csv`                          |
| Ghi vào    | Bảng `ticker_24h`                                  |

> **Lưu ý:** API ticker/24hr và bookTicker trả về snapshot tại thời điểm gọi, không phải dữ liệu lịch sử. Mỗi ngày lưu 1 record/coin.

#### 6.2.4. Extract Order Book Snapshot (Daily)

| Thuộc tính | Giá trị                            |
| ---------- | ---------------------------------- |
| API        | `/api/v3/depth`                    |
| Tần suất   | Daily (0 0 * * *), cùng daily_snapshot DAG |
| Đặc điểm   | Snapshot top N levels              |
| Output     | `data/raw/order_book_snapshot.csv` |
| Ghi vào    | Bảng `order_book_snapshot`         |

> **Lưu ý:** Lấy 1 snapshot/ngày để đơn giản hóa pipeline. Đủ để phân tích xu hướng áp lực mua/bán theo ngày mà không gây rate limit.

### 6.3. Transform - Xử lý với Spark

**Input:** CSV files từ Data Lake (`data/raw/*.csv`)

**Xử lý:**

1. Đọc tất cả CSV vào Spark DataFrame
2. Tính RSI(14) và MACD sử dụng Pandas UDF (`groupBy("symbol").applyInPandas`)
3. Xử lý missing values (forward fill, back fill, fillna)

**Output:** `data/processed/features.parquet`

> **Tại sao dùng Spark?** Dữ liệu 78 triệu records, Pandas sẽ chậm và tốn RAM. Spark xử lý distributed, tối ưu cho batch processing.

### 6.4. Load - Ghi vào PostgreSQL

| Bảng                  | Input                   | Mode   | Ghi chú                           |
| --------------------- | ----------------------- | ------ | --------------------------------- |
| `symbols`             | symbols.json            | Upsert | 1 lần setup, update nếu cần       |
| `klines`              | features.parquet        | Append | Dữ liệu mới mỗi phút (minutely_extract)    |
| `ticker_24h`          | ticker_24h.csv          | Append | 50 records/ngày (daily_snapshot)       |
| `order_book_snapshot` | order_book_snapshot.csv | Append | 50 records/ngày (daily_snapshot)       |
| `predictions`         | (từ inference)           | Append | 3,000 records/giờ (hourly_inference)   |

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

| DAG                  | Schedule        | Mô tả                                        |
| -------------------- | --------------- | ---------------------------------------------- |
| `minutely_extract`   | * * * * *       | Extract → Transform → Load klines mỗi phút     |
| `daily_snapshot`     | 0 0 * * *       | Ticker 24h + Order Book snapshot hàng ngày      |
| `hourly_inference`   | 0 * * * *       | Dự báo 60 phút, update actuals, mỗi giờ        |
| `weekly_retrain`     | 0 3 * * 0       | Train lại model với dữ liệu mới                |

### 7.3. DAG: minutely_extract

```
Trigger: Mỗi phút (* * * * *)
Timeout: 50 giây
max_active_runs: 1 (tránh overlap)

Note: ETL nặng chỉ lần đầu (bulk 3 năm × 50 coins = 78M rows, dùng Spark).
      Sau đó mỗi phút chỉ xử lý 50 rows (1 nến/coin) — Spark vẫn chạy được
      vì giữ nhất quán kiến trúc, overhead chấp nhận được (~15-20s).

┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ extract_klines   │────▶│    transform     │────▶│   load_klines    │
│                  │     │                  │     │                  │
│ - GET /klines    │     │ - Spark job      │     │ - Spark JDBC     │
│   ?limit=1       │     │ - Calc RSI, MACD │     │ - klines table   │
│ - 50 coins       │     │ - Save Parquet   │     │ - upsert         │
└──────────────────┘     └──────────────────┘     └──────────────────┘
     ~5 giây                   ~10 giây                  ~5 giây
```

> **Spark cho incremental:** Mặc dù 50 rows/phút thì Spark hơi overkill, nhưng giữ Spark để thống nhất codebase với bulk processing (78M rows) và giữ giá trị showcasing cho portfolio.

### 7.4. DAG: daily_snapshot

```
Trigger: 00:00 AM mỗi ngày (0 0 * * *)
Timeout: 15 phút

┌────────────────────┐     ┌────────────────────┐
│  extract_ticker    │───▶│    load_ticker      │
│                    │     │                    │
│- GET /ticker/24hr  │     │ - Spark JDBC       │
│- GET /bookTicker   │     │ - ticker_24h table │
│- Save CSV          │     │                    │
└────────────────────┘     └────────────────────┘
       ~3 giây                    ~5 giây

┌────────────────────┐     ┌────────────────────┐
│ extract_order_book │───▶│  load_order_book    │
│                    │     │                    │
│- GET /depth        │     │ - Spark JDBC       │
│- 50 coins          │     │ - order_book table │
│- Save CSV          │     │                    │
└────────────────────┘     └────────────────────┘
       ~10 giây                   ~5 giây

(2 nhánh chạy song song)
```

> **Note:** Ticker 24h là rolling 24h snapshot, Order book là snapshot áp lực mua/bán — cả hai lấy 1 lần/ngày là đủ cho phân tích.

### 7.5. DAG: weekly_retrain

```
Trigger: 03:00 AM Chủ Nhật
Timeout: 4 giờ

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ prepare_data │────▶│    train     │────▶│   evaluate   │
│              │     │              │     │              │
│ - Query DB   │     │ - LSTM train │     │ - Calc MAE   │
│ - Create     │     │ - GPU        │     │ - Save model │
│   sequences  │     │ - 100 epochs │     │   if better  │
└──────────────┘     └──────────────┘     └──────────────┘
    10 phút              2 giờ               5 phút
```

### 7.6. DAG: hourly_inference

```
Trigger: Đầu mỗi giờ (0 * * * *)
Timeout: 10 phút

┌──────────────┐     ┌──────────────┐     ┌─────────────────────┐     ┌──────────────┐
│  load_model  │────▶│   predict    │────▶│  save_predictions   │────▶│ update_actual│
│              │     │              │     │                     │     │              │
│ - Load .pth  │     │ - Get last   │     │ - Insert vào bảng   │     │ - Cập nhật   │
│ - To GPU     │     │   360 nến    │     │   predictions       │     │   actual_close│
│              │     │ - Predict    │     │ - 50 coins × 60     │     │   cho dự báo │
│              │     │   next 60 min│     │ = 3,000 records/giờ│     │   đã qua     │
└──────────────┘     └──────────────┘     └─────────────────────┘     └──────────────┘
    1 phút               3 phút                 1 phút                    1 phút
```

> **Note:** Task `update_actual` cập nhật giá close thực tế từ bảng klines cho các dự báo đã qua và tính error_pct.

---

## 8. Model LSTM — Dự báo giá nến 1-min (Scalping/Intraday)

### 8.1. Thiết kế tổng quan

**Bài toán:** Dùng 360 nến 1-min gần nhất (6h lookback) để dự báo giá close 60 nến tiếp theo (1h ahead).

**Tại sao 1-min scalping?**

| Khía cạnh            | Daily aggregated          | 1-min candles (được chọn)       |
| -------------------- | ------------------------- | -------------------------------- |
| Actionable           | 7 ngày chờ — chậm           | 1h ahead — trader action ngay    |
| RSI/MACD             | Phải aggregate lại daily  | RSI(14 phút) = scalping đúng chuẩn |
| Data volume          | ~1,095 rows/coin           | ~1.58M rows/coin — Big Data!    |
| Training time        | Vài phút                  | Hàng giờ (GPU recommended)       |
| Ứng dụng             | Portfolio rebalancing      | Scalping, day trading           |
| Prediction frequency | 1 lần/ngày                 | 24 lần/ngày (mỗi giờ)            |

**Pipeline:** Klines 1-min (raw) → Spark Transform (RSI-14, MACD) → LSTM (360→60)

### 8.2. Cấu hình

| Parameter    | Value | Giải thích                                                        |
| ------------ | ----- | ----------------------------------------------------------------- |
| Input window | 360   | 360 nến 1-min = 6 giờ lookback (bao trùm phên giao dịch)           |
| Features     | 7     | open, high, low, close, volume, rsi_14, macd                     |
| Hidden size  | 128   | Đủ lớn cho 1-min time series với nhiều noise                       |
| Num layers   | 2     | 2 LSTM layers                                                    |
| Output       | 60    | Predicted close price 60 phút tiếp theo                           |
| Dropout      | 0.2   | Moderate — data lớn đủ để không cần dropout cao                    |

### 8.3. Training Strategy

| Aspect               | Value            |
| -------------------- | ---------------- |
| Train/Val/Test split | 70% / 15% / 15% |
| Optimizer            | Adam, lr=0.001   |
| Loss function        | MSE              |
| Epochs               | 50               |
| Early stopping       | Patience = 10    |
| Batch size           | 64               |

### 8.4. Input/Output Shape

```
Input:  (batch_size, 360, 7)   # 360 nến 1-min (6h), 7 features/nến
Output: (batch_size, 60)       # predicted close price cho 60 phút tiếp theo
```

### 8.5. Data Volume (cho LSTM)

```
Training samples/coin = 1,576,800 nến - 360 (input) - 60 (output) + 1 = 1,576,381 samples
Tổng = 50 coins × 1,576,381 ≈ 78.8M training samples
~78M rows — đây là lý do cần Spark + GPU
```

---

## 9. Infrastructure

### 9.1. Docker Services

| Service           | Image              | Port | RAM | Mục đích       |
| ----------------- | ------------------ | ---- | --- | -------------- |
| PostgreSQL        | postgres:15        | 5432 | 4GB | Data Warehouse |
| Airflow Webserver | apache/airflow:2.8 | 8080 | 2GB | DAG UI         |
| Airflow Scheduler | apache/airflow:2.8 | -    | 1GB | Chạy DAGs      |
| Grafana           | grafana/grafana    | 3000 | 1GB | Dashboard      |

**Tổng RAM cho Docker:** ~8GB

### 9.2. Native Services (chạy trên host)

| Component | Lý do không Docker                      |
| --------- | --------------------------------------- |
| PySpark   | Cần nhiều RAM, chạy native hiệu quả hơn |
| PyTorch   | Cần truy cập GPU trực tiếp              |

### 9.3. Cấu trúc thư mục

```
crypto-pipeline/
├── config/
│   ├── config.py               # Centralized config (paths, DB, API, Spark, Model)
│   └── symbols.py              # SYMBOL_REGISTRY (50 coins, single source of truth)
├── airflow/
│   └── dags/
│       ├── minutely_extract.py # ETL klines mỗi phút (* * * * *)
│       ├── daily_snapshot.py   # Ticker 24h + Order Book (0 0 * * *)
│       ├── weekly_retrain.py   # Retrain Chủ Nhật 03:00 AM
│       └── hourly_inference.py # Inference mỗi giờ (0 * * * *)
├── data/
│   ├── raw/                    # Data Lake - Raw (CSV)
│   │   ├── BTCUSDT.csv
│   │   └── ...
│   └── processed/              # Data Lake - Processed (Parquet)
│       └── features.parquet
├── models/
│   ├── model.py                # LSTM definition (PyTorch)
│   └── lstm_v1.pth             # Trained weights
├── scripts/
│   ├── pre_extract.py          # Self-healing gap detection + recovery
│   ├── extract.py              # Data Vision bulk + REST API mỗi phút
│   ├── transform.py            # Spark: RSI-14, MACD-12/26/9 trên 1-min
│   ├── load.py                 # Spark JDBC upsert → PostgreSQL
│   ├── train.py                # LSTM training (360→60, 1-min candles)
│   ├── inference.py            # Predict 60 phút cho 50 coins (mỗi giờ)
│   └── update_actuals.py       # Cập nhật actual_close + error_pct
├── utils/
│   ├── binance_utils.py        # API wrappers (retry, rate limit)
│   ├── db_utils.py             # SQLAlchemy, Spark JDBC, upsert
│   ├── data_utils.py           # Timestamps, merge CSV, date utils
│   ├── exceptions.py           # Custom exceptions (E/T/L layers)
│   └── logger.py               # Logging config
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
│   TOP GAINERS 24H (Table)       │           ACTUAL vs PREDICTED (Time Series)       │
│   ┌───────────────────────┐     │   ┌─────────────────────────────────────────────┐ │
│   │ PEPE  +15.2%          │     │   │  ── Actual   ── Predicted                   │ │
│   │ WIF   +12.8%          │     │   │                                             │ │
│   │ RUNE  +8.5%           │     │   └─────────────────────────────────────────────┘ │
│   └───────────────────────┘     │                                                   │
│                                 │                                                   │
├─────────────────────────────────┼───────────────────────────────────────────────────┤
│                                 │                                                   │
│   TOP LOSERS 24H (Table)        │           MODEL PERFORMANCE (Stats)               │
│   ┌───────────────────────┐     │   ┌──────────┬──────────┬──────────┬──────────┐  │
│   │ FTM   -8.3%           │     │   │   MAE    │   RMSE   │  MAPE    │ Accuracy │  │
│   │ ALGO  -6.2%           │     │   │  $125.5  │  $180.2  │  0.12%   │  92.5%   │  │
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
| Data source | PostgreSQL             |
| Refresh     | 1 giờ                  |

```sql
SELECT
    s.base_asset AS coin,
    t.quote_volume_24h / 1000000000 AS volume_billion_usd
FROM ticker_24h t
JOIN symbols s ON t.symbol = s.symbol
WHERE t.snapshot_time = (SELECT MAX(snapshot_time) FROM ticker_24h)
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
WHERE t.snapshot_time = (SELECT MAX(snapshot_time) FROM ticker_24h)
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
WHERE t.snapshot_time = (SELECT MAX(snapshot_time) FROM ticker_24h)
  AND t.price_change_pct < 0
ORDER BY t.price_change_pct ASC
LIMIT 5;
```

#### Panel 4: BTC Price Chart (Time Series)

| Thuộc tính | Giá trị      |
| ---------- | ------------ |
| Loại       | Time series  |
| Time range | Last 7 days  |

> **Lưu ý:** Dữ liệu klines cập nhật mỗi phút (minutely_extract DAG). Chart hiển thị dữ liệu 1-min từ DB, refresh 1 phút để cập nhật real-time.

```sql
SELECT
    timestamp,
    close AS price
FROM klines
WHERE symbol = $symbol
  AND timestamp >= NOW() - INTERVAL '7 days'
ORDER BY timestamp;
```

#### Panel 5: Actual vs Predicted (Time Series)

| Thuộc tính | Giá trị                           |
| ---------- | --------------------------------- |
| Loại       | Time series (2 lines)             |
| Legend     | Actual (blue), Predicted (orange) |

```sql
SELECT
    p.target_time AS timestamp,
    p.actual_close,
    p.predicted_close
FROM predictions p
WHERE p.symbol = $symbol
  AND p.actual_close IS NOT NULL
  AND p.target_time >= NOW() - INTERVAL '30 days'
ORDER BY p.target_time;
```

> **Lưu ý:** Predictions cập nhật mỗi giờ (60 phút/lần), hiển thị 7 ngày để thấy trend model performance. Mỗi giờ có thêm 60 data points/coin.

#### Panel 6: Model Performance (Stat Panels)

| Metric   | Query                                                                                                                  |
| -------- | ---------------------------------------------------------------------------------------------------------------------- |
| MAE      | `SELECT AVG(ABS(actual_close - predicted_close)) FROM predictions WHERE actual_close IS NOT NULL`                      |
| MAPE     | `SELECT AVG(ABS(actual_close - predicted_close) / actual_close * 100) FROM predictions WHERE actual_close IS NOT NULL` |
| Accuracy | `SELECT COUNT(*) FILTER (WHERE error_pct < 1) * 100.0 / COUNT(*) FROM predictions WHERE actual_close IS NOT NULL`      |

#### Panel 7: RSI Heatmap (Table with color)

| Thuộc tính | Giá trị                               |
| ---------- | ------------------------------------- |
| Loại       | Table với color threshold             |
| Colors     | Red (>70), Green (<30), White (30-70) |

```sql
SELECT
    s.base_asset AS coin,
    k.rsi_14
FROM klines k
JOIN symbols s ON k.symbol = s.symbol
WHERE k.timestamp = (
    SELECT MAX(timestamp) FROM klines WHERE symbol = k.symbol
)
ORDER BY k.rsi_14 DESC;
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
| Prediction Error High | MAPE > 5% trong tuần qua               | Slack        |
| Volume Spike          | Volume 24h tăng > 200% so với hôm trước | Slack        |

### 10.5. Refresh Rate

> **Lưu ý:** Dữ liệu klines cập nhật mỗi phút (minutely_extract DAG), predictions cập nhật mỗi giờ (hourly_inference DAG). Refresh interval càng nhỏ càng nhanh thấy dữ liệu mới.

| Panel type        | Refresh interval | Giải thích                                       |
| ----------------- | ---------------- | ------------------------------------------------ |
| Price charts      | 1 phút           | Klines cập nhật mỗi phút, real-time tracking     |
| Volume/Gainers    | 1 giờ            | Snapshot daily, thay đổi ít                       |
| Model performance | 1 giờ            | Predictions cập nhật mỗi giờ                     |
| RSI Heatmap       | 1 phút           | RSI trên 1-min, cập nhật mỗi phút theo klines    |

---

## 11. Lộ trình Thực hiện (4 Tuần)

### Tuần 1: Setup & Extract

| Ngày | Task                                        | Deliverable                      |
| ---- | ------------------------------------------- | -------------------------------- |
| 1-2  | Setup Docker (PostgreSQL, Airflow, Grafana) | docker-compose.yml chạy được     |
| 3-4  | Viết extract.py, download dữ liệu 3 năm     | data/raw/\*.csv đầy đủ           |
| 5-7  | Test, verify data integrity                 | Không thiếu nến, không duplicate |

### Tuần 2: Transform & Load

| Ngày | Task                              | Deliverable                     |
| ---- | --------------------------------- | ------------------------------- |
| 1-3  | Viết transform.py với Spark       | data/processed/features.parquet |
| 4-5  | Viết load.py, tạo bảng PostgreSQL | Dữ liệu trong DB, query được    |
| 6-7  | Test full ETL pipeline            | E→T→L chạy end-to-end           |

### Tuần 3: Airflow & Automation

| Ngày | Task                                      | Deliverable                  |
| ---- | ----------------------------------------- | ---------------------------- |
| 1-2  | Setup Airflow, tạo minutely_extract DAG  | DAG chạy được từ UI          |
| 3-4  | Tạo weekly_retrain, hourly_inference DAGs | Tất cả DAGs hoạt động        |
| 5-7  | Test scheduling, error handling           | Retry hoạt động, logs đầy đủ |

### Tuần 4: Model & Dashboard

| Ngày | Task                          | Deliverable             |
| ---- | ----------------------------- | ----------------------- |
| 1-3  | Viết train.py, train LSTM     | models/lstm_v1.pth      |
| 4-5  | Setup Grafana dashboard       | Dashboard hiển thị đúng |
| 6-7  | Test end-to-end, viết báo cáo | Demo hoàn chỉnh         |

---

## 12. Checklist Trước Khi Demo

### Data Pipeline

- [ ] Extract: Download đủ 50 coins × 3 năm
- [ ] Transform: RSI, MACD tính đúng
- [ ] Load: Dữ liệu trong PostgreSQL, query nhanh

### Automation

- [ ] Airflow Web UI accessible (port 8080)
- [ ] minutely_extract DAG chạy thành công
- [ ] weekly_retrain DAG chạy thành công
- [ ] hourly_inference DAG chạy thành công

### Model

- [ ] LSTM train được, loss giảm
- [ ] Model saved (.pth file)
- [ ] Inference pipeline hoạt động

### Visualization

- [ ] Grafana accessible (port 3000)
- [ ] Dashboard hiển thị giá (cập nhật mỗi phút)
- [ ] Dashboard hiển thị predicted vs actual

---

## 13. Các Rủi ro & Giải pháp

| Rủi ro                  | Xác suất   | Tác động        | Giải pháp                                         |
| ----------------------- | ---------- | --------------- | ------------------------------------------------- |
| Binance rate limit      | Cao        | ETL fail        | Dùng Binance Data Vision cho historical data      |
| API lỗi tạm thời        | Trung bình | Mất data ngắn   | Retry với exponential backoff, skip 404 ngay       |
| Thiếu dữ liệu theo phút | Trung bình | Model lỗi       | Resample + forward fill hoặc linear interpolation |
| Downtime ngắn (< 30 ngày) | Thấp     | Gap dữ liệu     | Self-healing tự phát hiện và dùng REST API backfill |
| Downtime dài (≥ 30 ngày)  | Thấp     | Gap dữ liệu lớn | Self-healing dùng Data Vision bulk + REST API phần còn lại |
| Coin bị delist/migrate  | Thấp       | Dữ liệu bị cắt  | BREAK status + break_date giới hạn fetch tự động  |
| Spark out of memory     | Trung bình | Transform fail  | Tăng partition, xử lý theo batch nhỏ              |
| Model không hội tụ      | Trung bình | Prediction tệ   | Tune hyperparameters, thử architecture khác       |
| PostgreSQL slow query   | Thấp       | Dashboard lag   | Thêm index, partition table                       |
| Airflow scheduler crash | Thấp       | Jobs không chạy | Auto-restart với Docker, monitoring               |

### 13.1. Self-Healing Extract (`_pre_extract`)

Pipeline tự động phát hiện và phục hồi gap dữ liệu mỗi lần chạy `minutely_extract`, không cần can thiệp thủ công.

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
