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

### 3.3. Bảng `predictions` (Hourly inference - 60 nến/lần)

```
Số records = 50 coins × 3 năm × 365 ngày × 24 giờ × 60 predictions/lần
           = 50 × 3 × 8,760 × 60
           = 78,840,000 records

Kích thước = 78.84 triệu × 80 bytes/record ≈ 6 GB
```

### 3.4. Tổng hợp theo bảng

| Bảng          | Số records     | Kích thước   | Tần suất ghi      |
| ------------- | -------------- | ------------ | ----------------- |
| `symbols`     | 50             | < 1 KB       | 1 lần (setup)     |
| `klines`      | 78,840,000     | ~7.5 GB      | 72,000 recs/ngày  |
| `ticker_24h`  | 54,750         | ~8 MB        | 50 recs/ngày      |
| `predictions` | 78,840,000     | ~6 GB        | 72,000 recs/ngày  |
| **Total**     | **~158 triệu** | **~13.5 GB** | -                 |

### 3.5. Breakdown theo layer

| Layer                 | Format     | Kích thước | Ghi chú                            |
| --------------------- | ---------- | ---------- | ---------------------------------- |
| Raw (Data Lake)       | CSV        | ~7.5 GB    | Dữ liệu gốc từ Binance             |
| Processed (Data Lake) | Parquet    | ~2 GB      | Nén tốt hơn CSV                    |
| Warehouse             | PostgreSQL | ~16 GB     | Bao gồm index + predictions (~6GB) |

> **Kết luận:** Dữ liệu ~158 triệu records đủ lớn để minh họa Big Data processing với Spark, vẫn chạy được trên máy 16GB RAM.

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
    │  (CSV)  │   Window Func    │  (Parquet)  │              │             │
    └─────────┘                  └─────────────┘              └─────────────┘

    data/raw/                    data/processed/               crypto_dw
    BTCUSDT.csv                  features.parquet
```

---

## 5. Database Schema

### 5.1. Tổng quan - Star Schema (4 bảng)

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
```

> **Thiết kế Star Schema:** 1 Dimension table (`symbols`) + 3 Fact tables (`klines`, `ticker_24h`, `predictions`)

### 5.2. Nguồn dữ liệu từ Binance API

| Bảng          | Binance API Endpoint   | Mô tả                               | Tần suất thu thập |
| ------------- | ---------------------- | ----------------------------------- | ----------------- |
| `symbols`     | `/api/v3/exchangeInfo` | Thông tin trading pair              | 1 lần (setup)     |
| `klines`      | `/api/v3/klines`       | Dữ liệu nến (OHLCV)                 | Daily             |
| `ticker_24h`  | `/api/v3/ticker/24hr`  | Thống kê 24h (volume, price change) | Daily             |
| `predictions` | (Internal)             | Kết quả từ LSTM model               | Hourly            |

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

> **Mục đích:** Lưu riêng kết quả dự báo 60 nến/lần, tách biệt khỏi dữ liệu thực tế để dễ đánh giá model performance.

| Column            | Type        | Description                       | Nguồn      |
| ----------------- | ----------- | --------------------------------- | ---------- |
| `symbol`          | VARCHAR(20) | FK → symbols.symbol               | System     |
| `predicted_at`    | TIMESTAMPTZ | Thời điểm chạy dự báo             | System     |
| `step_index`      | INTEGER     | Vị trí nến dự báo (1-60)          | System     |
| `target_time`     | TIMESTAMPTZ | Thời điểm được dự báo             | System     |
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
| BTCUSDT | 2026-01-26 10:00:00 |  1   | 2026-01-26 10:01:00 | 102,345.5 | 102,400 | 0.053%    |
| BTCUSDT | 2026-01-26 10:00:00 |  2   | 2026-01-26 10:02:00 | 102,380.2 | 102,420 | 0.039%    |
| ...     | ...                 | ...  | ...                 | ...       | ...     | ...       |
| BTCUSDT | 2026-01-26 10:00:00 | 60   | 2026-01-26 11:00:00 | 102,890.5 | 102,850 | 0.039%    |
```

> **Ý nghĩa:** Tách predictions ra bảng riêng giúp:
>
> - Theo dõi performance của model theo thời gian
> - So sánh giữa các model versions
> - Không làm "ô nhiễm" dữ liệu gốc trong bảng klines

### 5.7. Relationships Diagram

```sql
-- Foreign Key Constraints
ALTER TABLE klines ADD CONSTRAINT fk_klines_symbol
    FOREIGN KEY (symbol) REFERENCES symbols(symbol);

ALTER TABLE ticker_24h ADD CONSTRAINT fk_ticker_symbol
    FOREIGN KEY (symbol) REFERENCES symbols(symbol);

ALTER TABLE predictions ADD CONSTRAINT fk_predictions_symbol
    FOREIGN KEY (symbol) REFERENCES symbols(symbol);
```

### 5.8. Ví dụ SQL Queries (JOIN các bảng)

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

### 5.9. Giải thích các chỉ số kỹ thuật

| Chỉ số          | Công thức          | Ý nghĩa                                         |
| --------------- | ------------------ | ----------------------------------------------- |
| **RSI (14)**    | 100 - 100/(1 + RS) | Đo momentum, > 70 = overbought, < 30 = oversold |
| **MACD**        | EMA(12) - EMA(26)  | Đo xu hướng và momentum                         |
| **MACD Signal** | EMA(9) của MACD    | Tín hiệu mua/bán khi MACD cắt Signal            |

---

## 6. ETL Pipeline

### 6.1. Tổng quan luồng dữ liệu (4 bảng)

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    EXTRACT                                               │
│                                                                                          │
│   ┌─────────────────┐   ┌─────────────────────┐   ┌─────────────────────────┐           │
│   │ /exchangeInfo   │   │    /klines          │   │    /ticker/24hr         │           │
│   │ (1 lần setup)   │   │ (Daily: 50 coins)   │   │ (Daily: 50 coins)       │           │
│   └────────┬────────┘   └──────────┬──────────┘   └────────────┬────────────┘           │
│            │                       │                           │                         │
│            ▼                       ▼                           ▼                         │
│      symbols.json            raw/{SYMBOL}.csv            ticker_24h.csv                 │
└────────────┬───────────────────────┬───────────────────────────┬────────────────────────┘
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
│    ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌───────────────┐  │
│    │    symbols      │   │     klines      │   │   ticker_24h    │   │  predictions  │  │
│    │  (dim table)    │   │  (fact table)   │   │  (fact table)   │   │ (fact table)  │  │
│    └─────────────────┘   └─────────────────┘   └─────────────────┘   └───────────────┘  │
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

#### 6.2.2. Extract Klines (Daily)

| Thuộc tính | Giá trị                                                                |
| ---------- | ---------------------------------------------------------------------- |
| API        | `/api/v3/klines`                                                       |
| Tần suất   | Daily (2:00 AM)                                                        |
| Chiến lược | Bulk download từ Binance Data Vision (lịch sử), REST API (dữ liệu mới) |
| Output     | `data/raw/{SYMBOL}.csv`                                                |
| Ghi vào    | Bảng `klines` (sau Transform)                                          |

#### 6.2.3. Extract Ticker 24h (Daily)

| Thuộc tính | Giá trị                            |
| ---------- | ---------------------------------- |
| API        | `/api/v3/ticker/24hr`              |
| Tần suất   | Daily (2:30 AM)                    |
| Đặc điểm   | 1 request lấy được tất cả 50 coins |
| Output     | `data/raw/ticker_24h.csv`          |
| Ghi vào    | Bảng `ticker_24h`                  |

> **Lưu ý:** API ticker/24hr trả về snapshot tại thời điểm gọi, không phải dữ liệu lịch sử. Mỗi ngày lưu 1 record/coin.

### 6.3. Transform - Xử lý với Spark

**Input:** CSV files từ Data Lake (`data/raw/*.csv`)

**Xử lý:**

1. Đọc tất cả CSV vào Spark DataFrame
2. Tính RSI(14) sử dụng Window Function
3. Tính MACD và MACD Signal
4. Xử lý missing values (forward fill)

**Output:** `data/processed/features.parquet`

> **Tại sao dùng Spark?** Dữ liệu 78 triệu records, Pandas sẽ chậm và tốn RAM. Spark xử lý distributed, tối ưu cho batch processing.

### 6.4. Load - Ghi vào PostgreSQL

| Bảng          | Input              | Mode   | Ghi chú                           |
| ------------- | ------------------ | ------ | --------------------------------- |
| `symbols`     | symbols.json       | Upsert | 1 lần setup, update nếu cần       |
| `klines`      | features.parquet   | Append | Dữ liệu mới mỗi ngày              |
| `ticker_24h`  | ticker_24h.csv     | Append | 50 records/ngày                   |
| `predictions` | (từ inference DAG) | Append | 50 records/giờ (hourly inference) |

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

| DAG                | Schedule        | Mô tả                                  |
| ------------------ | --------------- | -------------------------------------- |
| `daily_etl`        | 02:00 AM daily  | Extract → Transform → Load dữ liệu mới |
| `weekly_retrain`   | 03:00 AM Sunday | Train lại model với dữ liệu mới        |
| `hourly_inference` | Every hour      | Chạy dự báo và lưu kết quả             |

### 7.3. DAG: daily_etl

```
Trigger: 02:00 AM mỗi ngày
Timeout: 2 giờ

                           ┌──────────────────┐
                           │  extract_ticker  │
                           │                  │
                           │ - GET /ticker/24hr│
                           │ - Save CSV        │
                           └────────┬─────────┘
                                    │
┌──────────────────┐                │
│  extract_klines  │                │
│                  │                │
│ - GET /klines    │                │
│ - 50 coins       │                ▼
│ - Save CSV       │        ┌──────────────────┐     ┌──────────────────┐
└────────┬─────────┘        │  load_ticker     │     │                  │
         │                  │                  │     │                  │
         │                  │ - JDBC write     │     │                  │
         ▼                  │ - ticker_24h     │     │                  │
┌──────────────────┐        └──────────────────┘     │                  │
│   transform      │                                 │     load_klines  │
│                  │                                 │                  │
│ - Spark job      │─────────────────────────────────▶ - JDBC write    │
│ - Calc RSI, MACD │                                 │ - klines table  │
│ - Save Parquet   │                                 │                  │
└──────────────────┘                                 └──────────────────┘
      30 phút                   5 phút                    10 phút
```

> **Parallel execution:** `extract_klines` và `extract_ticker` chạy song song để tối ưu thời gian.

### 7.4. DAG: weekly_retrain

```
Trigger: 03:00 AM Chủ Nhật
Timeout: 4 giờ

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ prepare_data │────▶│    train     │────▶│   evaluate   │
│              │     │              │     │              │
│ - Query DB   │     │ - LSTM train │     │ - Calc MAE   │
│ - Create     │     │ - GPU        │     │ - Save model │
│   sequences  │     │ - 20 epochs  │     │   if better  │
└──────────────┘     └──────────────┘     └──────────────┘
    10 phút              2 giờ               5 phút
```

### 7.5. DAG: hourly_inference

```
Trigger: Mỗi giờ (xx:05)
Timeout: 15 phút

┌──────────────┐     ┌──────────────┐     ┌─────────────────────┐     ┌──────────────┐
│  load_model  │────▶│   predict    │────▶│  save_predictions   │────▶│ update_actual│
│              │     │              │     │                     │     │              │
│ - Load .pth  │     │ - Get last   │     │ - Insert vào bảng   │     │ - Sau 1 giờ  │
│ - To GPU     │     │   60 candles │     │   predictions       │     │ - Cập nhật   │
│              │     │ - Predict    │     │ - 50 coins × 60     │     │   actual_close│
│              │     │   next 60    │     │ = 3,000 records/giờ │     │ - Calc error │
└──────────────┘     └──────────────┘     └─────────────────────┘     └──────────────┘
    1 phút               5 phút                 2 phút                    (delayed)
```

> **Note:** Task `update_actual` được schedule chạy sau `save_predictions` 1 giờ để lấy giá thực tế của tất cả 60 nến và tính error_pct.

---

## 8. Model LSTM

### 8.1. Cấu hình

| Parameter    | Value | Giải thích                                   |
| ------------ | ----- | -------------------------------------------- |
| Input window | 60    | Dùng 60 nến quá khứ để dự báo                |
| Features     | 7     | open, high, low, close, volume, rsi_14, macd |
| Hidden size  | 128   | Số neurons trong LSTM layer                  |
| Num layers   | 2     | Số LSTM layers                               |
| Output       | 60    | Giá close của 60 nến tiếp theo (1 giờ)       |
| Dropout      | 0.2   | Tránh overfitting                            |

### 8.2. Training Strategy

| Aspect               | Value           |
| -------------------- | --------------- |
| Train/Val/Test split | 70% / 15% / 15% |
| Optimizer            | Adam, lr=0.001  |
| Loss function        | MSE             |
| Epochs               | 20-50           |
| Early stopping       | Patience = 5    |
| Batch size           | 64              |

### 8.3. Input/Output Shape

```
Input:  (batch_size, 60, 7)    # 60 timesteps, 7 features
Output: (batch_size, 60)       # predicted close prices for next 60 minutes
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
├── airflow/
│   └── dags/
│       ├── daily_etl.py
│       ├── weekly_retrain.py
│       └── hourly_inference.py
├── data/
│   ├── raw/                    # Data Lake - Raw
│   │   ├── BTCUSDT.csv
│   │   └── ...
│   └── processed/              # Data Lake - Processed
│       └── features.parquet
├── models/
│   └── lstm_v1.pth
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── train.py
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

| Thuộc tính | Giá trị       |
| ---------- | ------------- |
| Loại       | Time series   |
| Time range | Last 24 hours |

```sql
SELECT
    timestamp,
    close AS price
FROM klines
WHERE symbol = 'BTCUSDT'
  AND timestamp >= NOW() - INTERVAL '24 hours'
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
WHERE p.symbol = 'BTCUSDT'
  AND p.actual_close IS NOT NULL
  AND p.target_time >= NOW() - INTERVAL '24 hours'
ORDER BY p.target_time;
```

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
| Prediction Error High | MAE > threshold trong 1 giờ             | Slack        |
| Volume Spike          | Volume 24h tăng > 200% so với hôm trước | Slack        |

### 10.5. Refresh Rate

| Panel type        | Refresh interval |
| ----------------- | ---------------- |
| Price charts      | 1 phút           |
| Volume/Gainers    | 1 giờ            |
| Model performance | 1 giờ            |
| RSI Heatmap       | 5 phút           |

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
| 1-2  | Setup Airflow, tạo daily_etl DAG          | DAG chạy được từ UI          |
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
- [ ] daily_etl DAG chạy thành công
- [ ] weekly_retrain DAG chạy thành công
- [ ] hourly_inference DAG chạy thành công

### Model

- [ ] LSTM train được, loss giảm
- [ ] Model saved (.pth file)
- [ ] Inference pipeline hoạt động

### Visualization

- [ ] Grafana accessible (port 3000)
- [ ] Dashboard hiển thị giá real-time
- [ ] Dashboard hiển thị predicted vs actual

---

## 13. Các Rủi ro & Giải pháp

| Rủi ro                  | Xác suất   | Tác động        | Giải pháp                                    |
| ----------------------- | ---------- | --------------- | -------------------------------------------- |
| Binance rate limit      | Cao        | ETL fail        | Dùng Binance Data Vision cho historical data |
| Spark out of memory     | Trung bình | Transform fail  | Tăng partition, xử lý theo batch nhỏ         |
| Model không hội tụ      | Trung bình | Prediction tệ   | Tune hyperparameters, thử architecture khác  |
| PostgreSQL slow query   | Thấp       | Dashboard lag   | Thêm index, partition table                  |
| Airflow scheduler crash | Thấp       | Jobs không chạy | Auto-restart với Docker, monitoring          |
