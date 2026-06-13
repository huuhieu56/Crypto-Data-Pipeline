# Hệ thống Data Pipeline Big Data & LLM Advisor - Tư vấn giao dịch Crypto

---

## 1. Tổng quan Dự án

### 1.1. Bối cảnh & Vấn đề

Thị trường cryptocurrency hoạt động 24/7 với khối lượng giao dịch khổng lồ. Việc phân tích và đưa ra quyết định giao dịch đòi hỏi:

- Xử lý dữ liệu lớn (hàng triệu records)
- Pipeline tự động, không can thiệp thủ công
- AI chatbot tương tác, phân tích đa chiều dựa trên dữ liệu thị trường thực

### 1.2. Mục tiêu Đồ án

Xây dựng hệ thống **End-to-End Data Pipeline** minh họa vai trò của Data Engineer + AI Engineer:

| Giai đoạn       | Công nghệ             | Mục đích                                                |
| --------------- | --------------------- | ------------------------------------------------------- |
| **Extract**     | Python + Binance API  | Thu thập dữ liệu nến từ sàn giao dịch                   |
| **Transform**   | Python (pandas)       | Tính RSI/MACD + metrics order book (OBI, spread, walls) |
| **Load**        | ClickHouse            | Lưu trữ có cấu trúc, phục vụ analytics                  |
| **Store**       | MinIO (S3-compatible) | Object storage cho raw/processed data (Data Lake)       |
| **Orchestrate** | Apache Airflow        | Tự động hóa và lập lịch các jobs                        |
| **Chat**        | LLM (Gemini / OpenAI) | Chatbot tư vấn thị trường tương tác                     |
| **Visualize**   | Grafana               | Dashboard theo dõi real-time                            |

### 1.3. Phạm vi & Giới hạn

| Thành phần      | Giá trị                   | Ghi chú                          |
| --------------- | ------------------------- | -------------------------------- |
| Số lượng coin   | 50                        | Top vốn hóa, loại bỏ stablecoins |
| Khung thời gian | Nến 1 phút                | Phù hợp phân tích ngắn hạn       |
| Dữ liệu lịch sử | 3 năm (01/2023 - 01/2026) | Đủ để phân tích xu hướng         |
| AI Chat         | Chatbot tư vấn tương tác  | Dựa trên 30 nến daily + snapshot |

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

### 3.2. Bảng `ticker_24h` (Minutely snapshot)

```
Số records = 50 coins × 3 năm × 365 ngày × 24 giờ × 60 phút
           = 50 × 3 × 525,600
           = 78,840,000 records

Kích thước = 78.84 triệu × 150 bytes/record ≈ 11.2 GB
```

| Bảng                  | Số records     | Kích thước | Tần suất ghi  |
| --------------------- | -------------- | ---------- | ------------- |
| `symbols`             | 50             | < 1 KB     | 1 lần (setup) |
| `klines`              | 78,840,000     | ~7.5 GB    | 50 recs/phút  |
| `ticker_24h`          | 78,840,000     | ~11.2 GB   | 50 recs/phút  |
| `order_book_snapshot` | 78,840,000     | ~7.5 GB    | 50 recs/phút  |
| **Total**             | **~237 triệu** | **~26 GB** | -             |

### 3.5. Breakdown theo layer

| Layer                 | Format          | Kích thước | Ghi chú                             |
| --------------------- | --------------- | ---------- | ----------------------------------- |
| Raw (Data Lake)       | CSV → MinIO     | ~7.5 GB    | Dữ liệu gốc từ Binance              |
| Processed (Data Lake) | Parquet → MinIO | ~2 GB      | Nén tốt hơn CSV, có indicators      |
| Warehouse             | ClickHouse      | ~5 GB      | Bao gồm index + pre-aggregated data |

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
│                     │        └─ ClickHouse ─┘         │             │          │
│                     └─────────────────────────────────┘             │          │
├─────────────────────────────────────────────────────────────────────┼──────────┤
│                                                                     ▼          │
│  ┌─────────────────────────────────┐    ┌─────────────────────────────────┐   │
│  │        LLM CHAT ASSISTANT       │    │         VISUALIZATION           │   │
│  │  ┌───────────┐ ┌─────────────┐  │    │                                 │   │
│  │  │  Gemini / │ │chat-api     │  │    │         Grafana                 │   │
│  │  │  OpenAI   │ │FastAPI (:8501)│  │    │         Dashboard (w/ iframe)   │   │
│  │  └───────────┘ └─────────────┘  │    │                                 │   │
│  └─────────────────────────────────┘    └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2. Data Lake vs Data Warehouse

| Khía cạnh         | Data Lake                                        | Data Warehouse            |
| ----------------- | ------------------------------------------------ | ------------------------- |
| **Vị trí**        | MinIO buckets (`crypto-raw`, `crypto-processed`) | ClickHouse database       |
| **Format**        | CSV (raw), Parquet (processed)                   | Bảng SQL có schema        |
| **Schema**        | Schema-on-read (linh hoạt)                       | Schema-on-write (cố định) |
| **Mục đích**      | Lưu trữ, batch processing                        | Query nhanh, serving      |
| **Công cụ xử lý** | Python (pandas)                                  | ClickHouse SQL            |
| **Ai dùng**       | Data Engineer, Data Scientist                    | Analyst, Dashboard, LLM   |

### 4.3. Luồng dữ liệu chi tiết

```
[1] EXTRACT                    [2] TRANSFORM                 [3] LOAD
  Binance API                    Python (pandas)              ClickHouse
         │                              │                             │
         ▼                              ▼                             ▼
    ┌─────────┐                  ┌─────────────┐              ┌─────────────┐
  │  OHLCV  │   MinIO          │  + RSI      │   ClickHouse │   klines    │
  │  data   │  ──────────────▶ │  + MACD     │  ─────────▶  │   table     │
  │  (CSV)  │                  │  (Parquet)  │  insert_df   │             │
    └─────────┘                  └─────────────┘              └─────────────┘

    MinIO: crypto-raw/           MinIO: crypto-processed/       crypto_db
    BTCUSDT.csv                  klines/{SYMBOL}/{YYYY-MM}.parquet
```