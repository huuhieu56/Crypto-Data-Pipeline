**Đề tài:** Xây dựng hệ thống Data Pipeline Big Data & Deep Learning (LSTM) phân tích và dự báo giá Crypto.

## **1\. Tổng quan & Mục tiêu**

### **1.1. Mục tiêu Đồ án**

Xây dựng một hệ thống xử lý dữ liệu lớn "End-to-End" minh họa trọn vẹn quy trình của một Data Engineer và AI Engineer:

1. **Thu thập (Ingestion):** Xử lý dòng dữ liệu liên tục từ Binance API.  
2. **Lưu trữ (Polyglot Persistence):** Kết hợp MongoDB (NoSQL) cho dữ liệu thô và PostgreSQL (SQL) cho dữ liệu phân tích.  
3. **Xử lý (Big Data Processing):** Sử dụng **Apache Spark** để làm sạch, tính toán chỉ số kỹ thuật trên tập dữ liệu lớn.  
4. **Phân tích (Deep Learning):** Ứng dụng mạng nơ-ron **LSTM (PyTorch)** chạy trên **GPU** để dự báo xu hướng giá.  
5. **Trực quan hóa (Visualization):** Dashboard thời gian thực.

### **1.2. Chiến lược Thu thập Dữ liệu (Data Ingestion Strategy)**

> ⚠️ **QUAN TRỌNG:** Để tránh bị Binance chặn request (Rate Limit), hệ thống áp dụng chiến lược **Hybrid Ingestion**:

| Loại dữ liệu | Phương pháp | Lý do |
|--------------|-------------|-------|
| **Dữ liệu lịch sử (01/2023 - Thời điểm triển khai)** | Download file CSV/Parquet từ Binance Data Vision | Tránh rate limit, tốc độ cao, ổn định |
| **Dữ liệu real-time (Từ thời điểm triển khai trở đi)** | WebSocket streaming + REST API backup | Độ trễ thấp, cập nhật liên tục |

**Chi tiết triển khai:**

1. **Batch Download (Historical Data):**
   * Sử dụng **Binance Public Data**: `https://data.binance.vision/`
   * Download file ZIP chứa dữ liệu Klines theo tháng/ngày
   * Chạy 1 lần duy nhất khi khởi tạo hệ thống
   * Không bị giới hạn rate limit
   
   > 📌 **Script cần triển khai:** `scripts/download_historical.py`
   > - Tự động download từ Binance Data Vision (không cần gọi API)
   > - Async download song song nhiều files (giới hạn 10 concurrent)
   > - Skip files đã tồn tại, validate sau khi hoàn thành
   > - Import vào MongoDB sau khi download xong

2. **Real-time Streaming (Live Data):**
   * **Primary:** WebSocket stream `wss://stream.binance.com:9443/ws/<symbol>@kline_1m`
   * **Fallback:** REST API với exponential backoff khi WebSocket disconnect
   * Rate limit handling: Max 1200 requests/phút cho REST API
   
   > 📌 **Script cần triển khai:** `scripts/realtime_ingestion.py`
   > - WebSocket client với auto-reconnect
   > - Fallback sang REST API khi WebSocket fail
   > - Ghi log mọi lần disconnect/reconnect

3. **Xử lý Rate Limit:**
   * Implement **Token Bucket Algorithm** để kiểm soát request rate
   * **Exponential Backoff** với jitter khi gặp HTTP 429
   * **Request Queue** với priority cho các symbol quan trọng (BTC, ETH)

### **1.3. Phân tích Quy mô Dữ liệu (Sizing & Big Data Justification)**

Đây là phần chứng minh tính chất "Big Data" của đồ án.

* **Đối tượng:** Top **50 đồng coin** vốn hóa lớn nhất (tất cả pair với USDT):

| # | Symbol | # | Symbol | # | Symbol | # | Symbol | # | Symbol |
|---|--------|---|--------|---|--------|---|--------|---|--------|
| 1 | BTCUSDT | 11 | AVAXUSDT | 21 | NEARUSDT | 31 | INJUSDT | 41 | FTMUSDT |
| 2 | ETHUSDT | 12 | TONUSDT | 22 | APTUSDT | 32 | IMXUSDT | 42 | ALGOUSDT |
| 3 | BNBUSDT | 13 | SHIBUSDT | 23 | ICPUSDT | 33 | OPUSDT | 43 | FLOWUSDT |
| 4 | SOLUSDT | 14 | XLMUSDT | 24 | ETCUSDT | 34 | GRTUSDT | 44 | XTZUSDT |
| 5 | XRPUSDT | 15 | BCHUSDT | 25 | STXUSDT | 35 | THETAUSDT | 45 | AXSUSDT |
| 6 | DOGEUSDT | 16 | DOTUSDT | 26 | RENDERUSDT | 36 | FILUSDT | 46 | SANDUSDT |
| 7 | ADAUSDT | 17 | UNIUSDT | 27 | CROUSDT | 37 | ARUSDT | 47 | MANAUSDT |
| 8 | TRXUSDT | 18 | LTCUSDT | 28 | ATOMUSDT | 38 | MKRUSDT | 48 | NEOUSDT |
| 9 | LINKUSDT | 19 | HBARUSDT | 29 | VETUSDT | 39 | WIFUSDT | 49 | EOSUSDT |
| 10 | MATICUSDT | 20 | PEPEUSDT | 30 | ARBUSDT | 40 | RUNEUSDT | 50 | AAVEUSDT |

> 📌 **Lưu ý:** Danh sách này dựa trên vốn hóa thị trường tại thời điểm thiết kế. Cần cập nhật định kỳ nếu có thay đổi lớn. Loại bỏ stablecoins (USDT, USDC, DAI...) vì không có biến động giá.

* **Thời gian:** Dữ liệu lịch sử 3 năm (01/2023 \- Nay).  
* **Độ phân giải:** Nến 1 phút (1m).

**Tính toán chi tiết khối lượng dữ liệu (Volume):**

1. **Dữ liệu Nến (Klines):**  
   * Công thức: ![][image1].  
   * Mỗi bản ghi Klines gồm: Open, High, Low, Close, Volume, QuoteVol, Trades... (khoảng 150 bytes).  
   * Tổng dung lượng Klines: ![][image2].  
2. **Dữ liệu Sổ lệnh (Order Book Snapshots) \- Yếu tố Big Data chính:**  
   * Để huấn luyện AI chính xác, ta cần chụp snapshot thị trường (Bids/Asks) mỗi 5 phút/lần.  
   * Mỗi Snapshot là một JSON chứa mảng lồng nhau (Top 20 Bids, Top 20 Asks). Kích thước trung bình: 2KB/snapshot.  
   * Công thức: ![][image3].  
   * Tổng dung lượng Order Book: ![][image4].

**Tổng cộng:** Hệ thống cần xử lý và lưu trữ khoảng **\~45 GB** dữ liệu thô. Đây là con số đủ lớn để MySQL thông thường gặp khó khăn khi truy vấn và yêu cầu các giải pháp như MongoDB (Sharding/Replica) và Spark (Distributed Processing).

### **1.4. Data Quality & Validation Strategy**

> ⚠️ **VẤN ĐỀ THƯỜNG GẶP:** Dữ liệu crypto có thể bị thiếu (exchange downtime), trùng lặp (reconnect WebSocket), hoặc sai lệch (API glitch).

**Quy trình đảm bảo chất lượng dữ liệu:**

| Bước | Kiểm tra | Hành động khi phát hiện lỗi |
|------|----------|----------------------------|
| 1. Ingestion | Timestamp hợp lệ, giá > 0 | Reject & log |
| 2. Deduplication | Trùng (symbol, timestamp) | Keep first, discard rest |
| 3. Gap Detection | Thiếu nến > 1 phút | Trigger backfill job |
| 4. Outlier Detection | Giá thay đổi > 50% trong 1 phút | Flag for review |
| 5. Cross-validation | So sánh với nguồn backup | Alert nếu chênh lệch > 1% |

**Metrics theo dõi:**
* `data_freshness_seconds`: Độ trễ dữ liệu mới nhất
* `missing_candle_ratio`: Tỷ lệ nến bị thiếu
* `duplicate_count_24h`: Số bản ghi trùng trong 24h
* `outlier_count_24h`: Số outlier phát hiện được

## **2\. Kiến trúc Hệ thống (Architecture)**

Mô hình luồng dữ liệu (Data Flow) tối ưu cho phần cứng **Host 16GB RAM \+ GPU 4GB VRAM**:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA INGESTION LAYER                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐          │
│  │ Binance Data     │    │ WebSocket        │    │ REST API         │          │
│  │ Vision (Batch)   │    │ Stream (Primary) │    │ (Fallback)       │          │
│  │ [Historical]     │    │ [Real-time]      │    │ [Backup]         │          │
│  └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘          │
│           │                       │                       │                     │
│           └───────────────────────┴───────────────────────┘                     │
│                                   │                                             │
│                          ┌────────▼────────┐                                    │
│                          │ Rate Limiter    │                                    │
│                          │ + Validator     │                                    │
│                          └────────┬────────┘                                    │
└───────────────────────────────────┼─────────────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼─────────────────────────────────────────────┐
│                              RAW STORAGE LAYER                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────────┐           │
│  │                     MongoDB (binance_raw)                         │           │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │           │
│  │  │ klines_1m   │  │ depth_snap  │  │ ingestion   │               │           │
│  │  │ (Sharded)   │  │ (TTL Index) │  │ _logs       │               │           │
│  │  └─────────────┘  └─────────────┘  └─────────────┘               │           │
│  └──────────────────────────────────────────────────────────────────┘           │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼─────────────────────────────────────────────┐
│                              PROCESSING LAYER                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────────┐           │
│  │                     Apache Spark (CPU)                            │           │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │           │
│  │  │ Cleaning &  │─▶│ Feature     │─▶│ Normalize & │               │           │
│  │  │ Dedupe      │  │ Engineering │  │ Export      │               │           │
│  │  └─────────────┘  └─────────────┘  └─────────────┘               │           │
│  └──────────────────────────────────────────────────────────────────┘           │
│                                   │                                             │
│                          ┌────────▼────────┐                                    │
│                          │ Parquet Files   │                                    │
│                          │ (Intermediate)  │                                    │
│                          └────────┬────────┘                                    │
└───────────────────────────────────┼─────────────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼─────────────────────────────────────────────┐
│                              ML/AI LAYER                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────────┐           │
│  │                     PyTorch LSTM (GPU)                            │           │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │           │
│  │  │ Training    │  │ Validation  │  │ Inference   │               │           │
│  │  │ Pipeline    │  │ Pipeline    │  │ Pipeline    │               │           │
│  │  └─────────────┘  └─────────────┘  └─────────────┘               │           │
│  │                                                                   │           │
│  │  ┌─────────────────────────────────────────────────────┐         │           │
│  │  │ Model Registry: models/v{version}/model.pth         │         │           │
│  │  └─────────────────────────────────────────────────────┘         │           │
│  └──────────────────────────────────────────────────────────────────┘           │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼─────────────────────────────────────────────┐
│                              SERVING LAYER                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────┐    ┌─────────────────────────────┐             │
│  │ PostgreSQL (crypto_dw)      │    │ Grafana Dashboard           │             │
│  │ - fact_market_klines        │───▶│ - Real-time Price           │             │
│  │ - fact_predictions          │    │ - Prediction vs Actual      │             │
│  │ - dim_symbols               │    │ - Model Performance         │             │
│  └─────────────────────────────┘    └─────────────────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### **Chi tiết nhiệm vụ từng tầng:**

1. **Ingestion Layer:** 
   * **Batch Downloader:** Tải dữ liệu lịch sử từ Binance Data Vision (chạy 1 lần)
   * **WebSocket Client:** Streaming real-time với auto-reconnect
   * **REST Fallback:** Backup khi WebSocket fail, với rate limiting
   * **Validator:** Kiểm tra schema, loại bỏ dữ liệu không hợp lệ

2. **Raw Storage (MongoDB):** 
   * Lưu trữ dữ liệu dưới dạng Document (JSON) để đảm bảo tốc độ ghi (Write High-Throughput).
   * **Sharding** theo symbol để phân tán tải
   * **TTL Index** cho depth_snapshots (giữ 30 ngày gần nhất)
   * **Ingestion Logs** để audit và debug

3. **Processing Layer (Spark \- CPU):**  
   * Đọc dữ liệu từ Mongo với **Incremental Processing** (chỉ xử lý dữ liệu mới)
   * **Tính toán:** RSI, MACD, Bollinger Bands, Moving Averages.  
   * **Chuẩn hóa:** Min-Max Scaling (về khoảng 0-1 cho AI).
   * **Checkpoint:** Lưu watermark để biết đã xử lý đến đâu

4. **Modeling Layer (PyTorch \- GPU):** 
   * Huấn luyện model LSTM trên dữ liệu đã chuẩn hóa.
   * **Model Versioning:** Lưu model theo version với metadata (accuracy, training date)
   * **A/B Testing:** Support chạy song song nhiều model versions

5. **Serving Layer (PostgreSQL):** 
   * Lưu trữ kết quả dự báo và dữ liệu sạch phục vụ truy vấn báo cáo.
   * **Partitioning** theo thời gian để query hiệu quả

## **3\. Thiết kế Cơ sở Dữ liệu (Schema Design)**

### **3.1. Raw Zone: MongoDB (binance\_raw)**

Lưu trữ phi cấu trúc để linh hoạt.

* **Collection klines\_1m** (Sharded by symbol):  
  ```json
  {
    "_id": ObjectId("..."),
    "s": "BTCUSDT", 
    "t": 1672531200000,
    "o": "46000.5", "h": "46010.0", "l": "45990.0", "c": "46005.0", "v": "15.2",
    "ingested_at": ISODate("2026-01-26T10:00:00Z"),
    "source": "websocket"  // "batch" | "websocket" | "rest_api"
  }
  ```
  **Indexes:**
  - `{ s: 1, t: 1 }` (Compound, Unique) - Tránh duplicate
  - `{ ingested_at: 1 }` - Cho incremental processing

* **Collection depth\_snapshots** (TTL 30 days):  
  ```json
  {
    "_id": ObjectId("..."),
    "s": "BTCUSDT", 
    "t": 1672531200000,
    "bids": [["46000", "1.2"], ["45999", "0.5"]],
    "asks": [["46001", "2.0"], ["46002", "1.5"]],
    "expire_at": ISODate("2026-02-25T10:00:00Z")
  }
  ```
  **Indexes:**
  - `{ expire_at: 1 }` (TTL Index) - Tự động xóa sau 30 ngày
  - `{ s: 1, t: 1 }` (Compound)

* **Collection ingestion\_logs** (Audit & Debug):
  ```json
  {
    "_id": ObjectId("..."),
    "timestamp": ISODate("2026-01-26T10:00:00Z"),
    "source": "websocket",
    "symbols_processed": 50,
    "records_inserted": 50,
    "errors": [],
    "duration_ms": 150
  }
  ```

* **Collection processing\_watermarks** (Checkpoint cho Spark):
  ```json
  {
    "_id": "spark_etl_klines",
    "last_processed_timestamp": 1672531200000,
    "last_run": ISODate("2026-01-26T09:00:00Z"),
    "records_processed": 1000000
  }
  ```

### **3.2. Processed Zone: PostgreSQL (crypto\_dw)**

Sử dụng mô hình **Star Schema** tối ưu cho truy vấn.

* **Table dim\_symbols** (Danh mục coin): 
  ```sql
  CREATE TABLE dim_symbols (
    symbol_id SERIAL PRIMARY KEY,
    pair VARCHAR(20) NOT NULL UNIQUE,  -- BTCUSDT
    base_asset VARCHAR(10) NOT NULL,   -- BTC
    quote_asset VARCHAR(10) NOT NULL,  -- USDT
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
  );
  ```

* **Table fact\_market\_klines** (Dữ liệu lịch sử \- Partition theo tháng):  
  ```sql
  CREATE TABLE fact_market_klines (
    time TIMESTAMPTZ NOT NULL,
    symbol_id INTEGER REFERENCES dim_symbols(symbol_id),
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    -- Các chỉ báo kỹ thuật (từ Spark ETL)
    rsi_14 DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    bb_upper DOUBLE PRECISION,
    bb_lower DOUBLE PRECISION,
    volatility_20 DOUBLE PRECISION,
    -- Metadata
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, symbol_id)
  ) PARTITION BY RANGE (time);
  
  -- Tạo partition theo tháng
  CREATE TABLE fact_market_klines_2026_01 
    PARTITION OF fact_market_klines 
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
  ```

* **Table fact\_predictions** (Kết quả AI):  
  ```sql
  CREATE TABLE fact_predictions (
    prediction_id SERIAL,
    time TIMESTAMPTZ NOT NULL,
    symbol_id INTEGER REFERENCES dim_symbols(symbol_id),
    model_version VARCHAR(20) NOT NULL,  -- "v1.0.0"
    predicted_close DOUBLE PRECISION,
    confidence_score DOUBLE PRECISION,   -- 0-1
    actual_close DOUBLE PRECISION,       -- Cập nhật sau
    error_pct DOUBLE PRECISION,          -- |predicted - actual| / actual * 100
    PRIMARY KEY (time, symbol_id, model_version)
  ) PARTITION BY RANGE (time);
  ```

* **Table model\_registry** (Quản lý Model Versions):
  ```sql
  CREATE TABLE model_registry (
    model_id SERIAL PRIMARY KEY,
    version VARCHAR(20) NOT NULL UNIQUE,
    file_path VARCHAR(255) NOT NULL,
    training_date TIMESTAMPTZ NOT NULL,
    training_samples INTEGER,
    validation_mse DOUBLE PRECISION,
    validation_mae DOUBLE PRECISION,
    is_production BOOLEAN DEFAULT FALSE,
    metadata JSONB,  -- hyperparameters, features used, etc.
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
  ```

* **Materialized View mv\_prediction\_accuracy** (Dashboard Performance):
  ```sql
  CREATE MATERIALIZED VIEW mv_prediction_accuracy AS
  SELECT 
    date_trunc('day', time) AS date,
    symbol_id,
    model_version,
    COUNT(*) AS prediction_count,
    AVG(ABS(error_pct)) AS mae_pct,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(error_pct)) AS median_error_pct,
    SUM(CASE WHEN error_pct < 1 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS accuracy_1pct
  FROM fact_predictions
  WHERE actual_close IS NOT NULL
  GROUP BY 1, 2, 3;
  
  -- Refresh hàng ngày
  REFRESH MATERIALIZED VIEW mv_prediction_accuracy;
  ```

## **4\. Xử lý Dữ liệu & Tính toán (Spark ETL)**

Đây là bước bạn "xử lý" dữ liệu klines thô thành dữ liệu có ý nghĩa (Feature Engineering).

**Công nghệ:** PySpark (Chạy trên CPU).

### **4.1. Incremental Processing Strategy**

> ⚠️ **VẤN ĐỀ:** Xử lý lại toàn bộ 45GB mỗi lần rất tốn thời gian và tài nguyên.

**Giải pháp: Watermark-based Incremental Processing**

```python
# Đọc watermark từ MongoDB
last_processed = db.processing_watermarks.find_one({"_id": "spark_etl_klines"})
watermark_ts = last_processed["last_processed_timestamp"] if last_processed else 0

# Chỉ đọc dữ liệu mới
new_data = spark.read.format("mongodb") \
    .option("uri", MONGO_URI) \
    .option("collection", "klines_1m") \
    .load() \
    .filter(col("t") > watermark_ts)

# Sau khi xử lý xong, cập nhật watermark
max_timestamp = processed_df.agg(max("t")).collect()[0][0]
db.processing_watermarks.update_one(
    {"_id": "spark_etl_klines"},
    {"$set": {"last_processed_timestamp": max_timestamp, "last_run": datetime.now()}},
    upsert=True
)
```

### **4.2. Các phép tính cụ thể cần thực hiện:**

1. **Cleaning:** Cast kiểu dữ liệu (String \-\> Double), xử lý missing value (fill forward), loại bỏ duplicate theo timestamp.  

2. **Deduplication với Window Function:**
   ```python
   from pyspark.sql.window import Window
   
   # Giữ bản ghi đầu tiên nếu trùng (symbol, timestamp)
   window = Window.partitionBy("s", "t").orderBy("ingested_at")
   deduped_df = df.withColumn("row_num", row_number().over(window)) \
                  .filter(col("row_num") == 1) \
                  .drop("row_num")
   ```

3. **Window Functions (Tính chỉ báo kỹ thuật):**  
   * *RSI (Relative Strength Index):* Tính mức tăng/giảm trung bình trong cửa sổ 14 nến.  
   * *MACD:* Tính EMA(12) \- EMA(26).  
   * *Volatility:* Độ lệch chuẩn (StdDev) của giá đóng cửa trong 20 nến gần nhất.  

4. **Normalization (Bắt buộc cho Deep Learning):**  
   * Dữ liệu giá (ví dụ BTC 60.000$) và RSI (0-100) chênh lệch quá lớn khiến mạng LSTM khó hội tụ.  
   * Sử dụng MinMaxScaler của Spark MLlib để đưa tất cả features về khoảng \[0, 1\].
   * **Lưu ý:** Lưu scaler parameters để inverse transform khi inference!

5. **Gap Filling (Xử lý nến bị thiếu):**
   ```python
   # Tạo full range timestamps
   full_range = spark.range(start_ts, end_ts, step=60000)  # 1 phút = 60000ms
   
   # Left join để phát hiện gaps
   joined = full_range.join(df, full_range.id == df.t, "left")
   
   # Fill forward cho missing values
   filled = joined.withColumn("close", 
       last("close", ignorenulls=True).over(
           Window.partitionBy("s").orderBy("id").rowsBetween(Window.unboundedPreceding, 0)
       )
   )
   ```

### **4.3. Feature Store (Tái sử dụng Features)**

Lưu features đã tính toán để training và inference sử dụng chung:

```
data/
├── features/
│   ├── klines_features_2026_01.parquet
│   ├── klines_features_2026_02.parquet
│   └── ...
├── scalers/
│   ├── minmax_scaler_v1.pkl  # Lưu min/max values
│   └── ...
└── training/
    ├── train_2023_2025.parquet
    └── validation_2025_2026.parquet
```

### **4.4. Load dữ liệu vào PostgreSQL**

Sau khi Spark xử lý xong, ghi trực tiếp vào PostgreSQL qua JDBC:

```python
# Cấu hình JDBC connection
jdbc_url = "jdbc:postgresql://localhost:5432/crypto_dw"
jdbc_properties = {
    "user": "crypto_user",
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver"
}

# Ghi dữ liệu fact_market_klines
processed_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "fact_market_klines") \
    .options(**jdbc_properties) \
    .mode("append") \
    .save()

# Cập nhật watermark sau khi hoàn thành
db.processing_watermarks.update_one(
    {"_id": "spark_etl_klines"},
    {"$set": {
        "last_processed_timestamp": max_timestamp,
        "last_run": datetime.now(),
        "records_written_postgres": processed_df.count()
    }},
    upsert=True
)
```

> 📌 **Lưu ý:** Cần download PostgreSQL JDBC driver và đặt vào Spark jars folder.

## **5\. Module Deep Learning (PyTorch LSTM)**

Sử dụng GPU 4GB VRAM để huấn luyện mô hình.

### **5.1. Cấu hình Input/Output**

* **Input Window:** 60 nến quá khứ (Look-back window).  
* **Features:** \[Open, High, Low, Close, Volume, RSI, MACD\] = 7 features.  
* **Target:** Giá Close của nến tiếp theo (Next Step Prediction).

### **5.2. Kiến trúc Mạng (PyTorch Code Snippet)**

> ⚠️ **LỖI ĐÃ SỬA:** Code gốc thiếu lưu các tham số `num_layers` và `hidden_dim` vào `self`.

```python
import torch
import torch.nn as nn

class CryptoLSTM(nn.Module):
    def __init__(self, input_dim=7, hidden_dim=64, num_layers=2, output_dim=1, dropout=0.2):
        super(CryptoLSTM, self).__init__()
        
        # Lưu các tham số (BẮT BUỘC để sử dụng trong forward)
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.output_dim = output_dim
        
        # LSTM Layer: batch_first=True -> (batch, seq, feature)
        self.lstm = nn.LSTM(
            input_dim, 
            hidden_dim, 
            num_layers,
            batch_first=True, 
            dropout=dropout if num_layers > 1 else 0  # Dropout chỉ áp dụng khi > 1 layer
        )
        
        # Batch Normalization để ổn định training
        self.batch_norm = nn.BatchNorm1d(hidden_dim)
        
        # Fully Connected Layer
        self.fc = nn.Linear(hidden_dim, output_dim)

    def forward(self, x):
        # x shape: (Batch_Size, 60, 7)
        batch_size = x.size(0)
        
        # Initialize hidden & cell states (move to GPU)
        h0 = torch.zeros(self.num_layers, batch_size, self.hidden_dim).to(x.device)
        c0 = torch.zeros(self.num_layers, batch_size, self.hidden_dim).to(x.device)
        
        # LSTM forward pass
        out, (hn, cn) = self.lstm(x, (h0, c0))
        
        # Chỉ lấy output của nến cuối cùng để dự báo
        last_output = out[:, -1, :]  # Shape: (Batch_Size, hidden_dim)
        
        # Batch normalization
        normalized = self.batch_norm(last_output)
        
        # Final prediction
        prediction = self.fc(normalized)
        
        return prediction

    def predict_with_confidence(self, x, mc_samples=10):
        """Monte Carlo Dropout để ước lượng uncertainty"""
        self.train()  # Enable dropout
        predictions = []
        
        with torch.no_grad():
            for _ in range(mc_samples):
                pred = self.forward(x)
                predictions.append(pred)
        
        predictions = torch.stack(predictions)
        mean_pred = predictions.mean(dim=0)
        std_pred = predictions.std(dim=0)
        
        # Confidence score: 1 - normalized std
        confidence = 1 - (std_pred / mean_pred.abs().clamp(min=1e-8))
        
        self.eval()  # Reset về eval mode sau khi MC Dropout
        return mean_pred, confidence.clamp(0, 1)
```

### **5.3. Training Pipeline với Early Stopping**

```python
class EarlyStopping:
    def __init__(self, patience=5, min_delta=0.001):
        self.patience = patience
        self.min_delta = min_delta
        self.counter = 0
        self.best_loss = None
        self.should_stop = False
    
    def __call__(self, val_loss):
        if self.best_loss is None:
            self.best_loss = val_loss
        elif val_loss > self.best_loss - self.min_delta:
            self.counter += 1
            if self.counter >= self.patience:
                self.should_stop = True
        else:
            self.best_loss = val_loss
            self.counter = 0
        return self.should_stop

# Training loop với checkpointing
def train_model(model, train_loader, val_loader, epochs=50, lr=0.001):
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=3)
    criterion = nn.MSELoss()
    early_stopping = EarlyStopping(patience=5)
    
    best_val_loss = float('inf')
    
    for epoch in range(epochs):
        # Training
        model.train()
        train_loss = 0
        for batch_x, batch_y in train_loader:
            batch_x, batch_y = batch_x.cuda(), batch_y.cuda()
            
            optimizer.zero_grad()
            outputs = model(batch_x)
            loss = criterion(outputs, batch_y)
            loss.backward()
            
            # Gradient clipping để tránh exploding gradients
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            
            optimizer.step()
            train_loss += loss.item()
        
        # Validation
        model.eval()
        val_loss = 0
        with torch.no_grad():
            for batch_x, batch_y in val_loader:
                batch_x, batch_y = batch_x.cuda(), batch_y.cuda()
                outputs = model(batch_x)
                val_loss += criterion(outputs, batch_y).item()
        
        val_loss /= len(val_loader)
        scheduler.step(val_loss)
        
        # Save best model
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            torch.save({
                'epoch': epoch,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'val_loss': val_loss,
            }, f'models/best_model.pth')
        
        # Early stopping check
        if early_stopping(val_loss):
            print(f"Early stopping at epoch {epoch}")
            break
    
    return best_val_loss
```

### **5.4. Model Versioning với MLflow**

> 📌 **Thay đổi:** Sử dụng **MLflow** thay vì quản lý thủ công để tracking experiments, model registry, và deployment.

**Lý do chọn MLflow:**
- Lightweight, chạy được trên local (không cần server riêng)
- Tích hợp tốt với PyTorch
- UI để so sánh các experiment runs
- Model Registry với staging/production stages

**Cấu trúc MLflow:**
```
mlruns/                    # MLflow tracking directory
├── 0/                     # Default experiment
│   ├── run_id_1/
│   │   ├── metrics/       # val_loss, val_mae, etc.
│   │   ├── params/        # hyperparameters
│   │   ├── artifacts/     # model.pth, scaler.pkl
│   │   └── tags/
│   └── run_id_2/
└── models/                # Model Registry
    └── CryptoLSTM/
        ├── version_1/     # Stage: Production
        └── version_2/     # Stage: Staging
```

**Tích hợp vào Training Pipeline:**
```python
import mlflow
import mlflow.pytorch

# Khởi tạo experiment
mlflow.set_experiment("crypto_lstm_training")

with mlflow.start_run(run_name=f"train_{datetime.now().strftime('%Y%m%d_%H%M')}"):
    # Log parameters
    mlflow.log_params({
        "hidden_dim": 64,
        "num_layers": 2,
        "learning_rate": 0.001,
        "batch_size": 32,
        "lookback_window": 60
    })
    
    # Training loop...
    for epoch in range(epochs):
        train_loss = train_one_epoch()
        val_loss = validate()
        
        # Log metrics mỗi epoch
        mlflow.log_metrics({"train_loss": train_loss, "val_loss": val_loss}, step=epoch)
    
    # Log model và artifacts
    mlflow.pytorch.log_model(model, "model")
    mlflow.log_artifact("scaler_params.pkl")
    
    # Register model nếu performance tốt
    if val_loss < best_threshold:
        mlflow.register_model(f"runs:/{mlflow.active_run().info.run_id}/model", "CryptoLSTM")
```

**Chạy MLflow UI:**
```bash
mlflow ui --host 0.0.0.0 --port 5000
# Truy cập: http://localhost:5000
```

### **5.5. Inference API (FastAPI) + MLflow Integration**

Triển khai API để serving predictions real-time, tích hợp với MLflow Model Registry:

```python
# inference_api.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import torch
import numpy as np
import pickle
import mlflow
from datetime import datetime

app = FastAPI(
    title="Crypto Price Prediction API",
    description="Real-time cryptocurrency price prediction using LSTM",
    version="1.0.0"
)

# Global model state
model = None
scaler = None
model_version = None

@app.on_event("startup")
def load_model():
    """Load production model từ MLflow Registry"""
    global model, scaler, model_version
    
    # Load model từ MLflow Registry (Production stage)
    model_uri = "models:/CryptoLSTM/Production"
    model = mlflow.pytorch.load_model(model_uri)
    model.eval()
    model.cuda()
    
    # Load scaler từ artifact
    client = mlflow.tracking.MlflowClient()
    latest_version = client.get_latest_versions("CryptoLSTM", stages=["Production"])[0]
    model_version = latest_version.version
    
    artifact_path = client.download_artifacts(latest_version.run_id, "scaler_params.pkl")
    with open(artifact_path, "rb") as f:
        scaler = pickle.load(f)

class PredictionRequest(BaseModel):
    symbol: str
    features: list[list[float]]  # Shape: (60, 7) - 60 nến, 7 features

class PredictionResponse(BaseModel):
    symbol: str
    predicted_close: float
    confidence: float

@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    try:
        # Chuẩn hóa input
        features = np.array(request.features)
        normalized = scaler.transform(features)
        
        # Convert to tensor
        x = torch.FloatTensor(normalized).unsqueeze(0).cuda()
        
        # Inference
        with torch.no_grad():
            pred, conf = model.predict_with_confidence(x)
        
        # Inverse transform để lấy giá thực
        predicted_close = scaler.inverse_transform_close(pred.cpu().item())
        
        return PredictionResponse(
            symbol=request.symbol,
            predicted_close=predicted_close,
            confidence=conf.cpu().item()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "healthy", "model_loaded": model is not None}
```

**Chạy API:**
```bash
uvicorn inference_api:app --host 0.0.0.0 --port 8000
```

### **5.6. Job Scheduling (Linux Cron)**

Sử dụng Cron + Systemd thay vì Airflow để tiết kiệm RAM:

**Tổng quan các Scheduled Jobs:**

| Job Name | Schedule | Script | Mô tả |
|----------|----------|--------|-------|
| `CryptoIngestion` | Liên tục (Service) | `realtime_ingestion.py` | WebSocket streaming |
| `CryptoETL` | Daily 2:00 AM | `spark_etl.py` | Incremental ETL |
| `CryptoInference` | Every 5 min | `run_inference.py` | Batch predictions |
| `CryptoRetrain` | Weekly Sun 3:00 AM | `train_model.py` | Model retraining |
| `CryptoBackfill` | On-demand | `backfill_gaps.py` | Fill missing data |
| `CryptoCleanup` | Daily 4:00 AM | `cleanup_old_data.py` | Xóa data cũ |

```bash
# Crontab configuration - Mở bằng: crontab -e
# ┌───────────── minute (0-59)
# │ ┌───────────── hour (0-23)
# │ │ ┌───────────── day of month (1-31)
# │ │ │ ┌───────────── month (1-12)
# │ │ │ │ ┌───────────── day of week (0-6, Sunday=0)
# │ │ │ │ │
# * * * * * command

# 1. Daily ETL - Chạy 2AM mỗi ngày
0 2 * * * /home/user/crypto/scripts/run_etl.sh >> /home/user/crypto/logs/cron.log 2>&1

# 2. Inference Job - Chạy mỗi 5 phút
*/5 * * * * /home/user/crypto/scripts/run_inference.sh >> /home/user/crypto/logs/cron.log 2>&1

# 3. Weekly Model Retrain - Chạy 3AM Chủ Nhật
0 3 * * 0 /home/user/crypto/scripts/run_train.sh >> /home/user/crypto/logs/cron.log 2>&1

# 4. Cleanup Old Data - Chạy 4AM mỗi ngày
0 4 * * * /home/user/crypto/scripts/run_cleanup.sh >> /home/user/crypto/logs/cron.log 2>&1

# 5. Realtime Ingestion - Chạy như Systemd service (xem bên dưới)
```

**Systemd Service cho Realtime Ingestion:**

```bash
# /etc/systemd/system/crypto-ingestion.service
[Unit]
Description=Crypto Realtime Ingestion Service
After=network.target docker.service
Wants=docker.service

[Service]
Type=simple
User=user
WorkingDirectory=/home/user/crypto
Environment=PYTHONPATH=/home/user/crypto
ExecStart=/home/user/crypto/venv/bin/python scripts/realtime_ingestion.py
Restart=always
RestartSec=10
StandardOutput=append:/home/user/crypto/logs/ingestion.log
StandardError=append:/home/user/crypto/logs/ingestion.log

[Install]
WantedBy=multi-user.target
```

```bash
# Enable và start service
sudo systemctl daemon-reload
sudo systemctl enable crypto-ingestion
sudo systemctl start crypto-ingestion

# Xem status
sudo systemctl status crypto-ingestion
journalctl -u crypto-ingestion -f
```

**Bash wrapper scripts:**

```bash
#!/bin/bash
# scripts/run_etl.sh
set -e  # Exit on error

export PYTHONPATH=/home/user/crypto
LOG_FILE="/home/user/crypto/logs/etl_$(date +%Y%m%d).log"

cd /home/user/crypto
source venv/bin/activate

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting ETL..." >> "$LOG_FILE"

if python scripts/spark_etl.py 2>&1 | tee -a "$LOG_FILE"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ETL completed successfully" >> "$LOG_FILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ETL FAILED" >> "$LOG_FILE"
    # Gửi alert (curl webhook hoặc mail)
    # curl -X POST "https://hooks.slack.com/..." -d '{"text":"ETL Failed!"}'
    exit 1
fi
```

```bash
#!/bin/bash
# scripts/run_inference.sh
export PYTHONPATH=/home/user/crypto
LOG_FILE="/home/user/crypto/logs/inference_$(date +%Y%m%d).log"

cd /home/user/crypto
source venv/bin/activate
python scripts/run_inference.py 2>&1 | tee -a "$LOG_FILE"
```

```bash
#!/bin/bash
# scripts/run_train.sh
export PYTHONPATH=/home/user/crypto
LOG_FILE="/home/user/crypto/logs/train_$(date +%Y%m%d).log"

cd /home/user/crypto
source venv/bin/activate

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting model training..." >> "$LOG_FILE"
python scripts/train_model.py 2>&1 | tee -a "$LOG_FILE"

# Sau khi train xong, promote model mới lên Production nếu tốt hơn
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Evaluating model promotion..." >> "$LOG_FILE"
python scripts/promote_model.py 2>&1 | tee -a "$LOG_FILE"
```

```bash
# Cấp quyền execute cho tất cả scripts
chmod +x /home/user/crypto/scripts/*.sh
```

### **5.7. Inference Pipeline (Batch Predictions)**

> 📌 **Script cần triển khai:** `scripts/run_inference.py`
> - Chạy mỗi 5 phút qua Cron
> - Lấy 60 nến mới nhất từ PostgreSQL
> - Gọi model để predict
> - Lưu prediction vào `fact_predictions`
> - Cập nhật `actual_close` cho predictions cũ

```python
# scripts/run_inference.py (pseudo-code)
def run_batch_inference():
    """Chạy inference cho tất cả 50 symbols"""
    
    # 1. Load production model từ MLflow
    model = mlflow.pytorch.load_model("models:/CryptoLSTM/Production")
    
    # 2. Lấy 60 nến mới nhất cho mỗi symbol
    for symbol in SYMBOLS:
        features = get_latest_features(symbol, window=60)
        
        # 3. Predict
        prediction, confidence = model.predict_with_confidence(features)
        
        # 4. Lưu vào PostgreSQL
        save_prediction(symbol, prediction, confidence)
    
    # 5. Cập nhật actual_close cho predictions 1 phút trước
    update_actual_close()
    
    # 6. Log metrics
    log_inference_metrics()
```

## **6\. Cấu hình Hạ tầng (Infrastructure)**

Giải pháp triển khai trên Host 16GB RAM \+ GPU 4GB.

**Docker Compose Services:**

1. **MongoDB:** Limit 2GB RAM.  
2. **PostgreSQL:** Limit 4GB RAM (Cần RAM cho Indexing và TimescaleDB chunking).  
3. **Grafana:** Limit 1GB RAM.  
4. **Môi trường Python (Spark & PyTorch):** Chạy trực tiếp trên Host (Native) để:  
   * Spark dùng RAM hệ thống (dư khoảng 8GB).  
   * PyTorch truy cập trực tiếp CUDA Driver của GPU (Docker GPU setup trên Windows khá phức tạp, chạy native ổn định hơn).

### **6.1. Docker Compose Configuration**

```yaml
version: '3.8'

services:
  mongodb:
    image: mongo:6.0
    container_name: crypto_mongodb
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db
      - ./mongo-init.js:/docker-entrypoint-initdb.d/init.js
    environment:
      MONGO_INITDB_DATABASE: binance_raw
    deploy:
      resources:
        limits:
          memory: 2G
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 10s
      timeout: 5s
      retries: 5

  postgres:
    image: timescale/timescaledb:latest-pg15
    container_name: crypto_postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    environment:
      POSTGRES_DB: crypto_dw
      POSTGRES_USER: crypto_user
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    deploy:
      resources:
        limits:
          memory: 4G
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U crypto_user -d crypto_dw"]
      interval: 10s
      timeout: 5s
      retries: 5

  grafana:
    image: grafana/grafana:latest
    container_name: crypto_grafana
    ports:
      - "3000:3000"
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD}
    deploy:
      resources:
        limits:
          memory: 1G
    depends_on:
      postgres:
        condition: service_healthy

  # MLflow Tracking Server
  mlflow:
    image: python:3.10-slim
    container_name: crypto_mlflow
    ports:
      - "5000:5000"
    volumes:
      - mlflow_data:/mlflow
      - ./mlruns:/mlflow/mlruns
    command: >
      bash -c "pip install mlflow psycopg2-binary boto3 &&
               mlflow server 
               --backend-store-uri sqlite:///mlflow/mlflow.db
               --default-artifact-root /mlflow/mlruns
               --host 0.0.0.0
               --port 5000"
    deploy:
      resources:
        limits:
          memory: 512M
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  # FastAPI Inference Server (Optional - có thể chạy native)
  # inference-api:
  #   build: ./inference
  #   ports:
  #     - "8000:8000"
  #   environment:
  #     MLFLOW_TRACKING_URI: http://mlflow:5000
  #   depends_on:
  #     - mlflow
  #     - postgres

volumes:
  mongodb_data:
  postgres_data:
  mlflow_data:
  grafana_data:
```

### **6.2. Monitoring & Alerting**

> ⚠️ **QUAN TRỌNG:** Cần giám sát hệ thống để phát hiện lỗi sớm.

**Metrics cần theo dõi:**

| Component | Metric | Alert Threshold |
|-----------|--------|-----------------|
| Ingestion | `data_freshness_seconds` | > 120s |
| Ingestion | `websocket_disconnects_1h` | > 5 |
| MongoDB | `disk_usage_percent` | > 80% |
| Spark | `etl_job_duration_minutes` | > 30 |
| Model | `inference_latency_ms` | > 1000 |
| Model | `prediction_error_mae_24h` | > 5% |
| PostgreSQL | `connection_pool_usage` | > 90% |

**Grafana Dashboard Panels:**

1. **System Health:** CPU, RAM, Disk usage
2. **Data Pipeline:** Ingestion rate, ETL lag, data freshness
3. **Model Performance:** Prediction vs Actual, MAE over time
4. **Alerts:** Active alerts và incident history

## **7\. Error Handling & Recovery**

### **7.1. Ingestion Layer**

```python
class ResilientWebSocketClient:
    def __init__(self, symbols, max_retries=10):
        self.symbols = symbols
        self.max_retries = max_retries
        self.retry_count = 0
        self.backoff_base = 1  # seconds
    
    async def connect_with_retry(self):
        while self.retry_count < self.max_retries:
            try:
                await self._connect()
                self.retry_count = 0  # Reset on success
            except WebSocketException as e:
                self.retry_count += 1
                wait_time = self.backoff_base * (2 ** self.retry_count) + random.uniform(0, 1)
                logging.warning(f"WebSocket error: {e}. Retry {self.retry_count}/{self.max_retries} in {wait_time:.1f}s")
                
                # Fallback to REST API while waiting
                await self._fallback_rest_api()
                await asyncio.sleep(wait_time)
        
        raise MaxRetriesExceeded("WebSocket connection failed after max retries")
    
    async def _fallback_rest_api(self):
        """Sử dụng REST API khi WebSocket fail"""
        for symbol in self.symbols:
            try:
                data = await self._fetch_kline_rest(symbol)
                await self._save_to_mongo(data, source="rest_api")
            except RateLimitError:
                await asyncio.sleep(60)  # Wait 1 minute
```

### **7.2. Spark ETL Recovery**

```python
def run_etl_with_recovery():
    """ETL job với checkpoint và recovery"""
    try:
        # Đọc checkpoint
        checkpoint = load_checkpoint()
        
        # Xử lý incremental
        process_from_checkpoint(checkpoint)
        
        # Lưu checkpoint mới
        save_checkpoint(new_watermark)
        
    except SparkException as e:
        logging.error(f"Spark ETL failed: {e}")
        
        # Gửi alert
        send_alert("Spark ETL Failed", str(e))
        
        # Không update checkpoint -> sẽ retry từ điểm trước
        raise
```

### **7.3. Circuit Breaker Pattern**

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.last_failure_time = None
    
    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
            else:
                raise CircuitBreakerOpen("Circuit breaker is open")
        
        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
            raise
```

## **8\. Lộ trình Thực hiện (Roadmap)**

* **Tuần 1: Data Ingestion & Storage**  
  * ~~Viết script crawl dữ liệu Binance 3 năm.~~ → **Download batch từ Binance Data Vision**
  * Viết WebSocket client cho real-time data với auto-reconnect
  * Implement rate limiter và retry logic
  * Đẩy dữ liệu vào MongoDB với proper indexes.  
  * Kiểm tra tính toàn vẹn dữ liệu (không bị ngắt quãng thời gian).
  * **Deliverable:** Script download historical + streaming real-time hoạt động ổn định

* **Tuần 2: Spark ETL & Feature Engineering**  
  * Viết Spark Job đọc Mongo với **incremental processing**.
  * Implement deduplication và gap detection
  * Thực hiện tính toán RSI, MACD (như mục 4).
  * Lưu scaler parameters cho inference
  * Xuất ra file training\_data.parquet.
  * **Deliverable:** ETL pipeline chạy định kỳ, Feature Store structure

* **Tuần 3: Deep Learning Training (GPU)**  
  * Cài đặt PyTorch với CUDA.  
  * Xây dựng Dataset class load file Parquet.  
  * Implement training loop với Early Stopping và checkpointing
  * Train model LSTM qua 20-30 epochs.  
  * Implement model versioning
  * Lưu model (model.pth) với metadata.
  * **Deliverable:** Trained model v1.0.0 với validation metrics

* **Tuần 4: System Integration & Dashboard**  
  * Viết script Inference: Lấy dữ liệu mới \-\> Spark xử lý \-\> PyTorch dự báo \-\> Lưu vào Postgres.
  * Implement error handling và circuit breaker
  * Cấu hình Grafana vẽ biểu đồ so sánh Thực tế vs Dự báo.
  * Setup monitoring và alerting
  * Viết báo cáo tổng kết.
  * **Deliverable:** End-to-end pipeline running, Dashboard live

## **9\. Checklist Trước Khi Chạy Production**

- [ ] **Data Ingestion**
  - [ ] Historical data downloaded và verified (no gaps)
  - [ ] WebSocket client tested với disconnect/reconnect
  - [ ] Rate limiter configured đúng (1200 req/min)
  - [ ] Fallback REST API working

- [ ] **Data Quality**
  - [ ] Deduplication logic tested
  - [ ] Gap detection và backfill working
  - [ ] Outlier detection configured

- [ ] **Storage**
  - [ ] MongoDB indexes created
  - [ ] MongoDB TTL index cho depth_snapshots
  - [ ] PostgreSQL partitions created cho 6 tháng tới
  - [ ] Backup strategy implemented

- [ ] **Processing**
  - [ ] Spark incremental processing tested
  - [ ] Watermark/checkpoint mechanism working
  - [ ] Scaler parameters saved

- [ ] **Model**
  - [ ] Model trained với đủ data
  - [ ] Validation metrics acceptable (MAE < 2%)
  - [ ] Model versioning structure created
  - [ ] Inference pipeline tested

- [ ] **Monitoring**
  - [ ] Grafana dashboards configured
  - [ ] Alerts set up cho critical metrics
  - [ ] Logging đầy đủ ở tất cả layers

- [ ] **Security**
  - [ ] Environment variables cho passwords
  - [ ] MongoDB/PostgreSQL không expose public
  - [ ] API keys secured

## **10\. Các Vấn đề Tiềm ẩn & Giải pháp**

| Vấn đề | Nguyên nhân | Giải pháp |
|--------|-------------|-----------|
| Model accuracy giảm theo thời gian | Data drift, thị trường thay đổi | Retrain định kỳ (hàng tuần), monitor prediction error |
| MongoDB disk full | Depth snapshots tích lũy | TTL index tự động xóa sau 30 ngày |
| Spark OOM | Xử lý quá nhiều data | Incremental processing, tăng partition |
| WebSocket disconnect thường xuyên | Network instability | Auto-reconnect, multiple connection pools |
| Prediction latency cao | Model quá lớn | Quantization, ONNX optimization |
| PostgreSQL slow queries | Missing indexes, partition không đúng | EXPLAIN ANALYZE, optimize partition key |

[image1]: <data:image/png;base64,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>

[image2]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEoAAAAXCAYAAACswNlYAAAFk0lEQVR4XrVYXahVRRTem2tgZF7Nrrd7zzkzZ59z4lIvQteKoCAuIYUYUSGFEJL0A0FRkpYQET2EPUhBRPRDSEQREvbgQxkh+ZDYgy8GoU+JFhkoRYmCP31r1sze87dn7+vVD5b7zFrf+pk1s2f2NctaI/cVcTi0lE/KNn+40cyoLoev98cRtKA0oF2EdixChBlRBajjkL7OZqEFJXNY7RwuB1cpcixsTBdXKjiWlrQomhntEItT6mJGhVpDidnZ2Wt63d5qIcSjvV5vNY2Nvij6MyUxb4gWM8Z0VwljmMD6PhAmzTMp5RTkNciHoLzR7XZHPqsO4F5LPvA9CzkkEAPPTyAH0LAHYXsFuT81fIyfhe1fyCUpBYugsTgO3QVt+2i60+naeVTdqW0XzCuJik0TQIHrUNi7SHwC8h/GsxZZoSiKO2Dbi0ndg9+rMNE9PAm5GeGC9LYCOW7AJH8A9y/InGVS+TF5atpF1LDTtlEQ6LepPKjR6Ai0SKjzCGynqTbLy04e1FULZob8vEqIQuUDKPJeJH4dRXOjLBfdzN2wPYnhGOk6nc4K+ByMN7Za1tvwStFOAe8cNdlnEYbD4UrYDweNAuC7lRpFi+nPAvVs4N0mv8BwkWdmTExMLMG2W5EFXXCHSL7YUZTIfaopSk3cmOgp+ZX7DfIPVm+V4ZerLeVLRucD+e+H/SJkT20tOedualRgk2Kdyi/EPuqHYxwMBuNw+lIXSHISCR7P9ErbmJmZuR725/yVqIPdKFtPBy29mtB/C/uUx6dCtxqdv2p6N1GdVTMjBWFn3AfOx2ZsKG6jKkdaZujf5NiCXn8HOQreDqen1G2Rq1UtIF9DDkB/q01GgEeg38aetqWCfbzUNaoGi8DdBblAr61vJCDOcth/NhN1S3AmrRZjMByMV3bWW416QvLOnsJr30XOV/H7b8jL5uYsIYRcDsKLOoaFPMfhOgeno3geknyj7KPmUVCXS3RfkSldslGeD+q4k7h0ELuFVkQ9MXpl3VfHjqV/x0oimEYh33d6Xkb2w7ab6sh8d33F9h2lAvOoYMkNo7NjLuh0BOzJ/yYbZWEwGI6D9z2a9Nnk5OR1vt0gvaPQ7KK/TDezFDrYM+sYoZqE32gGvdZbJN+W2zP3MOdU+oONzil63V4YjW5eapFK0OEJzlpfX4c2jaLmw/4BeDto4Yzeb4KGOqNooqjlGX/7IMYm2hWy/F6SZyDvg7vMBEg0Kpuenr4R/F8g5yB3lQaKTx9ncDoCeR4BH4PsBOnPvnWgqzpyCtTpSXNGGX0CTY0yTSpwPmQ6F3bVLahpTRXbzdKvbr1dmVp1v4qc8qrbi+aiVSV0TdFG0U0n+YiBXVaHPW1zfLi9Td8wtgOdQwj0DRId7OJbBY5T9M2C534a29yw0ApclP6OKlHy6ZbZ3Kcz0roBaKdA/7AZl9AMfWN+Ravuf0cp5Cpv2agqG4dINUryRXZc+DuqKIpJODxtcW2MgbwW8pPkFTws+StYp2yGLuoMJnS7UljbBJPYSDZVmJDHWMQxjE/heXfJNA7Wb/jeBPlRRL7MM14Adc2XO8qCXrzgMhiNRkvh8zk2ziU838viH5xNE2+yV6BdKvi8O0XFGsGkfsdzB3GkdXt5HHxDyT/gPzDx6jLrPO/A77ykv/X4IN4E2Yvfb6FJD5Hd8Pv8t95JLyctilocPf4V4/WZfeQ0o4kW2n0NjVkXs9RAmTx7go4GLC7wt5ng/z1Y4x8jC0cer0nB0sXMUZhgjkPVKlcXQ51eo8Ec5I1Nzo+R+woLxpSgMCKEiIrRUM+CcaUCRuNElSFa0q4wzIonkLAnTK1Q+i80kIs2ezBl8+BT9Tgo3udFEKPMwz0EnMzbeFn+PmJBoq97TFeHBDc9+VAbauaD0Pt/PQtkMj7vm/EAAAAASUVORK5CYII=>

[image3]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAloAAAAUCAYAAABVlpPfAAAcJ0lEQVR4Xu1dC8xlVXW+f4a29GELFTowj7POnaGlDFqh2FLFVmJApbZNK7QQIWhKFERaC3RQEHlIifIUGRCKIyANQXnWwPCcwCiNIBgoBkozMEHMABEipIQhgZb5u7691jp3nXX3Off+j/s/xv9Ldv5z9mPttddee+21H+f+nU4rxmJEQj52pjCK2kdBcwFzBe29OzYgfe5gvvA5fZjmvplWYrOPyTQnVyYXt4CJo0+OfREBIX1Q9gUsYB5ifqr1/OS6h/nO/8gxGwKa8wZ/SI6GzLatYLTNHS31OYFfgCZ6zNnmzhXG5gofC1jAXMSqVat+eZ999vmlGL+tYcEOTCOmVZjTSmwB04VZ65ZpqniayAyLGa5uDmCGW1wyiOhjS5cuXcaP23e73XcVRfEpPPt8nOdcDlcg77Jly37Vp00nVqxY8Vtcxxrm4bSYNleBLoMcYzwAWUFmkB236QCOWhTzzBdwGz7DYRwhps0GmI8uh7XD6KPXq+CYjXH8ccuXL/8rfXVJk8HEy/OY292NrxM57BrzOIyxqv1TjJwIuK0f5Dpu43r/wMfDBnD8hp133vk3fPyosdtuu/0m98tn0X5u25ncV7/XyQgyyOngjo4lb8MQZ3Ys2rDpxsqVK3+H+d7Ida+HfsX0uQDIdpBcJwpu818yzcPd+/uY/qd9nrkM6AvjsBjP7TiSx8Y7VG8WQZ90Lixj3giMWQ43cfjz9Fyk9xTiePL6jmef1pHxvS+nXQw5T3QR6eabczFeYjqANNSNfDnbqeWRnqcxZQ2aGWB8ksxXz6I/Yno/RtgwZuK9HN5QhhC2cPhIyHMwd/5eUBh+PovDPaMyLKxc+ygPz8a02UGz8JnHPTgcy+FeDm/FdJ3cb+Y2fZIH8Erk4XDHqGQ3E+CB+bvchhdj/ETBNA6CTsX4iQDGErKN8Tl4vaJ+RwbO1pX4G6Lrr9MJJc31HszhJjgGrCN/ys+PcnjTZalBjfCWGN9DrlQdTONbTGOcZfK5mMZxP4SsYnwjBlc3EMzLBuZpf653R/57NNrP4cSOUsdkw++ncngYvHFYwc+3ct5jtHy0YQgtMpo+MC8HcF2vcDirMy3SmD5Ap5ivDU1ynSyYRpfp3Y1ntge/TWLjhteZEWFQo8jZaoyBmA7dd/pj4fxhnB1nX2L5ccje8vH7Bzg8xfmPJHHEbnbODuzQag6Xct/xHzqPw+3DzBdou84393A4C7aV//4XyYKkAt6R1jSXK42zkN5EYz6B23AO5W3+hMB0tmcaR/HfHWLaUFAFgTD/m4mcGRlasmTJco57yuXfkd8f5nCcz1fDII1vAZQakyfz8tcxba6BZbAH+CQx9Jt9GkQAGXG4xlYtJIqdneBmGpPtIugHRSd4EsRIV2wxflio4n8Dk25MyyHoVR/HPHOcHHV/1GBjupjrvB+TdUd5YqO7G8c9j3EXspsRxEQxpR1F7GwwjS+yLHaJaezsvZvT/j7GC/rENi3g+t6yHUVnX37OYQ/EMZ/H8PNT/LfU/AdBBqVOlt6GcfjPnB0bJcAn6qU5NiExPxdDtnjOyXUq0D7YrPSO7YxKOaYR3lab7nhw/DoOj3PYxPK6mvPs2xmyXbBlXO5HJDtBPmwwR4bz7MnvzzHdQ/Gu479yArCbxs+37L777m/Du+5O3V5mdt9ywLxCshjZEe/8fDiHJ1EP3oeZy0GjICkPRBrzDSqTlh2tlu51SSy7nUj0o4FOBp60Gqk1LqoGFbRfHcLrvpZm4YihjhYB1dCfb/Hixb/epjis8Es6Ezjig/DRmTEeg5lkVXMC3pnuH/Hz6xzWg4eYfxD6WzJZTJ5SU1vzyNezdOnSt7NsHpqKo6VG6VJ+3C6mJeSrniT6iI3ppN+XAKB9w/Svjj2sgjc5fbTxdVAts8SfQLLK7Y3HLAczBxjvphU/ZABZxPgIHSNH4RmTDD/fz+FVlsm7bHLgcLHlV4fzVJ0IB9qwmQDamjkGakRbtw2wT2PD2ieWyQWQLZ6jXGPeyYBp7VqO+HhW5bQIx0AD6oIdyNsCB/DM4dmco6X2eriJNHQglz1eF0wVVE9v0Nft+HkthycwaWscxjQWNYlvddYe8WNGeUrzRxvMafLt0vnmNVvE0IC53Gi49D4a8w2VozVsvzZATxuemDSdQUaKZFVU24bXzn+haNlN0O1+bJOejVV6SF6k9Z5QN8RjaUDpJJomGq5rBzYMBCVkOksx2Eo5ZoiT9BhW6px2NIePc/n3hvQKbBB35jzXY/Ue0zqifGv85NFmFAEIH50Z40tZQT3BvB6IdzexNjqpmFjAP9rB7X0nl3kf8uK+wHK5W7MHeAPvoB93PiBPzvM3nHYml/1bn5aRZZJjbjJUPioeOmoMrK3ggWn9WY4HADRRP+c9Be3uuImB4y4k2dlL2+dafxJz6EPsFGT7keOPLcNKr1TdMJ7sLgS/n+z1Km3V9zoV/Y3ju3GSI6DVhRy1fIefL+n0Ge9UEEbzFM5zfD0t8QWe10HOMS1CDfFNXN9VXh+Y7reifpdyZHg+h/fQ4GOxsbLslpAD+ojl8Cta/sKkS0t6uuQLcZ5dSI63sVv0feZhFf99P78/022YnDnPERwuivFK6xYu98cxLUInnqQf2kfohzRGmM5h4IfD4aXsYvbdedFx1WjDIlB+mLHUSXJMqMmR69vPMug4OayUXTTvHI/pWKzu6oB/HxfbYWixT+DnE2hrk3PrgTw2qUe5xrxNNiYjk0QX8uB8x8Z7X5q2mPncH88mP28DdKG1P+Rh5ZoAmXL4H9JjOL2H1wemf4DW0QqV/bPlVB2tAC73hzhGdVHoKxxbpV1Okvuk2AG8EXoEexTtLuwsp/8vl3tI7WBZyHHm3j5fDiR25+e+XTouMN+crXla53Kj4dMjjQyqeVfLJ3td9s812+M5thnQMZTGj1+gDqNLLh/08UTQgK1Ce5BWOEdL60ljOTN+Kp+E/x4RnN2SZOfvOdAGLUsDv6gTdYMuHFNLq0GJ38LhGn7+KYfT/QW5Ujoi1zlbmhSbRODYyr9C7/S8tVw9Yi67C5e7i8NX0BGMx/j5yI4o5g78/k2SuwTJceG440kMxDiHsznvVaUY34u7PSOOyRKrt3/BQET94FHTskA+pnWnowEkQ5bphFZQg6MVQbKiQDsuiGkA8/NJTrsOclHZfJ3DfVAO/vuclsV29L8prS+Q65tCtqZfKmWA7wq58t+PWrrK8megw88PgQbn+TJokGwfJ4Pp+SDlwYwz6JIobo0Hqh+boD+wutpfaWAX5h70fSnO0N0kPOAv2rMa8Vqu6kPm47u5foSDAj3ohlU/x23U9oAvDAwcAXyKw0/J6RX1BsoYp53E70/y8yL+exSJo4GVJ3aPHnOrzxqgI5x+XkdlBoBnGtLJakKhq8quu4SqDhkuNMPwJsPny0SQOGSPUK9/X+NwLhe/nt//guSIrXaErf30JNoF+XPe75OMu49yeLR090wC0GfH+TGDfiYqhnKyPPTi9rXor1Lv75FMDuMkx+7QOXyQgePBc6xOlUmyYRyeRnkOp9ep91DKeALNvrFEqsfav31yLESOSSc57kCOewOyEZl1TzbnCPqsNG3c4sgIzu51+v5KqUdIOWTsU7JNNKST5ZGTa4TKJGdjamNbj7NwPw470pD7I/z3MuOJ46Gj3+P4B4uCLieZuGCHNnFYW8gl8EtKGfuP9TlOblVbiD27gUMX79InxWaOP8nPUWoPrrZ8baB2RwuLKywCcXz4XCH2p4z5hgHmOy5/udNR3OXbyuE2JvmvkAuH63z7Vecg+3ESW4UdyMp+t0H7YotvV4zDX7z3SvXikNfy+/RII6A276q8rK6+eZtkLL/s7UKhY0htDq5vbHZjCA7OfRxe4vADknua0MlHOTxssuPneyFvksXMhwsZ/2mhWoijhaNizIm4u3YYFYnWeuOhDD4Jv+/P6Y91ZMwBX+P3Z0jugWIH8AqtF/b+Pzj9T1A3lz8SdRvdGlTAP8Suk640MAFfgU7XC3MbqKVzfDwApS9lNXynDQYSBTvKDdLrTQGXy5bcKxC40eD3U8g5LvAS+f11T1Mn43ScgAmXnx/x3iQ1ODMewZhVhizmGwQI2fPbBJXLxtxErJ7xenIDC5M8578Mz9perIiq83I7Dih1ZUiy2wFlWKskMBB+ghWFvlcDh8P5Lg8msbfQB5EP4yE6WsLDmPBQJIOwzvgAHfSr0k8GBH3OeW5F/xWyRY6JvrZrA5qhD6HIff1I0s5zO84sq27d2OntvMFBgNP0AbvzQKpXaIO+w7uATO0LKpPFOIfXS/mSypn+OtQwru5IuSk7WQDXeSj47vTqhV7iWCLtTlr/VQUyQHohu5D+yOiWUi9ymi4VztHitDPIHRvAHvD785AF9LZvQqwD7U8OQDmBnSwDdI55+XYhDvEmpvGhTu+LQtiacXI7Mbp4e4HkYnfNhuHd7FibQ4L2U2Yskeoxhw9TsxyTnpe93bZ0tKM6WNsxsp0kyBfvTG8F2mr3dtrg7VM5SSdLZdsn1xxyNsbLBO/q9D8IelBRlVO1kFZAH7YizSJIxh5klT4aKEh2b6IN8FBnpTbHqNO4hsOrTP870A9tX7VQbANhQmxwtJjOXaU4v6CTbEHRYK/bAP0juUv5HotDO7X91byp4+Vury9sb38N7dK86Ifal8FNMPq+XagH9SFumLncaPh0T8PHA9ARcvMuBXsd522nX9U1gFLHEJ51brgTfHqZ8PuLoG3vzjbZfPI9s/FA2S2PNr0qxNEaJ3cPjWRu2Kq0+nwSAPNX4XwSlVM1dwAkzvK1Had3/P4le27BmGcsGQ/rCJ/Ld46PB6y8NdRDGaulgXFtQHVPRWlUjot1diznO5/fP4I8yjsULrtyywADCnzBa84eUw2CtSHGe5B87VHGeA/wXIjRSG0g2TJPxxFWB2Rj+W3weKVUZb6UZAUJh6nWT06WFR2jTXqRMvBR8eDzyttYxYPxoYNvUzye8HqFfsRzg45Ufcjh6diPcPy6YlyrC5tAnOTY+cNggtPpjV3tvF7fYezTZAqYMfC61QasvkgG/amdIQx9E3QCW8e8fLajkyHawu+Xe72x/rP3HNAmm/icg3BdJxz/mg7YO4mjWoFkcsyO8whsy5NcCL9mos5AAMYjVo0Yw7gEj93Fmq40TRoe2reNd1t8+4Gox4WstJvkWIHzrSrUSeTw76BLzhgDpUwmW3XVjfZ0ffoAmH2alG1yqMk1JgLgO8rFy8TidKzhCz70921omy9j5XwZ7Y9Kl0yPfb/mAEexlN2RH3HeqzsNY6wrX+2ujPER1sZhxrfxSG73YxCcs1At+gAKH3C4+GpnWfW9Oj3g98+TLICrDYYmGK+efozDX7z3StXncsvv0yONCKrPuzV7HfuYarLvdWMhY8jGzzPIg7yWHt+1jM0n2OHCzjOeUyjcQkbz1cqDH+TDM2V8Eo0Hr5VPonKq0bE7eL5uzndOj4pCPW8csTyOM2PEUc9RMiN8NuU7Z3NulWsCiIwDpZzjgqHqN1goTPKIM+FYnthhVi52ftm7o4NtwqpNLYDxOY7DGhZaMZnjDsDaEOMNhdwBugPPatDTfZOcycAkpZO37b68hHirw/oFiI4W0/wQyTHRhTYwKUyUTpY5R+t+Wxnobk3FA+ff0+e1snGCgiNEci+t5giZXpAcw9QcLXL3G0Ifwsmr9WMpK+gv2LsHO1evknyJ+I8kA7a2yjW9Ih0sZUYf3arLdgVbwTTvwg4L/z2NdHcr5hkEc7LK3moavO3FdN/Jf39ciBFKgWR1B9nhvXb51sDpd3B4kmQyvI5pPOTHKtoPOZgOWJ9RuAhL0v9DXZ5mWndyPfuWcedlgDR0VX+8d85MV0qxM32G0DtaORtmNKx9OfTaLwxGPdad3VY58vunwUPhnGPQpTApwFgrHRwb1nZiB6CyTzTBXUKTK4LFebn6vAbwDf693Cg4WshTyL2hpCtFxp6kfMXUHS3tE/TBNaX8lAvG9c25eYfTjx5GT62NORmQLLqqXQzjEe132VpBmbtSAHgj2YWrxZPOtXaSgHaEdPR/n05FFHLH6gVP3/F/Ct5pwFxuNHx6pJFDqTabgr2OfYw2oC2ex1LHUKe+g91r71h+TKk+jZOevGCMFXInODk+/HwRF8XVkJrN17KVowV5K51qDgCQn5xP4vnSudWc6LFS8HGSY860U1aDEkPhnqOlW7y2ktNVWPUbUaXcPViHgGeLN1Dv+Opsi9OvaKBollZ1mnUuycXjZIBMOC5P36Dk98rRsmc/sDnueZSz9wwqI2ZGvtRjj5hxEFC/59cDxpHTrjUPm+T+2Nc6wdjqKvqqZe6Yz7ZI8Wx1QDZ4R2HvaLl+qX2Ky+9b+B0XBf9ZjxH6DCP1+uWSnXba6W2eD+PBZG98WNk4QXXkonjt7gFAMshfwVGKKbqjiR8SBV+1C8LYvtW6q36E7Jr6FQ6qTrzZi8amV0jX/O8ATxx/Z8owlvLgSOONcohPqjnPXt10rDCWJrYi52zVerkfWu4iCncxbDs+wvovxntAnqoPu65ckcZ17bgI8ZCD0wGMhUuobiTQjzdyeHDQMRf6GnqurzA8nwAPw+xsOX30fQyjNk5yp8N0szKEonMFdA7OYZ8NA/j9FLNhOaCMa3+fHitfPTkK7UqObkFR220A3a7YOuhBhVKOZqFrWec4g5p9Mtvk5NwKkyuCxXm5+rwGtBP8e7lQcLRKmZiqY0HXfxhbh2NxYOWm6mihDtD0cSQ/pvkzLncaxh4CP59OQ+6kWhshi0xauj5h7+7o6yq8gz70IFePDXPSnSsKl8fdkWvc6doKnXD6V2uv8vCA3RVt4sGV91c4cC/sTdO5QXO50bDyORoeFObdaK9jHyM/Odn7MWQ0VUdrYwjvKGt5OjrHFL35xDv5GDe4/mHjuNXRooxPonngl1Q+ifEFOggqq9Xexug4vb4i4rBdIUcT++LF7leR+6VjTFwc95AV0Il3c9kwEaEykgt9z3Oe30ccyQ4Oe+pjqO+ysptWhm9HGr8fQTJw0o6JxtUcLVN4cpMRGmsdpo1/Ah1t6eDZOy0B6IxjuZ6LosJ22aANa8wMWj/4rf0nXJLVzdMcXix6OxIvU+YLDu24+woqjrC4rqyEH8Cz22mpFMIGBv6WvUFT3cnCBECi6Ady+KqbQBBXGQ88k+5aVXwUwofxgHKej8iD8YE4ft7bymsZ+zkHfJ2GVUa6n0FiVLYr5bJhl8Ouvg8xiGI/UssvwXPajUz7EBf29P1b9A868ILL8FvhhC6TH1+8twz3JnIo5cf8bvNxjc5WA3RgnsH53yjqu1a4P5X9otfGQoz3ING5z5gcuMwHbbxpet+EqhPWRrSLX/Hr6odyni1tzgqg+lCTA2OsrDlbzaLAkSPXe3W0NyQXafdWHb6VZIcxTVDL6ne0ajYMMDvW5iBCxpQZSwh4VkPfKEe3C7hhJ9UVTIAkct2vDEcI0GW0qY0nh6x9KifgbOlRLn4Y82q8R7mG7Ak5G+NlgvdSdwBML0o57kq7MhpgJ8B/tTsOII0yjhboWZ6Euv38WJEZByT6e4G2BU7ARcP+tIaWhQ24thMUk3n5vIuDDp+MOkzeWid2BM+wMhEmH7Q3k3YMx29E3+Bdx39layh9NVdscDqCL+G+7Otr46GQL4BhP7od6QM4HNVCaZi5HDS0PFCnEYYxhXk32us4b2v+Z8vgaHHYgHcdP/gQoTaGSPSkGt94Rhz6vSP9VPkTmv+4QuY38I+No9rpW+EcrY7Yj8siDZVl5ZOQbBSkrzNVJ1Huc2X9bhfqg3PWD1YiTkt3D84juc3/A88UwAJ7NxM9icMhJF+TpS9+msynboNiNZouLPLfLxkzMJygRSJgfAn2wAr5RNgE/QDJigDKik92v0r6ybkGbA2uJjkiQ56NJD8jgS8JcISAdNCt7hVFcJlV5HiK4LRzMenGeA+0HfyRGCbj7zVu749tVQeF0vgYaqsWQJ0gfLUBh+S7JM4qVkD4GQMYszetvObBgIDThriXwUspkz8mBygryt/M4TFN/wfl2Va6uKyJPI+DnvWB50PTEw8dUeiKjwwPCC+zo5hWPiT9iPsb6Av09WqTtzokl5F85XRzKf9SBkqKgej7EF981fqRhJcsOO3/qF/WL0F/yekV4esxXaHpwgA7B4jHjsNX/CfGNajC48Iq5z0PY6eeIQHGEUccA+/UuL4wvqpQhKNXHRvQcdM1OO8HROMHkDgmOZowKOjDNHY03GDthQ64eDjX+0XaEZzvi5BvjGfAiYWD8ncxIYLkbsc5pdxlwmIh3gvEggVfMV9J8uvMOE6tvnQLNuwoUjvWq6EOlcE4QtTjQuJf1kvXWTlavSS7F5jsUTf0FUeDONqFXtf+u4bavWrl3oaixT7pYkDsU6bvPZQ/yLYn1yJvF/3YRnBySe+kNgYTLv/9NskFf9h2jJ1LSXYFbmC+MAnifijKYJyBhh97qANf9lV1kdPBUYLqthqhZq9J2nFTKcdA15D8GGslL9gpkg+71jctxKh31N3naOmJwlWc/ojWkb7stnTVkcs5/ieF2JD1HK70svE8WJzB2dX7SBykJ8pwx7VpLvc0UL6Ur1CzNAwk9trPu5W9LvLzdmV3OH0j5I78JGMojZ9CnKDaGEJ6IXMS9A100C/VfEIy3lE3aNzA+e5eunTJMrWZ4xqgb6uVRuILz7rAjj4J5q20wWFQG4N5Nc1rWu/qQo7RMUfiX+xh3oYDnwcYZkI4Xtqj0/BVClZzEL5548Og1G33GA9ondUR1xSxCJMfHrjOHYa4mzUXMWYDSlfUi3OGdgik3+9xRx2LPJ3CbfUjj3rx3mRXfEyBhwT0RRuNlF7/lwZ2Tm99GHURq4i+FS7AbdqbgnNDskt2O4d1Pn4bB7bV18J4WIQa4CM5/lWfcVrROum3JiY7Ucpvuh0CO5TTF7NRyKOLghpRn95mx4ZFIRMWy3F5nxyL+jHKojCOsOtWHQ0Z0B+gF+NbEcTWe22Xpwdk2ybXycJ20e3dP89jpN+Egrygj7k26S7MmibHUJ2pA1t22Ko60CcxEcAcW7bMtcZDjFck+lV/Z1Rl0Fzu+TOdyZABavMu3kP6sMD/lfTzUG0MkZ5C6K4zTrJq/aI8pnGofEwK5pO00LA6Uv3a9jS/dsvu4ibne+Ro6Jx5gfnMexsK52jFtLkOnUD7JjGA007IKTrJp7ybYvy2ChiKInPvxb6ai/ELyAM60yLHxq8ZHcZKuei7VicI3AXBRfhtFBOxmBPJOyxGQbMf6EfWi5Ni/ExiLvAwkzBHK8YvYAFzEiTHKjgOTKEsu02e+6ygzVRiR6CUjwgaAeNDclT6KBXYTi7w44A4228g3RA9z4E2k2x/Y0sdAT8KfFLLKnt+YWa6DY5SVo4xYxNKOcq/B4Gf3x/TFzB1zIwqCEg+ZJqFfuy1cvZ4iBi95EsB7u/anJU99l7AvMXolWiuYL60dPny5Uuo5X5WD7PRotmoc9vBqKU3MvojI7yAyWAudsdc5KkRU2B2CkUXMF8w1U6eavkFjAqz3zOzz8EIMBuNmo06J4sZ5nV6qpseKgv4xUS/9vTHLGBy+H9BTc36/pOshwAAAABJRU5ErkJggg==>

[image4]: <data:image/png;base64,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>

[image5]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABMAAAAXCAYAAADpwXTaAAABgUlEQVR4Xo1TvU7EMAxuJCSQ+BESHCdIz2mrwj3A8TBMjAxM7IyIkQmx3MLAyAvwYuw4/3acnLCUxv782f7StF3XMlUC1iKoWFqloFZUwSxUwi4uQWYNBkMU1cY00t1SCpLsImDvF4jQwzCR7fzwhnlx9rmDRHNSZUxKsRHw7VMoTIym5HzVGYvPYRgOAODhfLE44q2rY4KUmoWuBsyrAdiIpJPIZNGQdCUcALMxxrxhtJcpDQWhZ8VyJR73CQzcU9B54zCc4qTL/y7ANU0TCoRPPPL7fD2fuEbLi+UhJp9xbY1dYLbISr7bGwsv4gc5v8bAyzzP+1ke0erD8PJcUB5adf1qdYsNv6+07msMbiKb36TW+gwVfY3jaHI6FjgRolqY56gOj/WIF3DHk6G+bMOOmWLrKPfRYrOPvu81y5W268uPiB2xXt8c80Sc2mxNLUu0Xnolsba1+wqCpgRlFlWU5r2ACG6m5eLchB+N+nQEw7N5ODdL/3aNz7FGVAoI9ge10SiGbyO1rAAAAABJRU5ErkJggg==>