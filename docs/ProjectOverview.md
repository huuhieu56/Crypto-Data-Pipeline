# Hệ thống Data Pipeline Big Data + LLM Advisory cho Crypto (ClickHouse)

---

## 1. Tổng quan dự án

### 1.1. Bài toán

Thị trường crypto vận hành 24/7, dữ liệu biến động liên tục và khối lượng lớn. Mục tiêu của dự án là xây dựng pipeline tự động để:

- Thu thập dữ liệu nến 1 phút cho nhiều coin.
- Tính chỉ báo kỹ thuật để tạo ngữ cảnh phân tích.
- Sinh gợi ý đầu tư BUY/SELL/HOLD bằng LLM trên dữ liệu gần nhất.
- Hiển thị dashboard theo dõi thị trường và tín hiệu theo thời gian thực.

### 1.2. Mục tiêu hệ thống

| Giai đoạn | Công nghệ | Mục đích |
| --- | --- | --- |
| Extract | Python + Binance API | Thu thập dữ liệu nến, ticker, order book |
| Transform | Apache Spark | Tính RSI, MACD trên dữ liệu 1 phút |
| Load | ClickHouse | Lưu trữ và truy vấn nhanh cho analytics |
| Orchestrate | Apache Airflow | Lập lịch và điều phối jobs |
| Advisory | LLM API (Gemini/OpenAI) | Tổng hợp tín hiệu đầu tư theo ngữ cảnh đã xử lý |
| Visualize | Grafana | Dashboard giám sát dữ liệu và tín hiệu |

### 1.3. Phạm vi

| Thành phần | Giá trị |
| --- | --- |
| Số lượng coin | 50 |
| Khung thời gian dữ liệu gốc | 1 phút |
| Cửa sổ phân tích LLM hiện tại | 30 nến ngày gần nhất |
| Tần suất signal | Mỗi giờ 1 lần |
| Output | BUY / SELL / HOLD + confidence + reason + key_risk |

---

## 2. Danh sách symbol đầy đủ

Nguồn chuẩn vẫn là [config/symbols.py](config/symbols.py), nhưng danh sách dưới đây được ghi đầy đủ để dễ theo dõi trong tài liệu.

### 2.1. Nhóm TRADING

| STT | Symbol | Trạng thái | STT | Symbol | Trạng thái | STT | Symbol | Trạng thái |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | BTCUSDT | TRADING | 16 | UNIUSDT | TRADING | 31 | OPUSDT | TRADING |
| 2 | ETHUSDT | TRADING | 17 | LTCUSDT | TRADING | 32 | GRTUSDT | TRADING |
| 3 | BNBUSDT | TRADING | 18 | HBARUSDT | TRADING | 33 | THETAUSDT | TRADING |
| 4 | SOLUSDT | TRADING | 19 | PEPEUSDT | TRADING | 34 | FILUSDT | TRADING |
| 5 | XRPUSDT | TRADING | 20 | NEARUSDT | TRADING | 35 | ARUSDT | TRADING |
| 6 | DOGEUSDT | TRADING | 21 | APTUSDT | TRADING | 36 | WIFUSDT | TRADING |
| 7 | ADAUSDT | TRADING | 22 | ICPUSDT | TRADING | 37 | RUNEUSDT | TRADING |
| 8 | TRXUSDT | TRADING | 23 | ETCUSDT | TRADING | 38 | ALGOUSDT | TRADING |
| 9 | LINKUSDT | TRADING | 24 | STXUSDT | TRADING | 39 | FLOWUSDT | TRADING |
| 10 | AVAXUSDT | TRADING | 25 | RENDERUSDT | TRADING | 40 | XTZUSDT | TRADING |
| 11 | TONUSDT | TRADING | 26 | ATOMUSDT | TRADING | 41 | AXSUSDT | TRADING |
| 12 | SHIBUSDT | TRADING | 27 | VETUSDT | TRADING | 42 | SANDUSDT | TRADING |
| 13 | XLMUSDT | TRADING | 28 | ARBUSDT | TRADING | 43 | MANAUSDT | TRADING |
| 14 | BCHUSDT | TRADING | 29 | INJUSDT | TRADING | 44 | NEOUSDT | TRADING |
| 15 | DOTUSDT | TRADING | 30 | IMXUSDT | TRADING | 45 | AAVEUSDT | TRADING |

### 2.2. Nhóm BREAK

| Symbol | break_date | Ghi chú |
| --- | --- | --- |
| MATICUSDT | 2024-09-10 | Migrated to POL |
| CROUSDT | 2023-10-04 | Delisted |
| MKRUSDT | 2024-11-21 | Migrated to SKY |
| FTMUSDT | 2025-03-20 | Migrated to Sonic (S) |
| EOSUSDT | 2025-05-27 | Delisted |

---

## 3. Kiến trúc hệ thống

### 3.1. Luồng tổng quan

```text
Binance API -> Raw CSV -> Spark Transform -> ClickHouse
                                      |
                                      v
                                llm_signal.py
                                      |
                                      v
                                llm_signals table
                                      |
                                      v
                                  Grafana
```

### 3.2. Các layer

| Layer | Nội dung |
| --- | --- |
| Data Lake Raw | data/raw/ (CSV) |
| Data Lake Processed | data/processed/ (Parquet) |
| Warehouse | ClickHouse (crypto_db) |
| Advisory | scripts/llm_signal.py gọi LLM |

---

## 4. Database schema (ClickHouse)

### 4.1. Các bảng chính

| Bảng | Vai trò | Tần suất ghi |
| --- | --- | --- |
| symbols | Dimension thông tin coin | setup + update khi cần |
| klines | Fact candles 1 phút + RSI/MACD | mỗi phút |
| ticker_24h | Fact snapshot 24h | hằng ngày |
| order_book_snapshot | Fact áp lực mua/bán | hằng ngày |
| llm_signals | Fact signal BUY/SELL/HOLD | mỗi giờ |

### 4.2. Bảng llm_signals

Bảng llm_signals lưu kết quả từ LLM và context để audit/debug:

- symbol, generated_at
- signal (BUY|SELL|HOLD), confidence, reason, key_risk
- context: rsi_14, macd_cross, ob_imbalance, vol_change_pct, price_change_pct
- context xu hướng: data_window_minutes, trend_6h, trend_6h_pct
- metadata: llm_provider, model_version

---

## 5. ETL + Advisory pipeline

### 5.1. Extract

- [scripts/extract.py](scripts/extract.py): lấy dữ liệu klines 1 phút (bulk + incremental).
- [scripts/pre_extract.py](scripts/pre_extract.py): self-healing gap detection/backfill.
- Daily snapshot cho ticker_24h và order_book_snapshot.

### 5.2. Transform

- [scripts/transform.py](scripts/transform.py) dùng Spark để tính RSI/MACD.
- Output: parquet trong data/processed/.

### 5.3. Load

- [scripts/load.py](scripts/load.py) ghi vào ClickHouse.

### 5.4. LLM Signal (đã cập nhật)

- [scripts/llm_signal.py](scripts/llm_signal.py) lấy dữ liệu từ ClickHouse và tổng hợp thành đúng 30 nến ngày/symbol.
- Query daily aggregate dùng toDate + argMin/argMax/min/max/sum ngay trong ClickHouse để giảm tải Python.
- Gọi provider (gemini hoặc openai) qua [utils/llm_utils.py](utils/llm_utils.py).
- Parse JSON output và ghi vào llm_signals.
- Khi hết quota (429), hệ thống fail-fast và ghi fallback HOLD để DAG vẫn hoàn thành.

---

## 6. Scheduling với Airflow

### 6.1. Danh sách DAG

| DAG | Schedule | Mục đích |
| --- | --- | --- |
| minutely_extract | * * * * * | ETL klines mỗi phút |
| daily_snapshot | 0 0 * * * | ticker_24h + order_book snapshot |
| hourly_inference | 0 * * * * | chạy LLM signal mỗi giờ |

### 6.2. Hourly DAG (sau refactor)

hourly_inference chỉ còn 1 task:

1. llm_signal: tạo signal BUY/SELL/HOLD cho các symbol có dữ liệu hợp lệ.

---

## 7. Cấu hình LLM

File [config/llm_config.py](config/llm_config.py) quản lý:

- provider: LLM_PROVIDER=gemini|openai
- key: GEMINI_API_KEY, OPENAI_API_KEY
- model:
  - GEMINI_MODEL (mặc định: gemini-2.5-flash-lite)
  - OPENAI_MODEL (mặc định: gpt-5.4-nano)
- generation params: TEMPERATURE, MAX_TOKENS, TIMEOUT_SECONDS
- prompt/data params: LLM_DAILY_CANDLES (mặc định 30)
- batch/retry: BATCH_SIZE, MAX_RETRIES, RETRY_DELAY

---

## 8. Grafana dashboard

### 8.1. Panels nên có

1. Top volume 24h
2. Gainers/Losers 24h
3. Price chart theo symbol
4. RSI heatmap
5. Latest LLM Signals
6. Signal distribution (24h)

### 8.2. Query mẫu: latest signals

```sql
SELECT symbol, generated_at, signal, confidence, reason
FROM crypto_db.llm_signals
ORDER BY generated_at DESC
LIMIT 50;
```

---

## 9. Vận hành và checklist

### 9.1. Kiểm tra trước khi demo

- ClickHouse healthy và có dữ liệu klines.
- llm_signal.py chạy thành công cho ít nhất 1 symbol.
- llm_signals có dữ liệu mới theo giờ.
- Airflow DAG hourly_inference chạy pass.
- Dashboard đọc được signal mới nhất.

### 9.2. Rủi ro chính

| Rủi ro | Tác động | Giải pháp |
| --- | --- | --- |
| API key sai/hết quota | Không tạo được signal chuẩn | Retry + fallback HOLD |
| Thiếu dữ liệu coin | Bỏ qua coin đó | Lọc symbol có đủ 30 nến ngày |
| Rate limit provider | Trễ signal | Batch nhỏ + exponential backoff |
| DB downtime | DAG fail | Healthcheck + retry task |

---

## 10. Trạng thái hiện tại

- Hệ thống đã sử dụng ClickHouse làm warehouse chính.
- LLM signal layer đã tích hợp vào hourly pipeline.
- Chế độ context hiện tại dùng 30 nến ngày để giảm token/quota và ưu tiên tính ổn định khi vận hành.
