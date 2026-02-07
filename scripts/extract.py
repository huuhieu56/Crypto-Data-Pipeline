# =============================================================================
# Extract Script - Thu thập dữ liệu từ Binance Data Vision
# =============================================================================

import io
import time
import zipfile
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta


# =============================================================================
# Configuration
# =============================================================================
BINANCE_DATA_VISION_URL = "https://data.binance.vision/data/spot/monthly/klines/{symbol}/1m/{symbol}-1m-{year}-{month:02d}.zip"
MONTHS_BACK = 36

def find_project_root(start: Path) -> Path:
    current = start.resolve()
    for _ in range(10):
        if (current / "data" / "raw").exists():
            return current
        if current.parent == current:
            break
        current = current.parent
    return start.resolve()

PROJECT_ROOT = find_project_root(Path(__file__).parent)
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Helper Functions
# =============================================================================
def get_symbols() -> list:
    """Đọc danh sách symbols từ file CSV, chỉ lấy các symbol đang TRADING."""
    symbols_file = RAW_DATA_DIR / "symbols.csv"
    if not symbols_file.exists():
        print(f"ERROR: File not found: {symbols_file}")
        return []
    
    symbols_df = pd.read_csv(symbols_file)
    return symbols_df[symbols_df["status"] == "TRADING"]["symbol"].tolist()


def get_target_months(months_back: int) -> list[tuple[int, int]]:
    """Trả về danh sách (year, month) cần tải, tính từ tháng trước."""
    end_date = datetime.now() - relativedelta(months=1)
    return [
        ((end_date - relativedelta(months=i)).year, (end_date - relativedelta(months=i)).month)
        for i in range(months_back)
    ]


# =============================================================================
# Extract Klines from Binance Data Vision
# =============================================================================
def download_klines(symbol: str, year: int, month: int) -> pd.DataFrame | None:
    """Download dữ liệu klines 1 tháng từ Binance Data Vision."""
    url = BINANCE_DATA_VISION_URL.format(symbol=symbol, year=year, month=month)
    print(f"DOWNLOADING: {url}")
    
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as csv_file:
                df = pd.read_csv(
                    csv_file,
                    header=None,
                    usecols=range(11),
                    names=[
                        "open_time", "open", "high", "low", "close", "volume",
                        "close_time", "quote_volume", "trades", 
                        "taker_buy_base", "taker_buy_quote"
                    ]
                )
                
                # Binance Data Vision dùng microseconds (2025+) hoặc milliseconds (2024-)
                open_time_raw = df["open_time"].astype("int64")
                close_time_raw = df["close_time"].astype("int64")
            
                if open_time_raw.iloc[0] > 1e15:
                    df["open_time"] = pd.to_datetime(open_time_raw // 1000, unit="ms")
                    df["close_time"] = pd.to_datetime(close_time_raw // 1000, unit="ms")
                else:
                    df["open_time"] = pd.to_datetime(open_time_raw, unit="ms")
                    df["close_time"] = pd.to_datetime(close_time_raw, unit="ms")
                
                df["symbol"] = symbol

                return df
                
    except requests.HTTPError as e:
        print(f"WARNING: HTTP Error for {symbol} ({year}-{month:02d}): {e}")
        return None
    except Exception as e:
        print(f"ERROR: {symbol}: {e}")
        return None


def extract_klines(symbols: list, months_back: int = MONTHS_BACK) -> dict:
    """Tải dữ liệu klines cho danh sách symbols."""
    results = {}
    target_months = get_target_months(months_back)
    
    for idx, symbol in enumerate(symbols, 1):
        print(f"\n[{idx}/{len(symbols)}] Processing {symbol}...")
        all_data = []
        
        for year, month in target_months:
            df = download_klines(symbol, year, month)
            if df is not None:
                all_data.append(df)
                print(f"SUCCESS: {symbol}: {year}-{month:02d} - {len(df):,} records")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.sort_values("open_time").reset_index(drop=True)
            
            output_path = RAW_DATA_DIR / f"{symbol}.csv"
            combined_df.to_csv(output_path, index=False)
            print(f"SAVED: {symbol} -> {output_path} ({len(combined_df):,} records)")
            
            results[symbol] = combined_df
        else:
            print(f"ERROR: No data for {symbol}")
    
    return results


# =============================================================================
# Extract Recent Klines (từ API REST) để lấy klines mới nhất 
# =============================================================================
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
API_LIMIT = 1000 

# Lấy open_time cuối cùng của file csv 
def get_last_timestamp(symbol: str) -> int | None:
    """Đọc file CSV và trả về open_time cuối cùng dưới dạng milliseconds."""
    csv_path = RAW_DATA_DIR / f"{symbol}.csv"
    if not csv_path.exists():
        print(f"WARNING: File not found: {csv_path}")
        return None
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            return None
        
        last_open_time = df["open_time"].iloc[-1]
        
        # Chuyển đổi sang milliseconds
        if isinstance(last_open_time, str):
            dt = pd.to_datetime(last_open_time)
            return int(dt.timestamp() * 1000)
        elif isinstance(last_open_time, (int, float)):
            if last_open_time > 1e15: 
                return int(last_open_time // 1000)
            elif last_open_time > 1e12: 
                return int(last_open_time)
            else: 
                return int(last_open_time * 1000)
        else:
            return int(pd.Timestamp(last_open_time).timestamp() * 1000)
            
    except Exception as e:
        print(f"ERROR reading {csv_path}: {e}")
        return None

# Gọi Binance REST API để lấy klines từ start_time đến end_time.
def fetch_klines_from_api(symbol: str, start_time: int, end_time: int | None = None) -> list[pd.DataFrame]:
    all_data = []
    current_start = start_time + 60000
    
    while True:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": current_start,
            "limit": API_LIMIT
        }
        if end_time:
            params["endTime"] = end_time
        
        try:
            response = requests.get(BINANCE_API_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                break
            
            df = pd.DataFrame(data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades",
                "taker_buy_base", "taker_buy_quote", "ignore"
            ])
            
            df = df.drop(columns=["ignore"])
            
            numeric_cols = ["open", "high", "low", "close", "volume", 
                          "quote_volume", "taker_buy_base", "taker_buy_quote"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
            
            df["symbol"] = symbol
            all_data.append(df)
            
            print(f"  Fetched {len(df)} records from {df['open_time'].iloc[0]} to {df['open_time'].iloc[-1]}")
            
            # Nếu nhận được ít hơn limit, đã hết dữ liệu
            if len(data) < API_LIMIT:
                break
            
            # Cập nhật start_time cho lần gọi tiếp theo
            current_start = int(data[-1][0]) + 60000  # +1 phút

            time.sleep(0.1)
            
        except requests.HTTPError as e:
            print(f"ERROR: HTTP Error for {symbol}: {e}")
            break
        except Exception as e:
            print(f"ERROR: {symbol}: {e}")
            break
    
    return all_data

# Gọi get_last_timestamp và fetch_klines_from_api để cập nhật dữ liệu mới nhất, gộp vào file CSV.
def extract_recent_klines(symbols: list | None = None) -> dict:
    if symbols is None:
        symbols = get_symbols()
    
    if not symbols:
        print("ERROR: No symbols to process")
        return {}
    
    results = {}
    end_time = int(datetime.now().timestamp() * 1000)
    
    for idx, symbol in enumerate(symbols, 1):
        print(f"\n[{idx}/{len(symbols)}] Updating {symbol}...")
        
        last_ts = get_last_timestamp(symbol)
        if last_ts is None:
            print(f"  SKIP: No existing data for {symbol}")
            continue
        
        print(f"  Last timestamp: {pd.to_datetime(last_ts, unit='ms')}")
        
        new_data = fetch_klines_from_api(symbol, last_ts, end_time)
        
        if not new_data:
            print(f"  No new data for {symbol}")
            continue

        new_df = pd.concat(new_data, ignore_index=True)
        print(f"  Total new records: {len(new_df)}")
        
        csv_path = RAW_DATA_DIR / f"{symbol}.csv"
        old_df = pd.read_csv(csv_path)
        
        if not pd.api.types.is_datetime64_any_dtype(old_df["open_time"]):
            old_df["open_time"] = pd.to_datetime(old_df["open_time"])
        if not pd.api.types.is_datetime64_any_dtype(old_df["close_time"]):
            old_df["close_time"] = pd.to_datetime(old_df["close_time"])
        
        combined_df = pd.concat([old_df, new_df], ignore_index=True)
        
        # Loại bỏ duplicates dựa trên open_time
        combined_df = combined_df.drop_duplicates(subset=["open_time"], keep="last")

        combined_df = combined_df.sort_values("open_time").reset_index(drop=True)
        
        # Lưu lại
        combined_df.to_csv(csv_path, index=False)
        print(f"  SAVED: {csv_path} ({len(combined_df):,} total records, +{len(new_df)} new)")
        
        results[symbol] = combined_df
    
    return results


# =============================================================================
# Extract Ticker 24h (từ 2 endpoints: /ticker/24hr + /ticker/bookTicker)
# =============================================================================
TICKER_24H_URL = "https://api.binance.com/api/v3/ticker/24hr"
BOOK_TICKER_URL = "https://api.binance.com/api/v3/ticker/bookTicker"


def extract_ticker_24h(symbols: list | None = None) -> pd.DataFrame | None:
    """Thu thập dữ liệu ticker 24h từ Binance API.
    
    Kết hợp 2 endpoints:
    - /api/v3/ticker/24hr: thống kê biến động giá, volume, trades trong 24h
    - /api/v3/ticker/bookTicker: giá bid/ask tốt nhất hiện tại
    
    Merge theo symbol, tính spread_pct, lưu vào data/raw/ticker_24h.csv
    """
    if symbols is None:
        symbols = get_symbols()
    
    if not symbols:
        print("ERROR: No symbols to process")
        return None
    
    symbols_set = set(symbols)
    snapshot_time = datetime.utcnow()
    
    # --- 1. Gọi /api/v3/ticker/24hr (không truyền symbol → lấy tất cả, sau đó lọc) ---
    print("Fetching /api/v3/ticker/24hr ...")
    try:
        resp_ticker = requests.get(TICKER_24H_URL, timeout=30)
        resp_ticker.raise_for_status()
        ticker_data = resp_ticker.json()
    except Exception as e:
        print(f"ERROR: Failed to fetch ticker/24hr: {e}")
        return None
    
    ticker_df = pd.DataFrame(ticker_data)
    ticker_df = ticker_df[ticker_df["symbol"].isin(symbols_set)].copy()
    
    # Chọn và đổi tên các cột cần thiết
    ticker_df = ticker_df.rename(columns={
        "priceChange": "price_change",
        "priceChangePercent": "price_change_pct",
        "highPrice": "high_24h",
        "lowPrice": "low_24h",
        "volume": "volume_24h",
        "quoteVolume": "quote_volume_24h",
        "count": "trade_count",
    })
    ticker_cols = ["symbol", "price_change", "price_change_pct", "high_24h",
                   "low_24h", "volume_24h", "quote_volume_24h", "trade_count"]
    ticker_df = ticker_df[ticker_cols]
    
    print(f"  Received {len(ticker_df)} symbols from ticker/24hr")
    
    # --- 2. Gọi /api/v3/ticker/bookTicker (weight nhẹ, lấy tất cả) ---
    print("Fetching /api/v3/ticker/bookTicker ...")
    try:
        resp_book = requests.get(BOOK_TICKER_URL, timeout=30)
        resp_book.raise_for_status()
        book_data = resp_book.json()
    except Exception as e:
        print(f"ERROR: Failed to fetch ticker/bookTicker: {e}")
        return None
    
    book_df = pd.DataFrame(book_data)
    book_df = book_df[book_df["symbol"].isin(symbols_set)].copy()
    
    book_df = book_df.rename(columns={
        "bidPrice": "bid_price",
        "askPrice": "ask_price",
    })
    book_df = book_df[["symbol", "bid_price", "ask_price"]]
    
    print(f"  Received {len(book_df)} symbols from bookTicker")
    
    # --- 3. Merge 2 DataFrame theo symbol ---
    merged_df = ticker_df.merge(book_df, on="symbol", how="left")
    
    # --- 4. Chuyển đổi kiểu dữ liệu ---
    float_cols = ["price_change", "price_change_pct", "high_24h", "low_24h",
                  "volume_24h", "quote_volume_24h", "bid_price", "ask_price"]
    for col in float_cols:
        merged_df[col] = pd.to_numeric(merged_df[col], errors="coerce")
    
    merged_df["trade_count"] = pd.to_numeric(merged_df["trade_count"], errors="coerce").astype("Int64")
    
    # --- 5. Tính spread_pct = (ask - bid) / ask * 100 ---
    merged_df["spread_pct"] = (
        (merged_df["ask_price"] - merged_df["bid_price"]) / merged_df["ask_price"] * 100
    )
    
    # --- 6. Thêm snapshot_time ---
    merged_df.insert(1, "snapshot_time", snapshot_time)
    
    # --- 7. Sắp xếp cột theo đúng thứ tự ---
    final_cols = [
        "symbol", "snapshot_time", "price_change", "price_change_pct",
        "high_24h", "low_24h", "volume_24h", "quote_volume_24h",
        "trade_count", "bid_price", "ask_price", "spread_pct"
    ]
    merged_df = merged_df[final_cols]
    
    # --- 8. Lưu vào CSV (append nếu đã tồn tại) ---
    output_path = RAW_DATA_DIR / "ticker_24h.csv"
    
    if output_path.exists():
        old_df = pd.read_csv(output_path)
        merged_df = pd.concat([old_df, merged_df], ignore_index=True)
    
    merged_df.to_csv(output_path, index=False)
    print(f"  SAVED: {output_path} ({len(merged_df):,} total records)")
    
    return merged_df

# TODO: Implement extract_order_book_snapshot()
# - Gọi Binance /depth API (top N levels)
# - Tính total_bid_volume, total_ask_volume, imbalance
# - Lưu vào data/raw/order_book_snapshot.csv


# =============================================================================
# Main (hàm main chỉ dùng để test thủ công, sau này dùng airflow để update tự động)
# =============================================================================
def main():
    # Lấy danh sách symbols 
    symbols = get_symbols()
    if not symbols:
        return
    
    # Extract klines 3 years từ Binance Data Vision (monthly files) 
    #---------------------- CHẠY 1 LẦN ĐẦU TIÊN ----------------------
    # print(f"Starting extraction: {len(symbols)} symbols, {MONTHS_BACK} months")
    # klines_data = extract_klines(symbols, months_back=MONTHS_BACK)
    # total_records = sum(len(df) for df in klines_data.values())
    # print(f"\nSUCCESS: {len(klines_data)}/{len(symbols)} symbols, {total_records:,} records")
    
    # Extract recent klines từ REST API (cập nhật dữ liệu mới nhất)
    #-------------------------------------------------------------------
    # print(f"\nUpdating recent klines for {len(symbols)} symbols...")
    # recent_data = extract_recent_klines(symbols)
    # if recent_data:
    #     total_new = sum(len(df) for df in recent_data.values())
    #     print(f"\nSUCCESS: Updated {len(recent_data)}/{len(symbols)} symbols")
    
    # Extract ticker 24h
    print(f"\nExtracting ticker 24h for {len(symbols)} symbols...")
    ticker_df = extract_ticker_24h(symbols)
    if ticker_df is not None:
        print(f"SUCCESS: Ticker 24h - {len(ticker_df):,} records")
    
    # TODO: Extract order book snapshots
     

if __name__ == "__main__":
    main()
