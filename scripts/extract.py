# =============================================================================
# Extract Script - Thu thập dữ liệu từ Binance Data Vision
# =============================================================================

import io
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
# Extract Klines
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

# TODO: Implement extract_ticker_24h()
# - Gọi Binance /ticker/24hr và /ticker/bookTicker
# - Merge theo symbol và tính spread_pct
# - Lưu vào data/raw/ticker_24h.csv

# TODO: Implement extract_order_book_snapshot()
# - Gọi Binance /depth API (top N levels)
# - Tính total_bid_volume, total_ask_volume, imbalance
# - Lưu vào data/raw/order_book_snapshot.csv


# =============================================================================
# Main
# =============================================================================
def main():
    # Lấy danh sách symbols 
    symbols = get_symbols()
    if not symbols:
        return
    
    #Extract klines
    print(f"Starting extraction: {len(symbols)} symbols, {MONTHS_BACK} months")
    klines_data = extract_klines(symbols, months_back=MONTHS_BACK)
    
    total_records = sum(len(df) for df in klines_data.values())
    print(f"\nSUCCESS: {len(klines_data)}/{len(symbols)} symbols, {total_records:,} records")
    
    # TO DO: Extract ticker 24h
    
    # TO DO: Extract order book snapshots
     

if __name__ == "__main__":
    main()
