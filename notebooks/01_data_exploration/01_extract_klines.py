# 2. Extract Klines: Tải dữ liệu nến 1 phút từ Binance Data Vision
import io
import zipfile
import requests
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

BINANCE_DATA_VISION_URL = "https://data.binance.vision/data/spot/monthly/klines/{symbol}/1m/{symbol}-1m-{year}-{month:02d}.zip"

# Đường dẫn lưu file
RAW_DATA_DIR = Path("./data/raw")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Đọc danh sách symbols từ file CSV
symbols_df = pd.read_csv(RAW_DATA_DIR / "symbols.csv")
symbols_list = symbols_df["symbol"].tolist()

def download_klines(symbol: str, year: int, month: int) -> pd.DataFrame | None:
    url = BINANCE_DATA_VISION_URL.format(symbol=symbol, year=year, month=month)
    print(f"📥 Downloading: {url}")
    
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as csv_file:
                # Đọc CSV - Binance data có 12 cột, chỉ lấy 11 cột đầu (bỏ ignore)
                df = pd.read_csv(
                    csv_file,
                    header=None,
                    usecols=range(11),  # Chỉ đọc 11 cột đầu, bỏ cột ignore
                    names=[
                        "open_time", "open", "high", "low", "close", "volume",
                        "close_time", "quote_volume", "trades", 
                        "taker_buy_base", "taker_buy_quote"
                    ]
                )
                
                # Binance Data Vision dùng microseconds (2025+) hoặc milliseconds (2024-)
                # Detect format: microseconds có 16 chữ số (>1e15), milliseconds có 13 chữ số
                open_time_raw = df["open_time"].astype("int64")
                close_time_raw = df["close_time"].astype("int64")
                
                # Nếu timestamp > 1e15 thì là microseconds, cần chia 1000
                if open_time_raw.iloc[0] > 1e15:
                    df["open_time"] = pd.to_datetime(open_time_raw // 1000, unit="ms")
                    df["close_time"] = pd.to_datetime(close_time_raw // 1000, unit="ms")
                else:
                    df["open_time"] = pd.to_datetime(open_time_raw, unit="ms")
                    df["close_time"] = pd.to_datetime(close_time_raw, unit="ms")
                
                # Thêm các cột thời gian tiện dụng
                df["symbol"] = symbol
                df["minute_of_day"] = df["open_time"].dt.hour * 60 + df["open_time"].dt.minute
                df["day"] = df["open_time"].dt.day
                df["month"] = df["open_time"].dt.month
                df["year"] = df["open_time"].dt.year

                return df
                
    except requests.HTTPError as e:
        print(f"⚠️ HTTP Error for {symbol} ({year}-{month:02d}): {e}")
        return None
    except Exception as e:
        print(f"❌ Error downloading {symbol}: {e}")
        return None


def extract_klines(symbols: list, months_back: int = 1) -> dict:
    """
    Tải dữ liệu klines cho danh sách symbols.
    
    Args:
        symbols: Danh sách trading pairs
        months_back: Số tháng cần tải (tính từ tháng trước)
    
    Returns:
        Dict với key là symbol và value là DataFrame
    """
    results = {}
    
    # Tính tháng bắt đầu (tháng trước)
    end_date = datetime.now() - relativedelta(months=1)
    
    for idx, symbol in enumerate(symbols, 1):
        print(f"\n[{idx}/{len(symbols)}] Processing {symbol}...")
        all_data = []
        
        for i in range(months_back):
            target_date = end_date - relativedelta(months=i)
            year = target_date.year
            month = target_date.month
            
            df = download_klines(symbol, year, month)
            if df is not None:
                all_data.append(df)
                print(f"✅ {symbol}: {year}-{month:02d} - {len(df):,} records")
        
        if all_data:
            # Gộp tất cả các tháng
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.sort_values("open_time").reset_index(drop=True)
            
            # Lưu file CSV
            output_path = RAW_DATA_DIR / f"{symbol}.csv"
            combined_df.to_csv(output_path, index=False)
            print(f"💾 Saved {symbol} to {output_path} ({len(combined_df):,} records)")
            
            results[symbol] = combined_df
        else:
            print(f"❌ No data for {symbol}")
    
    return results


# Chạy extract với 1 tháng dữ liệu (tháng 01/2026)
print(f"🚀 Starting klines extraction for {len(symbols_list)} symbols...")
print(f"📋 Symbols: {symbols_list}")

klines_data = extract_klines(symbols_list, months_back=2)

# Thống kê kết quả
print("\n" + "=" * 50)
print(f"✅ Extraction completed!")
print(f"📊 Successfully downloaded: {len(klines_data)}/{len(symbols_list)} symbols")

total_records = sum(len(df) for df in klines_data.values())
print(f"📈 Total records: {total_records:,}")