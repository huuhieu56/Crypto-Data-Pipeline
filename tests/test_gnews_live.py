"""Test thật GNews API — gọi API thật, in kết quả ra terminal.

Chạy: python tests/test_gnews_live.py

Script này KHÔNG cần Docker, MinIO, hay Airflow. Chỉ cần:
- File .env có GNEWS_API_KEY
- pip install requests python-dotenv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os
from dotenv import load_dotenv

# Load .env
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests

API_KEY = os.getenv("GNEWS_API_KEY", "")
SEARCH_URL = "https://gnews.io/api/v4/search"
QUERY = "cryptocurrency OR bitcoin OR binance OR ethereum"


def main():
    if not API_KEY:
        print("[ERROR] GNEWS_API_KEY chua duoc set trong .env")
        return

    print(f"[KEY] API Key: {API_KEY[:8]}...{API_KEY[-4:]}")
    print(f"[QUERY] {QUERY}")
    print(f"[INFO] Dang goi GNews API...")
    print("-" * 60)

    resp = requests.get(SEARCH_URL, params={
        "q": QUERY,
        "lang": "en",
        "max": 5,
        "apikey": API_KEY,
    }, timeout=30)

    if resp.status_code != 200:
        print(f"[ERROR] API Error: {resp.status_code}")
        print(resp.text)
        return

    data = resp.json()
    articles = data.get("articles", [])

    print(f"[OK] Thanh cong! Tim thay {data.get('totalArticles', 0)} bai tong cong")
    print(f"[OK] Tra ve {len(articles)} bai trong request nay:")
    print("=" * 60)

    for i, art in enumerate(articles, 1):
        source = art.get("source", {})
        # Encode safe for Windows terminal
        title = (art.get('title') or 'N/A').encode('ascii', 'replace').decode()
        desc = ((art.get('description') or 'N/A')[:100]).encode('ascii', 'replace').decode()
        src = (source.get('name') or 'N/A').encode('ascii', 'replace').decode()
        print(f"\n--- Bai {i} ---")
        print(f"  Title:       {title}")
        print(f"  Description: {desc}...")
        print(f"  Source:      {src}")
        print(f"  URL:         {art.get('url', 'N/A')}")
        print(f"  Published:   {art.get('publishedAt', 'N/A')}")

    print("\n" + "=" * 60)
    print("[OK] GNews API hoat dong binh thuong!")
    print("   Du lieu nay se duoc luu vao MinIO khi chay qua Docker/Airflow.")


if __name__ == "__main__":
    main()
