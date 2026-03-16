"""Symbol registry for the Crypto Data Pipeline.

Top 50 coins by market cap (excluding stablecoins).
All modules MUST import symbol lists from this file.

Single source of truth: SYMBOL_REGISTRY.
Derived views (SYMBOLS, SYMBOLS_STATUS, etc.) are generated automatically.
To add/remove a coin, edit SYMBOL_REGISTRY only.
"""

# --- Symbol Registry ---------------------------------------------------------
# Format: { symbol: { status, break_date? } }
#   status     : "TRADING" | "BREAK"
#   break_date : "YYYY-MM-DD" (last trading day, only for BREAK symbols)

SYMBOL_REGISTRY: dict[str, dict] = {
    "BTCUSDT":    {"status": "TRADING"},
    "ETHUSDT":    {"status": "TRADING"},
    "BNBUSDT":    {"status": "TRADING"},
    "SOLUSDT":    {"status": "TRADING"},
    "XRPUSDT":    {"status": "TRADING"},
    "DOGEUSDT":   {"status": "TRADING"},
    "ADAUSDT":    {"status": "TRADING"},
    "TRXUSDT":    {"status": "TRADING"},
    "LINKUSDT":   {"status": "TRADING"},
    "MATICUSDT":  {"status": "BREAK", "break_date": "2024-09-10"},   # Migrated to POL
    "AVAXUSDT":   {"status": "TRADING"},
    "TONUSDT":    {"status": "TRADING"},
    "SHIBUSDT":   {"status": "TRADING"},
    "XLMUSDT":    {"status": "TRADING"},
    "BCHUSDT":    {"status": "TRADING"},
    "DOTUSDT":    {"status": "TRADING"},
    "UNIUSDT":    {"status": "TRADING"},
    "LTCUSDT":    {"status": "TRADING"},
    "HBARUSDT":   {"status": "TRADING"},
    "PEPEUSDT":   {"status": "TRADING"},
    "NEARUSDT":   {"status": "TRADING"},
    "APTUSDT":    {"status": "TRADING"},
    "ICPUSDT":    {"status": "TRADING"},
    "ETCUSDT":    {"status": "TRADING"},
    "STXUSDT":    {"status": "TRADING"},
    "RENDERUSDT": {"status": "TRADING"},
    "CROUSDT":    {"status": "BREAK", "break_date": "2023-10-04"},   # Delisted
    "ATOMUSDT":   {"status": "TRADING"},
    "VETUSDT":    {"status": "TRADING"},
    "ARBUSDT":    {"status": "TRADING"},
    "INJUSDT":    {"status": "TRADING"},
    "IMXUSDT":    {"status": "TRADING"},
    "OPUSDT":     {"status": "TRADING"},
    "GRTUSDT":    {"status": "TRADING"},
    "THETAUSDT":  {"status": "TRADING"},
    "FILUSDT":    {"status": "TRADING"},
    "ARUSDT":     {"status": "TRADING"},
    "MKRUSDT":    {"status": "BREAK", "break_date": "2024-11-21"},   # Migrated to SKY
    "WIFUSDT":    {"status": "TRADING"},
    "RUNEUSDT":   {"status": "TRADING"},
    "FTMUSDT":    {"status": "BREAK", "break_date": "2025-03-20"},   # Migrated to Sonic (S)
    "ALGOUSDT":   {"status": "TRADING"},
    "FLOWUSDT":   {"status": "TRADING"},
    "XTZUSDT":    {"status": "TRADING"},
    "AXSUSDT":    {"status": "TRADING"},
    "SANDUSDT":   {"status": "TRADING"},
    "MANAUSDT":   {"status": "TRADING"},
    "NEOUSDT":    {"status": "TRADING"},
    "EOSUSDT":    {"status": "BREAK", "break_date": "2025-05-27"},   # Delisted
    "AAVEUSDT":   {"status": "TRADING"},
}

# --- Derived Views (backward-compatible) -------------------------------------

SYMBOLS: list[str] = list(SYMBOL_REGISTRY.keys())

SYMBOLS_STATUS: dict[str, str] = {
    s: info["status"] for s, info in SYMBOL_REGISTRY.items()
}

BREAK_DATES: dict[str, str] = {
    s: info["break_date"]
    for s, info in SYMBOL_REGISTRY.items()
    if "break_date" in info
}

ACTIVE_SYMBOLS: list[str] = [
    s for s, info in SYMBOL_REGISTRY.items()
    if info["status"] == "TRADING"
]
