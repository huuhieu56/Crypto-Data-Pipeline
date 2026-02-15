# =============================================================================
# Symbols Configuration - Danh sach 50 Coins
# =============================================================================
# Top 50 coins theo von hoa thi truong, loai bo stablecoins.
# Tat ca module can danh sach symbols PHAI import tu day.
#
# SINGLE SOURCE OF TRUTH:
#   SYMBOL_REGISTRY — nested dict chua tat ca thong tin cua tung coin.
#   Cac bien SYMBOLS, SYMBOLS_STATUS, BREAK_DATES, ACTIVE_SYMBOLS duoc
#   derive tu registry de backward-compatible voi cac module khac.
#
# Them / sua coin → chi can sua SYMBOL_REGISTRY, khong can cap nhat 3 cho.
# =============================================================================

# ---------------------------------------------------------------------------
# Registry: { symbol: { status, break_date? } }
#   status     : "TRADING" | "BREAK"
#   break_date : "YYYY-MM-DD" — ngay cuoi cung giao dich (chi cho BREAK)
# ---------------------------------------------------------------------------
SYMBOL_REGISTRY: dict[str, dict] = {
    # 1-10
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
    # 11-20
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
    # 21-30
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
    # 31-40
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
    # 41-50
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

# ---------------------------------------------------------------------------
# Derived views — backward-compatible, KHONG can sua import o cac module khac
# ---------------------------------------------------------------------------
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
