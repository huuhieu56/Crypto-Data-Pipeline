# =============================================================================
# Symbols Configuration - Danh sach 50 Coins
# =============================================================================
# Top 50 coins theo von hoa thi truong, loai bo stablecoins.
# Tat ca module can danh sach symbols PHAI import tu day.
# =============================================================================

SYMBOLS: list[str] = [
    # 1-10
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "TRXUSDT",
    "LINKUSDT",
    "MATICUSDT",
    # 11-20
    "AVAXUSDT",
    "TONUSDT",
    "SHIBUSDT",
    "XLMUSDT",
    "BCHUSDT",
    "DOTUSDT",
    "UNIUSDT",
    "LTCUSDT",
    "HBARUSDT",
    "PEPEUSDT",
    # 21-30
    "NEARUSDT",
    "APTUSDT",
    "ICPUSDT",
    "ETCUSDT",
    "STXUSDT",
    "RENDERUSDT",
    "CROUSDT",
    "ATOMUSDT",
    "VETUSDT",
    "ARBUSDT",
    # 31-40
    "INJUSDT",
    "IMXUSDT",
    "OPUSDT",
    "GRTUSDT",
    "THETAUSDT",
    "FILUSDT",
    "ARUSDT",
    "MKRUSDT",
    "WIFUSDT",
    "RUNEUSDT",
    # 41-50
    "FTMUSDT",
    "ALGOUSDT",
    "FLOWUSDT",
    "XTZUSDT",
    "AXSUSDT",
    "SANDUSDT",
    "MANAUSDT",
    "NEOUSDT",
    "EOSUSDT",
    "AAVEUSDT",
]

# ---------------------------------------------------------------------------
# Trang thai giao dich cua tung symbol (dong bo tu symbols.csv / exchangeInfo)
# TRADING = dang hoat dong binh thuong
# BREAK   = tam ngung hoac bi huy niem yet
# ---------------------------------------------------------------------------
SYMBOLS_STATUS: dict[str, str] = {
    "BTCUSDT": "TRADING",
    "ETHUSDT": "TRADING",
    "BNBUSDT": "TRADING",
    "SOLUSDT": "TRADING",
    "XRPUSDT": "TRADING",
    "DOGEUSDT": "TRADING",
    "ADAUSDT": "TRADING",
    "TRXUSDT": "TRADING",
    "LINKUSDT": "TRADING",
    "MATICUSDT": "BREAK",
    "AVAXUSDT": "TRADING",
    "TONUSDT": "TRADING",
    "SHIBUSDT": "TRADING",
    "XLMUSDT": "TRADING",
    "BCHUSDT": "TRADING",
    "DOTUSDT": "TRADING",
    "UNIUSDT": "TRADING",
    "LTCUSDT": "TRADING",
    "HBARUSDT": "TRADING",
    "PEPEUSDT": "TRADING",
    "NEARUSDT": "TRADING",
    "APTUSDT": "TRADING",
    "ICPUSDT": "TRADING",
    "ETCUSDT": "TRADING",
    "STXUSDT": "TRADING",
    "RENDERUSDT": "TRADING",
    "CROUSDT": "TRADING",
    "ATOMUSDT": "TRADING",
    "VETUSDT": "TRADING",
    "ARBUSDT": "TRADING",
    "INJUSDT": "TRADING",
    "IMXUSDT": "TRADING",
    "OPUSDT": "TRADING",
    "GRTUSDT": "TRADING",
    "THETAUSDT": "TRADING",
    "FILUSDT": "TRADING",
    "ARUSDT": "TRADING",
    "MKRUSDT": "BREAK",
    "WIFUSDT": "TRADING",
    "RUNEUSDT": "TRADING",
    "FTMUSDT": "BREAK",
    "ALGOUSDT": "TRADING",
    "FLOWUSDT": "TRADING",
    "XTZUSDT": "TRADING",
    "AXSUSDT": "TRADING",
    "SANDUSDT": "TRADING",
    "MANAUSDT": "TRADING",
    "NEOUSDT": "TRADING",
    "EOSUSDT": "BREAK",
    "AAVEUSDT": "TRADING",
}

ACTIVE_SYMBOLS: list[str] = [
    s for s in SYMBOLS if SYMBOLS_STATUS.get(s) == "TRADING"
]
