# app/config.py
"""
全局共享配置。

数据分层：
    data/stocks/       ← 个股 (AAPL.json, MSFT.json ...)
    data/indices/      ← 指数 (^GSPC.json, ^VIX.json)
    data/sentiment/    ← 情绪/宏观 (FEAR_GREED.json)
"""

import os

# ===== 数据目录 =====
DATA_ROOT = "data"
STOCKS_DIR = os.path.join(DATA_ROOT, "stocks")
INDICES_DIR = os.path.join(DATA_ROOT, "indices")
SENTIMENT_DIR = os.path.join(DATA_ROOT, "sentiment")

# 情绪类 ticker 列表（非 OHLCV 格式）
SENTIMENT_TICKERS = {"FEAR_GREED"}

# 时间范围映射（"all" 不在此表中，表示不裁剪）
DATE_RANGE_OFFSET = {
    "1d":  {"days": 1},
    "5d":  {"days": 5},
    "1mo": {"months": 1},
    "6mo": {"months": 6},
    "1y":  {"years": 1},
    "2y":  {"years": 2},
    "5y":  {"years": 5},
    "10y": {"years": 10},
}


def get_data_path(ticker: str) -> str:
    """
    根据 ticker 自动路由到正确的数据目录，返回完整文件路径。

    规则:
        ^开头        → data/indices/
        FEAR_GREED等 → data/sentiment/
        其他         → data/stocks/
    """
    ticker = ticker.upper().strip()

    if ticker in SENTIMENT_TICKERS:
        return os.path.join(SENTIMENT_DIR, f"{ticker}.json")
    elif ticker.startswith("^"):
        return os.path.join(INDICES_DIR, f"{ticker}.json")
    else:
        return os.path.join(STOCKS_DIR, f"{ticker}.json")


# 向后兼容：旧代码中 DATA_DIR 的引用不会立刻报错
DATA_DIR = STOCKS_DIR
