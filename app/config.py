# app/config.py
"""
全局共享配置，避免多处重复定义。
"""

# 数据目录
DATA_DIR = "data/sp500_split"

# 时间范围映射（"all" 不在此表中，表示不裁剪，返回全部数据）
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