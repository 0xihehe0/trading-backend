'''
Author: yaojinxi 864554492@qq.com
Date: 2026-04-26 14:28:54
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2026-04-26 14:32:49
FilePath: \backend\app\strategies\ma_cross.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
# app/strategies/ma_cross.py
"""
均线交叉策略（MA Cross）

金叉买入，死叉卖出。
"""
import pandas as pd

# ===== 策略配置（前端可通过 /api/strategies 获取）=====
CONFIG = {
    "name": "ma_cross",
    "label": "均线交叉策略（MA Cross）",
    "description": "短期均线上穿长期均线时买入（金叉），下穿时卖出（死叉）。",
    "params": [
        {
            "key": "short_ma",
            "label": "短期均线",
            "type": "int",
            "default": 50,
            "min": 5,
            "max": 100,
        },
        {
            "key": "long_ma",
            "label": "长期均线",
            "type": "int",
            "default": 200,
            "min": 20,
            "max": 500,
        },
    ],
}


def generate_signals(df: pd.DataFrame, params: dict) -> list[dict]:
    """
    统一信号接口。

    参数:
        df: 必须包含 'date'(datetime) 和 'close' 列
        params: {"short_ma": int, "long_ma": int}

    返回:
        [{"type": "buy"/"sell", "date": "YYYY-MM-DD", "price": float}, ...]
    """
    short = params.get("short_ma", CONFIG["params"][0]["default"])
    long_ = params.get("long_ma", CONFIG["params"][1]["default"])

    df = df.copy()
    df["ma_short"] = df["close"].rolling(short).mean()
    df["ma_long"] = df["close"].rolling(long_).mean()
    df.dropna(inplace=True)

    if len(df) < 2:
        return []

    signals = []
    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        if prev["ma_short"] < prev["ma_long"] and curr["ma_short"] > curr["ma_long"]:
            signals.append({
                "type": "buy",
                "date": str(curr["date"].date()),
                "price": round(curr["close"], 2),
            })
        elif prev["ma_short"] > prev["ma_long"] and curr["ma_short"] < curr["ma_long"]:
            signals.append({
                "type": "sell",
                "date": str(curr["date"].date()),
                "price": round(curr["close"], 2),
            })

    return signals