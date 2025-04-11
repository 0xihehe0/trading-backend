'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 21:25:55
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-04-08 21:26:03
FilePath: \backend\app\services\signal_ma_cross.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import pandas as pd
from datetime import datetime, timedelta

def ma_cross_strategy(df, short=50, long=200):
    df['ma_short'] = df['close'].rolling(short).mean()
    df['ma_long'] = df['close'].rolling(long).mean()
    df.dropna(inplace=True)

    signals = []
    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        if prev['ma_short'] < prev['ma_long'] and curr['ma_short'] > curr['ma_long']:
            signals.append({"type": "buy", "date": str(curr['date'].date()), "price": round(curr['close'], 2)})
        elif prev['ma_short'] > prev['ma_long'] and curr['ma_short'] < curr['ma_long']:
            signals.append({"type": "sell", "date": str(curr['date'].date()), "price": round(curr['close'], 2)})

    return signals
