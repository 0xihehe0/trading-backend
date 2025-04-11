'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 17:47:32
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-04-11 21:48:08
FilePath: \backend\fetch_data.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import yfinance as yf
import pandas as pd
import json
import time
from datetime import datetime

# 🔁 替换为你下载好的成分股列表
CSV_PATH = "data/sp500_symbols.csv"  # 包含 symbol 列

    # # 1. Wikipedia 上抓取所有 HTML 表格
    # tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")

    # # 2. 第一个表格就是成分股列表
    # sp500_df = tables[0]

    # # 3. 只取我们需要的两列：代码 & 公司名称
    # sp500_df = sp500_df[['Symbol', 'Security']]

    # # 4. 去掉可能存在的空格或特殊字符
    # sp500_df['Symbol'] = sp500_df['Symbol'].str.replace('.', '-', regex=False).str.strip()

    # # 5. 保存为 CSV 文件（供抓历史数据使用）
    # sp500_df.to_csv("data/sp500_symbols.csv", index=False)

    # print("✅ 已保存成分股列表到 data/sp500_symbols.csv")

def fetch_and_save():
    df_symbols = pd.read_csv(CSV_PATH)
    symbols = df_symbols['Symbol'].dropna().unique().tolist()

    all_data = {}
    for i, symbol in enumerate(symbols):
        print(f"[{i+1}/{len(symbols)}] Fetching: {symbol}")
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period='max', interval='1d')
            df.reset_index(inplace=True)

            records = [
                {"date": str(row['Date'].date()), "close": round(row['Close'], 2)}
                for _, row in df.iterrows() if not pd.isna(row['Close'])
            ]
            all_data[symbol] = records
            time.sleep(1.2)
        except Exception as e:
            print(f"❌ Failed for {symbol}: {e}")

    with open("data/sp500.json", "w") as f:
        json.dump(all_data, f)

    print("✅ 数据更新完成")
    print("🕒 更新时间：", datetime.now())

if __name__ == "__main__":
    fetch_and_save()
