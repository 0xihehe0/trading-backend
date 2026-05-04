#!/usr/bin/env python3
"""
获取 CNN Fear & Greed 指数历史数据。

用法:
    python fetch_fear_greed.py

数据保存到 data/sentiment/FEAR_GREED.json
格式: [{"date": "2021-01-04", "value": 65.2, "label": "Greed"}, ...]
"""

import json
import os
import requests
from datetime import datetime

# ====== 配置 ======
OUTPUT_DIR = r"D:\trading\backend\data\sentiment"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "FEAR_GREED.json")
# ==================

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


def get_label(value):
    if value <= 25:
        return "Extreme Fear"
    elif value <= 45:
        return "Fear"
    elif value <= 55:
        return "Neutral"
    elif value <= 75:
        return "Greed"
    else:
        return "Extreme Greed"


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # CNN 接口：从指定日期到今天
    start_date = "2021-01-01"
    url = f"https://production.dataviz.cnn.io/index/fearandgreed/graphdata/{start_date}"

    print(f"📊 获取 CNN Fear & Greed 指数")
    print(f"📡 请求: {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        print(f"📬 状态码: {resp.status_code}")

        if resp.status_code != 200:
            print(f"❌ 请求失败，状态码: {resp.status_code}")
            print(f"   响应内容: {resp.text[:500]}")
            return

        data = resp.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ 网络请求失败: {e}")
        return
    except json.JSONDecodeError as e:
        print(f"❌ JSON 解析失败: {e}")
        print(f"   响应内容: {resp.text[:500]}")
        return

    # 打印返回的数据结构（调试用）
    print(f"📦 返回数据的顶层 key: {list(data.keys())}")

    # 尝试获取历史数据
    fg_historical = data.get("fear_and_greed_historical", {})
    fg_data = fg_historical.get("data", [])

    if not fg_data:
        print(f"⚠️  fear_and_greed_historical.data 为空")
        print(f"   fear_and_greed_historical 内容: {json.dumps(fg_historical, indent=2)[:500]}")

        # 尝试备用路径
        print(f"\n🔍 尝试其他数据路径...")
        for key in data.keys():
            val = data[key]
            if isinstance(val, dict) and "data" in val:
                print(f"   找到 '{key}.data'，长度: {len(val['data'])}")
            elif isinstance(val, list):
                print(f"   找到 '{key}'，列表长度: {len(val)}")
            else:
                print(f"   '{key}': {type(val).__name__}")
        return

    print(f"✅ 获取到 {len(fg_data)} 条数据")

    # 解析
    records = []
    seen = set()
    for item in fg_data:
        ts = item.get("x", 0)
        value = item.get("y", 0)

        date_str = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")

        if date_str not in seen:
            seen.add(date_str)
            records.append({
                "date": date_str,
                "value": round(float(value), 1),
                "label": get_label(value),
            })

    records.sort(key=lambda x: x["date"])

    # 写入
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)

    print(f"\n✅ 完成！共 {len(records)} 条记录")
    print(f"📅 {records[0]['date']} ~ {records[-1]['date']}")
    print(f"📈 最新: {records[-1]['date']} → {records[-1]['value']} ({records[-1]['label']})")
    print(f"💾 保存到: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
