#!/usr/bin/env python3
"""
将 sp500_split/ 下的文件迁移到分层数据目录。

    data/sp500_split/AAPL.json      → data/stocks/AAPL.json
    data/sp500_split/^GSPC.json     → data/indices/^GSPC.json
    data/sp500_split/^VIX.json      → data/indices/^VIX.json
    data/sp500_split/FEAR_GREED.json→ data/sentiment/FEAR_GREED.json

用法:
    python migrate_data.py              # 预览（不实际移动）
    python migrate_data.py --execute    # 执行迁移
"""

import os
import shutil
import argparse
from pathlib import Path

# ====== 配置 ======
OLD_DIR = r"D:\trading\backend\data\sp500_split"
DATA_ROOT = r"D:\trading\backend\data"

STOCKS_DIR = os.path.join(DATA_ROOT, "stocks")
INDICES_DIR = os.path.join(DATA_ROOT, "indices")
SENTIMENT_DIR = os.path.join(DATA_ROOT, "sentiment")

SENTIMENT_TICKERS = {"FEAR_GREED"}
# ==================


def classify(filename: str) -> str:
    """判断文件应该去哪个目录。"""
    name = Path(filename).stem  # 去掉 .json
    if name in SENTIMENT_TICKERS:
        return SENTIMENT_DIR
    elif name.startswith("^"):
        return INDICES_DIR
    else:
        return STOCKS_DIR


def main():
    parser = argparse.ArgumentParser(description="迁移数据到分层目录")
    parser.add_argument("--execute", action="store_true", help="实际执行迁移（不加则只预览）")
    args = parser.parse_args()

    if not os.path.isdir(OLD_DIR):
        print(f"❌ 源目录不存在: {OLD_DIR}")
        return

    files = sorted(Path(OLD_DIR).glob("*.json"))
    print(f"📁 源目录: {OLD_DIR}")
    print(f"📊 共 {len(files)} 个文件\n")

    # 统计
    counts = {"stocks": 0, "indices": 0, "sentiment": 0}

    for f in files:
        dest_dir = classify(f.name)
        dest_path = os.path.join(dest_dir, f.name)

        category = os.path.basename(dest_dir)
        counts[category] = counts.get(category, 0) + 1

        if args.execute:
            os.makedirs(dest_dir, exist_ok=True)
            shutil.copy2(str(f), dest_path)

    print(f"📈 个股 (stocks):     {counts.get('stocks', 0)}")
    print(f"📊 指数 (indices):    {counts.get('indices', 0)}")
    print(f"💭 情绪 (sentiment):  {counts.get('sentiment', 0)}")

    if args.execute:
        print(f"\n✅ 迁移完成！文件已复制到新目录。")
        print(f"   原始目录 {OLD_DIR} 未删除，确认新目录无误后可手动删除。")
    else:
        print(f"\n⚠️  以上为预览模式，未实际移动文件。")
        print(f"   确认无误后运行: python migrate_data.py --execute")


if __name__ == "__main__":
    main()
