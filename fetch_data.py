#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
拉取 Polygon 的逐笔成交(trades)数据，支持时间范围、自动分页、限流退避，保存到本地。
用法示例：
  export POLYGON_API_KEY="你的key"
  python polygon_trades.py --ticker AAPL --limit 1000 --start "2025-08-01T13:30:00Z" --end "2025-08-01T14:00:00Z" --csv aapl_trades.csv
"""

import os
import time
import argparse
from typing import Optional, List
from datetime import datetime, timezone

from polygon import RESTClient
from polygon.exceptions import BadResponse

def parse_iso_to_ns(s: Optional[str]) -> Optional[int]:
    """
    将 ISO8601 时间（如 2025-08-01T13:30:00Z）转为纳秒时间戳（Polygon v3 参数需要）。
    允许传 None。
    """
    if not s:
        return None
    # 兼容不带Z的串
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)

def fetch_trades(
    client: RESTClient,
    ticker: str,
    limit: int = 1000,
    order: str = "asc",
    sort: str = "timestamp",
    start_iso: Optional[str] = None,
    end_iso: Optional[str] = None,
    max_pages: int = 100,
    backoff_base: float = 0.5,
) -> List[dict]:
    """
    逐页拉取 trades，自动分页，遇到429等做指数退避。
    返回 list[dict]（已转为普通 dict，便于后续保存）。
    """
    ts_gte = parse_iso_to_ns(start_iso)
    ts_lt  = parse_iso_to_ns(end_iso)

    params = {
        "ticker": ticker,
        "order": order,
        "sort": sort,
        "limit": min(max(limit, 1), 50000),  # SDK/服务端会再做上限校验
    }
    # 只有传了才加，避免 SDK 不同版本的签名冲突
    if ts_gte is not None:
        params["timestamp_gte"] = ts_gte
