'''
Author: yaojinxi 864554492@qq.com
Date: 2026-04-26 14:28:46
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2026-04-26 14:32:37
FilePath: \backend\app\strategies\__init__.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
# app/strategies/__init__.py
"""
策略注册中心。

自动扫描当前目录下所有 .py 文件（排除 __init__.py），
每个文件必须包含:
    - CONFIG: dict  策略元信息（name, label, description, params）
    - generate_signals(df, params) -> list[dict]  信号生成函数

使用方式:
    from app.strategies import get_strategy, list_strategies

    # 获取单个策略
    strategy = get_strategy("ma_cross")
    signals = strategy["fn"](df, params)

    # 列出所有可用策略（给前端 /api/strategies 用）
    all_strategies = list_strategies()
"""

import importlib
import pkgutil
import os

# 策略注册表: { "ma_cross": {"config": {...}, "fn": generate_signals}, ... }
_registry: dict = {}


def _discover():
    """扫描当前包下所有模块，自动注册策略。"""
    package_dir = os.path.dirname(__file__)

    for finder, module_name, is_pkg in pkgutil.iter_modules([package_dir]):
        if module_name.startswith("_"):
            continue

        module = importlib.import_module(f".{module_name}", package=__name__)

        # 校验：必须有 CONFIG 和 generate_signals
        config = getattr(module, "CONFIG", None)
        fn = getattr(module, "generate_signals", None)

        if config is None or fn is None:
            print(f"⚠️ 策略模块 {module_name} 缺少 CONFIG 或 generate_signals，跳过")
            continue

        name = config.get("name", module_name)
        _registry[name] = {
            "config": config,
            "fn": fn,
        }

    print(f"📦 已加载 {len(_registry)} 个策略: {', '.join(_registry.keys())}")


def get_strategy(name: str) -> dict | None:
    """
    根据策略名获取策略。
    返回 {"config": {...}, "fn": generate_signals} 或 None。
    """
    return _registry.get(name)


def list_strategies() -> list[dict]:
    """
    返回所有已注册策略的 CONFIG 列表（不含函数引用），给前端用。
    """
    return [entry["config"] for entry in _registry.values()]


# 模块加载时自动扫描
_discover()