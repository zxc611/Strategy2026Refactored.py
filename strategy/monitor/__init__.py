# -*- coding: utf-8 -*-
"""S5套利/S6做市商策略实盘模拟监控包。

子模块：
    - arbitrage_monitor: 套利监控（配对交易/动态hedge ratio/协整检验/价差收敛）
    - market_making_monitor: 做市商行为监控（理论价格/双边报价/inventory/毒单检测）
"""
from __future__ import annotations

# 延迟导入，避免包初始化阶段触发重依赖
# 使用方按需 `from ali2026v3_trading.strategy.monitor.market_making_monitor import MarketMakingMonitor`
