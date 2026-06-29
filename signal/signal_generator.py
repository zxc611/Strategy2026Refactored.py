# MODULE_ID: M1-239
"""signal_generator.py - 已合并至 signal_service.py（五唯一性修复）
本文件保留为薄重导出层，维持外部API兼容。

历史职责（Phase1-Sprint1）：从signal_service.py的SignalService.generate_signal
拆分为7个独立过滤器方法 + SignalContext数据类。现已反向合并回signal_service.py，
消除间接调用开销。signal_generator.py仅保留为重导出垫片。

规范源：ali2026v3_trading.signal.signal_service
"""
from __future__ import annotations

from ali2026v3_trading.signal.signal_service import (
    SignalContext,
    SignalGenerator,
)

__all__ = ['SignalContext', 'SignalGenerator']
