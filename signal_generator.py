"""signal_generator.py - 重导出模块(P0-S2反向合并后)
SignalGenerator/SignalContext/SignalState/SIGNAL_STATE_TRANSITIONS已合并入signal_service.py
此文件仅保留重导出以维持外部API兼容性。
"""
from ali2026v3_trading.signal_service import (
    SignalGenerator,
    SignalContext,
    SignalState,
    SIGNAL_STATE_TRANSITIONS,
    SIGNAL_SERVICE_CONSTANTS,
)

__all__ = [
    'SignalGenerator',
    'SignalContext',
    'SignalState',
    'SIGNAL_STATE_TRANSITIONS',
    'SIGNAL_SERVICE_CONSTANTS',
]
