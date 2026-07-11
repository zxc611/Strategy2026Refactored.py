# MODULE_ID: M2-293
"""P1-24: 统一测试桩 — 消除5个测试文件中RiskSnapshotStub重复定义"""
from __future__ import annotations
from typing import Any


class RiskSnapshotStub:
    """统一RiskSnapshotStub — 合并5个测试文件的变体"""
    def __init__(
        self,
        action: str = "OPEN",
        symbol: str = "IF2606",
        instrument_id: str = None,
        amount: float = 1.0,
        price: float = 4000.0,
        equity: float = 1000000.0,
        account_id: str = "test",
        hedge_type: str = "none",
        signal: dict = None,
        is_valid: bool = True,
        days_to_expiry: int = 30,
        bar_datetime: Any = None,
    ):
        self.action = action
        self.symbol = symbol
        self.instrument_id = instrument_id or symbol
        self.amount = amount
        self.price = price
        self.equity = equity
        self.account_id = account_id
        self.hedge_type = hedge_type
        self.signal = signal or {}
        self.is_valid = is_valid
        self.days_to_expiry = days_to_expiry
        self.bar_datetime = bar_datetime
