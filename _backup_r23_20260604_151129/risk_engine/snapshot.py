from __future__ import annotations

import copy
import time
import logging
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone

@dataclass(frozen=True, slots=True)
class RiskSnapshot:
    snapshot_time: float
    signal: Dict[str, Any]
    action: str
    symbol: str
    instrument_id: str
    account_id: str
    amount: float
    price: float
    volume: float
    direction: str
    equity: float = 0.0
    market_price: float = 0.0
    avg_volume: float = 0.0
    days_to_expiry: int = 999
    hedge_type: str = 'speculation'
    bar_datetime: Optional[Any] = None
    is_valid: bool = True
    params: Optional[Any] = None
    strategy: Optional[Any] = None
    position_manager: Optional[Any] = None
    safety_meta_layer: Optional[Any] = None

    @classmethod
    def from_signal(cls, signal: Dict[str, Any], params: Any = None,
                    strategy: Any = None, position_manager: Any = None,
                    safety_meta_layer: Any = None) -> "RiskSnapshot":
        symbol = signal.get("symbol", "") or signal.get("instrument_id", "")
        return cls(
            snapshot_time=time.time(),
            signal=signal,
            action=signal.get("action", ""),
            symbol=symbol,
            instrument_id=signal.get("instrument_id", symbol),
            account_id=signal.get("account_id", "default"),
            amount=float(signal.get("amount", 0)),
            price=float(signal.get("price", signal.get("entry_price", 0))),
            volume=float(signal.get("volume", signal.get("amount", 1))),
            direction=signal.get("direction", ""),
            equity=float(signal.get("equity", 0)),
            market_price=float(signal.get("market_price", 0)),
            avg_volume=float(signal.get("avg_volume", 0)),
            days_to_expiry=int(signal.get("days_to_expiry", 999)),
            hedge_type=signal.get('hedge_type', 'speculation'),
            bar_datetime=signal.get('bar_datetime'),
            is_valid=signal.get("is_valid", True),
            params=params,
            strategy=strategy,
            position_manager=position_manager,
            safety_meta_layer=safety_meta_layer,
        )
