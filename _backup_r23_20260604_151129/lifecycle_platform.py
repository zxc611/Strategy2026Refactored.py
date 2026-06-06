"""
生命周期平台层 — 从strategy_lifecycle_mixin.py拆分
职责: 平台API绑定、订阅管理、行情推送
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional


class LifecyclePlatform:
    def __init__(self, provider):
        self._provider = provider
        self._platform_apis: Dict[str, Any] = {}
        self._subscribed_instruments: List[str] = []

    def bind_platform_apis(self, apis: Dict[str, Any]) -> None:
        self._platform_apis.update(apis)

    def get_platform_api(self, name: str) -> Optional[Any]:
        return self._platform_apis.get(name)

    def subscribe_instrument(self, instrument_id: str) -> None:
        if instrument_id not in self._subscribed_instruments:
            self._subscribed_instruments.append(instrument_id)

    def unsubscribe_instrument(self, instrument_id: str) -> None:
        if instrument_id in self._subscribed_instruments:
            self._subscribed_instruments.remove(instrument_id)

    def unsubscribe_all(self) -> None:
        self._subscribed_instruments.clear()

    @property
    def subscribed_instruments(self) -> List[str]:
        return list(self._subscribed_instruments)