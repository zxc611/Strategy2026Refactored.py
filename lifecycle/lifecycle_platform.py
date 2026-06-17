# MODULE_ID: M1-126
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""
生命周期平台层 — 从strategy_lifecycle_mixin.py拆分
职责: 平台API绑定、订阅管理、行情推送、API就绪检查、降级处理
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Set


class LifecyclePlatform:
    def __init__(self, provider):
        self._provider = provider
        self._platform_apis: Dict[str, Any] = {}
        self._subscribed_instruments: List[str] = []
        self._api_ready: bool = False
        self._kline_ready: bool = False
        self._degraded: bool = False
        self._degraded_reasons: List[str] = []

    def bind_platform_apis(self, apis: Dict[str, Any]) -> None:
        self._platform_apis.update(apis)
        self._api_ready = (
            callable(self._platform_apis.get('subscribe')) and
            callable(self._platform_apis.get('unsubscribe'))
        )
        self._kline_ready = callable(self._platform_apis.get('get_kline'))

    def get_platform_api(self, name: str) -> Optional[Any]:
        return self._platform_apis.get(name)

    def subscribe_instrument(self, instrument_id: str) -> None:
        if instrument_id not in self._subscribed_instruments:
            self._subscribed_instruments.append(instrument_id)
            subscribe_fn = self._platform_apis.get('subscribe')
            if callable(subscribe_fn):
                try:
                    subscribe_fn(instrument_id)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.warning("[LifecyclePlatform] subscribe失败: %s err=%s", instrument_id, e)

    def unsubscribe_instrument(self, instrument_id: str) -> None:
        if instrument_id in self._subscribed_instruments:
            self._subscribed_instruments.remove(instrument_id)
            unsubscribe_fn = self._platform_apis.get('unsubscribe')
            if callable(unsubscribe_fn):
                try:
                    unsubscribe_fn(instrument_id)
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.warning("[LifecyclePlatform] unsubscribe失败: %s err=%s", instrument_id, e)

    def unsubscribe_all(self) -> None:
        unsubscribe_fn = self._platform_apis.get('unsubscribe')
        for instrument_id in list(self._subscribed_instruments):
            if callable(unsubscribe_fn):
                try:
                    unsubscribe_fn(instrument_id)
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[R3-L2] suppressed exception", exc_info=True)
                    pass
                    pass
        self._subscribed_instruments.clear()

    @property
    def subscribed_instruments(self) -> List[str]:
        return list(self._subscribed_instruments)

    @property
    def api_ready(self) -> bool:
        return self._api_ready

    @property
    def kline_ready(self) -> bool:
        return self._kline_ready

    @property
    def is_degraded(self) -> bool:
        return self._degraded

    def mark_degraded(self, reason: str) -> None:
        self._degraded = True
        if reason not in self._degraded_reasons:
            self._degraded_reasons.append(reason)
            logging.warning("[LifecyclePlatform] 降级: %s", reason)

    def clear_degraded(self) -> None:
        self._degraded = False
        self._degraded_reasons.clear()

    @property
    def degraded_reasons(self) -> List[str]:
        return list(self._degraded_reasons)

    def check_api_health(self) -> Dict[str, bool]:
        result = {
            'subscribe': callable(self._platform_apis.get('subscribe')),
            'unsubscribe': callable(self._platform_apis.get('unsubscribe')),
            'get_kline': callable(self._platform_apis.get('get_kline')),
            'get_instrument': self._platform_apis.get('get_instrument') is not None,
            'insert_order': self._platform_apis.get('insert_order') is not None,
            'cancel_order': self._platform_apis.get('cancel_order') is not None,
        }
        self._api_ready = result['subscribe'] and result['unsubscribe']
        self._kline_ready = result['get_kline']
        return result
