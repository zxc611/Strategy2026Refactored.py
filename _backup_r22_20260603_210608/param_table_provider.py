"""
Phase3-Sprint8: ParamTableProvider — DEFAULT_PARAM_TABLE解耦
引入Protocol接口+依赖注入，消除14文件129次全局字典直接引用
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class ParamTableProvider(Protocol):
    """参数表提供者协议 — 替代全局DEFAULT_PARAM_TABLE直接引用"""

    def get_params(self, strategy_name: str) -> Dict[str, Any]:
        ...

    def get_default(self, key: str) -> Any:
        ...

    def list_strategies(self) -> List[str]:
        ...


class DefaultParamTableProvider:
    """默认参数表提供者 — 包装原有DEFAULT_PARAM_TABLE，向后兼容"""

    def __init__(self, param_table: Optional[Dict[str, Dict[str, Any]]] = None):
        self._table = param_table or {}
        self._lock = threading.RLock()

    def get_params(self, strategy_name: str) -> Dict[str, Any]:
        with self._lock:
            return dict(self._table.get(strategy_name, {}))

    def get_default(self, key: str) -> Any:
        with self._lock:
            for _strategy_params in self._table.values():
                if key in _strategy_params:
                    return _strategy_params[key]
        return None

    def list_strategies(self) -> List[str]:
        with self._lock:
            return list(self._table.keys())

    def update_param(self, strategy_name: str, key: str, value: Any) -> None:
        with self._lock:
            if strategy_name not in self._table:
                self._table[strategy_name] = {}
            self._table[strategy_name][key] = value


class CachedParamTableProvider:
    """带LRU缓存的参数表提供者 — 减少字典查找开销"""

    def __init__(self, provider: ParamTableProvider, cache_size: int = 128):
        self._provider = provider
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_size = cache_size
        self._lock = threading.RLock()

    def get_params(self, strategy_name: str) -> Dict[str, Any]:
        with self._lock:
            if strategy_name in self._cache:
                return dict(self._cache[strategy_name])
            result = self._provider.get_params(strategy_name)
            if len(self._cache) >= self._cache_size:
                _oldest = next(iter(self._cache))
                del self._cache[_oldest]
            self._cache[strategy_name] = result
            return dict(result)

    def get_default(self, key: str) -> Any:
        return self._provider.get_default(key)

    def list_strategies(self) -> List[str]:
        return self._provider.list_strategies()

    def invalidate_cache(self, strategy_name: Optional[str] = None) -> None:
        with self._lock:
            if strategy_name:
                self._cache.pop(strategy_name, None)
            else:
                self._cache.clear()


_provider_instance: Optional[ParamTableProvider] = None
_provider_lock = threading.Lock()


def get_param_table_provider() -> ParamTableProvider:
    """获取全局ParamTableProvider单例"""
    global _provider_instance
    if _provider_instance is None:
        with _provider_lock:
            if _provider_instance is None:
                try:
                    from ali2026v3_trading.config_params import DEFAULT_PARAM_TABLE
                    _provider_instance = CachedParamTableProvider(
                        DefaultParamTableProvider(DEFAULT_PARAM_TABLE)
                    )
                except ImportError:
                    _provider_instance = DefaultParamTableProvider()
                    logging.warning("[ParamTableProvider] config_params不可用,使用空Provider")
    return _provider_instance


def set_param_table_provider(provider: ParamTableProvider) -> None:
    """设置全局ParamTableProvider(用于依赖注入/测试)"""
    global _provider_instance
    with _provider_lock:
        _provider_instance = provider