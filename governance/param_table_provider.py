# MODULE_ID: M1-076
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""
Phase3-Sprint8: ParamTableProvider — DEFAULT_PARAM_TABLE解耦
引入Protocol接口+依赖注入，消除14文件129次全局字典直接引用
"""
from __future__ import annotations

import logging
import threading
import functools
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable
from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads


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
    """默认参数表提供者 — 包装原有DEFAULT_PARAM_TABLE，向后兼容

    DEFAULT_PARAM_TABLE是扁平字典(key→value)，不是嵌套字典(strategy→{key→value})。
    将整个表视为单个策略'_default'的参数集。'
    """

    _DEFAULT_STRATEGY = '_default'

    def __init__(self, param_table: Optional[Dict[str, Any]] = None):
        self._table = param_table or {}
        self._lock = threading.RLock()
        self._is_nested = self._detect_nested()

    def _detect_nested(self) -> bool:
        if not self._table:
            return False
        first_val = next(iter(self._table.values()))
        return isinstance(first_val, dict)

    def get_params(self, strategy_name: str) -> Dict[str, Any]:
        with self._lock:
            if self._is_nested:
                return dict(dict.get(self._table, strategy_name, {}))
            return dict(self._table)

    def get_default(self, key: str) -> Any:
        with self._lock:
            if self._is_nested:
                for _strategy_params in dict.values(self._table):
                    if isinstance(_strategy_params, dict) and dict.__contains__(_strategy_params, key):
                        return dict.__getitem__(_strategy_params, key)
            else:
                if dict.__contains__(self._table, key):
                    return dict.__getitem__(self._table, key)
        return None

    def list_strategies(self) -> List[str]:
        with self._lock:
            if self._is_nested:
                return list(dict.keys(self._table))
            return [self._DEFAULT_STRATEGY]

    def update_param(self, strategy_name: str, key: str, value: Any) -> None:
        with self._lock:
            if not dict.__contains__(self._table, strategy_name):
                dict.__setitem__(self._table, strategy_name, {})
            dict.__getitem__(self._table, strategy_name)[key] = value


class CachedParamTableProvider:
    """带LRU缓存的参数表提供者 — P1-41修复: 委托到functools.lru_cache统一缓存策略"""

    def __init__(self, provider: ParamTableProvider, cache_size: int = 128):
        self._provider = provider
        self._cache_size = cache_size
        self._lock = threading.RLock()
        # P1-41修复: 使用functools.lru_cache替代手动LRU实现
        @functools.lru_cache(maxsize=cache_size)
        def _cached_get_params(strategy_name: str) -> str:
            """lru_cache要求参数可hash，返回JSON字符串以支持不可序列化对象"""
            result = self._provider.get_params(strategy_name)
            return json_dumps(result, sort_keys=True)
        self._cached_get_params = _cached_get_params

    def get_params(self, strategy_name: str) -> Dict[str, Any]:
        # P1-41修复: 委托到lru_cache统一缓存
        try:
            cached_json = self._cached_get_params(strategy_name)
            return json_loads(cached_json)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            # fallback到直接查询
            return dict(self._provider.get_params(strategy_name))

    def get_default(self, key: str) -> Any:
        return self._provider.get_default(key)

    def list_strategies(self) -> List[str]:
        return self._provider.list_strategies()

    def invalidate_cache(self, strategy_name: Optional[str] = None) -> None:
        # P1-41修复: 使用lru_cache的标准清除API
        if strategy_name:
            self._cached_get_params.cache_clear()
        else:
            self._cached_get_params.cache_clear()


_provider_instance: Optional[ParamTableProvider] = None
_provider_lock = threading.Lock()


def get_param_table_provider() -> ParamTableProvider:
    """获取全局ParamTableProvider单例"""
    global _provider_instance
    if _provider_instance is None:
        with _provider_lock:
            if _provider_instance is None:
                try:
                    from ali2026v3_trading.config.config_params import DEFAULT_PARAM_TABLE
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