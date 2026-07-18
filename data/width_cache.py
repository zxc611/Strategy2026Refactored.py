# MODULE_ID: M1-053
"""
width_cache.py — 宽度强度缓存门面（Facade组合，消灭Mixin继承）

重构说明 (2026-06-11):
- WidthStrengthCache 不再继承5个Mixin，改为组合持有5个Service实例
- WidthCacheStateService作为主Service（含__init__），其余通过__getattr__委托
- 子服务通过反向__getattr__访问门面属性

合并说明 (2026-06-30):
- width_cache_context_mixin / diagnosis_mixin / sort_mixin 合入 width_cache_types.py
- 本文件改为从 width_cache_types 导入
"""
from data.width_cache_types import (
    _NoOpDiagnosisProbeManager,
    SortEntry,
    MAX_OPTION_CACHE_SIZE,
    MAX_INSTRUMENT_ID_MAP_SIZE,
    MAX_FUTURE_CACHE_SIZE,
    MAX_STATUS_COUNTS_SIZE,
    MAX_SORT_BUCKETS_FUTURES,
    _CACHE_TTL_SECONDS,
    WidthCacheContextMixin,
    _WidthCacheContextMixin,
    WidthCacheDiagnosisMixin,
    _WidthCacheDiagnosisMixin,
    WidthCacheSortService,
    _WidthCacheSortMixin,
)
from data.width_cache_state_mixin import WidthCacheStateService, _WidthCacheStateMixin
from data.width_cache_query_mixin import WidthCacheQueryService, _WidthCacheQueryMixin


class WidthStrengthCache(WidthCacheStateService):
    """宽度强度缓存 — Facade组合（消灭Mixin继承）

    继承WidthCacheStateService以保留旧__init__签名和数据属性，
    其余4个Mixin方法通过__getattr__委托到子Service实例。
    子Service实例通过_facade反向引用访问门面属性。
    """

    def __init__(self, tracked_option_tick_ids=None, params_service=None, strategy_id=None):
        super().__init__(tracked_option_tick_ids, params_service, strategy_id)
        self._context_service = WidthCacheContextMixin()
        self._context_service._facade = self
        self._sort_service = WidthCacheSortService()
        self._sort_service._facade = self
        self._query_service = WidthCacheQueryService()
        self._query_service._facade = self
        self._diagnosis_service = WidthCacheDiagnosisMixin()
        self._diagnosis_service._facade = self

    def __getattr__(self, name):
        for svc_attr in ('_context_service', '_sort_service',
                         '_query_service', '_diagnosis_service'):
            svc = self.__dict__.get(svc_attr)
            if svc is not None:
                for klass in type(svc).__mro__:
                    if name in klass.__dict__:
                        attr = klass.__dict__[name]
                        if hasattr(attr, '__get__'):
                            return attr.__get__(svc, type(svc))
                        return attr
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


_width_cache_instance = None

def get_width_cache(**kwargs):
    global _width_cache_instance
    if _width_cache_instance is None:
        _width_cache_instance = WidthStrengthCache(**kwargs)
    return _width_cache_instance


__all__ = [
    'WidthStrengthCache',
    '_NoOpDiagnosisProbeManager',
    'SortEntry',
    'MAX_OPTION_CACHE_SIZE',
    'MAX_INSTRUMENT_ID_MAP_SIZE',
    'MAX_FUTURE_CACHE_SIZE',
    'MAX_STATUS_COUNTS_SIZE',
    'MAX_SORT_BUCKETS_FUTURES',
    '_CACHE_TTL_SECONDS',
    'get_width_cache',
]
