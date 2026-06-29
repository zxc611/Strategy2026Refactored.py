# MODULE_ID: M1-046
"""
storage_query.py — StorageQuery (Facade组合，消灭Mixin继承)
READ + HELPER + INSTRUMENT + SCHEMA 方法组

拆分说明 (2026-06-10):
- storage_query_base.py: StorageQueryBaseService (基础工具方法 + 数据验证 + 交易所映射)
- storage_query_instrument.py: StorageInstrumentService (合约信息查询 + 注册/删除/批量 + 期权链)
- storage_query_history.py: StorageHistoryService (历史K线加载 + Schema迁移)
- 本文件: StorageQuery 组合持有3个Service实例，re-export

重构说明 (2026-06-11):
- 消灭Mixin继承，改为Facade组合模式
- StorageQuery 持有 base_service / instrument_service / history_service
- 通过 __getattr__ 委托到子服务
"""

import logging

from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService, _StorageQueryBaseMixin
from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService, _StorageQueryInstrumentMixin
from ali2026v3_trading.data.storage_query_history import StorageHistoryService, _StorageQueryHistoryMixin


logger = logging.getLogger(__name__)


class StorageQuery:
    """组合所有查询相关 Service 的统一类（Facade组合，消灭Mixin继承）"""

    def __init__(self, lock, params_service, data_service, maintenance_service=None):
        self._base_service = StorageQueryBaseService(lock, params_service, data_service, maintenance_service)
        self._instrument_service = StorageInstrumentService(self._base_service)
        self._history_service = StorageHistoryService(self._base_service)

    def __getattr__(self, name):
        # 递归保护：避免访问未初始化属性时无限递归
        if name in ('_base_service', '_instrument_service', '_history_service'):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        for svc in (self.__dict__.get('_base_service'),
                    self.__dict__.get('_instrument_service'),
                    self.__dict__.get('_history_service')):
            if svc is None:
                continue
            try:
                return getattr(svc, name)
            except AttributeError:
                continue
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


# 向后兼容：裸实例测试可直接调用不依赖self状态的方法
for _method_name in ('_validate_tick', '_validate_kline', '_normalize_tick_fields', '_to_timestamp',
                     '_get_instrument_info', '_get_info_by_id', '_warn_runtime_unregistered'):
    if not hasattr(StorageQuery, _method_name):
        setattr(StorageQuery, _method_name, getattr(StorageQueryBaseService, _method_name))


_StorageQueryMixin = StorageQuery
