# MODULE_ID: M1-042
"""
storage_core.py — 存储核心门面模块（Facade组合，消灭Mixin继承）

重构说明 (2026-06-11):
- _StorageCoreMixin 不再继承5个Mixin，改为组合持有5个Service实例
- 通过 __getattr__ 委托到子服务
- 子服务通过反向引用访问门面属性
- 向后兼容别名保留
"""

from ali2026v3_trading.data.storage_init_mixin import StorageInitService, _StorageInitMixin, _get_default_db_path
from ali2026v3_trading.data.storage_async_writer_mixin import StorageAsyncWriterService, _StorageAsyncWriterMixin
from ali2026v3_trading.data.storage_data_write_mixin import StorageDataWriteService, _StorageDataWriteMixin
from ali2026v3_trading.data.storage_snapshot_mixin import StorageSnapshotService, _StorageSnapshotMixin
from ali2026v3_trading.data.storage_lifecycle_mixin import StorageLifecycleService, _StorageLifecycleMixin


class _StorageCoreMixin:
    """存储核心门面 — 组合5个Service实例，消灭Mixin继承

    子服务通过 _facade 反向引用访问门面的属性（_lock, _params_service等）
    """

    def __getattr__(self, name):
        for svc_attr in ('_init_service', '_async_writer_service',
                         '_data_write_service', '_snapshot_service',
                         '_lifecycle_service'):
            svc = self.__dict__.get(svc_attr)
            if svc is not None:
                try:
                    return getattr(svc, name)
                except AttributeError:
                    continue
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


__all__ = ['_StorageCoreMixin', '_get_default_db_path']
