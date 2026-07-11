# MODULE_ID: M1-042
"""
storage_core.py — 存储核心模块（Facade组合+查询+WAL合并）

合并说明 (2026-06-30):
- 原 storage_core.py: _StorageCoreMixin Facade组合
- 原 storage_query.py: StorageQuery Facade组合 ← 合入
- 原 storage_wal_mixin.py: StorageWalService WAL溢写 ← 合入
"""

import logging
import os
import threading
from typing import Any, Dict, List, Optional

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads

from ali2026v3_trading.data.storage_init_mixin import StorageInitService, _StorageInitMixin, _get_default_db_path
from ali2026v3_trading.data.storage_async_writer_mixin import StorageAsyncWriterService, _StorageAsyncWriterMixin
from ali2026v3_trading.data.storage_data_write_mixin import StorageDataWriteService, _StorageDataWriteMixin
from ali2026v3_trading.data.storage_snapshot_mixin import StorageSnapshotService, _StorageSnapshotMixin
from ali2026v3_trading.data.storage_lifecycle_mixin import StorageLifecycleService, _StorageLifecycleMixin

from ali2026v3_trading.data.storage_query_base import StorageQueryBaseService, _StorageQueryBaseMixin
from ali2026v3_trading.data.storage_query_instrument import StorageInstrumentService, _StorageQueryInstrumentMixin
from ali2026v3_trading.data.storage_query_history import StorageHistoryService, _StorageQueryHistoryMixin


# ============================================================================
# _StorageCoreMixin — 存储核心门面（组合5个Service实例）
# ============================================================================

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


# ============================================================================
# StorageQuery — 查询Facade组合（合入自 storage_query.py）
# ============================================================================

_logger = logging.getLogger(__name__)


class StorageQuery:
    """组合所有查询相关 Service 的统一类（Facade组合，消灭Mixin继承）"""

    def __init__(self, lock, params_service, data_service, maintenance_service=None):
        self._base_service = StorageQueryBaseService(lock, params_service, data_service, maintenance_service)
        self._instrument_service = StorageInstrumentService(self._base_service)
        self._history_service = StorageHistoryService(self._base_service)

    def __getattr__(self, name):
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


for _method_name in ('_validate_tick', '_validate_kline', '_normalize_tick_fields', '_to_timestamp',
                     '_get_instrument_info', '_get_info_by_id', '_warn_runtime_unregistered'):
    if not hasattr(StorageQuery, _method_name):
        setattr(StorageQuery, _method_name, getattr(StorageQueryBaseService, _method_name))

_StorageQueryMixin = StorageQuery


# ============================================================================
# StorageWalService — WAL溢写服务（合入自 storage_wal_mixin.py）
# ============================================================================

class StorageWalService:

    def __init__(self, facade=None):
        self._facade = facade

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def _restore_spill_wal(self) -> None:
        if not os.path.exists(self._spill_wal_path):
            return
        restored = 0
        try:
            with open(self._spill_wal_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        import json as _json
                        task = _json.loads(line)
                        if isinstance(task, list) and len(task) >= 2:
                            self._pending_on_stop_data.append(task)
                            restored += 1
                    except (ValueError, TypeError):
                        continue
            if restored > 0:
                logging.info("[SpillWAL] 从WAL恢复 %d 条spill数据: %s (WAL保留至replay成功后清除)", restored, self._spill_wal_path)
        except Exception as e:
            logging.warning("[SpillWAL] WAL恢复失败: %s", e)

    @staticmethod
    def _wal_serialize(task) -> List[Any]:
        wal_entry = [task[0] if len(task) > 0 else '']
        args_data = task[1] if len(task) > 1 else None
        try:
            json_dumps(args_data)
            wal_entry.append(args_data)
        except (TypeError, ValueError):
            wal_entry.append(str(args_data))
        kwargs_data = task[2] if len(task) > 2 else {}
        try:
            json_dumps(kwargs_data)
            wal_entry.append(kwargs_data)
        except (TypeError, ValueError):
            wal_entry.append(str(kwargs_data))
        return wal_entry

    def _spill_wal_append(self, task) -> None:
        try:
            wal_entry = self._wal_serialize(task)
            with self._spill_wal_lock:
                with open(self._spill_wal_path, 'a', encoding='utf-8') as f:
                    f.write(json_dumps(wal_entry) + '\n')
                    f.flush()
                    os.fsync(f.fileno())
        except Exception as e:
            logging.debug("[SpillWAL] WAL追加失败(非致命): %s", e)

    def _spill_wal_append_batch(self, tasks: List[Any]) -> None:
        if not tasks:
            return
        try:
            with self._spill_wal_lock:
                with open(self._spill_wal_path, 'a', encoding='utf-8') as f:
                    for task in tasks:
                        try:
                            wal_entry = self._wal_serialize(task)
                            f.write(json_dumps(wal_entry) + '\n')
                        except Exception as _batch_wal_err:
                            logging.debug("[SpillWAL] 批量WAL单条写入失败(跳过): %s", _batch_wal_err)
                            continue
                    f.flush()
                    os.fsync(f.fileno())
        except Exception as e:
            logging.debug("[SpillWAL] 批量WAL写入失败(非致命): %s", e)

    def _spill_wal_clear(self) -> None:
        try:
            with self._spill_wal_lock:
                if os.path.exists(self._spill_wal_path):
                    os.remove(self._spill_wal_path)
        except Exception as e:
            logging.debug("[SpillWAL] WAL清除失败(非致命): %s", e)

    def _spill_wal_rewrite(self) -> None:
        try:
            with self._spill_wal_lock:
                tmp_path = self._spill_wal_path + '.tmp'
                with open(tmp_path, 'w', encoding='utf-8') as f:
                    for task in self._pending_on_stop_data:
                        try:
                            wal_entry = self._wal_serialize(task)
                            f.write(json_dumps(wal_entry) + '\n')
                        except Exception as _wal_err:
                            logging.debug("[SpillWAL] 单条WAL写入失败(跳过): %s", _wal_err)
                            continue
                os.replace(tmp_path, self._spill_wal_path)
        except Exception as e:
            logging.debug("[SpillWAL] WAL重写失败(非致命): %s", e)


_StorageWalMixin = StorageWalService


__all__ = [
    '_StorageCoreMixin', '_get_default_db_path',
    'StorageQuery', '_StorageQueryMixin',
    'StorageWalService', '_StorageWalMixin',
]
