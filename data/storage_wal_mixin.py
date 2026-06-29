# MODULE_ID: M1-051
"""
storage_wal_mixin.py — WAL(Write-Ahead Log)溢写Mixin
包含: _StorageWalMixin — 异步队列溢写WAL持久化与恢复
"""

import logging
import os
import threading
from typing import List, Any

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads


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
    def _wal_serialize(task) -> List[Any]:  # [R22-P2-TS16]
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

    def _spill_wal_append_batch(self, tasks: List[Any]) -> None:  # [R22-P2-TS18]
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
