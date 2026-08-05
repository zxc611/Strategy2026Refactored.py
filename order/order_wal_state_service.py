# [M1-49] WALдǰ__־

# MODULE_ID: M1-145

"""

order_wal_state_service.py - OrderWALStateService

R26: 从order_service.py提取的WAL写前日志+状态持久化方法



职责任

- WAL写前日志 (_ensure_wal_dir, _wal_path, _wal_write, _wal_read, _wal_delete, _recover_orphaned_orders)

- 状态持久化 (_persist_idempotent_key, _recover_idempotent_state, _rotate_jsonl_if_needed, _append_order_state, _recover_order_state)

- 补偿事务 (_execute_with_compensation_v2)

"""

from __future__ import annotations



import json

import logging

import os

import time

from datetime import datetime

from typing import Any, Callable, Dict, List, Optional



from infra.serialization_utils import json_dumps, json_loads

from infra.serialization_utils import safe_jsonl_append_line

from infra.resilience import is_disk_full_error

from infra.shared_utils import atomic_replace_file, sanitize_filename



from infra.shared_utils import CHINA_TZ





class OrderWALStateService:

    _ORDER_STATE_MAX_BYTES = 50 * 1024 * 1024

    _ORDER_STATE_BACKUP_COUNT = 3

    # [FIX-PAUSE-DELAY-20260721] R3根因修复: 订单状态JSONL无限累积导致capacity_exceeded
    # 根因: _recover_order_state()恢复全部历史订单(68380条) → 超过C++平台10000限制
    # 修复: 限制恢复数量+按日期过滤+自动归档历史订单
    _ORDER_STATE_RECOVER_MAX = 5000  # 最大恢复数量(远小于C++平台10000限制)
    _ORDER_STATE_ARCHIVE_ENABLED = True  # 启动时自动归档非当日订单
    # [FIX-PAUSE-DELAY-20260721] R2根因修复: 同步恢复68380条订单阻塞on_start 7+秒
    # 根因: _recover_order_state()在on_start流程中同步执行,阻塞C++平台超时判断
    # 修复: 异步恢复订单状态, on_start立即返回, 后台线程加载历史订单
    _ORDER_STATE_ASYNC_RECOVER = True
    _ORDER_STATE_RECOVER_TIMEOUT_SEC = 30.0  # 异步恢复最大耗时



    def __init__(self, provider):

        self._provider = provider

        self._persistence = None

        # [FIX-PAUSE-DELAY-EXT-20260721] 初始化线程管理属性
        self._order_state_recover_thread = None
        self._order_state_recover_stop_requested = False

        try:

            from order.order_persistence import OrderPersistenceService

            wal_dir = getattr(provider, '_wal_dir', 'orders_wal')

            idempotent_file = getattr(provider, '_idempotent_state_file', 'idempotent_state.jsonl')

            state_file = getattr(provider, '_order_state_file', 'order_state.jsonl')

            self._persistence = OrderPersistenceService(wal_dir, idempotent_file, state_file)

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.debug("[R3-L2] suppressed exception", exc_info=True)

            pass

            pass



    # [FIX-V3-02-20260722] JSONL文件按strategy_id分离
    # 根因: order_state.jsonl不包含strategy_id, 新实例加载旧实例68380条订单→capacity_exceeded
    # 修复: 当_provider_ref可用时, 将文件名从order_state.jsonl迁移到order_state_{strategy_id}.jsonl
    # 设计原则: 通过修改_provider._order_state_file属性实现, 所有读写代码无需修改
    def _migrate_state_file_to_strategy_id(self) -> None:
        """将order_state.jsonl迁移到order_state_{strategy_id}.jsonl

        [FIX-V3-02-20260722] 在_provider_ref设置后调用
        如果旧文件order_state.jsonl存在且新文件不存在, 执行迁移(重命名)
        如果新文件已存在(上一次运行), 不覆盖
        迁移成功后更新_provider._order_state_file指向新文件名
        """
        try:
            _ref = getattr(self._provider, '_provider_ref', None)
            if _ref is None:
                return
            _sid = getattr(_ref, 'strategy_id', None)
            if not _sid:
                return

            _old_file = getattr(self._provider, '_order_state_file', 'order_state.jsonl')
            _wal_dir = getattr(self._provider, '_wal_dir', 'orders_wal')
            _new_file = os.path.join(_wal_dir, f'order_state_{_sid}.jsonl')

            # 已经是strategy_id文件名, 无需迁移
            if _old_file == _new_file:
                return

            # 旧文件不存在, 直接更新文件名
            if not os.path.exists(_old_file):
                self._provider._order_state_file = _new_file
                logging.info("[FIX-V3-02] 订单状态文件已更新为strategy_id文件名: %s", _new_file)
                return

            # 新文件已存在(上次运行遗留), 不覆盖, 但更新文件名指向新文件
            if os.path.exists(_new_file):
                self._provider._order_state_file = _new_file
                logging.info("[FIX-V3-02] strategy_id文件已存在, 切换到: %s (旧文件保留: %s)", _new_file, _old_file)
                return

            # 旧文件存在, 新文件不存在 → 重命名迁移
            try:
                os.rename(_old_file, _new_file)
                self._provider._order_state_file = _new_file
                logging.info("[FIX-V3-02] 订单状态文件已迁移: %s → %s", _old_file, _new_file)
            except OSError as _rename_err:
                # Windows下可能文件被占用, 降级: 不迁移但记录警告
                logging.warning("[FIX-V3-02] 文件迁移失败(非致命,使用原文件): %s → %s, err=%s",
                               _old_file, _new_file, _rename_err)
        except Exception as _migrate_err:
            logging.debug("[FIX-V3-02] 迁移异常(非致命): %s", _migrate_err)

    # [FIX-V3-04-20260722] 清理旧strategy_id的JSONL文件
    # 根因: C++平台创建新实例后, 旧实例的order_state_{old_id}.jsonl残留在磁盘上
    # 修复: 在迁移完成后, 清理非当前strategy_id的旧文件(超过1小时的)
    def _cleanup_old_state_files(self) -> None:
        """清理非当前strategy_id的订单状态JSONL文件"""
        try:
            _ref = getattr(self._provider, '_provider_ref', None)
            if _ref is None:
                return
            _current_sid = getattr(_ref, 'strategy_id', None)
            if not _current_sid:
                return

            import glob
            _wal_dir = getattr(self._provider, '_wal_dir', 'orders_wal')
            _pattern = os.path.join(_wal_dir, 'order_state_*.jsonl')
            _cleaned = 0

            for _file in glob.glob(_pattern):
                _basename = os.path.basename(_file)
                # 跳过当前strategy_id的文件
                if f'order_state_{_current_sid}.jsonl' in _basename:
                    continue
                # 跳过归档文件
                if 'archive' in _basename:
                    continue

                # 只清理超过1小时的旧文件(避免清理刚创建的文件)
                try:
                    _age = time.time() - os.path.getmtime(_file)
                    if _age > 3600:
                        os.remove(_file)
                        _cleaned += 1
                        logging.info("[FIX-V3-04] 清理旧实例JSONL: %s (age=%.1fh)", _basename, _age / 3600)
                except OSError:
                    pass

            if _cleaned > 0:
                logging.info("[FIX-V3-04] 共清理%d个旧实例JSONL文件", _cleaned)
        except Exception as _cleanup_err:
            logging.debug("[FIX-V3-04] 清理旧JSONL异常(非阻断): %s", _cleanup_err)

    def _ensure_wal_dir(self) -> None:

        try:

            os.makedirs(self._provider._wal_dir, exist_ok=True)

        except Exception as e:

            # [FIX-WAL-EXCEPT-20260720] 扩展为except Exception，符合NEW-1硬约束
            logging.warning("[OrderService] R25-TO-03-FIX: WAL目录创建失败: %s", e)



    def _wal_path(self, order_id: str) -> str:

        safe_id = sanitize_filename(order_id)  # R2-3修复: 使用统一sanitize_filename

        return os.path.join(self._provider._wal_dir, f"{safe_id}.wal")



    def _wal_write(self, order_id: str, state: str, order: Dict) -> None:

        try:

            entry = {

                'order_id': order_id,

                'state': state,

                'instrument_id': order.get('instrument_id', ''),

                'direction': order.get('direction', ''),

                'volume': order.get('volume', 0),

                'price': order.get('price', 0),

                # FIX-R32-ACTION-PERSIST: 必须保存action字段，否则重启后订单丢失action，
                # 导致on_trade无法判断开仓/平仓，_reset_closing_flag_on_order_failure无法识别CLOSE订单，
                # 自成交检测_has_close检查失败无法回退到_reduce_position
                'action': order.get('action', ''),

                'timestamp': time.time(),

                'datetime': datetime.now(CHINA_TZ).isoformat(),

            }

            _wal_path = self._wal_path(order_id)

            # P2-22修复: 使用 atomic_replace_file 替代内联 os.replace

            _result = atomic_replace_file(_wal_path, json_dumps(entry))

            if not _result['success']:

                raise RuntimeError(_result.get('error', 'atomic_replace_file failed'))

        except Exception as e:

            # [FIX-WAL-EXCEPT-20260720] 扩展为except Exception，符合NEW-1硬约束
            # 原窄异常元组 (ValueError/KeyError/TypeError/RuntimeError/AttributeError)
            # 无法捕获 OSError (WinError 87 等)，导致WAL写入失败穿透到调用方
            logging.error("[OrderService] R25-TO-03-FIX: WAL写入失败: order=%s state=%s err=%s", order_id, state, e)

            if is_disk_full_error(e):

                logging.critical("[R33-P1-16] WAL写入失败: 磁盘满ENOSPC)! 订单持久化不可靠! err=%s", e)

                if not hasattr(self._provider, '_disk_full_mode'):

                    self._provider._disk_full_mode = True

                    logging.critical("[R33-P1-16] 进入磁盘满降级模式，后续订单仅内存暂停")

            if not hasattr(self._provider, '_wal_write_fail_count'):

                self._provider._wal_write_fail_count = 0

            self._provider._wal_write_fail_count += 1

            if self._provider._wal_write_fail_count >= 10:

                logging.critical("[R31-P1-10] WAL连续写入失败%d次，订单持久化不可靠!", self._provider._wal_write_fail_count)



    def _wal_read(self, order_id: str) -> Optional[Dict]:

        try:

            path = self._wal_path(order_id)

            if os.path.exists(path):

                with open(path, 'r', encoding='utf-8') as f:

                    return json_loads(f.read())  # R3-2修复: 使用统一json_loads

        except Exception as e:

            # [FIX-WAL-EXCEPT-20260720] 扩展为except Exception，符合NEW-1硬约束
            logging.warning("[OrderService] R25-TO-03-FIX: WAL读取失败: order=%s err=%s", order_id, e)

        return None



    def _wal_delete(self, order_id: str) -> None:

        try:

            path = self._wal_path(order_id)

            if os.path.exists(path):

                os.remove(path)

        except Exception as e:

            # [FIX-WAL-EXCEPT-20260720] 扩展为except Exception，符合NEW-1硬约束
            logging.warning("[OrderService] R25-TO-03-FIX: WAL删除失败: order=%s err=%s", order_id, e)



    _ORPHAN_SCAN_MAX_FILES = 500
    _ORPHAN_SCAN_TIMEOUT_SEC = 5.0
    _ORPHAN_CLEANUP_AGE_HOURS = 24.0

    def _recover_orphaned_orders(self) -> None:

        try:

            if not os.path.isdir(self._provider._wal_dir):

                return

            # [FIX-ORPHAN-EARLY-RETURN] _orders_by_id为空时扫描是NO-OP(无法恢复任何孤儿订单)
            # 直接跳过，避免无用的文件I/O
            if not self._provider._orders_by_id:
                return

            _scan_start = time.monotonic()
            orphaned_count = 0
            scanned_count = 0
            skipped_count = 0

            _all_files = []
            try:
                for fname in os.listdir(self._provider._wal_dir):
                    if fname.endswith('.wal'):
                        _all_files.append(fname)
            except Exception:
                pass

            _total_wal = len(_all_files)
            if _total_wal > self._ORPHAN_SCAN_MAX_FILES:
                logging.warning(
                    "[FIX-ORPHAN-PERF] .wal文件数=%d 超过扫描上限=%d, 跳过扫描并清理旧文件",
                    _total_wal, self._ORPHAN_SCAN_MAX_FILES
                )
                try:
                    _wal_dir = self._provider._wal_dir
                    _now = time.time()
                    _cutoff = _now - self._ORPHAN_CLEANUP_AGE_HOURS * 3600
                    _cleaned = 0
                    for fname in _all_files:
                        if time.monotonic() - _scan_start > self._ORPHAN_SCAN_TIMEOUT_SEC:
                            break
                        fpath = os.path.join(_wal_dir, fname)
                        try:
                            if os.path.getmtime(fpath) < _cutoff:
                                os.remove(fpath)
                                _cleaned += 1
                                skipped_count += 1
                        except Exception:
                            pass
                    if _cleaned > 0:
                        logging.info("[FIX-ORPHAN-PERF] 清理%d个超过%.0f小时的旧.wal文件(耗时%.1fs)",
                                     _cleaned, self._ORPHAN_CLEANUP_AGE_HOURS, time.monotonic() - _scan_start)
                except Exception as _clean_err:
                    logging.debug("[FIX-ORPHAN-PERF] 旧文件清理异常(非致命): %s", _clean_err)
            else:
                for fname in _all_files:
                    fpath = os.path.join(self._provider._wal_dir, fname)
                    scanned_count += 1
                    if time.monotonic() - _scan_start > self._ORPHAN_SCAN_TIMEOUT_SEC:
                        logging.warning("[FIX-ORPHAN-PERF] 扫描超时%.1fs, 停止扫描(已扫描=%d)",
                                       time.monotonic() - _scan_start, scanned_count)
                        break
                    orphaned_count = self._scan_single_wal(fpath, orphaned_count)

            _elapsed = time.monotonic() - _scan_start
            if orphaned_count > 0 or _total_wal > self._ORPHAN_SCAN_MAX_FILES:
                logging.info(
                    "[FIX-ORPHAN-PERF] 孤儿订单恢复完成: orphaned=%d scanned=%d skipped=%d total_wal=%d elapsed=%.2fs",
                    orphaned_count, scanned_count, skipped_count, _total_wal, _elapsed
                )
        except Exception as e:

            logging.warning("[OrderService] R25-TO-03-FIX: 孤儿订单恢复过程异常: %s", e)

    def _scan_single_wal(self, fpath: str, orphaned_count: int) -> int:

        try:

            with open(fpath, 'r', encoding='utf-8') as f:

                entry = json_loads(f.read())

            if entry.get('state') == 'PENDING':

                order_id = entry.get('order_id', '')

                _marked = False
                with self._provider._lock:

                    order = self._provider._orders_by_id.get(order_id)

                    if order and order.get('status') in ('SUBMITTED', 'PENDING'):

                        order['status'] = 'ORPHANED'

                        order['updated_at'] = datetime.now(CHINA_TZ)

                        orphaned_count += 1

                        _marked = True

                        logging.warning(

                            "[OrderService] R25-TO-03-FIX: 孤儿订单恢复: order_id=%s instrument=%s "
                            "状态从SUBMITTED/PENDING标记为ORPHANED",

                            order_id, entry.get('instrument_id', ''),

                        )

                # [FIX-ORPHAN-WAL] 仅在订单实际被标记ORPHANED时才写WAL+删除.wal文件
                # 原bug: _wal_write('ORPHANED')在if条件外，订单不存在时仍写WAL
                # 导致.wal文件残留且永远不会被清理
                if _marked:
                    self._wal_write(order_id, 'ORPHANED', {'order_id': order_id, 'instrument_id': entry.get('instrument_id', '')})
                    self._wal_delete(order_id)

        except Exception as e:

            logging.debug("[OrderService] R25-TO-03-FIX: WAL文件恢复异常: %s err=%s", fpath, e)

        return orphaned_count



    def _persist_idempotent_key(self, key: str) -> None:

        try:

            with self._provider._idempotent_lock:

                with open(self._provider._idempotent_state_file, 'a', encoding='utf-8') as f:

                    safe_jsonl_append_line(f, {'key': key, 'ts': time.time()})

        except Exception as e:

            # [FIX-WAL-EXCEPT-20260720] 扩展为except Exception，符合NEW-1硬约束
            logging.warning("[R16-P0-RES-02] 幂等键持久化失败: %s", e)



    def _recover_idempotent_state(self) -> None:

        try:

            if not os.path.exists(self._provider._idempotent_state_file):

                return

            recovered = 0

            with open(self._provider._idempotent_state_file, 'r', encoding='utf-8') as f:

                for line in f:

                    line = line.strip()

                    if not line:

                        continue

                    try:

                        record = json_loads(line)

                        key = record.get('key', '')

                        if key:

                            self._provider._order_idempotent_set.add(key)

                            recovered += 1

                    except (json.JSONDecodeError, KeyError):

                        continue

            if recovered > 0:

                logging.info("[R16-P0-RES-02] 幂等去重集合已恢复 %d条记录", recovered)
        except Exception as e:

            # [FIX-WAL-EXCEPT-20260720] 扩展为except Exception，符合NEW-1硬约束
            logging.warning("[R16-P0-RES-02] 幂等状态恢复失败 %s", e)



    # P2-01修复: 委托到infra/serialization_utils.py的公共函数

    def _rotate_jsonl_if_needed(self, filepath: str) -> None:

        from infra.serialization_utils import rotate_jsonl_if_needed as _rotate

        _rotate(filepath, self._ORDER_STATE_MAX_BYTES, self._ORDER_STATE_BACKUP_COUNT)



    def _append_order_state(self, order_id: str, state: str, order: Dict) -> None:

        try:

            record = {

                'order_id': order_id,

                'state': state,

                'instrument_id': order.get('instrument_id', ''),

                'direction': order.get('direction', ''),

                'volume': order.get('volume', 0),

                'price': order.get('price', 0),

                # FIX-R32-ACTION-PERSIST: 必须保存action字段，否则重启后订单丢失action
                'action': order.get('action', ''),

                # FIX-R37-PID-PERSIST: 持久化platform_order_id，重启后可重建platform_id→internal_id映射，
                # 避免on_order/on_trade回调时因映射丢失而退化为instrument模糊匹配(导致错单)
                'platform_order_id': order.get('platform_order_id', ''),

                'ts': time.time(),

            }

            with self._provider._order_state_lock:

                self._rotate_jsonl_if_needed(self._provider._order_state_file)

                with open(self._provider._order_state_file, 'a', encoding='utf-8') as f:

                    safe_jsonl_append_line(f, record)

            self._provider._append_state_fail_count = 0

        except Exception as e:

            # [FIX-WAL-EXCEPT-20260720] 扩展为except Exception，符合NEW-1硬约束
            self._provider._append_state_fail_count = getattr(self._provider, '_append_state_fail_count', 0) + 1

            logging.error("[R33-P1-10] 订单状态追加写失败(连续%d次): order=%s state=%s err=%s",

                          self._provider._append_state_fail_count, order_id, state, e)

            if is_disk_full_error(e):

                logging.critical("[R33-P1-16] 订单状态追加写失败: 磁盘满ENOSPC)! err=%s", e)

                if not hasattr(self._provider, '_disk_full_mode'):

                    self._provider._disk_full_mode = True

                    logging.critical("[R33-P1-16] 进入磁盘满降级模式，后续订单仅内存暂停")
            _threshold = getattr(self._provider, '_append_state_fail_critical_threshold', 10)

            if self._provider._append_state_fail_count >= _threshold:

                logging.critical(
                    "[R33-P1-10] CRITICAL: 订单状态追加写已连续失败%d，阈值%d，" "WAL写入链路可能已损坏，订单状态持久化丢失风险，", self._provider._append_state_fail_count, _threshold)


    def _recover_order_state(self) -> None:
        # [FIX-PAUSE-DELAY-20260721] R2+R3根因修复
        # 修复内容:
        #   1. R3: 按日期过滤+数量限制+自动归档(防止JSONL无限累积超过C++平台10000限制)
        #   2. R2: 异步恢复(on_start不等待,后台线程加载历史订单)
        # 历史根因:
        #   - 13:20:45 恢复68380条订单 → 超过C++平台10000限制 → capacity_exceeded
        #   - 13:20:38-13:20:45 同步恢复阻塞on_start 7+秒 → C++平台超时创建新实例
        if self._ORDER_STATE_ASYNC_RECOVER:
            self._recover_order_state_async()
        else:
            self._recover_order_state_sync()

    def _recover_order_state_async(self) -> None:
        """异步恢复订单状态 - on_start立即返回,后台线程加载历史订单

        [FIX-PAUSE-DELAY-20260721] R2根因修复
        根因: 同步恢复68380条订单阻塞on_start 7+秒, C++平台超时创建新实例
        修复: 启动后台线程异步加载, on_start不等待

        [FIX-PAUSE-DELAY-EXT-20260721] 非半拉子工程补全
        - 保存线程引用供stop方法关闭
        - 增加_stop_requested标志供优雅退出
        - 纳入55+工作关闭管理
        """
        import threading
        try:
            # 标记恢复进行中(降级模式: 仅记录新订单,不查询历史订单)
            self._provider._order_state_recovering = True
            self._provider._order_state_recovered_count = 0
            # [FIX-EXT] 清除停止标志(支持多次启动)
            self._order_state_recover_stop_requested = False

            _recover_thread = threading.Thread(
                target=self._async_recover_worker,
                name="OrderStateRecover",
                daemon=True,
            )
            # [FIX-EXT] 保存线程引用供stop_order_state_recover使用
            self._order_state_recover_thread = _recover_thread
            _recover_thread.start()
            logging.info("[FIX-PAUSE-DELAY-20260721] 订单状态异步恢复已启动(后台线程), on_start不阻塞")
        except Exception as e:
            # 异步启动失败,降级为同步恢复
            logging.warning("[FIX-PAUSE-DELAY-20260721] 异步恢复启动失败,降级为同步: %s", e)
            self._provider._order_state_recovering = False
            self._recover_order_state_sync()

    def _async_recover_worker(self) -> None:
        """异步恢复工作线程 - 实际执行订单状态恢复

        [FIX-PAUSE-DELAY-EXT-20260721] 增加_stop_requested检查
        支持on_stop/pause时优雅退出,避免线程泄漏
        """
        try:
            # [FIX-EXT] 检查停止标志,支持优雅退出
            if self._order_state_recover_stop_requested:
                logging.info("[FIX-PAUSE-DELAY-EXT-20260721] 异步恢复线程启动前已收到停止信号,跳过恢复")
                return
            self._recover_order_state_sync()
        except Exception as e:
            logging.error("[FIX-PAUSE-DELAY-20260721] 异步恢复订单状态失败: %s", e)
        finally:
            # 标记恢复完成(退出降级模式)
            self._provider._order_state_recovering = False
            # [FIX-ORPHAN-CALLBACK] 异步恢复完成后补调_recover_orphaned_orders
            # 根因: _recover_orphaned_orders在OrderService()构造函数中调用时_orders_by_id为空(NO-OP)
            # 异步恢复完成后_orders_by_id有数据，此时才能真正恢复孤儿订单
            try:
                self._recover_orphaned_orders()
            except Exception as _orphan_err:
                logging.warning("[FIX-ORPHAN-CALLBACK] 异步恢复后孤儿订单扫描失败(非致命): %s", _orphan_err)
            # [FIX-EXT] 清理线程引用
            self._order_state_recover_thread = None

    def stop_order_state_recover(self, timeout: float = 2.0) -> bool:
        """停止订单状态恢复线程 - 纳入55+工作关闭管理

        [FIX-PAUSE-DELAY-EXT-20260721] 非半拉子工程补全
        根因: OrderStateRecover线程未被stop方法管理,导致线程泄漏
        修复:
          1. 设置_stop_requested标志,通知worker优雅退出
          2. 等待线程结束(timeout秒)
          3. 清理降级模式标记
          4. 清理线程引用

        调用位置: strategy_2026.py on_stop/pause + lifecycle_callbacks.py on_stop
        """
        try:
            # 设置停止标志
            self._order_state_recover_stop_requested = True

            # 获取线程引用
            _thread = getattr(self, '_order_state_recover_thread', None)
            if _thread is not None and _thread.is_alive():
                logging.info("[FIX-PAUSE-DELAY-EXT-20260721] 等待OrderStateRecover线程结束(timeout=%.1fs)", timeout)
                _thread.join(timeout=timeout)
                if _thread.is_alive():
                    logging.warning("[FIX-PAUSE-DELAY-EXT-20260721] OrderStateRecover线程未在%.1fs内结束(非致命,daemon=True)", timeout)
                else:
                    logging.info("[FIX-PAUSE-DELAY-EXT-20260721] OrderStateRecover线程已正常结束")

            # 清理降级模式标记(防止下次启动处于错误降级模式)
            self._provider._order_state_recovering = False
            # 清理线程引用
            self._order_state_recover_thread = None
            return True
        except Exception as e:
            logging.warning("[FIX-PAUSE-DELAY-EXT-20260721] 停止OrderStateRecover线程异常(非致命): %s", e)
            # 即使异常也清理标记
            try:
                self._provider._order_state_recovering = False
            except Exception:
                pass
            return False

    def _recover_order_state_sync(self) -> None:
        """同步恢复订单状态 - 含按日期过滤+数量限制+自动归档

        [FIX-PAUSE-DELAY-20260721] R3根因修复
        根因: JSONL无限累积68380条, 超过C++平台10000限制 → capacity_exceeded
        修复:
          1. 按日期过滤: 只恢复当日订单(基于record.ts时间戳)
          2. 数量限制: 最多恢复_ORDER_STATE_RECOVER_MAX条(5000条)
          3. 自动归档: 非当日订单归档到order_state_archive_YYYYMMDD.jsonl
        """
        try:
            if not os.path.exists(self._provider._order_state_file):
                return

            # [FIX-PAUSE-DELAY-20260721] 步骤1: 读取全部记录并按日期分类
            _today_str = datetime.now().strftime('%Y-%m-%d')
            _today_records = []  # 当日订单(待恢复)
            _archive_records = {}  # 历史订单按日期归档 {date_str: [records]}
            _total_lines = 0
            _parse_failed = 0

            with open(self._provider._order_state_file, 'r', encoding='utf-8') as f:
                for line in f:
                    _total_lines += 1
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json_loads(line)
                        # 判断订单日期: 优先使用record.ts, fallback到record.date
                        _record_date = None
                        _ts = record.get('ts', 0)
                        if _ts:
                            try:
                                _record_date = datetime.fromtimestamp(float(_ts)).strftime('%Y-%m-%d')
                            except (ValueError, OSError, OverflowError):
                                _record_date = None
                        if not _record_date:
                            _record_date = record.get('date', _today_str)  # fallback到今日

                        if _record_date == _today_str:
                            _today_records.append(record)
                        else:
                            # 历史订单,归档
                            if _record_date not in _archive_records:
                                _archive_records[_record_date] = []
                            _archive_records[_record_date].append(record)
                    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                        _parse_failed += 1
                        continue

            # [FIX-PAUSE-DELAY-20260721] 步骤2: 自动归档历史订单
            _archived_count = 0
            if self._ORDER_STATE_ARCHIVE_ENABLED and _archive_records:
                _archived_count = self._archive_historical_orders(_archive_records)

            # [FIX-PAUSE-DELAY-20260721] 步骤3: 数量限制 - 只恢复最新的_ORDER_STATE_RECOVER_MAX条当日订单
            if len(_today_records) > self._ORDER_STATE_RECOVER_MAX:
                logging.warning(
                    "[FIX-PAUSE-DELAY-20260721] 当日订单%d条超过最大恢复限制%d条, 只恢复最新的%d条",
                    len(_today_records), self._ORDER_STATE_RECOVER_MAX, self._ORDER_STATE_RECOVER_MAX
                )
                _today_records = _today_records[-self._ORDER_STATE_RECOVER_MAX:]

            # [FIX-PAUSE-DELAY-20260721] 步骤4: 恢复当日订单到内存
            recovered = 0
            for record in _today_records:
                try:
                    order_id = record.get('order_id', '')
                    state = record.get('state', '')
                    instrument_id = record.get('instrument_id', '')

                    if order_id and state:
                        with self._provider._lock:
                            if order_id not in self._provider._orders_by_id:
                                self._provider._orders_by_id[order_id] = {
                                    'order_id': order_id,
                                    'instrument_id': instrument_id,
                                    'direction': record.get('direction', ''),
                                    'volume': record.get('volume', 0),
                                    'price': record.get('price', 0),
                                    'status': state,
                                    # FIX-R32-ACTION-PERSIST: 恢复action字段，否则重启后订单丢失action
                                    'action': record.get('action', ''),
                                    # FIX-R37-PID-PERSIST: 恢复platform_order_id字段
                                    'platform_order_id': record.get('platform_order_id', ''),
                                }
                            # FIX-R37-PID-PERSIST: 重建 platform_id→internal_id 映射
                            _pid = record.get('platform_order_id', '') or self._provider._orders_by_id[order_id].get('platform_order_id', '')
                            if _pid and str(_pid) != order_id and not str(_pid).startswith('ORD_'):
                                self._provider._platform_id_to_order_id[str(_pid)] = order_id
                        recovered += 1
                except (KeyError, TypeError, ValueError):
                    continue

            # [FIX-PAUSE-DELAY-20260721] 步骤5: 重写JSONL文件只保留当日订单(清理历史累积)
            if _archived_count > 0:
                self._rewrite_order_state_file(_today_records)

            # 更新恢复计数(供降级模式判断)
            self._provider._order_state_recovered_count = recovered

            _pid_count = len(self._provider._platform_id_to_order_id)
            logging.info(
                "[R16-P0-RES-05] 订单状态已从JSONL恢复: %d条记录(总行数=%d, 当日=%d, 归档=%d, 解析失败=%d, 限流=%s), platform_id映射重建: %d条",
                recovered, _total_lines, len(_today_records), _archived_count, _parse_failed,
                "是" if len(_today_records) > self._ORDER_STATE_RECOVER_MAX else "否",
                _pid_count
            )
        except Exception as e:
            # [FIX-WAL-EXCEPT-20260720] 扩展为except Exception，符合NEW-1硬约束
            logging.warning("[R16-P0-RES-05] 订单状态恢复失败 %s", e)

    def _archive_historical_orders(self, archive_records: Dict[str, list]) -> int:
        """归档历史订单到独立文件

        [FIX-PAUSE-DELAY-20260721] R3根因修复
        将非当日订单按日期归档到 order_state_archive_YYYYMMDD.jsonl
        防止JSONL文件无限累积超过C++平台10000限制
        """
        _archived_count = 0
        try:
            _wal_dir = os.path.dirname(self._provider._order_state_file)
            for _date_str, _records in archive_records.items():
                _archive_file = os.path.join(_wal_dir, f'order_state_archive_{_date_str}.jsonl')
                # 追加模式写入(同一天可能多次启动)
                with open(_archive_file, 'a', encoding='utf-8') as f:
                    for _record in _records:
                        try:
                            f.write(json_dumps(_record) + '\n')
                            _archived_count += 1
                        except (TypeError, ValueError, OverflowError):
                            continue
            if _archived_count > 0:
                logging.info(
                    "[FIX-PAUSE-DELAY-20260721] 历史订单已归档: %d条, 归档日期数: %d",
                    _archived_count, len(archive_records)
                )
        except Exception as e:
            logging.warning("[FIX-PAUSE-DELAY-20260721] 历史订单归档失败(非致命): %s", e)
        return _archived_count

    def _rewrite_order_state_file(self, today_records: list) -> None:
        """重写JSONL文件只保留当日订单

        [FIX-PAUSE-DELAY-20260721] R3根因修复
        归档后重写主文件, 防止下次启动再次累积历史订单

        [FIX-PAUSE-DELAY-EXT-20260721] 并发安全补全
        根因: _rewrite_order_state_file未使用_order_state_lock
              与_append_order_state存在并发竞争,可能导致新订单丢失
        修复: 使用_order_state_lock保护重写过程,确保与订单写入互斥
        """
        try:
            _rewrite_start_ts = time.time()  # [FIX-EXT] 记录重写开始时间,用于检测重写期间新写入的订单
            _tmp_file = self._provider._order_state_file + '.tmp'
            with open(_tmp_file, 'w', encoding='utf-8') as f:
                for _record in today_records:
                    try:
                        f.write(json_dumps(_record) + '\n')
                    except (TypeError, ValueError, OverflowError):
                        continue
            # [FIX-EXT] 使用_order_state_lock保护原子替换,防止与_append_order_state竞争
            with self._provider._order_state_lock:
                # 在锁内重新读取主文件,获取重写期间新写入的订单
                _new_records_during_rewrite = []
                if os.path.exists(self._provider._order_state_file):
                    try:
                        with open(self._provider._order_state_file, 'r', encoding='utf-8') as f_orig:
                            for line in f_orig:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    _rec = json_loads(line)
                                    # 检查是否是重写期间新写入的订单(ts > 重写开始时间)
                                    _rec_ts = _rec.get('ts', 0)
                                    if _rec_ts and _rec_ts > _rewrite_start_ts:
                                        _new_records_during_rewrite.append(_rec)
                                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                                    continue
                    except Exception as _read_err:
                        logging.warning("[FIX-PAUSE-DELAY-EXT-20260721] 读取新订单失败(非致命): %s", _read_err)

                # 将新订单追加到临时文件
                if _new_records_during_rewrite:
                    with open(_tmp_file, 'a', encoding='utf-8') as f_append:
                        for _rec in _new_records_during_rewrite:
                            try:
                                f_append.write(json_dumps(_rec) + '\n')
                            except (TypeError, ValueError, OverflowError):
                                continue

                # 原子替换(Windows下os.replace是原子的)
                os.replace(_tmp_file, self._provider._order_state_file)

            logging.info(
                "[FIX-PAUSE-DELAY-20260721] 订单状态JSONL已重写: 保留当日订单%d条, 重写期间新订单%d条",
                len(today_records), len(_new_records_during_rewrite)
            )
        except Exception as e:
            logging.warning("[FIX-PAUSE-DELAY-20260721] 重写JSONL文件失败(非致命): %s", e)



    def _execute_with_compensation_v2(

        self,

        steps: List[Dict[str, Any]],

        result_ids: List[str],

        compensate_fn: Optional[Callable] = None,

    ) -> List[str]:

        executed_ids: List[str] = []

        for i, step_params in enumerate(steps):

            result = self._provider.send_order(**step_params)

            if hasattr(result, 'order_id') and result.order_id:

                executed_ids.append(result.order_id)

                result_ids.append(result.order_id)

            else:

                logging.error("[R16-P0-RES-11] 补偿事务时d步失败，开始逆序撤单", i + 1)

                for oid in reversed(executed_ids):

                    try:

                        if compensate_fn:

                            compensate_fn(oid)

                        else:

                            with self._provider._lock:

                                order = self._provider._orders_by_id.get(oid)

                                if order:

                                    order['status'] = 'COMPENSATED'

                                    self._append_order_state(oid, 'COMPENSATED', order)

                            logging.info("[R16-P0-RES-11] 补偿撤单: %s", oid)

                    except Exception as ce:

                        # [FIX-WAL-EXCEPT-20260720] 扩展为except Exception，符合NEW-1硬约束
                        logging.error("[R16-P0-RES-11] 补偿撤单失败: %s err=%s", oid, ce)

                return executed_ids

        return executed_ids



    def remove_order_and_idempotent_key(self, provider, order_id: str, order: Dict) -> None:

        # FIX-R28: CLOSE订单的idempotent_key含signal_id，需同步构造以正确移除
        _action = order.get('action', '')
        if _action in ('CLOSE', 'close'):
            _idempotent_key = f"{order.get('instrument_id', '')}_{order.get('direction', '')}_{_action}_{order.get('volume', '')}_{round(order.get('price', 0), 4)}_{order.get('signal_id', '')}"
        else:
            _idempotent_key = f"{order.get('instrument_id', '')}_{order.get('direction', '')}_{_action}_{order.get('volume', '')}_{round(order.get('price', 0), 4)}"

        provider._order_idempotent_set.discard(_idempotent_key)

        provider._orders_by_id.pop(order_id, None)

