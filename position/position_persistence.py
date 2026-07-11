# MODULE_ID: M1-205
"""Position Persistence Service - 持仓持久化+快照

从position_service.py拆分(CC-09):
- _append_position_state: 追加写入持仓状态到JSONL
- _recover_position_state: 启动时从JSONL恢复持仓状态
- _recover_positions: _recover_positions别名，兼容strategy_core_service调用
- _load_position_configs: 加载持仓限额配置
- _save_position_configs: 保存持仓限额配置
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import Any

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, safe_jsonl_append_line
from ali2026v3_trading.infra.shared_utils import atomic_replace_file  # R8-5

try:
    from ali2026v3_trading.infra.security_service import structured_audit_log as _structured_audit_log  # R1-4修复
except ImportError:
    _structured_audit_log = None


class PositionPersistenceService:
    """持仓持久化+快照服务 — 从PositionService拆分"""

    _POSITION_STATE_MAX_BYTES = 50 * 1024 * 1024
    _POSITION_STATE_BACKUP_COUNT = 3

    def __init__(self, position_service: Any):
        self._ps = position_service

    # P2-01修复: 委托到infra/serialization_utils.py的公共函数
    def _rotate_jsonl_if_needed(self, filepath: str) -> None:
        from ali2026v3_trading.infra.serialization_utils import rotate_jsonl_if_needed as _rotate
        _rotate(filepath, self._POSITION_STATE_MAX_BYTES, self._POSITION_STATE_BACKUP_COUNT)

    def _append_position_state(self, instrument_id: str, position_id: str, action: str, detail: dict = None, signal_id: str = ''):
        try:
            record = {'instrument_id': instrument_id, 'position_id': position_id,
                      'action': action, 'ts': time.time()}
            if signal_id:
                record['signal_id'] = signal_id  # R25-P0-TR-01修复: 持仓变更记录携带signal_id
            if detail:
                record.update(detail)
            with self._ps._position_state_lock:
                os.makedirs(os.path.dirname(self._ps._position_state_file), exist_ok=True)
                self._rotate_jsonl_if_needed(self._ps._position_state_file)
                with open(self._ps._position_state_file, 'a', encoding='utf-8') as f:
                    safe_jsonl_append_line(f, record)
            if _structured_audit_log:
                _structured_audit_log('position_change', action, {
                    'instrument_id': instrument_id, 'position_id': position_id,
                    'signal_id': signal_id or '(empty)', 'detail': detail
                })
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[PositionService] R15-P0-RES-06: _append_position_state failed: %s", e)

    def _recover_position_state(self):
        try:
            if not os.path.exists(self._ps._position_state_file):
                return
            recovered = 0
            total_records = 0
            close_records = 0
            # FIX-R37-POS-RECOVER: 先收集所有OPEN记录，再用CLOSE记录回放平仓
            # 这样可以正确恢复持仓的open_time/止盈止损价等关键字段
            _open_snapshots = {}  # position_id -> snapshot dict
            _close_ops = []  # [(instrument_id, position_id, close_detail)]
            # FIX-R37-UNIQUE-UPDATE(E1/E2): 收集 ROLLBACK 和 UPDATE 操作
            # E1: ROLLBACK 记录在恢复时被忽略，导致已回滚持仓在重启后"复活"为幽灵持仓
            # E2: 部分平仓(UPDATE)未持久化，重启后 volume 恢复为开仓原始量
            _rollback_ops = []  # [(instrument_id, position_id)]
            with open(self._ps._position_state_file, 'r', encoding='utf-8') as f:
                for line in f:
                    total_records += 1
                    try:
                        record = json_loads(line.strip())
                        inst_id = record.get('instrument_id')
                        pid = record.get('position_id')
                        act = record.get('action')
                        if inst_id and pid and act == 'OPEN':
                            # FIX-R37-POS-RECOVER: 优先使用完整快照恢复PositionRecord
                            _snapshot = record.get('snapshot')
                            if _snapshot:
                                _open_snapshots[pid] = _snapshot
                            else:
                                # 兼容旧格式(无snapshot字段): 只记录instrument_id
                                _open_snapshots[pid] = {
                                    'instrument_id': inst_id,
                                    'volume': record.get('volume', 0),
                                    'open_price': record.get('price', 0),
                                    'open_reason': record.get('open_reason', ''),
                                }
                        elif inst_id and pid and act == 'CLOSE':
                            close_records += 1
                            _close_ops.append((inst_id, pid, record))
                        # FIX-R37-UNIQUE-UPDATE(E1): ROLLBACK 记录等同于持仓删除，与 CLOSE 一起回放
                        elif inst_id and pid and act == 'ROLLBACK':
                            close_records += 1
                            _close_ops.append((inst_id, pid, record))
                        # FIX-R37-UNIQUE-UPDATE(E2): UPDATE 记录覆盖 OPEN 快照为最新状态
                        # 部分平仓后写入 UPDATE 记录，恢复时使用最新 volume
                        elif inst_id and pid and act == 'UPDATE':
                            _snapshot = record.get('snapshot')
                            if _snapshot:
                                _open_snapshots[pid] = _snapshot  # 覆盖OPEN快照为最新状态
                    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                        continue
            # FIX-R37-POS-RECOVER: 从快照重建PositionRecord
            from ali2026v3_trading.position.position_service import PositionRecord
            for pid, snap in _open_snapshots.items():
                inst_id = snap.get('instrument_id', '')
                if not inst_id:
                    continue
                # FIX-OPEN-UNIQUE(P2): 恢复路径绕过 _add_position 全部校验，
                # 增加基本数据完整性校验，避免恢复无效持仓
                _snap_vol = snap.get('volume', 0)
                _snap_price = snap.get('open_price', 0.0)
                if _snap_vol == 0:
                    logging.warning("[R37-POS-RECOVER] P2跳过volume=0的无效持仓: pid=%s inst=%s", pid, inst_id)
                    continue
                if _snap_price <= 0:
                    logging.warning("[R37-POS-RECOVER] P2跳过open_price<=0的无效持仓: pid=%s inst=%s", pid, inst_id)
                    continue
                try:
                    # 完整快照恢复
                    if 'position_id' in snap and 'open_time' in snap:
                        # FIX-20260704-GHOST-PERSIST: CANNOT_CLOSE持仓在恢复时直接删除
                        # 根因: 重启后closing_order_id被重置为空，CANNOT_CLOSE标记丢失，
                        # 导致幽灵持仓每次重启都重新触发TimeStop/StopProfit→平台拒绝→重试耗尽→CANNOT_CLOSE
                        # 每轮产生321条ERROR+WARNING洪泛(229条平仓重试耗尽+233条平仓下单失败+88条R28重试)
                        # 修复: closing_order_id为CANNOT_CLOSE的持仓是已确认无法平仓的幽灵持仓，恢复时跳过
                        _saved_closing_oid = snap.get('closing_order_id', '')
                        if _saved_closing_oid == 'CANNOT_CLOSE':
                            logging.info("[GHOST-PERSIST] 跳过恢复CANNOT_CLOSE幽灵持仓: pid=%s inst=%s", pid, inst_id)
                            continue
                        rec = PositionRecord.from_dict(snap)
                        # 重启后_closing必须重置为False，避免卡住
                        rec._closing = False
                        rec.closing_order_id = ''
                        # FIX-READ-UNIQUE-12: 重启恢复时重置close_method，
                        # 避免快照中残留的close_method(如'stop_loss')误导后续平仓触发路径
                        if hasattr(rec, 'close_method'):
                            rec.close_method = ''
                        self._ps.positions.setdefault(inst_id, {})[pid] = rec
                        recovered += 1
                    else:
                        # 旧格式兼容: 只创建空instrument字典(降级行为)
                        self._ps.positions.setdefault(inst_id, {})
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    logging.debug("[PositionService] R15-P0-RES-06: PositionRecord恢复失败 pid=%s: %s", pid, _r3_err)
                    self._ps.positions.setdefault(inst_id, {})
            # FIX-R37-POS-RECOVER: 回放CLOSE记录，减少或删除已平仓的持仓
            for inst_id, pid, close_record in _close_ops:
                _pos_dict = self._ps.positions.get(inst_id, {})
                if pid in _pos_dict:
                    # 该持仓在OPEN之后被CLOSE，删除它
                    del _pos_dict[pid]
                    if not _pos_dict:
                        del self._ps.positions[inst_id]
            if recovered > 0:
                logging.info("[PositionService] R15-P0-RES-06: 从position_state.jsonl恢复%d条持仓(完整快照), CLOSE回放%d条",
                             recovered, close_records)
            if total_records > 0:
                _open_count = len(_open_snapshots)
                if close_records > 0:
                    logging.info(
                        "[PositionService] DR-01: 持仓恢复统计 — "
                        "total_records=%d OPEN=%d CLOSE=%d recovered=%d",
                        total_records, _open_count, close_records, recovered,
                    )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[PositionService] R15-P0-RES-06: _recover_position_state failed: %s", e)

    def _recover_positions(self):
        self._recover_position_state()

    def _load_position_configs(self) -> None:
        try:
            from ali2026v3_trading.config.config_service import get_config
            from ali2026v3_trading.position.position_service import PositionLimitConfig
            config = get_config()
            data = getattr(config, 'option_buy_limits', None)
            if data is None:
                if not os.path.exists(self._ps.config_file):
                    return
                with open(self._ps.config_file, "r", encoding="utf-8") as f:
                    data = json_loads(f.read())  # R3-2修复
            elif not isinstance(data, dict):
                return

            with self._ps.global_lock:
                for account_id, config_data in data.items():
                    if not isinstance(config_data, dict):
                        continue
                    if "effective_until" in config_data and isinstance(config_data["effective_until"], str):
                        try:
                            config_data["effective_until"] = datetime.strptime(
                                config_data["effective_until"], "%Y-%m-%d %H:%M:%S"
                            )
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                            logging.error(f"[PositionService._load_position_configs] Error parsing date: {e}")
                            continue
                    if "created_at" in config_data and isinstance(config_data["created_at"], str):
                        try:
                            config_data["created_at"] = datetime.strptime(
                                config_data["created_at"], "%Y-%m-%d %H:%M:%S"
                            )
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                            logging.error(f"[PositionService._load_position_configs] Error parsing date: {e}")
                            continue
                    try:
                        cfg = PositionLimitConfig(**config_data)
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                        logging.error(f"[PositionService._load_position_configs] Error creating config: {e}")
                        continue
                    if cfg.effective_until and datetime.now(_CHINA_TZ) > cfg.effective_until:
                        continue
                    if self._ps._risk_bridge:
                        self._ps._risk_bridge.set_position_limit(account_id, cfg.limit_amount, cfg.effective_until)

            logging.info(f"[PositionService._load_position_configs] Loaded from {self._ps.config_file}")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error(f"[PositionService._load_position_configs] Error: {e}")
            with self._ps.global_lock:
                pass

    def _save_position_configs(self) -> None:
        try:
            if not self._ps._risk_bridge:
                return
            from ali2026v3_trading.position.position_service import PositionLimitConfig
            save_data = {}
            with self._ps.global_lock:
                for account_id, limit_info in getattr(self._ps._risk_bridge._risk_service, '_position_limits', {}).items():
                    if isinstance(limit_info, PositionLimitConfig):
                        limit_amount = limit_info.limit_amount
                        effective_until = limit_info.effective_until
                    elif isinstance(limit_info, dict):
                        limit_amount = limit_info.get('limit_amount', 0)
                        effective_until = limit_info.get('effective_until')
                    else:
                        limit_amount = 0
                        effective_until = None
                    save_data[account_id] = {
                        "limit_amount": float(limit_amount),
                        "account_id": account_id,
                        "effective_until": effective_until.strftime("%Y-%m-%d %H:%M:%S")
                            if effective_until else None,
                    }

            atomic_replace_file(self._ps.config_file, json_dumps(save_data, indent=2))  # R9-1

            logging.debug(f"[PositionService._save_position_configs] Saved to {self._ps.config_file}")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error(f"[PositionService._save_position_configs] Error: {e}")
