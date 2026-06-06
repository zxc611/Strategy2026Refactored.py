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
from datetime import datetime, timedelta, timezone
from typing import Any

_CHINA_TZ = timezone(timedelta(hours=8))

from ali2026v3_trading.serialization_utils import json_dumps, safe_jsonl_append_line

try:
    from ali2026v3_trading.audit_log_utils import structured_audit_log as _structured_audit_log  # R27-CP-05-FIX
except ImportError:
    _structured_audit_log = None


class PositionPersistenceService:
    """持仓持久化+快照服务 — 从PositionService拆分"""

    _POSITION_STATE_MAX_BYTES = 50 * 1024 * 1024
    _POSITION_STATE_BACKUP_COUNT = 3

    def __init__(self, position_service: Any):
        self._ps = position_service

    def _rotate_jsonl_if_needed(self, filepath: str) -> None:
        try:
            if not os.path.exists(filepath):
                return
            if os.path.getsize(filepath) > self._POSITION_STATE_MAX_BYTES:
                for i in range(self._POSITION_STATE_BACKUP_COUNT, 0, -1):
                    src = f"{filepath}.{i}"
                    dst = f"{filepath}.{i + 1}"
                    if os.path.exists(src):
                        if i == self._POSITION_STATE_BACKUP_COUNT:
                            os.remove(src)
                        else:
                            os.rename(src, dst)
                os.rename(filepath, f"{filepath}.1")
        except Exception:
            pass

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
        except Exception as e:
            logging.debug("[PositionService] R15-P0-RES-06: _append_position_state failed: %s", e)

    def _recover_position_state(self):
        try:
            if not os.path.exists(self._ps._position_state_file):
                return
            recovered = 0
            total_records = 0
            with open(self._ps._position_state_file, 'r', encoding='utf-8') as f:
                for line in f:
                    total_records += 1
                    try:
                        import json as _json
                        record = _json.loads(line.strip())
                        inst_id = record.get('instrument_id')
                        pid = record.get('position_id')
                        act = record.get('action')
                        if inst_id and pid and act == 'OPEN':
                            self._ps.positions.setdefault(inst_id, {})
                            recovered += 1
                    except Exception:
                        continue
            if recovered > 0:
                logging.info("[PositionService] R15-P0-RES-06: 从position_state.jsonl恢复%d条持仓", recovered)
            if total_records > 0 and recovered != total_records:
                logging.warning(
                    "[PositionService] DR-01: 恢复完整性校验警告 — "
                    "total_records=%d recovered=%d (差异=%d)",
                    total_records, recovered, total_records - recovered,
                )
        except Exception as e:
            logging.debug("[PositionService] R15-P0-RES-06: _recover_position_state failed: %s", e)

    def _recover_positions(self):
        self._recover_position_state()

    def _load_position_configs(self) -> None:
        try:
            from ali2026v3_trading.config_service import get_config
            from ali2026v3_trading.position_service import PositionLimitConfig
            config = get_config()
            data = getattr(config, 'option_buy_limits', None)
            if data is None:
                if not os.path.exists(self._ps.config_file):
                    return
                with open(self._ps.config_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
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
                        except Exception as e:
                            logging.error(f"[PositionService._load_position_configs] Error parsing date: {e}")
                            continue
                    if "created_at" in config_data and isinstance(config_data["created_at"], str):
                        try:
                            config_data["created_at"] = datetime.strptime(
                                config_data["created_at"], "%Y-%m-%d %H:%M:%S"
                            )
                        except Exception as e:
                            logging.error(f"[PositionService._load_position_configs] Error parsing date: {e}")
                            continue
                    try:
                        cfg = PositionLimitConfig(**config_data)
                    except Exception as e:
                        logging.error(f"[PositionService._load_position_configs] Error creating config: {e}")
                        continue
                    if cfg.effective_until and datetime.now(_CHINA_TZ) > cfg.effective_until:
                        continue
                    if self._ps._risk_bridge:
                        self._ps._risk_bridge.set_position_limit(account_id, cfg.limit_amount, cfg.effective_until)

            logging.info(f"[PositionService._load_position_configs] Loaded from {self._ps.config_file}")

        except Exception as e:
            logging.error(f"[PositionService._load_position_configs] Error: {e}")
            with self._ps.global_lock:
                pass

    def _save_position_configs(self) -> None:
        try:
            if not self._ps._risk_bridge:
                return
            from ali2026v3_trading.position_service import PositionLimitConfig
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

            with open(self._ps.config_file, "w", encoding="utf-8") as f:
                f.write(json_dumps(save_data, indent=2))

            logging.debug(f"[PositionService._save_position_configs] Saved to {self._ps.config_file}")

        except Exception as e:
            logging.error(f"[PositionService._save_position_configs] Error: {e}")
