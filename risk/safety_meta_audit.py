# [M1-39] ��ȫԪ��-�����־
# MODULE_ID: M1-219
from __future__ import annotations

import json
import logging
import os
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer, safe_jsonl_append_line
from ali2026v3_trading.infra.shared_utils import atomic_replace_file

# 风控专用审计：职责边�?断路器状态持久化+合规审批记录。与config/config_logging.AuditLogger(日志审计)、param_pool/l1_quantification/meta_audit_engine(参数审计)分工�?
from ali2026v3_trading.risk._utils import (
    _get_tz_aware_now, safe_get_float, safe_get_int,
    api_version, structured_audit_log,
)



class CircuitBreakerStateStore:
    """断路器状态持久化服务 �?审计与状态存�?

    负责：断路器状态的save/load/跨日重置逻辑
    """

    def __init__(self, params: Any = None):
        self.params = params
        self._lock = threading.RLock()
        self._circuit_breaker_state_path: str = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), ".circuit_breaker_state.json"
        )

    def save_state(self, trading_paused_until: float, pause_reason: str,
                   circuit_breaker_calm_until: float, circuit_breaker_shadow_mode: bool,
                   circuit_breaker_shadow_until: float, circuit_breaker_activated_at: float,
                   state_data: Dict[str, Any]) -> None:
        """持久化断路器状态到JSON文件"""
        try:
            now = time.time()
            state = {
                "trading_paused_until": trading_paused_until,
                "pause_reason": pause_reason,
                "circuit_breaker_calm_until": circuit_breaker_calm_until,
                "circuit_breaker_shadow_mode": circuit_breaker_shadow_mode,
                "circuit_breaker_shadow_until": circuit_breaker_shadow_until,
                "circuit_breaker_activated_at": circuit_breaker_activated_at,
                "daily_hard_stop_triggered": state_data.get("daily_hard_stop_triggered", False),
                "daily_new_open_blocked": state_data.get("daily_new_open_blocked", False),
                "daily_start_equity": state_data.get("daily_start_equity"),
                "daily_peak_equity": state_data.get("daily_peak_equity", 0.0),
                "daily_drawdown": state_data.get("daily_drawdown", 0.0),
                "prev_5day_avg_profit": state_data.get("prev_5day_avg_profit", 0.0),
                "current_date": state_data.get("current_date"),
                "saved_at": now,
            }
            # P2-22修复: 使用 atomic_replace_file 替代内联 os.replace
            _result = atomic_replace_file(self._circuit_breaker_state_path, json_dumps(state))
            if not _result['success']:
                raise RuntimeError(_result.get('error', 'atomic_replace_file failed'))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[SafetyMetaLayer] 断路器状态持久化失败: %s", e)

    def load_state(self) -> Dict[str, Any]:
        result = {}
        try:
            if not os.path.exists(self._circuit_breaker_state_path):
                return result
            with open(self._circuit_breaker_state_path, "r", encoding="utf-8") as f:
                state = json_loads(f.read())  # R5-2
            now = time.time()
            if state.get("trading_paused_until", 0) > now:
                self._trading_paused_until = state["trading_paused_until"]
                self._pause_reason = state.get("pause_reason", "")
            if state.get("circuit_breaker_calm_until", 0) > now:
                self._circuit_breaker_calm_until = state["circuit_breaker_calm_until"]
            if state.get("circuit_breaker_shadow_mode", False):
                if state.get("circuit_breaker_shadow_until", 0) > now:
                    self._circuit_breaker_shadow_mode = True
                    self._circuit_breaker_shadow_until = state["circuit_breaker_shadow_until"]
            self._circuit_breaker_activated_at = state.get("circuit_breaker_activated_at", 0.0)
            saved_date = state.get("current_date")
            today = _get_tz_aware_now().strftime("%Y-%m-%d")
            if saved_date and saved_date == today:
                result = {
                    "daily_hard_stop_triggered": state.get("daily_hard_stop_triggered", False),
                    "daily_new_open_blocked": state.get("daily_new_open_blocked", False),
                    "daily_start_equity": state.get("daily_start_equity"),
                    "daily_peak_equity": state.get("daily_peak_equity", 0.0),
                    "daily_drawdown": state.get("daily_drawdown", 0.0),
                    "prev_5day_avg_profit": state.get("prev_5day_avg_profit", 0.0),
                    "current_date": saved_date,
                }
            else:
                _saved_hard_stop = state.get("daily_hard_stop_triggered", False)
                if _saved_hard_stop:
                    result["daily_hard_stop_triggered"] = True
                    logging.warning(
                        "[P0-R9-08] DR-08: 断路器hard_stop跨日保留(%s�?s), 需人工confirm_daily_resume()恢复",
                        saved_date, today,
                    )
                else:
                    result["daily_hard_stop_triggered"] = False
                result["daily_new_open_blocked"] = False
                logging.info(
                    "[SafetyMetaLayer] DR-08: 断路器状态跨�?%s�?s), 日回撤状态已重置",
                    saved_date, today,
                )
            logging.info(
                "[SafetyMetaLayer] 断路器状态已恢复: paused_until=%.1f calm_until=%.1f shadow=%s hard_stop=%s new_open_blocked=%s",
                self._trading_paused_until, self._circuit_breaker_calm_until,
                self._circuit_breaker_shadow_mode,
                result.get("daily_hard_stop_triggered", False), result.get("daily_new_open_blocked", False),
            )
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[SafetyMetaLayer] 断路器状态恢复失�? %s", e)
        return result

    def write_compliance_audit(self, caller_id: str, approval_context: Optional[Dict[str, Any]],
                               daily_drawdown: float) -> None:
        """写入合规审批记录到JSONL审计日志"""
        try:
            audit_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
            os.makedirs(audit_dir, exist_ok=True)
            audit_path = os.path.join(audit_dir, 'compliance_audit.jsonl')
            audit_entry = {
                'event': 'confirm_daily_resume',
                'timestamp': _get_tz_aware_now().isoformat(),
                'caller_id': caller_id,
                'approval_context': approval_context or {},
                'previous_state': {
                    'daily_hard_stop_triggered': True,
                    'daily_drawdown': daily_drawdown,
                },
                'new_state': {
                    'daily_hard_stop_triggered': False,
                    'daily_new_open_blocked': False,
                },
            }
            with open(audit_path, 'a', encoding='utf-8') as f:
                safe_jsonl_append_line(f, audit_entry)
            logging.info("[SafetyMetaLayer] CMP-P1-02: 合规审批记录已写�?%s", audit_path)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[SafetyMetaLayer] CMP-P1-02: 合规审批记录写入失败: %s", e)
