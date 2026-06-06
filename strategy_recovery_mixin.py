"""
strategy_recovery_mixin.py - StrategyRecoveryMixin
R27: 从strategy_core_service.py提取的灾备恢复方法

职责：
- 崩溃后自动化恢复 (_auto_recovery_flow)
- 看门狗重启 (_watchdog_restart)
- 检查点恢复 (recover_from_checkpoint)
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any, Dict

try:
    from ali2026v3_trading.config_params import CHINA_TZ
except Exception:
    from datetime import timezone, timedelta
    CHINA_TZ = timezone(timedelta(hours=8))


class StrategyRecoveryMixin:
    # DR-P1-04: 崩溃后自动化恢复
    def _auto_recovery_flow(self) -> bool:
        """DR-P1-04修复: 检测是否从崩溃中恢复，自动恢复状态

        返回 True 表示正常或恢复成功，False 表示恢复失败（进入SAFE_MODE）。
        """
        _recovery_marker = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', '_shutdown_gracefully.marker'
        )
        _recovery_log_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'recovery_events.jsonl'
        )

        _crashed = os.path.exists(_recovery_marker)

        if not _crashed:
            logging.info("[DR-P1-04] 正常启动，无需恢复流程")
            return True

        logging.warning("[DR-P1-04] 检测到非正常关闭(_shutdown_gracefully.marker存在)，启动自动化恢复...")

        _recovery_start = time.time()
        _recovery_success = True
        _recovery_details = {
            'positions_restored': False,
            'orders_restored': False,
            'circuit_breaker_restored': False,
            'idempotent_restored': False,
        }

        try:
            # 1. 恢复订单状态（从JSONL）
            self._ensure_order_service()
            if self._order_service is not None:
                try:
                    # R16-P0-RES-05修复: 调用已实现的_recover_order_state方法
                    if hasattr(self._order_service, '_recover_order_state'):
                        self._order_service._recover_order_state()
                        _recovery_details['orders_restored'] = True
                        logging.info("[DR-P1-04] 订单状态已从JSONL恢复")
                    else:
                        logging.warning("[DR-P1-04] OrderService缺少_recover_order_state方法")
                        _recovery_details['orders_restored'] = False
                except Exception as e:
                    logging.error("[DR-P1-04] 订单状态恢复失败: %s", e)
                    _recovery_details['orders_restored'] = False
        except Exception as e:
            logging.error("[DR-P1-04] 订单服务获取失败: %s", e)

        try:
            # 2. 恢复持仓状态（从JSONL）
            self._ensure_position_service()
            if self._position_service is not None:
                try:
                    if hasattr(self._position_service, '_recover_positions'):
                        self._position_service._recover_positions()
                    _recovery_details['positions_restored'] = True
                    logging.info("[DR-P1-04] 持仓状态已恢复")
                except Exception as e:
                    logging.error("[DR-P1-04] 持仓状态恢复失败: %s", e)
                    _recovery_details['positions_restored'] = False
        except Exception as e:
            logging.error("[DR-P1-04] 持仓服务获取失败: %s", e)

        try:
            # 3. 恢复断路器状态
            if hasattr(self, '_order_service') and self._order_service is not None:
                self._order_service._circuit_breaker_open = getattr(
                    self._order_service, '_circuit_breaker_open', False
                )
                self._order_service._consecutive_failures = getattr(
                    self._order_service, '_consecutive_failures', 0
                )
                _recovery_details['circuit_breaker_restored'] = True
                logging.info("[DR-P1-04] 断路器状态已恢复")
        except Exception as e:
            logging.error("[DR-P1-04] 断路器状态恢复失败: %s", e)

        try:
            # 4. 恢复幂等状态
            if hasattr(self, '_order_service') and self._order_service is not None:
                # R16-P0-RES-02修复: 调用已实现的_recover_idempotent_state方法
                if hasattr(self._order_service, '_recover_idempotent_state'):
                    self._order_service._recover_idempotent_state()
                    _recovery_details['idempotent_restored'] = True
                    logging.info("[DR-P1-04] 幂等状态已恢复")
                else:
                    logging.warning("[DR-P1-04] OrderService缺少_recover_idempotent_state方法")
                    _recovery_details['idempotent_restored'] = False
        except Exception as e:
            logging.error("[DR-P1-04] 幂等状态恢复失败: %s", e)

        # 记录恢复日志
        try:
            os.makedirs(os.path.dirname(_recovery_log_path), exist_ok=True)
            _recovery_record = {
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'event_type': 'auto_recovery',
                'strategy_id': self.strategy_id,
                'was_crashed': True,
                'recovery_success': all(_recovery_details.values()),
                'recovery_duration_sec': round(time.time() - _recovery_start, 3),
                'details': _recovery_details,
            }
            with open(_recovery_log_path, 'a', encoding='utf-8') as f:
                from ali2026v3_trading.serialization_utils import safe_jsonl_append_line
                safe_jsonl_append_line(f, _recovery_record)
        except Exception as e:
            logging.warning("[DR-P1-04] 恢复日志写入失败: %s", e)

        # 如果恢复失败，自动进入SAFE_MODE
        if not all(_recovery_details.values()):
            logging.critical("[DR-P1-04] 恢复未完全成功，进入SAFE_MODE（仅平仓不开仓）")
            self._health_pause_new_open = True
            self._is_trading = False
            # 尝试删除标记文件避免下次仍触发恢复
            try:
                if os.path.exists(_recovery_marker):
                    os.remove(_recovery_marker)
            except Exception:
                logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
                pass
            return False

        # 恢复成功，写入正常关闭标记
        try:
            if os.path.exists(_recovery_marker):
                os.remove(_recovery_marker)
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

        logging.info("[DR-P1-04] 自动化恢复完成，耗时 %.2fs", time.time() - _recovery_start)
        return True

    def _watchdog_restart(self, max_restarts=3, cooldown_sec=60):
        import time as _time
        for attempt in range(max_restarts):
            try:
                self._auto_recovery_flow()
                return True
            except Exception as e:
                logging.critical("[WATCHDOG] Restart attempt %d/%d failed: %s", attempt+1, max_restarts, e)
                if attempt < max_restarts - 1:
                    _time.sleep(cooldown_sec)
        return False

    # DFG-P1-13修复: 回测检查点恢复 — 重启时从检查点恢复策略状态
    def recover_from_checkpoint(self) -> Dict[str, Any]:
        """DFG-P1-13修复: 从回测检查点恢复策略状态

        检查点文件路径: logs/checkpoint_{strategy_id}.json
        恢复内容: 策略状态、持仓快照、风控参数、信号冷却等

        Returns:
            Dict: 恢复结果 {recovered: bool, fields: [...], errors: [...]}
        """
        _checkpoint_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs',
            f'checkpoint_{self.strategy_id}.json'
        )
        _result = {
            'recovered': False,
            'fields': [],
            'errors': [],
            'checkpoint_path': _checkpoint_path,
        }

        if not os.path.exists(_checkpoint_path):
            logging.debug("[DFG-P1-13] 检查点文件不存在: %s", _checkpoint_path)
            return _result

        try:
            with open(_checkpoint_path, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
        except Exception as e:
            logging.warning("[DFG-P1-13] 检查点文件读取失败: %s", e)
            _result['errors'].append(f'read_failed: {e}')
            return _result

        # 恢复策略状态
        try:
            _saved_state = checkpoint.get('strategy_state')
            if _saved_state:
                from ali2026v3_trading.strategy_lifecycle_mixin import StrategyState, _state_key
                for _s in StrategyState:
                    if _state_key(_s) == _saved_state or str(_s) == _saved_state:
                        self._state = _s
                        _result['fields'].append('strategy_state')
                        break
        except Exception as e:
            _result['errors'].append(f'state_restore_failed: {e}')

        # 恢复风控参数
        try:
            _risk_params = checkpoint.get('risk_params', {})
            if _risk_params:
                self._ensure_order_service()
                if self._order_service and hasattr(self._order_service, '_risk_service'):
                    _rs = self._order_service._risk_service
                    if _rs and isinstance(getattr(_rs, 'params', None), dict):
                        _rs.params.update(_risk_params)
                        _result['fields'].append('risk_params')
        except Exception as e:
            _result['errors'].append(f'risk_params_restore_failed: {e}')

        # 恢复信号冷却时间
        try:
            _cooldowns = checkpoint.get('signal_cooldowns', {})
            if _cooldowns and hasattr(self, '_signal_service') and self._signal_service:
                for _key, _ts in _cooldowns.items():
                    self._signal_service._cooldown_times[_key] = _ts
                _result['fields'].append('signal_cooldowns')
        except Exception as e:
            _result['errors'].append(f'cooldowns_restore_failed: {e}')

        # 恢复统计计数器
        try:
            _stats = checkpoint.get('stats', {})
            if _stats:
                for _k, _v in _stats.items():
                    if _k in self._stats:
                        self._stats[_k] = _v
                _result['fields'].append('stats')
        except Exception as e:
            _result['errors'].append(f'stats_restore_failed: {e}')

        _result['recovered'] = len(_result['fields']) > 0
        if _result['recovered']:
            logging.info("[DFG-P1-13] 检查点恢复成功: fields=%s", _result['fields'])
        else:
            logging.warning("[DFG-P1-13] 检查点恢复无有效字段")

        return _result
