"""
recovery_service.py - RecoveryService (G2b Mixin消除)
从StrategyRecoveryMixin提取的独立灾备恢复服务，使用组合替代继承

职责：
- 崩溃后自动化恢复 (auto_recovery_flow)
- 看门狗重启 (watchdog_restart)
- 检查点恢复 (recover_from_checkpoint)
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from ali2026v3_trading.infra.shared_utils import CHINA_TZ


class RecoveryService:
    """独立灾备恢复服务 — 通过注入回调访问策略状态，不依赖Mixin继承"""

    def __init__(
        self,
        strategy_id: str,
        state_store: Optional[Any] = None,
        ensure_order_service_fn: Optional[Callable] = None,
        ensure_position_service_fn: Optional[Callable] = None,
        get_order_service_fn: Optional[Callable] = None,
        get_position_service_fn: Optional[Callable] = None,
        get_signal_service_fn: Optional[Callable] = None,
        get_stats_fn: Optional[Callable] = None,
        get_state_fn: Optional[Callable] = None,
        set_state_fn: Optional[Callable] = None,
        get_health_pause_new_open_fn: Optional[Callable] = None,
        set_health_pause_new_open_fn: Optional[Callable] = None,
        get_is_trading_fn: Optional[Callable] = None,
        set_is_trading_fn: Optional[Callable] = None,
    ):
        self._strategy_id = strategy_id
        self._state_store = state_store
        self._ensure_order_service = ensure_order_service_fn
        self._ensure_position_service = ensure_position_service_fn
        self._get_order_service = get_order_service_fn
        self._get_position_service = get_position_service_fn
        self._get_signal_service = get_signal_service_fn
        self._get_stats = get_stats_fn
        self._get_state = get_state_fn
        self._set_state = set_state_fn
        self._get_health_pause_new_open = get_health_pause_new_open_fn
        self._set_health_pause_new_open = set_health_pause_new_open_fn
        self._get_is_trading = get_is_trading_fn
        self._set_is_trading = set_is_trading_fn

    # -- helper: 获取服务实例 --
    def _order_service(self):
        fn = self._get_order_service
        return fn() if fn else None

    def _position_service(self):
        fn = self._get_position_service
        return fn() if fn else None

    def _signal_service(self):
        fn = self._get_signal_service
        return fn() if fn else None

    def _stats(self):
        fn = self._get_stats
        return fn() if fn else None

    # DR-P1-04: 崩溃后自动化恢复
    def auto_recovery_flow(self) -> bool:
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
            if self._ensure_order_service:
                self._ensure_order_service()
            _os = self._order_service()
            if _os is not None:
                try:
                    # R16-P0-RES-05修复: 调用已实现的_recover_order_state方法
                    if hasattr(_os, '_recover_order_state'):
                        _os._recover_order_state()
                        _recovery_details['orders_restored'] = True
                        logging.info("[DR-P1-04] 订单状态已从JSONL恢复")
                    else:
                        logging.warning("[DR-P1-04] OrderService缺少_recover_order_state方法")
                        _recovery_details['orders_restored'] = False
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.error("[DR-P1-04] 订单状态恢复失败: %s", e)
                    _recovery_details['orders_restored'] = False
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[DR-P1-04] 订单服务获取失败: %s", e)

        try:
            # 2. 恢复持仓状态（从JSONL）
            if self._ensure_position_service:
                self._ensure_position_service()
            _ps = self._position_service()
            if _ps is not None:
                try:
                    if hasattr(_ps, '_recover_positions'):
                        _ps._recover_positions()
                    _recovery_details['positions_restored'] = True
                    logging.info("[DR-P1-04] 持仓状态已恢复")
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.error("[DR-P1-04] 持仓状态恢复失败: %s", e)
                    _recovery_details['positions_restored'] = False
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[DR-P1-04] 持仓服务获取失败: %s", e)

        try:
            # 3. 恢复断路器状态
            _os2 = self._order_service()
            if _os2 is not None:
                _os2._circuit_breaker_open = getattr(
                    _os2, '_circuit_breaker_open', False
                )
                _os2._consecutive_failures = getattr(
                    _os2, '_consecutive_failures', 0
                )
                _recovery_details['circuit_breaker_restored'] = True
                logging.info("[DR-P1-04] 断路器状态已恢复")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[DR-P1-04] 断路器状态恢复失败: %s", e)

        try:
            # 4. 恢复幂等状态
            _os3 = self._order_service()
            if _os3 is not None:
                # R16-P0-RES-02修复: 调用已实现的_recover_idempotent_state方法
                if hasattr(_os3, '_recover_idempotent_state'):
                    _os3._recover_idempotent_state()
                    _recovery_details['idempotent_restored'] = True
                    logging.info("[DR-P1-04] 幂等状态已恢复")
                else:
                    logging.warning("[DR-P1-04] OrderService缺少_recover_idempotent_state方法")
                    _recovery_details['idempotent_restored'] = False
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[DR-P1-04] 幂等状态恢复失败: %s", e)

        # 记录恢复日志
        try:
            os.makedirs(os.path.dirname(_recovery_log_path), exist_ok=True)
            _recovery_record = {
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'event_type': 'auto_recovery',
                'strategy_id': self._strategy_id,
                'was_crashed': True,
                'recovery_success': all(_recovery_details.values()),
                'recovery_duration_sec': round(time.time() - _recovery_start, 3),
                'details': _recovery_details,
            }
            with open(_recovery_log_path, 'a', encoding='utf-8') as f:
                from ali2026v3_trading.infra.serialization_utils import safe_jsonl_append_line
                safe_jsonl_append_line(f, _recovery_record)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[DR-P1-04] 恢复日志写入失败: %s", e)

        # 如果恢复失败，自动进入SAFE_MODE
        if not all(_recovery_details.values()):
            logging.critical("[DR-P1-04] 恢复未完全成功，进入SAFE_MODE（仅平仓不开仓）")
            if self._set_health_pause_new_open:
                self._set_health_pause_new_open(True)
            if self._set_is_trading:
                self._set_is_trading(False)
            # 尝试删除标记文件避免下次仍触发恢复
            try:
                if os.path.exists(_recovery_marker):
                    os.remove(_recovery_marker)
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.warning("[R22-EP-P1] RecoveryService exception swallowed")
                pass
            return False

        # 恢复成功，写入正常关闭标记
        try:
            if os.path.exists(_recovery_marker):
                os.remove(_recovery_marker)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.warning("[R22-EP-P1] RecoveryService exception swallowed")
            pass

        logging.info("[DR-P1-04] 自动化恢复完成，耗时 %.2fs", time.time() - _recovery_start)
        return True

    def watchdog_restart(self, max_restarts: int = 3, cooldown_sec: int = 60) -> bool:
        import time as _time
        for attempt in range(max_restarts):
            try:
                self.auto_recovery_flow()
                return True
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.critical("[WATCHDOG] Restart attempt %d/%d failed: %s", attempt + 1, max_restarts, e)
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
            f'checkpoint_{self._strategy_id}.json'
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
                checkpoint = json_loads(f.read())  # R5-2
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logging.warning("[DFG-P1-13] 检查点文件读取失败: %s", e)
            _result['errors'].append(f'read_failed: {e}')
            return _result

        # 恢复策略状态
        try:
            _saved_state = checkpoint.get('strategy_state')
            if _saved_state:
                from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState, _state_key
                for _s in StrategyState:
                    if _state_key(_s) == _saved_state or str(_s) == _saved_state:
                        if self._set_state:
                            self._set_state(_s)
                        _result['fields'].append('strategy_state')
                        break
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            _result['errors'].append(f'state_restore_failed: {e}')

        # 恢复风控参数
        try:
            _risk_params = checkpoint.get('risk_params', {})
            if _risk_params:
                if self._ensure_order_service:
                    self._ensure_order_service()
                _os = self._order_service()
                if _os and hasattr(_os, '_risk_service'):
                    _rs = _os._risk_service
                    if _rs and isinstance(getattr(_rs, 'params', None), dict):
                        _rs.params.update(_risk_params)
                        _result['fields'].append('risk_params')
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            _result['errors'].append(f'risk_params_restore_failed: {e}')

        # 恢复信号冷却时间
        try:
            _cooldowns = checkpoint.get('signal_cooldowns', {})
            _ss = self._signal_service()
            if _cooldowns and _ss:
                for _key, _ts in _cooldowns.items():
                    _ss._cooldown_times[_key] = _ts
                _result['fields'].append('signal_cooldowns')
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            _result['errors'].append(f'cooldowns_restore_failed: {e}')

        # 恢复统计计数器
        try:
            _stats = checkpoint.get('stats', {})
            _cur_stats = self._stats()
            if _stats and _cur_stats is not None:
                for _k, _v in _stats.items():
                    if _k in _cur_stats:
                        _cur_stats[_k] = _v
                _result['fields'].append('stats')
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            _result['errors'].append(f'stats_restore_failed: {e}')

        _result['recovered'] = len(_result['fields']) > 0
        if _result['recovered']:
            logging.info("[DFG-P1-13] 检查点恢复成功: fields=%s", _result['fields'])
        else:
            logging.warning("[DFG-P1-13] 检查点恢复无有效字段")

        return _result
