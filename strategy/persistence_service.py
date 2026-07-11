# MODULE_ID: M1-257
"""persistence_service.py - 检查点与灾备恢复服务合并
合并自 checkpoint_service.py + recovery_service.py (2026-06-12)

职责：
- 保存检查点 (save_checkpoint)
- 系统事件检查点回调 (on_system_event_checkpoint)
- 验证服务依赖 (validate_service_dependencies)
- 状态同步到存储 (sync_state_to_store)
- 崩溃后自动化恢复 (auto_recovery_flow)
- 看门狗重启 (watchdog_restart)
- 检查点恢复 (recover_from_checkpoint)
"""
from __future__ import annotations

import copy
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from ali2026v3_trading.config.config_params import json_dumps
from ali2026v3_trading.infra.shared_utils import atomic_replace_file, CHINA_TZ


# ============================================================
# Section 1: CheckpointService (原 checkpoint_service.py)
# ============================================================

class CheckpointService:
    """独立检查点/状态同步服务 — 通过注入回调访问策略状态，不依赖Mixin继承"""

    def __init__(
        self,
        strategy_id: str,
        state_store: Optional[Any] = None,
        ensure_order_service_fn: Optional[Callable] = None,
        get_order_service_fn: Optional[Callable] = None,
        get_position_service_fn: Optional[Callable] = None,
        get_signal_service_fn: Optional[Callable] = None,
        get_stats_fn: Optional[Callable] = None,
        get_state_fn: Optional[Callable] = None,
        recover_from_checkpoint_fn: Optional[Callable] = None,
        # _sync_state_to_store 需要大量属性访问，使用 provider 模式
        get_attr_fn: Optional[Callable] = None,
        set_attr_fn: Optional[Callable] = None,
        has_attr_fn: Optional[Callable] = None,
    ):
        self._strategy_id = strategy_id
        self._state_store = state_store
        self._ensure_order_service = ensure_order_service_fn
        self._get_order_service = get_order_service_fn
        self._get_position_service = get_position_service_fn
        self._get_signal_service = get_signal_service_fn
        self._get_stats = get_stats_fn
        self._get_state = get_state_fn
        self._recover_from_checkpoint = recover_from_checkpoint_fn
        self._get_attr = get_attr_fn
        self._set_attr = set_attr_fn
        self._has_attr = has_attr_fn

    # -- helper: 获取服务实例 --
    def _order_service(self):
        fn = self._get_order_service
        return fn() if fn else None

    def _signal_service(self):
        fn = self._get_signal_service
        return fn() if fn else None

    def _stats(self):
        fn = self._get_stats
        return fn() if fn else None

    def _state(self):
        fn = self._get_state
        return fn() if fn else None

    def save_checkpoint(self) -> bool:
        """DFG-P1-13修复: 保存当前策略状态到检查点文件"""
        _checkpoint_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs',
            f'checkpoint_{self._strategy_id}.json'
        )
        try:
            os.makedirs(os.path.dirname(_checkpoint_path), exist_ok=True)
            try:
                from ali2026v3_trading import __version__ as _strategy_version
            except ImportError:
                _strategy_version = 'unknown'

            _cur_state = self._state()
            _cur_stats = self._stats()
            checkpoint = {
                'strategy_id': self._strategy_id,
                'strategy_version': _strategy_version,
                'strategy_state': str(_cur_state) if _cur_state is not None else '',
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'stats': dict(_cur_stats) if _cur_stats else {},
                'signal_cooldowns': {},
                'risk_params': {},
            }
            _ss = self._signal_service()
            if _ss:
                checkpoint['signal_cooldowns'] = dict(
                    getattr(_ss, '_cooldown_times', {})
                )
            if self._ensure_order_service:
                self._ensure_order_service()
            _os = self._order_service()
            if _os and hasattr(_os, '_risk_service'):
                _rs = _os._risk_service
                if _rs and isinstance(getattr(_rs, 'params', None), dict):
                    checkpoint['risk_params'] = dict(_rs.params)

            _result = atomic_replace_file(_checkpoint_path, json_dumps(checkpoint, indent=2))
            if not _result['success']:
                raise RuntimeError(_result.get('error', 'atomic_replace_file failed'))
            logging.debug("[DFG-P1-13] 检查点已保存: %s", _checkpoint_path)
            return True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[DFG-P1-13] 检查点保存失败: %s", e)
            return False

    def on_system_event_checkpoint(self, event: Any) -> None:
        """DFG-P1-13修复: 消费系统启动事件，触发检查点恢复"""
        try:
            _system_type = None
            if isinstance(event, dict):
                _system_type = event.get('system_type', '')
            elif hasattr(event, 'system_type'):
                _system_type = event.system_type
            if _system_type == 'STARTUP':
                if self._recover_from_checkpoint:
                    _result = self._recover_from_checkpoint()
                else:
                    _result = {}
                if _result.get('recovered'):
                    logging.info("[DFG-P1-13] 系统启动时检查点恢复成功: fields=%s", _result.get('fields', []))
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.warning("[R22-EP-P1] CheckpointService exception swallowed")
            pass

    def validate_service_dependencies(self) -> None:
        """验证所有服务依赖的回调已正确注入，防止运行时错误。"""
        _REQUIRED_FNS = {
            'RecoveryService': [
                '_ensure_order_service', '_get_order_service',
                '_get_position_service', '_get_stats',
            ],
            'CheckpointService': [
                '_get_order_service', '_get_stats', '_get_state',
            ],
        }
        for service_name, fns in _REQUIRED_FNS.items():
            for fn_name in fns:
                if getattr(self, fn_name, None) is None:
                    raise AttributeError(
                        f"[R13-P0-API-07] {service_name} required callback '{fn_name}' not injected. "
                        f"Ensure all required callbacks are provided during service creation."
                    )

    def sync_state_to_store(self) -> None:
        """将策略状态同步到 state_store"""
        if self._state_store is None:
            return
        state_val_keys = [
            'strategy_id', '_is_running', '_is_paused', '_is_trading',
            '_destroyed', '_initialized',
            '_runtime_market_center', '_fallback_market_center',
            '_health_pause_new_open', '_connection_state', '_lifecycle_run_id',
            '_processed_trade_ids', '_heartbeat_consecutive_failures',
            '_last_heartbeat_check', '_heartbeat_interval_sec', '_heartbeat_max_failures',
            '_connection_stale_threshold_sec', '_reconnect_attempts',
            '_max_reconnect_attempts', '_degraded_since',
            '_recovery_check_interval_sec', '_last_recovery_check',
            '_storage_warm_started', '_platform_subscribe_completed',
            '_config_loaded', '_option_status_log_interval',
            '_last_option_status_log_time', '_pqs_tick_count',
            '_pqs_last_update_time',
            '_stop_executed', '_cert_path', '_init_pending',
            '_runtime_market_center_ref', '_start_executed',
            '_callbacks_enabled', '_runtime_strategy_ref',
            '_init_error', '_api_ready', '_last_tick_time',
            '_ssl_verify', '_init_instruments_result', '_option_ids',
            '_storage_init_error', '_future_ids',
            '_analytics_warmup_done', '_is_real_strategy',
        ]
        state_ref_keys = [
            'params', '_state', '_subscribed_instruments',
            '_runtime_strategy_host', '_stats', '_e2e_counters',
            '_e2e_first_tick_set', '_e2e_shard_enqueued', '_e2e_shard_persisted',
            '_lock', '_stop_requested',
            '_signal_service', '_order_service', '_position_service',
            '_hft_engine', '_state_param_manager', '_safety_meta_layer',
            't_type_service', '_scheduler_manager', '_event_bus',
            '_data_access',
            '_snapshot_collector', '_shadow_engine', '_strategy_ecosystem',
            '_box_spring_strategy', '_instrument_mgr', '_historical_mgr',
            '_strategy', '_AUTO_CALLER_IDS', '_runtime_config',
            '_state_store', '_callback_group', '_storage_warm_timer',
            '_params', '_strategy_executor', '_ssl_context',
            '_storage', '_atexit_cleanup_executor',
            '_ensure_order_service', '_ensure_position_service',
            '_ensure_runtime_config_loaded', '_sync_state_to_store',
            '_ensure_hft_engine', '_ensure_snapshot_collector',
            '_start_output_mode_ui', '_destroy_output_mode_ui',
            '_extract_runtime_market_center', '_extract_contract_year_month',
            '_init_tick_handler_mixin', '_init_historical_kline_mixin',
            '_register_callbacks', '_on_system_event_checkpoint',
            '_check_ecosystem_exclusion', '_feed_shadow_engine',
            '_publish_event', '_get_fallback_market_center',
            '_schedule_storage_warmup', '_log_tick_summary',
            '_validate_mixin_attributes', '_update_position_from_trade',
            '_resolve_open_reason',
        ]
        _get = self._get_attr
        if _get is None:
            return

        for key in state_val_keys:
            val = _get(key)
            if val is not None:
                self._state_store.set(key, val)
        for key in state_ref_keys:
            val = _get(key)
            if val is not None:
                # FIX-20260708: state_ref_keys应使用set_ref(引用)而非set(深拷贝)
                # 原代码先尝试set()(深拷贝)，仅当TypeError/copy.Error才回退set_ref()
                # 导致_stats(dict)被深拷贝，后续p._stats['start_time']=...等修改不可见
                # 表现为：运行时长=0:00:00, 信号数量=0, 交易数量=0（状态报告恒为初始快照）
                try:
                    self._state_store.set_ref(key, val)
                except (TypeError, AttributeError):
                    # set_ref失败时回退到set(深拷贝)，保证至少能存储
                    try:
                        self._state_store.set(key, val)
                    except (TypeError, copy.Error):
                        pass
        try:
            _has = self._has_attr or (lambda k: _get(k) is not None)
            if _has('storage'):
                storage = _get('storage')
                if storage is not None:
                    self._state_store.set_ref('storage', storage)
        except AttributeError as e:
            logging.debug("[R26-FIX] storage属性未初始化，跳过state_store.set_ref: %s", e)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.warning("[R26-FIX] state_store.set_ref('storage')失败: %s", e)


# ============================================================
# Section 2: RecoveryService (原 recovery_service.py)
# ============================================================

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
        """DR-P1-04修复: 检测是否从崩溃中恢复，自动恢复状态"""
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
            if self._ensure_order_service:
                self._ensure_order_service()
            _os = self._order_service()
            if _os is not None:
                try:
                    if hasattr(_os, '_recover_order_state'):
                        _os._recover_order_state()
                        _recovery_details['orders_restored'] = True
                        logging.info("[DR-P1-04] 订单状态已从JSONL恢复")
                    else:
                        logging.warning("[DR-P1-04] OrderService缺少。recover_order_state方法")
                        _recovery_details['orders_restored'] = False
                except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                    logging.error("[DR-P1-04] 订单状态恢复失败: %s", e)
                    _recovery_details['orders_restored'] = False
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.error("[DR-P1-04] 订单服务获取失败: %s", e)

        try:
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
            _os3 = self._order_service()
            if _os3 is not None:
                if hasattr(_os3, '_recover_idempotent_state'):
                    _os3._recover_idempotent_state()
                    _recovery_details['idempotent_restored'] = True
                    logging.info("[DR-P1-04] 幂等状态已恢复")
                else:
                    logging.warning("[DR-P1-04] OrderService缺少。recover_idempotent_state方法")
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

        if not all(_recovery_details.values()):
            logging.critical("[DR-P1-04] 恢复未完全成功，进入SAFE_MODE（仅平仓不开仓）")
            if self._set_health_pause_new_open:
                self._set_health_pause_new_open(True)
            if self._set_is_trading:
                self._set_is_trading(False)
            try:
                if os.path.exists(_recovery_marker):
                    os.remove(_recovery_marker)
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.warning("[R22-EP-P1] RecoveryService exception swallowed")
                pass
            return False

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

    def recover_from_checkpoint(self) -> Dict[str, Any]:
        """DFG-P1-13修复: 从回测检查点恢复策略状态"""
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
                from ali2026v3_trading.config.config_params import json_loads
                checkpoint = json_loads(f.read())
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
