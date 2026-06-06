"""
strategy_checkpoint_mixin.py - StrategyCheckpointMixin
R27: 从strategy_core_service.py提取的检查点/状态同步方法

职责：
- 保存检查点 (save_checkpoint)
- 系统事件检查点回调 (_on_system_event_checkpoint)
- 状态同步到存储 (_sync_state_to_store)
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any, Dict

from ali2026v3_trading.config_params import json_dumps

try:
    from ali2026v3_trading.config_params import CHINA_TZ
except Exception:
    from datetime import timezone, timedelta
    CHINA_TZ = timezone(timedelta(hours=8))


class StrategyCheckpointMixin:
    def save_checkpoint(self) -> bool:
        """DFG-P1-13修复: 保存当前策略状态到检查点文件

        Returns:
            bool: 保存是否成功
        """
        _checkpoint_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs',
            f'checkpoint_{self.strategy_id}.json'
        )
        try:
            os.makedirs(os.path.dirname(_checkpoint_path), exist_ok=True)
            # P2-R11-11修复: 添加strategy_version到checkpoint文件，确保恢复时版本兼容性检查
            try:
                from ali2026v3_trading import __version__ as _strategy_version
            except ImportError:
                _strategy_version = 'unknown'
            checkpoint = {
                'strategy_id': self.strategy_id,
                'strategy_version': _strategy_version,  # P2-R11-11修复
                'strategy_state': str(self._state),
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'stats': dict(self._stats),
                'signal_cooldowns': {},
                'risk_params': {},
            }
            # 保存信号冷却
            if hasattr(self, '_signal_service') and self._signal_service:
                checkpoint['signal_cooldowns'] = dict(
                    getattr(self._signal_service, '_cooldown_times', {})
                )
            # 保存风控参数
            self._ensure_order_service()
            if self._order_service and hasattr(self._order_service, '_risk_service'):
                _rs = self._order_service._risk_service
                if _rs and isinstance(getattr(_rs, 'params', None), dict):
                    checkpoint['risk_params'] = dict(_rs.params)

            import tempfile
            _tmp_fd, _tmp_path = tempfile.mkstemp(dir=os.path.dirname(_checkpoint_path), suffix='.tmp')
            try:
                with os.fdopen(_tmp_fd, 'w', encoding='utf-8') as f:
                    f.write(json_dumps(checkpoint, indent=2))
                os.replace(_tmp_path, _checkpoint_path)
            except Exception:
                try:
                    os.unlink(_tmp_path)
                except OSError:
                    pass
                raise
            logging.debug("[DFG-P1-13] 检查点已保存: %s", _checkpoint_path)
            return True
        except Exception as e:
            logging.warning("[DFG-P1-13] 检查点保存失败: %s", e)
            return False

    # DFG-P1-13修复: 系统启动事件消费者 — 触发检查点恢复
    def _on_system_event_checkpoint(self, event: Any) -> None:
        """DFG-P1-13修复: 消费系统启动事件，触发检查点恢复

        当系统启动事件（STARTUP）发布时，自动尝试从检查点恢复策略状态，
        解决回测checkpoint数据无恢复消费者的数据流断裂问题。
        """
        try:
            _system_type = None
            if isinstance(event, dict):
                _system_type = event.get('system_type', '')
            elif hasattr(event, 'system_type'):
                _system_type = event.system_type
            if _system_type == 'STARTUP':
                _result = self.recover_from_checkpoint()
                if _result.get('recovered'):
                    logging.info("[DFG-P1-13] 系统启动时检查点恢复成功: fields=%s", _result.get('fields', []))
        except Exception:
            logging.warning("[R22-EP-P1] StrategyCoreService exception swallowed")
            pass

    # R13-P0-API-07修复: Mixin初始化依赖隐式属性设置，添加显式检查
    def _validate_mixin_attributes(self) -> None:
        """验证所有Mixin依赖的属性已正确初始化，防止隐式属性缺失导致运行时错误。"""
        _REQUIRED_ATTRS = {
            '_LifecycleMixin': ['_state', '_state_lock', '_lock'],
            'HistoricalKlineMixin': ['_historical_load_in_progress', '_historical_loader_lock'],
            'TickHandlerMixin': ['_tick_count', '_shard_router'],
        }
        for mixin_name, attrs in _REQUIRED_ATTRS.items():
            for attr in attrs:
                if not hasattr(self, attr):
                    raise AttributeError(
                        f"[R13-P0-API-07] {mixin_name} required attribute '{attr}' not initialized. "
                        f"Ensure _init_*_mixin() is called in __init__ before this check."
                    )

    def _sync_state_to_store(self) -> None:
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
        for key in state_val_keys:
            val = getattr(self, key, None)
            if val is not None:
                self._state_store.set(key, val)
        for key in state_ref_keys:
            val = getattr(self, key, None)
            if val is not None:
                try:
                    self._state_store.set(key, val)
                except (TypeError, copy.Error):
                    self._state_store.set_ref(key, val)
        try:
            if hasattr(self, 'storage'):
                storage = self.storage
                if storage is not None:
                    self._state_store.set_ref('storage', storage)
        except AttributeError as e:
            logging.debug("[R26-FIX] storage属性未初始化，跳过state_store.set_ref: %s", e)
        except Exception as e:
            logging.warning("[R26-FIX] state_store.set_ref('storage')失败: %s", e)
