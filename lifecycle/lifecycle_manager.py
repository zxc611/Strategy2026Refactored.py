"""
lifecycle_manager.py - LifecycleManager
Phase 2 (CC-02+CC-04): 从_LifecycleMixin提取的生命周期管理Manager

职责：
- 策略状态管理（_state, _is_running, _is_paused）
- 状态转换（transition_to）
- 生命周期操作（start/pause/resume/stop/destroy/prepare_restart）
- 状态查询（get_state/is_running/is_paused/is_trading/get_uptime）

Provider接口：通过provider（StrategyCoreService实例）获取共享属性：
- provider._state_lock: 状态锁
- provider._lock: 通用锁
- provider._scheduler_manager: 调度器
- provider._stats: 性能统计
- provider._event_bus: 事件总线

设计原则：
- Composition over Inheritance
- 独立可测试，不依赖Mixin继承链
- 通过provider接口显式依赖，替代self._隐式交叉引用
"""

import logging
import time
import threading
from datetime import datetime
from typing import Any, Dict, Optional

from ali2026v3_trading.strategy_lifecycle_mixin import StrategyState, _state_key, _state_is
from ali2026v3_trading.shared_utils import CHINA_TZ


class LifecycleManager:
    """生命周期管理Manager — 状态/转换/查询"""

    VALID_STATE_TRANSITIONS = {
        StrategyState.INITIALIZING: [StrategyState.RUNNING, StrategyState.ERROR, StrategyState.DEGRADED],
        StrategyState.RUNNING: [StrategyState.PAUSED, StrategyState.STOPPED, StrategyState.ERROR, StrategyState.DEGRADED, StrategyState.PARALLEL_RUNNING],
        StrategyState.PARALLEL_RUNNING: [StrategyState.RUNNING, StrategyState.PAUSED, StrategyState.STOPPED, StrategyState.ERROR, StrategyState.DEGRADED],
        StrategyState.PAUSED: [StrategyState.RUNNING, StrategyState.STOPPED, StrategyState.ERROR],
        StrategyState.DEGRADED: [StrategyState.RUNNING, StrategyState.STOPPED, StrategyState.ERROR, StrategyState.DEGRADED_STOP],
        StrategyState.ERROR: [StrategyState.INITIALIZING, StrategyState.STOPPED],
        StrategyState.DEGRADED_STOP: [StrategyState.STOPPED, StrategyState.INITIALIZING],
        StrategyState.STOPPED: [StrategyState.INITIALIZING],
    }

    def __init__(self, provider: Any = None):
        self._provider = provider
        self._state: StrategyState = StrategyState.INITIALIZING
        self._is_running: bool = False
        self._is_paused: bool = False
        self._is_trading: bool = True
        self._destroyed: bool = False
        self._initialized: bool = False
        self._state_lock: threading.RLock = threading.RLock()

    @property
    def state(self) -> StrategyState:
        return self._state

    @state.setter
    def state(self, value: StrategyState):
        self._state = value

    @property
    def is_running(self) -> bool:
        return self._is_running

    @is_running.setter
    def is_running(self, value: bool):
        self._is_running = value

    @property
    def is_paused(self) -> bool:
        return self._is_paused

    @is_paused.setter
    def is_paused(self, value: bool):
        self._is_paused = value

    @property
    def lock(self) -> threading.RLock:
        return self._state_lock

    @property
    def destroyed(self) -> bool:
        return self._destroyed

    @destroyed.setter
    def destroyed(self, value: bool):
        self._destroyed = value

    @property
    def initialized(self) -> bool:
        return self._initialized

    @initialized.setter
    def initialized(self, value: bool):
        self._initialized = value

    def transition_to(self, new_state: StrategyState) -> bool:
        """线程安全的状态转换方法"""
        with self._state_lock:
            old_state = self._state
            if not _state_is(old_state, new_state):
                valid_targets = self.VALID_STATE_TRANSITIONS.get(old_state, [])
                if not any(_state_is(new_state, t) for t in valid_targets):
                    logging.warning(
                        "[LifecycleManager] Invalid state transition: %s -> %s, allowed: %s",
                        old_state.value, new_state.value,
                        [s.value for s in valid_targets]
                    )
                    return False
            self._state = new_state
            _new_key = _state_key(new_state)
            if _new_key == 'running':
                self._is_running = True
            elif _new_key in ('paused', 'stopped', 'degraded', 'degraded_stop', 'error'):
                self._is_running = False
            if _new_key in ('degraded', 'stopped', 'degraded_stop'):
                try:
                    from ali2026v3_trading.config_params import invalidate_strategy_cache
                    _sid = None
                    if self._provider:
                        _sid = getattr(self._provider, 'strategy_id', None) or getattr(self._provider, '_strategy_id', None)
                    if _sid:
                        invalidate_strategy_cache(_sid)
                        logging.info("[LifecycleManager] 策略%s降级/停止，已清理策略级参数缓存", _sid)
                except Exception as _e:
                    logging.warning("[LifecycleManager] 缓存清理失败: %s", _e)
            logging.info("[LifecycleManager] State transition: %s -> %s", old_state.value, new_state.value)
            return True

    def get_state(self) -> StrategyState:
        return self._state

    def is_running_check(self) -> bool:
        return self._is_running and not self._is_paused

    def is_paused_check(self) -> bool:
        return self._is_paused

    def is_trading_check(self) -> bool:
        try:
            from ali2026v3_trading.scheduler_service import is_market_open
            if is_market_open():
                return True
        except Exception:
            pass
        return self._is_trading

    def get_uptime(self, stats: Optional[Dict] = None) -> float:
        if stats is None or not stats.get('start_time'):
            return 0.0
        elapsed = (datetime.now(CHINA_TZ) - stats['start_time']).total_seconds()
        return elapsed
