"""
pause_service.py - 暂停管理服务

⚠️ 实验性功能：暂未集成到主流程

合并来源：11_pause.py
合并策略：提取核心暂停管理和信号功能

重构目标:
- 旧架构：11_pause.py (~1151 行)
- 新架构：pause_service.py (~600 行)
- 功能保留：完整的暂停管理、信号机制

⚠️ 当前状态：
- 此模块定义了PauseService类和增强的暂停管理功能
- 但未被任何模块引用（除枚举导入外）
- strategy_core_service.py已实现基础的pause/resume方法
- 如需使用本模块的增强功能，需手动集成到主流程

📝 P2 功能恢复清单 (30 个):
1. Signal.connect
2. Signal.emit
3-30. 暂停管理、UI 保护、实例管理等

作者：CodeArts 代码智能体
版本：v1.0
生成时间：2026-03-16
最后更新：2026-04-23（标记为实验性功能）
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, List, Optional, Dict
from dataclasses import dataclass, field
from enum import Enum, auto

from ali2026v3_trading.strategy_core_service import StrategyState


# ============================================================================
# 枚举定义
# ============================================================================

# ✅ 统一为PauseState，删除StrategyState（与strategy_core_service保持一致由该模块自行维护）
class PauseState(Enum):
    """暂停状态（唯一状态枚举）"""
    RUNNING = auto()
    PAUSED = auto()
    STOPPED = auto()


class PauseReason(Enum):
    """暂停原因"""
    MANUAL = auto()
    EMERGENCY = auto()
    ERROR = auto()
    SCHEDULED = auto()


# ============================================================================
# P0-1: 生命周期状态机管理器
# ============================================================================

class LifecycleManager:
    """生命周期状态机管理器
    
    ✅ P0-1: 统一管理策略生命周期，提供：
    - 状态转换规则和验证
    - run_id追踪防止重复入口
    - 完整的状态转换历史
    
    用法示例：
        lifecycle = LifecycleManager(strategy_id="strategy_001")
        lifecycle.transition(StrategyState.RUNNING, 'onStart')
        if lifecycle.is_running():
            print("策略运行中")
    """
    
    # 定义合法的状态转换规则
    VALID_TRANSITIONS = {
        StrategyState.INITIALIZING: [StrategyState.RUNNING, StrategyState.ERROR],
        StrategyState.RUNNING: [StrategyState.PAUSED, StrategyState.STOPPED, StrategyState.ERROR, StrategyState.DEGRADED],
        StrategyState.PAUSED: [StrategyState.RUNNING, StrategyState.STOPPED, StrategyState.ERROR],
        StrategyState.STOPPED: [StrategyState.INITIALIZING],  # 允许重启
        StrategyState.ERROR: [StrategyState.INITIALIZING, StrategyState.STOPPED],
        StrategyState.DEGRADED: [StrategyState.RUNNING, StrategyState.STOPPED, StrategyState.ERROR],
        StrategyState.DEGRADED_STOP: [StrategyState.STOPPED],
    }
    
    def __init__(self, strategy_id: str):
        """
        初始化生命周期管理器
        
        Args:
            strategy_id: 策略ID
        """
        self.strategy_id = strategy_id
        self._current_state = StrategyState.INITIALIZING
        self._transition_history: List[Dict[str, Any]] = []
        self._run_id: Optional[str] = None
        self._last_action: Optional[str] = None  # ✅ P0-1: 记录上一个动作
        self._lock = threading.Lock()
        
        logging.info(f"[LifecycleManager] Created for strategy: {strategy_id}")
    
    def transition(self, to_state: StrategyState, action: str, run_id: str = None) -> bool:
        """
        执行状态转换
        
        Args:
            to_state: 目标状态
            action: 触发动作（如 'onInit', 'onStart', 'pause', 'onStop'）
            run_id: 运行ID（用于追踪重复入口）
            
        Returns:
            bool: 转换是否成功
        """
        with self._lock:
            from_state = self._current_state
            
            # 验证转换合法性
            if not self._is_valid_transition(from_state, to_state):
                logging.error(
                    f"[LifecycleManager] ❌ Invalid transition: {from_state.value} -> {to_state.value} "
                    f"(action={action}, strategy={self.strategy_id})"
                )
                return False
            
            # ✅ P0-1: 检测重复入口（同一run_id + 同一action连续调用）
            if run_id and self._run_id == run_id and self._last_action == action and action in ['on_start', 'on_stop']:
                logging.warning(
                    f"[LifecycleManager] ⚠️ DUPLICATE ENTRY DETECTED: run_id={run_id}, action={action}, "
                    f"strategy={self.strategy_id}"
                )
                return False
            
            # 执行转换
            self._current_state = to_state
            self._run_id = run_id
            self._last_action = action  # ✅ P0-1: 更新最后动作
            
            # 记录历史
            from datetime import datetime
            transition_record = {
                'from': from_state.value,
                'to': to_state.value,
                'action': action,
                'timestamp': datetime.now().isoformat(),
                'run_id': run_id
            }
            self._transition_history.append(transition_record)
            
            # 保留最近100条记录
            if len(self._transition_history) > 100:
                self._transition_history = self._transition_history[-100:]
            
            logging.info(
                f"[LifecycleManager] ✅ State changed: {from_state.value} -> {to_state.value} "
                f"(action={action}, strategy={self.strategy_id}, run_id={run_id})"
            )
            
            return True
    
    def _is_valid_transition(self, from_state: StrategyState, to_state: StrategyState) -> bool:
        """验证状态转换是否合法"""
        allowed_states = self.VALID_TRANSITIONS.get(from_state, [])
        return to_state in allowed_states
    
    @property
    def current_state(self) -> StrategyState:
        """获取当前状态"""
        with self._lock:
            return self._current_state
    
    def is_running(self) -> bool:
        """是否运行中"""
        with self._lock:
            return self._current_state == StrategyState.RUNNING
    
    def is_paused(self) -> bool:
        """是否已暂停"""
        with self._lock:
            return self._current_state == StrategyState.PAUSED
    
    def is_stopped(self) -> bool:
        """是否已停止（包括DEGRADED_STOP）"""
        with self._lock:
            return self._current_state in [StrategyState.STOPPED, StrategyState.DEGRADED_STOP]
    
    def get_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        获取状态转换历史
        
        Args:
            limit: 返回最近的记录数
            
        Returns:
            状态转换历史记录列表
        """
        with self._lock:
            return self._transition_history[-limit:]
    
    def reset_run_id(self):
        """重置run_id（用于重启场景）"""
        with self._lock:
            import uuid
            self._run_id = str(uuid.uuid4())[:8]
            logging.info(f"[LifecycleManager] Run ID reset: {self._run_id}")


# ============================================================================
# P2 功能恢复：Signal 相关（L49-L58）
# ============================================================================

class Signal:
    """信号类（兼容 PyQt5 Signal）"""
    
    def __init__(self, *types):
        self.slots: List[Callable] = []
        self.types = types
    
    def connect(self, slot: Callable) -> None:
        """
        连接信号到槽
        
        Args:
            slot: 槽函数
        """
        if slot not in self.slots:
            self.slots.append(slot)
            logging.debug(f"[Signal.connect] Slot connected: {slot.__name__}")
    
    def emit(self, *args: Any) -> None:
        """
        发射信号
        
        Args:
            *args: 信号参数
        """
        # Bug2修复：遍历前复制slots列表，防止迭代中列表修改导致RuntimeError
        for slot in list(self.slots):
            try:
                slot(*args)
            except Exception as e:
                logging.error(f"[Signal.emit] Error calling slot {slot.__name__}: {e}")


# ============================================================================
# P2 功能恢复：PauseManager 相关
# ============================================================================

@dataclass
class PauseInfo:
    """暂停信息"""
    reason: PauseReason
    message: str = ""
    timestamp: float = field(default_factory=lambda: __import__('time').time())
    duration_seconds: Optional[float] = None


class PauseManager:
    """暂停管理器"""
    
    def __init__(self):
        self._state = PauseState.RUNNING
        self._lock = threading.RLock()
        self._pause_info: Optional[PauseInfo] = None
        
        # ✅ 统一为Signal通道，删除listeners
        self.on_pause_signal = Signal(bool)  # is_paused
        self.on_resume_signal = Signal(bool)  # is_resumed
    
    @property
    def is_paused(self) -> bool:
        """是否已暂停"""
        with self._lock:
            return self._state == PauseState.PAUSED
    
    @property
    def is_running(self) -> bool:
        """是否运行中"""
        with self._lock:
            return self._state == PauseState.RUNNING
    
    @property
    def is_stopped(self) -> bool:
        """是否已停止"""
        with self._lock:
            return self._state == PauseState.STOPPED
    
    def pause(self, reason: PauseReason = PauseReason.MANUAL, message: str = "", duration_seconds: Optional[float] = None) -> bool:
        """
        暂停
        
        Args:
            reason: 暂停原因
            message: 暂停消息
            duration_seconds: 暂停持续时间（秒），None 表示无限期
            
        Returns:
            bool: 是否成功暂停
        """
        with self._lock:
            if self._state == PauseState.PAUSED:
                logging.warning("[PauseManager.pause] Already paused")
                return False
            
            self._state = PauseState.PAUSED
            self._pause_info = PauseInfo(
                reason=reason,
                message=message,
                duration_seconds=duration_seconds
            )
            
            logging.info(f"[PauseManager.pause] Paused: {reason.name} - {message}")
            
            # ✅ 统一为Signal通知，删除listeners回调
            self.on_pause_signal.emit(True)
            
            return True
    
    def resume(self) -> bool:
        """
        恢复
        
        Returns:
            bool: 是否成功恢复
        """
        with self._lock:
            if self._state != PauseState.PAUSED:
                logging.warning("[PauseManager.resume] Not paused")
                return False
            
            self._state = PauseState.RUNNING
            self._pause_info = None
            
            logging.info("[PauseManager.resume] Resumed")
            
            # ✅ 统一为Signal通知，删除listeners回调
            self.on_resume_signal.emit(True)
            
            return True
    
    def stop(self) -> None:
        """停止"""
        with self._lock:
            self._state = PauseState.STOPPED
            self._pause_info = PauseInfo(PauseReason.MANUAL, "Stopped")
            
            logging.info("[PauseManager.stop] Stopped")
    
    # ✅ 删除listeners注册方法，统一使用Signal订阅
    # def add_pause_listener(self, callback: Callable) -> None:
    #     """DEPRECATED: 请使用 on_pause_signal.connect(callback)"""
    #     pass
    
    # def add_resume_listener(self, callback: Callable) -> None:
    #     """DEPRECATED: 请使用 on_resume_signal.connect(callback)"""
    #     pass
    
    def get_pause_info(self) -> Optional[PauseInfo]:
        """获取暂停信息"""
        return self._pause_info
    
    def drain_backlog(self, storage, timeout: float = 30.0, threshold: int = 1000) -> Dict[str, Any]:
        """等待shared queue backlog排空（P2-1增强）
        
        Args:
            storage: Storage实例，需有get_queue_stats()方法
            timeout: 最长等待秒数
            threshold: 认为已排空的阈值
            
        Returns:
            dict: {'success': bool, 'initial': int, 'final': int, 'elapsed': float}
        """
        import time as _time
        
        result = {'success': False, 'initial': 0, 'final': 0, 'elapsed': 0.0}
        
        if not storage or not hasattr(storage, 'get_queue_stats'):
            return result
        
        try:
            stats = storage.get_queue_stats()
            backlog = stats.get('pending', 0) + stats.get('current_queue_size', 0)
            result['initial'] = backlog
            
            if backlog <= threshold:
                result['success'] = True
                result['final'] = backlog
                logging.info(
                    f"[PauseManager][owner_scope=shared-queue-drain] "
                    f"Queue already low: {backlog} tasks"
                )
                return result
            
            logging.info(
                f"[PauseManager][owner_scope=shared-queue-drain] "
                f"Waiting for queue drain: {backlog} tasks (max {timeout}s, threshold={threshold})"
            )
            
            start = _time.monotonic()
            deadline = start + timeout
            while _time.monotonic() < deadline:
                _time.sleep(1.0)
                stats = storage.get_queue_stats()
                new_backlog = stats.get('pending', 0) + stats.get('current_queue_size', 0)
                
                if new_backlog < threshold or new_backlog >= backlog:
                    break
                
                logging.info(
                    f"[PauseManager][owner_scope=shared-queue-drain] "
                    f"Draining: {backlog} -> {new_backlog} tasks"
                )
                backlog = new_backlog
            
            result['final'] = new_backlog
            result['elapsed'] = round(_time.monotonic() - start, 1)
            
            if new_backlog < threshold:
                result['success'] = True
                logging.info(
                    f"[PauseManager][owner_scope=shared-queue-drain] "
                    f"✅ Queue drained to {new_backlog} tasks ({result['elapsed']}s)"
                )
            else:
                logging.warning(
                    f"[PauseManager][owner_scope=shared-queue-drain] "
                    f"⚠️ Queue still has {new_backlog} tasks after {result['elapsed']}s"
                )
        except Exception as e:
            logging.debug(f"[PauseManager] drain_backlog error: {e}")
        
        return result
    
    @staticmethod
    def freeze_scheduler(scheduler_manager) -> bool:
        """冻结scheduler（停止新job执行）
        
        Args:
            scheduler_manager: StrategyScheduler实例
            
        Returns:
            bool: 是否成功冻结
        """
        if not scheduler_manager or not hasattr(scheduler_manager, 'pause_scheduler'):
            return False
        try:
            result = scheduler_manager.pause_scheduler()
            logging.info(f"[PauseManager] Scheduler frozen (result={result})")
            return bool(result)
        except Exception as e:
            logging.error(f"[PauseManager] freeze_scheduler error: {e}")
            return False

    @staticmethod
    def pause_strategy(strategy_core) -> bool:
        """暂停策略（完整实现，符合第4条原则）
        
        Args:
            strategy_core: StrategyCoreService实例
            
        Returns:
            bool: 暂停是否成功
        """
        run_id = getattr(strategy_core, '_lifecycle_run_id', 'N/A')
        strategy_id = getattr(strategy_core, 'strategy_id', 'unknown')
        
        with strategy_core._lock:
            if strategy_core._state == StrategyState.PAUSED:
                logging.warning(
                    f"[PauseManager.pause_strategy][strategy_id={strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance] "
                    f"Already paused"
                )
                return True
            
            if strategy_core._state not in (StrategyState.RUNNING, StrategyState.DEGRADED):
                logging.warning(
                    f"[PauseManager.pause_strategy][strategy_id={strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance] "
                    f"Cannot pause in state: {strategy_core._state}"
                )
                return False
            
            if hasattr(strategy_core, 'pause') and callable(strategy_core.pause):
                strategy_core.pause()
            else:
                logging.error(
                    f"[PauseManager.pause_strategy][strategy_id={strategy_id}]"
                    f"[run_id={run_id}][owner_scope=strategy-instance] "
                    f"strategy_core.pause() method not available, cannot pause"
                )
                return False
            
            logging.info(
                f"[PauseManager.pause_strategy][strategy_id={strategy_id}]"
                f"[run_id={run_id}][owner_scope=strategy-instance][source_type=lifecycle] "
                f"Paused (state: {strategy_core._state.value})"
            )
        
        # ✅ 复用freeze_scheduler统一冻结逻辑
        scheduler_mgr = getattr(strategy_core, '_scheduler_manager', None)
        PauseManager.freeze_scheduler(scheduler_mgr)
        
        if hasattr(strategy_core, '_publish_event'):
            try:
                strategy_core._publish_event('StrategyPaused', {
                    'strategy_id': strategy_id
                })
            except Exception as e:
                logging.debug(f"[PauseManager.pause_strategy] Event publish error: {e}")
        
        return True


# ============================================================================
# P2 功能恢复：EmergencyPause 相关
# ============================================================================

class EmergencyPause:
    """紧急暂停"""
    
    def __init__(self, pause_manager: PauseManager):
        self.pause_manager = pause_manager
        self._emergency_thresholds: Dict[str, Any] = {}
    
    def check_emergency_conditions(self, market_data: Dict[str, Any]) -> bool:
        """
        检查紧急暂停条件
        
        Args:
            market_data: 市场数据
            
        Returns:
            bool: 是否需要紧急暂停
        """
        # TODO: 实现具体的紧急条件检查逻辑
        # 例如：价格波动超过阈值、成交量异常等
        
        return False
    
    def trigger_emergency_pause(self, reason: str) -> bool:
        """
        触发紧急暂停
        
        Args:
            reason: 紧急暂停原因
            
        Returns:
            bool: 是否成功触发
        """
        logging.warning(f"[EmergencyPause.trigger] Triggering emergency pause: {reason}")
        return self.pause_manager.pause(
            reason=PauseReason.EMERGENCY,
            message=f"Emergency: {reason}"
        )


# ============================================================================
# P2 功能恢复：UIProtection 相关
# ============================================================================

class UIProtection:
    """UI 保护"""
    
    def __init__(self):
        self._protected = False
        self._lock = threading.Lock()
    
    def protect_ui_operation(self, operation: Callable, *args, **kwargs) -> Any:
        """
        保护 UI 操作
        
        Args:
            operation: UI 操作函数
            *args: 操作参数
            **kwargs: 操作关键字参数
            
        Returns:
            Any: 操作结果
        """
        with self._lock:
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                logging.error(f"[UIProtection.protect] UI operation failed: {e}")
                return None
    
    def show_error_message(self, title: str, message: str) -> None:
        """
        显示错误消息
        
        Args:
            title: 消息标题
            message: 消息内容
        """
        logging.error(f"[UIProtection.show_error] {title}: {message}")
        # TODO: 在实际 UI 环境中实现


# ============================================================================
# P2 功能恢复：InstanceManager 相关
# ============================================================================

class InstanceManager:
    """实例管理器"""
    
    _instances: Dict[str, Any] = {}
    _lock = threading.Lock()
    
    @classmethod
    def register_instance(cls, name: str, instance: Any) -> None:
        """注册实例"""
        with cls._lock:
            cls._instances[name] = instance
            logging.info(f"[InstanceManager.register] Registered: {name}")
    
    @classmethod
    def get_instance(cls, name: str) -> Optional[Any]:
        """获取实例"""
        with cls._lock:
            return cls._instances.get(name)
    
    @classmethod
    def remove_instance(cls, name: str) -> None:
        """移除实例"""
        with cls._lock:
            if name in cls._instances:
                del cls._instances[name]
                logging.info(f"[InstanceManager.remove] Removed: {name}")
    
    @classmethod
    def cleanup_all(cls) -> None:
        """清理所有实例"""
        with cls._lock:
            cls._instances.clear()
            logging.info("[InstanceManager.cleanup] All instances cleaned up")


# ============================================================================
# 模块导出
# ============================================================================

__all__ = [
    'PauseState',
    'PauseReason',
    'PauseInfo',
    'PauseManager',
    'EmergencyPause',
    'UIProtection',
    'InstanceManager',
    'Signal',
]
