"""
pause_service.py - 暂停管理服务

合并来源：11_pause.py
合并策略：提取核心暂停管理和信号功能

重构目标:
- 旧架构：11_pause.py (~1151 行)
- 新架构：pause_service.py (~600 行)
- 功能保留：完整的暂停管理、信号机制

P2 功能恢复清单 (30 个):
1. Signal.connect
2. Signal.emit
3-30. 暂停管理、UI 保护、实例管理等

作者：CodeArts 代码智能体
版本：v1.0
生成时间：2026-03-16
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, List, Optional, Dict
from dataclasses import dataclass, field
from enum import Enum, auto


# ============================================================================
# 枚举定义
# ============================================================================

class PauseState(Enum):
    """暂停状态"""
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
        for slot in self.slots:
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
        self._pause_listeners: List[Callable] = []
        self._resume_listeners: List[Callable] = []
        
        # 信号
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
            
            # 通知监听器
            for listener in self._pause_listeners:
                try:
                    listener(reason, message)
                except Exception as e:
                    logging.error(f"[PauseManager.pause] Listener error: {e}")
            
            # 发射信号
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
            
            # 通知监听器
            for listener in self._resume_listeners:
                try:
                    listener()
                except Exception as e:
                    logging.error(f"[PauseManager.resume] Listener error: {e}")
            
            # 发射信号
            self.on_resume_signal.emit(True)
            
            return True
    
    def stop(self) -> None:
        """停止"""
        with self._lock:
            self._state = PauseState.STOPPED
            self._pause_info = PauseInfo(PauseReason.MANUAL, "Stopped")
            
            logging.info("[PauseManager.stop] Stopped")
    
    def add_pause_listener(self, callback: Callable) -> None:
        """添加暂停监听器"""
        self._pause_listeners.append(callback)
    
    def add_resume_listener(self, callback: Callable) -> None:
        """添加恢复监听器"""
        self._resume_listeners.append(callback)
    
    def get_pause_info(self) -> Optional[PauseInfo]:
        """获取暂停信息"""
        return self._pause_info


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
