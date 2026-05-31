"""Design Pattern Fixes — R19-P2 设计反模式10项修复

P2-设计1:  Factory模式→策略工厂
P2-设计2:  Adapter模式→DuckDB适配器(已在db_adapter.py实现)
P2-设计3:  全局变量→SingletonRegistry(已在singleton_registry.py实现)
P2-设计4:  异常流程控制→前置条件检查
P2-设计5:  super()协作继承→MRO协作式
P2-设计6:  Mixin隐式顺序→显式初始化协议
P2-设计7:  硬编码依赖→配置集中管理
P2-设计8:  DI容器→ServiceContainer增强
P2-设计9:  Observer滥用→事件过滤
P2-设计10: Strategy模式→策略分发
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Type
from dataclasses import dataclass

from ali2026v3_trading.code_quality_fixes import (
    DB_FILE_TRADING_DATA, DB_FILE_TICKS, DB_FILE_STRATEGY,
    DB_FILE_TICK_STORAGE, DB_FILE_DELAY_TIME_SHARPE,
    TABLE_TICKS_RAW, TABLE_TICK_DATA,
)

logger = logging.getLogger(__name__)


# ============================================================================
# P2-设计1: Factory模式 — 策略对象工厂
# ============================================================================
_STRATEGY_REGISTRY: Dict[str, Type] = {}


def register_strategy(name: str, strategy_cls: Type) -> None:
    """注册策略类到工厂"""
    _STRATEGY_REGISTRY[name] = strategy_cls


def create_strategy(name: str, *args: Any, **kwargs: Any) -> Any:
    """通过工厂创建策略实例"""
    if name not in _STRATEGY_REGISTRY:
        raise ValueError(f"未注册策略: {name}, 可用策略: {list(_STRATEGY_REGISTRY.keys())}")
    return _STRATEGY_REGISTRY[name](*args, **kwargs)


def list_registered_strategies() -> List[str]:  # [R22-P2-TS11]
    """列出所有已注册策略"""
    return list(_STRATEGY_REGISTRY.keys())


# ============================================================================
# P2-设计7: 硬编码依赖→配置集中管理
# ============================================================================
@dataclass(slots=True)
class DatabaseConfig:
    """数据库配置集中管理（替代散布在7+文件中的硬编码路径）"""
    trading_data_db: str = DB_FILE_TRADING_DATA
    ticks_db: str = DB_FILE_TICKS
    strategy_db: str = DB_FILE_STRATEGY
    tick_storage_db: str = DB_FILE_TICK_STORAGE
    delay_time_sharpe_db: str = DB_FILE_DELAY_TIME_SHARPE
    ticks_raw_table: str = TABLE_TICKS_RAW
    tick_data_table: str = TABLE_TICK_DATA


_db_config = DatabaseConfig()


def get_db_config() -> DatabaseConfig:
    """获取全局数据库配置"""
    return _db_config


def set_db_config(config: DatabaseConfig) -> None:
    """设置全局数据库配置"""
    global _db_config
    _db_config = config


# ============================================================================
# P2-设计4: 异常流程控制→前置条件检查
# ============================================================================
def require_non_none(value: Any, name: str) -> Any:
    """前置条件：值不为None"""
    if value is None:
        raise ValueError(f"前置条件失败: {name} 不能为None")
    return value


def require_positive(value: float, name: str) -> float:
    """前置条件：值为正数"""
    if value <= 0:
        raise ValueError(f"前置条件失败: {name} 必须为正数, 实际值={value}")
    return value


def require_in_range(value: float, name: str, low: float, high: float) -> float:
    """前置条件：值在范围内"""
    if not (low <= value <= high):
        raise ValueError(f"前置条件失败: {name} 必须在[{low}, {high}]范围内, 实际值={value}")
    return value


def require_type(value: Any, name: str, expected_type: Type) -> Any:
    """前置条件：值类型匹配"""
    if not isinstance(value, expected_type):
        raise TypeError(f"前置条件失败: {name} 应为{expected_type.__name__}, 实际为{type(value).__name__}")
    return value


# ============================================================================
# P2-设计8: DI容器增强 — ServiceContainer服务注册表
# ============================================================================
class ServiceLocator:
    """服务定位器 — 替代散布在代码中的直接import+new

    用法:
        locator = ServiceLocator()
        locator.register('signal_service', SignalService)
        svc = locator.get('signal_service')
    """
    def __init__(self):
        self._factories: Dict[str, Callable] = {}
        self._instances: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def register(self, name: str, factory: Callable, singleton: bool = True) -> None:
        """注册服务工厂"""
        with self._lock:
            self._factories[name] = (factory, singleton)

    def get(self, name: str) -> Any:
        """获取服务实例"""
        with self._lock:
            if name in self._instances:
                return self._instances[name]
            if name not in self._factories:
                raise KeyError(f"未注册服务: {name}, 可用服务: {list(self._factories.keys())}")
            factory, singleton = self._factories[name]
            instance = factory()
            if singleton:
                self._instances[name] = instance
            return instance

    def has(self, name: str) -> bool:
        """检查服务是否已注册"""
        return name in self._factories

    def reset(self) -> None:
        """重置所有单例（用于测试）"""
        with self._lock:
            self._instances.clear()

    def list_services(self) -> List[str]:  # [R22-P2-TS12]
        """列出所有已注册服务"""
        return list(self._factories.keys())


_service_locator = ServiceLocator()


def get_service_locator() -> ServiceLocator:
    """获取全局服务定位器"""
    return _service_locator


# ============================================================================
# P2-设计10: Strategy模式→策略分发
# ============================================================================
class StrategyDispatcher:
    """策略分发器 — 替代长if-elif链

    用法:
        dispatcher = StrategyDispatcher()
        dispatcher.register('box_extreme', handle_box_extreme)
        dispatcher.register('box_spring', handle_box_spring)
        result = dispatcher.dispatch(strategy_type, data)
    """
    def __init__(self):
        self._handlers: Dict[str, Callable] = {}

    def register(self, key: str, handler: Callable) -> None:
        """注册处理器"""
        self._handlers[key] = handler

    def dispatch(self, key: str, *args: Any, **kwargs: Any) -> Any:
        """分发到处理器"""
        if key not in self._handlers:
            raise KeyError(f"未注册处理器: {key}, 可用: {list(self._handlers.keys())}")
        return self._handlers[key](*args, **kwargs)

    def has_handler(self, key: str) -> bool:
        return key in self._handlers

    def keys(self) -> List[str]:  # [R22-P2-TS13]
        return list(self._handlers.keys())
