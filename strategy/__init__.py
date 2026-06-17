# MODULE_ID: M1-243
"""strategy/子系统(P2-S1) — 公共API精确白名单

PA-03 三层物理隔离: __init__.py 仅重导出 Facade 层公共符号。
严格约束: __all__ 长度 ≤ 7
禁止: import *、重导出任何 _ 前缀模块
禁止: 重导出 logger / json / copy 等实现细节

注意: 使用惰性导入（__getattr__）避免循环依赖。
      config/config_params → strategy/strategy_config → strategy/__init__
      如果 __init__ 急切导入 shadow_strategy_facade → shadow_strategy_core → config_params
      会形成循环。惰性导入打破此循环。

Mixin消灭(G2b): StrategyCoreService已退化为纯Facade，零Mixin继承。
      6个Service通过ServiceFactory注入: LifecycleService, KlineDataService,
      TickProcessingService, RecoveryService, CheckpointService, InstrumentService。
"""
from __future__ import annotations

__all__ = [
    "StrategyCoreService",
    "ServiceFactory",
    "ShadowStrategyEngine",
    "ShadowTradeRecord",
    "AlphaMetrics",
    "get_shadow_strategy_engine",

]


def __getattr__(name: str):
    """惰性导入：仅在首次访问时加载，避免循环依赖"""
    if name == "StrategyCoreService":
        from .strategy_core_service import StrategyCoreService  # 真实来源: strategy.strategy_core_service
        return StrategyCoreService
    if name == "ServiceFactory":
        from .service_factory import ServiceFactory  # 真实来源: strategy.service_factory
        return ServiceFactory
    if name in ("ShadowStrategyEngine", "ShadowTradeRecord", "AlphaMetrics", "get_shadow_strategy_engine"):
        from . import shadow_strategy_facade as _facade  # 真实来源: strategy.shadow_strategy_facade
        return getattr(_facade, name)
    _DEPRECATED_MODULES = {
        "_box_spring_types": ".box_spring_detector",
        "box_detector": ".box_spring_detector",
        "checkpoint_service": ".persistence_service",
        "recovery_service": ".persistence_service",
        "service_factory": ".strategy_core_service",
        "shadow_strategy_types": ".shadow_strategy_core",
        "strategy_core_service_builder": ".strategy_core_service",
        "strategy_tick_handler": ".tick_hft",
        "tick_hft_dispatch": ".tick_hft",
    }
    if name in _DEPRECATED_MODULES:
        import importlib
        return importlib.import_module(_DEPRECATED_MODULES[name], __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
