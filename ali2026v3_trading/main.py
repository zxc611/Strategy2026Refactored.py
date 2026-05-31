"""
InfiniTrader strategy compatibility entry.

This module is kept as a thin wrapper because some platform configurations
still load `main.py` directly.

Note: This is now a PURE EXPORT LAYER - it only re-exports symbols from
t_type_bootstrap for backward compatibility. All actual implementations
are in t_type_bootstrap.py and ali2026v3_trading service modules.

Important: Uses lazy imports via __getattr__ to avoid circular import issues.
The export list is defined in __all__ and _LAZY_IMPORTS must be consistent.

R21-CC-P1-12修复: asyncio完全未使用 — 现状说明与未来规划
  现状：全系统使用同步I/O + threading多线程模型，无任何asyncio使用。
  原因：CTP平台SDK为同步回调模型，DuckDB Python API为同步，文件I/O为同步。
  影响：同步I/O阻塞GIL，高并发场景下tick处理延迟增加。
  未来规划（长期）：
    1. Phase 1: 将非热路径I/O（日志写入、参数加载）改为asyncio
    2. Phase 2: 引入aioduckdb替代同步DuckDB查询
    3. Phase 3: CTP回调层保持同步，内部处理层改为asyncio事件循环
    4. 注意：CTP SDK本身是同步的，asyncio改造需在回调入口做bridge

R21-MEM-P2-12修复: Python协程栈深度限制
  Python协程栈深度受sys.getrecursionlimit()限制(默认1000)，与函数调用栈共享。
  当前项目无async/await代码，暂无协程栈溢出风险。未来asyncio改造注意事项：
    1. asyncio.create_task()创建的协程在事件循环中顺序执行，不增加调用栈深度
    2. 但await嵌套链过深（如await func_a() → await func_b() → ...）会累积栈帧
    3. 建议asyncio改造时：避免深层await嵌套(>50层)；必要时用asyncio.TaskGroup扁平化
    4. 若需调整栈深度：sys.setrecursionlimit()需谨慎，过大可能导致C栈溢出崩溃

R21-CC-P2-04修复: 信号量(Semaphore)使用不统一 — 全项目搜索threading.Semaphore无任何匹配，当前无需修复，若后续引入需统一封装

R21-CC-P2-01修复: 无asyncio事件循环管理
  现状：本模块及全系统未创建或管理asyncio事件循环(loop)。
  风险：若未来引入asyncio组件(如aioduckdb/aiohttp)，缺少统一事件循环管理将导致：
    1. 多处各自创建loop，资源浪费且可能冲突
    2. 事件循环生命周期与策略生命周期不绑定，策略停止时loop未关闭
    3. 线程安全：asyncio loop非线程安全，跨线程调用需run_in_executor
  建议：在StrategyCoreService.__init__中创建并管理统一的asyncio事件循环，
    或引入独立的AsyncLoopManager单例，提供get_loop()/run_coroutine()接口。
    事件循环应在策略start()时启动、stop()时优雅关闭(pending tasks取消+loop.close())。
"""

from __future__ import annotations

# Lazy imports to break circular dependencies:
# main.py → t_type_bootstrap.py → ali2026v3_trading.* → (back to main.py)
# 
# By using __getattr__, we delay the import until the symbol is actually accessed,
# which breaks the circular dependency chain.

# Single source of truth for exports - used by both __getattr__ and __all__
_LAZY_IMPORTS = {
    'Strategy2026': ('ali2026v3_trading.strategy_core_service', 'Strategy2026'),
    'ServiceContainer': ('ali2026v3_trading.config_service', 'ServiceContainer'),
    'InstrumentDataManager': ('ali2026v3_trading', 'InstrumentDataManager'),
    'OrderService': ('ali2026v3_trading.order_service', 'OrderService'),
    'RiskService': ('ali2026v3_trading.risk_service', 'RiskService'),
    'ParamsService': ('ali2026v3_trading.params_service', 'ParamsService'),
    'StrategyCore': ('ali2026v3_trading.strategy_core_service', 'StrategyCoreService'),
    'StrategyState': ('ali2026v3_trading.strategy_lifecycle_mixin', 'StrategyState'),
    'ConfigService': ('ali2026v3_trading.config_service', 'ConfigService'),
}

_IMPORT_CACHE: dict = {}  # R21-MEM-P2-10修复: 模块导入缓存无TTL/大小限制，但模块导入后不会变化，无需TTL；条目数有限(<=_LAZY_IMPORTS长度)无需max_size

__all__ = list(_LAZY_IMPORTS.keys())


def __getattr__(name):
    """Lazy import to avoid circular imports.
    
    This delays importing from t_type_bootstrap until the symbol is actually
    accessed, breaking the circular dependency chain.
    
    Performance: Uses cache to avoid repeated imports.
    """
    if name in _IMPORT_CACHE:
        return _IMPORT_CACHE[name]
    
    if name in _LAZY_IMPORTS:
        import importlib  # R21-CC-P2-03修复: 动态导入importlib — 用于延迟加载避免循环依赖，但需注意：
        # 1. importlib.import_module在每次调用时执行模块搜索，高频调用有性能开销（本处有缓存所以影响小）
        # 2. 动态导入的模块中若包含大数据对象，被pickle序列化到子进程时开销显著
        # 3. 动态导入的模块路径为硬编码字符串，无外部可控参数，当前安全风险可控
        entry = _LAZY_IMPORTS[name]
        if isinstance(entry, tuple):
            module_name, attr_name = entry
        else:
            module_name, attr_name = entry, name
        module = importlib.import_module(module_name)
        attr = getattr(module, attr_name)
        _IMPORT_CACHE[name] = attr
        return attr
    
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
