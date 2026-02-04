"""
Strategy2026 Refactored Entry Point (Bootstrap Wrapper).
Logic is encapsulated in your_quant_project.strategy.container to prevent regression.
"""
import sys
import os
import traceback
from datetime import datetime

# -----------------------------------------------------------------------------
# 1. 路径引导 BOOTSTRAP
# -----------------------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# [FIX] Ensure parent directories are in path for 'pythongo' resolution
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
pystrategy_dir = os.path.dirname(parent_dir)
if pystrategy_dir not in sys.path:
    sys.path.insert(0, pystrategy_dir)

# -----------------------------------------------------------------------------
# 2. 容器加载 (Force Reload for Hot-Fix)
# -----------------------------------------------------------------------------
try:
    import importlib
    
    # [Deep Reload] 按照依赖顺序重载所有子模块，确保修改对继承链生效
    # 0. 基础工具
    import your_quant_project.strategy.params as m_params
    import your_quant_project.strategy.market_calendar as m_cal
    import your_quant_project.strategy.platform_compat as m_compat
    import your_quant_project.strategy.context_utils as m_ctx
    import your_quant_project.strategy.scheduler_utils as m_sch
    import your_quant_project.strategy.emergency_pause as m_pause
    import your_quant_project.strategy.validation as m_valid

    # 1. 核心业务
    import your_quant_project.strategy.instruments as m_instr
    import your_quant_project.strategy.kline_manager as m_kline
    import your_quant_project.strategy.subscriptions as m_sub
    import your_quant_project.strategy.data_container as m_data
    import your_quant_project.strategy.calculation as m_calc
    import your_quant_project.strategy.trading_logic as m_logic
    import your_quant_project.strategy.order_execution as m_exec
    import your_quant_project.strategy.position_manager as m_pos
    import your_quant_project.strategy.ui_mixin as m_ui
    import your_quant_project.strategy.container as m_ctr
    
    # [HOTFIX] Deep Force reload
    print(">>> [BOOTSTRAP] Deep Reloading Strategy Modules...")
    
    # 尝试使用高级动态命名空间清洗器
    try:
        from your_quant_project.strategy.namespace_manager import GarbageSweeper, ConfigLoader, NamespaceDashboard
        import threading
        
        print(">>> [BOOTSTRAP] Taking out the trash (SmartGC)...")
        sweeper = GarbageSweeper()
        # 激进模式清洗 your_quant_project 下的所有模块引用
        cleaned_refs = sweeper.sweep("your_quant_project")
        print(f">>> [BOOTSTRAP] SmartGC cleaned {cleaned_refs} references.")

        # 启动监控面板 (如果配置启用)
        config = ConfigLoader.load()
        if config.get('dynamic_namespace', {}).get('monitoring', {}).get('enable_dashboard', False):
             print(">>> [BOOTSTRAP] Starting Namespace Dashboard...")
             dashboard = NamespaceDashboard()
             dashboard.start_monitoring()

    except Exception as e:
        print(f">>> [BOOTSTRAP] SmartGC Init Failed ({e}), falling back to brute force...")
        # 暴力清除 sys.modules 缓存，强迫 Python 重新从磁盘读取文件
        # 这是解决 "修改不生效" 的终极方案
        modules_to_kill = [
            "your_quant_project.strategy.params",
            "your_quant_project.strategy.market_calendar",
            "your_quant_project.strategy.platform_compat",
            "your_quant_project.strategy.context_utils",
            "your_quant_project.strategy.scheduler_utils",
            "your_quant_project.strategy.emergency_pause",
            "your_quant_project.strategy.validation",
            "your_quant_project.strategy.instruments",
            "your_quant_project.strategy.kline_manager",
            "your_quant_project.strategy.subscriptions",
            "your_quant_project.strategy.data_container",
            "your_quant_project.strategy.calculation",
            "your_quant_project.strategy.position_manager",
            "your_quant_project.strategy.order_execution",
            "your_quant_project.strategy.trading_logic",
            "your_quant_project.strategy.ui_mixin",
            "your_quant_project.strategy.container",
        ]
        
        for m in modules_to_kill:
            keys_to_remove = [k for k in sys.modules if k.startswith(m) or k == m] # Better matching
            for k in keys_to_remove:
                if k in sys.modules:
                    del sys.modules[k]

    # 重新导入
    import your_quant_project.strategy.params
    import your_quant_project.strategy.market_calendar
    import your_quant_project.strategy.platform_compat
    import your_quant_project.strategy.context_utils
    import your_quant_project.strategy.scheduler_utils
    import your_quant_project.strategy.emergency_pause
    import your_quant_project.strategy.validation
    import your_quant_project.strategy.instruments
    import your_quant_project.strategy.kline_manager
    import your_quant_project.strategy.subscriptions
    import your_quant_project.strategy.data_container
    import your_quant_project.strategy.calculation
    import your_quant_project.strategy.position_manager
    import your_quant_project.strategy.order_execution
    import your_quant_project.strategy.trading_logic
    import your_quant_project.strategy.ui_mixin
    import your_quant_project.strategy.container

    print(">>> [BOOTSTRAP] All modules freshly imported from disk.")
    
    from your_quant_project.strategy.container import StrategyContainer, Params
    print(f"[BOOTSTRAP] Successfully RELOADED StrategyContainer from {StrategyContainer.__module__}")
except ImportError as e:
    # 严重错误：无法加载容器
    with open(os.path.join(current_dir, "BOOTSTRAP_ERROR.txt"), "w") as f:
        f.write(f"Bootstrap Import Error: {e}\n{traceback.format_exc()}")
    # Fallback to definition to avoid crash if possible, but state is critical
    print(f"[BOOTSTRAP] FATAL ERROR: {e}")
    raise e

# -----------------------------------------------------------------------------
# 3. 策略定义 (Thin Wrapper)
# -----------------------------------------------------------------------------
class Strategy2026Refactored(StrategyContainer):
    """
    具体的策略类，继承自固化的容器逻辑。
    """
    pass

# Compatibility Aliases
class Strategy(Strategy2026Refactored):
    pass

class Strategy2026_Refactored(Strategy2026Refactored):
    pass

__all__ = ["Strategy2026Refactored", "Strategy2026_Refactored", "Params", "Strategy"]

# [DEBUG] Mark successful load
try:
    with open(os.path.join(current_dir, "DEBUG_ENTRY_LOADED.txt"), "w") as f:
        f.write(f"V23 Container Bootstrap Loaded: {datetime.now()}")
except: pass




