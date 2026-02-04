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
        import pkgutil
        import your_quant_project.strategy
        
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

        # 动态发现并重载所有模块
        print(">>> [BOOTSTRAP] Discovering and reloading all strategy modules...")
        package_path = os.path.dirname(your_quant_project.strategy.__file__)
        found_modules = []
        
        # 1. 扫描所有模块
        for _, name, _ in pkgutil.iter_modules([package_path]):
            if name == "container": continue # 放最后
            found_modules.append(f"your_quant_project.strategy.{name}")
            
        # 2. 暴力清除
        for m in found_modules + ["your_quant_project.strategy.container"]:
            if m in sys.modules:
                del sys.modules[m]
                
        # 3. 按顺序重新导入
        # 优先导入基础模块（如果有特定依赖顺序需求，可以在这里调整）
        priority_modules = [
            "your_quant_project.strategy.params",
            "your_quant_project.strategy.namespace_manager",
            "your_quant_project.strategy.hot_reloader"
        ]
        
        # 先导入高优先级
        for m in priority_modules:
            if m in found_modules:
                importlib.import_module(m)
                found_modules.remove(m)
        
        # 导入剩余模块
        for m in found_modules:
            importlib.import_module(m)
            
        # 最后导入容器
        import your_quant_project.strategy.container

    except Exception as e:
        print(f">>> [BOOTSTRAP] SmartGC Init Failed ({e}), falling back to brute force...")
        # 暴力清除 sys.modules 缓存 - 终极方案 (Fallback)
        # 获取包路径下所有 .py 文件模拟 reload 列表
        import your_quant_project.strategy
        pkg_dir = os.path.dirname(your_quant_project.strategy.__file__)
        all_files = [f[:-3] for f in os.listdir(pkg_dir) if f.endswith(".py") and f != "__init__.py"]
        
        for name in all_files:
            full_name = f"your_quant_project.strategy.{name}"
            if full_name in sys.modules:
                del sys.modules[full_name]

            # 重新导入
            try:
                importlib.import_module(full_name)
            except Exception as import_err:
                print(f"!!! Failed to reload {full_name}: {import_err}")

    # 重新导入 (Explicitly import container to expose Strategy class)
    import your_quant_project.strategy.container
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




