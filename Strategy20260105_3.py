"""
Strategy2026 Refactored Entry Point (Bootstrap Wrapper).
Logic is encapsulated in your_quant_project.strategy.container to prevent regression.
Acts as a drop-in replacement for the old Strategy20260105_3.py to enforce new architecture.
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

# [FIX] Ensure parent directories are in path for "pythongo" resolution
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
    # Ensure package can be imported
    import your_quant_project.strategy.params
    import your_quant_project.strategy.calculation
    import your_quant_project.strategy.ui_mixin
    import your_quant_project.strategy.container
    
    # [HOTFIX] Force reload modules to apply changes without restarting platform
    importlib.reload(your_quant_project.strategy.params)
    importlib.reload(your_quant_project.strategy.calculation)
    importlib.reload(your_quant_project.strategy.ui_mixin)
    importlib.reload(your_quant_project.strategy.container)
    
    from your_quant_project.strategy.container import StrategyContainer, Params
    print(f"[BOOTSTRAP] Successfully RELOADED StrategyContainer from {StrategyContainer.__module__}")
except ImportError as e:
    # 严重错误：无法加载容器
    with open(os.path.join(current_dir, "BOOTSTRAP_ERROR.txt"), "w") as f:
        f.write(f"Bootstrap Import Error: {e}\n{traceback.format_exc()}")
    print(f"[BOOTSTRAP] FATAL ERROR: {e}")
    # Try to define a dummy class so platform doesn"t crash immediately upon import scan
    class StrategyContainer: pass
    class Params: pass
    raise e

# -----------------------------------------------------------------------------
# 3. 策略定义 (Thin Wrapper)
# -----------------------------------------------------------------------------
class Strategy(StrategyContainer):
    """
    Standard Entry Point Class Name "Strategy" preferred by some loaders.
    """
    pass

class Strategy20260105_3(StrategyContainer):
    """
    Legacy Entry Point Class Name to match filename for specific loaders.
    """
    pass

class Strategy2026Refactored(StrategyContainer):
    pass

# Export all potential class names
__all__ = ["Strategy", "Strategy20260105_3", "Strategy2026Refactored", "Params"]

# [DEBUG] Mark successful load
try:
    with open(os.path.join(current_dir, "DEBUG_ENTRY_LOADED_LegacyName.txt"), "w") as f:
        f.write(f"Legacy Filename Bootstrap Loaded: {datetime.now()}")
except: pass
