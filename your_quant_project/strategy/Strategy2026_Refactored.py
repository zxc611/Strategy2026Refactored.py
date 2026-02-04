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
    import gc
    
    # [Expert Hot-Reload] 动态全量重载与资源回收
    target_package = 'your_quant_project'
    modules_to_purge = [name for name in sys.modules if name.startswith(target_package)]
    
    if modules_to_purge:
        print(f">>> [BOOTSTRAP] Found {len(modules_to_purge)} stale modules. Starting cleanup sequence...")
        
        # [Phase 1] 资源回收 (Resource Disposal)
        # 尝试关闭遗留的全局单例 (如 UI 窗口)，防止 "Zombie Windows"
        if 'your_quant_project.strategy.ui_mixin' in sys.modules:
            try:
                legacy_ui_mod = sys.modules['your_quant_project.strategy.ui_mixin']
                if hasattr(legacy_ui_mod, 'UIMixin'):
                    # 检查类属性单例
                    legacy_cls = legacy_ui_mod.UIMixin
                    root = getattr(legacy_cls, '_ui_global_root', None)
                    if root:
                         # 检测是否是有效的 Tk 实例
                        if hasattr(root, 'destroy'):
                            print(f"[CLEANUP] Destroying legacy UI Root from previous session: {root}")
                            try:
                                # 尝试在主线程直接销毁，或调度销毁
                                # 注意：跨线程操作 Tkinter 是危险的，但留下 Zombie 窗口更糟
                                # 使用 after 注入事件循环是比较安全的方式，但如果 loop 已死则无效
                                root.after(0, root.destroy)
                            except Exception:
                                # 如果 after 失败，尝试暴力销毁
                                try:
                                    root.destroy()
                                    root.quit()
                                except Exception:
                                    pass
                            
                            # 清除引用
                            setattr(legacy_cls, '_ui_global_root', None)
                            setattr(legacy_cls, '_ui_global_running', False)
            except Exception as e:
                print(f"[WARN] Error during UI cleanup: {e}")

        # [Phase 2] 命名空间清洗 (Namespace Purging)
        for module_name in modules_to_purge:
            try:
                # 核心：从 sys.modules 移除
                del sys.modules[module_name]
            except Exception as e:
                print(f"[WARN] Failed to purge {module_name}: {e}")
                
        # [Phase 3] 垃圾回收
        gc.collect() # 显式触发 GC，帮助解释器释放旧对象的内存
        print(">>> [BOOTSTRAP] Cache purged and GC collected.")

    else:
        print(f">>> [BOOTSTRAP] Clean slate. First run detected for '{target_package}'.")

    # 重新加载入口 -> Python 会递归地重新加载所有子模块
    import your_quant_project.strategy.container
    
    # 再次确认
    importlib.reload(your_quant_project.strategy.container)
    
    # 导入最终类
    from your_quant_project.strategy.container import StrategyContainer, Params

    print(f">>> [BOOTSTRAP] RELOAD SUCCESS. StrategyContainer loaded from: {your_quant_project.strategy.container.__file__}")
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




