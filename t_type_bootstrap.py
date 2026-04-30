"""
T型图系统引导模块 - 简化版

使用方法：
    from t_type_bootstrap import Strategy2026
    strategy = Strategy2026()

作者：CodeArts 代码智能体
版本：v2.0 (简化版)
生成时间：2026-03-19
"""

from __future__ import annotations

import sys
import os
import traceback
import logging
import importlib
import json
import inspect
import time
import threading
from datetime import datetime

# =============================================================================
# 统一配置 - 必须在任何导入之前设置 sys.path
# =============================================================================
# 设置 demo 目录到 sys.path，确保 pythongo 包的相对导入能正常工作
script_dir = os.path.dirname(os.path.abspath(__file__))  # demo directory

# 调试：打印当前 sys.path 状态
def _debug_print_sys_path(label=""):
    """调试函数：打印当前 sys.path 状态"""
    print(f"[DEBUG{label}] sys.path ({len(sys.path)} entries):")
    for i, p in enumerate(sys.path[:10]):  # 只打印前10个
        print(f"  [{i}] {p}")
    if len(sys.path) > 10:
        print(f"  ... and {len(sys.path) - 10} more")

_debug_print_sys_path("(before setup)")

# 确保 demo 目录在 sys.path 中，并且优先级最高
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)
    print(f"[DEBUG] Added to sys.path: {script_dir}")
else:
    # 如果已在 sys.path 中，确保它在最前面
    sys.path.remove(script_dir)
    sys.path.insert(0, script_dir)
    print(f"[DEBUG] Moved to front of sys.path: {script_dir}")

_debug_print_sys_path("(after setup)")

# 导入平台的write_log函数
try:
    from pythongo.infini import write_log
except ImportError:
    # 如果导入失败，使用print作为备用
    def write_log(msg):
        print(msg)

# 自定义日志处理器，将日志输出重定向到平台的write_log函数
class PlatformLogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        write_log(msg)

# =============================================================================
# 统一日志配置
# =============================================================================
# Logging will be configured by config_service module
# No need to import from logging_config

# Initialize logging once at module load time
# Will be done by config_service when imported

# Use centralized configuration from config_service to avoid duplicate imports
from ali2026v3_trading.config_service import setup_logging, setup_paths, get_paths, ServiceContainer

# Initialize logging FIRST before any logging calls
setup_logging()
paths = setup_paths()
_ali2026_dir = paths['ali2026_dir']
_current_dir = _ali2026_dir  # Alias for backward compatibility

# Import platform base class (must inherit, otherwise platform cannot recognize)
# Track BaseStrategy source for debugging
BaseStrategy = None
_BASE_STRATEGY_SOURCE = None

try:
    from pythongo.base import BaseStrategy
    _BASE_STRATEGY_SOURCE = 'pythongo.base'
except ImportError:
    try:
        from pythongo.ui import BaseStrategy
        _BASE_STRATEGY_SOURCE = 'pythongo.ui'
    except ImportError:
        class BaseStrategy(object):
            def __init__(self):
                self.strategy_id = 0
                self.strategy_name = ""
        _BASE_STRATEGY_SOURCE = 'fallback'

# Log BaseStrategy source for debugging (logging is now initialized)
logging.info(f"[引导] BaseStrategy 加载自: {_BASE_STRATEGY_SOURCE}")

# Exception handling decorators (unified)
from ali2026v3_trading.diagnosis_service import (
    handle_import_errors,
    log_exceptions,
    safe_call,
    optional_import,
    required_import,
)



_runtime_log_dir = os.path.join(os.path.dirname(__file__), 'auto_logs')
_runtime_log_file = os.path.join(_runtime_log_dir, 'bootstrap_runtime.log')
_minimal_mode_file = os.path.join(_runtime_log_dir, 'bootstrap_minimal_mode.txt')


# ========== 先定义 TTypeBootstrap，供 Strategy2026 继承 ==========
class TTypeBootstrap(BaseStrategy):
    """引导包装器 - 用于初始化和管理 T 型图系统
    
    继承 BaseStrategy 以支持 InfiniTrader 平台集成。
    
    DEPRECATED: 所有功能已迁移至 strategy_core_service.Strategy2026
    此类仅保留作为导入层，方法体全部替换为 pass
    """
    
    def __init__(self):
        """初始化引导器"""
        super().__init__()
        self._instance_id = id(self)
        # 所有功能已迁移，此处仅占位
        # 不再设置 _strategy_cache，让 Strategy2026 自己设置
        pass
    
    def _ensure_strategy(self):
        """确保策略实例存在"""
        # 功能已迁移至 strategy_core_service
        pass
    
    def output(self, msg):
        """输出日志到平台控制台"""
        # 功能已迁移
        pass
    
    def get_kline_data(self, exchange, instrument_id, style, count):
        """获取 K 线数据"""
        # 功能已迁移至 market_data_service
        pass

    # 平台回调方法 - 全部为 pass（策略已直接使用 Strategy2026）
    def onInit(self):
        """InfiniTrader 平台初始化回调"""
        # 此方法已废弃，平台应直接使用 Strategy2026
        pass

    def onStart(self):
        """InfiniTrader 平台启动回调"""
        # 此方法已废弃，平台应直接使用 Strategy2026
        pass

    def onStop(self):
        """InfiniTrader 平台停止回调"""
        # 此方法已废弃，平台应直接使用 Strategy2026
        pass

    def onTick(self, tick):
        """InfiniTrader 平台 Tick 回调"""
        # 此方法已废弃，平台应直接使用 Strategy2026
        pass

    def onOrder(self, order):
        """InfiniTrader 平台订单回调"""
        # 此方法已废弃，平台应直接使用 Strategy2026
        pass

    def onTrade(self, trade):
        """InfiniTrader 平台成交回调"""
        # 此方法已废弃，平台应直接使用 Strategy2026
        pass

    # 蛇形命名兼容方法
    def on_init(self):
        """策略初始化回调 (蛇形命名)"""
        # 兼容方法，已迁移
        pass

    def on_start(self):
        """策略启动回调（蛇形命名）"""
        # 兼容方法，已迁移
        pass

    def on_stop(self):
        """策略停止回调（蛇形命名）"""
        # 兼容方法，已迁移
        pass


# 工具函数 - 保留但简化
def _trace(msg: str) -> None:
    """写入 bootstrap 独立运行日志"""
    # 功能保留，但简化实现
    pass


def _base_strategy_debug_info() -> str:
    """返回当前 BaseStrategy 的来源与 __init__ 签名"""
    # 调试函数，保留
    return "TTypeBootstrap debug info"


# 环境配置函数 - 保留但简化
_PYTHONGO_ENV_CONFIGURED = False

def _setup_pythongo_environment():
    """自动配置 pythongo 运行环境"""
    # 功能已迁移至 config_service
    global _PYTHONGO_ENV_CONFIGURED
    _PYTHONGO_ENV_CONFIGURED = True
    pass


def reset_pythongo_config(force=False):
    """重置并重新配置 pythongo 环境"""
    # 功能已迁移
    pass


def get_pythongo_config_status():
    """获取当前配置状态"""
    # 功能已迁移
    return {
        'configured': _PYTHONGO_ENV_CONFIGURED,
        'api_key_set': False,
        'api_secret_set': False,
        'account_id': '',
    }

# 最小空策略模式标志
_EMPTY_INIT_MODE = False

# 在模块加载时立即配置环境
_setup_pythongo_environment()
_trace(f"[bootstrap] BaseStrategy selected: {_base_strategy_debug_info()}")

# 最小空策略对照开关：
# 环境变量 TT_BOOTSTRAP_EMPTY_INIT=1 或文件 bootstrap_minimal_mode.txt 内容为 ON/1/TRUE/YES
# 开启后 onInit 直接返回 True，不创建真实 Strategy2026。
_minimal_mode_raw = os.getenv('TT_BOOTSTRAP_EMPTY_INIT', '').strip().upper()
if not _minimal_mode_raw:
    try:
        if os.path.exists(_minimal_mode_file):
            with open(_minimal_mode_file, 'r', encoding='utf-8') as f:
                _minimal_mode_raw = f.read().strip().upper()
    except Exception as e:
        _minimal_mode_raw = ''
_EMPTY_INIT_MODE = _minimal_mode_raw in {'ON', '1', 'TRUE', 'YES', 'Y'}
# =============================================================================
# 从 ali2026v3_trading 导入服务并创建 Strategy2026 类
# =============================================================================

try:
    # demo 目录已在文件开头添加到 sys.path，无需重复添加
    
    # 调试：在导入前检查 sys.path 状态
    _debug_print_sys_path("(before importing ali2026v3_trading)")
    print(f"[DEBUG] Checking if demo dir is in sys.path: {script_dir in sys.path}")
    print(f"[DEBUG] Demo dir position in sys.path: {sys.path.index(script_dir) if script_dir in sys.path else 'NOT FOUND'}")

    # 导入必要的服务模块（使用完整的包名）
    from ali2026v3_trading.strategy_core_service import StrategyCoreService, StrategyState
    from ali2026v3_trading.storage import InstrumentDataManager
    from ali2026v3_trading.order_service import OrderService
    from ali2026v3_trading.risk_service import RiskService
    from ali2026v3_trading.params_service import ParamsService
    from ali2026v3_trading.config_service import ConfigService
    from ali2026v3_trading.storage import InstrumentDataManager as DatabaseManager
    from ali2026v3_trading.ui_service import UIMixin
    # is_market_open 和 get_market_time_service 已迁移到 scheduler_service
    from ali2026v3_trading.scheduler_service import is_market_open, get_market_time_service
        
    # Strategy2026 和 ServiceContainer 已从相应服务模块导入
    # 不再在此处定义
    
    # 从 strategy_core_service 导入 Strategy2026（同时保留 Strategy2026 和 Strategy 两个名称）
    from ali2026v3_trading.strategy_core_service import Strategy2026
    Strategy = Strategy2026  # 向后兼容
    
    logging.info(f"[t_type_bootstrap] 成功从 ali2026v3_trading 服务创建 Strategy2026")

except Exception as e:
    logging.warning(f"[t_type_bootstrap] ali2026v3_trading 导入失败：{e}")
    # 不再提供 fallback - 让异常传播以便诊断
    raise

# =============================================================================
# BaseStrategy 兼容性检查
# =============================================================================

def verify_base_strategy_compatibility():
    """Verify that BaseStrategy is suitable as a base class.
    
    Note: pythongo.base.BaseStrategy is intentionally an empty base class.
    The callback methods (onInit, onStart, onStop) are implemented by:
    - TTypeBootstrap (which inherits from BaseStrategy)
    - Strategy2026 (which inherits from TTypeBootstrap)
    
    This is the correct design pattern - BaseStrategy provides the type hierarchy,
    while concrete implementations add the actual methods.
    
    Returns:
        bool: Always True if BaseStrategy is a proper class
    """
    # Just verify it's a proper class - no need to check for methods
    if not isinstance(BaseStrategy, type):
        logging.error(f"[引导] BaseStrategy 不是一个正确的类: {type(BaseStrategy)}")
        return False
    
    # Log success - BaseStrategy doesn't need onInit/onStart/onStop itself
    logging.info(
        f"[引导] BaseStrategy 验证成功: {_BASE_STRATEGY_SOURCE}. "
        f"回调方法由 TTypeBootstrap/Strategy2026 实现."
    )
    return True


__all__ = [
    'Strategy2026',  # 主策略类（推荐）
    'Strategy',  # Strategy2026 的别名
    't_type_bootstrap',  # 指向 Strategy2026 类（供平台使用）
    't_type_bootstrap_instance',  # ✅ P1 Bug #56修复：实际创建的策略实例（供平台UI检测）
    'ServiceContainer',  # 服务容器（从 config_service 导入）
    'reset_pythongo_config',  # 重置配置接口
    'get_pythongo_config_status',  # 配置状态查询
    'reset_param_cache',  # 重置参数缓存接口
    # 以下为向后兼容的包装器（不推荐使用）
    'TTypeBootstrap',  # 引导包装器 (驼峰命名)
]

# 兼容性别名 (供平台使用)
# ⚠️ 重要：t_type_bootstrap 必须保持为类，因为平台会调用它来创建实例
# 实际的实例引用在 t_type_bootstrap_instance 中
t_type_bootstrap = Strategy2026

# =============================================================================
# 模块级缓存 - 策略实例和参数表
# =============================================================================
# 策略实例缓存（模块级，所有 TTypeBootstrap 实例共享）
_strategy_cache = None
_strategy_cache_lock = threading.Lock()

def register_strategy_cache(strategy_instance) -> bool:
    """
    线程安全的策略实例注册函数。
    
    Args:
        strategy_instance: Strategy2026 实例
        
    Returns:
        bool: 注册是否成功
    """
    global _strategy_cache
    with _strategy_cache_lock:
        old = _strategy_cache
        _strategy_cache = strategy_instance
        logging.info(f"[t_type_bootstrap] Strategy cache registered: {old} -> {strategy_instance}")
        return True

# ✅ P1 Bug #56修复：将 t_type_bootstrap 设置为实际创建的策略实例
# 平台UI通过检查此变量来判断策略实例是否已创建
# 初始为 None，在 Strategy2026.__init__ 中会更新为实际实例
t_type_bootstrap_instance = None


