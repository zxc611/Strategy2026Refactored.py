"""
quant_trading_system_T型图 - 量化交易系统 T 型图架构包

这个包包含了基于 T 型图架构的量化交易系统的核心模块:
- constants: 常量定义
- models: 数据模型
- utils: 工具函数
- cache: 缓存管理
- scheduler: 调度器
- params: 参数管理
- data_manager: 数据管理
- position: 持仓管理
- executor: 执行器
- gate: 网关层
- pause: 暂停服务
- trading_logic: 交易逻辑
- strategy_core: 策略核心
- diagnosis: 诊断服务
- ui: UI 服务
"""

__version__ = '1.0.0'
__author__ = 'Quant Trading System'

# 导入核心组件，方便外部使用
try:
    # Use absolute imports for package exports
    from ali2026v3_trading.storage import InstrumentDataManager
    from ali2026v3_trading.order_service import OrderService
    from ali2026v3_trading.risk_service import RiskService
    from ali2026v3_trading.params_service import ParamsService
    from ali2026v3_trading.config_service import ConfigService
except ImportError as e:
    # 允许部分导入失败，不影响包的加载
    import logging
    logging.warning(f"[ali2026v3_trading] Partial import failed: {e}")
    logging.warning(f"quant_trading_system_T 型图 package partial import: {e}")
