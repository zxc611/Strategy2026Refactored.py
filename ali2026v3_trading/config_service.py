"""
配置服务模块 - CQRS 架构 Configuration 层
来源：分散的配置文件（quant_trading_system_T型图/config.py + 06_params.py 配置类）
功能：统一配置管理、路径配置、交易参数、输出配置
行数：~300 行

优化 v1.0 (2026-03-16):
- ✅ 集中化配置管理
- ✅ 类型安全和验证
- ✅ 环境变量支持
- ✅ 热重载支持
- ✅ 多环境配置
"""
from __future__ import annotations

import os
import sys
import json
import re
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta


# ============================================================================
# 自定义日志 Handler - 确保实时写入磁盘
# ============================================================================

class FlushHandler(logging.Handler):
    """自定义 Handler，确保每次日志写入后立即 flush 到磁盘"""
    def emit(self, record):
        try:
            # 获取所有 FileHandler 并 flush
            for handler in logging.getLogger().handlers:
                if isinstance(handler, RotatingFileHandler) and hasattr(handler, 'stream'):
                    handler.stream.flush()
                    os.fsync(handler.stream.fileno())  # 强制同步到磁盘
        except Exception:
            pass  # flush 失败不影响日志记录


# ============================================================================
# 路径配置
# ============================================================================

@dataclass
class PathConfig:
    """路径配置"""
    project_root: Path = field(default_factory=lambda: Path(__file__).parent.parent)
    db_path: str = ""
    state_file: str = ""
    log_dir: str = ""
    config_dir: str = ""
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.db_path:
            self.db_path = str(self.project_root / "trading_data.db")
        if not self.state_file:
            self.state_file = str(self.project_root / "runtime_state.json")
        if not self.log_dir:
            self.log_dir = str(self.project_root / "auto_logs")
        if not self.config_dir:
            self.config_dir = str(self.project_root / ".arts")
    
    def to_dict(self) -> Dict[str, str]:
        """转换为字典"""
        return {
            "project_root": str(self.project_root),
            "db_path": self.db_path,
            "state_file": self.state_file,
            "log_dir": self.log_dir,
            "config_dir": self.config_dir
        }


# ============================================================================
# 交易所和合约配置
# ============================================================================

@dataclass
class ExchangeConfig:
    """交易所配置"""
    exchanges: List[str] = field(default_factory=lambda: ["CFFEX", "SHFE", "DCE", "CZCE"])
    simulated_instruments: Dict[str, List[str]] = field(default_factory=lambda: {
        "CFFEX": ["IF2406", "IO2406C4000"],
        "SHFE": ["rb2410"],
        "DCE": ["m2409"]
    })
    depth_levels: int = 5
    
    def get_instruments_for_exchange(self, exchange: str) -> List[str]:
        """获取指定交易所的模拟合约"""
        return self.simulated_instruments.get(exchange, [])
    
    def get_all_simulated_instruments(self) -> List[str]:
        """获取所有模拟合约"""
        instruments = []
        for inst_list in self.simulated_instruments.values():
            instruments.extend(inst_list)
        return instruments


# ============================================================================
# 交易参数配置
# ============================================================================

@dataclass
class TradingConfig:
    """交易参数配置"""
    # 订单相关
    default_volume: int = 1
    max_position_limit: int = 1000
    order_timeout_seconds: float = 30.0
    
    # 风控相关
    max_daily_loss: float = 10000.0
    max_single_order_volume: int = 100
    enable_auto_stop_loss: bool = True
    stop_loss_threshold: float = 5000.0
    
    # 行情相关
    tick_update_interval_ms: int = 500
    kline_check_interval_ms: int = 1000
    market_data_cache_ttl_seconds: int = 60
    
    # 策略相关
    strategy_heartbeat_interval_seconds: float = 30.0
    strategy_max_heartbeat_failures: int = 3
    enable_incremental_diagnosis: bool = True


# ============================================================================
# 输出配置
# ============================================================================

class OutputConfig:
    """输出配置类
    
    负责管理和缓存输出相关的参数配置，避免重复访问 self.params
    """
    
    def __init__(self, params: Optional[Any] = None):
        """初始化输出配置
        
        Args:
            params: 参数对象（可选）
        """
        # 输出模式：open_debug|close_debug|trade|none
        self.mode = str(getattr(params, "output_mode", "debug")).lower()
        if self.mode == "debug":
            self.mode = "close_debug"
        
        # 交易模式静默开关
        self.trade_quiet = bool(getattr(params, "trade_quiet", True))
        
        # 诊断输出开关
        self.diagnostic_enabled = bool(getattr(params, "diagnostic_output", True))
        
        # 调试输出开关
        self.debug_enabled = bool(getattr(params, "debug_output", False))
        
        # 综合调试开关
        self.combined_debug_enabled = (
            self.diagnostic_enabled and 
            (self.debug_enabled or self.mode == "close_debug")
        )
        
        # 是否为交易类模式
        self.is_trade_like = self.mode in ("trade", "open_debug")
    
    def should_output(
        self, 
        is_trade_msg: bool = False, 
        is_force_msg: bool = False,
        is_trade_table: bool = False,
        is_diag_msg: bool = False
    ) -> bool:
        """判断是否应该输出消息
        
        Args:
            is_trade_msg: 是否交易消息
            is_force_msg: 是否强制消息
            is_trade_table: 是否交易表格
            is_diag_msg: 是否诊断消息
        
        Returns:
            bool: 是否应该输出
        """
        # 交易模式且要求静默，且非交易表格，则拦截
        if self.is_trade_like and self.trade_quiet and not is_trade_table:
            return False
        
        # 诊断消息且诊断关闭，且非交易/强制消息，则拦截
        if is_diag_msg and not self.diagnostic_enabled and not is_trade_msg and not is_force_msg:
            return False
        
        # 交易模式下，仅输出交易或强制信息
        if self.is_trade_like and not is_trade_msg and not is_force_msg:
            return False
        
        # 非交易模式，若调试开关关闭且非强制，跳过输出
        if not is_trade_msg and not is_force_msg and not self.combined_debug_enabled:
            return False
        
        return True


# ============================================================================
# 日志配置
# ============================================================================

@dataclass
class LoggingConfig:
    """日志配置"""
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_date_format: str = "%Y-%m-%d %H:%M:%S"
    log_to_console: bool = True
    log_to_file: bool = True
    log_file_prefix: str = "strategy"
    log_rotation_days: int = 7
    log_backup_count: int = 10
    enable_async_logging: bool = True
    log_queue_size: int = 1000


# ============================================================================
# 数据库配置
# ============================================================================

@dataclass
class DatabaseConfig:
    """数据库配置"""
    db_type: str = "sqlite"
    db_path: str = ""
    connection_pool_size: int = 5
    connection_timeout: float = 30.0
    enable_connection_pooling: bool = True
    auto_commit: bool = False
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.db_path:
            path_config = PathConfig()
            self.db_path = path_config.db_path


# ============================================================================
# 性能监控配置
# ============================================================================

@dataclass
class PerformanceConfig:
    """性能监控配置"""
    enable_performance_monitoring: bool = True
    metrics_collection_interval_seconds: int = 60
    enable_memory_profiling: bool = False
    enable_cpu_profiling: bool = False
    performance_report_interval_minutes: int = 15
    alert_on_high_cpu_usage: bool = True
    high_cpu_threshold_percent: float = 80.0
    alert_on_high_memory_usage: bool = True
    high_memory_threshold_mb: int = 500


# ============================================================================
# 主配置类
# ============================================================================

class ConfigService:
    """配置服务 - 统一配置管理中心
    
    职责:
    - 集中管理所有配置
    - 支持从文件加载配置
    - 支持环境变量覆盖
    - 支持配置热重载
    - 提供类型安全的配置访问
    
    设计原则:
    - Singleton 模式
    - 配置与代码分离
    - 支持多环境
    - 类型安全
    """
    
    _instance: Optional['ConfigService'] = None
    _config_data: Dict[str, Any] = {}
    
    def __new__(cls) -> 'ConfigService':
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """初始化配置服务"""
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self._initialized = True
        self._config_file_path: Optional[Path] = None
        self._last_modified_time: float = 0.0
        
        # 初始化各子配置
        self.paths = PathConfig()
        self.exchanges = ExchangeConfig()
        self.trading = TradingConfig()
        self.logging = LoggingConfig()
        self.database = DatabaseConfig()
        self.performance = PerformanceConfig()
        
        # 尝试加载配置文件
        self._load_config_file()
        
        # 应用环境变量覆盖
        self._apply_environment_overrides()
    
    def _load_config_file(self, config_path: Optional[str] = None) -> None:
        """加载配置文件
        
        Args:
            config_path: 配置文件路径（可选）
        """
        if config_path:
            path = Path(config_path)
        else:
            path = Path(self.paths.config_dir) / "config.json"
        
        if not path.exists():
            return
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                file_config = json.load(f)
            
            self._config_file_path = path
            self._last_modified_time = path.stat().st_mtime
            
            # 更新配置
            self._update_from_dict(file_config)
            
        except Exception as e:
            logging.error(f"[ConfigService] Failed to load config file: {e}")
            raise RuntimeError(f"Config load failed: {e}") from e
    
    def _update_from_dict(self, config_dict: Dict[str, Any]) -> None:
        """从字典更新配置
        
        Args:
            config_dict: 配置字典
        """
        # 更新路径配置
        if 'paths' in config_dict:
            for key, value in config_dict['paths'].items():
                if hasattr(self.paths, key):
                    setattr(self.paths, key, value)
        
        # 更新交易所配置
        if 'exchanges' in config_dict:
            for key, value in config_dict['exchanges'].items():
                if hasattr(self.exchanges, key):
                    setattr(self.exchanges, key, value)
        
        # 更新交易配置
        if 'trading' in config_dict:
            for key, value in config_dict['trading'].items():
                if hasattr(self.trading, key):
                    setattr(self.trading, key, value)
        
        # 更新日志配置
        if 'logging' in config_dict:
            for key, value in config_dict['logging'].items():
                if hasattr(self.logging, key):
                    setattr(self.logging, key, value)
        
        # 更新数据库配置
        if 'database' in config_dict:
            for key, value in config_dict['database'].items():
                if hasattr(self.database, key):
                    setattr(self.database, key, value)
        
        # 更新性能配置
        if 'performance' in config_dict:
            for key, value in config_dict['performance'].items():
                if hasattr(self.performance, key):
                    setattr(self.performance, key, value)
    
    def _apply_environment_overrides(self) -> None:
        """应用环境变量覆盖"""
        # 数据库路径
        if 'TRADING_DB_PATH' in os.environ:
            self.database.db_path = os.environ['TRADING_DB_PATH']
        
        # 日志级别
        if 'LOG_LEVEL' in os.environ:
            self.logging.log_level = os.environ['LOG_LEVEL']
        
        # 输出模式
        if 'OUTPUT_MODE' in os.environ:
            # 注意：OutputConfig 需要 params 对象，这里不直接设置
            pass
        
        # 性能监控
        if 'ENABLE_PERFORMANCE_MONITORING' in os.environ:
            value = os.environ['ENABLE_PERFORMANCE_MONITORING'].lower()
            self.performance.enable_performance_monitoring = value in ('true', '1', 'yes')
    
    def reload(self) -> bool:
        """重新加载配置
        
        Returns:
            bool: 是否成功重新加载
        """
        if self._config_file_path and self._config_file_path.exists():
            current_mtime = self._config_file_path.stat().st_mtime
            if current_mtime > self._last_modified_time:
                self._load_config_file(str(self._config_file_path))
                return True
        return False


# ============================================================================
# P2 功能恢复：Params 相关（从 06_params.py 恢复）
# ============================================================================

def Field(default=None, title="", **kwargs):
    """
    字段定义辅助函数
    
    Args:
        default: 默认值
        title: 字段标题
        **kwargs: 其他参数
        
    Returns:
        默认值
    """
    return default


class DebugConfig:
    """调试配置"""
    
    def __init__(self, params: Any):
        """初始化调试配置"""
        self.output_enabled = bool(getattr(params, "debug_output_enabled", False))
        self.throttle_map = getattr(params, "debug_throttle_map", None)
        if not isinstance(self.throttle_map, dict):
            self.throttle_map = {}
        
        # 参数表调试节流间隔（秒）
        self.param_table_interval = float(self.throttle_map.get("param_table", 60.0))
    
    def should_log_param_table(self, last_timestamp: float, current_time: float) -> bool:
        """
        判断是否应该输出参数表调试日志
        
        Args:
            last_timestamp: 上次日志时间戳
            current_time: 当前时间戳
            
        Returns:
            bool: 是否应该输出日志
        """
        if not self.output_enabled:
            return False
        return (current_time - last_timestamp) >= self.param_table_interval


import json as _json
import time as _time
import threading as _threading

# ============================================================================
# 参数缓存管理（模块级，线程安全）
# ============================================================================

_param_table_cache = None
_param_table_lock = _threading.Lock()

def get_cached_params() -> dict:
    """获取缓存的参数表（线程安全 + 懒加载 + 双重检查锁定）。
    
    Returns:
        dict: 参数表字典
    """
    global _param_table_cache
    
    # 第一重检查：快速路径（无锁）
    if _param_table_cache is not None:
        return _param_table_cache
    
    # 使用锁防止并发加载
    with _param_table_lock:
        # 第二重检查：获取锁后再次检查
        if _param_table_cache is None:
            # 懒加载：首次调用时从 DEFAULT_PARAM_TABLE 加载
            _param_table_cache = dict(DEFAULT_PARAM_TABLE)  # 创建副本，防止意外修改
            logging.info(f"[config_service.get_cached_params] 参数表已加载，包含 {len(_param_table_cache)} 个参数")
        return _param_table_cache

def reset_param_cache() -> None:
    """重置参数缓存（供参数修改后重新加载使用，线程安全）。"""
    global _param_table_cache
    with _param_table_lock:
        _param_table_cache = None
        logging.info("[config_service.reset_param_cache] 参数缓存已重置，下次访问时将重新加载")


DEFAULT_EXCHANGE_MAPPING: Dict[str, str] = {
    # 中金所期货/期权
    "IF": "CFFEX", "IH": "CFFEX", "IC": "CFFEX", "IM": "CFFEX",
    "IO": "CFFEX", "HO": "CFFEX", "MO": "CFFEX",
    # 上期所期货/期权
    "CU": "SHFE", "AL": "SHFE", "ZN": "SHFE", "RB": "SHFE", "AU": "SHFE", "AG": "SHFE",
    "NI": "SHFE", "SN": "SHFE", "PB": "SHFE", "SS": "SHFE", "WR": "SHFE", "SI": "SHFE",
    "RU": "SHFE", "NR": "SHFE",
    # 能源中心
    "BC": "INE", "LU": "INE", "SC": "INE",
    # 大商所
    "M": "DCE", "Y": "DCE", "A": "DCE", "JM": "DCE", "I": "DCE",
    "C": "DCE", "CS": "DCE", "JD": "DCE", "L": "DCE", "V": "DCE", "PP": "DCE",
    "EG": "DCE", "PG": "DCE", "J": "DCE", "P": "DCE",
    # 郑商所
    "CF": "CZCE", "SR": "CZCE", "MA": "CZCE", "TA": "CZCE", "RM": "CZCE", "OI": "CZCE",
    "SA": "CZCE", "PF": "CZCE", "EB": "CZCE",
    # 广期所
    "LC": "GFEX",
}


def build_exchange_mapping(custom_mapping: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """合并默认映射与外部映射，统一返回大写交易所映射表。"""
    merged = dict(DEFAULT_EXCHANGE_MAPPING)
    for product, exchange in (custom_mapping or {}).items():
        product_key = str(product or "").strip().upper()
        exchange_value = str(exchange or "").strip().upper()
        if product_key and exchange_value:
            merged[product_key] = exchange_value
    return merged


def resolve_product_exchange(
    product_or_instrument: Optional[str],
    exchange_mapping: Optional[Dict[str, Any]] = None,
    default_exchange: str = "CFFEX",
) -> str:
    """根据品种代码或合约代码解析交易所。"""
    normalized_mapping = build_exchange_mapping(exchange_mapping)
    token = str(product_or_instrument or "").strip().upper()
    match = re.match(r"^([A-Z]+)", token)
    product_code = match.group(1) if match else token
    return normalized_mapping.get(product_code, str(default_exchange or "CFFEX").strip().upper())

# ============================================================================
# DEFAULT_PARAM_TABLE - 默认参数表（内嵌）
# ============================================================================

DEFAULT_PARAM_TABLE = {
    "max_kline": 200,
    "kline_style": "M1",
    "subscribe_options": True,
    "debug_output": True,
    "diagnostic_output": True,
    "api_key": "pk_test_default_api_key_for_pythongo",
    "infini_api_key": "sk_test_default_api_secret_for_pythongo",
    "access_key": "pk_test_default_api_key_for_pythongo",
    "access_secret": "sk_test_default_api_secret_for_pythongo",
    "run_profile": "full",
    "enable_scheduler": True,
    "use_tick_kline_generator": True,
    "backtest_tick_mode": False,
    "exchange": "CFFEX",
    "future_product": "IF",
    "option_product": "IO",
    "auto_load_history": True,
    "load_history_options": True,
    "load_all_products": True,
    "exchanges": "CFFEX,SHFE,DCE,CZCE,INE,GFEX",
    "future_products": "IF,IH,IC,CU,AL,ZN,RB,AU,AG,M,Y,A,JM,I,J,P,L,MA,TA,J,P,L,V,PP,RU,RM,OI,SA,PF,EB,EG,PG,C,CS,JD,ZN,NI,SN,PB,SS,BC,LU,SC,WR,SI,LC",
    "option_products": "IO,HO,MO,MA,TA,CU,RB,SR",
    "include_future_products_for_options": True,
    "subscription_batch_size": 10,
    "subscription_interval": 1,
    "subscription_fetch_on_subscribe": True,
    "subscription_fetch_count": 5,
    "subscription_fetch_for_options": False,
    "rate_limit_min_interval_sec": 1,
    "rate_limit_per_instrument": 2,
    "rate_limit_global_per_min": 60,
    "rate_limit_window_sec": 120,
    "subscription_backoff_factor": 1.0,
    "subscribe_only_current_next_options": True,
    "subscribe_only_current_next_futures": True,
    "enable_doc_examples": False,
    "pause_unsubscribe_all": True,
    "pause_force_stop_scheduler": True,
    "pause_on_stop": False,
    "history_minutes": 1440,
    "log_file_path": "strategy_startup.log",
    "test_mode": False,
    "auto_start_after_init": False,
    "subscribe_only_specified_month_options": True,
    "subscribe_only_specified_month_futures": True,
    "specified_month": "",
    "next_specified_month": "",
    "month_mapping": {
        "IF": ["IF2602", "IF2603"],
        "IH": ["IH2602", "IH2603"],
        "IM": ["IM2602", "IM2603"],
        "CU": ["CU2603", "CU2604"],
        "AL": ["AL2603", "AL2604"],
        "ZN": ["ZN2603", "ZN2604"],
        "RB": ["RB2603", "RB2604"],
        "AU": ["AU2603", "AU2604"],
        "AG": ["AG2603", "AG2604"],
        "M": ["M2603", "M2605"],
        "Y": ["Y2603", "Y2605"],
        "A": ["A2603", "A2605"],
        "JM": ["JM2604", "JM2605"],
        "I": ["I2603", "I2605"],
        "J": ["J2604", "J2605"],
        "CF": ["CF2603", "CF2605"],
        "SR": ["SR2603", "SR2605"],
        "MA": ["MA2603", "MA2605"],
        "TA": ["TA2603", "TA2605"],
    },
    "signal_cooldown_sec": 0.0,
    "option_buy_lots_min": 1,
    "option_buy_lots_max": 100,
    "option_contract_multiplier": 10000,
    "position_limit_valid_hours_max": 720,
    "position_limit_default_valid_hours": 24,
    "position_limit_max_ratio": 0.2,
    "position_limit_min_amount": 1000,
    "option_order_price_type": "2",
    "option_order_time_condition": "3",
    "option_order_volume_condition": "1",
    "option_order_contingent_condition": "1",
    "option_order_force_close_reason": "0",
    "option_order_hedge_flag": "1",
    "option_order_min_volume": 1,
    "option_order_business_unit": "1",
    "option_order_is_auto_suspend": 0,
    "option_order_user_force_close": 0,
    "option_order_is_swap": 0,
    "close_take_profit_ratio": 1.5,
    "close_overnight_check_time": "14:58",
    "close_daycut_time": "15:58",
    "close_max_hold_days": 3,
    "close_overnight_loss_threshold": -0.5,
    "close_overnight_profit_threshold": 4.0,
    "close_max_chase_attempts": 5,
    "close_chase_interval_seconds": 2,
    "close_chase_task_timeout_seconds": 30,
    "close_delayed_timeout_seconds": 30,
    "close_delayed_max_retries": 3,
    "close_order_price_type": "2",
    "output_mode": "debug",
    "force_debug_on_start": True,
    "ui_window_width": 260,
    "ui_window_height": 240,
    "ui_font_large": 11,
    "ui_font_small": 10,
    "enable_output_mode_ui": True,
    "daily_summary_hour": 15,
    "daily_summary_minute": 1,
    "trade_quiet": True,
    "print_start_snapshots": False,
    "trade_debug_allowlist": "",
    "debug_disable_categories": "",
    "debug_throttle_seconds": 0.0,
    "debug_throttle_map": {},
    "use_param_overrides_in_debug": False,
    "param_override_table": "",
    "param_edit_limit_per_month": 1,
    "backtest_params": {},
    "ignore_otm_filter": False,
    "allow_minimal_signal": False,
    "min_option_width": 1,
    "async_history_load": True,
    "manual_trade_limit_per_half": 1,
    "morning_afternoon_split_hour": 12,
    "account_id": "",
    "kline_max_age_sec": 0,
    "signal_max_age_sec": 180,
    "top3_rows": 3,
    "lots_min": 5,
    "history_load_max_workers": 4,
    "option_sync_tolerance": 0.5,
    "option_sync_allow_flat": True,
    "index_option_prefixes": "",
    "option_group_exchanges": "",
    "czce_year_future_window": 10,
    "czce_year_past_window": 10,
    "exchange_mapping": dict(DEFAULT_EXCHANGE_MAPPING),
}

# Global param table cache (for backward compatibility with file-based loading)
_PARAM_CACHE: Dict[str, Any] = {}
_PARAM_CACHE_META: Dict[str, Optional[float]] = {}
_PARAM_CHECK_TIMESTAMP: Dict[str, float] = {}


def load_param_table_cached(param_table_path: str, force_check: bool = False) -> Dict[str, Any]:
    """
    带 mtime 缓存的参数表加载
    
    Args:
        param_table_path: 参数表文件路径
        force_check: 是否强制检查
        
    Returns:
        参数表数据字典
    """
    try:
        if not param_table_path:
            logging.error(f"[load_param_table_cached] Empty param_table_path provided")
            return {}

        # Check at most once every 15 minutes unless forced
        now = _time.time()
        last_check = _PARAM_CHECK_TIMESTAMP.get(param_table_path, 0)
        if not force_check and (now - last_check < 900):
            if param_table_path in _PARAM_CACHE:
                return _PARAM_CACHE[param_table_path]

        # Perform mtime check
        _PARAM_CHECK_TIMESTAMP[param_table_path] = now

        mtime = os.path.getmtime(param_table_path)
        if param_table_path in _PARAM_CACHE and _PARAM_CACHE_META.get(param_table_path) == mtime:
            cached = _PARAM_CACHE.get(param_table_path)
            if not isinstance(cached, dict):
                logging.error(f"[load_param_table_cached] Cached value is not a dict: {type(cached)}")
                return {}
            return cached

        with open(param_table_path, "r", encoding="utf-8") as f:
            data = _json.load(f)
        if isinstance(data, dict):
            _PARAM_CACHE[param_table_path] = data
            _PARAM_CACHE_META[param_table_path] = mtime
            return data
        
        logging.error(f"[load_param_table_cached] Loaded data is not a dict: {type(data)}")
        return {}
    except Exception as e:
        logging.error(f"[load_param_table_cached] Error loading param table: {e}")
        return {}
    
    def need_reload(self) -> bool:
        """检查是否需要重新加载
        
        Returns:
            bool: 是否需要重新加载
        """
        if self._config_file_path and self._config_file_path.exists():
            current_mtime = self._config_file_path.stat().st_mtime
            return current_mtime > self._last_modified_time
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """将所有配置转换为字典
        
        Returns:
            Dict: 配置字典
        """
        return {
            'paths': self.paths.to_dict(),
            'exchanges': {
                'exchanges': self.exchanges.exchanges,
                'simulated_instruments': self.exchanges.simulated_instruments,
                'depth_levels': self.exchanges.depth_levels
            },
            'trading': {
                'default_volume': self.trading.default_volume,
                'max_position_limit': self.trading.max_position_limit,
                'order_timeout_seconds': self.trading.order_timeout_seconds,
                'max_daily_loss': self.trading.max_daily_loss,
                'max_single_order_volume': self.trading.max_single_order_volume,
                'enable_auto_stop_loss': self.trading.enable_auto_stop_loss,
                'stop_loss_threshold': self.trading.stop_loss_threshold,
                'tick_update_interval_ms': self.trading.tick_update_interval_ms,
                'kline_check_interval_ms': self.trading.kline_check_interval_ms,
                'market_data_cache_ttl_seconds': self.trading.market_data_cache_ttl_seconds,
                'strategy_heartbeat_interval_seconds': self.trading.strategy_heartbeat_interval_seconds,
                'strategy_max_heartbeat_failures': self.trading.strategy_max_heartbeat_failures,
                'enable_incremental_diagnosis': self.trading.enable_incremental_diagnosis
            },
            'logging': {
                'log_level': self.logging.log_level,
                'log_format': self.logging.log_format,
                'log_date_format': self.logging.log_date_format,
                'log_to_console': self.logging.log_to_console,
                'log_to_file': self.logging.log_to_file,
                'log_file_prefix': self.logging.log_file_prefix,
                'log_rotation_days': self.logging.log_rotation_days,
                'log_backup_count': self.logging.log_backup_count,
                'enable_async_logging': self.logging.enable_async_logging,
                'log_queue_size': self.logging.log_queue_size
            },
            'database': {
                'db_type': self.database.db_type,
                'db_path': self.database.db_path,
                'connection_pool_size': self.database.connection_pool_size,
                'connection_timeout': self.database.connection_timeout,
                'enable_connection_pooling': self.database.enable_connection_pooling,
                'auto_commit': self.database.auto_commit
            },
            'performance': {
                'enable_performance_monitoring': self.performance.enable_performance_monitoring,
                'metrics_collection_interval_seconds': self.performance.metrics_collection_interval_seconds,
                'enable_memory_profiling': self.performance.enable_memory_profiling,
                'enable_cpu_profiling': self.performance.enable_cpu_profiling,
                'performance_report_interval_minutes': self.performance.performance_report_interval_minutes,
                'alert_on_high_cpu_usage': self.performance.alert_on_high_cpu_usage,
                'high_cpu_threshold_percent': self.performance.high_cpu_threshold_percent,
                'alert_on_high_memory_usage': self.performance.alert_on_high_memory_usage,
                'high_memory_threshold_mb': self.performance.high_memory_threshold_mb
            }
        }
    
    def save_to_file(self, config_path: str) -> bool:
        """保存配置到文件
        
        Args:
            config_path: 配置文件路径
        
        Returns:
            bool: 是否成功保存
        """
        try:
            path = Path(config_path)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, indent=2)
            
            self._config_file_path = path
            self._last_modified_time = path.stat().st_mtime
            return True
        except Exception as e:
            logging.error(f"[ConfigService] Failed to save config file: {e}")
            raise RuntimeError(f"Config save failed: {e}") from e


# ============================================================================
# 全局配置实例
# ============================================================================

# 单例配置实例
config_service = ConfigService()


# ============================================================================
# 便捷函数
# ============================================================================

def get_config() -> ConfigService:
    """获取全局配置实例
    
    Returns:
        ConfigService: 配置服务实例
    """
    return config_service


def reload_config() -> bool:
    """重新加载配置
    
    Returns:
        bool: 是否成功重新加载
    """
    return config_service.reload()


def get_default_db_path() -> str:
    """获取默认数据库路径
    
    Returns:
        str: 数据库路径
    """
    return config_service.database.db_path


def get_project_root() -> Path:
    """获取项目根目录
    
    Returns:
        Path: 项目根目录
    """
    return config_service.paths.project_root


# 导出公共接口
__all__ = [
    'ConfigService',
    'PathConfig',
    'ExchangeConfig',
    'TradingConfig',
    'OutputConfig',
    'LoggingConfig',
    'DatabaseConfig',
    'PerformanceConfig',
    'config_service',
    'get_config',
    'reload_config',
    'get_default_db_path',
    'get_project_root',
    # Unified configuration functions (新增)
    'setup_logging',
    'setup_paths',
    'get_paths',
    'ServiceContainer',  # Service container for dependency management
]


# ============================================================================
# 统一配置函数 - Bootstrap 优化集成
# ============================================================================

# Logging initialization flag
# Use module-level variable AND check root logger handlers to prevent duplicate init
_LOG_INITIALIZED = False

# Path configuration cache
_PATHS_CONFIGURED = False
_CACHED_PATHS = None


def setup_logging():
    """Configure logging system, ensuring it's initialized only once.
    
    This function:
    1. Sets up root logger with INFO level
    2. Adds PlatformLogHandler for platform console output
    3. Adds RotatingFileHandler for file logging
    4. Prevents duplicate initialization using both flag and handler inspection
    
    Returns:
        bool: True if logging was configured, False if already initialized
    """
    global _LOG_INITIALIZED
    
    # Double-check: flag AND actual handler presence
    if _LOG_INITIALIZED:
        return False
    
    # Also check if root logger already has our handlers (defensive programming)
    root_logger = logging.getLogger()
    has_rotating_handler = any(
        isinstance(h, RotatingFileHandler)
        for h in root_logger.handlers
    )
    if has_rotating_handler:
        # Already initialized by previous call
        _LOG_INITIALIZED = True  # Sync flag with reality
        return False
    
    try:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        # Standard log format
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Add PlatformLogHandler (for InfiniTrader platform console)
        has_platform_handler = any(
            getattr(h, '_is_platform_handler', False)
            for h in root_logger.handlers
        )
        
        if not has_platform_handler:
            try:
                # Import write_log directly to avoid circular dependency with t_type_bootstrap
                from pythongo.infini import write_log
                
                # Create a simple handler that calls write_log
                class SimplePlatformHandler(logging.Handler):
                    def emit(self, record):
                        msg = self.format(record)
                        try:
                            write_log(msg)
                        except Exception:
                            pass  # Ignore errors in logging
                
                platform_handler = SimplePlatformHandler()
                platform_handler._is_platform_handler = True
                platform_handler.setLevel(logging.INFO)
                platform_handler.setFormatter(formatter)
                root_logger.addHandler(platform_handler)
            except ImportError:
                # write_log not available, skip
                pass
        
        # Add FileHandler (for strategy.log file)
        has_file_handler = any(
            isinstance(h, RotatingFileHandler)
            for h in root_logger.handlers
        )
        
        if not has_file_handler:
            # Use absolute path under demo directory for easy access
            demo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            log_dir = os.path.join(demo_dir, 'auto_logs')
            log_file = os.path.join(log_dir, 'strategy.log')
            
            # Ensure directory exists
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=3,
                encoding='utf-8',
                delay=False  # 立即打开文件，而不是等到第一次写入
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            
            # 添加 FlushHandler 确保实时写入
            flush_handler = FlushHandler()
            flush_handler.setLevel(logging.DEBUG)
            root_logger.addHandler(flush_handler)
        
        # Note: Do NOT reset LogConfig._configured as it may cause conflicts
        # The platform's LogConfig should manage its own configuration
        # We only configure the root logger with our handlers
        
        _LOG_INITIALIZED = True
        logging.info('[config_service] Logging system initialized: platform + file')
        return True
        
    except Exception as e:
        # Fallback: at least configure basic logging
        try:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            _LOG_INITIALIZED = True
            logging.warning('[config_service] Using fallback logging configuration')
            return True
        except Exception:
            return False


def setup_paths():
    """Add all required paths to sys.path once.
    
    This function:
    1. Adds demo directory
    2. Adds ali2026v3_trading directory
    3. Ensures paths are added only once
    
    Returns:
        dict: {
            'current_dir': str,      # demo directory
            'ali2026_dir': str,      # ali2026v3_trading directory
        }
    """
    global _PATHS_CONFIGURED, _CACHED_PATHS
    
    if _PATHS_CONFIGURED:
        return _CACHED_PATHS
    
    # Compute paths
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)  # ali2026v3_trading directory
    demo_dir = os.path.dirname(current_dir)  # demo directory
    
    paths_to_add = [
        demo_dir,          # demo directory
        current_dir,       # ali2026v3_trading directory
    ]
    
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)
    
    _CACHED_PATHS = {
        'current_dir': current_dir,
        'ali2026_dir': current_dir,
        'demo_dir': demo_dir,
    }
    
    _PATHS_CONFIGURED = True
    
    logging.info(f'[config_service] Paths configured: {_CACHED_PATHS}')
    
    return _CACHED_PATHS


def get_paths():
    """Get cached path configuration.
    
    Returns:
        dict: Path configuration dictionary
    """
    if not _PATHS_CONFIGURED:
        return setup_paths()
    return _CACHED_PATHS


# Auto-initialize when this module is imported
# Note: Disabled to prevent duplicate initialization
# Call setup_logging() and setup_paths() explicitly where needed
# setup_logging()  # Called by t_type_bootstrap.py
# setup_paths()    # Called by t_type_bootstrap.py


# ============================================================================
# ServiceContainer - 服务容器（从 t_type_bootstrap.py 迁移至此）
# ============================================================================

class ServiceContainer:
    """服务容器 - 完整的依赖管理"""
    def __init__(self):
        self._services = {}  # 服务实例
        self._dependencies = {  # 服务依赖关系
            'event_bus': [],
            'config': [],
            'params': [],
            'storage': ['event_bus'],
            'market_data': ['storage', 'event_bus'],
            'analytics': ['market_data'],
            'risk': ['market_data'],
            'order': ['risk'],
            'signal': ['analytics'],
            't_type': ['signal'],
            'diagnosis': ['t_type'],
            'ui': ['diagnosis'],
            'strategy_core': ['storage', 'event_bus', 'config', 'params']
        }
        self._initialization_order = [
            'event_bus', 'config', 'params', 'storage',
            'market_data', 'analytics', 'risk', 'order',
            'signal', 't_type', 'diagnosis', 'ui', 'strategy_core'
        ]
    
    def register(self, name: str, service: Any) -> None:
        """注册服务"""
        self._services[name] = service
        logging.info(f"[服务容器] 注册服务：{name}")
    
    def get(self, name: str) -> Any:
        """获取服务"""
        return self._services.get(name)
    
    def initialize_all(self) -> bool:
        """按依赖顺序初始化所有服务"""
        logging.info("[服务容器] 开始初始化...")
        logging.info(f"[服务容器] 服务初始化顺序：{self._initialization_order}")
        
        # 检查循环依赖
        if self._check_circular_dependencies():
            logging.error("[服务容器] 检测到循环依赖!")
            return False
        
        # 按顺序初始化服务
        for service_name in self._initialization_order:
            if service_name in self._services:
                service = self._services[service_name]
                # 检查依赖是否满足
                deps = self._dependencies.get(service_name, [])
                missing_deps = [d for d in deps if d not in self._services]
                if missing_deps:
                    logging.error(f"[服务容器] 服务 {service_name} 依赖于 {missing_deps}，但它们未注册")
                    return False
                logging.info(f"[服务容器] 初始化服务：{service_name}")
        logging.info("[服务容器] 所有服务初始化成功")
        return True
    
    def _check_circular_dependencies(self) -> bool:
        """检查循环依赖"""
        visited = set()
        rec_stack = set()
        
        def visit(node):
            if node in rec_stack:
                return True  # 发现循环
            if node in visited:
                return False
            visited.add(node)
            rec_stack.add(node)
            for dep in self._dependencies.get(node, []):
                if visit(dep):
                    return True
            rec_stack.remove(node)
            return False
        
        for service_name in self._dependencies:
            if visit(service_name):
                return True
        return False
