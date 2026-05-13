"""
配置服务模块 - CQRS 架构 Configuration 层
来源：分散的配置文件（quant_trading_system_T型图/config.py + 06_params.py 配置类）
功能：统一配置管理、路径配置、交易参数、输出配置

重构 v2.0 (2026-05-07):
- ✅ ExchangeConfig/品种映射/合约解析 → config_exchange.py
- ✅ 参数表/缓存/校验 → config_params.py
- ✅ 本模块保留：路径/交易/输出/日志配置 + 全局单例 + 便捷函数
- ✅ 向后兼容：所有 from ali2026v3_trading.config_service import xxx 不变
"""
from __future__ import annotations

import os
import sys
import json
import copy
import logging
import time
import threading
from logging.handlers import RotatingFileHandler
try:
    from concurrent_log_handler import ConcurrentRotatingFileHandler
    _HAS_CONCURRENT_HANDLER = True
except ImportError:
    _HAS_CONCURRENT_HANDLER = False
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from ali2026v3_trading.shared_utils import normalize_year_month

from ali2026v3_trading.config_exchange import (
    ExchangeConfig,
    _get_option_underlying_product,
    ensure_products_with_retry,
    build_exchange_mapping,
    resolve_product_exchange,
    make_platform_future_id,
)

from ali2026v3_trading.config_params import (
    DEFAULT_PARAM_TABLE,
    get_cached_params,
    reset_param_cache,
    update_cached_params,
    load_default_params_from_json,
    get_params_metadata,
    validate_params,
    apply_environment,
    rebuild_default_param_table,
    _normalize_option_params_payload,
    load_option_params_from_file,
    merge_option_params_to_default,
    CACHE_TTL,
)


# ============================================================================
# 自定义日志 Handler - 确保实时写入磁盘
# ============================================================================

class FlushHandler(logging.Handler):
    """自定义 Handler，确保每次日志写入后立即 flush 到磁盘"""
    def emit(self, record):
        try:
            for handler in logging.getLogger().handlers:
                if isinstance(handler, RotatingFileHandler) and hasattr(handler, 'stream'):
                    handler.stream.flush()
                    os.fsync(handler.stream.fileno())
        except Exception:
            pass


# ============================================================================
# 路径配置
# ============================================================================

@dataclass
class PathConfig:
    """路径配置"""
    project_root: str = str(Path(__file__).parent.parent)
    db_path: str = str(Path(__file__).parent.parent / "data" / "strategy.duckdb")
    state_file: str = str(Path(__file__).parent.parent / "data" / "strategy_state.json")
    log_dir: str = str(Path(__file__).parent / "logs")
    config_dir: str = str(Path(__file__).parent / "config")

    def to_dict(self) -> Dict[str, Any]:
        return {
            'project_root': self.project_root,
            'db_path': self.db_path,
            'state_file': self.state_file,
            'log_dir': self.log_dir,
            'config_dir': self.config_dir,
        }


# ============================================================================
# 交易参数配置
# ============================================================================

@dataclass
class TradingConfig:
    """交易参数配置"""
    default_volume: int = 1
    max_position_limit: int = 100
    order_timeout_seconds: float = 5.0
    max_daily_loss: float = 10000.0
    max_single_order_volume: int = 10
    enable_auto_stop_loss: bool = True
    stop_loss_threshold: float = 0.05
    tick_update_interval_ms: int = 100
    kline_check_interval_ms: int = 1000
    market_data_cache_ttl_seconds: int = 60
    strategy_heartbeat_interval_seconds: int = 30
    strategy_max_heartbeat_failures: int = 3
    enable_incremental_diagnosis: bool = True


# ============================================================================
# 输出配置
# ============================================================================

class OutputConfig:
    """输出配置"""
    def __init__(self):
        self.mode = "debug"
        self.diagnostic_output = True
        self.debug_output = True
        self._diagnostic_throttle_sec = 0.0
        self._debug_throttle_sec = 0.0

    def should_output(self, category: str = "debug") -> bool:
        if category == "diagnostic":
            return self.diagnostic_output
        if category == "debug":
            return self.debug_output
        return True

    def set_mode(self, mode: str):
        self.mode = mode
        if mode == "quiet":
            self.diagnostic_output = False
            self.debug_output = False
        elif mode == "normal":
            self.diagnostic_output = False
            self.debug_output = True
        elif mode == "debug":
            self.diagnostic_output = True
            self.debug_output = True


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
    log_rotation_days: int = 1
    log_backup_count: int = 30
    enable_async_logging: bool = False
    log_queue_size: int = 1000


# ============================================================================
# 数据库配置
# ============================================================================

@dataclass
class DatabaseConfig:
    """数据库配置"""
    db_type: str = "duckdb"
    db_path: str = str(Path(__file__).parent.parent / "data" / "strategy.duckdb")
    connection_pool_size: int = 5
    connection_timeout: float = 30.0
    enable_connection_pooling: bool = True
    auto_commit: bool = False


# ============================================================================
# 数据路径配置
# ============================================================================

@dataclass
class DataPathsConfig:
    """数据路径配置"""
    duckdb_file: str = ""
    parquet_path: str = ""
    flush_windows_str: str = "09:00-11:30,13:00-15:15"

    def __post_init__(self):
        if not self.duckdb_file:
            env_val = os.environ.get('DUCKDB_FILE', '').strip()
            self.duckdb_file = env_val if env_val else str(Path(__file__).parent.parent / "data" / "strategy.duckdb")
        if not self.parquet_path:
            env_val = os.environ.get('TICK_DATA_PATH', '').strip()
            self.parquet_path = env_val if env_val else str(Path(__file__).parent.parent / "data" / "ticks")
        env_flush = os.environ.get('INTRADAY_FLUSH_WINDOWS', '').strip()
        if env_flush:
            self.flush_windows_str = env_flush

    def to_dict(self) -> Dict[str, Any]:
        return {
            'duckdb_file': self.duckdb_file,
            'parquet_path': self.parquet_path,
            'flush_windows_str': self.flush_windows_str,
        }


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
    performance_report_interval_minutes: int = 60
    alert_on_high_cpu_usage: bool = True
    high_cpu_threshold_percent: float = 80.0
    alert_on_high_memory_usage: bool = True
    high_memory_threshold_mb: int = 500


# ============================================================================
# 主配置类
# ============================================================================

class ConfigService:
    """配置服务 - 统一配置管理中心"""
    
    _instance: Optional['ConfigService'] = None
    _config_data: Dict[str, Any] = {}
    _config_data_lock = threading.Lock()
    _init_lock = threading.Lock()
    
    def __new__(cls) -> 'ConfigService':
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True
        self._config_file_path: Optional[Path] = None
        self._last_modified_time: float = 0.0
        self.paths = PathConfig()
        self.exchanges = ExchangeConfig()
        self.trading = TradingConfig()
        self.logging = LoggingConfig()
        self.database = DatabaseConfig()
        self.data_paths = DataPathsConfig()
        self.performance = PerformanceConfig()
        self._load_config_file()
        self._apply_environment_overrides()
    
    def _load_config_file(self, config_path: Optional[str] = None) -> None:
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
            self._update_from_dict(file_config)
        except Exception as e:
            logging.error(f"[ConfigService] Failed to load config file: {e}")
            raise RuntimeError(f"Config load failed: {e}") from e
    
    def _update_from_dict(self, config_dict: Dict[str, Any]) -> None:
        if 'paths' in config_dict:
            for key, value in config_dict['paths'].items():
                if hasattr(self.paths, key):
                    setattr(self.paths, key, value)
        if 'exchanges' in config_dict:
            for key, value in config_dict['exchanges'].items():
                if hasattr(self.exchanges, key):
                    setattr(self.exchanges, key, value)
        if 'trading' in config_dict:
            for key, value in config_dict['trading'].items():
                if hasattr(self.trading, key):
                    setattr(self.trading, key, value)
        if 'logging' in config_dict:
            for key, value in config_dict['logging'].items():
                if hasattr(self.logging, key):
                    setattr(self.logging, key, value)
        if 'database' in config_dict:
            for key, value in config_dict['database'].items():
                if hasattr(self.database, key):
                    setattr(self.database, key, value)
        if 'performance' in config_dict:
            for key, value in config_dict['performance'].items():
                if hasattr(self.performance, key):
                    setattr(self.performance, key, value)
        if 'data_paths' in config_dict:
            for key, value in config_dict['data_paths'].items():
                if hasattr(self.data_paths, key):
                    setattr(self.data_paths, key, value)
    
    def _apply_environment_overrides(self) -> None:
        if 'TRADING_DB_PATH' in os.environ:
            self.database.db_path = os.environ['TRADING_DB_PATH']
        if 'LOG_LEVEL' in os.environ:
            self.logging.log_level = os.environ['LOG_LEVEL']
        if 'OUTPUT_MODE' in os.environ:
            pass
        if 'ENABLE_PERFORMANCE_MONITORING' in os.environ:
            value = os.environ['ENABLE_PERFORMANCE_MONITORING'].lower()
            self.performance.enable_performance_monitoring = value in ('true', '1', 'yes')
        if 'DUCKDB_FILE' in os.environ:
            val = os.environ['DUCKDB_FILE']
            self.data_paths.duckdb_file = val.strip() if val.strip() else self.data_paths.duckdb_file
        if 'TICK_DATA_PATH' in os.environ:
            val = os.environ['TICK_DATA_PATH']
            self.data_paths.parquet_path = val.strip() if val.strip() else self.data_paths.parquet_path
        if 'INTRADAY_FLUSH_WINDOWS' in os.environ:
            val = os.environ['INTRADAY_FLUSH_WINDOWS']
            self.data_paths.flush_windows_str = val.strip() if val.strip() else self.data_paths.flush_windows_str
    
    def reload(self) -> bool:
        if self._config_file_path and self._config_file_path.exists():
            current_mtime = self._config_file_path.stat().st_mtime
            if current_mtime > self._last_modified_time:
                self._load_config_file(str(self._config_file_path))
                return True
        return False
    
    def need_reload(self) -> bool:
        if self._config_file_path and self._config_file_path.exists():
            current_mtime = self._config_file_path.stat().st_mtime
            return current_mtime > self._last_modified_time
        return False
    
    def to_dict(self) -> Dict[str, Any]:
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
            },
            'data_paths': self.data_paths.to_dict()
        }
    
    def save_to_file(self, config_path: str) -> bool:
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
# Params 相关辅助
# ============================================================================

def Field(default=None, title="", **kwargs):
    return default


class DebugConfig:
    """调试配置"""
    def __init__(self, params: Any):
        self.output_enabled = bool(getattr(params, "debug_output_enabled", False))
        self.throttle_map = getattr(params, "debug_throttle_map", None)
        if not isinstance(self.throttle_map, dict):
            self.throttle_map = {}
        self.param_table_interval = float(self.throttle_map.get("param_table", 60.0))
    
    def should_log_param_table(self, last_timestamp: float, current_time: float) -> bool:
        if not self.output_enabled:
            return False
        return (current_time - last_timestamp) >= self.param_table_interval


# ============================================================================
# 全局配置实例
# ============================================================================

_config_service_instance: Optional['ConfigService'] = None
_config_service_lock = threading.Lock()


# ============================================================================
# 便捷函数
# ============================================================================

def get_config() -> ConfigService:
    global _config_service_instance
    if _config_service_instance is None:
        with _config_service_lock:
            if _config_service_instance is None:
                _config_service_instance = ConfigService()
    return _config_service_instance

class _ConfigServiceProxy:
    def __getattr__(self, name):
        return getattr(get_config(), name)
    def __repr__(self):
        return repr(get_config())

config_service = _ConfigServiceProxy()


def reload_config() -> bool:
    return config_service.reload()


def get_default_db_path() -> str:
    return config_service.database.db_path


def get_project_root() -> Path:
    return config_service.paths.project_root


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
    'setup_logging',
    'setup_paths',
    'get_paths',
    'ServiceContainer',
    'DEFAULT_PARAM_TABLE',
    'get_cached_params',
    'reset_param_cache',
    'update_cached_params',
    'resolve_product_exchange',
    'build_exchange_mapping',
    'make_platform_future_id',
    'ensure_products_with_retry',
]


# ============================================================================
# 统一配置函数 - Bootstrap 优化集成
# ============================================================================

_LOG_INITIALIZED = False
_PATHS_CONFIGURED = False
_CACHED_PATHS = None


def setup_logging():
    global _LOG_INITIALIZED
    if _LOG_INITIALIZED:
        return False
    root_logger = logging.getLogger()
    has_rotating_handler = any(
        isinstance(h, RotatingFileHandler)
        for h in root_logger.handlers
    )
    if has_rotating_handler:
        _LOG_INITIALIZED = True
        return False
    try:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        has_platform_handler = any(
            getattr(h, '_is_platform_handler', False)
            for h in root_logger.handlers
        )
        if not has_platform_handler:
            try:
                from pythongo.infini import write_log
                class SimplePlatformHandler(logging.Handler):
                    def emit(self, record):
                        msg = self.format(record)
                        try:
                            write_log(msg)
                        except Exception:
                            pass
                platform_handler = SimplePlatformHandler()
                platform_handler._is_platform_handler = True
                platform_handler.setLevel(logging.INFO)
                platform_handler.setFormatter(formatter)
                root_logger.addHandler(platform_handler)
            except ImportError:
                pass
        has_file_handler = any(
            isinstance(h, RotatingFileHandler)
            for h in root_logger.handlers
        )
        if not has_file_handler:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            log_dir = os.path.join(current_dir, 'logs')
            log_file = os.path.join(log_dir, 'strategy.log')
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            if _HAS_CONCURRENT_HANDLER:
                file_handler = ConcurrentRotatingFileHandler(
                    log_file,
                    maxBytes=50*1024*1024,
                    backupCount=5,
                    encoding='utf-8',
                    delay=False,
                    use_gzip=False
                )
                logging.info('[config_service] Using ConcurrentRotatingFileHandler for multi-process safety')
            else:
                file_handler = RotatingFileHandler(
                    log_file,
                    maxBytes=50*1024*1024,
                    backupCount=5,
                    encoding='utf-8',
                    delay=False
                )
                logging.warning('[config_service] Using RotatingFileHandler (not multi-process safe)')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            flush_handler = FlushHandler()
            flush_handler.setLevel(logging.DEBUG)
            root_logger.addHandler(flush_handler)
        _LOG_INITIALIZED = True
        logging.info('[config_service] Logging system initialized: platform + file')
        return True
    except Exception as e:
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
    global _PATHS_CONFIGURED, _CACHED_PATHS
    if _PATHS_CONFIGURED:
        return _CACHED_PATHS
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)
    paths_to_add = [current_dir]
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)
    _CACHED_PATHS = {'ali2026_dir': current_dir}
    _PATHS_CONFIGURED = True
    logging.info(f'[config_service] Paths configured: {_CACHED_PATHS}')
    return _CACHED_PATHS


def get_paths():
    if not _PATHS_CONFIGURED:
        return setup_paths()
    return _CACHED_PATHS


from ali2026v3_trading.service_container import ServiceContainer  # noqa: E402
