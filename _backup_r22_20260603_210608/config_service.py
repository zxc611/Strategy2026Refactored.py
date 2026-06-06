"""
配置服务模块 - CQRS 架构 Configuration 层
来源：分散的配置文件（quant_trading_system_T型图/config.py + 06_params.py 配置类）
功能：统一配置管理、路径配置、交易参数、输出配置

重构 v2.0 (2026-05-07):
- ✅ ExchangeConfig/品种映射/合约解析 → config_exchange.py
- ✅ 参数表/缓存/校验 → config_params.py
- ✅ 本模块保留：路径/交易/输出/日志配置 + 全局单例 + 便捷函数
- ✅ 向后兼容：所有 from ali2026v3_trading.config_service import xxx 不变

R13-P2-LOG-18修复: 项目集中日志规范
====================================

1. 日志格式:
   - 标准格式: [模块名] 描述信息 key=value ...
   - 示例: [ConfigService] 环境变量覆盖: DB_PATH=/data/strategy.duckdb
   - 标签统一使用方括号，模块名首字母大写驼峰

2. 日志级别约定:
   - DEBUG:   开发调试信息，生产环境默认关闭（如结构化日志写入异常）
   - INFO:    正常业务流程关键节点（如信号生成、状态切换、配置加载）
   - WARNING: 非致命异常/需要关注的降级（如历史截断、Greeks残差统计不显著）
   - ERROR:   致命错误/需要人工介入（如EventBus发布失败、审计日志写入失败）

3. 结构化日志:
   - 信号/交易等关键事件使用 StructuredLogger (JSONL格式) 记录
   - 常规日志使用 logging 标准库，格式字符串使用 %s 占位符（非f-string）
   - 周期性仪表盘日志支持变更摘要模式，仅输出显著变化的字段

4. 敏感数据脱敏 (R13-P2-LOG-17):
   - 环境变量中包含 password/secret/key/token/credential/auth 的值
     在日志中仅显示前2字符 + "***"
   - 示例: DB_SECRET_KEY=ab*** 而非 DB_SECRET_KEY=actual_secret_value

5. 审计日志 (R13-P0-LOG-08):
   - 关键操作（交易执行、配置变更）使用 AuditLogger 记录
   - 每条日志附带前一条的SHA256哈希，支持完整性验证

6. 性能监控日志 (R13-P2-LOG-12):
   - PathCounter 在 record_call 时自动检查阈值
   - 超阈值时输出 WARNING 级别告警，包含具体超限指标
"""
from __future__ import annotations

import os
import sys
import json
import time
import logging
import threading
from logging.handlers import RotatingFileHandler
# OPTIONAL-DEPENDENCY: concurrent_log_handler用于高并发日志写入优化  # R17-P2-DEP-04: 可选依赖，缺失时降级为RotatingFileHandler
try:
    from concurrent_log_handler import ConcurrentRotatingFileHandler
    _HAS_CONCURRENT_HANDLER = True
except ImportError:
    _HAS_CONCURRENT_HANDLER = False
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import timezone, timedelta

_CHINA_TZ = timezone(timedelta(hours=8))

from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer, safe_pickle_load, safe_pickle_dump

from ali2026v3_trading.shared_utils import normalize_year_month
from ali2026v3_trading.resilience_utils import is_disk_full_error

# P2修复: config_service版本号标记（增加版本号覆盖率）
_CONFIG_SERVICE_MODULE_VERSION = "2.0.0"  # P2修复: 配置服务模块版本
_PATH_CONFIG_VERSION = "1.0"              # P2修复: 路径配置版本
_TRADING_CONFIG_VERSION = "1.0"           # P2修复: 交易配置版本
_LOGGING_CONFIG_VERSION = "1.0"           # P2修复: 日志配置版本
_DATABASE_CONFIG_VERSION = "1.0"          # P2修复: 数据库配置版本
_PERFORMANCE_CONFIG_VERSION = "1.0"       # P2
# SEC-P2-01/02/03/06修复: 集成security_hardening安全加固
try:
    from ali2026v3_trading.security_hardening import (
        SANITIZE_ERROR_MSG, _truncate_stack_trace, _safe_unpickle,
        SecurityProfile, ENV_SECURITY_PROFILES,
        apply_security_profile,
    )
    import sys as _sys
    _orig_excepthook = _sys.excepthook
    def _secure_excepthook(exc_type, exc_val, exc_tb):
        import traceback as _tb
        _tb_text = ''.join(_tb.format_exception(exc_type, exc_val, exc_tb))
        _sanitized = SANITIZE_ERROR_MSG(_tb_text)
        logging.error("[SEC-P2-02] 异常(已截断):\n%s", _sanitized[:500])
    _sys.excepthook = _secure_excepthook
    _profile = ENV_SECURITY_PROFILES.get(
        os.environ.get('TRADE_ENV', 'production').lower(),
        SecurityProfile.PRODUCTION
    )
    apply_security_profile(_profile)
    logging.info("[SEC-P2-03] 安全配置已应用: profile=%s", _profile.name)
except ImportError:
    logging.debug("[SEC-P2] security_hardening模块不可用，跳过安全加固")
except Exception as _e:
    logging.warning("[SEC-P2] 安全加固集成失败: %s", _e)

修复: 性能配置版本

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
    """自定义 Handler，确保日志定期flush到磁盘
    LG-01修复: 移除os.fsync()同步阻塞，改为异步flush+周期性fsync，
    避免tick处理路径IO延迟导致风控/下单延迟"""
    _last_fsync_time = 0.0
    _fsync_interval = 5.0  # 每5秒执行一次fsync，而非每条日志

    def emit(self, record):
        try:
            now = time.time()
            do_fsync = (now - self._last_fsync_time >= self._fsync_interval)
            for handler in logging.getLogger().handlers:
                if isinstance(handler, RotatingFileHandler) and hasattr(handler, 'stream'):
                    handler.stream.flush()
                    if do_fsync:
                        os.fsync(handler.stream.fileno())
            if do_fsync:
                FlushHandler._last_fsync_time = now
        except Exception as _flush_err:
            # R32-P2-11修复: FlushHandler异常升级为warning(生产环境可见)，原R31用debug在生产不输出
            logging.warning("[R32-P2-11] FlushHandler flush/fsync失败: %s", _flush_err)
            if is_disk_full_error(_flush_err):
                logging.critical("[R33-P1-16] 日志flush失败: 磁盘满(ENOSPC)! 日志可能丢失")


# R13-P0-LOG-08修复: 防篡改审计日志 — SHA256哈希链
class AuditLogger:
    """R13-P0-LOG-08修复: 防篡改审计日志，每条日志附带前一条的SHA256哈希

    使用方式:
        audit = AuditLogger('/path/to/audit.log')
        audit.log('TRADE_EXECUTED', {'instrument': 'IF2506', 'volume': 1, 'price': 3950.2})
        # 验证完整性:
        is_valid = AuditLogger.verify_chain('/path/to/audit.log')
    """

    def __init__(self, log_path: str):
        self._log_path = log_path
        self._prev_hash: str = '0' * 64  # 创世哈希
        self._lock = threading.Lock()
        # R15-P1-SEC-04修复: 设置日志文件权限0o600
        try:
            if os.path.exists(log_path):
                os.chmod(log_path, 0o600)
        except Exception:
            pass
        # 如果日志文件已存在，恢复最后的哈希值
        self._recover_hash()

    def _recover_hash(self) -> None:
        """从已有日志文件恢复最后一条的哈希值"""
        try:
            if not os.path.exists(self._log_path):
                return
            with open(self._log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            if lines:
                last_line = lines[-1].strip()
                if last_line:
                    # 格式: timestamp | event_type | detail_json | prev_hash | current_hash
                    parts = last_line.split(' | ')
                    if len(parts) >= 5:
                        self._prev_hash = parts[-1].strip()
        except Exception:
            self._prev_hash = '0' * 64

    def log(self, event_type: str, detail: dict) -> str:
        """写入一条审计日志

        Args:
            event_type: 事件类型
            detail: 事件详情字典

        Returns:
            当前条目的哈希值
        """
        import hashlib
        from datetime import datetime as _dt

        timestamp = _dt.now(_CHINA_TZ).isoformat()
        detail_json = json_dumps(detail, sort_keys=True)

        # 计算当前条目的哈希: SHA256(prev_hash + timestamp + event_type + detail_json)
        raw = f"{self._prev_hash}{timestamp}{event_type}{detail_json}"
        current_hash = hashlib.sha256(raw.encode('utf-8')).hexdigest()

        log_line = f"{timestamp} | {event_type} | {detail_json} | {self._prev_hash} | {current_hash}"

        with self._lock:
            try:
                log_dir = os.path.dirname(self._log_path)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
                with open(self._log_path, 'a', encoding='utf-8') as f:
                    f.write(log_line + '\n')
                    f.flush()
                    os.fsync(f.fileno())
            except Exception as e:
                logging.error('[AuditLogger] R13-P0-LOG-08修复: 写入审计日志失败: %s', e)

            self._prev_hash = current_hash

        return current_hash

    @staticmethod
    def verify_chain(log_path: str) -> Tuple[bool, int]:
        """验证审计日志链的完整性

        Args:
            log_path: 审计日志文件路径

        Returns:
            (是否完整, 已验证的条目数)
        """
        import hashlib

        if not os.path.exists(log_path):
            return True, 0

        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception:
            return False, 0

        prev_hash = '0' * 64
        count = 0

        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
            parts = line.split(' | ')
            if len(parts) < 5:
                return False, count

            timestamp = parts[0]
            event_type = parts[1]
            detail_json = parts[2]
            stored_prev_hash = parts[3]
            stored_current_hash = parts[4]

            # 验证prev_hash链接
            if stored_prev_hash != prev_hash:
                return False, count

            # 验证当前哈希
            raw = f"{stored_prev_hash}{timestamp}{event_type}{detail_json}"
            expected_hash = hashlib.sha256(raw.encode('utf-8')).hexdigest()
            if stored_current_hash != expected_hash:
                return False, count

            prev_hash = stored_current_hash
            count += 1

        return True, count


# ============================================================================
# R13-P1-LOG-02修复: 集中日志格式常量 — 所有模块统一使用
# ============================================================================
LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s"  # R13-P1-LOG-02修复 [R22-P2-TIME01]
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"  # R13-P1-LOG-02修复

# R14-P1-LOG-09修复: 结构化JSON日志格式化器
class StructuredJsonFormatter(logging.Formatter):
    """结构化JSON日志格式化器 — 替代纯文本格式，便于日志聚合系统消费"""
    def format(self, record):
        log_obj = {
            'timestamp': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'line': record.lineno,
        }
        if record.exc_info and record.exc_info[0] is not None:
            log_obj['exception'] = self.formatException(record.exc_info)
        return json_dumps(log_obj)


# R13-P1-LOG-04修复: 结构化日志标准字段定义
@dataclass(slots=True)
class StructuredLogEntry:
    """结构化日志条目 — 定义标准字段，确保日志聚合与监控一致

    所有模块写入结构化日志时应使用此数据类，保证字段名和类型统一。
    """
    timestamp: str = ""
    event_type: str = ""
    module: str = ""
    level: str = "INFO"
    message: str = ""
    instrument_id: str = ""
    order_id: str = ""
    signal_id: str = ""
    strategy_id: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        from dataclasses import asdict
        return asdict(self)

    def to_json(self) -> str:
        return json_dumps(self.to_dict())


# ============================================================================
# 路径配置
# ============================================================================

@dataclass(slots=True)
class PathConfig:
    """路径配置"""
    project_root: str = str(Path(__file__).parent.parent)
    db_path: str = str(Path(__file__).parent.parent / "data" / "strategy.duckdb")
    state_file: str = str(Path(__file__).parent.parent / "data" / "strategy_state.json")
    log_dir: str = str(Path(__file__).parent / "logs")
    config_dir: str = str(Path(__file__).parent / "config")

    def to_dict(self) -> Dict[str, Any]:
        from dataclasses import asdict
        return asdict(self)


# ============================================================================
# 交易参数配置
# ============================================================================

@dataclass(slots=True)
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

class UIConfig:
    """UI显示配置常量 - 不影响策略逻辑，不作为策略参数暴露"""
    WINDOW_WIDTH: int = 260
    WINDOW_HEIGHT: int = 240
    FONT_LARGE: int = 11
    FONT_SMALL: int = 10
    TOP3_ROWS: int = 3


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

@dataclass(slots=True)
class LoggingConfig:
    """日志配置"""
    log_level: str = "INFO"
    log_format: str = LOG_FORMAT  # R13-P1-LOG-02修复: 使用集中常量
    log_date_format: str = LOG_DATE_FORMAT  # R13-P1-LOG-02修复: 使用集中常量
    log_to_console: bool = True
    log_to_file: bool = True
    log_file_prefix: str = "strategy"
    log_rotation_days: int = 1
    log_backup_count: int = 30
    enable_async_logging: bool = False
    log_queue_size: int = 1000

    # R13-P0-LOG-06修复: QueueHandler异步日志支持
    _async_queue_handler: Any = None
    _async_queue_listener: Any = None

    def setup_async_logging(self, root_logger: logging.Logger) -> bool:
        """R13-P0-LOG-06修复: 设置QueueHandler异步日志，使用单独线程处理文件写入

        Args:
            root_logger: 根日志器

        Returns:
            True if async logging was set up successfully
        """
        if not self.enable_async_logging:
            return False

        try:
            import queue
            from logging.handlers import QueueHandler, QueueListener

            log_queue = queue.Queue(maxsize=self.log_queue_size)

            # 收集现有的非QueueHandler handlers
            existing_handlers = [
                h for h in root_logger.handlers
                if not isinstance(h, QueueHandler)
            ]

            if not existing_handlers:
                return False

            # 创建QueueListener，在单独线程中处理日志写入
            listener = QueueListener(
                log_queue, *existing_handlers,
                respect_handler_level=True
            )
            listener.start()

            # 移除原有handlers，添加QueueHandler
            for h in existing_handlers:
                root_logger.removeHandler(h)

            queue_handler = QueueHandler(log_queue)
            root_logger.addHandler(queue_handler)

            self._async_queue_handler = queue_handler
            self._async_queue_listener = listener

            logging.info('[LoggingConfig] R13-P0-LOG-06修复: 异步日志已启用, queue_size=%d', self.log_queue_size)
            return True
        except Exception as e:
            logging.warning('[LoggingConfig] R13-P0-LOG-06修复: 异步日志设置失败: %s', e)
            return False

    def stop_async_logging(self) -> None:
        """R13-P0-LOG-06修复: 停止异步日志监听器"""
        if self._async_queue_listener is not None:
            try:
                self._async_queue_listener.stop()
                self._async_queue_listener = None
                self._async_queue_handler = None
            except Exception as e:
                logging.warning('[LoggingConfig] R13-P0-LOG-06修复: 停止异步日志失败: %s', e)


# ============================================================================
# 数据库配置
# ============================================================================

@dataclass(slots=True)
class DatabaseConfig:
    """数据库配置"""
    db_type: str = "duckdb"
    db_path: str = str(Path(__file__).parent.parent / "data" / "strategy.duckdb")
    connection_pool_size: int = 5
    connection_timeout: float = 30.0
    enable_connection_pooling: bool = True
    auto_commit: bool = False

    def __post_init__(self):
        pass


# ============================================================================
# 数据路径配置
# ============================================================================

@dataclass(slots=True)
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
        from dataclasses import asdict
        return asdict(self)


# ============================================================================
# 性能监控配置
# ============================================================================

@dataclass(slots=True)
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
        # AP-03: deprecation warning — 推荐使用get_config_service()而非ConfigService()直接构造
        import warnings
        warnings.warn(
            "ConfigService() direct construction is deprecated; use get_config_service() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return cls._instance
    
    def __init__(self):
        with type(self)._init_lock:
            if getattr(self, '_initialized', False):
                return
            self._initialized = True
            self._config_file_path: Optional[Path] = None
            self._last_modified_time: float = 0.0
            # P2-R11-13修复: 配置热加载冷却期 — 使用time.monotonic()避免系统时钟跳变干扰
            self._last_config_reload_monotonic: float = 0.0
            self._CONFIG_RELOAD_COOLDOWN_SEC: float = 5.0  # P2-R11-13修复: 最小重载间隔(秒)
            # SER-P1-14修复: 配置快照和回滚机制
            self._config_snapshot: Dict[str, Any] = {}
            self._snapshot_timestamp: float = 0.0
            self.paths = PathConfig()
            self.exchanges = ExchangeConfig()
            self.trading = TradingConfig()
            self.logging = LoggingConfig()
            self.database = DatabaseConfig()
            self.data_paths = DataPathsConfig()
            self.performance = PerformanceConfig()
            self.output = OutputConfig()
            self._load_config_file()
            self._apply_environment_overrides()
            self._sync_config_data()
            self._cascade_threshold_grid: Optional[Dict[str, Any]] = None
            self._plr_thresholds: Optional[Dict[str, Any]] = None
            self._load_yaml_configs()
    
    # UPG-10修复: config.json schema校验
    _CONFIG_SCHEMA = {
        'paths': {
            'type': dict,
            'required_keys': [],
        },
        'trading': {
            'type': dict,
            'required_keys': ['default_volume', 'max_position_limit', 'max_daily_loss'],
            'key_types': {
                'default_volume': int,
                'max_position_limit': int,
                'max_daily_loss': (int, float),
            },
        },
        'logging': {
            'type': dict,
            'required_keys': ['log_level'],
            'key_types': {
                'log_level': str,
            },
        },
        'database': {
            'type': dict,
            'required_keys': ['db_type', 'db_path'],
            'key_types': {
                'db_type': str,
                'db_path': str,
            },
        },
    }

    def _validate_config_schema(self, config: Dict[str, Any]) -> None:
        """UPG-10修复: 校验config.json的schema

        检查每个顶层section的必需字段和字段类型，
        缺失必需字段或类型不匹配时抛出RuntimeError。
        """
        errors = []
        for section_name, schema in self._CONFIG_SCHEMA.items():
            if section_name not in config:
                continue  # section不存在不报错(使用默认值)
            section = config[section_name]
            if not isinstance(section, schema['type']):
                errors.append(f"{section_name}: 期望type={schema['type'].__name__}, 实际={type(section).__name__}")
                continue
            # 检查必需字段
            for key in schema.get('required_keys', []):
                if key not in section:
                    errors.append(f"{section_name}.{key}: 必需字段缺失")
            # 检查字段类型
            for key, expected_type in schema.get('key_types', {}).items():
                if key in section and not isinstance(section[key], expected_type):
                    if isinstance(expected_type, tuple):
                        type_names = '/'.join(t.__name__ for t in expected_type)
                    else:
                        type_names = expected_type.__name__
                    errors.append(f"{section_name}.{key}: 期望type={type_names}, 实际={type(section[key]).__name__}")
        if errors:
            error_msg = '; '.join(errors)
            logging.error("[ConfigService] UPG-10: config.json schema校验失败: %s", error_msg)
            raise RuntimeError(f"UPG-10: config schema validation failed: {error_msg}")

    def _load_config_file(self, config_path: Optional[str] = None) -> None:
        if config_path:
            path = Path(config_path)
        else:
            path = Path(self.paths.config_dir) / "config.json"
        # R15-P1-SEC-02修复: 路径消毒+越权检查
        try:
            _resolved = os.path.realpath(str(path))
            _project_root = os.path.realpath(os.path.dirname(os.path.abspath(__file__)))
            if not _resolved.startswith(_project_root):
                logging.error("R15-P1-SEC-02: 配置文件路径越权 path=%s resolved=%s", path, _resolved)
                return
        except Exception as _pce:
            logging.warning("R15-P1-SEC-02: 路径消毒检查异常: %s", _pce)
        if not path.exists():
            return
        try:
            with open(path, 'r', encoding='utf-8') as f:
                file_config = json.load(f)
            # R15-P1-SEC-03修复: 基本schema检查(顶层key必须为dict)
            if not isinstance(file_config, dict):
                raise RuntimeError(f"R15-P1-SEC-03: config顶层必须为dict, 实际为{type(file_config).__name__}")
            # UPG-10修复: 配置schema校验
            self._validate_config_schema(file_config)
            self._config_file_path = path
            self._last_modified_time = path.stat().st_mtime
            self._update_from_dict(file_config)
        except Exception as e:
            logging.error("[ConfigService] Failed to load config file: %s", e)  # R13-P2-LOG-01修复
            raise RuntimeError(f"Config load failed: {e}") from e
    
    def _update_from_dict(self, config_dict: Dict[str, Any]) -> None:
        # R24-P1-IV-12修复: UI参数校验
        try:
            from ali2026v3_trading.config_params import validate_ui_params
            is_valid, errors = validate_ui_params(config_dict)
            if not is_valid:
                logging.warning("[R24-P1-IV-12] UI参数校验失败: %s", errors)
        except Exception:
            pass
        # R24-P1-IV-12修复: _update_from_dict类型验证辅助函数
        def _safe_setattr(obj, key, value):
            """在setattr前检查类型一致性，类型不匹配时记录warning"""
            if not hasattr(obj, key):
                return
            old_value = getattr(obj, key)
            if old_value is not None and value is not None:
                old_type = type(old_value)
                new_type = type(value)
                # 允许int/float互转，其他类型不匹配时告警
                _numeric_types = (int, float)
                if not (isinstance(old_value, _numeric_types) and isinstance(value, _numeric_types)):
                    if old_type != new_type:
                        logging.warning(
                            "[R24-P1-IV-12] 配置类型不匹配: key=%s old_type=%s new_type=%s old_value=%r new_value=%r, 仍将设置",
                            key, old_type.__name__, new_type.__name__, old_value, value,
                        )
            setattr(obj, key, value)

        if 'paths' in config_dict:
            for key, value in config_dict['paths'].items():
                if hasattr(self.paths, key):
                    _safe_setattr(self.paths, key, value)
        if 'exchanges' in config_dict:
            for key, value in config_dict['exchanges'].items():
                if hasattr(self.exchanges, key):
                    _safe_setattr(self.exchanges, key, value)
        if 'trading' in config_dict:
            for key, value in config_dict['trading'].items():
                if hasattr(self.trading, key):
                    _safe_setattr(self.trading, key, value)
        if 'logging' in config_dict:
            for key, value in config_dict['logging'].items():
                if hasattr(self.logging, key):
                    _safe_setattr(self.logging, key, value)
        if 'database' in config_dict:
            for key, value in config_dict['database'].items():
                if hasattr(self.database, key):
                    _safe_setattr(self.database, key, value)
        if 'performance' in config_dict:
            for key, value in config_dict['performance'].items():
                if hasattr(self.performance, key):
                    _safe_setattr(self.performance, key, value)
        if 'data_paths' in config_dict:
            for key, value in config_dict['data_paths'].items():
                if hasattr(self.data_paths, key):
                    _safe_setattr(self.data_paths, key, value)
        if 'output' in config_dict:
            for key, value in config_dict['output'].items():
                if key == 'mode':
                    self.output.set_mode(value)
                elif hasattr(self.output, key):
                    _safe_setattr(self.output, key, value)
    
    # R13-P2-LOG-17修复: 敏感关键词列表，匹配到的环境变量值在日志中脱敏
    _SENSITIVE_KEYWORDS = ('password', 'secret', 'key', 'token', 'credential', 'auth')

    @staticmethod
    def _mask_sensitive_value(key: str, value: str) -> str:
        """R13-P2-LOG-17修复: 对包含敏感关键词的环境变量值进行脱敏

        若key中包含password/secret/key/token等关键词，仅显示前2字符+***。
        """
        key_lower = key.lower()
        for kw in ConfigService._SENSITIVE_KEYWORDS:
            if kw in key_lower:
                if len(value) <= 2:
                    return '***'
                return value[:2] + '***'
        return value

    def _apply_environment_overrides(self) -> None:
        # R13-P2-LOG-17修复: 记录环境变量覆盖，敏感值脱敏
        _overrides_applied = []
        # R24-P2-IV-03修复: 环境变量覆盖添加基本值域验证
        _VALID_LOG_LEVELS = frozenset(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
        if 'TRADING_DB_PATH' in os.environ:
            self.database.db_path = os.environ['TRADING_DB_PATH']
            _overrides_applied.append(('TRADING_DB_PATH', os.environ['TRADING_DB_PATH']))
        if 'LOG_LEVEL' in os.environ:
            _level = os.environ['LOG_LEVEL'].upper()
            if _level in _VALID_LOG_LEVELS:
                self.logging.log_level = _level
                _overrides_applied.append(('LOG_LEVEL', _level))
            else:
                logging.warning("[R24-P2-IV-03] LOG_LEVEL环境变量值无效: %s, 有效值: %s", _level, sorted(_VALID_LOG_LEVELS))
        if 'OUTPUT_MODE' in os.environ:
            output_mode = os.environ['OUTPUT_MODE'].lower()
            # R26-P2-IV-03修复: OUTPUT_MODE值域校验
            _VALID_OUTPUT_MODES = frozenset(['json', 'text', 'csv', 'table'])
            if output_mode in _VALID_OUTPUT_MODES:
                self.output.set_mode(output_mode)
                _overrides_applied.append(('OUTPUT_MODE', os.environ['OUTPUT_MODE']))
            else:
                logging.warning("[R26-P2-IV-03] OUTPUT_MODE环境变量值无效: %s, 有效值: %s", output_mode, sorted(_VALID_OUTPUT_MODES))
        if 'ENABLE_PERFORMANCE_MONITORING' in os.environ:
            value = os.environ['ENABLE_PERFORMANCE_MONITORING'].lower()
            self.performance.enable_performance_monitoring = value in ('true', '1', 'yes')
            _overrides_applied.append(('ENABLE_PERFORMANCE_MONITORING', os.environ['ENABLE_PERFORMANCE_MONITORING']))
        if 'DUCKDB_FILE' in os.environ:
            val = os.environ['DUCKDB_FILE']
            self.data_paths.duckdb_file = val.strip() if val.strip() else self.data_paths.duckdb_file
            _overrides_applied.append(('DUCKDB_FILE', val))
        if 'TICK_DATA_PATH' in os.environ:
            val = os.environ['TICK_DATA_PATH']
            self.data_paths.parquet_path = val.strip() if val.strip() else self.data_paths.parquet_path
            _overrides_applied.append(('TICK_DATA_PATH', val))
        if 'INTRADAY_FLUSH_WINDOWS' in os.environ:
            val = os.environ['INTRADAY_FLUSH_WINDOWS']
            self.data_paths.flush_windows_str = val.strip() if val.strip() else self.data_paths.flush_windows_str
            _overrides_applied.append(('INTRADAY_FLUSH_WINDOWS', val))
        if _overrides_applied:
            masked = ", ".join(
                "%s=%s" % (k, self._mask_sensitive_value(k, v))
                for k, v in _overrides_applied
            )
            logging.info("[ConfigService] 环境变量覆盖: %s # R13-P2-LOG-17修复", masked)
    
    def _sync_config_data(self) -> None:
        with type(self)._config_data_lock:
            type(self)._config_data = self.to_dict()
    
    def get_config_data(self) -> Dict[str, Any]:
        with type(self)._config_data_lock:
            if not type(self)._config_data:
                self._sync_config_data()
            return dict(type(self)._config_data)
    
    # UPG-P1-06修复: 配置格式自动升级
    _CONFIG_FORMAT_VERSION = "2.0"
    _CONFIG_FORMAT_MIGRATIONS = {
        # (from_version, to_version): migration_function_name
        ("1.0", "2.0"): "_migrate_config_v1_to_v2",
    }

    # R17-P1-CFG-P1-06: YAML版本号验证 — 加载YAML时校验config_version
    _YAML_REQUIRED_VERSION = "2.0"

    def _validate_yaml_config_version(self, yaml_data: Dict[str, Any]) -> bool:
        """R17-P1-CFG-P1-06: 验证YAML配置版本号"""
        version = yaml_data.get('config_version', yaml_data.get('format_version', ''))
        if not version:
            logging.warning("[R17-P1-CFG-P1-06] YAML配置缺少config_version字段，默认通过")
            return True
        if version != self._YAML_REQUIRED_VERSION:
            logging.warning(
                "[R17-P1-CFG-P1-06] YAML配置版本不匹配: expected=%s actual=%s",
                self._YAML_REQUIRED_VERSION, version,
            )
            return False
        return True

    def _upgrade_config_format(self, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """UPG-P1-06修复: 配置格式自动升级

        检查配置中的format_version字段，按序执行迁移链。
        迁移失败时返回原始配置（保守策略）。

        Args:
            raw_config: 原始配置字典

        Returns:
            迁移后的配置字典
        """
        current_version = raw_config.get('format_version', '1.0')
        if current_version == self._CONFIG_FORMAT_VERSION:
            return raw_config

        config = dict(raw_config)
        migrated = False

        while current_version != self._CONFIG_FORMAT_VERSION:
            migration_key = (current_version, self._CONFIG_FORMAT_VERSION)
            # 查找直接迁移或逐步迁移
            next_step = None
            for (from_v, to_v), func_name in self._CONFIG_FORMAT_MIGRATIONS.items():
                if from_v == current_version:
                    next_step = ((from_v, to_v), func_name)
                    break

            if next_step is None:
                logging.warning(
                    "UPG-P1-06: 无迁移路径 from=%s to=%s, 保留原配置",
                    current_version, self._CONFIG_FORMAT_VERSION,
                )
                break

            (from_v, to_v), func_name = next_step
            migrator = getattr(self, func_name, None)
            if migrator is None:
                logging.warning(
                    "UPG-P1-06: 迁移函数%s不存在, 保留原配置", func_name,
                )
                break

            try:
                config = migrator(config)
                current_version = to_v
                config['format_version'] = to_v
                migrated = True
                logging.info("UPG-P1-06: 配置格式迁移 %s → %s 成功", from_v, to_v)
            except Exception as e:
                logging.error(
                    "UPG-P1-06: 配置格式迁移 %s → %s 失败: %s, 保留原配置",
                    from_v, to_v, e,
                )
                break

        if migrated:
            logging.info("UPG-P1-06: 配置格式升级完成 → %s", current_version)
        return config

    def _migrate_config_v1_to_v2(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """UPG-P1-06修复: v1.0 → v2.0 迁移

        迁移内容:
        - trading.max_daily_loss: str → float
        - paths新增flush_windows_str字段(默认值)
        - exchanges新增simulated_instruments字段(默认空列表)
        """
        result = dict(config)

        # trading.max_daily_loss 类型迁移
        trading = result.get('trading', {})
        if isinstance(trading.get('max_daily_loss'), str):
            try:
                trading['max_daily_loss'] = float(trading['max_daily_loss'])
            except (ValueError, TypeError):
                trading['max_daily_loss'] = 50000.0
            result['trading'] = trading

        # paths新增字段
        paths = result.get('paths', {})
        if 'flush_windows_str' not in paths:
            paths['flush_windows_str'] = '09:30,14:30'
            result['paths'] = paths

        # exchanges新增字段
        exchanges = result.get('exchanges', {})
        if 'simulated_instruments' not in exchanges:
            exchanges['simulated_instruments'] = []
            result['exchanges'] = exchanges

        result['format_version'] = '2.0'
        return result

    # UPG-P1-08修复: 基于文件的分布式锁机制
    _distributed_lock_dir: Optional[str] = None

    def _get_distributed_lock_path(self, lock_name: str) -> str:
        """UPG-P1-08修复: 获取分布式锁文件路径"""
        if self._distributed_lock_dir is None:
            self._distributed_lock_dir = os.path.join(
                os.path.dirname(str(self._config_file_path)), '.locks'
            )
        os.makedirs(self._distributed_lock_dir, exist_ok=True)
        return os.path.join(self._distributed_lock_dir, f"{lock_name}.lock")

    def _acquire_distributed_lock(self, lock_name: str, timeout_sec: float = 30.0) -> bool:
        """UPG-P1-08修复: 获取基于文件的分布式锁

        用于多节点/多进程协调，防止并发配置更新。

        Args:
            lock_name: 锁名称
            timeout_sec: 超时时间(秒)

        Returns:
            bool: 是否成功获取锁
        """
        lock_path = self._get_distributed_lock_path(lock_name)
        deadline = time.time() + timeout_sec

        while time.time() < deadline:
            try:
                # 尝试独占创建锁文件
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()}\n{time.time()}".encode())
                os.close(fd)
                return True
            except FileExistsError:
                # 检查锁是否过期（超过60秒视为过期）
                try:
                    lock_age = time.time() - os.path.getmtime(lock_path)
                    if lock_age > 60.0:
                        os.unlink(lock_path)
                        logging.info("UPG-P1-08: 清除过期锁 %s (age=%.1fs)", lock_name, lock_age)
                except OSError:
                    pass
                time.sleep(0.1)  # R23-P2-06标记: P2级阻塞重试
            except OSError as e:
                logging.warning("UPG-P1-08: 获取锁失败 %s: %s", lock_name, e)
                return False

        logging.warning("UPG-P1-08: 获取锁超时 %s (%.1fs)", lock_name, timeout_sec)
        return False

    def _release_distributed_lock(self, lock_name: str) -> None:
        """UPG-P1-08修复: 释放分布式锁"""
        lock_path = self._get_distributed_lock_path(lock_name)
        try:
            os.unlink(lock_path)
        except OSError:
            pass

    def reload(self) -> bool:
        if self._config_file_path and self._config_file_path.exists():
            current_mtime = self._config_file_path.stat().st_mtime
            if current_mtime > self._last_modified_time:
                # P2-R11-13修复: 配置热加载冷却期 — time.monotonic()不受系统时钟跳变影响
                _now_mono = time.monotonic()
                if _now_mono - self._last_config_reload_monotonic < self._CONFIG_RELOAD_COOLDOWN_SEC:
                    logging.debug(
                        "[ConfigService] P2-R11-13: 配置重载冷却中 (elapsed=%.2fs < cooldown=%.2fs)",
                        _now_mono - self._last_config_reload_monotonic,
                        self._CONFIG_RELOAD_COOLDOWN_SEC,
                    )
                    return False
                self._last_config_reload_monotonic = _now_mono
                # UPG-P1-08修复: 使用分布式锁防止多节点并发更新
                lock_acquired = self._acquire_distributed_lock('config_reload', timeout_sec=10.0)
                if not lock_acquired:
                    logging.warning("UPG-P1-08: 无法获取配置重载锁，跳过本次reload")
                    return False
                try:
                    # SER-P1-14修复: 热加载前创建配置快照
                    self.create_config_snapshot('before_reload')
                    try:
                        self._load_config_file(str(self._config_file_path))
                        # UPG-P1-06修复: 配置格式自动升级
                        raw = self.to_dict()
                        upgraded = self._upgrade_config_format(raw)
                        if upgraded is not raw:
                            self._update_from_dict(upgraded)
                        self._sync_config_data()
                        return True
                    except Exception as e:
                        logging.error("SER-P1-14: 热加载失败，回滚到配置快照: %s", e)
                        self.rollback_config_snapshot('before_reload')
                        return False
                finally:
                    self._release_distributed_lock('config_reload')
        return False

    def _notify_config_update_via_eventbus(self, updated_keys: Optional[List[str]] = None) -> None:
        """UPG-P1-08修复: 通过EventBus通知其他节点配置已更新

        在多节点部署中，当一个节点完成配置更新后，
        通过EventBus发布配置变更事件，通知其他节点刷新本地缓存。

        Args:
            updated_keys: 更新的配置键列表
        """
        try:
            from ali2026v3_trading.event_bus import EventBus
            event_bus = EventBus.get_instance()
            if event_bus is not None:
                event_bus.publish(type('ConfigUpdatedEvent', (), {
                    'type': 'ConfigUpdatedEvent',
                    'source': 'config_service',
                    'node_id': os.environ.get('NODE_ID', os.getpid()),
                    'updated_keys': updated_keys or [],
                    'timestamp': time.time(),
                })(), async_mode=True)
                logging.info(
                    "UPG-P1-08: 配置更新事件已通过EventBus发布 (keys=%s)",
                    updated_keys,
                )
        except ImportError:
            logging.debug("UPG-P1-08: EventBus不可用，跳过配置更新通知")
        except Exception as e:
            logging.warning("UPG-P1-08: 配置更新EventBus通知失败: %s", e)

    def _subscribe_config_update_events(self) -> None:
        """UPG-P1-08修复: 订阅其他节点的配置更新事件

        当收到其他节点的配置更新通知时，自动刷新本地配置缓存。
        """
        try:
            from ali2026v3_trading.event_bus import EventBus
            event_bus = EventBus.get_instance()
            if event_bus is not None:
                def _on_config_updated(event):
                    node_id = getattr(event, 'node_id', None)
                    current_node = os.environ.get('NODE_ID', os.getpid())
                    if node_id != current_node:
                        logging.info(
                            "UPG-P1-08: 收到节点%s的配置更新通知，刷新本地缓存",
                            node_id,
                        )
                        self._last_modified_time = 0  # 强制下次reload
                event_bus.subscribe('ConfigUpdatedEvent', _on_config_updated)
                logging.info("UPG-P1-08: 已订阅配置更新事件")
        except ImportError:
            logging.debug("UPG-P1-08: EventBus不可用，跳过配置更新订阅")
        except Exception as e:
            logging.warning("UPG-P1-08: 配置更新事件订阅失败: %s", e)
    
    def need_reload(self) -> bool:
        if self._config_file_path and self._config_file_path.exists():
            current_mtime = self._config_file_path.stat().st_mtime
            return current_mtime > self._last_modified_time
        return False
    
    def create_config_snapshot(self, snapshot_name: Optional[str] = None) -> str:
        """
        SER-P1-14修复: 创建配置快照
        
        使用方式：
        >>> snapshot_id = config_service.create_config_snapshot('before_reload')
        
        参数：
        - snapshot_name: 快照名称（可选，默认使用时间戳）
        
        返回：
        - 快照ID（用于回滚时引用）
        """
        import time
        snapshot_id = snapshot_name or f"snapshot_{int(time.time() * 1000)}"
        self._config_snapshot = {
            'id': snapshot_id,
            'config': self.to_dict(),
            'timestamp': time.time(),
        }
        self._snapshot_timestamp = time.time()
        logging.info(
            "[ConfigService] SER-P1-14: 配置快照已创建 id=%s timestamp=%.3f",
            snapshot_id,
            self._snapshot_timestamp,
        )
        return snapshot_id
    
    def rollback_config_snapshot(self, snapshot_id: Optional[str] = None) -> bool:
        """
        SER-P1-14修复: 回滚到配置快照
        
        使用方式：
        >>> success = config_service.rollback_config_snapshot('before_reload')
        
        参数：
        - snapshot_id: 快照ID（可选，默认回滚到最近一次快照）
        
        返回：
        - True: 回滚成功
        - False: 回滚失败（快照不存在或ID不匹配）
        """
        if not self._config_snapshot:
            logging.warning("[ConfigService] SER-P1-14: 无可用快照，回滚失败")
            return False
        
        if snapshot_id and self._config_snapshot.get('id') != snapshot_id:
            logging.warning(
                "[ConfigService] SER-P1-14: 快照ID不匹配 expected=%s actual=%s，回滚失败",
                snapshot_id,
                self._config_snapshot.get('id'),
            )
            return False
        
        try:
            config_data = self._config_snapshot['config']
            self._update_from_dict(config_data)
            self._sync_config_data()
            logging.info(
                "[ConfigService] SER-P1-14: 配置已回滚到快照 id=%s timestamp=%.3f",
                self._config_snapshot['id'],
                self._config_snapshot['timestamp'],
            )
            return True
        except Exception as e:
            logging.error("[ConfigService] SER-P1-14: 配置回滚失败: %s", e)
            return False
    
    def get_snapshot_info(self) -> Optional[Dict[str, Any]]:
        """
        SER-P1-14修复: 获取当前快照信息
        
        返回：
        - 快照信息字典（包含id、timestamp、config大小）
        - None: 无快照
        """
        if not self._config_snapshot:
            return None
        return {
            'id': self._config_snapshot['id'],
            'timestamp': self._config_snapshot['timestamp'],
            'config_size': len(json_dumps(self._config_snapshot['config'])),
        }

    def load_checkpoint_safe(self, checkpoint_path: str) -> Optional[Dict[str, Any]]:
        """SER-P1-07修复: 使用safe_pickle_load安全加载checkpoint文件。
        
        替代直接pickle.load，增加文件大小限制和RCE风险防护。
        """
        try:
            data = safe_pickle_load(checkpoint_path)
            if isinstance(data, dict):
                logging.info("[SER-P1-07] checkpoint安全加载成功: %s", checkpoint_path)
                return data
            logging.warning("[SER-P1-07] checkpoint数据类型异常: %s, 期望dict实际%s", checkpoint_path, type(data).__name__)
            return None
        except Exception as e:
            logging.warning("[SER-P1-07] checkpoint安全加载失败: %s, err=%s", checkpoint_path, e)
            return None

    def save_checkpoint_safe(self, data: Dict[str, Any], checkpoint_path: str) -> bool:
        """SER-P1-07修复: 使用safe_pickle_dump安全保存checkpoint文件。"""
        try:
            safe_pickle_dump(data, checkpoint_path)
            logging.info("[SER-P1-07] checkpoint安全保存成功: %s", checkpoint_path)
            return True
        except Exception as e:
            logging.warning("[SER-P1-07] checkpoint安全保存失败: %s, err=%s", checkpoint_path, e)
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
            'data_paths': self.data_paths.to_dict(),
            'output': {
                'mode': self.output.mode,
                'diagnostic_output': self.output.diagnostic_output,
                'debug_output': self.output.debug_output,
            }
        }
    
    def save_to_file(self, config_path: str) -> bool:
        try:
            path = Path(config_path)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(json_dumps(self.to_dict(), indent=2))
            self._config_file_path = path
            self._last_modified_time = path.stat().st_mtime
            return True
        except Exception as e:
            logging.error("[ConfigService] Failed to save config file: %s", e)  # R13-P2-LOG-01修复
            raise RuntimeError(f"Config save failed: {e}") from e

    # P2-项19修复: 多环境配置差异管理
    def diff_config(self, other_config: Dict[str, Any]) -> Dict[str, Any]:
        """P2-项19修复: 对比当前配置与另一环境配置的差异

        用于多环境(开发/测试/生产)配置一致性检查。

        Args:
            other_config: 另一环境的配置字典

        Returns:
            Dict: {only_in_self, only_in_other, value_diffs, diff_count}
        """
        current = self.to_dict()
        only_in_self = []
        only_in_other = []
        value_diffs = []

        def _diff_recursive(d1: Dict, d2: Dict, prefix: str = '') -> None:
            all_keys = set(d1.keys()) | set(d2.keys())
            for key in all_keys:
                path = f'{prefix}.{key}' if prefix else key
                if key not in d2:
                    only_in_self.append(path)
                elif key not in d1:
                    only_in_other.append(path)
                elif isinstance(d1[key], dict) and isinstance(d2[key], dict):
                    _diff_recursive(d1[key], d2[key], path)
                elif d1[key] != d2[key]:
                    value_diffs.append({
                        'path': path,
                        'self_value': d1[key],
                        'other_value': d2[key],
                    })

        _diff_recursive(current, other_config)

        return {
            'only_in_self': only_in_self,
            'only_in_other': only_in_other,
            'value_diffs': value_diffs,
            'diff_count': len(only_in_self) + len(only_in_other) + len(value_diffs),
        }

    # UPG-P1-11修复: 代码/手册/配置版本对齐检查
    _CONFIG_VERSION = "2.0"  # 配置版本号

    def check_version_alignment(self, code_version: str = None,
                                 manual_version: str = None,
                                 config_version: str = None) -> Dict[str, Any]:
        """UPG-P1-11修复: 检查代码版本、手册版本、配置版本是否对齐

        确保代码实现、操作手册、配置格式三者的版本一致，
        防止版本漂移导致操作错误或配置不兼容。

        Args:
            code_version: 代码版本号（默认使用shared_utils._CODE_VERSION）
            manual_version: 手册版本号（默认从环境变量MANUAL_VERSION读取）
            config_version: 配置版本号（默认使用_CONFIG_FORMAT_VERSION）

        Returns:
            Dict: {
                aligned: bool,
                code_version: str,
                manual_version: str,
                config_version: str,
                mismatches: list,
                warning: str,
            }
        """
        # 获取各版本号
        try:
            from ali2026v3_trading.shared_utils import _CODE_VERSION
            cv = code_version or _CODE_VERSION
        except ImportError:
            cv = code_version or "unknown"

        mv = manual_version or os.environ.get('MANUAL_VERSION', 'unknown')
        cfgv = config_version or self._CONFIG_FORMAT_VERSION

        mismatches = []

        # 检查代码版本与手册版本
        if cv != mv and mv != 'unknown':
            mismatches.append(
                f"代码版本({cv})与手册版本({mv})不一致"
            )

        # 检查代码版本与配置版本
        if cv != cfgv:
            mismatches.append(
                f"代码版本({cv})与配置版本({cfgv})不一致"
            )

        # 检查手册版本与配置版本
        if mv != cfgv and mv != 'unknown':
            mismatches.append(
                f"手册版本({mv})与配置版本({cfgv})不一致"
            )

        aligned = len(mismatches) == 0
        warning = ""
        if not aligned:
            warning = (
                f"版本对齐检查发现{len(mismatches)}处不一致: "
                + "; ".join(mismatches)
            )
            logging.warning("UPG-P1-11: %s", warning)
        else:
            logging.info(
                "UPG-P1-11: 版本对齐检查通过: code=%s manual=%s config=%s",
                cv, mv, cfgv,
            )

        return {
            'aligned': aligned,
            'code_version': cv,
            'manual_version': mv,
            'config_version': cfgv,
            'mismatches': mismatches,
            'warning': warning,
        }

    def _load_yaml_configs(self) -> None:
        try:
            self._cascade_threshold_grid = load_cascade_threshold_grid()
            if self._cascade_threshold_grid:
                logging.info("[ConfigService] P1-4: cascade_threshold_grid.yaml 已加载, keys=%s",
                             list(self._cascade_threshold_grid.keys())[:5])
        except Exception as _e:
            logging.warning("[ConfigService] P1-4: 加载cascade_threshold_grid.yaml失败: %s", _e)
        try:
            self._plr_thresholds = load_plr_thresholds()
            if self._plr_thresholds:
                logging.info("[ConfigService] P1-4: plr_thresholds.yaml 已加载, keys=%s",
                             list(self._plr_thresholds.keys())[:5])
        except Exception as _e:
            logging.warning("[ConfigService] P1-4: 加载plr_thresholds.yaml失败: %s", _e)

    def get_cascade_threshold_grid(self) -> Optional[Dict[str, Any]]:
        if self._cascade_threshold_grid is None:
            self._cascade_threshold_grid = load_cascade_threshold_grid()
        return self._cascade_threshold_grid

    def get_plr_thresholds(self) -> Optional[Dict[str, Any]]:
        if self._plr_thresholds is None:
            self._plr_thresholds = load_plr_thresholds()
        return self._plr_thresholds


# ============================================================================
# P1-4修复: 孤立YAML配置文件加载器
# ============================================================================

def _load_yaml_file(yaml_path: str) -> Dict[str, Any]:
    try:
        import yaml
    except ImportError:
        try:
            from ruamel.yaml import YAML as _YAML
            _yaml_loader = _YAML()
            with open(yaml_path, 'r', encoding='utf-8') as _f:
                return dict(_yaml_loader.load(_f) or {})
        except ImportError:
            logging.error("[ConfigService] P1-4: yaml和ruamel.yaml均不可用，无法加载YAML配置")
            return {}
    with open(yaml_path, 'r', encoding='utf-8') as _f:
        data = yaml.safe_load(_f)
    # SEC-P1-03修复: YAML基本schema验证
    if not isinstance(data, dict):
        return {}
    _required_top_keys = {'products', 'thresholds', 'default'}
    _found_keys = set(data.keys())
    if _required_top_keys & _found_keys:
        for _k, _v in data.items():
            if _k in ('products', 'thresholds') and not isinstance(_v, (dict, list)):
                logging.error("[SEC-P1-03] YAML字段类型错误: %s期望dict/list, 实际%s", _k, type(_v).__name__)
    return data if isinstance(data, dict) else {}


def load_cascade_threshold_grid() -> Dict[str, Any]:
    _module_dir = os.path.dirname(os.path.abspath(__file__))
    _yaml_path = os.path.join(_module_dir, "config", "cascade_threshold_grid.yaml")
    if not os.path.exists(_yaml_path):
        # R32-P2-09修复: 瀑布阈值网格配置缺失升级为error(原仅warning)
        logging.error("[ConfigService] R32-P2-09: cascade_threshold_grid.yaml 不存在: %s (策略评分将无阈值基准)", _yaml_path)
        return {}
    data = _load_yaml_file(_yaml_path)
    logging.info("[ConfigService] P1-4: load_cascade_threshold_grid() 加载成功, path=%s", _yaml_path)
    return data


def load_plr_thresholds() -> Dict[str, Any]:
    _module_dir = os.path.dirname(os.path.abspath(__file__))
    _yaml_path = os.path.join(_module_dir, "param_pool", "plr_thresholds.yaml")
    if not os.path.exists(_yaml_path):
        _fallback_path = os.path.join(_module_dir, "param_pool", "plr_thresholds.yaml")
        if os.path.exists(_fallback_path):
            _yaml_path = _fallback_path
        else:
            # R32-P2-09修复: PLR阈值配置缺失升级为error(原仅warning)
            logging.error("[ConfigService] R32-P2-09: plr_thresholds.yaml 不存在 (param_pool/ 和 参数池/ 均未找到, PLR评判将无阈值)")
            return {}
    data = _load_yaml_file(_yaml_path)
    logging.info("[ConfigService] P1-4: load_plr_thresholds() 加载成功, path=%s", _yaml_path)
    return data


# ============================================================================
# Params 相关辅助
# ============================================================================

def ConfigField(default=None, title="", **kwargs):
    from dataclasses import field as _dc_field
    return _dc_field(default=default, metadata={"title": title, **kwargs})


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
                # AP-03: SingletonRegistry注册
                try:
                    from ali2026v3_trading.singleton_registry import SingletonRegistry
                    registry = SingletonRegistry.get_registry("config_service")
                    registry.register_singleton("config_service.instance", _config_service_instance)
                except Exception:
                    pass
    return _config_service_instance


def get_config_service() -> ConfigService:
    """AP-03: 统一单例获取入口，消除ConfigService()直接构造的双路径问题"""
    return get_config()

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
    'UIConfig',
    'LoggingConfig',
    'DatabaseConfig',
    'PerformanceConfig',
    'AuditLogger',  # R13-P0-LOG-08修复
    'LOG_FORMAT',  # R13-P1-LOG-02修复
    'LOG_DATE_FORMAT',  # R13-P1-LOG-02修复
    'StructuredLogEntry',  # R13-P1-LOG-04修复
    'config_service',
    'get_config',
    'get_config_service',  # AP-03
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
    'load_cascade_threshold_grid',
    'load_plr_thresholds',
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
    has_valid_rotating_handler = False
    stale_handlers = []
    for h in root_logger.handlers[:]:
        if isinstance(h, RotatingFileHandler):
            try:
                if h.stream and not h.stream.closed:
                    has_valid_rotating_handler = True
                else:
                    stale_handlers.append(h)
            except Exception:
                stale_handlers.append(h)
    for h in stale_handlers:
        try:
            root_logger.removeHandler(h)
            h.close()
        except Exception:
            pass
    if has_valid_rotating_handler:
        _LOG_INITIALIZED = True
        return False
    try:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            LOG_FORMAT,  # R13-P1-LOG-02修复: 使用集中常量
            datefmt=LOG_DATE_FORMAT  # R13-P1-LOG-02修复: 使用集中常量
        )
        has_platform_handler = any(
            getattr(h, '_is_platform_handler', False)
            for h in root_logger.handlers
        )
        if not has_platform_handler:
            try:
                from pythongo.infini import write_log
                class SimplePlatformHandler(logging.Handler):
                    _emit_error_count = 0
                    def emit(self, record):
                        msg = self.format(record)
                        try:
                            write_log(msg)
                        except Exception as _plh_err:
                            SimplePlatformHandler._emit_error_count += 1
                            if SimplePlatformHandler._emit_error_count <= 10 or SimplePlatformHandler._emit_error_count % 100 == 0:
                                logging.getLogger(__name__ + '.platform').warning(
                                    "[LG-05] Platform log write failed (count=%d): %s",
                                    SimplePlatformHandler._emit_error_count, _plh_err)
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
            file_handler.setLevel(logging.INFO)  # LG-P1-01修复: 生产环境默认INFO，避免DEBUG日志无限增长
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            # R13-P2-LOG-02修复: 设置日志文件权限为0o600，仅所有者可读写
            try:
                os.chmod(log_file, 0o600)
            except (OSError, AttributeError):
                pass  # Windows可能不支持完整chmod
            flush_handler = FlushHandler()
            flush_handler.setLevel(logging.DEBUG)
            root_logger.addHandler(flush_handler)
        _LOG_INITIALIZED = True
        logging.info('[config_service] Logging system initialized: platform + file')
        # LG-06修复: 日志初始化后执行健康检查
        try:
            _health = check_logging_health()
            if not _health.get('healthy', False):
                logging.warning('[LG-06] Logging health check failed after init: %s', _health)
        except Exception:
            pass
        # R15-P2-LOG修复: StructuredJsonlLogger集成主日志流
        try:
            from ali2026v3_trading.health_check_api import StructuredJsonlLogger
            _sjl = StructuredJsonlLogger(log_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs'))
            class _JsonlLogHandler(logging.Handler):
                def emit(self, record):
                    try:
                        _sjl._signal_log_f.write(self.format(record) + '\n')
                        _sjl._signal_log_f.flush()
                    except Exception as _jsonl_err:
                        if not hasattr(self, '_jsonl_err_count'):
                            self._jsonl_err_count = 0
                        self._jsonl_err_count += 1
                        if self._jsonl_err_count <= 10 or self._jsonl_err_count % 100 == 0:
                            logging.warning("[LG-P1-10] _JsonlLogHandler.emit异常(%d次): %s", self._jsonl_err_count, _jsonl_err)
            _jsonl_handler = _JsonlLogHandler()
            _jsonl_handler.setLevel(logging.WARNING)
            root_logger.addHandler(_jsonl_handler)
        except Exception as _sjl_err:
            logging.debug('[config_service] R15-P2-LOG: StructuredJsonlLogger集成跳过: %s', _sjl_err)
        # R13-P0-LOG-06修复: 初始化后尝试启用异步日志
        try:
            cfg = get_config()
            if cfg.logging.enable_async_logging:
                cfg.logging.setup_async_logging(root_logger)
        except Exception as _async_e:
            logging.debug('[config_service] R13-P0-LOG-06修复: 异步日志启用跳过: %s', _async_e)
        return True
    except Exception as e:
        try:
            logging.basicConfig(
                level=logging.INFO,
                format=LOG_FORMAT  # R13-P1-LOG-02修复: 使用集中常量
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
        # R14-P0-DEP-05修复: 加目录存在性验证，防止注入不存在的路径
        if path not in sys.path and os.path.isdir(path):
            sys.path.insert(0, path)
    _CACHED_PATHS = {'ali2026_dir': current_dir}
    _PATHS_CONFIGURED = True
    logging.info('[config_service] Paths configured: %s', _CACHED_PATHS)
    return _CACHED_PATHS


def get_paths():
    if not _PATHS_CONFIGURED:
        return setup_paths()
    return _CACHED_PATHS


# LG-06修复: 日志系统健康检查与崩溃恢复
_logging_health_state = {
    'last_check_time': 0.0,
    'handler_failures': 0,
    'total_checks': 0,
}

def check_logging_health() -> Dict[str, Any]:
    """LG-06修复: 日志系统健康检查——检测handler失效、文件不可写等"""
    now = time.time()
    _logging_health_state['total_checks'] += 1
    _logging_health_state['last_check_time'] = now
    root_logger = logging.getLogger()
    handlers = root_logger.handlers
    alive_handlers = 0
    dead_handlers = 0
    for h in handlers:
        try:
            if hasattr(h, 'stream') and h.stream is None:
                dead_handlers += 1
            elif hasattr(h, 'baseFilename') and not os.path.exists(h.baseFilename):
                dead_handlers += 1
            else:
                alive_handlers += 1
        except Exception:
            dead_handlers += 1
    _logging_health_state['handler_failures'] = dead_handlers
    is_healthy = dead_handlers == 0 and alive_handlers > 0
    if not is_healthy:
        logging.getLogger(__name__ + '.health').error(
            "[LG-06] Logging system unhealthy: alive=%d dead=%d total_checks=%d",
            alive_handlers, dead_handlers, _logging_health_state['total_checks'])
    return {
        'healthy': is_healthy,
        'alive_handlers': alive_handlers,
        'dead_handlers': dead_handlers,
        'total_checks': _logging_health_state['total_checks'],
    }


from ali2026v3_trading.service_container import ServiceContainer  # noqa: E402
