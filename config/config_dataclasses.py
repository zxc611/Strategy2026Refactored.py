# [M1-81] 配置数据类定义
# MODULE_ID: M1-008
"""
config_dataclasses.py �?配置数据类定�?
拆分�?config_service.py (P3大文件拆分方�?

包含:
- PathConfig: 路径配置
- TradingConfig: 交易参数配置
- UIConfig: UI显示配置常量
- OutputConfig: 输出配置
- LoggingConfig: 日志配置
- DatabaseConfig: 数据库配�?
- DataPathsConfig: 数据路径配置
- PerformanceConfig: 性能监控配置
- DebugConfig: 调试配置
- StructuredLogEntry: 结构化日志条�?
- ConfigField: 参数字段工厂
- LOG_FORMAT / LOG_DATE_FORMAT: 集中日志格式常量
"""
from __future__ import annotations

import os
import logging
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass, field
from ali2026v3_trading.infra.serialization_utils import json_dumps
from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ

LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# P1-53修复: strategy.duckdb路径统一为模块级常量，消�?处重�?
_DEFAULT_STRATEGY_DB_PATH = str(Path(__file__).parent.parent.parent / "data" / "strategy.duckdb")


@dataclass(slots=True)
class PathConfig:
    """路径配置"""
    project_root: str = str(Path(__file__).parent.parent.parent)
    db_path: str = _DEFAULT_STRATEGY_DB_PATH
    state_file: str = str(Path(__file__).parent.parent.parent / "data" / "strategy_state.json")
    log_dir: str = str(Path(__file__).parent / "logs")
    config_dir: str = str(Path(__file__).parent)

    def to_dict(self) -> Dict[str, Any]:
        from dataclasses import asdict
        return asdict(self)


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


class UIConfig:
    """UI显示配置常量 - 不影响策略逻辑，不作为策略参数暴露"""
    WINDOW_WIDTH: int = 260
    WINDOW_HEIGHT: int = 240
    FONT_LARGE: int = 11
    FONT_SMALL: int = 10
    TOP3_ROWS: int = 3


@dataclass(slots=True)
class LoggingConfig:
    """日志配置"""
    log_level: str = "INFO"
    log_format: str = LOG_FORMAT
    log_date_format: str = LOG_DATE_FORMAT
    log_to_console: bool = True
    log_to_file: bool = True
    log_file_prefix: str = "strategy"
    log_rotation_days: int = 1
    log_backup_count: int = 30
    enable_async_logging: bool = False
    log_queue_size: int = 1000

    _async_queue_handler: Any = None
    _async_queue_listener: Any = None

    def setup_async_logging(self, root_logger: logging.Logger) -> bool:
        """R13-P0-LOG-06修复: 设置QueueHandler异步日志，使用单独线程处理文件写�?""
        if not self.enable_async_logging:
            return False

        try:
            import queue
            from logging.handlers import QueueHandler, QueueListener

            log_queue = queue.Queue(maxsize=self.log_queue_size)

            existing_handlers = [
                h for h in root_logger.handlers
                if not isinstance(h, QueueHandler)
            ]

            if not existing_handlers:
                return False

            listener = QueueListener(
                log_queue, *existing_handlers,
                respect_handler_level=True
            )
            listener.start()

            for h in existing_handlers:
                root_logger.removeHandler(h)

            queue_handler = QueueHandler(log_queue)
            root_logger.addHandler(queue_handler)

            self._async_queue_handler = queue_handler
            self._async_queue_listener = listener

            logging.info('[LoggingConfig] R13-P0-LOG-06修复: 异步日志已启�? queue_size=%d', self.log_queue_size)
            return True
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning('[LoggingConfig] R13-P0-LOG-06修复: 异步日志设置失败: %s', e)
            return False

    def stop_async_logging(self) -> None:
        """R13-P0-LOG-06修复: 停止异步日志监听�?""
        if self._async_queue_listener is not None:
            try:
                self._async_queue_listener.stop()
                self._async_queue_listener = None
                self._async_queue_handler = None
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning('[LoggingConfig] R13-P0-LOG-06修复: 停止异步日志失败: %s', e)


@dataclass(slots=True)
class DatabaseConfig:
    """数据库配�?""
    db_type: str = "duckdb"
    db_path: str = _DEFAULT_STRATEGY_DB_PATH
    connection_pool_size: int = 5
    connection_timeout: float = 30.0
    enable_connection_pooling: bool = True
    auto_commit: bool = False

    def __post_init__(self):
        pass


@dataclass(slots=True)
class DataPathsConfig:
    """数据路径配置"""
    duckdb_file: str = ""
    parquet_path: str = ""
    flush_windows_str: str = "09:00-11:30,13:00-15:15"

    def __post_init__(self):
        if not self.duckdb_file:
            env_val = os.environ.get('DUCKDB_FILE', '').strip()
            self.duckdb_file = env_val if env_val else _DEFAULT_STRATEGY_DB_PATH
        if not self.parquet_path:
            env_val = os.environ.get('TICK_DATA_PATH', '').strip()
            self.parquet_path = env_val if env_val else str(Path(__file__).parent.parent.parent / "data" / "ticks")
        env_flush = os.environ.get('INTRADAY_FLUSH_WINDOWS', '').strip()
        if env_flush:
            self.flush_windows_str = env_flush

    def to_dict(self) -> Dict[str, Any]:
        from dataclasses import asdict
        return asdict(self)


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


@dataclass(slots=True)
class StructuredLogEntry:
    """结构化日志条�?�?定义标准字段，确保日志聚合与监控一�?""
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


__all__ = [
    'PathConfig', 'TradingConfig', 'UIConfig', 'OutputConfig',
    'LoggingConfig', 'DatabaseConfig', 'DataPathsConfig', 'PerformanceConfig',
    'StructuredLogEntry', 'ConfigField', 'DebugConfig',
    'LOG_FORMAT', 'LOG_DATE_FORMAT', '_CHINA_TZ',
]

# ============================================================================
# 参数数据类（�?_params_dataclasses.py�?
# ============================================================================
class OutputConfig:
    """输出配置 �?增加交易输出控制"""
    def __init__(self, mode: str = "debug", trade_quiet: bool = True,
                 diagnostic_enabled: bool = True, debug_enabled: bool = False):
        self.mode = mode
        self.diagnostic_output = True
        self.debug_output = True
        self._diagnostic_throttle_sec = 0.0
        self._debug_throttle_sec = 0.0
        if mode == "debug":
            mode = "close_debug"
        self.mode = mode
        self.trade_quiet = trade_quiet
        self.diagnostic_enabled = diagnostic_enabled
        self.debug_enabled = debug_enabled
        self.is_trade_like = mode in ("trade", "open_debug")
        self.combined_debug_enabled = (
            self.diagnostic_enabled and
            (self.debug_enabled or mode == "close_debug")
        )

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

    def should_output_trade(self, is_trade: bool = False, is_force: bool = False,
                            is_trade_table: bool = False, is_diag: bool = False) -> bool:
        if self.is_trade_like and self.trade_quiet and not is_trade_table:
            return False
        if is_diag and not self.diagnostic_enabled and not is_trade and not is_force:
            return False
        if self.is_trade_like and not is_trade and not is_force:
            return False
        if not is_trade and not is_force and not self.combined_debug_enabled:
            return False
        return True


# ============================================================================
# 观察者接�?
# ============================================================================

class ParamObserver:
    # TODO(P1-11): plan to unify to EventBus
    """参数观察者接�?""

    def on_param_changed(self, key: str, old_value: Any, new_value: Any) -> None:
        """参数变更回调"""
        pass


class ParamAuditObserver(ParamObserver):
    """参数修改审计观察�?—�?金融合规红线

    记录结构化审计日志，包含：谁/何时/改了什�?旧值→新�?调用栈摘要�?
    审计日志写入专用 logger (params_audit)，可与普通日志分离归档�?
    """

    _SAFETY_CRITICAL_KEYS = frozenset({
        'close_take_profit_ratio', 'close_stop_loss_ratio',
        'daily_loss_hard_stop_pct', 'circuit_breaker_trigger_sigma',
        'max_risk_ratio',
        'hft_hard_time_stop_ms', 'spring_hard_time_stop_sec',
        'resonance_hard_time_stop_min', 'box_hard_time_stop_min',
        'spring_stop_profit_ratio', 'spring_max_loss_pct',
    })

    def __init__(self, max_stack_depth: int = 3):
        self._audit_logger = logging.getLogger('params_audit')
        if not self._audit_logger.handlers:
            # R24-P2-AT-04修复: 添加FileHandler确保持久化，保留StreamHandler用于控制台输�?
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                '%(asctime)s.%(msecs)03d | AUDIT | %(message)s'  # [R22-P2-TIME03]
            ))
            self._audit_logger.addHandler(handler)
            # 添加文件持久�?
            try:
                _audit_log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
                os.makedirs(_audit_log_dir, exist_ok=True)
                _audit_log_path = os.path.join(_audit_log_dir, 'params_audit.log')
                _file_handler = logging.FileHandler(_audit_log_path, encoding='utf-8')
                _file_handler.setFormatter(logging.Formatter(
                    '%(asctime)s.%(msecs)03d | AUDIT | %(message)s'
                ))
                self._audit_logger.addHandler(_file_handler)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as _e:
                logging.warning("[R24-P2-AT-04] params_audit FileHandler创建失败: %s，仅使用StreamHandler", _e)
            self._audit_logger.setLevel(logging.INFO)
        self._max_stack_depth = max_stack_depth

    def on_param_changed(self, key: str, old_value: Any, new_value: Any) -> None:
        import traceback
        stack_summary = ' -> '.join(
            f"{frame.filename}:{frame.lineno}({frame.function})"
            for frame in traceback.extract_stack()[-self._max_stack_depth - 1:-1]
        )
        risk_flag = ' ⚠️ SAFETY_CRITICAL' if key in self._SAFETY_CRITICAL_KEYS else ''
        self._audit_logger.info(
            "param_changed | key=%s | old=%r | new=%r | stack=[%s]%s",
            key, old_value, new_value, stack_summary, risk_flag,
        )
        if key in self._SAFETY_CRITICAL_KEYS:
            self._audit_logger.warning(
                "SAFETY_PARAM_MODIFIED | key=%s | old=%r | new=%r",
                key, old_value, new_value,
            )

