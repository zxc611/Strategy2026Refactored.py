# [M1-80] 配置服务Facade
"""
配置服务模块 - CQRS 架构 Configuration 层
来源：分散的配置文件（quant_trading_system_T型图/config.py + 06_params.py 配置类）
功能：统一配置管理、路径配置、交易参数、输出配置

重构 v2.0 (2026-05-07):
- ✅ ExchangeConfig/品种映射/合约解析 → config_exchange.py
- ✅ 参数表/缓存/校验 → config_params.py
- ✅ 本模块保留：路径/交易/输出/日志配置 + 全局单例 + 便捷函数
- ✅ 向后兼容：所有 from config.config_service import xxx 不变

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

from datetime import timezone, timedelta

from infra.serialization_utils import json_dumps, json_loads, json_default_serializer, safe_pickle_load, safe_pickle_dump, yaml_safe_load, yaml_safe_dump

from infra.shared_utils import normalize_year_month, CHINA_TZ as _CHINA_TZ  # 五唯一性修复：统一从 shared_utils 导入 CHINA_TZ
from infra.resilience import is_disk_full_error

# P2修复: config_service版本号标记（增加版本号覆盖率）
_CONFIG_SERVICE_MODULE_VERSION = "2.0.0"  # P2修复: 配置服务模块版本
_PATH_CONFIG_VERSION = "1.0"              # P2修复: 路径配置版本
_TRADING_CONFIG_VERSION = "1.0"           # P2修复: 交易配置版本
_LOGGING_CONFIG_VERSION = "1.0"           # P2修复: 日志配置版本
_DATABASE_CONFIG_VERSION = "1.0"          # P2修复: 数据库配置版本
_PERFORMANCE_CONFIG_VERSION = "1.0"       # P2修复: 性能配置版本
try:
    from infra.security import (
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


from config.config_exchange import (
    ExchangeConfig,
    _get_option_underlying_product,
    ensure_products_with_retry,
    build_exchange_mapping,
    resolve_product_exchange,
    make_platform_future_id,
)

from config.config_params import (
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
    get_param,
)

# P2-06修复: 统一使用 config_dataclasses 的规范定义(3级parent), 删除本文件中的重复定义
from config.config_dataclasses import (  # noqa: F401
    PathConfig,
    DatabaseConfig,
    DataPathsConfig,
    TradingConfig,
    UIConfig,
    OutputConfig,
    LoggingConfig,
    PerformanceConfig,
    StructuredLogEntry,
    DebugConfig,
    ConfigField,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
)

from config.config_logging import (  # noqa: F401
    FlushHandler,
    AuditLogger,
    StructuredJsonFormatter,
    setup_logging,
    setup_paths,
    get_paths,
    check_logging_health,
)

from config.config_loader import (  # noqa: F401
    load_cascade_threshold_grid,
    load_plr_thresholds,
)

from governance.param_table_provider import get_param_table_provider as _get_param_table_provider
_param_provider = _get_param_table_provider()

def get_param_provider():
    """P2-2-FIX: 返回全局。param_provider单例"""
    return _param_provider


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
            from config.config_params import validate_ui_params
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

        检查配置中的format_version字段，按序执行迁移链。'
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

        用于多节点/多进程协调，防止并发配置更新。'
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
                # 检查锁是否过期（超过60秒视为过期）'
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
        通过EventBus发布配置变更事件，通知其他节点刷新本地缓存。'
        Args:
            updated_keys: 更新的配置键列表
        """
        try:
            from infra.event_bus import EventBus
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

        当收到其他节点的配置更新通知时，自动刷新本地配置缓存。'
        """
        try:
            from infra.event_bus import EventBus
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
        - snapshot_name: 快照名称（可选，默认使用时间戳）'
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
        - snapshot_id: 快照ID（可选，默认回滚到最近一次快照）'
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
        - 快照信息字典（包含id、timestamp、config大小）'
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
        """SER-P1-07修复: 使用safe_pickle_load安全加载checkpoint文件。'
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

        用于多环境(开发/测试/生产)配置一致性检查。'
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
        防止版本漂移导致操作错误或配置不兼容。'
        Args:
            code_version: 代码版本号（默认使用shared_utils._CODE_VERSION）
            manual_version: 手册版本号（默认从环境变量MANUAL_VERSION读取）'
            config_version: 配置版本号（默认使用例CONFIG_FORMAT_VERSION）

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
            from infra.shared_utils import _CODE_VERSION
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
                    from infra.registry_service import SingletonRegistry
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
    'get_param',
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



from infra.service_container import ServiceContainer  # noqa: E402

ConfigQueryService = ConfigService
ConfigServiceQueryMixin = ConfigService
