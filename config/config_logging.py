# [M1-84] 日志配置与审计
# MODULE_ID: M1-011
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""
config_logging.py �?日志配置与审�?
拆分�?config_service.py (P3大文件拆分方�?

包含:
- FlushHandler: 自定义日志Handler
- AuditLogger: 防篡改审计日�?SHA256哈希�?
- StructuredJsonFormatter: 结构化JSON日志格式化器
- setup_logging: 统一日志初始�?
- setup_paths: 路径配置
- get_paths: 获取路径
- check_logging_health: 日志健康检�?
"""
from __future__ import annotations

import os
import sys
import time
import logging
import threading
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Tuple

try:
    from concurrent_log_handler import ConcurrentRotatingFileHandler
    _HAS_CONCURRENT_HANDLER = True
except ImportError:
    _HAS_CONCURRENT_HANDLER = False

from ali2026v3_trading.infra.serialization_utils import json_dumps
from ali2026v3_trading.infra.resilience import is_disk_full_error
from ali2026v3_trading.config.config_dataclasses import LOG_FORMAT, LOG_DATE_FORMAT


# [P2-05] 权威日志配置入口，与infra/shared_utils.py的日志工具分�?
class FlushHandler(logging.Handler):
    """自定�?Handler，确保日志定期flush到磁�?
    LG-01修复: 移除os.fsync()同步阻塞，改为异步flush+周期性fsync�?
    避免tick处理路径IO延迟导致风控/下单延迟"""
    _last_fsync_time = 0.0
    _fsync_interval = 5.0

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
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as _flush_err:
            logging.warning("[R32-P2-11] FlushHandler flush/fsync失败: %s", _flush_err)
            if is_disk_full_error(_flush_err):
                logging.critical("[R33-P1-16] 日志flush失败: 磁盘�?ENOSPC)! 日志可能丢失")


class AuditLogger:
    """R13-P0-LOG-08修复: 防篡改审计日志，每条日志附带前一条的SHA256哈希

    计划统一到infra审计工具：本类为日志审计专用，职责边�?防篡改哈希链日志。与risk/safety_meta_audit(风控审计)、param_pool/l1_quantification/meta_audit_engine(参数审计)分工�?""

    def __init__(self, log_path: str):
        self._log_path = log_path
        self._prev_hash: str = '0' * 64
        self._lock = threading.Lock()
        try:
            if os.path.exists(log_path):
                os.chmod(log_path, 0o600)
        except (ValueError, KeyError, TypeError, AttributeError, IOError) as _r3_err:
            pass
        self._recover_hash()

    def _recover_hash(self) -> None:
        """从已有日志文件恢复最后一条的哈希�?""
        try:
            if not os.path.exists(self._log_path):
                return
            with open(self._log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            if lines:
                last_line = lines[-1].strip()
                if last_line:
                    parts = last_line.split(' | ')
                    if len(parts) >= 5:
                        self._prev_hash = parts[-1].strip()
        except (ValueError, KeyError, TypeError, AttributeError, IOError) as _r3_err:
            self._prev_hash = '0' * 64

    def log(self, event_type: str, detail: dict) -> str:
        """写入一条审计日�?""
        from ali2026v3_trading.infra.shared_utils import compute_content_hash
        from datetime import datetime as _dt
        from ali2026v3_trading.config.config_dataclasses import _CHINA_TZ

        timestamp = _dt.now(_CHINA_TZ).isoformat()
        detail_json = json_dumps(detail, sort_keys=True)

        raw = f"{self._prev_hash}{timestamp}{event_type}{detail_json}"
        current_hash = compute_content_hash(raw, algorithm='sha256')  # R5-3

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
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
                logging.error('[AuditLogger] R13-P0-LOG-08修复: 写入审计日志失败: %s', e)

            self._prev_hash = current_hash

        # P1-07修复: 委托到structured_audit_log统一审计入口，消除独立审计记�?
        self._delegate_to_structured_audit_log(event_type, detail)

        return current_hash

    def _delegate_to_structured_audit_log(self, event_type: str, detail: dict) -> None:
        """P1-07修复: 委托到risk/_utils.structured_audit_log统一审计入口�?""
        try:
            from ali2026v3_trading.risk._utils import structured_audit_log as _sal
            _sal(event_type, 'AuditLogger.delegated', details=detail)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            logging.debug("[P1-07] AuditLogger委托structured_audit_log失败(非致�?: %s", e)

    @staticmethod
    def verify_chain(log_path: str) -> Tuple[bool, int]:
        """验证审计日志链的完整�?""
        from ali2026v3_trading.infra.shared_utils import compute_content_hash

        if not os.path.exists(log_path):
            return True, 0

        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except (ValueError, KeyError, TypeError, AttributeError, IOError) as _r3_err:
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

            if stored_prev_hash != prev_hash:
                return False, count

            raw = f"{stored_prev_hash}{timestamp}{event_type}{detail_json}"
            expected_hash = compute_content_hash(raw, algorithm='sha256')  # R5-3
            if stored_current_hash != expected_hash:
                return False, count

            prev_hash = stored_current_hash
            count += 1

        return True, count


class StructuredJsonFormatter(logging.Formatter):
    """结构化JSON日志格式化器 �?替代纯文本格式，便于日志聚合系统消费"""
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


_LOG_INITIALIZED = False
_PATHS_CONFIGURED = False
_CACHED_PATHS = None
_logging_health_state = {
    'last_check_time': 0.0,
    'handler_failures': 0,
    'total_checks': 0,
}


def setup_logging():
    """统一日志初始�?""
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
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                stale_handlers.append(h)
    for h in stale_handlers:
        try:
            root_logger.removeHandler(h)
            h.close()
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            pass
    if has_valid_rotating_handler:
        _LOG_INITIALIZED = True
        return False
    try:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
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
                        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _plh_err:
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
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            try:
                os.chmod(log_file, 0o600)
            except (OSError, AttributeError):
                pass
            flush_handler = FlushHandler()
            flush_handler.setLevel(logging.DEBUG)
            root_logger.addHandler(flush_handler)
        _LOG_INITIALIZED = True
        logging.info('[config_service] Logging system initialized: platform + file')
        try:
            _health = check_logging_health()
            if not _health.get('healthy', False):
                logging.warning('[LG-06] Logging health check failed after init: %s', _health)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            pass
        try:
            from ali2026v3_trading.infra.health_monitor import StructuredJsonlLogger
            _sjl = StructuredJsonlLogger(log_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs'))
            class _JsonlLogHandler(logging.Handler):
                def emit(self, record):
                    try:
                        _sjl._signal_log_f.write(self.format(record) + '\n')
                        _sjl._signal_log_f.flush()
                    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as _jsonl_err:
                        if not hasattr(self, '_jsonl_err_count'):
                            self._jsonl_err_count = 0
                        self._jsonl_err_count += 1
                        if self._jsonl_err_count <= 10 or self._jsonl_err_count % 100 == 0:
                            logging.warning("[LG-P1-10] _JsonlLogHandler.emit异常(%d�?: %s", self._jsonl_err_count, _jsonl_err)
            _jsonl_handler = _JsonlLogHandler()
            _jsonl_handler.setLevel(logging.WARNING)
            root_logger.addHandler(_jsonl_handler)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _sjl_err:
            logging.debug('[config_service] R15-P2-LOG: StructuredJsonlLogger集成跳过: %s', _sjl_err)
        try:
            from ali2026v3_trading.config.config_service import get_config
            cfg = get_config()
            if cfg.logging.enable_async_logging:
                cfg.logging.setup_async_logging(root_logger)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _async_e:
            logging.debug('[config_service] R13-P0-LOG-06修复: 异步日志启用跳过: %s', _async_e)
        return True
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        try:
            logging.basicConfig(
                level=logging.INFO,
                format=LOG_FORMAT
            )
            _LOG_INITIALIZED = True
            logging.warning('[config_service] Using fallback logging configuration')
            return True
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            return False


def setup_paths():
    """路径配置"""
    global _PATHS_CONFIGURED, _CACHED_PATHS
    if _PATHS_CONFIGURED:
        return _CACHED_PATHS
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)
    paths_to_add = [current_dir]
    for path in paths_to_add:
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


def check_logging_health() -> Dict[str, Any]:
    """LG-06修复: 日志系统健康检查——检测handler失效、文件不可写�?""
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
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
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


__all__ = [
    'FlushHandler', 'AuditLogger', 'StructuredJsonFormatter',
    'setup_logging', 'setup_paths', 'get_paths', 'check_logging_health',
    '_LOG_INITIALIZED', '_PATHS_CONFIGURED', '_CACHED_PATHS',
    '_logging_health_state',
]