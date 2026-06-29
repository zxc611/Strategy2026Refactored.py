# [M1-70] 性能监控
"""
performance_monitor.py - 性能监控工具模块

提供路径计数器、函数调用统计等性能监控功能。
从 strategy_core_service.py 迁移而来，实现关注点分离。

职责：
- 函数/方法调用次数统计
- 执行时间监控
- 错误率跟踪
- 性能数据导出

设计原则：
- 单例模式确保全局唯一
- 线程安全
- 低开销（可禁用）
"""
from __future__ import annotations

import time
import os
import threading
import logging
import json
from ali2026v3_trading.infra.serialization_utils import json_dumps
from datetime import datetime
from typing import Any, Dict, Optional, Callable
from functools import wraps

_path_counter_lock = threading.Lock()


class PathCounter:
    """路径计数器 - 记录函数/方法调用次数和执行时间
    
    使用示例：
        from ali2026v3_trading.infra.performance_monitor import PathCounter, count_call
        
        @count_call()
        def my_function():
            pass
        
        # 手动记录
        PathCounter.record_call("custom.path", execution_time=0.5)
        
        # 导出报告
        PathCounter.export_to_json("performance_report.json")
        PathCounter.print_summary()
    """

    # R13-P2-LOG-12修复: 可配置的阈值告警
    _ALERT_THRESHOLDS: Dict[str, Dict[str, float]] = {
        'avg_time_ms': 5000.0,     # 单次调用平均耗时超过5秒告警
        'error_rate': 0.1,         # 错误率超过10%告警
        'total_time_sec': 300.0,   # 累计耗时超过5分钟告警
    }

    _persist_filepath: Optional[str] = None
    _persist_thread: Optional[threading.Thread] = None
    _persist_stop_event = threading.Event()
    _PERSIST_INTERVAL_SEC = 300.0

    def __new__(cls):
        from ali2026v3_trading.infra.registry_service import SingletonRegistry
        _registry = SingletonRegistry.get_registry('performance_monitor')
        _inst = _registry.get('instance')
        if _inst is None:
            with _path_counter_lock:
                _inst = _registry.get('instance')
                if _inst is None:
                    _inst = super().__new__(cls)
                    _inst._initialized = False
                    _registry.set('instance', _inst)
        return _inst
    
    def __init__(self):
        if self._initialized:
            return
        
        self._counters: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._enabled = True
        self._start_time = time.time()
        self._initialized = True
    
    @classmethod
    def enable(cls) -> None:
        """启用性能监控"""
        counter = cls()
        counter._enabled = True
        logging.info("[PathCounter] Performance monitoring enabled")
    
    @classmethod
    def disable(cls) -> None:
        """禁用性能监控（零开销）"""
        counter = cls()
        counter._enabled = False
        logging.info("[PathCounter] Performance monitoring disabled")
    
    @classmethod
    def is_enabled(cls) -> bool:
        """检查是否启用"""
        counter = cls()
        return counter._enabled

    @classmethod
    def start_auto_persist(cls, filepath: str = "performance_report.json", interval_sec: float = 300.0) -> None:
        """RES-P1-17修复: 启动自动定期持久化后台线程"""
        cls._PERSIST_INTERVAL_SEC = interval_sec
        cls._persist_filepath = filepath
        if cls._persist_thread is not None and cls._persist_thread.is_alive():
            return
        cls._persist_stop_event.clear()
        def _persist_loop():
            while not cls._persist_stop_event.wait(timeout=cls._PERSIST_INTERVAL_SEC):
                try:
                    cls.get_stats(output_format='json', filepath=cls._persist_filepath)
                except Exception as e:
                    logging.error("[PathCounter] Auto-persist failed: %s", e)
        cls._persist_thread = threading.Thread(target=_persist_loop, daemon=True, name="PathCounterAutoPersist")
        cls._persist_thread.start()
        logging.info("[RES-P1-17] 性能指标自动持久化已启动: interval=%.0fs filepath=%s", interval_sec, filepath)

    @classmethod
    def stop_auto_persist(cls) -> None:
        """RES-P1-17修复: 停止自动持久化并执行最后一次保存"""
        cls._persist_stop_event.set()
        if cls._persist_thread is not None:
            cls._persist_thread.join(timeout=5.0)
        if cls._persist_filepath:
            try:
                cls.get_stats(output_format='json', filepath=cls._persist_filepath)
            except Exception:
                pass
    
    @classmethod
    def record_call(cls, path: str, execution_time: float = 0.0, exception: Optional[Exception] = None) -> None:
        """记录一次函数调用
        
        Args:
            path: 调用路径（如 module.function 或 Class.method）
            execution_time: 执行时间（秒）'
            exception: 异常信息（如果有）
        """
        counter = cls()
        if not counter._enabled:
            return
        
        with counter._lock:
            if path not in counter._counters:
                counter._counters[path] = {
                    'count': 0,
                    'total_time': 0.0,
                    'errors': 0,
                    'last_error': None
                }
            
            data = counter._counters[path]
            data['count'] += 1
            data['total_time'] += execution_time
            
            if exception is not None:
                data['errors'] += 1
                data['last_error'] = str(exception)

            # R13-P2-LOG-12修复: 阈值告警检查
            thresholds = cls._ALERT_THRESHOLDS
            count = data['count']
            total_time = data['total_time']
            errors = data['errors']
            avg_time_ms = (total_time / count * 1000) if count > 0 else 0
            error_rate = errors / count if count > 0 else 0
            alerts = []
            if avg_time_ms > thresholds['avg_time_ms']:
                alerts.append("avg_time=%.1fms>%.1fms" % (avg_time_ms, thresholds['avg_time_ms']))
            if error_rate > thresholds['error_rate']:
                alerts.append("error_rate=%.1f%%>%.1f%%" % (error_rate * 100, thresholds['error_rate'] * 100))
            if total_time > thresholds['total_time_sec']:
                alerts.append("total_time=%.1fs>%.1fs" % (total_time, thresholds['total_time_sec']))
            if alerts:
                logging.warning("[PerformanceMonitor] %s: %s", path, "; ".join(alerts))
    
    @classmethod
    def get_stats(cls, output_format: str = 'dict', **kwargs) -> Any:
        """获取性能统计数据（统一接口）'
        Args:
            output_format: 输出格式 ('dict', 'json', 'console')
                   - 'dict': 返回字典（默认）'
                   - 'json': 导出到JSON文件（需指定filepath参数）'
                   - 'console': 打印到控制台
        
        Returns:
            dict: 性能统计数据（当output_format='dict'时）'
        Usage:
            # 获取字典数据
            stats = PathCounter.get_stats()
            
            # 导出到JSON
            PathCounter.get_stats(output_format='json', filepath='report.json')
            
            # 打印到控制台
            PathCounter.get_stats(output_format='console', top_n=20)
        """
        counter = cls()
        with counter._lock:
            # R23-P2-12修复: 添加时区参数
            from datetime import timezone
            result = {
                'service_name': 'PerformanceMonitor',
                'start_time': datetime.fromtimestamp(counter._start_time, tz=timezone.utc).isoformat(),
                'uptime_seconds': time.time() - counter._start_time,
                'total_paths': len(counter._counters),
                'counters': {}
            }
            
            for path, stats in counter._counters.items():
                avg_time = (stats['total_time'] / stats['count']) if stats['count'] > 0 else 0
                result['counters'][path] = {
                    'count': stats['count'],
                    'total_time': round(stats['total_time'], 6),
                    'avg_time': round(avg_time * 1000, 3),  # 毫秒
                    'errors': stats['errors'],
                    'last_error': stats['last_error']
                }
            
            # 根据格式输出
            if output_format == 'json':
                import sys
                filepath = kwargs.get('filepath', 'performance_report.json')
                try:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(json_dumps(result, indent=2))
                    logging.info(f"[PathCounter] Performance report exported to {filepath}")
                except Exception as e:
                    logging.error(f"[PathCounter] Failed to export report: {e}")
                return None
            elif output_format == 'console':
                top_n = kwargs.get('top_n', 20)
                print(f"\n{'='*80}")
                print(f"Path Counter Summary (Uptime: {result['uptime_seconds']:.1f}s)")
                print(f"{'='*80}")
                
                sorted_paths = sorted(
                    result['counters'].items(),
                    key=lambda x: x[1]['count'],
                    reverse=True
                )[:top_n]
                
                if not sorted_paths:
                    print("No calls recorded yet.")
                else:
                    for path, stats in sorted_paths:
                        error_flag = f" ({stats['errors']} errors)" if stats['errors'] > 0 else ""
                        print(f"{stats['count']:6d} calls | {stats['avg_time']:8.3f}ms avg | {path}{error_flag}")
                
                print(f"{'='*80}\n")
                return None
            else:
                return result
    
    @classmethod
    def reset(cls) -> None:
        from ali2026v3_trading.infra.registry_service import SingletonRegistry
        _registry = SingletonRegistry.get_registry('performance_monitor')
        _registry.remove('instance')
        counter = cls()
        with counter._lock:
            counter._counters.clear()
            counter._start_time = time.time()
            logging.info("[PathCounter] All counters reset")


def count_call(path: Optional[str] = None):
    """统一装饰器：记录函数/方法调用
    
    Args:
        path: 自定义路径名称
              - 对于函数：默认为 module.function
              - 对于方法：默认为 ClassName.method_name
    
    Usage:
        # 函数
        @count_call()
        def my_function():
            pass
        
        # 方法
        class MyClass:
            @count_call()
            def my_method(self):
                pass
        
        # 自定义路径
        @count_call("custom.name")
        def another_function():
            pass
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not PathCounter.is_enabled():
                return func(*args, **kwargs)
            
            call_path = path
            if call_path is None:
                is_bound_method = False
                is_classmethod = False
                
                if args:
                    first_arg = args[0]
                    if hasattr(first_arg, '__class__'):
                        arg_class = first_arg.__class__
                        if isinstance(first_arg, type):
                            is_classmethod = True
                            call_path = f"{first_arg.__name__}.{func.__name__}"
                        elif arg_class.__module__ != 'builtins' and not isinstance(first_arg, (int, float, str, list, dict, tuple, set, bytes)):
                            is_bound_method = True
                            call_path = f"{arg_class.__name__}.{func.__name__}"
                
                if not is_bound_method and not is_classmethod:
                    call_path = f"{func.__module__}.{func.__name__}"
            
            start_time = time.time()
            exception = None
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                exception = e
                raise
            finally:
                execution_time = time.time() - start_time
                PathCounter.record_call(call_path, execution_time, exception)
        
        return wrapper
    return decorator



__all__ = ['PathCounter', 'count_call', 'ChaosFaultInjector']


# ============================================================================
# R15-P0-RES-09修复: 混沌工程/故障注入基础设施
# ============================================================================
class ChaosFaultInjector:
    """R15-P0-RES-09修复: 轻量故障注入框架，用于验证容错措施是否真正生效

    用法:
        injector = ChaosFaultInjector()
        injector.register('duckdb_connection', lambda: (_ for _ in ()).throw(Exception("simulated db failure")))
        injector.inject('duckdb_connection')  # 模拟DuckDB连接失败
    """

    _FAULT_TYPES = frozenset({
        'duckdb_connection', 'duckdb_write', 'duckdb_read',
        'platform_api_timeout', 'platform_api_error',
        'disk_full', 'memory_pressure',
        'network_timeout', 'network_disconnect',
        'thread_pool_reject', 'event_bus_overload',
    })

    def __init__(self):
        self._fault_handlers: Dict[str, callable] = {}
        self._injected_faults: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._enabled = os.environ.get("CHAOS_FAULT_INJECTION", "false").lower() in ("true", "1", "yes")

    def is_enabled(self) -> bool:
        return self._enabled

    def register(self, fault_type: str, handler: callable) -> None:
        if fault_type not in self._FAULT_TYPES:
            raise ValueError(f"Unknown fault type: {fault_type}, allowed: {self._FAULT_TYPES}")
        self._fault_handlers[fault_type] = handler

    def inject(self, fault_type: str, **kwargs) -> Any:
        """注入故障，返回handler结果或抛出异常"""
        if not self._enabled:
            return None
        handler = self._fault_handlers.get(fault_type)
        if handler is None:
            logging.warning("[ChaosFaultInjector] No handler for fault_type=%s", fault_type)
            return None
        with self._lock:
            self._injected_faults[fault_type] = {
                'timestamp': time.time(),
                'kwargs': kwargs,
            }
        logging.warning("[ChaosFaultInjector] Injecting fault: %s kwargs=%s", fault_type, kwargs)
        return handler()

    def get_injected_faults(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._injected_faults)

    def clear(self) -> None:
        with self._lock:
            self._injected_faults.clear()


# 全局单例
_chaos_injector: Optional[ChaosFaultInjector] = None
_chaos_injector_lock = threading.Lock()


def get_chaos_injector() -> ChaosFaultInjector:
    global _chaos_injector
    with _chaos_injector_lock:
        if _chaos_injector is None:
            _chaos_injector = ChaosFaultInjector()
        return _chaos_injector
