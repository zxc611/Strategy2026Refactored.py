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
import threading
import logging
import json
from datetime import datetime
from typing import Any, Dict, Optional, Callable
from functools import wraps

_path_counter_lock = threading.Lock()


class PathCounter:
    """路径计数器 - 记录函数/方法调用次数和执行时间
    
    使用示例：
        from ali2026v3_trading.performance_monitor import PathCounter, count_call
        
        @count_call()
        def my_function():
            pass
        
        # 手动记录
        PathCounter.record_call("custom.path", execution_time=0.5)
        
        # 导出报告
        PathCounter.export_to_json("performance_report.json")
        PathCounter.print_summary()
    """
    
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            with _path_counter_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
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
    def record_call(cls, path: str, execution_time: float = 0.0, exception: Optional[Exception] = None) -> None:
        """记录一次函数调用
        
        Args:
            path: 调用路径（如 module.function 或 Class.method）
            execution_time: 执行时间（秒）
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
    
    @classmethod
    def get_stats(cls, format: str = 'dict', **kwargs) -> Any:
        """获取性能统计数据（统一接口）
        
        Args:
            format: 输出格式 ('dict', 'json', 'console')
                   - 'dict': 返回字典（默认）
                   - 'json': 导出到JSON文件（需指定filepath参数）
                   - 'console': 打印到控制台
        
        Returns:
            dict: 性能统计数据（当format='dict'时）
        
        Usage:
            # 获取字典数据
            stats = PathCounter.get_stats()
            
            # 导出到JSON
            PathCounter.get_stats(format='json', filepath='report.json')
            
            # 打印到控制台
            PathCounter.get_stats(format='console', top_n=20)
        """
        counter = cls()
        with counter._lock:
            result = {
                'service_name': 'PerformanceMonitor',  # ✅ ID唯一：统一标识服务来源
                'start_time': datetime.fromtimestamp(counter._start_time).isoformat(),
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
            if format == 'json':
                import sys
                filepath = kwargs.get('filepath', 'performance_report.json')
                try:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(result, f, indent=2, ensure_ascii=False)
                    logging.info(f"[PathCounter] Performance report exported to {filepath}")
                except Exception as e:
                    logging.error(f"[PathCounter] Failed to export report: {e}")
                return None
            elif format == 'console':
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
        """重置所有计数器"""
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
            
            # 自动检测方法调用（第一个参数是 self）
            if args and hasattr(args[0], '__class__'):
                call_path = path or f"{type(args[0]).__name__}.{func.__name__}"
            else:
                call_path = path or f"{func.__module__}.{func.__name__}"
            
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



__all__ = ['PathCounter', 'count_call']
