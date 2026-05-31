"""
R16-P0-RES-09修复: 混沌工程/故障注入框架
验证容错措施在故障条件下是否真正生效
"""
import threading
import time
import logging
import random
from typing import Callable, Dict, List, Optional, Any
from contextlib import contextmanager


class FaultInjector:
    """故障注入器 — 在受控条件下注入故障，验证容错措施"""

    def __init__(self):
        self._active_faults: Dict[str, Dict[str, Any]] = {}
        self._fault_history: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def inject_exception(self, fault_id: str, target_fn: Callable,
                         exception_type: type = RuntimeError,
                         exception_msg: str = 'chaos_injected_exception',
                         probability: float = 1.0) -> Callable:
        """为目标函数注入异常"""
        def wrapper(*args, **kwargs):
            if random.random() < probability:
                with self._lock:
                    self._active_faults[fault_id] = {
                        'type': 'exception',
                        'exception': exception_type.__name__,
                        'msg': exception_msg,
                        'timestamp': time.time(),
                    }
                    self._fault_history.append({
                        'fault_id': fault_id,
                        'action': 'exception_injected',
                        'timestamp': time.time(),
                    })
                raise exception_type(exception_msg)
            return target_fn(*args, **kwargs)
        return wrapper

    def inject_delay(self, fault_id: str, delay_seconds: float,
                     probability: float = 1.0) -> Callable:
        """返回一个延迟注入装饰器"""
        def decorator(fn):
            def wrapper(*args, **kwargs):
                if random.random() < probability:
                    with self._lock:
                        self._active_faults[fault_id] = {
                            'type': 'delay',
                            'delay_seconds': delay_seconds,
                            'timestamp': time.time(),
                        }
                        self._fault_history.append({
                            'fault_id': fault_id,
                            'action': 'delay_injected',
                            'delay': delay_seconds,
                            'timestamp': time.time(),
                        })
                    time.sleep(delay_seconds)
                return fn(*args, **kwargs)
            return wrapper
        return decorator

    @contextmanager
    def inject_network_partition(self, fault_id: str, duration_sec: float = 5.0):
        """网络分区注入上下文管理器"""
        with self._lock:
            self._active_faults[fault_id] = {
                'type': 'network_partition',
                'duration': duration_sec,
                'timestamp': time.time(),
            }
        logging.warning("[ChaosEngineering] 网络分区注入: %s, 持续%.1fs", fault_id, duration_sec)
        try:
            yield
        finally:
            with self._lock:
                self._fault_history.append({
                    'fault_id': fault_id,
                    'action': 'network_partition_ended',
                    'timestamp': time.time(),
                })
                self._active_faults.pop(fault_id, None)

    @contextmanager
    def inject_disk_full(self, fault_id: str):
        """磁盘满注入上下文管理器"""
        with self._lock:
            self._active_faults[fault_id] = {
                'type': 'disk_full',
                'timestamp': time.time(),
            }
        logging.warning("[ChaosEngineering] 磁盘满注入: %s", fault_id)
        try:
            yield
        finally:
            with self._lock:
                self._fault_history.append({
                    'fault_id': fault_id,
                    'action': 'disk_full_ended',
                    'timestamp': time.time(),
                })
                self._active_faults.pop(fault_id, None)

    def get_active_faults(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._active_faults)

    def get_fault_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._fault_history)

    def clear_history(self):
        with self._lock:
            self._fault_history.clear()


class ChaosExperiment:
    """混沌实验 — 定义和运行故障场景"""

    def __init__(self, name: str, injector: Optional[FaultInjector] = None):
        self.name = name
        self._injector = injector or FaultInjector()
        self._results: List[Dict[str, Any]] = []

    def run_experiment(self, hypothesis: str, fault_fn: Callable,
                       verify_fn: Callable, timeout_sec: float = 30.0) -> Dict[str, Any]:
        """运行一个混沌实验

        Args:
            hypothesis: 假设描述（如"断路器在连续3次失败后应打开"）
            fault_fn: 注入故障的函数
            verify_fn: 验证容错措施是否生效的函数，返回bool
            timeout_sec: 实验超时

        Returns:
            实验结果字典
        """
        result = {
            'experiment': self.name,
            'hypothesis': hypothesis,
            'timestamp': time.time(),
        }
        try:
            fault_fn(self._injector)
            passed = verify_fn()
            result['verdict'] = 'PASS' if passed else 'FAIL'
            result['detail'] = '容错措施生效' if passed else '容错措施未生效!!!'
            if not passed:
                logging.error("[ChaosEngineering] 实验失败: %s — %s", self.name, hypothesis)
            else:
                logging.info("[ChaosEngineering] 实验通过: %s — %s", self.name, hypothesis)
        except Exception as e:
            result['verdict'] = 'ERROR'
            result['detail'] = str(e)
            logging.error("[ChaosEngineering] 实验异常: %s — %s", self.name, e)
        self._results.append(result)
        return result

    def get_results(self) -> List[Dict[str, Any]]:
        return list(self._results)

    def summary(self) -> str:
        total = len(self._results)
        passed = sum(1 for r in self._results if r.get('verdict') == 'PASS')
        failed = sum(1 for r in self._results if r.get('verdict') == 'FAIL')
        errored = sum(1 for r in self._results if r.get('verdict') == 'ERROR')
        return (f"混沌实验[{self.name}]: 总{total}项, 通过{passed}, "
                f"失败{failed}, 异常{errored}")


_fault_injector: Optional[FaultInjector] = None
_fault_injector_lock = threading.Lock()


def get_fault_injector() -> FaultInjector:
    """获取全局故障注入器单例"""
    global _fault_injector
    with _fault_injector_lock:
        if _fault_injector is None:
            _fault_injector = FaultInjector()
        return _fault_injector
