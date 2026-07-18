"""
scheduler_service.py - 调度服务

合并来源：05_scheduler.py
合并策略：简化调度器实现，提取核心调度功能

重构目标：
- 旧架构：05_scheduler.py (~320行)
- 新架构：scheduler_service.py (~400行)
- 功能增强：超时、重试、优先级、取消

核心改进：
1. 单一职责：专注于定时任务调度
2. 防阻塞设计：所有任务在独立线程执行
3. 线程安全：支持并发操作
4. 增强功能：超时、重试、优先级、取消

作者：CodeArts 代码智能体
版本：v1.1
生成时间：2026-03-16
"""

from __future__ import annotations

import time
import logging
import logging.handlers
import threading
from typing import Any, Callable, Dict, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from datetime import time as dt_time, timezone  # ENV-P1-01修复: 导入timezone
from enum import Enum, auto

# P1-08修复: 从 market_time_service 重新导出，向后兼容
from infra.market_time_service import (  # noqa: F401
    MarketTimeService,
    is_market_open,
    get_market_time_service,
    TimeSyncChecker,
    get_time_sync_checker,
)



# ============================================================================
# 枚举与数据结构
# ============================================================================

class JobStatus(Enum):
    """任务状态"""
    PENDING = auto()     # 等待中
    RUNNING = auto()     # 运行中
    PAUSED = auto()      # 已暂停
    FAILED = auto()      # 失败
    CANCELLED = auto()   # 已取消


@dataclass(slots=True)
class JobInfo:
    """任务信息"""
    job_id: str
    func: Callable
    interval: float = 60.0
    last_run: float = 0.0
    run_count: int = 0
    status: JobStatus = JobStatus.PENDING
    run_async: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))  # ENV-P1-01修复: UTC时区
    # 新增字段
    priority: int = 0           # 优先级（数值越大优先级越高）
    timeout: float = 30.0       # 超时时间（秒）
    max_retries: int = 0        # 最大重试次数
    retry_count: int = 0        # 当前重试次数
    last_error: Optional[str] = None  # 最后一次错误信息
    _is_executing: bool = False  # 重叠执行防护标记


@dataclass(slots=True)
class OnceJobInfo:
    """一次性任务信息"""
    job_id: str
    func: Callable
    delay: float
    cancelled: bool = False
    thread: Optional[threading.Thread] = None
    stop_event: Optional[threading.Event] = None

    def is_running(self) -> bool:
        """检查任务是否正在运行"""
        if self.thread is None:
            return False
        return self.thread.is_alive()

    def stop(self) -> bool:
        """停止一次性任务"""
        if self.stop_event is not None:
            self.stop_event.set()
            return True
        return False


# ============================================================================
# 调度服务
# ============================================================================

class SchedulerService:
    """
    调度服务 - 定时任务管理

    职责：
    1. 定时任务调度
    2. 任务执行管理
    3. 防阻塞保护
    4. 任务状态监控

    职责边界：通用定时任务调度(超时/重试)，与strategy/StrategyScheduler(策略业务调度)分工。
    本类负责通用任务调度(超时/重试/优先级/取消)，StrategyScheduler负责策略业务调度(交易循环/持仓风控/缓存刷写)。

    使用方式：
        scheduler = SchedulerService()
        scheduler.add_job(my_func, interval=60, job_id="task1", priority=10)
        scheduler.start()
    """

    def __init__(self, logger_func: Optional[Callable] = None):
        """
        初始化调度服务

        Args:
            logger_func: 日志输出函数（可选）
        """
        self._logger = logger_func or print
        # P2-5修复: 添加FileHandler确保日志文件创建
        try:
            _scheduler_logger = logging.getLogger('infra.scheduler_service')
            if not _scheduler_logger.handlers:
                import os
                from config._params_canary_env import DEFAULT_LOG_DIR
                _log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), DEFAULT_LOG_DIR)
                os.makedirs(_log_dir, exist_ok=True)
                _log_path = os.path.join(_log_dir, 'scheduler_service.log')
                _fh = logging.handlers.RotatingFileHandler(
                    _log_path, maxBytes=10 * 1024 * 1024, backupCount=3, encoding='utf-8',
                )
                _fh.setFormatter(logging.Formatter(
                    '%(asctime)s %(levelname)s [%(name)s] %(message)s',
                ))
                _scheduler_logger.addHandler(_fh)
                _scheduler_logger.setLevel(logging.INFO)
        except (OSError, IOError, ValueError) as _err:
            logging.debug("[scheduler_service] I/O操作降级: %s", _err)
        self._jobs: Dict[str, JobInfo] = {}
        self._once_jobs: Dict[str, OnceJobInfo] = {}
        # ✅ P1修复：使用threading.Event替代bool标志，消除竞态
        self._running_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._active_threads: Dict[str, threading.Thread] = {}

        # 配置
        self._min_interval = 1.0
        self._tick_interval = 0.5
        self._stop_timeout = 1.0
        self._default_timeout = 30.0
        self._thread_cleanup_interval = 60.0
        self._last_cleanup = 0.0

    # ========================================================================
    # 生命周期管理
    # ========================================================================

    def start(self) -> bool:
        """启动调度器"""
        with self._lock:
            if self._running_event.is_set():
                self._log("[Scheduler] Already running")
                return False

            self._running_event.set()
            self._thread = threading.Thread(
                target=self._master_loop,
                daemon=True,
                name="SchedulerMasterLoop"
            )
            self._thread.start()
            self._log("[Scheduler] Started")
            return True

    def stop(self) -> None:
        """停止调度器"""
        # ✅ P1修复：使用Event.clear()替代直接赋值，线程安全
        self._running_event.clear()
        self._log("[Scheduler] Stopping...")

        # 取消所有一次性任务
        with self._lock:
            for job in self._once_jobs.values():
                job.cancelled = True

        # 等待主线程退出
        if self._thread and self._thread.is_alive():
            try:
                self._thread.join(timeout=self._stop_timeout)
            except (RuntimeError, OSError) as e:
                logging.warning(f"[Scheduler] Error joining thread: {e}")

        self._log("[Scheduler] Stopped")

    def is_running(self) -> bool:
        """检查是否运行中"""
        return self._running_event.is_set()

    # ========================================================================
    # 任务管理
    # ========================================================================

    def add_job(self, func: Callable, interval: float = 60.0,
                job_id: Optional[str] = None, run_async: bool = True,
                priority: int = 0, timeout: float = 30.0,
                max_retries: int = 0) -> Optional[str]:
        """
        添加定时任务

        Args:
            func: 任务函数
            interval: 执行间隔（秒）
            job_id: 任务ID（可选）
            run_async: 是否异步执行（默认True，防止阻塞）
            priority: 优先级（数值越大优先级越高）
            timeout: 超时时间（秒）
            max_retries: 最大重试次数

        Returns:
            任务ID，失败返回None
        """
        if not callable(func):
            logging.error("[Scheduler] func must be callable")
            return None

        # 生成任务ID
        if job_id is None:
            job_id = f"job_{int(time.time() * 1000)}"

        # 限制最小间隔
        interval = max(self._min_interval, float(interval))

        with self._lock:
            self._jobs[job_id] = JobInfo(
                job_id=job_id,
                func=func,
                interval=interval,
                run_async=run_async,
                priority=priority,
                timeout=max(1.0, timeout),
                max_retries=max(0, max_retries),
            )

        self._log(f"[Scheduler] Added job: {job_id} (interval: {interval}s, priority: {priority})")
        return job_id

    def remove_job(self, job_id: str) -> bool:
        """移除任务"""
        with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                self._log(f"[Scheduler] Removed job: {job_id}")
                return True
            return False

    def get_job(self, job_id: str) -> Optional[JobInfo]:
        """获取任务信息"""
        with self._lock:
            return self._jobs.get(job_id)

    def get_all_jobs(self) -> List[JobInfo]:
        """获取所有任务（按优先级排序）"""
        with self._lock:
            jobs = list(self._jobs.values())
            return sorted(jobs, key=lambda j: j.priority, reverse=True)

    def pause_job(self, job_id: str) -> bool:
        """暂停任务"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.status = JobStatus.PAUSED
                return True
            return False

    def resume_job(self, job_id: str) -> bool:
        """恢复任务"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job and job.status == JobStatus.PAUSED:
                job.status = JobStatus.PENDING
                return True
            return False

    def cancel_job(self, job_id: str) -> bool:
        """取消任务（统一入口：同时处理定时任务和一次性任务）"""
        with self._lock:
            # 取消定时任务
            if job_id in self._jobs:
                self._jobs[job_id].status = JobStatus.CANCELLED
                return True
            # 取消一次性任务（复用统一入口）
            if job_id in self._once_jobs:
                job_info = self._once_jobs[job_id]
                job_info.cancelled = True
                # P1 Bug #82修复：触发stop_event提前中断等待
                if hasattr(job_info, 'stop_event'):
                    job_info.stop_event.set()
                self._log(f"[Scheduler] Cancelled once job: {job_id}")
                return True
            return False

    # ========================================================================
    # 一次性任务
    # ========================================================================

    def add_once_job(self, func: Callable, delay: float,
                     job_id: Optional[str] = None) -> Optional[str]:
        """
        添加一次性延迟任务

        Args:
            func: 任务函数
            delay: 延迟时间（秒）
            job_id: 任务ID（可选）

        Returns:
            任务ID
        """
        if job_id is None:
            job_id = f"once_{int(time.time() * 1000)}"

        # Bug3修复：重复任务检查
        with self._lock:
            if job_id in self._once_jobs:
                existing_job = self._once_jobs[job_id]
                if existing_job.is_running():
                    self._log(f"[Scheduler] Once job {job_id} already running, skipped")
                    return None

        job_info = OnceJobInfo(job_id=job_id, func=func, delay=delay)

        # P1 Bug #82修复：使用threading.Event.wait实现精确延迟
        stop_event = threading.Event()

        def _delayed_run():
            try:
                if not stop_event.wait(timeout=delay):
                    if self._running_event.is_set() and not job_info.cancelled:
                        try:
                            func()
                        except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as e:
                            logging.error(f"[Scheduler] Once job {job_id} failed: {e}")
            finally:
                with self._lock:
                    if job_id in self._once_jobs:
                        del self._once_jobs[job_id]

        thread = threading.Thread(target=_delayed_run, daemon=True, name=f"OnceJob_{job_id}")
        job_info.thread = thread
        job_info.stop_event = stop_event  # 保存stop_event用于取消
        thread.start()

        with self._lock:
            self._once_jobs[job_id] = job_info

        self._log(f"[Scheduler] Added once job: {job_id} (delay: {delay}s)")
        return job_id

    # ========================================================================
    # 主循环
    # ========================================================================

    def _master_loop(self) -> None:
        """主调度循环"""
        while self._running_event.is_set():
            try:
                now = time.time()

                # 定期清理死线程
                if now - self._last_cleanup > self._thread_cleanup_interval:
                    self._cleanup_dead_threads()
                    self._last_cleanup = now

                # 复制任务列表避免长时间锁定，并按优先级排序
                with self._lock:
                    jobs_to_run = sorted(
                        [job for job in self._jobs.values()
                         if job.status not in (JobStatus.PAUSED, JobStatus.CANCELLED)],
                        key=lambda j: j.priority,
                        reverse=True
                    )

                for job in jobs_to_run:
                    self._try_run_job(job, now)

                # 分段休眠，快速响应停止
                for _ in range(int(self._tick_interval * 10)):
                    if not self._running_event.is_set():
                        break
                    time.sleep(0.1)

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.error(f"[Scheduler] Master loop error: {e}")
                time.sleep(1)

    def _try_run_job(self, job: JobInfo, now: float) -> None:
        """尝试执行任务"""
        if now - job.last_run < job.interval:
            return

        try:
            # 所有任务都在独立线程执行，防止阻塞
            self._run_job_with_timeout(job)

            # 更新执行时间 - [R22-TIME-P1-07] 使用预期时间避免漂移累积
            with self._lock:
                if job.job_id in self._jobs:
                    self._jobs[job.job_id].last_run = now  # 使用调度时的now，而非time.time()
                    self._jobs[job.job_id].run_count += 1
                    self._jobs[job.job_id].status = JobStatus.PENDING
                    self._jobs[job.job_id].retry_count = 0
                    self._jobs[job.job_id].last_error = None

        except (ValueError, TypeError, KeyError, AttributeError, RuntimeError) as e:
            error_msg = str(e)
            logging.error(f"[Scheduler] Job {job.job_id} error: {error_msg}")

            with self._lock:
                if job.job_id in self._jobs:
                    self._jobs[job.job_id].last_error = error_msg

                    if job.retry_count < job.max_retries:
                        job.retry_count += 1
                        self._log(f"[Scheduler] Job {job.job_id} retry {job.retry_count}/{job.max_retries}")
                        self._jobs[job.job_id].last_run = 0
                    else:
                        self._jobs[job.job_id].status = JobStatus.FAILED

    def _run_job_with_timeout(self, job: JobInfo) -> None:
        """带超时保护的任务执行

        R5-E-11修复: 添加重叠执行防护，防止同一任务并发执行。
        R15-P1-RES-14修复: 添加task_timeout_sec默认300秒，超时cancel。
        """
        # R15-P1-RES-14修复: 全局task超时上限
        _task_timeout_sec = getattr(self, '_task_timeout_sec', 300.0)
        _effective_timeout = min(job.timeout, _task_timeout_sec)
        # R5-E-11修复: 重叠执行防护
        if getattr(job, '_is_executing', False):
            logging.warning("[R5-E-11] Job %s仍在执行中，跳过本次调度(防重叠)", job.job_id)
            return
        job._is_executing = True
        try:
            with self._lock:
                if job.job_id in self._jobs:
                    self._jobs[job.job_id].status = JobStatus.RUNNING

            result_container = {'error': None, 'done': False}

            cancel_event = threading.Event()

            def _run():
                try:
                    if not cancel_event.is_set():
                        job.func()
                except Exception as e:
                    # FIX-20260707: 从窄异常列表扩展为Exception，
                    # 防止ImportError/NameError等未捕获异常导致线程静默死亡，
                    # 造成check_position_risk等关键job永远不执行
                    logging.error(f"[Scheduler] Job {job.job_id} execution error: {e}", exc_info=True)
                    result_container['error'] = e
                finally:
                    result_container['done'] = True

            thread = threading.Thread(target=_run, daemon=True, name=f"JobWorker_{job.job_id}")
            # ✅ P1修复：加锁保护_active_threads写入，避免并发竞态
            with self._lock:
                self._active_threads[job.job_id] = thread
            thread.start()

            # 等待超时
            thread.join(timeout=_effective_timeout)

            if not result_container['done']:
                logging.warning(f"[Scheduler] Job {job.job_id} timeout after {_effective_timeout}s, setting cancel event")
                cancel_event.set()
                with self._lock:
                    self._active_threads.pop(job.job_id, None)
                raise TimeoutError(f"Job {job.job_id} exceeded timeout of {job.timeout}s")

            if result_container['error']:
                raise result_container['error']
        finally:
            _still_alive = False
            try:
                _still_alive = thread.is_alive()
            except NameError:
                pass
            if _still_alive:
                job._is_executing = True
                logging.warning("[R5-E-11] Job %s超时后线程仍存活，保持_is_executing=True防重叠", job.job_id)
            else:
                job._is_executing = False

    def _cleanup_dead_threads(self) -> None:
        """清理已结束的线程"""
        with self._lock:
            dead_jobs = [
                job_id for job_id, thread in self._active_threads.items()
                if not thread.is_alive()
            ]
            for job_id in dead_jobs:
                del self._active_threads[job_id]

            if dead_jobs:
                self._log(f"[Scheduler] Cleaned up {len(dead_jobs)} dead threads")

    # ========================================================================
    # 辅助方法
    # ========================================================================

    def _log(self, message: str) -> None:
        """输出日志"""
        try:
            if self._logger:
                self._logger(message)
        except (ValueError, TypeError, AttributeError, RuntimeError) as e:
            logging.warning(f"[Scheduler] Log error: {e}")

    # ✅ ID唯一：get_stats统一接口，返回值含service_name="SchedulerService"
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            return {
                'service_name': 'SchedulerService',  # ✅ ID唯一：统一标识服务来源
                "running": self._running_event.is_set(),
                "total_jobs": len(self._jobs),
                "once_jobs": len(self._once_jobs),
                "active_threads": sum(1 for t in self._active_threads.values() if t.is_alive()),  # R21-MEM-P2-03修复: 生成器替代列表推导式，避免临时列表
                "jobs": {
                    job_id: {
                        "interval": job.interval,
                        "run_count": job.run_count,
                        "status": job.status.name,
                        "priority": job.priority,
                        "retry_count": job.retry_count,
                        "last_error": job.last_error,
                    }
                    for job_id, job in self._jobs.items()
                }
            }

    def clear(self) -> None:
        """清空所有任务"""
        with self._lock:
            # 取消所有一次性任务
            for job in self._once_jobs.values():
                job.cancelled = True
            self._jobs.clear()
            self._once_jobs.clear()
            self._active_threads.clear()


# ============================================================================
# Section 4: 调度器管理代理 (原 scheduler_manager_proxy.py)
# ============================================================================

class SchedulerManagerProxy:
    """调度器管理Proxy — 初始化/停止/任务注册代理"""

    def __init__(self, provider: Any = None):
        self._provider = provider

    def _get_scheduler(self) -> Any:
        if self._provider:
            return getattr(self._provider, '_scheduler_manager', None)
        return None

    def init_scheduler(self) -> None:
        """初始化调度器"""
        scheduler = self._get_scheduler()
        if scheduler and hasattr(scheduler, 'initialize'):
            scheduler.initialize()

    def stop_scheduler(self, strategy_id: str = '') -> None:
        """停止调度器"""
        scheduler = self._get_scheduler()
        if not scheduler:
            return
        if not strategy_id and self._provider:
            strategy_id = getattr(self._provider, 'strategy_id', '')
        try:
            scheduler.stop_strategy_jobs(strategy_id)
        except (AttributeError, RuntimeError, ValueError, TypeError) as e:
            logging.warning("[SchedulerManagerProxy] stop_strategy_jobs error: %s", e)
        try:
            scheduler.remove_jobs_by_owner('GLOBAL')
        except (AttributeError, RuntimeError, ValueError, TypeError) as e:
            logging.warning("[SchedulerManagerProxy] remove GLOBAL jobs error: %s", e)
        try:
            scheduler.shutdown()
        except (AttributeError, RuntimeError, ValueError, TypeError) as e:
            logging.warning("[SchedulerManagerProxy] shutdown error: %s", e)
            return

        import threading as _th
        _alive_threads = [t for t in _th.enumerate() if t.name.startswith('scheduler_') and t.is_alive()]
        if _alive_threads:
            _deadline = time.time() + 10.0
            for t in _alive_threads:
                t.join(timeout=max(0, _deadline - time.time()))
            _still_alive = [t for t in _alive_threads if t.is_alive()]
            if _still_alive:
                logging.critical("R15-P1-RES-06: 调度器线程超时未退出，强制终止: %s",
                                 [t.name for t in _still_alive])
                os._exit(1)

    def add_option_status_diagnosis_job(self, t_type_service: Any = None) -> None:
        """添加期权5种状态诊断定时任务"""
        scheduler = self._get_scheduler()
        if scheduler and hasattr(scheduler, 'register_option_diagnosis_task'):
            scheduler.register_option_diagnosis_task(t_type_service)

    def add_tick_sync_job(self, data_service: Any = None) -> None:
        """添加缓存刷写定时任务"""
        scheduler = self._get_scheduler()
        if scheduler and hasattr(scheduler, 'register_cache_flush_task'):
            scheduler.register_cache_flush_task(data_service)

    def add_14_contracts_diagnosis_job(self, storage: Any = None,
                                       query_service: Any = None) -> None:
        """添加重点监控合约诊断定时任务"""
        scheduler = self._get_scheduler()
        if scheduler and hasattr(scheduler, 'register_14_contracts_diagnosis_task'):
            scheduler.register_14_contracts_diagnosis_task(
                storage=storage, query_service=query_service
            )

    def add_trading_jobs(self, strategy_id: str = '', run_id: str = None,
                         execute_option_trading_cycle: Any = None,
                         check_position_risk: Any = None,
                         order_service: Any = None) -> None:
        """注册交易定时任务"""
        scheduler = self._get_scheduler()
        if scheduler and hasattr(scheduler, 'register_trading_jobs'):
            scheduler.register_trading_jobs(
                strategy_id=strategy_id,
                run_id=run_id,
                execute_option_trading_cycle=execute_option_trading_cycle,
                check_position_risk=check_position_risk,
                order_service=order_service
            )

    def ensure_check_pending_orders_job(self, order_service: Any = None,
                                         strategy_id: str = '',
                                         scheduler: Any = None,
                                         run_id: str = None) -> None:
        """确保check_pending_orders任务已注册"""
        if order_service is None and self._provider:
            order_service = getattr(self._provider, '_order_service', None)
        if not order_service or not hasattr(order_service, 'check_pending_orders'):
            return
        if scheduler is None:
            scheduler = self._get_scheduler()
        if not scheduler or not hasattr(scheduler, 'scheduler') or not scheduler.scheduler:
            return
        if not strategy_id and self._provider:
            strategy_id = getattr(self._provider, 'strategy_id', '')
        if not run_id and self._provider:
            run_id = getattr(self._provider, '_lifecycle_run_id', None)
        job_id = f'{strategy_id}_check_pending_orders'
        try:
            existing = scheduler.scheduler.get_job(job_id)
            if existing is not None:
                return
        except (ImportError, AttributeError) as _err:
            logging.debug("[scheduler_manager_proxy] 属性访问降级: %s", _err)
        try:
            scheduler.add_job_with_owner(
                func=order_service.check_pending_orders,
                trigger='interval',
                job_id=job_id,
                strategy_id=strategy_id,
                run_id=run_id,
                owner_scope='strategy',
                seconds=3
            )
            logging.info("[SchedulerManagerProxy] 补偿注册check_pending_orders job成功")
        except (AttributeError, RuntimeError, ValueError, TypeError, KeyError) as e:
            logging.error("[SchedulerManagerProxy] 补偿注册check_pending_orders job失败: %s", e)


# ============================================================================
# 模块导出
# ============================================================================

# P1-43修复: 工厂函数，避免生产代码直接实例化SchedulerService
_scheduler_service_instance = None


def get_scheduler_service():
    """P1-43修复: SchedulerService单例工厂——生产代码应通过此函数获取实例"""
    global _scheduler_service_instance
    if _scheduler_service_instance is None:
        _scheduler_service_instance = SchedulerService()
    return _scheduler_service_instance


__all__ = [
    'SchedulerService',
    'get_scheduler_service',
    'SchedulerManagerProxy',
    'JobStatus',
    'JobInfo',
    'OnceJobInfo',
    # P1-08修复: 以下从 market_time_service 重新导出
    'MarketTimeService',
    'is_market_open',
    'get_market_time_service',
    'TimeSyncChecker',
    'get_time_sync_checker',
]
