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


# ============================================================================
# DR-P1-12修复: 时钟同步检测
# ============================================================================
class TimeSyncChecker:
    """DR-P1-12: 使用简单时间源比对检测时钟偏差

    偏差 > 5秒  → WARNING
    偏差 > 30秒 → CRITICAL → 触发暂停交易
    """

    WARNING_THRESHOLD_SEC = 5.0
    CRITICAL_THRESHOLD_SEC = 30.0
    CHECK_INTERVAL_SEC = 300.0  # 每5分钟检查一次

    def __init__(self):
        self._last_check_time: float = 0.0
        self._last_deviation: float = 0.0
        self._status: str = 'HEALTHY'  # HEALTHY / WARNING / CRITICAL
        self._lock = threading.Lock()

    def check_time_sync(self) -> Dict[str, Any]:
        """DR-P1-12: 检测系统时钟与外部时间源的偏差

        Returns:
            dict: 包含 deviation_sec, status, message
        """
        now = time.time()
        if now - self._last_check_time < self.CHECK_INTERVAL_SEC:
            return {
                'deviation_sec': self._last_deviation,
                'status': self._status,
                'message': f'上次检查: {self._status} (偏差={self._last_deviation:.1f}s)',
            }

        deviation = self._measure_time_deviation()
        self._last_check_time = now
        self._last_deviation = deviation

        with self._lock:
            if abs(deviation) >= self.CRITICAL_THRESHOLD_SEC:
                self._status = 'CRITICAL'
                msg = (f"[DR-P1-12] CRITICAL: 时钟偏差{deviation:.1f}秒超过{self.CRITICAL_THRESHOLD_SEC}秒阈值,"
                       f"建议暂停交易!")
                logging.critical(msg)
            elif abs(deviation) >= self.WARNING_THRESHOLD_SEC:
                self._status = 'WARNING'
                msg = f"[DR-P1-12] WARNING: 时钟偏差{deviation:.1f}秒超过{self.WARNING_THRESHOLD_SEC}秒阈值"
                logging.warning(msg)
            else:
                self._status = 'HEALTHY'
                msg = f"[DR-P1-12] 时钟同步正常,偏差={deviation:.3f}秒"
                logging.info(msg)

            return {
                'deviation_sec': deviation,
                'status': self._status,
                'message': msg,
            }

    def _measure_time_deviation(self) -> float:
        """测量本地时钟与外部时间源的偏差"""
        try:
            # 方法1: 使用HTTP Date头获取外部时间
            # R21-CC-P1-06修复: urllib网络I/O已有timeout=5.0，防止后台线程阻塞
            import urllib.request
            req = urllib.request.Request('http://www.baidu.com', method='HEAD')
            req_start = time.time()
            response = urllib.request.urlopen(req, timeout=5.0)
            req_end = time.time()
            server_date = response.headers.get('Date', '')
            if server_date:
                from email.utils import parsedate_to_datetime
                server_time = parsedate_to_datetime(server_date).timestamp()
                rtt = req_end - req_start
                estimated_server_now = server_time + rtt / 2.0
                local_now = req_end
                return local_now - estimated_server_now
        except Exception as e:
            logging.debug("[DR-P1-12] HTTP时间同步检测失败: %s", e)

        try:
            # 方法2: 使用time.time()自身作为后备
            before = time.time()
            time.sleep(0.01)  # R23-P2-09标记: P2级调度等待
            after = time.time()
            drift = (after - before) - 0.01
            if abs(drift) > 0.1:
                return drift
        except Exception:
            pass

        return 0.0

    def get_status(self) -> str:
        with self._lock:
            return self._status

    def is_critical(self) -> bool:
        return self.get_status() == 'CRITICAL'

    def is_warning(self) -> bool:
        return self.get_status() == 'WARNING'


# 全局单例
_time_sync_checker: Optional[TimeSyncChecker] = None
_time_sync_lock = threading.Lock()


def get_time_sync_checker() -> TimeSyncChecker:
    global _time_sync_checker
    with _time_sync_lock:
        if _time_sync_checker is None:
            _time_sync_checker = TimeSyncChecker()
        return _time_sync_checker


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
            _scheduler_logger = logging.getLogger('ali2026v3_trading.infra.scheduler_service')
            if not _scheduler_logger.handlers:
                import os
                _log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
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
        except Exception:
            pass
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
            except Exception as e:
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
                        except Exception as e:
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

            except Exception as e:
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

        except Exception as e:
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
                    logging.error(f"[Scheduler] Job {job.job_id} execution error: {e}")
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
        except Exception as e:
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
# 模块导出
# ============================================================================

__all__ = [
    'SchedulerService',
    'JobStatus',
    'JobInfo',
    'OnceJobInfo',
]


# ============================================================================
# P1 功能恢复：交易日历相关（从 01_constants.py 恢复）
# ============================================================================

# ✅ 删除is_trading_day模块级函数，统一使用MarketTimeService.is_trading_day方法



_market_time_service_instance: Optional['MarketTimeService'] = None
_market_time_service_lock = threading.Lock()

def get_market_time_service() -> 'MarketTimeService':
    global _market_time_service_instance
    if _market_time_service_instance is None:
        with _market_time_service_lock:
            if _market_time_service_instance is None:
                _market_time_service_instance = MarketTimeService()
    return _market_time_service_instance

def is_market_open(exchange: Optional[str] = None) -> bool:
    """市场是否开盘（委托给MarketTimeService）"""
    return get_market_time_service().is_market_open(exchange)

# ✅ 删除is_trading_day模块级函数，统一使用MarketTimeService

class MarketTimeService:
    def __init__(self):
        self._sessions = {
            'SHFE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'DCE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'CZCE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'INE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'GFEX': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'CFFEX': [(9, 30, 11, 30), (13, 0, 15, 15)],  # P2-8修复: 中金所日盘时段
        }
        self._night_sessions = {
            # [R22-TIME-P1-11] 夜盘扩展至凌晨02:30，覆盖完整交易时段
            # P2-8修复: 商品期货(SHFE/DCE/CZCE/INE/GFEX)夜盘至次日02:30
            'SHFE': [(21, 0, 23, 0), (0, 0, 2, 30)],
            'DCE': [(21, 0, 23, 0), (0, 0, 2, 30)],
            'CZCE': [(21, 0, 23, 30), (0, 0, 2, 30)],
            'INE': [(21, 0, 23, 0), (0, 0, 2, 30)],
            'GFEX': [(21, 0, 23, 0), (0, 0, 2, 30)],
            # P2-8修复: 金融期货(CFFEX)夜盘至23:00，无次日凌晨段
            'CFFEX': [(21, 0, 23, 0)],
        }
        # [R22-TIME-P1-14] 默认节假日数据，防止非交易日判断失效
        self.holidays: set = set()  # 仍为空集合，但提供add_default_holidays方法
    
    def add_holiday(self, d: datetime.date) -> None:
        """添加节假日"""
        self.holidays.add(d)
    
    def add_default_holidays(self) -> None:
        """[R22-TIME-P1-14] 添加2026年默认中国法定节假日"""
        import datetime as _dt
        _default_2026 = [
            _dt.date(2026,1,1), _dt.date(2026,1,2), _dt.date(2026,1,3),  # 元旦
            _dt.date(2026,2,17), _dt.date(2026,2,18), _dt.date(2026,2,19),  # 春节
            _dt.date(2026,2,20), _dt.date(2026,2,21), _dt.date(2026,2,22),
            _dt.date(2026,4,4), _dt.date(2026,4,5), _dt.date(2026,4,6),  # 清明
            _dt.date(2026,5,1), _dt.date(2026,5,2), _dt.date(2026,5,3),  # 劳动节
            _dt.date(2026,6,19), _dt.date(2026,6,20), _dt.date(2026,6,21),  # 端午
            _dt.date(2026,10,1), _dt.date(2026,10,2), _dt.date(2026,10,3),  # 国庆
            _dt.date(2026,10,4), _dt.date(2026,10,5), _dt.date(2026,10,6),
            _dt.date(2026,10,7), _dt.date(2026,10,8),
        ]
        self.holidays.update(_default_2026)
    
    def is_trading_day(self, target_date: datetime.date, holiday_dates: Optional[set] = None) -> bool:
        """
        判断是否为交易日
        
        Args:
            target_date: 目标日期
            holiday_dates: 额外的节假日集合（可选）
            
        Returns:
            bool: 是否为交易日
        """
        if target_date.weekday() >= 5:  # 周末
            return False
        
        # 检查内部节假日
        if target_date in self.holidays:
            return False
        
        # 检查外部传入的节假日
        if holiday_dates and target_date in holiday_dates:
            return False
        
        return True
    
    def is_market_open(self, exchange: Optional[str] = None) -> bool:
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ  # [R22-TIME-P1-01] 统一时区常量
        now = datetime.now(CHINA_TZ)
        now_time = now.time()
        exchanges = [exchange] if exchange else list(self._sessions.keys())
        for exch in exchanges:
            sessions = self._sessions.get(exch, [])
            for start_h, start_m, end_h, end_m in sessions:
                start_time = dt_time(start_h, start_m)
                end_time = dt_time(end_h, end_m)
                if start_time <= now_time <= end_time:
                    return True
            night_sessions = self._night_sessions.get(exch, [])
            for start_h, start_m, end_h, end_m in night_sessions:
                start_time = dt_time(start_h, start_m)
                end_time = dt_time(end_h, end_m)
                # P1 Bug #83修复：正确处理跨午夜时段
                if start_time <= end_time:
                    # 不跨午夜：start <= now <= end
                    if start_time <= now_time <= end_time:
                        return True
                else:
                    # 跨午夜：now >= start OR now <= end
                    if now_time >= start_time or now_time <= end_time:
                        return True
        return False
