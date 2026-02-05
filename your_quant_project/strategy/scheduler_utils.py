"""������ȸ������� (Ultra-Robust Version)��"""
from __future__ import annotations

import threading
import time
import traceback
from typing import Any, Callable, Dict, Optional, List

class RobustLoopScheduler:
    """
    ���򡢽�׳����ѭ����������
    �������κ��ⲿ���ӵĿ⣬������һ�� While True ѭ����
    ȷ�� run_trading_cycle ������ζ��ᱻִ�С�
    """
    def __init__(self, logger_func=print):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.running = False
        self.logger = logger_func
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._active_async_threads: Dict[str, threading.Thread] = {}

    def start(self):
        """�������Ź�ѭ��"""
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._master_loop, daemon=True, name="RobustMasterLoop")
        self._thread.start()
        try:
            self.logger("[Scheduler] RobustLoopScheduler Started.")
        except: pass

    def stop(self):
        """ֹͣ"""
        self.running = False
        try:
            self.logger("[Scheduler] RobustLoopScheduler Stopping...")
        except: pass

    def add_job(self, func: Callable, trigger: str, **kwargs) -> Any:
        """
        ��������
        Ŀǰ��֧�� interval ���͵Ķ�������
        """
        job_id = kwargs.get("id", f"job_{len(self.jobs)}")
        seconds = kwargs.get("seconds", 60) # Default 60s
        run_async = kwargs.get("run_async", False)

        with self._lock:
            self.jobs[job_id] = {
                "func": func,
                "interval": max(1, float(seconds)), # Minimum 1s
                "last_run": 0.0,
                "id": job_id,
                "run_async": run_async
            }
        try:
            self.logger(f"[Scheduler] Added Job: {job_id} (Interval: {seconds}s)")
        except: pass
        return job_id

    def remove_job(self, job_id: str):
        with self._lock:
            if job_id in self.jobs:
                del self.jobs[job_id]

    def get_job(self, job_id: str):
        with self._lock:
            return self.jobs.get(job_id)

    def _master_loop(self):
        """������ѭ�������������������"""
        while self.running:
            try:
                now = time.time()
                
                # Copy jobs to avoid locking during execution
                with self._lock:
                    active_jobs = list(self.jobs.values())

                for job in active_jobs:
                    job_id = job["id"]
                    interval = job["interval"]
                    last_run = job["last_run"]

                    if (now - last_run) >= interval:
                        try:
                            # [Fix] Run Job in Thread to avoid blocking the scheduler loop
                            # Robustness: Even if func hangs, the master loop continues.
                            def _job_wrapper(f, jid):
                                try:
                                    f()
                                except Exception as job_e:
                                    try:
                                        self.logger(f"[Scheduler] Job {jid} Async Error: {job_e}")
                                    except:
                                        print(f"[Scheduler] Job {jid} Async Error: {job_e}")

                            # Only spawn thread if interval is significant or blocking is suspected
                            # For safety, we spawn for everything > 1s or specifically marked.
                            # For simplicity/robustness here: Spawn for TRADING CYCLE (usually large interval)
                            # Run others sync? No, let's spawn for robustness if requested.
                            run_async_flag = job.get("run_async", False) or False
                            if job_id == "run_trading_cycle" or run_async_flag:
                                # [Fix] Thread Explosion Protection
                                # Check if previous thread for this job is still alive
                                if job_id in self._active_async_threads:
                                    existing_thread = self._active_async_threads[job_id]
                                    if existing_thread.is_alive():
                                        # [Optimization] Log warning if stuck (every 60s approx)
                                        # Use modulo on current time to limit spam
                                        if int(now) % 60 == 0:
                                            try:
                                                self.logger(f"[Scheduler] Warning: Job {job_id} is running longer than interval. Skipping this cycle.")
                                            except: pass
                                        # Skip this run, previous still working
                                        continue

                                t = threading.Thread(target=_job_wrapper, args=(job["func"], job_id), 
                                                 name=f"JobWorker_{job_id}", daemon=True)
                                self._active_async_threads[job_id] = t
                                t.start()
                            else:
                                # Keep small tasks sync to avoid thread explosion?
                                # Actually, user reported "position module not started".
                                # If sync tasks block, scheduler dies.
                                # Let's run all in thread but use a simple guard?
                                # Compromise: run_trading_cycle is the blocker.
                                job["func"]()
                            
                            # update time
                            with self._lock:
                                if job_id in self.jobs:
                                    self.jobs[job_id]["last_run"] = time.time()
                        except Exception as e:
                            try:
                                self.logger(f"[Scheduler] Error executing {job_id}: {e}")
                            except: pass
                            traceback.print_exc()

                # Sleep a small Tick (e.g. 0.5s) to prevent CPU spin
                time.sleep(0.5)

            except Exception as e:
                try:
                    self.logger(f"[Scheduler] Master Loop Critical Error: {e}")
                except: pass
                time.sleep(5) # Wait before retry

class SchedulerMixin:
    def _create_default_scheduler(self) -> Any:
        # [Fix] Abandon APScheduler/PythonGO Scheduler completely for stability.
        # Use the RobustLoopScheduler directly.
        self._debug("Using RobustLoopScheduler (Force)")
        
        # Pass self.output if available, else print
        logger = getattr(self, "output", print)
        scheduler = RobustLoopScheduler(logger_func=logger)
        scheduler.start()
        return scheduler

    def stop_scheduler(self):
        try:
            if hasattr(self, "scheduler") and self.scheduler:
                self.scheduler.stop()
        except Exception:
            pass

    def _safe_add_job(self, func: Callable, trigger: str, **kwargs) -> Any:
        return self._safe_add_periodic_job(kwargs.get("id", "unknown"), func, kwargs.get("seconds", 60))

    def _safe_add_once_job(self, job_id: str, func: Callable, delay_seconds: float) -> Optional[Any]:
        # Robust scheduler doesn support date trigger natively yet.
        # Workaround: Add interval job that removes itself?
        # Or just run in a thread.
        try:
            import threading
            def _delayed_run():
                time.sleep(delay_seconds)
                try:
                    func()
                except Exception as e:
                    print(f"Delayed job {job_id} failed: {e}")
            
            threading.Thread(target=_delayed_run, daemon=True, name=f"OnceJob_{job_id}").start()
            return True
        except Exception:
            return None

    def _safe_add_periodic_job(self, job_id: str, func: Callable, interval_seconds: float, **kwargs) -> Optional[Any]:
        try:
            if (not hasattr(self, "scheduler")) or (self.scheduler is None):
                self.scheduler = self._create_default_scheduler()
            else:
                try:
                    if hasattr(self.scheduler, "running") and not getattr(self.scheduler, "running"):
                        self.scheduler.start()
                except Exception:
                    pass
            
            # Direct add content
            self.scheduler.add_job(func, "interval", seconds=interval_seconds, id=job_id, **kwargs)
            return job_id
        except Exception as e:
            self._debug(f"Failed to add periodic job {job_id}: {e}")
            return None
            
    def _remove_job_silent(self, job_id: str) -> None:
        try:
            if self.scheduler:
                self.scheduler.remove_job(job_id)
        except Exception:
            pass

    def _safe_add_interval_job(self, job_id: str, func: Callable, seconds: int, kwargs: Optional[dict] = None) -> None:
        self._safe_add_periodic_job(job_id, func, interval_seconds=seconds)

    def _schedule_daily_signal_summary(self) -> None:
        pass
