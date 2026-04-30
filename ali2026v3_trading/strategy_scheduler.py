"""
strategy_scheduler.py - 策略定时任务管理

职责：
1. APScheduler初始化和管理
2. 定时任务注册（期权诊断、缓存刷写）
3. 调度器生命周期管理

作者：CodeArts 代码智能体
版本：v1.0
生成时间：2026-04-11
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional


class StrategyScheduler:
    """策略调度器管理器
    
    封装APScheduler的初始化和定时任务注册逻辑，
    从StrategyCoreService中解耦出来。
    """
    
    def __init__(self):
        self._scheduler = None
        self._state_checker: Optional[Callable[[], bool]] = None
        self._job_owners: dict[str, dict[str, Any]] = {}  # ✅ P0-3: job -> owner 映射
        self._job_owners_lock = threading.Lock()  # P0-3: _job_owners 线程安全保护
    
    @property
    def scheduler(self):
        """获取调度器实例"""
        return self._scheduler

    def bind_state_checker(self, state_checker: Optional[Callable[[], bool]]) -> None:
        """绑定策略状态检查器，用于暂停/停止时短路后台定时任务。"""
        self._state_checker = state_checker

    def _can_run_jobs(self) -> bool:
        """统一判定当前是否允许执行策略相关定时任务。"""
        if not callable(self._state_checker):
            return True

        try:
            result = bool(self._state_checker())
            if not result:
                logging.debug("[StrategyScheduler][owner_scope=strategy] Jobs blocked by state_checker")
            return result
        except Exception as exc:
            logging.debug("[StrategyScheduler][owner_scope=strategy] State checker failed: %s", exc)
            return False
    
    def initialize(self) -> None:
        """初始化调度器（APScheduler为必需依赖，带有限重试）"""
        max_retries = 3
        retry_delay = 5.0  # 秒
        
        for attempt in range(1, max_retries + 1):
            try:
                from apscheduler.schedulers.background import BackgroundScheduler
                self._scheduler = BackgroundScheduler()
                self._scheduler.start()
                logging.info(f"[StrategyScheduler] APScheduler initialized (attempt {attempt})")
                return  # 成功则返回
            except Exception as e:
                # ✅ 统一异常处理：ImportError和运行时异常采用相同重试策略
                if attempt < max_retries:
                    logging.warning(
                        f"[StrategyScheduler] APScheduler init failed (attempt {attempt}/{max_retries}): {type(e).__name__}: {e}"
                    )
                    logging.info(f"[StrategyScheduler] Retrying in {retry_delay}s...")
                    import time
                    time.sleep(retry_delay)
                else:
                    # 重试耗尽，抛出异常由上层处理
                    if isinstance(e, ImportError):
                        error_msg = (
                            "APScheduler is required but not installed.\n"
                            "Please install: pip install apscheduler>=3.10.0\n"
                            "Or use: pip install -r requirements.txt"
                        )
                    else:
                        error_msg = (
                            f"APScheduler initialization failed after {max_retries} attempts.\n"
                            f"Last error: {type(e).__name__}: {e}\n"
                            "This may indicate a system resource issue or APScheduler configuration problem.\n"
                            "Please check system logs and consider restarting the platform."
                        )
                    logging.critical(f"[StrategyScheduler] {error_msg}")
                    raise RuntimeError(error_msg) from e
    
    def shutdown(self, wait: bool = True, wait_for_zero: bool = False, zero_timeout: float = 10.0) -> None:
        """停止调度器
        
        ✅ P0-2: 两阶段停止 - 先冻结新任务，再等待归零，最后shutdown
        
        Args:
            wait: True=等待运行中任务完成(可能阻塞); False=立即断开调度器,再限时join
            wait_for_zero: True=等待job归零后再shutdown
            zero_timeout: 等待job归零的超时时间（秒）
        """
        if not self._scheduler:
            return
        
        try:
            # ✅ P0-2: Phase 1 - 冻结新任务
            if wait_for_zero and hasattr(self._scheduler, 'pause'):
                self._scheduler.pause()
                logging.info("[StrategyScheduler] Phase 1: Scheduler paused (no new jobs)")
            
            # ✅ P0-2: Phase 2 - 等待job归零
            if wait_for_zero:
                zero_result = self.wait_for_jobs_zero(timeout=zero_timeout)
                if not zero_result:
                    logging.warning(
                        "[StrategyScheduler] ⚠️ Jobs not zero after %.1fs, proceeding with shutdown anyway",
                        zero_timeout
                    )
            
            # Phase 3 - shutdown
            self._scheduler.shutdown(wait=wait)
            if not wait:
                import threading
                for t in threading.enumerate():
                    if t.name and 'APScheduler' in t.name and t.is_alive():
                        t.join(timeout=3.0)
            
            logging.info("[StrategyScheduler] Shutdown complete")
        except Exception as e:
            logging.error(f"[StrategyScheduler] Shutdown error: {e}")
    
    def pause_scheduler(self) -> bool:
        """冻结调度器（不停止运行中任务，但拒绝新任务）
        
        ✅ P0-2: Phase 1 of two-phase stop
        
        Returns:
            bool: 是否成功冻结
        """
        if not self._scheduler:
            return True
        
        try:
            if hasattr(self._scheduler, 'pause'):
                self._scheduler.pause()
                logging.info("[StrategyScheduler] Scheduler paused (no new jobs will run)")
                return True
        except Exception as e:
            logging.error(f"[StrategyScheduler] pause_scheduler error: {e}")
            return False
        return True
    
    def get_running_job_count(self) -> int:
        if not self._scheduler:
            return 0
        
        try:
            running_count = 0
            if hasattr(self._scheduler, '_executors'):
                for executor in self._scheduler._executors.values():
                    if hasattr(executor, '_instances'):
                        running_count += len(executor._instances)
            return running_count
        except Exception as e:
            logging.debug(f"[StrategyScheduler] get_running_job_count error: {e}")
            return 0
    
    def get_registered_job_count(self) -> int:
        if not self._scheduler:
            return 0
        try:
            return len(self._scheduler.get_jobs())
        except Exception as e:
            logging.debug(f"[StrategyScheduler] get_registered_job_count error: {e}")
            return 0
    
    def wait_for_jobs_zero(self, timeout: float = 10.0) -> bool:
        if not self._scheduler:
            return True
        
        import time
        
        start_time = time.monotonic()
        check_interval = 0.5
        
        while time.monotonic() - start_time < timeout:
            running_count = self.get_running_job_count()
            registered_count = self.get_registered_job_count()
            
            if running_count == 0 and registered_count == 0:
                logging.info(
                    "[StrategyScheduler] All jobs zero confirmed (running=%d, registered=%d) after %.1fs",
                    running_count, registered_count, time.monotonic() - start_time
                )
                return True
            
            # running_count==0 即视为安全：已无job在执行，registered残留是APScheduler内部引用
            # remove_jobs_by_owner已从_job_owners移除，scheduler.get_jobs()可能返回陈旧引用
            if running_count == 0 and registered_count > 0:
                # 等待额外1秒确认running确实为0（避免竞态）
                time.sleep(1.0)
                recheck_running = self.get_running_job_count()
                if recheck_running == 0:
                    logging.info(
                        "[StrategyScheduler] Jobs safe to stop: running=0, registered=%d (stale refs) after %.1fs",
                        registered_count, time.monotonic() - start_time
                    )
                    return True
            
            logging.debug(
                "[StrategyScheduler] Waiting for jobs to zero: %d running, %d registered, %.1fs elapsed",
                running_count, registered_count, time.monotonic() - start_time
            )
            time.sleep(check_interval)
        
        final_running = self.get_running_job_count()
        final_registered = self.get_registered_job_count()
        logging.warning(
            "[StrategyScheduler] Jobs not zero after %.1fs: %d running, %d registered",
            timeout, final_running, final_registered
        )
        return False
    
    # ========================================================================
    # ✅ P0-3: 统一 job 注册包装器（带 owner metadata）
    # ========================================================================
    
    def add_job_with_owner(
        self,
        func: Callable,
        trigger: str,
        job_id: str,
        strategy_id: str,
        run_id: Optional[str] = None,
        owner_scope: str = "strategy",
        **trigger_args
    ) -> None:
        """
        统一的 job 注册方法，自动绑定 owner metadata
        
        Args:
            func: 要执行的函数
            trigger: 触发器类型 ('interval', 'cron', 'date')
            job_id: job ID
            strategy_id: 策略ID（owner标识）
            run_id: 运行ID（用于追踪）
            owner_scope: owner范围 ('strategy', 'global', 'shared')
            **trigger_args: 触发器参数 (seconds, minutes, hour等)
        """
        if not self._scheduler:
            logging.warning("[StrategyScheduler.add_job_with_owner] Scheduler not initialized")
            return
        
        try:
            # 注册 job
            self._scheduler.add_job(
                func,
                trigger,
                id=job_id,
                replace_existing=True,
                **trigger_args
            )
            
            # 记录 owner metadata（线程安全）
            with self._job_owners_lock:
                self._job_owners[job_id] = {
                    'strategy_id': strategy_id,
                    'run_id': run_id,
                    'owner_scope': owner_scope,
                    'func_name': getattr(func, '__name__', str(func))
                }
            
            logging.debug(
                f"[StrategyScheduler] Job registered: {job_id} "
                f"(owner={strategy_id}, scope={owner_scope})"
            )
            
        except Exception as e:
            logging.error(f"[StrategyScheduler.add_job_with_owner] Failed to register job {job_id}: {e}")
    
    def remove_jobs_by_owner(self, strategy_id: str) -> int:
        """
        根据 owner 移除所有相关 job
        
        Args:
            strategy_id: 策略ID
            
        Returns:
            移除的 job 数量
        """
        if not self._scheduler:
            return 0
        
        removed_count = 0
        with self._job_owners_lock:
            jobs_to_remove = [
                job_id for job_id, meta in self._job_owners.items()
                if meta.get('strategy_id') == strategy_id
            ]
            for job_id in jobs_to_remove:
                try:
                    self._scheduler.remove_job(job_id)
                    self._job_owners.pop(job_id, None)
                    removed_count += 1
                    logging.debug(f"[StrategyScheduler] Removed job: {job_id} (owner={strategy_id})")
                except Exception as e:
                    logging.warning(f"[StrategyScheduler] Failed to remove job {job_id}: {e}")
        
        logging.info(
            f"[StrategyScheduler] Removed {removed_count} jobs for strategy {strategy_id}"
        )
        return removed_count
    
    def get_jobs_by_owner(self, strategy_id: str) -> list[dict[str, Any]]:
        """
        查询指定 owner 的所有 job
        
        Args:
            strategy_id: 策略ID
            
        Returns:
            job 信息列表
        """
        with self._job_owners_lock:
            return [
                {'job_id': job_id, **meta}
                for job_id, meta in self._job_owners.items()
                if meta.get('strategy_id') == strategy_id
            ]
    
    def verify_jobs_removed(self, strategy_id: str) -> bool:
        """
        验证指定 owner 的所有 job 是否已移除
        
        Args:
            strategy_id: 策略ID
            
        Returns:
            True=已全部移除, False=仍有残留
        """
        remaining = self.get_jobs_by_owner(strategy_id)
        if remaining:
            logging.warning(
                f"[StrategyScheduler] ⚠️ {len(remaining)} jobs still exist for strategy {strategy_id}: "
                f"{[j['job_id'] for j in remaining]}"
            )
            return False
        return True
    
    def register_trading_jobs(
        self,
        strategy_id: str,
        run_id: Optional[str],
        execute_option_trading_cycle: Callable,
        check_position_risk: Callable,
        order_service: Any
    ) -> None:
        """
        注册交易定时任务（封装 P0-3 owner metadata）
        
        Args:
            strategy_id: 策略ID
            run_id: 运行ID
            execute_option_trading_cycle: 期权交易周期函数
            check_position_risk: 持仓风控函数
            order_service: 订单服务对象
        """
        try:
            # P0-3修复：job_id 加入 strategy_id 前缀，避免多实例场景下互相覆盖
            # 注册 3 个核心交易 job
            self.add_job_with_owner(
                func=execute_option_trading_cycle,
                trigger='interval',
                job_id=f'{strategy_id}_option_trading_cycle',
                strategy_id=strategy_id,
                run_id=run_id,
                owner_scope='strategy',
                seconds=30
            )
            
            self.add_job_with_owner(
                func=check_position_risk,
                trigger='interval',
                job_id=f'{strategy_id}_position_risk_check',
                strategy_id=strategy_id,
                run_id=run_id,
                owner_scope='strategy',
                seconds=5
            )
            
            if order_service and hasattr(order_service, 'check_pending_orders'):
                self.add_job_with_owner(
                    func=order_service.check_pending_orders,
                    trigger='interval',
                    job_id=f'{strategy_id}_check_pending_orders',
                    strategy_id=strategy_id,
                    run_id=run_id,
                    owner_scope='strategy',
                    seconds=3
                )
            
            logging.info(
                f"[StrategyScheduler] ✅ Registered 3 trading jobs (strategy={strategy_id}, run_id={run_id}): "
                f"option_trading_cycle(30s)/position_risk(5s)/pending_orders(3s)"
            )
        except Exception as e:
            logging.error(f"[StrategyScheduler] Failed to register trading jobs: {e}")
    
    def stop_strategy_jobs(self, strategy_id: str) -> int:
        """
        停止策略的所有 job（封装 P0-3 owner 移除 + 验证）
        
        Args:
            strategy_id: 策略ID
            
        Returns:
            移除的 job 数量
        """
        try:
            removed_count = self.remove_jobs_by_owner(strategy_id)
            logging.info(f"[StrategyScheduler] Removed {removed_count} jobs for strategy {strategy_id}")
            
            if not self.verify_jobs_removed(strategy_id):
                logging.warning(f"[StrategyScheduler] ⚠️ Some jobs may still exist for strategy {strategy_id}")
            
            return removed_count
        except Exception as e:
            logging.error(f"[StrategyScheduler] Failed to stop strategy jobs: {e}")
            return 0
    
    def register_option_diagnosis_task(self, t_type_service: Any) -> None:
        """注册期权5种状态诊断定时任务
        
        每3分钟输出一次期权5种状态的统计信息，帮助监控期权同步情况。
        
        Args:
            t_type_service: TTypeService实例
        """
        try:
            # 检查 t_type_service 是否可用
            if not t_type_service:
                logging.debug("[StrategyScheduler] t_type_service not available, skip option status diagnosis job")
                return
            
            def _diagnose_job():
                try:
                    if not self._can_run_jobs():
                        return
                    # 输出所有产品的期权状态诊断
                    t_type_service.print_option_status_diagnosis(future_internal_id=None, top_n=5)
                except Exception as e:
                    logging.error(f"[StrategyScheduler] Option status diagnosis failed: {e}", exc_info=True)
            
            # 添加定时任务：每3分钟执行一次
            if self._scheduler is not None and hasattr(self._scheduler, 'add_job'):
                # ✅ P0-3: 使用统一注册方法，标记为全局任务
                self.add_job_with_owner(
                    func=_diagnose_job,
                    trigger='interval',
                    job_id='option_status_diagnosis',
                    strategy_id='GLOBAL',
                    run_id=None,
                    owner_scope='global',
                    minutes=3
                )
                logging.info("[StrategyScheduler] ✅ 期权5种状态诊断任务已添加 (每3分钟)")
            else:
                logging.warning("[StrategyScheduler] Scheduler not available or does not support add_job")
        except Exception as e:
            logging.error(f"[StrategyScheduler] Failed to add option status diagnosis job: {e}", exc_info=True)
    
    def register_cache_flush_task(self, data_service: Any) -> None:
        """注册缓存刷写定时任务
        
        收市时段将 RealTimeCache 内存数据批量刷写到 DuckDB ticks_raw，
        同时截断WAL文件。刷写后 DuckDB 数据可供历史查询使用。
        
        刷写窗口：
        - 中午休市时段：12:00 - 12:50
        - 下午收盘后：15:30 - 20:00
        
        Args:
            data_service: DataService实例
        """
        try:
            def _is_in_flush_window() -> bool:
                """检查当前时间是否在刷写窗口内"""
                from datetime import datetime
                now = datetime.now()
                hour = now.hour
                minute = now.minute
                
                # 窗口1：12:00 - 12:50（中午休市）
                if hour == 12 and minute <= 50:
                    return True
                
                # 窗口2：15:30 - 20:00（下午收盘后）
                if hour == 15 and minute >= 30:
                    return True
                if 16 <= hour <= 19:
                    return True
                
                return False
            
            def _flush_job():
                cache_ticks = None
                cache = None
                try:
                    if not self._can_run_jobs():
                        return
                    if not _is_in_flush_window():
                        logging.debug("[StrategyScheduler] Cache flush skipped: outside flush window")
                        return
                    
                    cache = getattr(data_service, 'realtime_cache', None)
                    if not cache:
                        logging.debug("[StrategyScheduler] Cache flush skipped: no realtime_cache")
                        return
                    
                    cache_ticks = cache.drain_all_ticks(clear_cache=False)
                    if not cache_ticks:
                        logging.debug("[StrategyScheduler] Cache flush skipped: no data to flush")
                        return
                    
                    logging.info(f"[StrategyScheduler] Cache flush starting: {len(cache_ticks)} ticks...")
                    inserted = data_service.batch_insert_from_cache(cache_ticks)
                    
                    if inserted > 0:
                        cache.clear_latest_ticks()
                        cache.truncate_wal()
                        logging.info(f"[StrategyScheduler] Cache flush success: {inserted:,} records written, WAL truncated")
                    else:
                        logging.warning(f"[StrategyScheduler] Cache flush: 0 records written, cache retained")
                        
                except Exception as e:
                    if cache_ticks and cache:
                        try:
                            cache.restore_ticks(cache_ticks)
                        except Exception as restore_err:
                            logging.error(f"[StrategyScheduler] Cache restore also failed: {restore_err}, {len(cache_ticks)} ticks lost")
                    retry_info = f", {len(cache_ticks)} ticks pending retry" if cache_ticks else ""
                    logging.error(f"[StrategyScheduler] Cache flush failed: {e}{retry_info}, WAL not truncated", exc_info=True)
            
            # 添加定时任务：每5分钟检查一次
            if self._scheduler is not None and hasattr(self._scheduler, 'add_job'):
                # ✅ P0-3: 使用统一注册方法，标记为全局任务
                self.add_job_with_owner(
                    func=_flush_job,
                    trigger='interval',
                    job_id='tick_data_sync',
                    strategy_id='GLOBAL',
                    run_id=None,
                    owner_scope='global',
                    minutes=5
                )
                logging.info("[StrategyScheduler] 缓存刷写任务已添加 (每5分钟，窗口: 12:00-12:50, 15:30-20:00)")
            else:
                logging.warning("[StrategyScheduler] Scheduler not available or does not support add_job")
        except Exception as e:
            logging.error(f"[StrategyScheduler] Failed to add cache flush job: {e}", exc_info=True)
    
    def register_14_contracts_diagnosis_task(self, storage=None, query_service=None) -> None:
        """注册重点合约12环节诊断定时任务
        
        每30秒输出一次重点监控合约的12环节诊断状态，
        使用汇总输出模式避免日志被CRITICAL告警淹没。
        
        Args:
            storage: Storage实例
            query_service: QueryService实例
        """
        try:
            from ali2026v3_trading.diagnosis_service import MONITORED_CONTRACT_COUNT, run_14_contracts_periodic_diagnostic
            
            def _diagnose_job():
                try:
                    if not self._can_run_jobs():
                        return
                    if not storage:
                        logging.debug("[StrategyScheduler] Storage not available, skip monitored contracts diagnosis")
                        return
                    
                    # 执行12环节诊断
                    run_14_contracts_periodic_diagnostic(storage=storage, query_service=query_service)
                except Exception as e:
                    logging.error(f"[StrategyScheduler] monitored contracts diagnosis failed: {e}", exc_info=True)
            
            # 添加定时任务：每30秒执行一次
            if self._scheduler is not None and hasattr(self._scheduler, 'add_job'):
                # ✅ P0-3: 使用统一注册方法，标记为全局任务
                self.add_job_with_owner(
                    func=_diagnose_job,
                    trigger='interval',
                    job_id='14_contracts_diagnosis',
                    strategy_id='GLOBAL',
                    run_id=None,
                    owner_scope='global',
                    seconds=30
                )
                logging.info("[StrategyScheduler] ✅ %d合约12环节诊断任务已添加 (每30秒)", MONITORED_CONTRACT_COUNT)
            else:
                logging.warning("[StrategyScheduler] Scheduler not available or does not support add_job")
        except ImportError as e:
            logging.warning(f"[StrategyScheduler] Cannot import diagnosis_service: {e}")
        except Exception as e:
            logging.error(f"[StrategyScheduler] Failed to add monitored contracts diagnosis job: {e}", exc_info=True)
