"""
scheduler_manager_proxy.py - SchedulerManagerProxy
Phase 2 (CC-02+CC-04): 从_LifecycleMixin提取的调度器管理Proxy

职责：
- 调度器初始化/停止代理（_init_scheduler, _stop_scheduler）
- 定时任务注册（_add_option_status_diagnosis_job, _add_tick_sync_job等）
- 检查待定订单任务（_ensure_check_pending_orders_job）

注意：调度器核心逻辑已在StrategyScheduler类中，本Proxy仅做方法代理和参数适配。

Provider接口：
- provider._scheduler_manager: StrategyScheduler实例
- provider.strategy_id: 策略ID
- provider._order_service: 订单服务
"""

import logging
import os
import time
import threading
from typing import Any, Dict, Optional


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
        except Exception as e:
            logging.warning("[SchedulerManagerProxy] stop_strategy_jobs error: %s", e)
        try:
            scheduler.remove_jobs_by_owner('GLOBAL')
        except Exception as e:
            logging.warning("[SchedulerManagerProxy] remove GLOBAL jobs error: %s", e)
        try:
            scheduler.shutdown()
        except Exception as e:
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
        except Exception:
            pass
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
        except Exception as e:
            logging.error("[SchedulerManagerProxy] 补偿注册check_pending_orders job失败: %s", e)
