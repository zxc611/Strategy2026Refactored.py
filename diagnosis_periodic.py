"""
周期性诊断模块 - 从 diagnosis_service.py 拆分
职责：周期性14合约诊断、宽限期管理、资源快照、资源扫描器、控制动作日志
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Dict, List, Any, Optional, Set

from ali2026v3_trading.diagnosis_probe import (
    MONITORED_CONTRACTS, MONITORED_CONTRACT_COUNT, MONITORED_DIAG_LABEL,
    is_monitored_contract, get_contract_info, validate_contract_format,
    diagnose_pre_registration, diagnose_format_validation,
    diagnose_config_file_presence, diagnose_config_loaded_status,
    DiagnosisProbeManager,
)


def _get_runtime_state():
    runtime_strategy = None
    runtime_core = None
    runtime_subscribed: Set[str] = set()
    historical_in_progress = False
    startup_ready = False
    try:
        from ali2026v3_trading.config_service import get_cached_params
        cached_params = get_cached_params() or {}
        runtime_strategy = cached_params.get('strategy')
        runtime_core = getattr(runtime_strategy, 'strategy_core', None) if runtime_strategy is not None else None
        subscribed_source = runtime_core if runtime_core is not None else runtime_strategy
        runtime_subscribed = set(getattr(subscribed_source, '_subscribed_instruments', []) or [])
        historical_in_progress = bool(getattr(subscribed_source, '_historical_load_in_progress', False))
        startup_ready = bool(
            getattr(runtime_strategy, '_start_completed', False)
            or getattr(runtime_core, '_is_running', False)
            or runtime_subscribed
        )
    except Exception as exc:
        logging.debug(f"[{MONITORED_DIAG_LABEL}] 获取运行时状态失败: {exc}")
    return runtime_strategy, runtime_core, runtime_subscribed, historical_in_progress, startup_ready


def _emit_periodic_resource_ownership_snapshot(runtime_core) -> None:
    strategy_id = getattr(runtime_core, 'strategy_id', 'unknown') if runtime_core else 'unknown'
    run_id = getattr(runtime_core, '_lifecycle_run_id', 'N/A') if runtime_core else 'N/A'
    phase = 'periodic-diagnosis'
    allowed_prefixes = (
        'Main', 'Thread-', 'APScheduler', 'ThreadPoolExecutor',
        'Storage-AsyncWriter[shared-service]', 'Storage-Cleanup[shared-service]',
        'SubAsyncWriter[shared-service]', 'SubRetry[shared-service]', 'SubCleanup[shared-service]',
        'TTypeService-Preload[shared-service]', 'onStop-worker',
    )
    threads = threading.enumerate()
    strategy_threads = []
    shared_threads = []
    system_threads = []
    for thread_obj in threads:
        name = thread_obj.name or ''
        if '[shared-service]' in name:
            shared_threads.append(name)
        elif any(name.startswith(prefix) for prefix in allowed_prefixes):
            system_threads.append(name)
        elif 'strategy' in name.lower() or strategy_id in name:
            strategy_threads.append(name)
        elif name and not name.startswith('Main'):
            system_threads.append(name)
    logging.info(
        f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}][phase={phase}]"
        f"[source_type=resource-ownership] "
        f"Thread scan: total={len(threads)}, shared-service={len(shared_threads)}, "
        f"strategy-instance={len(strategy_threads)}, system={len(system_threads)}"
    )
    if shared_threads:
        for name in shared_threads:
            logging.info(
                f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                f"[run_id={run_id}][source_type=resource-ownership] "
                f"Thread alive: {name} (expected: continues after strategy stop)"
            )
    if strategy_threads:
        for name in strategy_threads:
            logging.warning(
                f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                f"[run_id={run_id}][source_type=resource-ownership] "
                f"⚠️ LEAKED thread: {name} (expected: should be gone after strategy stop)"
            )
    else:
        logging.info(
            f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
            f"[run_id={run_id}][source_type=resource-ownership] "
            f"✅ No strategy-instance threads leaked"
        )
    scheduler_mgr = getattr(runtime_core, '_scheduler_manager', None) if runtime_core else None
    if scheduler_mgr and hasattr(scheduler_mgr, 'get_jobs_by_owner'):
        try:
            remaining_jobs = scheduler_mgr.get_jobs_by_owner(strategy_id)
            if remaining_jobs:
                job_ids = [job['job_id'] for job in remaining_jobs]
                logging.warning(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=strategy-job] "
                    f"⚠️ LEAKED scheduler jobs: {job_ids}"
                )
            else:
                logging.info(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=strategy-job] "
                    f"✅ No strategy-instance scheduler jobs leaked"
                )
        except Exception as exc:
            logging.debug(
                f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                f"[source_type=resource-ownership] Scheduler diagnosis error: {exc}"
            )
    storage_obj = getattr(runtime_core, '_storage', None) if runtime_core else None
    if storage_obj and hasattr(storage_obj, 'get_queue_stats'):
        try:
            qstats = storage_obj.get_queue_stats()
            qsize = qstats.get('current_queue_size', 0)
            tick_fill = qstats.get('tick_fill_rate', 0)
            kline_fill = qstats.get('kline_fill_rate', 0)
            maint_fill = qstats.get('maintenance_fill_rate', 0)
            spill_cnt = qstats.get('spill_count', 0)
            replay_cnt = qstats.get('replay_count', 0)
            pending_cnt = qstats.get('pending_on_stop_data_size', 0)
            if qsize > 0:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=shared-queue-drain] "
                    f"Storage queue backlog: {qsize} tasks tick_fill={tick_fill:.1f}% kline_fill={kline_fill:.1f}% maint_fill={maint_fill:.1f}%"
                )
            else:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=shared-queue-drain] "
                    f"✅ Storage queue empty"
                )
            shard_backlog_parts = []
            for k, v in sorted(qstats.items()):
                if k.startswith('tick_shard_') and k.endswith('_size') and v > 0:
                    si = k.replace('tick_shard_', '').replace('_size', '')
                    fill_key = f'tick_shard_{si}_fill'
                    fill_val = qstats.get(fill_key, 0)
                    shard_backlog_parts.append(f"shard-{si}={v}({fill_val:.1f}%)")
            if shard_backlog_parts:
                logging.info(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=per-shard-backlog] {', '.join(shard_backlog_parts)}"
                )
            if spill_cnt > 0 or pending_cnt > 0:
                logging.info(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=spill-replay] spill=%d replay=%d pending=%d",
                    spill_cnt, replay_cnt, pending_cnt
                )
        except Exception as exc:
            logging.debug(
                f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                f"[source_type=resource-ownership] Storage queue diagnosis error: {exc}"
            )
    event_bus = getattr(runtime_core, '_event_bus', None) if runtime_core else None
    if event_bus and hasattr(event_bus, '_pending_events'):
        try:
            pending = getattr(event_bus, '_pending_events', 0)
            if pending > 0:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=event-tail] "
                    f"EventBus pending callbacks: {pending} (expected: drain in progress)"
                )
            else:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=event-tail] "
                    f"✅ EventBus pending callbacks empty"
                )
        except Exception as exc:
            logging.debug(
                f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                f"[source_type=resource-ownership] EventBus diagnosis error: {exc}"
            )


_DIAGNOSIS_STARTUP_GRACE_SECONDS = 60.0
_first_diagnosis_call_time: Optional[float] = None
_first_diagnosis_call_lock = threading.Lock()


def _ensure_grace_period_started() -> float:
    global _first_diagnosis_call_time
    now = time.time()
    with _first_diagnosis_call_lock:
        if _first_diagnosis_call_time is None:
            _first_diagnosis_call_time = now
            logging.info(
                "[%s] 诊断宽限期开始计时，宽限期=%.0fs，诊断将在%.0fs后启动",
                MONITORED_DIAG_LABEL, _DIAGNOSIS_STARTUP_GRACE_SECONDS,
                _DIAGNOSIS_STARTUP_GRACE_SECONDS,
            )
        elapsed = now - _first_diagnosis_call_time
    return elapsed


def reset_diagnosis_grace_period():
    global _first_diagnosis_call_time
    with _first_diagnosis_call_lock:
        _first_diagnosis_call_time = None
    logging.info("[%s] 诊断宽限期已重置", MONITORED_DIAG_LABEL)


def run_14_contracts_periodic_diagnostic(storage=None, query_service=None) -> None:
    logging.info("\n" + "="*100)
    logging.info("📊 [%s] 开始检查", MONITORED_DIAG_LABEL)
    logging.info("="*100)

    runtime_strategy, runtime_core, runtime_subscribed, historical_in_progress, startup_ready = _get_runtime_state()

    if historical_in_progress:
        logging.info(
            "[%s] 历史K线加载进行中，跳过本轮以避免启动期误报: subscribed=%d" % MONITORED_DIAG_LABEL,
            len(runtime_subscribed),
        )
        logging.info("="*100)
        return

    if runtime_strategy is not None and not startup_ready:
        logging.info("[%s] 启动桥接尚未完成，跳过本轮以避免初始化误报", MONITORED_DIAG_LABEL)
        logging.info("="*100)
        return

    _emit_periodic_resource_ownership_snapshot(runtime_core)

    runtime_strategy, runtime_core, runtime_subscribed, historical_in_progress, startup_ready = _get_runtime_state()

    if historical_in_progress:
        logging.info(
            "[%s] 历史K线加载进行中，跳过本轮以避免启动期误报: subscribed=%d" % MONITORED_DIAG_LABEL,
            len(runtime_subscribed),
        )
        logging.info("="*100)
        return

    if runtime_strategy is not None and not startup_ready:
        logging.info("[%s] 启动桥接尚未完成，跳过本轮以避免初始化误报", MONITORED_DIAG_LABEL)
        logging.info("="*100)
        return

    grace_elapsed = _ensure_grace_period_started()
    if grace_elapsed < _DIAGNOSIS_STARTUP_GRACE_SECONDS:
        logging.info(
            "[%s] 诊断宽限期未过(已过%.0fs/%.0fs)，跳过本轮以避免启动期误报",
            MONITORED_DIAG_LABEL, grace_elapsed, _DIAGNOSIS_STARTUP_GRACE_SECONDS,
        )
        logging.info("="*100)
        return
    
    success_contracts = []
    failed_contracts = []
    
    for contract, info in MONITORED_CONTRACTS.items():
        try:
            exists_in_file = False
            loaded_in_memory = False
            in_subscribe_list = False
            is_valid_format = False
            expected_format = None
            
            try:
                import os
                config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ali2026v3_trading')
                config_lines = []
                for cfg_name in ['subscription_futures_fixed.txt', 'subscription_options_fixed.txt']:
                    cfg_path = os.path.join(config_dir, cfg_name)
                    if os.path.exists(cfg_path):
                        with open(cfg_path, 'r', encoding='utf-8') as f:
                            config_lines.extend(l.strip() for l in f if l.strip() and not l.startswith('#'))
                exists_in_file = any(
                    contract in line or line.endswith('.' + contract)
                    for line in config_lines
                )
            except Exception as e:
                logging.debug(f"Diagnosis: config file check failed: {e}")
            
            try:
                from ali2026v3_trading.config_service import get_cached_params
                cached_params = get_cached_params() or {}
                if isinstance(cached_params, dict):
                    loaded_in_memory = contract in cached_params
                else:
                    loaded_in_memory = hasattr(cached_params, contract)
            except Exception as e:
                logging.debug(f"Diagnosis: memory load check failed: {e}")
            
            in_subscribe_list = contract in runtime_subscribed
            
            is_valid_format, expected_format, _ = validate_contract_format(contract)
            
            diagnose_config_file_presence(contract, exists_in_file)
            diagnose_config_loaded_status(contract, loaded_in_memory)
            diagnose_format_validation(contract, is_valid_format, expected_format, contract)
            
            subscribed = False
            registered = False
            has_internal_id = False
            internal_id = None
            tick_entry_count = 0
            tick_buffered = False
            storage_enqueued = False
            async_written = False
            duckdb_inserted = False
            tick_count = 0
            kline_count = 0
            
            if runtime_core is not None:
                sub_mgr = getattr(runtime_core, '_subscription_manager', None)
                if sub_mgr:
                    subscribed = bool(sub_mgr.is_subscribed(contract))
                    reg_info = sub_mgr.get_registration_info(contract)
                    if reg_info:
                        registered = True
                        internal_id = reg_info.get('internal_id')
                        has_internal_id = internal_id is not None
            
            if runtime_strategy is not None:
                reg_info2 = getattr(runtime_strategy, '_option_info', {}).get(contract)
                if reg_info2 and isinstance(reg_info2, dict):
                    if not registered:
                        registered = True
                    if not has_internal_id:
                        internal_id = reg_info2.get('internal_id')
                        has_internal_id = internal_id is not None
            
            if query_service:
                try:
                    tick_count = query_service.get_tick_count(contract)
                    kline_count = query_service.get_kline_count(contract)
                except Exception as e:
                    logging.debug(f"Diagnosis: query_service check failed: {e}")
            
            if storage:
                try:
                    if hasattr(storage, '_tick_shard_queues'):
                        for q in storage._tick_shard_queues:
                            try:
                                for item in list(q.queue)[:100]:
                                    if isinstance(item, dict):
                                        if item.get('instrument_id') == contract:
                                            storage_enqueued = True
                                            break
                                    elif isinstance(item, (list, tuple)) and len(item) > 0:
                                        args = item
                                        if len(args) > 0 and str(args[0]) == contract:
                                            storage_enqueued = True
                                            break
                                        if len(args) > 0:
                                            try:
                                                internal_id = args[0]
                                                info_by_id = storage._get_info_by_id(int(internal_id))
                                                if info_by_id and info_by_id.get('instrument_id') == contract:
                                                    storage_enqueued = True
                                                    break
                                            except Exception as e:
                                                logging.debug(f"Diagnosis: storage enqueue check failed: {e}")
                                if storage_enqueued:
                                    break
                            except Exception as e:
                                logging.debug(f"Diagnosis: queue inspection failed: {e}")
                except Exception as e:
                    logging.debug(f"Diagnosis: async write status check failed: {e}")
                
                if query_service:
                    try:
                        kline_count = query_service.get_kline_count(contract)
                        tick_count = query_service.get_tick_count(contract)
                        duckdb_inserted = (kline_count > 0 or tick_count > 0)
                    except Exception as e:
                        pass
            
            tick_source = runtime_core if runtime_core is not None else runtime_strategy
            if tick_source and hasattr(tick_source, '_tick_buffer'):
                try:
                    buf = tick_source._tick_buffer
                    if isinstance(buf, list):
                        tick_entry_count = sum(1 for t in buf if isinstance(t, dict) and t.get('instrument_id') == contract)
                        tick_buffered = (tick_entry_count > 0)
                except Exception as e:
                    logging.debug(f"Diagnosis: tick buffer check failed: {e}")
            
            if storage and hasattr(storage, '_queue_stats'):
                try:
                    async_written = storage._queue_stats.get('total_written', 0) > 0
                except Exception as e:
                    logging.debug(f"Diagnosis: async write stats check failed: {e}")
            
            diagnose_pre_registration(contract, in_subscribe_list, registered)
            
            has_tick_data = tick_count > 0
            has_kline_data = kline_count > 0
            is_success = registered and has_internal_id
            
            if is_success:
                success_contracts.append(contract)
            else:
                failure_reasons = []
                if not exists_in_file:
                    failure_reasons.insert(0, "❌ 不在配置文件中")
                elif not loaded_in_memory:
                    failure_reasons.insert(0, "❌ 配置表未加载到内存")
                elif not in_subscribe_list:
                    failure_reasons.insert(0, "❌ 未加入订阅列表")
                elif not is_valid_format:
                    failure_reasons.insert(0, f"❌ 格式错误: {expected_format}")
                elif in_subscribe_list and not registered:
                    failure_reasons.insert(0, "⚠️ 在订阅列表但未注册")
                elif not registered:
                    failure_reasons.insert(0, "❌ 合约未注册")
                else:
                    if not has_tick_data:
                        failure_reasons.append("⚠️ 无Tick数据(实时链路异常)")
                    if not has_kline_data:
                        if has_tick_data:
                            failure_reasons.append("⚠️ 无K线数据(Tick已入库但K线未合成)")
                        else:
                            failure_reasons.append("⚠️ 无K线数据(无数据源)")
                
                failed_contracts.append({
                    'contract': contract,
                    'exists_in_file': exists_in_file, 'loaded_in_memory': loaded_in_memory,
                    'in_subscribe_list': in_subscribe_list, 'is_valid_format': is_valid_format,
                    'subscribed': subscribed, 'registered': bool(registered),
                    'has_internal_id': has_internal_id, 'tick_entry_count': tick_entry_count,
                    'tick_buffered': tick_buffered, 'storage_enqueued': storage_enqueued,
                    'async_written': async_written, 'duckdb_inserted': duckdb_inserted,
                    'has_kline_data': has_kline_data, 'has_tick_data': has_tick_data,
                    'internal_id': str(internal_id), 'kline_count': int(kline_count),
                    'tick_count': int(tick_count), 'reasons': failure_reasons
                })
                    
        except Exception as e:
            failed_contracts.append({
                'contract': contract,
                'exists_in_file': False, 'loaded_in_memory': False,
                'in_subscribe_list': False, 'is_valid_format': False,
                'subscribed': False, 'registered': False,
                'has_internal_id': False, 'tick_entry_count': 0,
                'tick_buffered': False, 'storage_enqueued': False,
                'async_written': False, 'duckdb_inserted': False,
                'has_kline_data': False, 'has_tick_data': False,
                'internal_id': 'N/A', 'kline_count': 0, 'tick_count': 0,
                'reasons': [f"❌ 异常: {str(e)}"]
            })
    
    if success_contracts:
        logging.info(f"✅ 正常合约 ({len(success_contracts)}/{len(MONITORED_CONTRACTS)}):")
        futures = [c for c in success_contracts if MONITORED_CONTRACTS[c]['type'] == 'future']
        options = [c for c in success_contracts if MONITORED_CONTRACTS[c]['type'] == 'option']
        if futures:
            logging.info(f"  期货: {', '.join(futures)}")
        if options:
            logging.info(f"  期权: {', '.join(options)}")
    
    if failed_contracts:
        logging.info(f"\n❌ 异常合约 ({len(failed_contracts)}/{len(MONITORED_CONTRACTS)}):")
        reason_stats = {}
        for fail_info in failed_contracts:
            reasons = fail_info['reasons']
            primary_reason = reasons[0] if reasons else "未知原因"
            if primary_reason not in reason_stats:
                reason_stats[primary_reason] = []
            reason_stats[primary_reason].append(fail_info['contract'])
        for reason, contracts in reason_stats.items():
            logging.info(f"  {reason} ({len(contracts)}个): {', '.join(contracts[:5])}{'...' if len(contracts) > 5 else ''}")
        logging.info(f"\n  [{MONITORED_CONTRACT_COUNT}Diagnosis] 📊 12环节状态汇总:")
        环节_stats = {
            '1-Config': sum(1 for f in failed_contracts if not f.get('exists_in_file', False)),
            '2-Memory': sum(1 for f in failed_contracts if not f.get('loaded_in_memory', False)),
            '3-Subscribe': sum(1 for f in failed_contracts if not f.get('in_subscribe_list', False)),
            '4-Format': sum(1 for f in failed_contracts if not f.get('is_valid_format', False)),
            '5-Subscribed': sum(1 for f in failed_contracts if not f.get('subscribed', False)),
            '6-Registered': sum(1 for f in failed_contracts if not f.get('registered', False)),
            '7-ID': sum(1 for f in failed_contracts if not f.get('has_internal_id', False)),
            '8-TickEntry': sum(1 for f in failed_contracts if f.get('tick_entry_count', 0) == 0),
            '9-TickBuffer': sum(1 for f in failed_contracts if not f.get('tick_buffered', False)),
            '10-Enqueue': sum(1 for f in failed_contracts if not f.get('storage_enqueued', False)),
            '11-AsyncWrite': sum(1 for f in failed_contracts if not f.get('async_written', False)),
            '12-DuckDB': sum(1 for f in failed_contracts if not f.get('duckdb_inserted', False)),
        }
        for stage_name, fail_count in 环节_stats.items():
            if fail_count > 0:
                status = '❌' if fail_count == len(failed_contracts) else '⚠️'
                logging.info(f"    {stage_name:15s}: {fail_count}/{len(failed_contracts)} {status}")
    
    total = len(MONITORED_CONTRACTS)
    success_rate = (len(success_contracts) / total * 100) if total > 0 else 0
    logging.info(f"\n📈 诊断结果: {len(success_contracts)}/{total} 正常 ({success_rate:.1f}%)")
    
    if storage and hasattr(storage, '_tick_shard_queues'):
        try:
            tick_total = sum(q.qsize() for q in storage._tick_shard_queues)
            tick_max = sum(q.maxsize for q in storage._tick_shard_queues)
            kline_size = storage._kline_queue.qsize() if hasattr(storage, '_kline_queue') else 0
            kline_max = storage._kline_queue.maxsize if hasattr(storage, '_kline_queue') else 1
            maint_size = storage._maintenance_queue.qsize() if hasattr(storage, '_maintenance_queue') else 0
            total_size = tick_total + kline_size + maint_size
            total_max = tick_max + kline_max + (storage._maintenance_queue.maxsize if hasattr(storage, '_maintenance_queue') else 0)
            usage_rate = (total_size / total_max * 100) if total_max > 0 else 0
            if usage_rate > 90:
                logging.critical(f"[QUEUE_CRITICAL] 写入队列使用率超过90%: {usage_rate:.1f}% ({total_size}/{total_max})")
            elif usage_rate > 80:
                logging.warning(f"[QUEUE_WARNING] 写入队列使用率超过80%: {usage_rate:.1f}% ({total_size}/{total_max})")
            elif usage_rate > 70:
                logging.info(f"[QUEUE_INFO] 写入队列使用率: {usage_rate:.1f}% ({total_size}/{total_max})")
            for si in range(len(storage._tick_shard_queues)):
                q = storage._tick_shard_queues[si]
                sz = q.qsize()
                if sz > 0:
                    fill = sz / q.maxsize * 100 if q.maxsize > 0 else 0
                    logging.info(f"  TickShard-{si}: {sz}/{q.maxsize} ({fill:.1f}%)")
            if hasattr(storage, 'get_queue_stats'):
                stats = storage.get_queue_stats()
                logging.info(f"队列统计: 接收={stats.get('total_received', 0)}, 写入={stats.get('total_written', 0)}, 丢弃={stats.get('drops_count', 0)}, 峰值={stats.get('max_queue_size_seen', 0)}")
        except Exception as e:
            logging.debug(f"[QUEUE_CHECK] 队列状态检查失败: {e}")
    
    logging.info("="*100)


class ResourceOwnershipScanner:
    """资源所有权诊断扫描器"""

    @staticmethod
    def log_resource_ownership_table(strategy_core, phase: str = 'unknown') -> None:
        import threading as _threading
        strategy_id = getattr(strategy_core, 'strategy_id', 'unknown')
        run_id = getattr(strategy_core, '_lifecycle_run_id', 'N/A')
        ALLOWED_PREFIXES = (
            'Main', 'Thread-', 'APScheduler', 'ThreadPoolExecutor',
            'Storage-AsyncWriter[shared-service]', 'Storage-Cleanup[shared-service]',
            'SubAsyncWriter[shared-service]', 'SubRetry[shared-service]', 'SubCleanup[shared-service]',
            'TTypeService-Preload[shared-service]', 'onStop-worker',
        )
        threads = _threading.enumerate()
        strategy_threads = []
        shared_threads = []
        system_threads = []
        for t in threads:
            name = t.name or ''
            if '[shared-service]' in name:
                shared_threads.append(name)
            elif any(name.startswith(p) for p in ALLOWED_PREFIXES):
                system_threads.append(name)
            elif 'strategy' in name.lower() or strategy_id in name:
                strategy_threads.append(name)
            elif name and not name.startswith('Main'):
                system_threads.append(name)
        logging.info(
            f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}][phase={phase}]"
            f"[source_type=resource-ownership] "
            f"Thread scan: total={len(threads)}, shared-service={len(shared_threads)}, "
            f"strategy-instance={len(strategy_threads)}, system={len(system_threads)}"
        )
        if shared_threads:
            for name in shared_threads:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"Thread alive: {name} (expected: continues after strategy stop)"
                )
        if strategy_threads:
            for name in strategy_threads:
                logging.warning(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"⚠️ LEAKED thread: {name} (expected: should be gone after strategy stop)"
                )
        else:
            if phase == 'stop':
                logging.info(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"✅ No strategy-instance threads leaked"
                )
        scheduler_mgr = getattr(strategy_core, '_scheduler_manager', None)
        if scheduler_mgr and hasattr(scheduler_mgr, 'get_jobs_by_owner'):
            try:
                remaining_jobs = scheduler_mgr.get_jobs_by_owner(strategy_id)
                if remaining_jobs:
                    job_ids = [j['job_id'] for j in remaining_jobs]
                    logging.warning(
                        f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=strategy-job] "
                        f"⚠️ LEAKED scheduler jobs: {job_ids}"
                    )
                else:
                    if phase == 'stop':
                        logging.info(
                            f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                            f"[run_id={run_id}][source_type=strategy-job] "
                            f"✅ No strategy-instance scheduler jobs leaked"
                        )
            except Exception as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] Scheduler diagnosis error: {e}"
                )
        storage_obj = getattr(strategy_core, '_storage', None)
        if storage_obj and hasattr(storage_obj, 'get_queue_stats'):
            try:
                qstats = storage_obj.get_queue_stats()
                qsize = qstats.get('current_queue_size', 0)
                tick_fill = qstats.get('tick_fill_rate', 0)
                kline_fill = qstats.get('kline_fill_rate', 0)
                maint_fill = qstats.get('maintenance_fill_rate', 0)
                if qsize > 0:
                    shard_parts = []
                    for k, v in sorted(qstats.items()):
                        if k.startswith('tick_shard_') and k.endswith('_size') and v > 0:
                            si = k.replace('tick_shard_', '').replace('_size', '')
                            fl = qstats.get(f'tick_shard_{si}_fill', 0)
                            shard_parts.append(f"s{si}={v}({fl:.1f}%)")
                    shard_info = " " + " ".join(shard_parts) if shard_parts else ""
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=shared-queue-drain] "
                        f"Storage queue backlog: {qsize} tick={tick_fill:.1f}% kline={kline_fill:.1f}% maint={maint_fill:.1f}%{shard_info}"
                    )
                else:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=shared-queue-drain] "
                        f"Storage queue empty"
                    )
            except Exception as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] Storage queue diagnosis error: {e}"
                )
        event_bus = getattr(strategy_core, '_event_bus', None)
        if event_bus and hasattr(event_bus, '_pending_events'):
            try:
                pending = getattr(event_bus, '_pending_events', 0)
                if pending > 0:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=event-tail] "
                        f"EventBus pending callbacks: {pending} (expected: drain in progress)"
                    )
                else:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=event-tail] "
                        f"✅ EventBus pending callbacks empty"
                    )
            except Exception as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] EventBus diagnosis error: {e}"
                )


class ControlActionLogger:
    """控制动作日志记录器"""

    _logger = logging.getLogger("ControlActionLogger")

    @staticmethod
    def log_control_action_enter(action_type, strategy_id, run_id, source):
        ControlActionLogger._logger.info(
            f"[ControlActionLogger][owner_scope=control-action] ENTER {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}] [source={source}]"
        )

    @staticmethod
    def log_control_action_success(action_type, strategy_id, run_id, result=None, source=None):
        base_msg = (
            f"[ControlActionLogger][owner_scope=control-action] SUCCESS {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}]"
        )
        if source is not None:
            base_msg += f" [source={source}]"
        if result is not None:
            base_msg += f" [result={result}]"
        ControlActionLogger._logger.info(base_msg)

    @staticmethod
    def log_control_action_fail(action_type, strategy_id, run_id, error, source=None):
        base_msg = (
            f"[ControlActionLogger][owner_scope=control-action] FAIL {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}] [error={error}]"
        )
        if source is not None:
            base_msg += f" [source={source}]"
        ControlActionLogger._logger.error(base_msg)

    @staticmethod
    def log_control_action_blocked(action_type, strategy_id, run_id, reason, source=None):
        base_msg = (
            f"[ControlActionLogger][owner_scope=control-action] BLOCKED {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}] [reason={reason}]"
        )
        if source is not None:
            base_msg += f" [source={source}]"
        ControlActionLogger._logger.info(base_msg)
