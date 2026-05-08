"""
诊断探针管理模块 - 从 diagnosis_service.py 拆分
职责：12环节诊断探针、监控合约定义、合约格式验证
"""
from __future__ import annotations

import time
import threading
import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import deque

from ali2026v3_trading.shared_utils import normalize_instrument_id


MONITORED_CONTRACTS = {
    'IH2605': {'exchange': 'CFFEX', 'name': '上证50股指期货', 'type': 'future'},
    'IF2605': {'exchange': 'CFFEX', 'name': '沪深300股指期货', 'type': 'future'},
    'IM2605': {'exchange': 'CFFEX', 'name': '中证1000股指期货', 'type': 'future'},
    'cu2605': {'exchange': 'SHFE', 'name': '铜期货', 'type': 'future'},
    'au2605': {'exchange': 'SHFE', 'name': '黄金期货', 'type': 'future'},
    'al2605': {'exchange': 'SHFE', 'name': '铝期货', 'type': 'future'},
    'zn2605': {'exchange': 'SHFE', 'name': '锌期货', 'type': 'future'},
    'HO2605-C-2800': {'underlying': 'IH2605', 'exchange': 'CFFEX', 'name': 'IH看涨2800', 'type': 'option'},
    'IO2605-C-4200': {'underlying': 'IF2605', 'exchange': 'CFFEX', 'name': 'IF看涨4200', 'type': 'option'},
    'MO2605-C-7000': {'underlying': 'IM2605', 'exchange': 'CFFEX', 'name': 'IM看涨7000', 'type': 'option'},
    'cu2605C90000': {'underlying': 'cu2605', 'exchange': 'SHFE', 'name': '铜看涨90000', 'type': 'option'},
    'au2605C1000': {'underlying': 'au2605', 'exchange': 'SHFE', 'name': '黄金看涨1000', 'type': 'option'},
    'al2605C24000': {'underlying': 'al2605', 'exchange': 'SHFE', 'name': '铝看涨24000', 'type': 'option'},
    'zn2605C26000': {'underlying': 'zn2605', 'exchange': 'SHFE', 'name': '锌看涨26000', 'type': 'option'},
    'cu2605C104000': {'underlying': 'cu2605', 'exchange': 'SHFE', 'name': '铜看涨104000', 'type': 'option'},
    'au2605C1032': {'underlying': 'au2605', 'exchange': 'SHFE', 'name': '黄金看涨1032', 'type': 'option'},
    'al2605C22600': {'underlying': 'al2605', 'exchange': 'SHFE', 'name': '铝看涨22600', 'type': 'option'},
    'al2605C25200': {'underlying': 'al2605', 'exchange': 'SHFE', 'name': '铝看涨25200', 'type': 'option'},
    'al2605C26200': {'underlying': 'al2605', 'exchange': 'SHFE', 'name': '铝看涨26200', 'type': 'option'},
    'zn2605C20000': {'underlying': 'zn2605', 'exchange': 'SHFE', 'name': '锌看涨20000', 'type': 'option'},
}


def _load_monitored_contracts_from_config() -> Dict[str, Dict]:
    try:
        env_contracts = os.getenv('MONITORED_CONTRACTS_LIST', '').strip()
        if env_contracts:
            import json
            custom = json.loads(env_contracts)
            if isinstance(custom, dict) and custom:
                logging.info(f"Loaded {len(custom)} monitored contracts from MONITORED_CONTRACTS_LIST env")
                return custom
    except Exception as e:
        logging.warning(f"Failed to load MONITORED_CONTRACTS from env, using hardcoded fallback ({len(MONITORED_CONTRACTS)} contracts): {e}")
    logging.debug(f"Using {len(MONITORED_CONTRACTS)} hardcoded monitored contracts as fallback")
    return MONITORED_CONTRACTS


MONITORED_CONTRACTS = _load_monitored_contracts_from_config()

MONITORED_CONTRACT_SET = set(MONITORED_CONTRACTS.keys())
MONITORED_CONTRACT_COUNT = len(MONITORED_CONTRACTS)
MONITORED_DIAG_LABEL = f'{MONITORED_CONTRACT_COUNT}合约诊断'
_PARSE_TRACE_KEYS: deque = deque(maxlen=10000)
_PARSE_TRACE_LOCK = threading.RLock()

_is_monitored_cache_result: Optional[bool] = None
_is_monitored_cache_time: float = 0.0
_IS_MONITORED_CACHE_TTL = 1.0

_get_cached_params_fn = None
_get_cached_params_lock = threading.Lock()


def _get_config_cached_params():
    global _get_cached_params_fn
    if _get_cached_params_fn is None:
        with _get_cached_params_lock:
            if _get_cached_params_fn is None:
                try:
                    from ali2026v3_trading.config_service import get_cached_params
                    _get_cached_params_fn = get_cached_params
                except ImportError:
                    _get_cached_params_fn = lambda: None
                    logging.debug("is_monitored_contract: config_service not available, diagnostics disabled")
    return _get_cached_params_fn()


def is_monitored_contract(instrument_id: str) -> bool:
    global _is_monitored_cache_result, _is_monitored_cache_time
    now = time.monotonic()
    if _is_monitored_cache_result is not None and (now - _is_monitored_cache_time) < _IS_MONITORED_CACHE_TTL:
        return _is_monitored_cache_result
    try:
        cached_params = _get_config_cached_params()
        if cached_params and isinstance(cached_params, dict):
            strategy = cached_params.get('strategy')
            if strategy:
                params = getattr(strategy, 'params', None)
                if params:
                    result = bool(getattr(params, 'diagnostic_output', False))
                    _is_monitored_cache_result = result
                    _is_monitored_cache_time = now
                    return result
        _is_monitored_cache_result = False
        _is_monitored_cache_time = now
        return False
    except Exception as e:
        logging.debug(f"is_monitored_contract check failed: {e}")
        _is_monitored_cache_result = False
        _is_monitored_cache_time = now
        return False


def get_contract_info(instrument_id: str) -> Optional[Dict[str, Any]]:
    return MONITORED_CONTRACTS.get(instrument_id)


def _should_trace_parse_contract(original_id: str, transformed_id: Optional[str] = None) -> bool:
    if original_id in MONITORED_CONTRACT_SET or transformed_id in MONITORED_CONTRACT_SET:
        return True
    return bool(original_id and transformed_id and original_id != transformed_id)


def diagnose_parse_transform(stage: str, original_id: str, transformed_id: str,
                             detail: Optional[str] = None, level: str = "INFO") -> None:
    original = str(original_id or '').strip()
    transformed = str(transformed_id or '').strip()
    detail_text = str(detail).strip() if detail else None
    if not _should_trace_parse_contract(original, transformed):
        return
    trace_key = (stage, original, transformed, detail_text)
    with _PARSE_TRACE_LOCK:
        if trace_key in _PARSE_TRACE_KEYS:
            return
        _PARSE_TRACE_KEYS.append(trace_key)
    message = f"[PROBE_PARSE_TRANSFORM] Stage={stage} | Original={original} | Transformed={transformed}"
    if detail_text:
        message += f" | Detail={detail_text}"
    log_func = {
        'DEBUG': logging.debug, 'INFO': logging.info, 'WARN': logging.warning,
        'WARNING': logging.warning, 'ERROR': logging.error,
    }.get(str(level or 'INFO').upper(), logging.info)
    log_func(message)


def diagnose_parse_failure(stage: str, instrument_id: str, reason: str) -> None:
    instrument = str(instrument_id or '').strip()
    if instrument not in MONITORED_CONTRACT_SET:
        return
    logging.error("[PROBE_PARSE_FAILURE] Stage=%s | Instrument=%s | Reason=%s", stage, instrument, reason)


_CONTRACT_PATTERN = re.compile(r'^([A-Za-z]{2,4})(\d{4})(?:-([CP])-(\d+)|([CP])(\d+))?$')


def validate_contract_format(instrument_id: str) -> tuple:
    info = MONITORED_CONTRACTS.get(instrument_id)
    if not info:
        return False, "未知合约", instrument_id
    contract_type = info['type']
    if contract_type == 'future':
        match = _CONTRACT_PATTERN.match(instrument_id)
        expected = "期货标准格式: XX####"
        if match and not match.group(3) and not match.group(5):
            return True, expected, instrument_id
        return False, expected, instrument_id
    if contract_type == 'option':
        underlying = info.get('underlying', '')
        exchange = info.get('exchange', '')
        try:
            from ali2026v3_trading.subscription_manager import SubscriptionManager
            parsed = SubscriptionManager.parse_option(instrument_id)
            if parsed:
                if exchange == 'CFFEX':
                    prefix = parsed.get('product', '')
                    if underlying.startswith('IH') and prefix != 'HO':
                        return False, "股指期权应使用HO代码（IH→HO）", instrument_id
                    if underlying.startswith('IF') and prefix != 'IO':
                        return False, "股指期权应使用IO代码（IF→IO）", instrument_id
                    if underlying.startswith('IM') and prefix != 'MO':
                        return False, "股指期权应使用MO代码（IM→MO）", instrument_id
                    return True, "股指期权标准格式: XX####-C/P-#####", instrument_id
                return True, "商品期权标准格式: xx####C/P#####", instrument_id
        except (ValueError, KeyError, TypeError):
            pass
        if exchange == 'CFFEX':
            return False, "股指期权标准格式: XX####-C/P-#####", instrument_id
        return False, "商品期权标准格式: xx####C/P#####", instrument_id
    return False, "未知类型", instrument_id


def log_diagnostic_event(probe_name: str, instrument_id: str, **kwargs) -> None:
    if not is_monitored_contract(instrument_id):
        return
    info = get_contract_info(instrument_id)
    contract_type = info['type'] if info else 'unknown'
    parts = [f"[{probe_name}] Contract={instrument_id} | Type={contract_type}"]
    for key, value in kwargs.items():
        parts.append(f"{key}={value}")
    message = " | ".join(parts)
    if 'FAILED' in probe_name or 'ERROR' in probe_name:
        logging.error(message)
    elif 'WARNING' in probe_name:
        logging.warning(message)
    else:
        logging.info(message)


def diagnose_format_validation(instrument_id: str, is_valid_format: bool,
                              expected_format: Optional[str] = None,
                              actual_format: Optional[str] = None) -> None:
    if not is_monitored_contract(instrument_id):
        return
    status = "✅" if is_valid_format else "❌"
    if is_valid_format:
        logging.debug(f"[PROBE_FORMAT_VALIDATION] {instrument_id} | Format={status}")
    else:
        msg = f"[PROBE_FORMAT_VALIDATION_FAILED] {instrument_id} | Format={status}"
        if expected_format and actual_format:
            msg += f" | Expected={expected_format} | Actual={actual_format}"
        logging.error(msg)


def diagnose_config_file_presence(instrument_id: str, exists_in_file: bool) -> None:
    if not is_monitored_contract(instrument_id):
        return
    status = "✅" if exists_in_file else "❌"
    logging.info(f"[PROBE_CONFIG_FILE] {instrument_id} | InFile={status}")


def diagnose_config_loaded_status(instrument_id: str, loaded_in_memory: bool,
                                 in_cache: bool = False) -> None:
    if not is_monitored_contract(instrument_id):
        return
    mem_status = "✅" if loaded_in_memory else "❌"
    cache_status = "✅" if in_cache else "❌"
    logging.info(f"[PROBE_CONFIG_LOADED] {instrument_id} | InMemory={mem_status} | InCache={cache_status}")


def diagnose_pre_registration(instrument_id: str, in_subscribe_list: bool,
                              registered: bool) -> None:
    if not is_monitored_contract(instrument_id):
        return
    sub_status = "✅" if in_subscribe_list else "❌"
    reg_status = "✅" if registered else "❌"
    logging.info(f"[PROBE_PRE_REGISTRATION] {instrument_id} | InSubList={sub_status} | Registered={reg_status}")


def diagnose_subscription(instrument_id: str, contract_type: str, success: bool, reason: str = None) -> None:
    if not is_monitored_contract(instrument_id):
        return
    status = "✅" if success else "❌"
    msg = f"[PROBE_SUBSCRIPTION] {instrument_id} | Type={contract_type} | Success={status}"
    if reason:
        msg += f" | Reason={reason}"
    if success:
        logging.debug(msg)
    else:
        logging.warning(msg)


def diagnose_registration(instrument_id: str, source: str, internal_id: int, instrument_type: str) -> None:
    if not is_monitored_contract(instrument_id):
        return
    logging.debug(f"[PROBE_REGISTRATION] {instrument_id} | Source={source} | ID={internal_id} | Type={instrument_type}")


def diagnose_id_lookup(instrument_id: str, internal_id: int, instrument_type: str) -> None:
    if not is_monitored_contract(instrument_id):
        return
    logging.debug(f"[PROBE_ID_LOOKUP] {instrument_id} | ID={internal_id} | Type={instrument_type}")


_TICK_ENTRY_SUMMARY_INTERVAL = 60.0
_tick_entry_last_output_time = 0.0
_tick_entry_accum = {}
_tick_entry_accum_lock = threading.Lock()


def diagnose_tick_entry(instrument_id: str, price: float, volume: int,
                        open_interest: float, contract_type: str) -> None:
    global _tick_entry_last_output_time
    if not is_monitored_contract(instrument_id):
        return
    now = time.time()
    with _tick_entry_accum_lock:
        if instrument_id not in _tick_entry_accum:
            _tick_entry_accum[instrument_id] = {'count': 0, 'last_price': None, 'last_oi': None}
        _tick_entry_accum[instrument_id]['count'] += 1
        _tick_entry_accum[instrument_id]['last_price'] = price
        _tick_entry_accum[instrument_id]['last_oi'] = open_interest
        if now - _tick_entry_last_output_time >= _TICK_ENTRY_SUMMARY_INTERVAL:
            for iid, accum in _tick_entry_accum.items():
                logging.info(
                    "[PROBE_TICK_ENTRY] %s | Count=%d | LastPrice=%s | LastOI=%s | Type=%s",
                    iid, accum['count'], accum['last_price'], accum['last_oi'], contract_type,
                )
            _tick_entry_accum.clear()
            _tick_entry_last_output_time = now


def diagnose_tick_buffer(instrument_id: str, buffered: bool) -> None:
    if not is_monitored_contract(instrument_id):
        return
    logging.debug(f"[PROBE_TICK_BUFFER] {instrument_id} | Buffered={'✅' if buffered else '❌'}")


def diagnose_tick_flush(instrument_id: str, flushed: bool) -> None:
    if not is_monitored_contract(instrument_id):
        return
    logging.debug(f"[PROBE_TICK_FLUSH] {instrument_id} | Flushed={'✅' if flushed else '❌'}")


def diagnose_storage_enqueue(instrument_id: str, success: bool, reason: str = None) -> None:
    if not is_monitored_contract(instrument_id):
        return
    status = "✅" if success else "❌"
    msg = f"[PROBE_STORAGE_ENQUEUE] {instrument_id} | Success={status}"
    if reason:
        msg += f" | Reason={reason}"
    logging.debug(msg)


def diagnose_async_write(instrument_id: str, func_name: str, data_count: int) -> None:
    if not is_monitored_contract(instrument_id):
        return
    logging.debug(f"[PROBE_ASYNC_WRITE] {instrument_id} | Func={func_name} | Count={data_count}")


def diagnose_duckdb_insert(instrument_id: str, success: bool, reason: str = None) -> None:
    if not is_monitored_contract(instrument_id):
        return
    status = "✅" if success else "❌"
    msg = f"[PROBE_DUCKDB_INSERT] {instrument_id} | Success={status}"
    if reason:
        msg += f" | Reason={reason}"
    logging.debug(msg)


class DiagnosisProbeManager:
    """12环节诊断探针统一管理器"""

    _shard_enqueue_counts: Dict[int, int] = {}
    _shard_enqueue_lock = threading.Lock()

    @staticmethod
    def on_subscribe(instrument_id: str, contract_type: str, success: bool, reason: str = None):
        diagnose_subscription(instrument_id, contract_type, success, reason)

    @staticmethod
    def on_register(instrument_id: str, source: str, internal_id: int, instrument_type: str):
        diagnose_registration(instrument_id, source, internal_id, instrument_type)
        diagnose_id_lookup(instrument_id, internal_id, instrument_type)

    @staticmethod
    def on_tick_entry(instrument_id: str, price: float, volume: int, open_interest: float, contract_type: str):
        diagnose_tick_entry(instrument_id, price, volume, open_interest, contract_type)
        DiagnosisProbeManager.record_contract_tick(instrument_id, price, volume, open_interest, contract_type)

    @staticmethod
    def on_storage_enqueue(instrument_id: str, success: bool, reason: str = None, shard_idx: int = -1):
        diagnose_storage_enqueue(instrument_id, success, reason)
        if shard_idx >= 0:
            with DiagnosisProbeManager._shard_enqueue_lock:
                DiagnosisProbeManager._shard_enqueue_counts[shard_idx] = DiagnosisProbeManager._shard_enqueue_counts.get(shard_idx, 0) + 1

    @staticmethod
    def on_async_write(instrument_id: str, func_name: str, data_count: int):
        diagnose_async_write(instrument_id, func_name, data_count)

    @staticmethod
    def on_parse_transform(stage: str, original_id: str, transformed_id: str,
                           detail: str = None, level: str = 'INFO'):
        diagnose_parse_transform(stage, original_id, transformed_id, detail, level)

    @staticmethod
    def on_parse_failure(stage: str, instrument_id: str, reason: str):
        diagnose_parse_failure(stage, instrument_id, reason)

    _startup_lock = threading.RLock()
    _startup_seq = 0
    _startup_run: Optional[Dict[str, Any]] = None
    _contract_watch_lock = threading.RLock()
    _contract_watch_run: Optional[Dict[str, Any]] = None
    _contract_watch_thread: Optional[threading.Thread] = None
    _contract_watch_stop = threading.Event()

    @classmethod
    def start_contract_watch(cls, subscribed_instruments=None, timeout_seconds=30.0,
                             summary_interval_seconds=10.0, label=MONITORED_DIAG_LABEL):
        watch_targets = []
        subscribed_set = set(str(item).strip() for item in (subscribed_instruments or []) if str(item).strip())
        for instrument_id, info in MONITORED_CONTRACTS.items():
            watch_targets.append({
                'instrument_id': instrument_id, 'exchange': info.get('exchange', ''),
                'type': info.get('type', 'unknown'), 'underlying': info.get('underlying', ''),
                'name': info.get('name', instrument_id), 'in_subscribe_list': instrument_id in subscribed_set,
                'watch_started_at': time.time(), 'first_tick_at': None, 'last_tick_at': None,
                'first_price': None, 'last_price': None, 'tick_count': 0,
            })
        run = {
            'run_id': f"contract-watch-{int(time.time())}", 'label': label,
            'started_at': time.time(), 'started_wall': datetime.now().isoformat(),
            'timeout_seconds': float(timeout_seconds),
            'summary_interval_seconds': max(1.0, float(summary_interval_seconds)),
            'contracts': {item['instrument_id']: item for item in watch_targets},
            'finished': False,
        }
        with cls._contract_watch_lock:
            cls._contract_watch_run = run
            cls._contract_watch_stop.clear()
        logging.info("=" * 80)
        logging.info("[ContractWatch] 启动 %s | run=%s | total=%d | in_subscribe_list=%d | timeout=%.1fs | interval=%.1fs",
                     label, run['run_id'], len(run['contracts']),
                     sum(1 for item in watch_targets if item['in_subscribe_list']),
                     run['timeout_seconds'], run['summary_interval_seconds'])
        for item in watch_targets:
            logging.info("[ContractWatch] target=%s | type=%s | exchange=%s | underlying=%s | in_subscribe_list=%s | name=%s",
                         item['instrument_id'], item['type'], item['exchange'],
                         item['underlying'] or '-', 'Y' if item['in_subscribe_list'] else 'N', item['name'])
        logging.info("=" * 80)

        def _watch_loop():
            last_emit = 0.0
            while not cls._contract_watch_stop.wait(1.0):
                current_run = cls._get_contract_watch_run()
                if current_run is None:
                    return
                now = time.time()
                if now - last_emit >= current_run['summary_interval_seconds']:
                    cls.emit_contract_watch_summary(reason='periodic')
                    last_emit = now
                if now - current_run['started_at'] >= current_run['timeout_seconds']:
                    cls.emit_contract_watch_summary(reason='timeout', final=True)
                    return

        thread = threading.Thread(target=_watch_loop, name='ContractWatchSummary', daemon=True)
        with cls._contract_watch_lock:
            cls._contract_watch_thread = thread
        thread.start()
        logging.info("[ContractWatch] START_CONFIRMED run=%s thread=%s alive=%s timeout=%.1fs interval=%.1fs subscribed_targets=%d total_targets=%d",
                     run['run_id'], thread.name, thread.is_alive(), run['timeout_seconds'],
                     run['summary_interval_seconds'],
                     sum(1 for item in watch_targets if item['in_subscribe_list']), len(watch_targets))
        return run

    @classmethod
    def _get_contract_watch_run(cls):
        with cls._contract_watch_lock:
            return cls._contract_watch_run

    @classmethod
    def record_contract_tick(cls, instrument_id, price, volume, open_interest, contract_type):
        instrument = str(instrument_id or '').strip()
        if not instrument:
            return
        with cls._contract_watch_lock:
            run = cls._contract_watch_run
            if run is None or run.get('finished'):
                return
            state = run['contracts'].get(instrument)
            if state is None:
                return
            now = time.time()
            first_tick = state['first_tick_at'] is None
            state['tick_count'] += 1
            state['last_tick_at'] = now
            state['last_price'] = price
            if first_tick:
                state['first_tick_at'] = now
                state['first_price'] = price
        if first_tick:
            elapsed = now - run['started_at']
            logging.info("[ContractWatch] FIRST_TICK run=%s contract=%s type=%s price=%s vol=%s oi=%s elapsed=%.3fs",
                         run['run_id'], instrument, contract_type, price, volume, open_interest, elapsed)

    @classmethod
    def emit_contract_watch_summary(cls, reason: str, final: bool = False):
        with cls._contract_watch_lock:
            run = cls._contract_watch_run
            if run is None:
                return
            now = time.time()
            total = len(run['contracts'])
            in_subscribe = [item for item in run['contracts'].values() if item['in_subscribe_list']]
            first_tick = [item for item in run['contracts'].values() if item['first_tick_at'] is not None]
            no_tick = [item for item in run['contracts'].values() if item['in_subscribe_list'] and item['first_tick_at'] is None]
            not_subscribed = [item for item in run['contracts'].values() if not item['in_subscribe_list']]
            elapsed = now - run['started_at']
            if final:
                run['finished'] = True
                cls._contract_watch_stop.set()
        logging.info("-" * 80)
        logging.info("[ContractWatch] summary run=%s reason=%s final=%s elapsed=%.3fs total=%d in_subscribe=%d first_tick=%d no_tick=%d not_subscribed=%d",
                     run['run_id'], reason, final, elapsed, total, len(in_subscribe), len(first_tick), len(no_tick), len(not_subscribed))
        for item in sorted(first_tick, key=lambda c: c['first_tick_at'] or 0.0):
            first_elapsed = (item['first_tick_at'] or now) - run['started_at']
            last_elapsed = (item['last_tick_at'] or now) - run['started_at']
            logging.info("[ContractWatch] OK contract=%s type=%s ticks=%d first=%.3fs last=%.3fs first_price=%s last_price=%s",
                         item['instrument_id'], item['type'], item['tick_count'], first_elapsed, last_elapsed, item['first_price'], item['last_price'])
        for item in no_tick:
            logging.warning("[ContractWatch] NO_TICK contract=%s type=%s waited=%.3fs name=%s",
                            item['instrument_id'], item['type'], elapsed, item['name'])
        for item in not_subscribed:
            logging.warning("[ContractWatch] NOT_IN_SUBSCRIBE_LIST contract=%s type=%s name=%s",
                            item['instrument_id'], item['type'], item['name'])
        logging.info("-" * 80)

    @classmethod
    def stop_contract_watch(cls, reason: str = 'stop'):
        run = cls._get_contract_watch_run()
        if run is None:
            logging.warning("[ContractWatch] STOP_REQUEST reason=%s but no active run", reason)
            return
        logging.info("[ContractWatch] STOP_REQUEST run=%s reason=%s finished=%s",
                     run.get('run_id', '-'), reason, run.get('finished', False))
        cls.emit_contract_watch_summary(reason=reason, final=True)

    @classmethod
    def begin_startup_timeline(cls, source: str, strategy_id: str = None, reset: bool = False) -> str:
        with cls._startup_lock:
            run = cls._startup_run
            if reset or run is None or run.get('finished'):
                cls._startup_seq += 1
                run = {'run_id': f"startup-{cls._startup_seq}", 'source': source,
                       'strategy_id': strategy_id or '', 'started_at': time.perf_counter(),
                       'started_wall': datetime.now().isoformat(), 'spans': [], 'events': [], 'finished': False}
                cls._startup_run = run
                logging.info("[StartupProbe] begin run=%s source=%s strategy_id=%s", run['run_id'], source, strategy_id or '-')
            else:
                logging.info("[StartupProbe] resume run=%s source=%s strategy_id=%s", run['run_id'], source, run.get('strategy_id') or '-')
        cls.mark_startup_event(f"timeline.begin:{source}")
        return run['run_id']

    @classmethod
    def _require_startup_run(cls):
        with cls._startup_lock:
            return cls._startup_run

    @classmethod
    def mark_startup_event(cls, name: str, detail: str = None):
        run = cls._require_startup_run()
        if run is None:
            return
        elapsed = time.perf_counter() - run['started_at']
        event = {'name': name, 'detail': detail or '', 'elapsed': elapsed, 'wall_time': datetime.now().isoformat()}
        with cls._startup_lock:
            current = cls._startup_run
            if current is None:
                return
            current['events'].append(event)
        if detail:
            logging.info("[StartupProbe] event run=%s t=%.3fs name=%s detail=%s", run['run_id'], elapsed, name, detail)
        else:
            logging.info("[StartupProbe] event run=%s t=%.3fs name=%s", run['run_id'], elapsed, name)

    @classmethod
    @contextmanager
    def startup_step(cls, name: str, detail: str = None):
        run = cls._require_startup_run()
        if run is None:
            yield
            return
        step_start = time.perf_counter()
        exc = None
        logging.info("[StartupProbe] step_begin run=%s step=%s detail=%s", run['run_id'], name, detail or '-')
        try:
            yield
        except Exception as error:
            exc = error
            raise
        finally:
            elapsed = time.perf_counter() - step_start
            total_elapsed = time.perf_counter() - run['started_at']
            span = {'name': name, 'detail': detail or '', 'elapsed': elapsed,
                    'status': 'error' if exc is not None else 'ok',
                    'error': str(exc) if exc is not None else '',
                    'finished_at': datetime.now().isoformat(), 'total_elapsed': total_elapsed}
            with cls._startup_lock:
                current = cls._startup_run
                if current is not None:
                    current['spans'].append(span)
            logging.info("[StartupProbe] step_end run=%s step=%s status=%s elapsed=%.3fs total=%.3fs",
                         run['run_id'], name, span['status'], elapsed, total_elapsed)

    @classmethod
    def emit_startup_summary(cls, reason: str, final: bool = False, top_n: int = 15):
        with cls._startup_lock:
            run = cls._startup_run
            if run is None:
                return
            total_elapsed = time.perf_counter() - run['started_at']
            spans = list(run['spans'])
            events = list(run['events'])
            aggregate = {}
            for span in spans:
                bucket = aggregate.setdefault(span['name'], {'count': 0, 'total': 0.0, 'max': 0.0, 'errors': 0})
                bucket['count'] += 1
                bucket['total'] += float(span['elapsed'])
                bucket['max'] = max(bucket['max'], float(span['elapsed']))
                if span['status'] != 'ok':
                    bucket['errors'] += 1
            sorted_steps = sorted(aggregate.items(), key=lambda item: item[1]['total'], reverse=True)[:top_n]
            recent_events = events[-8:]
            if final:
                run['finished'] = True
        logging.info("[StartupProbe] summary run=%s reason=%s total=%.3fs spans=%d events=%d final=%s",
                     run['run_id'], reason, total_elapsed, len(spans), len(events), final)
        for index, (step_name, stats) in enumerate(sorted_steps, start=1):
            avg = stats['total'] / stats['count'] if stats['count'] else 0.0
            logging.info("[StartupProbe] top%d step=%s total=%.3fs avg=%.3fs max=%.3fs count=%d errors=%d",
                         index, step_name, stats['total'], avg, stats['max'], stats['count'], stats['errors'])
        for event in recent_events:
            if event['detail']:
                logging.info("[StartupProbe] recent_event t=%.3fs name=%s detail=%s", event['elapsed'], event['name'], event['detail'])
            else:
                logging.info("[StartupProbe] recent_event t=%.3fs name=%s", event['elapsed'], event['name'])

    @staticmethod
    def diagnose_subscribe_api_return(strategy_core, test_contracts=None):
        if test_contracts is None:
            test_contracts = [
                ('SHFE', 'al2605C25200', 'AL期权-有tick'), ('SHFE', 'au2605C1032', 'AU期权-无tick'),
                ('SHFE', 'cu2605C104000', 'CU期权-无tick'), ('SHFE', 'zn2605C20000', 'ZN期权-无tick'),
            ]
        results = {'timestamp': datetime.now().isoformat(), 'contracts': {},
                   'summary': {'total': 0, 'success': 0, 'failed': 0, 'no_return': 0}}
        logging.info("=" * 80)
        logging.info("[SubscribeReturnDiag] 开始诊断 sub_market_data 返回值")
        logging.info("=" * 80)
        sub_api = getattr(strategy_core, '_runtime_strategy_host', None)
        if sub_api is None:
            sub_api = strategy_core
        sub_method = getattr(sub_api, 'sub_market_data', None)
        if not callable(sub_method):
            logging.error("[SubscribeReturnDiag] sub_market_data 方法不可用")
            results['error'] = 'sub_market_data not callable'
            return results
        for exchange, inst_id, desc in test_contracts:
            results['summary']['total'] += 1
            contract_result = {'exchange': exchange, 'instrument_id': inst_id, 'desc': desc,
                               'return_value': None, 'return_type': None, 'exception': None, 'status': 'unknown'}
            try:
                ret = sub_method(exchange, inst_id)
                contract_result['return_value'] = str(ret)
                contract_result['return_type'] = type(ret).__name__
                if ret is None:
                    contract_result['status'] = 'no_return'
                    results['summary']['no_return'] += 1
                    logging.warning(f"[SubscribeReturnDiag] {inst_id} ({desc}): 返回值=None, 可能被忽略")
                elif ret is False:
                    contract_result['status'] = 'failed'
                    results['summary']['failed'] += 1
                    logging.error(f"[SubscribeReturnDiag] {inst_id} ({desc}): 返回值=False, 订阅失败")
                elif ret is True:
                    contract_result['status'] = 'success'
                    results['summary']['success'] += 1
                    logging.info(f"[SubscribeReturnDiag] {inst_id} ({desc}): 返回值=True, 订阅成功")
                else:
                    contract_result['status'] = 'other'
                    logging.info(f"[SubscribeReturnDiag] {inst_id} ({desc}): 返回值={ret}, 类型={type(ret).__name__}")
            except Exception as e:
                contract_result['exception'] = str(e)
                contract_result['status'] = 'exception'
                results['summary']['failed'] += 1
                logging.error(f"[SubscribeReturnDiag] {inst_id} ({desc}): 异常={e}")
            results['contracts'][inst_id] = contract_result
        logging.info("-" * 80)
        logging.info(f"[SubscribeReturnDiag] 汇总: total={results['summary']['total']}, success={results['summary']['success']}, failed={results['summary']['failed']}, no_return={results['summary']['no_return']}")
        logging.info("=" * 80)
        return results

    @staticmethod
    def diagnose_option_metadata_diff(strategy_core):
        from ali2026v3_trading.params_service import get_params_service
        ps = get_params_service()
        test_contracts = [('al2605C25200', 'AL期权-有tick'), ('au2605C1032', 'AU期权-无tick'),
                          ('cu2605C104000', 'CU期权-无tick'), ('zn2605C20000', 'ZN期权-无tick'),
                          ('HO2605-C-2800', 'HO指数期权-有tick')]
        results = {'timestamp': datetime.now().isoformat(), 'contracts': {}, 'diff_fields': {}}
        logging.info("=" * 80)
        logging.info("[OptionMetaDiff] 开始诊断期权元数据差异")
        logging.info("=" * 80)
        all_fields = set()
        for inst_id, desc in test_contracts:
            meta = ps.get_instrument_meta_by_id(inst_id)
            if meta:
                all_fields.update(meta.keys())
        key_fields = ['exchange', 'product', 'product_type', 'instrument_type', 'price_tick', 'multiplier',
                      'trading_hours', 'underlying_symbol', 'underlying_id', 'min_price_tick', 'contract_size',
                      'delivery_month', 'option_type', 'strike_price', 'expiration_date']
        key_fields = [f for f in key_fields if f in all_fields]
        logging.info(f"[OptionMetaDiff] 关键字段: {key_fields}")
        logging.info("-" * 80)
        header = f"{'合约':<20} {'描述':<15}"
        for f in key_fields[:8]:
            header += f" {f[:12]:<12}"
        logging.info(header)
        logging.info("-" * 120)
        for inst_id, desc in test_contracts:
            meta = ps.get_instrument_meta_by_id(inst_id)
            row = f"{inst_id:<20} {desc:<15}"
            for f in key_fields[:8]:
                v = meta.get(f, 'N/A') if meta else 'N/A'
                row += f" {str(v)[:12]:<12}"
            logging.info(row)
            results['contracts'][inst_id] = {'desc': desc, 'meta': meta, 'found': meta is not None}
        logging.info("=" * 80)
        return results
