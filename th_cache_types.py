# MODULE_ID: M1-059
"""
width_cache_types.py — 宽度缓存类型定义+辅助Mixin合并

合并说明 (2026-06-30):
- 原 width_cache_types.py: _NoOpDiagnosisProbeManager, SortEntry, 模块常量
- 原 width_cache_context_mixin.py: WidthCacheContextMixin ← 合入
- 原 width_cache_diagnosis_mixin.py: WidthCacheDiagnosisMixin ← 合入
- 原 width_cache_sort_mixin.py: WidthCacheSortService ← 合入
"""
from __future__ import annotations

import heapq
import logging
import struct
import threading
import time
import zlib
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from dataclasses import dataclass, field

from ali2026v3_trading.infra.shared_utils import to_float32, CHINA_TZ


class _NoOpDiagnosisProbeManager:
    """DiagnosisProbeManager导入失败时的降级替代，所有操作为no-op"""
    @staticmethod
    @contextmanager
    def startup_step(name):
        yield
    @staticmethod
    def mark_startup_event(name, detail=''):
        pass


__all__ = [
    'SortEntry', 'WidthStrengthCache',
    'MAX_OPTION_CACHE_SIZE', 'MAX_INSTRUMENT_ID_MAP_SIZE', 'MAX_FUTURE_CACHE_SIZE',
    'MAX_STATUS_COUNTS_SIZE', 'MAX_SORT_BUCKETS_FUTURES',
    'WidthCacheContextMixin', '_WidthCacheContextMixin',
    'WidthCacheDiagnosisMixin', '_WidthCacheDiagnosisMixin',
    'WidthCacheSortService', '_WidthCacheSortMixin',
]


# ============================================================================
# SortEntry - 排序桶条目
# ============================================================================

@dataclass(order=True, slots=True)
class SortEntry:
    """
    分组D准备：排序桶条目，用于 by_sort_bucket 索引结构
    
    排序优先级（从高到低）：
    1. sync_flag: True > False (同步虚值优先)
    2. volume: 成交量越大越靠前
    3. strike_distance: 虚值距离越小越靠前（更接近实值）
    4. last_tick_ts: 最新tick时间越新越靠前
    5. internal_id: 兜底排序，保证稳定性
    """
    sort_key: Tuple = field(compare=True)
    internal_id: int = field(compare=False)
    instrument_id: str = field(compare=False)
    option_type: str = field(compare=False)
    strike_price: float = field(compare=False)
    volume: float = field(compare=False)
    price: float = field(compare=False)
    sync_flag: bool = field(compare=False)
    month: str = field(compare=False)
    product: str = field(compare=False)
    
    @staticmethod
    def create(internal_id: int, instrument_id: str, option_type: str, 
               strike_price: float, volume: float, price: float,
               sync_flag: bool, m_price: float, month: str, product: str,
               last_tick_ts: float = 0.0) -> 'SortEntry':
        if option_type == 'CALL':
            strike_distance = strike_price - m_price if m_price > 0 else float('inf')
        else:
            strike_distance = m_price - strike_price if m_price > 0 else float('inf')
        
        sort_key = (
            not sync_flag,
            -(volume if volume > 0 else 0.01),
            strike_distance,
            -last_tick_ts,
            internal_id,
        )
        
        return SortEntry(
            sort_key=sort_key,
            internal_id=internal_id,
            instrument_id=instrument_id,
            option_type=option_type,
            strike_price=strike_price,
            volume=volume,
            price=price,
            sync_flag=sync_flag,
            month=month,
            product=product,
        )


# ============================================================================
# 模块常量
# ============================================================================

try:
    from ali2026v3_trading.infra.event_bus import get_global_event_bus, EventBus
    _HAS_EVENT_BUS = get_global_event_bus() is not None
except ImportError as e:
    logging.warning(f"[TTypeService] EventBus import failed: {e}")
    EventBus = None
    get_global_event_bus = None
    _HAS_EVENT_BUS = False

try:
    from ali2026v3_trading.infra.event_bus import KLineEvent as EventBusKLineEvent
except ImportError:
    EventBusKLineEvent = None

MAX_OPTION_CACHE_SIZE = 20000
MAX_INSTRUMENT_ID_MAP_SIZE = 20000
MAX_FUTURE_CACHE_SIZE = 500
MAX_STATUS_COUNTS_SIZE = 20000
MAX_SORT_BUCKETS_FUTURES = 500
_CACHE_TTL_SECONDS = 86400


# ============================================================================
# 上下文解析Mixin（合入自 width_cache_context_mixin.py）
# ============================================================================

class WidthCacheContextMixin:

    def __init__(self):
        self._facade = None

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    @staticmethod
    def _normalize_instrument_id(instrument_id: str) -> str:
        from ali2026v3_trading.infra.shared_utils import normalize_instrument_id
        return normalize_instrument_id(instrument_id)

    @staticmethod
    def _normalize_option_type(opt_type: str) -> str:
        from ali2026v3_trading.infra.shared_utils import normalize_option_type
        return normalize_option_type(opt_type)
    
    def _resolve_internal_id(self, instrument_id: str) -> Optional[int]:
        normalized = self._normalize_instrument_id(instrument_id)
        
        iid = self._instrument_id_to_internal_id.get(normalized)
        if iid is not None:
            return iid
        
        try:
            ps = self._get_params()
            iid = ps.get_internal_id(normalized)
            if iid is not None:
                logging.info("[_resolve_internal_id] ParamsService回退命中: %s -> %d (配置文件外合约?)", normalized, iid)
                self._instrument_id_to_internal_id[normalized] = iid
                self._instrument_id_timestamps[normalized] = time.time()
                self._internal_id_to_instrument_id[iid] = normalized
                return iid
        except Exception as e:
            logging.warning(f"[_resolve_internal_id] ParamsService查询失败: {e}")
        
        return None

    def _get_future_product_by_underlying_id(self, underlying_future_id) -> str:
        if underlying_future_id is None or underlying_future_id == '':
            return ''
        if isinstance(underlying_future_id, str):
            logging.warning(
                f"[_get_future_product_by_underlying_id] 收到字符串 underlying_future_id='{underlying_future_id}'，"
                f"期望为整数 internal_id。请检查调用方是否错误传入了合约代码。"
            )
            return ''
        try:
            future_info = self._get_params().get_instrument_meta(int(underlying_future_id))
            if future_info:
                return str(future_info.get('product', '')).upper()
        except Exception as e:
            logging.warning(f"[_get_future_product_by_underlying_id] ParamsService查询失败: {e}")
        return ''

    def _get_future_price_by_id_and_month(self, underlying_future_id: Optional[int], month: str) -> float:
        if underlying_future_id is None:
            return 0.0
        future_product = self._get_future_product_by_underlying_id(underlying_future_id)
        if not future_product:
            return 0.0
        result = float(self._future_prices_by_month.get(future_product, {}).get(month, 0.0) or 0.0)
        # FIX-P1-21: 查找失败诊断日志，帮助定位产品键/月份键不匹配问题
        if result <= 0.0 and future_product:
            if not hasattr(self, '_price_lookup_miss_count'):
                self._price_lookup_miss_count = 0
            self._price_lookup_miss_count += 1
            if self._price_lookup_miss_count <= 10 or self._price_lookup_miss_count % 1000 == 1:
                _avail = list(self._future_prices_by_month.get(future_product, {}).keys())[:5]
                logging.debug("[PriceLookupMiss] product=%s month=%s available_months=%s count=%d",
                             future_product, month, _avail, self._price_lookup_miss_count)
        return result

    def _get_future_runtime_state(self, underlying_future_id: Optional[int], month: str) -> Dict[str, Any]:
        future_id = None
        if underlying_future_id not in (None, ''):
            try:
                future_id = int(underlying_future_id)
            except (TypeError, ValueError):
                logging.warning(
                    f"[_get_future_runtime_state] underlying_future_id 类型错误: "
                    f"got {type(underlying_future_id).__name__}={underlying_future_id!r}, expected int"
                )
        future_product = self._get_future_product_by_underlying_id(future_id)
        return {
            'future_id': future_id,
            'future_product': future_product,
            'initialized': self._future_initialized.get(future_id, False) if future_id is not None else False,
            'rising': self._future_rising.get(future_id, False) if future_id is not None else None,
            'price': self._get_future_price_by_id_and_month(future_id, month),
        }

    def _get_option_display_products(self, info: Optional[Dict[str, Any]]) -> Dict[str, str]:
        if not info:
            return {'underlying_product': '', 'future_product': ''}

        option_product = ''
        internal_id = info.get('internal_id')
        instrument_id = info.get('instrument_id')
        params_service = self._get_params()

        if internal_id is not None:
            meta = params_service.get_instrument_meta(int(internal_id))
            if meta:
                option_product = str(meta.get('product') or '').upper()

        if not option_product and instrument_id:
            meta = params_service.get_instrument_meta_by_id(str(instrument_id))
            if meta:
                option_product = str(meta.get('product') or '').upper()

        future_product = self._get_future_product_by_underlying_id(info.get('underlying_future_id'))
        return {
            'underlying_product': option_product or future_product,
            'future_product': future_product,
        }

    @staticmethod
    def _extract_strike_price(instrument_id: str) -> float:
        from ali2026v3_trading.infra.shared_utils import extract_strike_price
        result = extract_strike_price(instrument_id)
        return result if result is not None else 0.0

    def _resolve_option_context(self, instrument_id: str) -> Dict[str, Any]:
        normalized = self._normalize_instrument_id(instrument_id)
        context: Dict[str, Any] = {
            'instrument_id': normalized,
            'internal_id': None,
            'underlying_future_id': None,
            'month': '',
            'strike_price': 0.0,
            'option_type': '',
        }

        internal_id = self._instrument_id_to_internal_id.get(normalized)
        info = self._option_info.get(internal_id) if internal_id is not None else None
        if info:
            context.update({
                'internal_id': internal_id,
                'underlying_future_id': info.get('underlying_future_id'),
                'month': info.get('month', ''),
                'strike_price': float(info.get('strike_price') or 0.0),
                'option_type': str(info.get('option_type') or ''),
            })
            return context

        params_meta = self._get_params().get_instrument_meta_by_id(normalized)
        if params_meta:
            context.update({
                'internal_id': params_meta.get('internal_id'),
                'underlying_future_id': params_meta.get('underlying_future_id'),
                'month': str(params_meta.get('year_month') or params_meta.get('month') or ''),
                'strike_price': float(params_meta.get('strike_price') or 0.0),
                'option_type': str(params_meta.get('option_type') or ''),
            })
        return context

    def _get_underlying_price_by_future_id(self, underlying_future_id: Optional[int]) -> Optional[float]:
        if underlying_future_id in (None, ''):
            return None
        future_info = self._get_params().get_instrument_meta(int(underlying_future_id))
        if not future_info:
            return None
        future_instrument_id = future_info.get('instrument_id')
        if not future_instrument_id:
            return None
        return self.get_underlying_price_from_service(str(future_instrument_id))

    def _should_trace_option_tick(self, instrument_id: str) -> bool:
        normalized = self._normalize_instrument_id(instrument_id)
        normalized_upper = normalized.upper()
        return any(n.upper() == normalized_upper for n in self._tracked_option_tick_ids)

    def _log_tracked_option_tick(
        self,
        instrument_id: str,
        info: Optional[Dict[str, Any]],
        prev_price: float,
        current_price: float,
        note: str = '',
    ) -> None:
        internal_id = self._resolve_internal_id(instrument_id)
        iid = internal_id if internal_id is not None else instrument_id
        last_distinct_price = self._option_last_distinct_price.get(iid)
        direction = self._option_last_direction.get(iid)
        current_status = self._current_status.get(iid, 'other')
        direction_text = 'up' if direction is True else ('down' if direction is False else 'none')
        
        underlying_future_id = info.get('underlying_future_id')
        future_state = self._get_future_runtime_state(underlying_future_id, info.get('month', '') if info else '')
        display_meta = self._get_option_display_products(info)
        
        logging.info(
            "[TTypeTickTrace] instrument=%s product=%s month=%s option_type=%s prev_price=%.4f current_price=%.4f last_distinct_price=%s direction=%s current_status=%s sync=%s note=%s future_init=%s future_rising=%s",
            instrument_id,
            display_meta.get('underlying_product', ''),
            info.get('month', '') if info else '',
            info.get('option_type', '') if info else '',
            float(prev_price or 0.0),
            float(current_price or 0.0),
            f"{float(last_distinct_price):.4f}" if last_distinct_price is not None else 'None',
            direction_text,
            current_status,
            self._sync_flag.get(iid, False),
            note or '-',
            future_state['initialized'],
            future_state['rising'],
        )

    def _get_current_trading_date(self) -> str:
        return datetime.now(CHINA_TZ).strftime('%Y%m%d')

    def _get_future_price_for_month(self, product: str, month: str) -> float:
        return self._future_prices_by_month.get(product, {}).get(month, 0.0)

    def _is_out_of_the_money(self, option_type: str, underlying_price: float, strike: float) -> bool:
        if option_type == 'CALL':
            return underlying_price < strike
        else:
            return underlying_price > strike


_WidthCacheContextMixin = WidthCacheContextMixin


# ============================================================================
# 诊断Mixin（合入自 width_cache_diagnosis_mixin.py）
# ============================================================================

class WidthCacheDiagnosisMixin:

    def __init__(self):
        self._facade = None

    def __getattr__(self, name):
        if self._facade is not None:
            for klass in type(self._facade).__mro__:
                if name in klass.__dict__:
                    attr = klass.__dict__[name]
                    if hasattr(attr, '__get__'):
                        return attr.__get__(self._facade, type(self._facade))
                    return attr
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    @staticmethod
    def _is_commodity_exchange(exchange: str) -> bool:
        return str(exchange or '').upper() in {'CZCE', 'DCE', 'SHFE', 'INE', 'GFEX'}

    def _get_tick_entry_snapshot(self) -> Dict[str, Any]:
        try:
            from ali2026v3_trading.infra import health_monitor as hm
            lock = getattr(hm, '_tick_entry_accum_lock', None)
            accum = getattr(hm, '_tick_entry_accum', None)
            if lock is None or accum is None:
                return {'available': False, 'reason': 'tick_entry_probe_unavailable'}
            with lock:
                items = []
                total_ticks = 0
                for instrument_id, data in accum.items():
                    count = int(data.get('count', 0) or 0)
                    total_ticks += count
                    items.append({
                        'instrument_id': instrument_id,
                        'count': count,
                        'last_price': data.get('last_price'),
                        'last_oi': data.get('last_oi'),
                    })
            items.sort(key=lambda x: (-x['count'], x['instrument_id']))
            return {
                'available': True,
                'tracked_contracts': len(items),
                'total_ticks': total_ticks,
                'top_contracts': items[:10],
            }
        except Exception as exc:
            return {'available': False, 'reason': str(exc)}

    def _build_future_score_rows(self, facade, future_internal_id: Optional[int] = None) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        status_counts = getattr(facade, '_status_counts', {})
        future_initialized = getattr(facade, '_future_initialized', {})
        future_rising_map = getattr(facade, '_future_rising', {})
        for fid, month_data in status_counts.items():
            if future_internal_id is not None and fid != future_internal_id:
                continue

            fp_info = facade._get_params().get_instrument_meta(fid)
            if not fp_info:
                continue

            product = str(fp_info.get('product', '')).upper()
            exchange = str(fp_info.get('exchange', '')).upper()
            initialized = bool(future_initialized.get(fid, False))
            future_rising = future_rising_map.get(fid, False)
            opt_type = 'CALL' if future_rising else 'PUT'
            raw_months = list(month_data.keys())
            months = facade._get_scoring_months(fid, raw_months)
            n_months = len(months)

            if n_months > 0:
                weights = facade.resolve_month_weights(n_months)
                month_list = []
                total_other = 0
                total_all = 0
                for mth in months:
                    counts = month_data.get(mth, {}).get(opt_type, {})
                    cr = counts.get('correct_rise', 0)
                    wr = counts.get('wrong_rise', 0)
                    cf = counts.get('correct_fall', 0)
                    wf = counts.get('wrong_fall', 0)
                    other = counts.get('other', 0)
                    total = cr + wr + cf + wf + other
                    total_other += other
                    total_all += total
                    month_list.append({
                        'cr': cr, 'wr': wr, 'cf': cf, 'wf': wf,
                        'other': other, 'total': total,
                    })
                correct_up_pct = facade.compute_correct_up_pct(month_list, weights)
                noise_ratio = facade.compute_noise_ratio(month_list, weights)
                coverage = facade.compute_coverage(month_list, weights)
                total_cr = sum(m['cr'] for m in month_list)
                total_rise = sum(m['cr'] + m['wr'] for m in month_list)
                wilson = facade.wilson_lower_bound(total_cr, total_rise)
                tier = facade.determine_tier(coverage, wilson, correct_up_pct, noise_ratio)
                other_ratio = (total_other / total_all) if total_all > 0 else 0.0
            else:
                correct_up_pct = 0.0
                noise_ratio = 0.0
                coverage = 0.0
                wilson = 0.0
                tier = 4
                other_ratio = 0.0
                total_all = 0

            rows.append({
                'future_internal_id': fid,
                'product': product,
                'exchange': exchange,
                'initialized': initialized,
                'future_rising': bool(future_rising),
                'months': months,
                'month_count': n_months,
                'total': total_all,
                'other_ratio': other_ratio,
                'coverage': coverage,
                'correct_up_pct': correct_up_pct,
                'noise_ratio': noise_ratio,
                'wilson': wilson,
                'tier': tier,
                'is_commodity': self._is_commodity_exchange(exchange),
            })
        rows.sort(key=lambda x: (x['tier'], -x['correct_up_pct'], -x['wilson'], x['product']))
        return rows

    @staticmethod
    def _summarize_future_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        tier_counts = {1: 0, 2: 0, 3: 0, 4: 0}
        initialized = 0
        commodity_rows = []
        for row in rows:
            tier_counts[row['tier']] = tier_counts.get(row['tier'], 0) + 1
            if row['initialized']:
                initialized += 1
            if row['is_commodity']:
                commodity_rows.append(row)

        def _avg(key: str, subset: List[Dict[str, Any]]) -> float:
            if not subset:
                return 0.0
            return sum(float(item.get(key, 0.0) or 0.0) for item in subset) / len(subset)

        return {
            'total_futures': len(rows),
            'initialized_futures': initialized,
            'tier_counts': tier_counts,
            'commodity_count': len(commodity_rows),
            'commodity_avg_coverage': _avg('coverage', commodity_rows),
            'commodity_avg_correct_up_pct': _avg('correct_up_pct', commodity_rows),
            'commodity_avg_noise_ratio': _avg('noise_ratio', commodity_rows),
            'commodity_avg_other_ratio': _avg('other_ratio', commodity_rows),
        }

    def print_status_diagnosis(self, future_internal_id: int = None, top_n: int = 10):
        try:
            facade = getattr(self, '_facade', self)
            if hasattr(facade, 'get_cache_stats'):
                try:
                    stats = facade.get_cache_stats()
                except Exception as _gs_err:
                    logging.warning("[WidthStrengthCache.print_status_diagnosis] get_cache_stats()异常: %s", _gs_err)
                    stats = {}
            else:
                stats = {}
            logging.info(
                "[WidthStrengthCache.print_status_diagnosis] future_id=%s top_n=%d stats=%s",
                future_internal_id, top_n, stats
            )

            tick_snapshot = self._get_tick_entry_snapshot()
            logging.info("[TickEntryDiagnosis] snapshot=%s", tick_snapshot)

            status_counts = getattr(facade, '_status_counts', {})
            if status_counts:
                total_correct_rise = 0
                total_wrong_rise = 0
                total_correct_fall = 0
                total_wrong_fall = 0
                total_other = 0
                for future_id, months_data in status_counts.items():
                    if future_internal_id is not None and future_id != future_internal_id:
                        continue
                    for month, opt_types in months_data.items():
                        for opt_type, bucket in opt_types.items():
                            total_correct_rise += bucket.get('correct_rise', 0)
                            total_wrong_rise += bucket.get('wrong_rise', 0)
                            total_correct_fall += bucket.get('correct_fall', 0)
                            total_wrong_fall += bucket.get('wrong_fall', 0)
                            total_other += bucket.get('other', 0)
                total = total_correct_rise + total_wrong_rise + total_correct_fall + total_wrong_fall + total_other
                logging.info(
                    "[5种状态统计] total=%d | correct_rise=%d wrong_rise=%d correct_fall=%d wrong_fall=%d other=%d",
                    total, total_correct_rise, total_wrong_rise, total_correct_fall, total_wrong_fall, total_other
                )
                if total > 0:
                    logging.info(
                        "[5种状态占比] correct_rise=%.1f%% wrong_rise=%.1f%% correct_fall=%.1f%% wrong_fall=%.1f%% other=%.1f%%",
                        total_correct_rise / total * 100, total_wrong_rise / total * 100,
                        total_correct_fall / total * 100, total_wrong_fall / total * 100,
                        total_other / total * 100
                    )
            else:
                logging.warning("[5种状态统计] _status_counts为空，期权状态尚未计算")

            try:
                _fut_init = getattr(facade, '_future_initialized', {})
                _fut_init_true = sum(1 for v in _fut_init.values() if v)
                _fut_init_total = len(_fut_init)
                _opt_price = getattr(facade, '_option_price', {})
                _opt_info = getattr(facade, '_option_info', {})
                _opt_last_dir = getattr(facade, '_option_last_direction', {})
                _opt_dir_valid = sum(1 for v in _opt_last_dir.values() if v is not None)
                _oot_diag_no_iid = getattr(facade, '_oot_diag_no_iid', 0)
                _oot_diag_no_info = getattr(facade, '_oot_diag_no_info', 0)
                _oot_diag_first_tick = getattr(facade, '_oot_diag_first_tick', 0)
                _oot_diag_classify = getattr(facade, '_oot_diag_classify', 0)
                logging.debug("[R22-DIAG] future_initialized: true=%d/%d | option_price: %d | option_info: %d | option_dir_valid: %d | oot_no_iid=%d oot_no_info=%d oot_first=%d oot_classify=%d",
                              _fut_init_true, _fut_init_total, len(_opt_price), len(_opt_info), _opt_dir_valid,
                              _oot_diag_no_iid, _oot_diag_no_info, _oot_diag_first_tick, _oot_diag_classify)
            except Exception as _diag_err:
                logging.debug("[R22-DIAG] error: %s", _diag_err, exc_info=True)

            future_rows = self._build_future_score_rows(facade, future_internal_id)
            summary = self._summarize_future_rows(future_rows)
            logging.info("[FutureScoreDiagnosis] summary=%s", summary)

            if future_rows:
                top_rows = future_rows[:top_n]
                logging.info("[FutureScoreDiagnosis] top_rows=%s", top_rows)

                tier4_rows = [
                    {
                        'product': row['product'],
                        'exchange': row['exchange'],
                        'coverage': round(row['coverage'], 4),
                        'correct_up_pct': round(row['correct_up_pct'], 4),
                        'noise_ratio': round(row['noise_ratio'], 4),
                        'other_ratio': round(row['other_ratio'], 4),
                        'months': row['months'],
                    }
                    for row in future_rows if row['tier'] == 4
                ][:top_n]
                logging.info("[FutureScoreDiagnosis] tier4_samples=%s", tier4_rows)

                commodity_rows = [
                    {
                        'product': row['product'],
                        'exchange': row['exchange'],
                        'tier': row['tier'],
                        'coverage': round(row['coverage'], 4),
                        'correct_up_pct': round(row['correct_up_pct'], 4),
                        'noise_ratio': round(row['noise_ratio'], 4),
                        'other_ratio': round(row['other_ratio'], 4),
                        'initialized': row['initialized'],
                        'months': row['months'],
                    }
                    for row in future_rows if row['is_commodity']
                ][:top_n]
                logging.info("[CommodityFutureScoreDiagnosis] samples=%s", commodity_rows)

                product_coverage = {}
                for row in future_rows:
                    prod = row['product']
                    if prod not in product_coverage:
                        product_coverage[prod] = {'exchange': row['exchange'], 'coverage': row['coverage'],
                                                   'tier': row['tier'], 'initialized': row['initialized'],
                                                   'months': row['months'], 'other_ratio': row['other_ratio']}
                coverage_sorted = sorted(product_coverage.items(), key=lambda x: -x[1]['coverage'])[:top_n]
                coverage_output = [
                    {'product': k, 'exchange': v['exchange'], 'coverage': round(v['coverage'], 4),
                     'tier': v['tier'], 'initialized': v['initialized'], 'other_ratio': round(v['other_ratio'], 4),
                     'months': v['months']}
                    for k, v in coverage_sorted
                ]
                logging.info("[ProductCoverageDiagnosis] top_%d=%s", top_n, coverage_output)
                _avg_cov = sum(v['coverage'] for v in coverage_output) / len(coverage_output) if coverage_output else 0
                _tier_dist = {}
                for v in coverage_output:
                    _t = f"tier{v['tier']}"
                    _tier_dist[_t] = _tier_dist.get(_t, 0) + 1
                logging.info(
                    "[ProductCoverageDiagnosis] summary: avg_coverage=%.4f, tier_dist=%s, commodity_avg_coverage=%.4f",
                    _avg_cov, _tier_dist,
                    sum(v['coverage'] for v in coverage_output if v['exchange'] not in ('CFFEX',)) / max(1, sum(1 for v in coverage_output if v['exchange'] not in ('CFFEX',)))
                )

            if future_internal_id is not None and hasattr(facade, 'get_width_strength_summary'):
                months = facade.get_all_months(future_internal_id) if hasattr(facade, 'get_all_months') else []
                if months:
                    summary = facade.get_width_strength_summary(future_internal_id, months)
                    logging.info(
                        "[WidthStrengthCache.print_status_diagnosis] future_id=%s summary=%s",
                        future_internal_id, summary
                    )
        except Exception as e:
            logging.debug("[WidthStrengthCache.print_status_diagnosis] error: %s", e)


_WidthCacheDiagnosisMixin = WidthCacheDiagnosisMixin


# ============================================================================
# 排序桶索引维护Mixin（合入自 width_cache_sort_mixin.py）
# ============================================================================

class WidthCacheSortService:

    def __init__(self):
        self._facade = None

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def _update_sort_bucket(self, internal_id: int, info: Dict, future_internal_id: int, month: str, opt_type: str):
        with self._lock:
            self._do_update_sort_bucket(internal_id, info, future_internal_id, month, opt_type)

    def _do_update_sort_bucket(self, internal_id: int, info: Dict, future_internal_id: int, month: str, opt_type: str):
        try:
            future_info = self._get_params().get_instrument_meta(future_internal_id)
            if not future_info:
                logging.warning(f"[_update_sort_bucket] Unknown future_internal_id: {future_internal_id}")
                return

            m_price = self._get_future_price_by_id_and_month(future_internal_id, month)
            if m_price <= 0:
                return

            strike = info.get('strike_price', 0)
            volume = self._option_daily_volume.get(internal_id, self._option_volume.get(internal_id, 0))
            price = self._option_price.get(internal_id, 0)
            sync_flag = self._sync_flag.get(internal_id, False)
            instrument_id = info.get('instrument_id', self._internal_id_to_instrument_id.get(internal_id, str(internal_id)))

            current_status = self._current_status.get(internal_id, 'other')
            if current_status not in ('correct_rise', 'correct_fall'):
                self._remove_from_sort_bucket(internal_id, future_internal_id, month, opt_type)
                return

            if not self._is_out_of_the_money(opt_type, m_price, strike):
                self._remove_from_sort_bucket(internal_id, future_internal_id, month, opt_type)
                return

            entry = SortEntry.create(
                internal_id=internal_id,
                instrument_id=instrument_id,
                option_type=opt_type,
                strike_price=strike,
                volume=volume,
                price=price,
                sync_flag=sync_flag,
                m_price=m_price,
                month=month,
                product=str(future_info.get('product', '')).upper(),
                last_tick_ts=time.monotonic(),
            )

            bucket = self._sort_buckets[future_internal_id][month][opt_type]

            existing_idx = None
            for i, e in enumerate(bucket):
                if e.internal_id == internal_id:
                    existing_idx = i
                    break

            if existing_idx is not None:
                bucket[existing_idx] = bucket[-1]
                bucket.pop()
                self._buckets_dirty.add((future_internal_id, month, opt_type))

            if len(bucket) < self._sort_bucket_max_size:
                heapq.heappush(bucket, entry)
                self._buckets_dirty.add((future_internal_id, month, opt_type))
            else:
                worst_idx = max(range(len(bucket)), key=lambda i: bucket[i])
                if entry < bucket[worst_idx]:
                    bucket[worst_idx] = entry
                    self._buckets_dirty.add((future_internal_id, month, opt_type))

        except Exception as e:
            logging.warning(f"[_update_sort_bucket] Error updating sort bucket for {internal_id}: {e}")

    def _remove_from_sort_bucket(self, internal_id: int, future_internal_id: int, month: str, opt_type: str):
        with self._lock:
            self._do_remove_from_sort_bucket(internal_id, future_internal_id, month, opt_type)

    def _do_remove_from_sort_bucket(self, internal_id: int, future_internal_id: int, month: str, opt_type: str):
        try:
            bucket = self._sort_buckets.get(future_internal_id, {}).get(month, {}).get(opt_type, [])
            for i, entry in enumerate(bucket):
                if entry.internal_id == internal_id:
                    bucket[i] = bucket[-1]
                    bucket.pop()
                    self._buckets_dirty.add((future_internal_id, month, opt_type))
                    break
        except Exception as e:
            logging.warning(f"[_remove_from_sort_bucket] Error removing {internal_id}: {e}")


_WidthCacheSortMixin = WidthCacheSortService
