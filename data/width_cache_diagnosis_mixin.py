# MODULE_ID: M1-055
"""
width_cache_diagnosis_mixin.py — 诊断Mixin
包含: _WidthCacheDiagnosisMixin — 宽度缓存诊断功能
"""
from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional


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
                        'cr': cr,
                        'wr': wr,
                        'cf': cf,
                        'wf': wf,
                        'other': other,
                        'total': total,
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

            # 输出5种状态统计
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

            # FIX-R22-DIAG: 打印future_initialized和option_price的状态
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