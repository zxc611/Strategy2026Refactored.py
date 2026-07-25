# MODULE_ID: M1-056
"""
width_cache_query_mixin.py — 查询Mixin
包含: _WidthCacheQueryMixin
"""
from __future__ import annotations

import logging
import heapq
import time
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from datetime import datetime

from infra.shared_utils import to_float32, CHINA_TZ
from data.width_cache_types import (
    _NoOpDiagnosisProbeManager, SortEntry,
    MAX_OPTION_CACHE_SIZE, MAX_INSTRUMENT_ID_MAP_SIZE, MAX_FUTURE_CACHE_SIZE,
    MAX_STATUS_COUNTS_SIZE, MAX_SORT_BUCKETS_FUTURES, _CACHE_TTL_SECONDS,
)
# 五唯一性修复：从 final_three_layer_config 统一导入 Tier 阈值与月份权重常量
from config.tvf_param_loader import (
    MONTH_WEIGHTS_5, TIER1_WILSON_THRESHOLD,
    TIER2_COVERAGE_THRESHOLD, TIER2_CORRECT_UP_THRESHOLD,
    TIER3_CORRECT_UP_THRESHOLD,
    SORTER_CONFIG,
)
from config.config_exchange import (
    month_mapping as DEFAULT_MONTH_MAPPING,
    get_runtime_scoring_months,
)


class WidthCacheQueryService:

    # 五唯一性修复：常量统一从 final_three_layer_config 引用，避免重复定义导致数值漂移
    # MONTH_WEIGHTS_5 / TIER1_WILSON_THRESHOLD / TIER2_COVERAGE_THRESHOLD /
    # TIER2_CORRECT_UP_THRESHOLD / TIER3_CORRECT_UP_THRESHOLD 已删除（原 0.6/0.5/0.5/0.4 与规范值 0.50/0.40/0.45/0.35 不一致）
    MAX_MONTHS_FOR_SCORING = 5
    TIER1_LOTS = 2
    TIER2_LOTS = 1
    TOP_FUTURES_COUNT = 8


    def __init__(self):
        self._facade = None
        self._sorter_a = None
        self._sorter_b = None
        self._sorter_c = None
        # v2.8 §21: 排序方案配置覆盖（回测时允许外部注入，实盘从params_service读取）
        self._sorter_config_override: Optional[Dict[str, Any]] = None

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    # v2.8 §21: 排序方案配置注入接口（回测三方案切换用）
    def configure_sorter(self, config: Optional[Dict[str, Any]]) -> None:
        """v2.8 §21: 注入排序方案配置覆盖。

        用途：
        - 回测驱动器调用此方法切换 scoring_scheme（scheme_1/2/3/all）
        - 实盘不调用此方法，使用 SORTER_CONFIG 默认值（scheme_1）或 params_service 覆盖
        - 传入 None 清除覆盖，恢复默认行为

        Args:
            config: 排序配置字典（如 {'scoring_scheme': 'scheme_2', 'output_mode': 'research'}），
                    或 None 清除覆盖
        """
        self._sorter_config_override = config
        # 清除已实例化的排序器，强制下次调用时用新配置重建
        self._sorter_a = None
        self._sorter_b = None
        self._sorter_c = None

    @staticmethod
    def _resolve_month_mapping(params: Any) -> Dict[str, Any]:
        if params is None:
            return DEFAULT_MONTH_MAPPING
        try:
            mapping = params.get('month_mapping', None)
        except Exception:
            mapping = None
        if not mapping:
            mapping = getattr(params, 'month_mapping', None)
        return mapping or DEFAULT_MONTH_MAPPING

    def get_width_strength_summary(self, future_internal_id: int, months: List[str], option_type: str = None) -> Dict[str, int]:
        """P2 优化：获取指定月份组合的五类状态汇总
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）'
            months: 月份列表
            option_type: 可选，'CALL' 或 'PUT'
            
        Returns:
            Dict: 五类状态计数
        """
        with self._lock:
            # ✅ 从 id_cache 获取 product
            info = self._get_params().get_instrument_meta(future_internal_id)
            if not info:
                logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
                return {'correct_rise': 0, 'wrong_rise': 0, 'correct_fall': 0, 'wrong_fall': 0, 'other': 0}
            
            product = str(info.get('product', '')).upper()
            summary = {'correct_rise': 0, 'wrong_rise': 0, 'correct_fall': 0, 'wrong_fall': 0, 'other': 0}
            for month in months:
                # ✅ 使用 future_internal_id 作为键
                month_data = self._status_counts.get(future_internal_id, {}).get(month, {})
                target_types = [self._normalize_option_type(option_type)] if option_type else ['CALL', 'PUT']
                
                for ot in target_types:
                    counts = month_data.get(ot, {})
                    for key in summary:
                        summary[key] += counts.get(key, 0)
            return summary

    def get_future_rising(self, future_internal_id: int) -> bool:
        """🟡 中优先级：封装 _future_rising 访问
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）'
        Returns:
            bool: 期货是否上涨
        """
        with self._lock:
            # ✅ 直接查询，使用 future_internal_id 作为键
            return self._future_rising.get(future_internal_id, False)
    
    def get_all_months(self, future_internal_id: int) -> List[str]:
        """获取某期货已注册的所有月份
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            
        Returns:
            List[str]: 月份列表
        """
        # ✅ 从 id_cache 获取 product
        info = self._get_params().get_instrument_meta(future_internal_id)
        if not info:
            logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
            return []
        
        product = str(info.get('product', '')).upper()
        # ✅ 使用 future_internal_id 作为键
        return self._months.get(future_internal_id, [])
    
    def get_width_strength(self, future_internal_id: int, months: List[str], option_type: str = None) -> int:
        """🔴 核心方法：O(1) 查询宽度强度（同步虚值期权计数）'
        Args:
            future_internal_id: 期货合约 internal_id（主键）
            months: 月份列表
            option_type: 可选，'CALL' 或 'PUT'，None 表示两者都算
            
        Returns:
            int: 同步虚值期权计数
        """
        with self._lock:
            # ✅ 从 id_cache 获取 product
            info = self._get_params().get_instrument_meta(future_internal_id)
            if not info:
                logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
                return 0
            
            product = str(info.get('product', '')).upper()
            strength = 0
            target_types = [self._normalize_option_type(option_type)] if option_type else ['CALL', 'PUT']
            
            for month in months:
                for opt_type in target_types:
                    # ✅ O(1) 查询：直接从内存字典读取，使用 future_internal_id 作为键
                    count = self._sync_otm_count.get(future_internal_id, {}).get(month, {}).get(opt_type, 0)
                    strength += count
            
            self._query_count += 1
            self._cache_hits += 1
            
            return strength
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息（含内存占用估算）"""
        import sys
        
        with self._lock:
            total_queries = self._query_count
            hit_rate = (self._cache_hits / total_queries * 100) if total_queries > 0 else 0.0
            
            # ✅ 低优先级：内存占用估算
            try:
                memory_estimate_bytes = (
                    sys.getsizeof(self._future_prev_price) +
                    sys.getsizeof(self._option_price) +
                    sys.getsizeof(self._option_info) +
                    sys.getsizeof(self._sync_otm_count) +
                    sys.getsizeof(self._sync_flag)
                )
                memory_estimate_kb = round(memory_estimate_bytes / 1024, 2)
            except Exception as e:
                logging.debug(f"[print_status_diagnosis] Memory estimation failed: {e}")
                memory_estimate_kb = None
            
            return {
                'total_futures': len(self._future_prev_price),
                'total_options': len(self._option_info),
                'total_products': len(self._months),
                'sync_flags_count': len(self._sync_flag),
                'query_count': self._query_count,
                'cache_hits': self._cache_hits,
                'cache_misses': self._query_count - self._cache_hits,
                'hit_rate_percent': round(hit_rate, 2),
                'memory_estimate_kb': memory_estimate_kb
            }
    
    def select_from_sort_bucket(self, future_internal_id: int, month: str, opt_type: str, top_n: int = 1) -> List[Dict[str, Any]]:
        """
        ✅ 分组D准备：从排序桶直接读取最优期权
        
        这是 select_otm_targets_by_volume 的优化版本，直接从排序桶读取，
        避免全量扫描。'
        Args:
            future_internal_id: 期货合约 internal_id（主键）'
            month: 月份
            opt_type: CALL/PUT
            top_n: 返回前N个
        
        Returns:
            最优期权列表
        """
        with self._lock:
            try:
                bucket_key = (future_internal_id, month, opt_type)
                if bucket_key in self._buckets_dirty:
                    bucket = self._sort_buckets.get(future_internal_id, {}).get(month, {}).get(opt_type, [])
                    if bucket:
                        heapq.heapify(bucket)
                    self._buckets_dirty.discard(bucket_key)

                bucket = self._sort_buckets.get(future_internal_id, {}).get(month, {}).get(opt_type, [])
                if not bucket:
                    return []
                
                # 堆已经按排序键排好序，直接取前top_n个
                sorted_entries = sorted(bucket)[:top_n]
                
                results = []
                for entry in sorted_entries:
                    results.append({
                        'instrument_id': entry.instrument_id,
                        'internal_id': entry.internal_id,
                        'option_type': entry.option_type,
                        'strike_price': entry.strike_price,
                        'volume': entry.volume,
                        'price': entry.price,
                        'month': entry.month,
                        'product': entry.product,
                        'sync_flag': entry.sync_flag,
                        'current_status': getattr(entry, 'current_status', 'other'),
                    })
                
                return results
                
            except Exception as e:
                logging.error(f"[select_from_sort_bucket] Error: {e}")
                return []

    def get_all_options(self) -> List[Dict[str, Any]]:
        """获取所有已注册的期权合约列表"""
        with self._lock:
            results = []
            for internal_id, info in self._option_info.items():
                display_meta = self._get_option_display_products(info)
                results.append({
                    'internal_id': internal_id,
                    'instrument_id': info.get('instrument_id'),
                    'option_type': info.get('option_type'),
                    'strike_price': info.get('strike_price'),
                    'underlying_product': display_meta.get('underlying_product'),
                    'future_product': display_meta.get('future_product'),
                    'underlying_future_id': info.get('underlying_future_id'),
                    'month': info.get('month'),
                    'price': self._option_price.get(internal_id, 0.0),
                })
            return results

    @staticmethod
    def wilson_lower_bound(pos: float, total: float, z: float = 1.96) -> float:
        """Wilson score interval lower bound (design report Section 8)."""
        if total <= 0:
            return 0.0
        p = pos / total
        n = total
        denom = 1.0 + z * z / n
        center = p + z * z / (2 * n)
        spread = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
        return max(0.0, (center - spread) / denom)

    # FIX-S4-ROOT-20260720-v2: DTE/IV近似计算（替代不存在的DataWriterMixin方法）

    @staticmethod
    def _approx_dte_from_year_month(year_month: str) -> int:
        """从year_month(如'2607')推算距到期日的交易日数

        中国金融期货(CFFEX)期权到期日: 合约月份第三个周五
        商品期货期权到期日略有不同，此处统一用第三个周五近似
        """
        if not year_month or len(year_month) < 4:
            return 3  # fallback
        try:
            from datetime import date, timedelta
            _yy = int(year_month[:2]) + 2000
            _mm = int(year_month[2:4]) if len(year_month) >= 4 else 1
            # 找当月第三个周五
            first_day = date(_yy, _mm, 1)
            # 周一=0, 周五=4
            first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
            third_friday = first_friday + timedelta(days=14)
            today = date.today()
            dte = (third_friday - today).days
            # 转换为交易日(约70%)
            trading_days = max(1, int(dte * 0.7))
            return trading_days
        except (ValueError, TypeError):
            return 3  # fallback

    @staticmethod
    def _approx_iv_from_moneyness(premium: float, strike: float, underlying: float,
                                   dte: int, option_type: str) -> float:
        """从moneyness简单近似IV

        使用Breakeven-Vol近似: IV ≈ premium / (underlying * sqrt(dte/252))
        这是一个粗略近似，但比硬编码0.15更好
        """
        if underlying <= 0 or dte <= 0 or premium <= 0:
            return 0.15  # fallback
        try:
            import math
            _t = dte / 252.0  # 年化时间
            _sqrt_t = math.sqrt(_t) if _t > 0 else 0.1
            # 简单近似: IV = premium / (underlying * sqrt_t)
            # 对于ATM期权: premium ≈ 0.4 * underlying * IV * sqrt_t
            # → IV ≈ premium / (0.4 * underlying * sqrt_t)
            _iv = premium / (0.4 * underlying * _sqrt_t)
            # 合理范围限制
            _iv = max(0.05, min(_iv, 2.0))
            return _iv
        except (ValueError, ZeroDivisionError):
            return 0.15

    @staticmethod
    def resolve_month_weights(n_months: int) -> Tuple[float, ...]:
        """Resolve month weights for n months (design report Section 4.3).

        - n_months <= 0: return empty tuple
        - n_months <= 5: truncate MONTH_WEIGHTS_5 to first n and normalize
        - n_months > 5:  return MONTH_WEIGHTS_5 (5 elements); caller must
                          truncate months to first 5 (design report 4.3:
                          "取前5个月份参与评分，超出的月份不参与计算")
        """
        if n_months <= 0:
            return ()
        if n_months > WidthCacheQueryService.MAX_MONTHS_FOR_SCORING:
            # BUG-12 fix: return 5-element weights, caller truncates months
            return MONTH_WEIGHTS_5
        raw_weights = MONTH_WEIGHTS_5[:n_months]
        raw_sum = sum(raw_weights)
        if raw_sum <= 0:
            per = 1.0 / n_months
            return tuple(per for _ in range(n_months))
        return tuple(w / raw_sum for w in raw_weights)

    @staticmethod
    def compute_correct_up_pct(month_data: List[Dict], weights: Tuple[float, ...]) -> float:
        """Compute weighted correct_up_pct (design report Section 3.4).

        五唯一性修复：对齐规范公式
        correct_up_pct = Σ[w_i × (cr_i + cf_i)] / Σ[w_i × (cr_i + cf_i + wr_i + wf_i)]
        other状态不参与分母计算，避免夜盘other占比高导致correct_up_pct被稀释。
        """
        numerator = 0.0
        denominator = 0.0
        for i, data in enumerate(month_data):
            if i >= len(weights):
                break
            w = weights[i]
            cr = data.get('cr', 0)
            cf = data.get('cf', 0)
            wr = data.get('wr', 0)
            wf = data.get('wf', 0)
            sync_total = cr + cf + wr + wf
            numerator += w * (cr + cf)
            denominator += w * sync_total
        return numerator / denominator if denominator > 0 else 0.0

    @staticmethod
    def compute_noise_ratio(month_data: List[Dict], weights: Tuple[float, ...]) -> float:
        """Compute weighted noise_ratio (design report Section 2)."""
        result = 0.0
        for i, data in enumerate(month_data):
            if i >= len(weights):
                break
            w = weights[i]
            wr = data.get('wr', 0)
            wf = data.get('wf', 0)
            total = data.get('total', 0)
            if total > 0:
                result += w * ((wr + wf) / total)
        return result

    @staticmethod
    def compute_coverage(month_data: List[Dict], weights: Tuple[float, ...]) -> float:
        """Compute active trading coverage = active classified months / denominator.

        `other` is diagnostic inventory, not an effective trading classification.
        Pure-other months must not inflate coverage or make inactive option chains
        look tradeable.

        FIX-13 RC-14: 分母自适应实际月份数，避免冷启动期间coverage永远=0.2被永久阻断
        根因: 原公式 active_months/MAX_MONTHS_FOR_SCORING(=5)，1个月分类数据→0.2，
              2个月→0.40，导致冷启动期间coverage始终 < TIER2_COVERAGE_THRESHOLD(原0.40)
              永远无法进入Tier1/Tier2，所有月份被拒入Tier3，形成80%无法有效分类的死循环。
        修复: 分母 = min(len(month_data), MAX_MONTHS_FOR_SCORING)
              1个月份 → 1/1 = 1.0 (放行进入Tier2评估)
              2个月份 → 2/2 = 1.0
              5个月份 → 5/5 = 1.0
              8个月份 → 5/8（截断到5个月份评估）= 1.0
        """
        active_months = 0
        evaluated_months = 0
        for i, data in enumerate(month_data):
            if i >= len(weights):
                break
            evaluated_months += 1
            classified_total = (data.get('cr', 0) + data.get('cf', 0) +
                                data.get('wr', 0) + data.get('wf', 0))
            if classified_total > 0:
                active_months += 1
        # FIX-13 RC-14: 分母自适应，避免冷启动期间分母>实际月份数导致coverage被人为压低
        max_months = WidthCacheQueryService.MAX_MONTHS_FOR_SCORING
        denominator = min(evaluated_months, max_months) if evaluated_months > 0 else max_months
        return active_months / denominator if denominator > 0 else 0.0

    @staticmethod
    def compute_th(month_data: List[Dict], weights: Tuple[float, ...]) -> float:
        """Compute trend healthiness (design report Section 3.2).

        th = Σ[w_i × CR_i / (CR_i + CF_i)]
        TH < 0.5 triggers trend decay warning.
        """
        result = 0.0
        for i, data in enumerate(month_data):
            if i >= len(weights):
                break
            w = weights[i]
            cr = data.get('cr', 0)
            cf = data.get('cf', 0)
            sync_total = cr + cf
            if sync_total > 0:
                result += w * (cr / sync_total)
        return result

    @staticmethod
    def compute_ra(month_data: List[Dict], weights: Tuple[float, ...]) -> float:
        """Compute risk-adjusted ratio (design report Section 3.2).

        ra = Σ[w_i × (1 - WR_i / (CR_i + WR_i))]
        RA < 0.5 triggers divergence warning.
        """
        result = 0.0
        for i, data in enumerate(month_data):
            if i >= len(weights):
                break
            w = weights[i]
            cr = data.get('cr', 0)
            wr = data.get('wr', 0)
            rise = cr + wr
            if rise > 0:
                result += w * (1.0 - wr / rise)
        return result

    @staticmethod
    def determine_tier(coverage: float, wilson: float, correct_up_pct: float, noise_ratio: float,
                       tier3_correct_up_override: float = None) -> int:
        """Determine tier (design report Section 3.5).

        五唯一性修复：对齐规范阈值和运算符
        - Tier 1: coverage >= 0.8 AND wilson >= TIER1_WILSON_THRESHOLD
        - Tier 2: coverage >= TIER2_COVERAGE_THRESHOLD AND correct_up_pct >= TIER2_CORRECT_UP_THRESHOLD
        - Tier 3: correct_up_pct >= TIER3_CORRECT_UP_THRESHOLD
        - Tier 4: otherwise (noise filter)

        FIX-R4修复：当correct_up_pct == 0时，区分"无分类数据"与"有数据但全错"。
        - coverage == 0（无分类数据）：降级为Tier 3（允许交易），而非Tier 4。
          原因：平台未推送tick导致无分类数据，不等于品种质量差。
        - coverage > 0 但 correct_up_pct == 0（有数据但全错）：保持Tier 4。
        这样避免84%期货因"无数据"被误判为"不可交易"。

        noise_ratio参数保留用于未来扩展，当前不参与硬过滤（避免夜盘other占比高导致全过滤）。
        """
        # FIX-II (R10-1-1): 从SORTER_CONFIG读取运行时阈值，而非导入的模块级常量
        # 根因: determine_tier使用模块级常量(TIER1_WILSON_THRESHOLD等)，
        #       这些常量是默认值，运行时覆盖仅修改SORTER_CONFIG dict，
        #       但determine_tier不读SORTER_CONFIG→覆盖无效
        # 修复: 优先从SORTER_CONFIG读取，fallback到模块级常量
        _tier1_wilson = SORTER_CONFIG.get('tier1_wilson_threshold', TIER1_WILSON_THRESHOLD)
        _tier2_coverage = SORTER_CONFIG.get('tier2_coverage_threshold', TIER2_COVERAGE_THRESHOLD)
        _tier2_correct_up = SORTER_CONFIG.get('tier2_correct_up_threshold', TIER2_CORRECT_UP_THRESHOLD)
        _tier3_correct_up = SORTER_CONFIG.get('tier3_correct_up_threshold', TIER3_CORRECT_UP_THRESHOLD)
        # FIX-C (2026-07-23): 支持参数覆盖tier3阈值，避免临时修改全局SORTER_CONFIG的线程安全问题
        if tier3_correct_up_override is not None:
            _tier3_correct_up = tier3_correct_up_override
        if correct_up_pct > 0:
            if coverage >= 0.8 and wilson >= _tier1_wilson:
                return 1
            elif (coverage >= _tier2_coverage
                  and correct_up_pct >= _tier2_correct_up):
                return 2
            elif correct_up_pct >= _tier3_correct_up:
                return 3
            else:
                return 4
        else:
            # FIX-R4: correct_up_pct == 0 时区分"无数据"与"全错"
            if coverage == 0.0:
                # 无分类数据（平台未推送tick），降级为Tier 3允许交易
                return 3
            return 4

    def _get_scoring_months(self, fid: int, available_months: List[str]) -> List[str]:
        """Resolve scoring months for a future (design report Section 4.4).

        Priority:
        1. month_mapping[product] -> extract YYMM (first 5)
        2. Fallback: available_months sorted lexicographically (first 5)

        Args:
            fid: future_internal_id
            available_months: months present in _status_counts for this future

        Returns:
            List of up to MAX_MONTHS_FOR_SCORING month strings in scoring order.
        """
        max_n = self.MAX_MONTHS_FOR_SCORING
        available_set = {str(month) for month in available_months if month}

        runtime_months = get_runtime_scoring_months(count=max_n)
        runtime_hits = [month for month in runtime_months if month in available_set]
        if runtime_hits:
            return runtime_hits

        # Try month_mapping first
        try:
            fp_info = self._get_params().get_instrument_meta(fid)
            if fp_info:
                product = str(fp_info.get('product', '')).upper()
                params = self._get_params()
                month_mapping = self._resolve_month_mapping(params)
                mapped = month_mapping.get(product)
                if mapped:
                    ym = str(fp_info.get('year_month', '') or '')
                    year_prefix = ym[:2] if len(ym) >= 4 else ''
                    scoring = []
                    for entry in mapped:
                        if isinstance(entry, str) and len(entry) >= 4:
                            # Contract ID format like "IF2606" -> extract "2606"
                            month = entry[-4:]
                            if month in available_set:
                                scoring.append(month)
                        elif isinstance(entry, int):
                            if not year_prefix:
                                continue
                            month = f"{year_prefix}{entry:02d}"
                            if month in available_set:
                                scoring.append(month)
                    if scoring:
                        return scoring[:max_n]
        except Exception as e:
            logging.debug("[_get_scoring_months] month_mapping lookup failed: %s", e)

        # Fallback: sort available months lexicographically (YYMM is naturally ordered)
        return sorted(available_months)[:max_n]

    def select_otm_targets_by_volume(self, future_internal_id: int = None) -> List[Dict[str, Any]]:
        """
        ✅ 分组D优化：使用 by_sort_bucket 索引，避免全量扫描

        优化前：遍历 _options_by_future_type 全量筛选 O(N)
        优化后：直接从排序桶读取 O(1)

        Args:
            future_internal_id: 可选，指定期货合约 internal_id。如果为 None，则自动选择最优期货。
        """
        # ✅ WidthStrengthCache自身就是缓存，直接使用self
        # FIX-P0-24: 原query_count仅在get_width_strength()中递增，导致print_status_diagnosis
        # 显示query_count=0误导诊断（实际select_otm_targets已被调用但未计数）
        self._query_count += 1
        with self._lock:
            # BUG-15 fix: lazy backfill is_main_month for legacy options (design report 4.4)
            self._backfill_is_main_month()
            # 第一步：五态采集 + 全维评分（使用 future_internal_id 作为键）
            future_scores = []
            for fid, month_data in self._status_counts.items():
                if future_internal_id is not None and fid != future_internal_id:
                    continue
                if not self._future_initialized.get(fid, False):
                    continue

                fp_info = self._get_params().get_instrument_meta(fid)
                if not fp_info:
                    continue

                future_rising = self._future_rising.get(fid, False)
                opt_type = 'CALL' if future_rising else 'PUT'

                # BUG-13 fix: use _get_scoring_months to order months by month_mapping
                raw_months = list(month_data.keys())
                months = self._get_scoring_months(fid, raw_months)
                n_months = len(months)
                if n_months == 0:
                    continue

                weights = self.resolve_month_weights(n_months)

                month_list = []
                for mth in months:
                    counts = month_data.get(mth, {}).get(opt_type, {})
                    cr = counts.get('correct_rise', 0)
                    wr = counts.get('wrong_rise', 0)
                    cf = counts.get('correct_fall', 0)
                    wf = counts.get('wrong_fall', 0)
                    other = counts.get('other', 0)
                    total = cr + wr + cf + wf + other
                    month_list.append({
                        'cr': cr, 'wr': wr, 'cf': cf, 'wf': wf, 'other': other, 'total': total
                    })

                correct_up_pct = self.compute_correct_up_pct(month_list, weights)
                noise_ratio = self.compute_noise_ratio(month_list, weights)
                coverage = self.compute_coverage(month_list, weights)
                # BUG-17 fix: compute diagnostic indicators tc/th/ra
                tc = correct_up_pct  # tc equals correct_up_pct per design report 3.2
                th = self.compute_th(month_list, weights)
                ra = self.compute_ra(month_list, weights)

                total_cr = sum(m['cr'] for m in month_list)
                total_rise = sum(m['cr'] + m['wr'] for m in month_list)
                wilson = self.wilson_lower_bound(total_cr, total_rise)

                tier = self.determine_tier(coverage, wilson, correct_up_pct, noise_ratio)

                # FIX-P0-3-RC10 (2026-07-23): Tier4自适应降级机制
                # 当>80%的期货被判定为Tier4时，自动将Tier4降级为Tier3
                # 避免冷启动/低数据量期间大量期货被排除
                # 第一遍：收集所有tier，暂不过滤
                future_scores.append({
                    'future_internal_id': fid,
                    'tier': tier,
                    'correct_up_pct': correct_up_pct,
                    'wilson': wilson,
                    'noise_ratio': noise_ratio,
                    'coverage': coverage,
                    'tc': tc,
                    'th': th,
                    'ra': ra,
                })

            # FIX-P0-3-RC10: Tier4自适应降级 — 检查Tier4占比
            _total_futures = len(future_scores)
            if _total_futures > 0:
                _tier4_count = sum(1 for fs in future_scores if fs['tier'] == 4)
                _tier4_ratio = _tier4_count / _total_futures
                _auto_upgrade = _tier4_ratio > 0.80

                if _auto_upgrade:
                    # FIX-C (2026-07-23): 使用参数覆盖而非修改全局SORTER_CONFIG
                    # 原代码临时修改SORTER_CONFIG['tier3_correct_up_threshold']存在线程安全问题
                    # 其他线程并发调用determine_tier可能读到被修改的阈值
                    _orig_tier3 = SORTER_CONFIG.get('tier3_correct_up_threshold', 0.35)
                    _relaxed_tier3 = max(0.05, _orig_tier3 * 0.5)
                    logging.warning(
                        "[FIX-P0-3-RC10] Tier4自适应降级触发: tier4=%d/%d(%.1f%%), "
                        "tier3_threshold %.2f→%.2f",
                        _tier4_count, _total_futures, _tier4_ratio * 100,
                        _orig_tier3, _relaxed_tier3,
                    )
                    # 重新计算tier（通过参数传递relaxed阈值，不修改全局配置）
                    for fs in future_scores:
                        if fs['tier'] == 4:
                            fs['tier'] = self.determine_tier(
                                fs['coverage'], fs['wilson'], fs['correct_up_pct'], fs['noise_ratio'],
                                tier3_correct_up_override=_relaxed_tier3,
                            )

                # 过滤Tier4（经过自适应降级后可能减少）
                _filtered_scores = [fs for fs in future_scores if fs['tier'] != 4]
                _post_filter_tier4 = sum(1 for fs in future_scores if fs['tier'] == 4)
                if _post_filter_tier4 > 0:
                    _tier4_skip_count = getattr(self, '_tier4_skip_count', 0) + _post_filter_tier4
                    self._tier4_skip_count = _tier4_skip_count
                    if _tier4_skip_count <= 5 or _tier4_skip_count % 100 == 0:
                        logging.info(
                            "[FIX-P0-19] Tier4过滤: %d/%d个期货被跳过(累计%d), auto_upgrade=%s",
                            _post_filter_tier4, _total_futures, _tier4_skip_count, _auto_upgrade,
                        )
                future_scores = _filtered_scores

            # 排序：tier升序, correct_up_pct降序, wilson降序
            future_scores.sort(key=lambda x: (x['tier'], -x['correct_up_pct'], -x['wilson']))
            top_futures = future_scores[:self.TOP_FUTURES_COUNT]
            # FIX-OPTION-RANKING-OBSERVABILITY-20260723: 添加期权五态排序可观测性日志
            # 根因: select_otm_targets_by_volume被调用(OptionTrading日志449条证明)但内部日志都是debug级别
            #   或条件性输出 → 日志中option_ranking/select_otm_targets/五态全部为0 → 无法验证排序是否真正起作用
            # 修复: 添加INFO级别排序结果日志，关键字[OPTION_RANKING]，便于从日志验证功能运行
            # 原则: 每100次调用输出1次摘要(首次5次每次输出)，包含候选数/过滤后/top数/tier分布/首位信息
            _opt_ranking_count = getattr(self, '_opt_ranking_log_count', 0) + 1
            self._opt_ranking_log_count = _opt_ranking_count
            if _opt_ranking_count <= 5 or _opt_ranking_count % 100 == 0:
                _tier_summary = {}
                for fs in future_scores:
                    _t = fs['tier']
                    _tier_summary[_t] = _tier_summary.get(_t, 0) + 1
                _top_info = 'None'
                if top_futures:
                    _tf = top_futures[0]
                    _top_info = "fid=%s tier=%d cup=%.4f wilson=%.4f" % (
                        _tf['future_internal_id'], _tf['tier'],
                        _tf['correct_up_pct'], _tf['wilson'])
                logging.info(
                    "[OPTION_RANKING] 五态排序完成: 候选=%d 过滤Tier4后=%d top=%d tier分布=%s 首位=%s call#=%d",
                    _total_futures, len(future_scores), len(top_futures), _tier_summary, _top_info, _opt_ranking_count
                )
            if not top_futures:
                return []

            # 第二步：✅ 从排序桶直接读取最优期权（避免全量扫描）
            targets = []
            for rank, fs in enumerate(top_futures, start=1):
                fid = fs['future_internal_id']
                try:
                    from position.position_service import get_position_service
                    pos_svc = get_position_service()
                except Exception as e:
                    logging.warning(f"[select_otm_targets_by_volume] PositionService导入失败: {e}")
                    pos_svc = None

                if not self._get_params().get_instrument_meta(fid):
                    continue

                future_rising = self._future_rising.get(fid, False)
                opt_type = 'CALL' if future_rising else 'PUT'

                all_buckets = self._sort_buckets.get(fid, {})
                best_candidates = []
                for mth in all_buckets:
                    mth_candidates = self.select_from_sort_bucket(fid, mth, opt_type, top_n=1)
                    if mth_candidates and mth_candidates[0]:
                        best_candidates.append(mth_candidates[0])
                if not best_candidates:
                    logging.debug(f"[select_otm_targets] Sort bucket empty for future_internal_id={fid}, skipping")
                    continue

                best_candidates.sort(key=lambda x: x['volume'], reverse=True)

                # BUG-14 fix: iterate candidates, skip those with existing positions
                best = None
                for cand in best_candidates:
                    if pos_svc and hasattr(pos_svc, 'has_position') and pos_svc.has_position(cand['instrument_id']):
                        continue
                    best = cand
                    break
                if best is None:
                    logging.debug(f"[select_otm_targets] All candidates have positions for fid={fid}, skipping")
                    continue

                lots = self.TIER1_LOTS if fs['tier'] == 1 else self.TIER2_LOTS
                # FIX-21 RC-25: 补充detect_spring所需的期权元数据（6字段）
                # 根因: targets缺少option_instrument_id/iv/premium_price/days_to_expiry/
                #   order_flow_imbalance/option_chain_activity，导致detect_spring使用默认值
                #   iv=0.0→update_iv返回50.0>5.0(过滤), days_to_expiry=30>5(过滤)→S3全部失败
                _fp_meta = self._get_params().get_instrument_meta(fid) or {}
                _fp_prod = str(_fp_meta.get('product', '')).upper()
                _fp_ym = str(_fp_meta.get('year_month', '') or '')
                _underlying = self._future_prices_by_month.get(_fp_prod, {}).get(_fp_ym, 0.0)
                _opt_inst = best.get('instrument_id', '')
                # FIX-S4-WAF-20260720: underlying_future_instrument_id空值守卫
                # 根因: _fp_meta可能为None或instrument_id为空 → fallback到期权ID
                #   → strategy_business_layer.py L937 `or t.get('instrument_id', '')` 兜底为期权ID
                #   → bss.on_tick收到期权ID而非期货ID → box存储key错乱 → detect_spring失败
                # 修复: 空时跳过该target, 避免污染下游S4-Spring集成路径
                _future_inst_id_str = str(_fp_meta.get('instrument_id', ''))
                if not _future_inst_id_str or _underlying <= 0:
                    logging.debug(
                        "[select_otm_targets_by_volume] SKIP: fid=%s future_inst_id=%r underlying=%.4f (空值守卫)",
                        fid, _future_inst_id_str, _underlying,
                    )
                    continue
                # FIX-S4-ROOT-20260720-v2: 用year_month推算DTE，不再依赖不存在的DataWriterMixin
                # 原DataWriterMixin._compute_days_to_expiry/_compute_implied_volatility不存在
                # → ImportError被except捕获 → _dte/_iv=None → 永远fallback到默认值(半拉子)
                # 修复: 直接从year_month字段推算DTE，IV用简单moneyness近似
                _dte = self._approx_dte_from_year_month(best.get('month', ''))
                _iv = self._approx_iv_from_moneyness(
                    best.get('price', 0.0), best.get('strike_price', 0.0),
                    _underlying, _dte, best.get('option_type', ''))
                # BUG-11 fix: include all design report 9.3 fields
                targets.append({
                    **best,
                    'lots': lots,
                    'product_rank': rank,
                    # CHAIN-BUG-2 fix: 买权策略(买CALL/买PUT均为BUY)显式声明方向，避免下游隐式默认
                    'direction': 'BUY',
                    'tier': fs['tier'],
                    'correct_up_pct': fs['correct_up_pct'],
                    'wilson': fs['wilson'],
                    'coverage': fs['coverage'],
                    'noise_ratio': fs['noise_ratio'],
                    'tc': fs['tc'],
                    'th': fs['th'],
                    'ra': fs['ra'],
                    # FIX-S4-ROOT-20260720: 期货元数据（S4-Spring box检测和strike_close条件需要）
                    'underlying_future_instrument_id': str(_fp_meta.get('instrument_id', '')),
                    'underlying_future_price': _underlying,
                    # FIX-21 RC-25: detect_spring所需期权元数据
                    'option_instrument_id': _opt_inst,
                    'premium_price': best.get('price', 0.0),
                    'days_to_expiry': _dte if _dte is not None else 3,
                    'iv': _iv if _iv and _iv > 0 else 0.15,
                    'order_flow_imbalance': 0.0,
                    'option_chain_activity': 0.0,
                })

            return targets

    def _ensure_signal_sorters(self):
        if self._sorter_a is None:
            # v2.5: AlphaEngine/ClusterView/GlobalView（旧名 IntraProductSorter 等为别名）
            from data.three_layer_sort import (
                AlphaEngine, ClusterView, GlobalView,
            )
            # v2.8 §21: 构建排序配置
            # 优先级：外部覆盖(回测) > params_service(实盘运行时) > SORTER_CONFIG默认
            sorter_config = self._build_sorter_config()
            self._sorter_a = AlphaEngine(sorter_config=sorter_config) if sorter_config else AlphaEngine()
            self._sorter_b = ClusterView()
            self._sorter_c = GlobalView()

    def _build_sorter_config(self) -> Optional[Dict[str, Any]]:
        """v2.8 §21: 构建排序配置字典。

        优先级：
        1. 外部覆盖（_sorter_config_override，回测驱动器通过 configure_sorter 注入）
        2. params_service 运行时配置（实盘从 params_default.json 读取）
        3. None（回退到 AlphaEngine 内部的 SORTER_CONFIG 默认值）

        Returns:
            排序配置字典或 None（使用默认）
        """
        # 1. 外部覆盖优先（回测三方案切换）
        if self._sorter_config_override is not None:
            return dict(self._sorter_config_override)

        # 2. 从 params_service 读取运行时配置（实盘）
        try:
            params = None
            if self._facade is not None:
                params = self._facade._get_params() if hasattr(self._facade, '_get_params') else None
            if params is None:
                from config.params_service import get_params_service as _get_ps
                params = _get_ps()

            scoring_scheme = params.get_str('sorter.scoring_scheme', '') if hasattr(params, 'get_str') else ''
            output_mode = params.get_str('sorter.output_mode', '') if hasattr(params, 'get_str') else ''
            if scoring_scheme or output_mode:
                from config.tvf_param_loader import SORTER_CONFIG as _DEFAULT_SC
                cfg = dict(_DEFAULT_SC)
                if scoring_scheme and scoring_scheme in ('scheme_1', 'scheme_2', 'scheme_3', 'all'):
                    cfg['scoring_scheme'] = scoring_scheme
                if output_mode and output_mode in ('research', 'production'):
                    cfg['output_mode'] = output_mode
                return cfg
        except Exception:
            pass

        # 3. 回退到默认（AlphaEngine 内部使用 SORTER_CONFIG）
        return None

    def select_otm_targets_signal_sources(self, future_internal_id: int = None, signal_source: str = 'C') -> List[Dict[str, Any]]:
        """并列信号源排序入口（最终落地方案v2.0）。

        三个信号源A/B/C各自独立排序，由signal_source参数选择使用哪个输出。
        默认使用信号源C（全局排序）的输出。
        """
        signal_source = str(signal_source or 'C').upper()
        if signal_source not in ('A', 'B', 'C'):
            signal_source = 'C'
        self._ensure_signal_sorters()
        # FIX-P0-24: 与select_otm_targets_by_volume一致，递增query_count
        self._query_count += 1

        source_a_results = []

        with self._lock:
            self._backfill_is_main_month()

            for fid, month_data in self._status_counts.items():
                if future_internal_id is not None and fid != future_internal_id:
                    continue
                if not self._future_initialized.get(fid, False):
                    continue

                fp_info = self._get_params().get_instrument_meta(fid)
                if not fp_info:
                    continue

                product = str(fp_info.get('product', '')).upper()
                future_rising = self._future_rising.get(fid, False)
                opt_type = 'CALL' if future_rising else 'PUT'

                raw_months = list(month_data.keys())
                months = self._get_scoring_months(fid, raw_months)
                if not months:
                    continue

                month_data_list = []
                for idx, mth in enumerate(months):
                    counts = month_data.get(mth, {}).get(opt_type, {})
                    cr = counts.get('correct_rise', 0)
                    wr = counts.get('wrong_rise', 0)
                    cf = counts.get('correct_fall', 0)
                    wf = counts.get('wrong_fall', 0)
                    other = counts.get('other', 0)

                    month_type = 'front_month' if idx == 0 else (
                        'next_month' if idx == 1 else 'quarter_month'
                    )

                    bucket_entries = self.select_from_sort_bucket(fid, mth, opt_type, top_n=1)
                    liquidity = 0.0
                    if bucket_entries:
                        vol = bucket_entries[0].get('volume', 0)
                        liquidity = min(1.0, vol / 500.0) if vol > 0 else 0.0

                    month_data_list.append({
                        'month': mth,
                        'counts': {'CR': cr, 'CF': cf, 'WR': wr, 'WF': wf, 'Other': other},
                        'last_update': {s: time.time() for s in ('CR', 'CF', 'WR', 'WF', 'Other')},
                        'greeks': {'delta': 0.5, 'gamma': 0.03, 'theta': -0.5, 'vega': 0.05},
                        'option_price': 100.0,
                        'month_type': month_type,
                        'liquidity': liquidity,
                    })

                if not month_data_list:
                    continue

                result_a = self._sorter_a.rank(product, month_data_list)
                result_a['future_internal_id'] = fid
                result_a['opt_type'] = opt_type
                source_a_results.append(result_a)

        if not source_a_results:
            return []

        result_b = self._sorter_b.rank('all', source_a_results)
        result_c = self._sorter_c.rank(source_a_results)

        if signal_source == 'A':
            sorted_products = []
            for r in source_a_results:
                # v2.5: 优先使用 product_score_ts / final_score，向后兼容 best_score
                score = r.get('final_score', r.get('product_score_ts', r.get('best_score', 0.0)))
                sorted_products.append({
                    'product_id': r.get('product_id', ''),
                    'product_score': r.get('product_score', score),
                    'product_score_ts': r.get('product_score_ts', score),
                    'final_score': r.get('final_score', score),
                    'best_score': score,  # 向后兼容
                    'correct_up_pct': r.get('correct_up_pct', 0.0),
                    'tier': r.get('tier', 4),
                    'best_month': r.get('best_month'),
                })
            sorted_products.sort(key=lambda x: -x['best_score'])
            for i, p in enumerate(sorted_products):
                p['global_rank'] = i + 1
                p['global_percentile'] = (len(sorted_products) - i) / len(sorted_products) if sorted_products else None
        elif signal_source == 'B':
            sorted_products = result_b.get('sorted_products', [])
        else:
            sorted_products = result_c.get('sorted_products', [])

        # FIX-OPTION-RANKING-OBSERVABILITY-20260723: select_otm_targets_signal_sources也添加排序结果日志
        # 根因: 该函数是另一个排序入口(signal_source A/B/C)，与select_otm_targets_by_volume并列
        #   但之前只在select_otm_targets_by_volume添加了[OPTION_RANKING]日志，此函数没有 → 半拉子工程
        # 修复: 补充可观测性日志，确保两个排序入口都有日志输出，便于从日志验证排序功能运行
        _opt_ranking_count_ss = getattr(self, '_opt_ranking_log_count_ss', 0) + 1
        self._opt_ranking_log_count_ss = _opt_ranking_count_ss
        if _opt_ranking_count_ss <= 5 or _opt_ranking_count_ss % 100 == 0:
            _tier_summary_ss = {}
            for _sp in sorted_products:
                _t = _sp.get('tier', 4)
                _tier_summary_ss[_t] = _tier_summary_ss.get(_t, 0) + 1
            _top_info_ss = 'None'
            if sorted_products:
                _sp0 = sorted_products[0]
                _top_info_ss = "pid=%s tier=%s score=%.4f" % (
                    _sp0.get('product_id', ''), _sp0.get('tier', 4),
                    _sp0.get('best_score', 0.0))
            logging.info(
                "[OPTION_RANKING] 信号源%s排序完成: 候选=%d tier4过滤后=%d tier分布=%s 首位=%s call#=%d",
                signal_source, len(source_a_results), len(sorted_products), _tier_summary_ss, _top_info_ss, _opt_ranking_count_ss
            )

        targets = []
        for sp in sorted_products:
            if sp.get('tier', 4) == 4:
                continue
            fid = None
            # v2.5: 同时获取 month_candidates 供按优先级尝试
            month_candidates_info = None
            for r in source_a_results:
                if r.get('product_id') == sp.get('product_id'):
                    fid = r.get('future_internal_id')
                    month_candidates_info = r.get('month_candidates') or r.get('candidates')
                    break
            if fid is None:
                continue

            future_rising = self._future_rising.get(fid, False)
            opt_type = 'CALL' if future_rising else 'PUT'

            # v2.8.1: 优先按 month_candidates 顺序尝试（按 primary_score 降序，scheme 感知）
            # 若 month_candidates 不可用，回退到遍历所有月份桶（向后兼容）
            best_candidates = []
            if month_candidates_info:
                # v2.8.1: 按候选列表优先级尝试，利用 score_delta_to_next / rank_confidence 决策
                for cand_info in month_candidates_info:
                    mth = cand_info.get('month')
                    if not mth:
                        continue
                    mth_candidates = self.select_from_sort_bucket(fid, mth, opt_type, top_n=1)
                    if mth_candidates and mth_candidates[0]:
                        entry = mth_candidates[0]
                        # v2.8.1: 附加候选诊断信息（含 v2.8 D 字段 + 向后兼容 net_score）
                        entry['rank_confidence'] = cand_info.get('rank_confidence', 0.0)
                        entry['score_delta_to_next'] = cand_info.get('score_delta_to_next')
                        entry['net_score'] = cand_info.get('net_score', 0.0)
                        entry['D'] = cand_info.get('D', 0.0)
                        entry['primary_score'] = cand_info.get('primary_score', cand_info.get('net_score', 0.0))
                        best_candidates.append(entry)
            else:
                # 向后兼容：无 month_candidates 时遍历所有月份桶
                all_buckets = self._sort_buckets.get(fid, {})
                for mth in all_buckets:
                    mth_candidates = self.select_from_sort_bucket(fid, mth, opt_type, top_n=1)
                    if mth_candidates and mth_candidates[0]:
                        best_candidates.append(mth_candidates[0])
            if not best_candidates:
                continue

            # v2.8.1: month_candidates 已按 primary_score 降序（scheme 感知），
            # 优先使用高 rank_confidence 的候选
            # 仅当无 month_candidates_info 时才按 volume 排序（向后兼容）
            if month_candidates_info:
                # 保持 month_candidates 的 primary_score 优先顺序
                pass
            else:
                best_candidates.sort(key=lambda x: x.get('volume', 0), reverse=True)

            try:
                from position.position_service import get_position_service
                pos_svc = get_position_service()
            except Exception:
                pos_svc = None

            best = None
            for cand in best_candidates:
                if pos_svc and hasattr(pos_svc, 'has_position') and pos_svc.has_position(cand['instrument_id']):
                    continue
                best = cand
                break
            if best is None:
                continue

            lots = self.TIER1_LOTS if sp.get('tier') == 1 else self.TIER2_LOTS
            # FIX-S4-ROOT-20260720: 补充期货/期权元数据字段
            # 根因: targets缺少underlying_future_instrument_id/underlying_future_price/
            #   option_instrument_id/iv/premium_price/days_to_expiry等字段，
            #   导致strategy_business_layer.py用期权ID和期权premium作为期货ID和期货价格
            #   传入detect_spring→strike_close条件永远失败→S4零下单
            _fp_meta = self._get_params().get_instrument_meta(fid) or {}
            _fp_prod = str(_fp_meta.get('product', '')).upper()
            _fp_ym = str(_fp_meta.get('year_month', '') or '')
            _underlying_price = self._future_prices_by_month.get(_fp_prod, {}).get(_fp_ym, 0.0)
            _future_inst_id = str(_fp_meta.get('instrument_id', ''))
            _opt_inst = best.get('instrument_id', '')
            # FIX-S4-WAF-20260720: underlying_future_instrument_id空值守卫(与select_otm_targets_by_volume对齐)
            # 根因: _future_inst_id为空时, strategy_business_layer.py L964 fallback到期权ID
            #   → bss.on_tick收到期权ID, box存储key错乱, detect_spring失败
            # 修复: 空时跳过该target, 避免污染下游S4-Spring集成路径
            if not _future_inst_id or _underlying_price <= 0:
                logging.debug(
                    "[select_otm_targets_signal_sources] SKIP: fid=%s future_inst_id=%r underlying=%.4f (空值守卫)",
                    fid, _future_inst_id, _underlying_price,
                )
                continue
            # FIX-S4-ROOT-20260720-v2: 用year_month推算DTE，不再依赖不存在的DataWriterMixin
            _dte = self._approx_dte_from_year_month(best.get('month', ''))
            _iv = self._approx_iv_from_moneyness(
                best.get('price', 0.0), best.get('strike_price', 0.0),
                _underlying_price, _dte, best.get('option_type', ''))
            targets.append({
                **best,
                'lots': lots,
                'product_rank': sp.get('global_rank', 0),
                'direction': 'BUY',
                'tier': sp.get('tier', 4),
                'correct_up_pct': sp.get('correct_up_pct', 0.0),
                'wilson': sp.get('wilson', 0.0),       # FIX-P2-2-RC8 (2026-07-23): 从硬编码0.0→真实值
                'coverage': sp.get('coverage', 0.0),   # FIX-P2-2-RC8: 从硬编码0.0→真实值
                'noise_ratio': sp.get('noise_ratio', 0.0), # FIX-P2-2-RC8: 从硬编码0.0→真实值
                'signal_source': signal_source,
                'global_percentile': sp.get('global_percentile'),
                # FIX-S4-ROOT-20260720: 期货元数据（S4-Spring box检测和strike_close条件需要）
                'underlying_future_instrument_id': _future_inst_id,
                'underlying_future_price': _underlying_price,
                # FIX-S4-ROOT-20260720: 期权元数据（FIX-21 RC-25对齐select_otm_targets_by_volume）
                'option_instrument_id': _opt_inst,
                'premium_price': best.get('price', 0.0),
                'days_to_expiry': _dte if _dte is not None else 3,
                'iv': _iv if _iv and _iv > 0 else 0.15,
                'order_flow_imbalance': 0.0,
                'option_chain_activity': 0.0,
            })

        return targets


    def calculate_option_width(
        self,
        instrument_id: str,
        underlying_price: Optional[float] = None,
        strike_price: float = 0.0,
        option_type: str = 'CALL'
    ) -> Dict[str, Any]:
        """计算期权宽度 - P2优化：优先使用固化数据，避免重复计算
        
        Args:
            instrument_id: 期权合约代码
            underlying_price: 标的价格（可选，如不传则从缓存获取）'
            strike_price: 行权价（可选，如为 0 则从缓存读取固化值）
            option_type: 期权类型 ('CALL'/'PUT')
            
        Returns:
            Dict: 计算结果 {width, moneyness, is_otm, ...}
        """
        try:
            source = 'fallback'
            option_context = self._resolve_option_context(instrument_id)

            # ✅ P2 优化：直接读取固化数据（WidthStrengthCache自身即缓存）'
            # 通过 instrument_id -> internal_id 查找
            iid = self._instrument_id_to_internal_id.get(
                self._normalize_instrument_id(instrument_id)
            )
            info = self._option_info.get(iid) if iid is not None else None
            if info:
                source = 'cache'
                # 直接使用固化字段
                if strike_price <= 0:
                    strike_price = info['strike_price']
                if self._normalize_option_type(option_type) == 'CALL':
                    option_type = info['option_type']
                
                # 从缓存获取对应月份的期货价格
                if underlying_price is None:
                    underlying_future_id = info.get('underlying_future_id')
                    month = info['month']
                    underlying_price = self._get_future_price_by_id_and_month(underlying_future_id, month)
            
            if strike_price <= 0:
                strike_price = option_context['strike_price'] or strike_price
            if (not option_type or self._normalize_option_type(option_type) == 'CALL') and option_context['option_type']:
                option_type = option_context['option_type'] or option_type

            if underlying_price is None or underlying_price <= 0:
                month = option_context['month']
                underlying_future_id = option_context['underlying_future_id']
                if underlying_future_id not in (None, '') and month:
                    cached_price = self._get_future_price_by_id_and_month(underlying_future_id, month)
                    if cached_price > 0:
                        underlying_price = cached_price
                        if source == 'fallback':
                            source = 'cache'

                if underlying_price is None or underlying_price <= 0:
                    underlying_price = self._get_underlying_price_by_future_id(underlying_future_id)
            
            # 再次降级：解析行权价
            if strike_price <= 0:
                strike_price = self._extract_strike_price(instrument_id)
            
            # 参数验证
            if not instrument_id or (underlying_price is not None and underlying_price <= 0) or strike_price <= 0:
                logging.error(f"[WidthStrengthCache] Invalid params: {instrument_id}, {underlying_price}, {strike_price}")
                return {}

            # 计算虚实值程度
            opt_type_normalized = self._normalize_option_type(option_type)
            if opt_type_normalized == 'CALL':
                moneyness = (underlying_price - strike_price) / strike_price * 100
                is_otm = underlying_price < strike_price
            else:
                moneyness = (strike_price - underlying_price) / strike_price * 100
                is_otm = underlying_price > strike_price
            
            # 计算宽度（绝对值）'
            width = abs(underlying_price - strike_price)
            
            result = {
                'instrument_id': instrument_id,
                'underlying_price': to_float32(underlying_price) if underlying_price else None,
                'strike_price': to_float32(strike_price),
                'option_type': self._normalize_option_type(option_type),
                'width': to_float32(width),
                'moneyness_percent': to_float32(moneyness),
                'is_otm': is_otm,
                'is_itm': not is_otm,
                'calculated_at': datetime.now(CHINA_TZ),
                'source': source,
            }

            logging.debug(f"[WidthStrengthCache] Calculated: {instrument_id} width={width:.2f}, moneyness={moneyness:.2f}%")
            return result
            
        except Exception as e:
            logging.error(f"[WidthStrengthCache] Calculation error: {e}")
            return {}
    
    # ======================== ✅ DataService 集成增强功能 ========================

    def calculate_option_width_strength(
        self,
        specified_months: Optional[List[str]] = None,  # 如 ['2601', '2602', '2603', '2604', '2605']
        underlying_future_id: int = None,  # ✅ 期货合约 internal_id（主键）'
        option_type_filter: Optional[str] = None,  # 'CALL' / 'PUT' / None
        min_width_threshold: float = 4.0
    ) -> Dict[str, Any]:
        """
        计算期权宽度强度 - 优先使用内存缓存，降级到 DuckDB
        
        核心逻辑：
        1. 优先从 WidthStrengthCache 获取（O(1)，< 100 ns）
        2. 如果缓存不可用，降级到 DuckDB 查询（10-100 μs）
        
        Args:
            specified_months: 指定月份列表（如 ['2601', '2602', '2603', '2604', '2605']）
                             必须显式指定，不自动生成
            underlying_future_id: 期货合约 internal_id（主键）'
            option_type_filter: 期权类型过滤 ('CALL'/'PUT'/None)
            min_width_threshold: 最小宽度阈值
        
        Returns:
            Dict: {
                'product': str,                    # 产品代码
                'future_rising': bool,             # 期货是否上涨
                'width_strength': int,             # 宽度强度
                'month_details': Dict,             # 各月详情
                'all_sync': bool,                  # 是否全部同步
                'calculated_at': datetime
            }
        """
        # ✅ 优先使用内存缓存（O(1) 查询）'
        if specified_months and underlying_future_id:
            try:
                # 🟡 中优先级：使用封装方法访问，避免直接访问私有属性
                future_rising = self.get_future_rising(underlying_future_id)
                width_strength = self.get_width_strength(
                    underlying_future_id, specified_months, option_type_filter
                )
                
                # 构建月详情
                month_details = {}
                for month in specified_months:
                    cnt = self.get_width_strength(
                        underlying_future_id, [month], option_type_filter
                    )
                    month_details[month] = {
                        'sync_otm_count': cnt,
                        'future_rising': future_rising
                    }
                
                # ✅ 从 id_cache 获取 product
                info = self._get_params().get_instrument_meta(underlying_future_id)
                product = str(info.get('product', '')).upper() if info else ''
                
                return {
                    'product': product,
                    'future_rising': future_rising,
                    'width_strength': width_strength,
                    'month_details': month_details,
                    'all_sync': width_strength > 0,
                    'total_months': len(specified_months),
                    'calculated_at': datetime.now(CHINA_TZ)
                }
            except Exception as e:
                logging.error(f"[WidthStrengthCache] WidthStrengthCache calculation failed: {e}")
                # ✅ 序号132修复：内存缓存为唯一策略，不再降级到DB查询
                return self._empty_width_result(product, specified_months)
    
    # ✅ 接口唯一：_empty_width_result唯一实现，同文件内调用

    def _empty_width_result(self, product: str, months: List[str]) -> Dict[str, Any]:
        """返回空的宽度强度结果（降级失败时使用）"""
        month_details = {month: {'sync_otm_count': 0, 'future_rising': False} for month in months}
        return {
            'product': product,
            'future_rising': False,
            'width_strength': 0,
            'month_details': month_details,
            'all_sync': False,
            'total_months': len(months),
            'calculated_at': datetime.now(CHINA_TZ),
            'source': 'empty_fallback'
        }

    def select_trading_targets(
        self,
        width_strength_results: Dict[str, Dict[str, Any]],
        top_n: int = 5,
        min_width_threshold: float = 4.0
    ) -> List[Dict[str, Any]]:
        """
        按照宽度三原则选择交易标的
        
        宽度三原则：
        1. 同月份：同一到期月份的期权（已在计算时分别统计）'
        2. 同方向：都是看涨或都是看跌（已根据期货方向选择）
        3. 同虚实度：都是虚值期权（已过滤）
        
        Args:
            width_strength_results: {underlying_symbol: width_strength_result}
            top_n: 返回前 N 个交易标的
            min_width_threshold: 最小宽度阈值（默认>4）
        
        Returns:
            List[Dict]: 交易标的列表（已排序）
        """
        try:
            # 1. 收集所有结果
            all_results = []
            for symbol, result in width_strength_results.items():
                if not result:
                    continue
                
                width_strength = result.get('width_strength', 0)
                all_sync = result.get('all_sync', False)
                
                # 2. 过滤：宽度必须大于阈值
                if width_strength <= min_width_threshold:
                    continue
                
                # 3. 添加到候选列表
                all_results.append({
                    'symbol': symbol,
                    'width_strength': width_strength,
                    'all_sync': all_sync,
                    'future_rising': result.get('future_rising', False),
                    'month_details': result.get('month_details', {}),
                    'total_months': result.get('total_months', 0),
                    'priority': 0 if all_sync else 1  # 全部同步优先级更高
                })
            
            # 4. 排序规则：
            # - 优先级 1：全部同步 > 部分同步
            # - 优先级 2：宽度强度从大到小
            all_results.sort(key=lambda x: (x['priority'], -x['width_strength']))
            
            # 5. 截取 Top N
            trading_targets = all_results[:top_n]
            
            # 6. 分配信号类型
            signals = []
            if trading_targets:
                max_width = trading_targets[0]['width_strength']
                
                for i, target in enumerate(trading_targets):
                    if target['all_sync'] and target['width_strength'] == max_width:
                        signal_type = "最优信号"
                    elif target['all_sync']:
                        signal_type = "全同步信号"
                    elif not target['all_sync'] and target['width_strength'] == max_width:
                        signal_type = "次优信号"
                    else:
                        signal_type = "部分同步信号"
                    
                    signals.append({
                        **target,
                        'signal_type': signal_type,
                        'rank': i + 1
                    })
            
            return signals
        
        except Exception as e:
            logging.error(f"[WidthStrengthCache] Error in select_trading_targets: {e}", exc_info=True)
            return []
    
    # ✅ ID唯一：clear_cache统一接口，服务=TTypeService


_WidthCacheQueryMixin = WidthCacheQueryService

