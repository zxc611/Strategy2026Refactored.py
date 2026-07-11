# MODULE_ID: M1-058
"""
width_cache_state_mixin.py — 状态管理Mixin
包含: _WidthCacheStateMixin
"""
from __future__ import annotations

import threading
import logging
import heapq
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from ali2026v3_trading.infra.shared_utils import to_float32, CHINA_TZ
from ali2026v3_trading.data.width_cache_types import (
    _NoOpDiagnosisProbeManager, SortEntry,
    MAX_OPTION_CACHE_SIZE, MAX_INSTRUMENT_ID_MAP_SIZE, MAX_FUTURE_CACHE_SIZE,
    MAX_STATUS_COUNTS_SIZE, MAX_SORT_BUCKETS_FUTURES, _CACHE_TTL_SECONDS,
)


class WidthCacheStateService:

    def __init__(self, tracked_option_tick_ids: Optional[set] = None, params_service=None, strategy_id: Optional[str] = None):
        self._lock = threading.RLock()
        self._strategy_id = strategy_id or 'global'
        self._tracked_option_tick_ids = set(tracked_option_tick_ids) if tracked_option_tick_ids else set()
        self._params_service = params_service
        
        # 产品 -> 月份 -> 期货最新价 (P1 修复：实现分月精准定价)
        self._future_prices_by_month: Dict[str, Dict[str, float]] = defaultdict(dict)
        # future_internal_id -> 期货前一个价格（用于方向判断）'
        self._future_prev_price: Dict[int, float] = {}
        # future_internal_id -> 期货是否上涨（最新方向）
        self._future_rising: Dict[int, bool] = {}
        
        # ---- internal_id 主键化：单合约运行时状态缓存 ----
        # 期权 internal_id -> 最新价
        self._option_price: Dict[int, float] = {}
        # 期权 internal_id -> 前一个价格
        self._option_prev_price: Dict[int, float] = {}
        # 期权 internal_id -> 最近一笔有效且不同的价格，用于定义方向锚点
        self._option_last_distinct_price: Dict[int, Optional[float]] = {}
        # 期权 internal_id -> 最近一次有效方向，True=上涨，False=下跌，None=尚无方向
        self._option_last_direction: Dict[int, Optional[bool]] = {}
        # 期权 internal_id -> 基本信息（只保留 internal_id 关系字段，不缓存产品映射语义）
        self._option_info: Dict[int, Dict] = {}
        # 期权 internal_id -> 累计成交量（用于虚值开仓排序）
        self._option_volume: Dict[int, float] = {}
        self._option_daily_volume: Dict[int, float] = {}
        self._current_trading_date: str = ''
        
        # ✅ 索引：future_internal_id -> 类型 -> internal_id 列表（加速期货方向变化时的重算）'
        self._options_by_future_type: Dict[int, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))
        
        # future_internal_id -> 月份 -> option_type -> 五类状态计数
        # 1. correct_rise, 2. wrong_rise, 3. correct_fall, 4. wrong_fall, 5. other
        self._status_counts: Dict[int, Dict[str, Dict[str, Dict[str, int]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(self._new_status_bucket))
        )
        # ✅ 同步虚值期权计数（排序二原则核心）：future_internal_id -> 月份 -> option_type -> count
        self._sync_otm_count: Dict[int, Dict[str, Dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
        # future_internal_id -> 所有已注册月份列表
        self._months: Dict[int, List[str]] = defaultdict(list)
        
        # 期权 internal_id -> 上次是否同步（用于增量更新）
        self._sync_flag: Dict[int, bool] = {}
        # 期权 internal_id -> 当前五态状态（用于保证 _status_counts 始终表示当前状态）
        self._current_status: Dict[int, str] = {}
        
        # ✅ 期货是否已收到足够数据以判断方向（避免注册初值触发伪方向）
        self._future_initialized: Dict[int, bool] = {}
        
        # ✅ instrument_id -> internal_id 入口映射（兼容层：平台回调仍传 instrument_id）
        self._instrument_id_to_internal_id: Dict[str, int] = {}
        # internal_id -> instrument_id 反向映射
        self._internal_id_to_instrument_id: Dict[int, str] = {}
        
        # ✅ 性能监控：查询统计
        self._query_count: int = 0
        self._cache_hits: int = 0
        self._cache_misses: int = 0
        
        # ✅ 分组D准备：by_sort_bucket 索引结构（future_internal_id -> 月份 -> option_type -> 排序堆）
        # 使用heapq维护前N个最优期权，避免全量扫描
        self._sort_buckets: Dict[int, Dict[str, Dict[str, List[SortEntry]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        # 每个桶的最大容量（只保留前N个）
        self._sort_bucket_max_size: int = 50

        self._buckets_dirty: set = set()
        self._pending_classify_options: Dict[int, List[Dict]] = defaultdict(list)

        self._option_info_timestamps: Dict[int, float] = {}
        self._instrument_id_timestamps: Dict[str, float] = {}
        self._last_cache_cleanup: float = time.time()
        # AS-01：守恒断言定时检查
        self._last_conservation_audit: float = 0.0

    def _decrement_status_count_for_option(self, internal_id: int, info: Optional[Dict[str, Any]] = None) -> None:
        info = info or self._option_info.get(internal_id)
        if not info:
            return
        status = self._current_status.get(internal_id)
        if status is None:
            return
        future_id = info.get('underlying_future_id')
        month = info.get('month')
        opt_type = info.get('option_type')
        bucket = self._status_counts.get(future_id, {}).get(month, {}).get(opt_type)
        if bucket is not None:
            bucket[status] = max(0, bucket.get(status, 0) - 1)

    def _remove_option_runtime_state(self, internal_id: int, opt_info: Optional[Dict[str, Any]] = None) -> None:
        opt_info = opt_info or self._option_info.get(internal_id)
        self._decrement_status_count_for_option(internal_id, opt_info)
        instrument_id = (opt_info or {}).get('instrument_id') or self._internal_id_to_instrument_id.get(internal_id)
        if instrument_id:
            self._instrument_id_to_internal_id.pop(instrument_id, None)
            self._instrument_id_timestamps.pop(instrument_id, None)
        if opt_info:
            future_id = opt_info.get('underlying_future_id')
            opt_type = opt_info.get('option_type')
            if future_id is not None and opt_type:
                option_list = self._options_by_future_type.get(future_id, {}).get(opt_type, [])
                if internal_id in option_list:
                    option_list[:] = [iid for iid in option_list if iid != internal_id]
        self._option_info.pop(internal_id, None)
        self._option_prev_price.pop(internal_id, None)
        self._option_last_distinct_price.pop(internal_id, None)
        self._option_last_direction.pop(internal_id, None)
        self._option_volume.pop(internal_id, None)
        self._option_daily_volume.pop(internal_id, None)
        self._sync_flag.pop(internal_id, None)
        self._current_status.pop(internal_id, None)
        self._option_price.pop(internal_id, None)
        self._option_info_timestamps.pop(internal_id, None)
        self._internal_id_to_instrument_id.pop(internal_id, None)

    def _enforce_cache_size_limit(self) -> None:
        _now = time.time()
        if _now - self._last_cache_cleanup < 60:
            return
        self._last_cache_cleanup = _now
        _expired_cutoff = _now - _CACHE_TTL_SECONDS
        _expired_ids = [k for k, ts in self._option_info_timestamps.items() if ts < _expired_cutoff]
        _skipped_active = 0
        for _eid in _expired_ids:
            if self._option_price.get(_eid, 0) > 0:
                _skipped_active += 1
                self._option_info_timestamps[_eid] = _now
                continue
            _opt_info = self._option_info.get(_eid)
            self._remove_option_runtime_state(_eid, _opt_info)
        if _expired_ids:
            logging.warning("[CacheEvict] TTL淘汰期权: count=%d skipped_active=%d remaining=%d", len(_expired_ids) - _skipped_active, _skipped_active, len(self._option_info))
        if len(self._option_info) > MAX_OPTION_CACHE_SIZE:
            _sorted_keys = sorted(self._option_info_timestamps, key=self._option_info_timestamps.get)
            _to_remove = []
            _skipped_active = 0
            for _eid in _sorted_keys:
                if len(self._option_info) - len(_to_remove) <= MAX_OPTION_CACHE_SIZE:
                    break
                if self._option_price.get(_eid, 0) > 0:
                    _skipped_active += 1
                    self._option_info_timestamps[_eid] = _now
                    continue
                _to_remove.append(_eid)
            for _eid in _to_remove:
                self._remove_option_runtime_state(_eid)
            if _to_remove:
                logging.warning("[CacheEvict] 容量淘汰期权: count=%d skipped_active=%d remaining=%d limit=%d", len(_to_remove), _skipped_active, len(self._option_info), MAX_OPTION_CACHE_SIZE)
        # instrument_id缓存淘汰
        _expired_iids = [k for k, ts in self._instrument_id_timestamps.items() if ts < _expired_cutoff]
        for _iid in _expired_iids:
            self._instrument_id_to_internal_id.pop(_iid, None)
            del self._instrument_id_timestamps[_iid]
        if len(self._instrument_id_to_internal_id) > MAX_INSTRUMENT_ID_MAP_SIZE:
            _sorted_keys = sorted(self._instrument_id_timestamps, key=self._instrument_id_timestamps.get)
            _to_remove_iids = _sorted_keys[:len(self._instrument_id_to_internal_id) - MAX_INSTRUMENT_ID_MAP_SIZE]
            for _iid in _to_remove_iids:
                self._instrument_id_to_internal_id.pop(_iid, None)
                self._instrument_id_timestamps.pop(_iid, None)
        # RES-P1-08修复: 期货缓存上限控制（P2-3: 跳过有活跃期权的期货）
        if len(self._future_prev_price) > MAX_FUTURE_CACHE_SIZE:
            _oldest_fids = list(self._future_prev_price.keys())[:len(self._future_prev_price) - MAX_FUTURE_CACHE_SIZE]
            _skipped_active = []
            _evicted_fids = []
            for _fid in _oldest_fids:
                _active_opts = sum(len(v) for v in self._options_by_future_type.get(_fid, {}).values())
                if _active_opts > 0:
                    _skipped_active.append(_fid)
                    continue
                self._future_prev_price.pop(_fid, None)
                self._future_rising.pop(_fid, None)
                self._future_initialized.pop(_fid, None)
                self._status_counts.pop(_fid, None)
                self._sync_otm_count.pop(_fid, None)
                self._sort_buckets.pop(_fid, None)
                self._months.pop(_fid, None)
                self._options_by_future_type.pop(_fid, None)
                _fp_info = self._get_params().get_instrument_meta(_fid) if _fid else None
                if _fp_info:
                    _fp_product = str(_fp_info.get('product', '')).upper()
                    if _fp_product in self._future_prices_by_month:
                        del self._future_prices_by_month[_fp_product]
                _evicted_fids.append(_fid)
            if _evicted_fids or _skipped_active:
                logging.warning("[CacheEvict] 期货缓存淘汰: evicted=%d skipped_active=%d fids=%s",
                               len(_evicted_fids), len(_skipped_active), _evicted_fids[:5])
        if len(self._sort_buckets) > MAX_SORT_BUCKETS_FUTURES:
            _excess = list(self._sort_buckets.keys())[MAX_SORT_BUCKETS_FUTURES:]
            for _fid in _excess:
                self._sort_buckets.pop(_fid, None)
        # AS-01：守恒断言检查
        self._audit_conservation()
    
    def _audit_conservation(self) -> None:
        """AS-01：数据守恒断言检查（每5分钟执行一次）"""
        _now = time.time()
        if _now - self._last_conservation_audit < 300:
            return
        
        # 注册过程中跳过检查（_option_info数在快速增长）
        if len(self._option_info) < 10000:
            return
        
        self._last_conservation_audit = _now
        
        # 断言1：_option_info数 == _status_counts total
        _option_info_count = len(self._option_info)
        _status_total = 0
        for _fid, _months_data in self._status_counts.items():
            for _month, _types_data in _months_data.items():
                for _otype, _bucket in _types_data.items():
                    _status_total += sum(_bucket.values())
        
        # 允许1的误差（注册过程中的短暂不一致）
        if abs(_option_info_count - _status_total) > 1:
            logging.warning(
                "[ConservationAudit] 断言失败: _option_info=%d != _status_counts_total=%d (diff=%d)",
                _option_info_count, _status_total, abs(_option_info_count - _status_total)
            )
        else:
            logging.info(
                "[ConservationAudit] 断言通过: _option_info=%d ≈ _status_counts_total=%d",
                _option_info_count, _status_total
            )

    def _get_params(self):
        if self._params_service is not None:
            return self._params_service
        from ali2026v3_trading.config.params_service import get_params_service as _get_ps
        self._params_service = _get_ps()
        return self._params_service

    def _check_is_main_month(self, underlying_future_id: Optional[int], month: str) -> bool:
        """Check if a month is a main delivery month (design report Section 4.4).

        Compares the option month against month_mapping[product][2:] (the 3
        main-month slots beyond the front two). Falls back to False if the
        mapping is unavailable.
        """
        if underlying_future_id is None or not month:
            return False
        try:
            fp_info = self._get_params().get_instrument_meta(underlying_future_id)
            if not fp_info:
                return False
            product = str(fp_info.get('product', '')).upper()
            params = self._get_params()
            month_mapping = getattr(params, 'month_mapping', None) or {}
            mapped = month_mapping.get(product)
            if not mapped:
                return False
            ym = str(fp_info.get('year_month', '') or '')
            year_prefix = ym[:2] if len(ym) >= 4 else ''
            main_months = []
            for entry in mapped[2:]:  # positions [2], [3], [4] are main months
                if isinstance(entry, str) and len(entry) >= 4:
                    main_months.append(entry[-4:])
                elif isinstance(entry, int) and year_prefix:
                    main_months.append(f"{year_prefix}{entry:02d}")
            return month in main_months
        except Exception as e:
            logging.debug("[_check_is_main_month] lookup failed: %s", e)
            return False

    def _backfill_is_main_month(self) -> None:
        """Backfill is_main_month for legacy options (design report Section 4.4).

        Called once on first select_otm_targets_by_volume invocation via the
        _is_main_month_backfilled flag. Only options missing the field are
        updated.
        """
        if getattr(self, '_is_main_month_backfilled', False):
            return
        for iid, info in self._option_info.items():
            if 'is_main_month' not in info:
                info['is_main_month'] = self._check_is_main_month(
                    info.get('underlying_future_id'), info.get('month', '')
                )
        self._is_main_month_backfilled = True


    def _mk(self, key):
        """P1-R11-21: strategy_id namespace composite key for cache isolation"""
        return (self._strategy_id, key)

    @staticmethod
    def _new_status_bucket() -> Dict[str, int]:
        return {
            'correct_rise': 0,
            'wrong_rise': 0,
            'correct_fall': 0,
            'wrong_fall': 0,
            'other': 0,
        }

    # ✅ 方法唯一：委托shared_utils.normalize_instrument_id，不再独立实现
    def _classify_status(self, underlying_future_id: int, month: str, opt_type: str,
                         current_price: float, prev_price: float,
                         option_direction: Optional[bool] = None) -> str:
        """根据当前内存状态计算单个期权的当前五态状态。

        返回的字符串是 state_param.py 中 _STATE_MAP 的契约键：
        'correct_rise', 'wrong_rise', 'correct_fall', 'wrong_fall', 'other'
        禁止随意修改键名，否则五态到三态映射会断裂。
        """
        future_state = self._get_future_runtime_state(underlying_future_id, month)
        # FIX-R22-DIAG: 诊断返回'other'的具体原因（前10次）
        if not hasattr(self, '_r22_diag_count'):
            self._r22_diag_count = 0
        self._r22_diag_count += 1
        if self._r22_diag_count <= 10:
            logging.info("[R22-DIAG] classify: fid=%s month=%s opt=%s cur=%s prev=%s dir=%s | fstate_fid=%s init=%s rising=%s price=%s",
                          underlying_future_id, month, opt_type, current_price, prev_price, option_direction,
                          future_state.get('future_id'), future_state.get('initialized'), future_state.get('rising'), future_state.get('price'))
        elif self._r22_diag_count % 1000 == 0:
            logging.info("[R22-DIAG] classify count=%d", self._r22_diag_count)
        if future_state['future_id'] is None:
            return 'other'

        if not future_state['initialized']:
            return 'other'

        # FIX-P0-19: rising=None表示期货方向未知，应返回'other'而非隐式转为False(下跌)
        # 与_compute_sync_flag中"rising is None → return False"保持一致
        if future_state.get('rising') is None:
            return 'other'

        if option_direction is None:
            if current_price > 0 and prev_price > 0:
                if current_price > prev_price:
                    option_direction = True
                elif current_price < prev_price:
                    option_direction = False

        if option_direction is None:
            future_rising = bool(future_state.get('rising', False))
            if opt_type == 'CALL':
                option_direction = future_rising
            else:
                option_direction = not future_rising

        if option_direction is None:
            return 'other'

        future_rising = bool(future_state['rising'])
        option_rising = option_direction

        if opt_type == 'CALL':
            if future_rising and option_rising:
                return 'correct_rise'
            if not future_rising and option_rising:
                return 'wrong_rise'
            if not future_rising and not option_rising:
                return 'correct_fall'
            if future_rising and not option_rising:
                return 'wrong_fall'
        else:
            if not future_rising and option_rising:
                return 'correct_rise'
            if future_rising and option_rising:
                return 'wrong_rise'
            if future_rising and not option_rising:
                return 'correct_fall'
            if not future_rising and not option_rising:
                return 'wrong_fall'

        return 'other'

    def _set_current_status(self, internal_id: int, info: Dict[str, Any], new_status: str) -> None:
        """更新单个期权的当前状态，并同步修正 _status_counts。"""
        # ✅ 使用 underlying_future_id 作为键
        future_id = info.get('underlying_future_id')
        if future_id is None:
            logging.warning(f"[_set_current_status] Missing underlying_future_id for internal_id={internal_id}")
            return
        
        month = info['month']
        opt_type = info['option_type']

        if future_id not in self._status_counts:
            self._status_counts[future_id] = defaultdict(lambda: defaultdict(self._new_status_bucket))
        if month not in self._status_counts[future_id]:
            self._status_counts[future_id][month] = defaultdict(self._new_status_bucket)
        if opt_type not in self._status_counts[future_id][month]:
            self._status_counts[future_id][month][opt_type] = self._new_status_bucket()
        bucket = self._status_counts[future_id][month][opt_type]
        old_status = self._current_status.get(internal_id)

        if old_status is not None:
            bucket[old_status] = max(0, bucket.get(old_status, 0) - 1)

        bucket[new_status] += 1
        self._current_status[internal_id] = new_status

    def _update_option_direction(self, internal_id: int, price: float) -> Optional[bool]:
        """维护期权最近一次有效方向。'
        定义：
        - 只使用最近两笔不同且大于 0 的价格来生成方向
        - 平价 Tick 不改变方向
        - FIX-P0-10: price<=0时保留已有方向（仅清空锚点价格），避免级联other效应
          原设计清空方向导致大量期权被分类为'other'，correct_up_pct极低
        """
        if price <= 0:
            self._option_last_distinct_price[internal_id] = None
            return self._option_last_direction.get(internal_id)

        anchor_price = self._option_last_distinct_price.get(internal_id)
        if anchor_price is None or anchor_price <= 0:
            self._option_last_distinct_price[internal_id] = price
            self._option_last_direction[internal_id] = None
            return None

        if price > anchor_price:
            self._option_last_distinct_price[internal_id] = price
            self._option_last_direction[internal_id] = True
            return True

        if price < anchor_price:
            self._option_last_distinct_price[internal_id] = price
            self._option_last_direction[internal_id] = False
            return False

        return self._option_last_direction.get(internal_id)

    def _compute_sync_flag(self, info: Dict[str, Any], current_price: float,
                           option_direction: Optional[bool]) -> bool:
        underlying_future_id = info.get('underlying_future_id')
        month = info['month']
        opt_type = info['option_type']
        strike = info['strike_price']
        future_state = self._get_future_runtime_state(underlying_future_id, month)

        if future_state['future_id'] is None or not future_state['initialized']:
            return False

        if future_state.get('rising') is None:
            return False

        # FIX-P0-09: price<=0不再直接返回False，允许方向已知时计算sync_flag
        # 仅当方向为None时才返回False（方向未知无法判断同步性）
        if option_direction is None:
            return False

        future_rising = bool(future_state['rising'])
        future_price = future_state['price']
        is_otm = self._is_out_of_the_money(opt_type, future_price, strike)
        is_correct = (
            (future_rising and opt_type == 'CALL' and option_direction)
            or (not future_rising and opt_type == 'CALL' and not option_direction)
            or (not future_rising and opt_type == 'PUT' and option_direction)
            or (future_rising and opt_type == 'PUT' and not option_direction)
        )
        return is_correct and is_otm

    def register_future(self, future_internal_id: int, initial_price: float, month: str = None):
        """注册期货品种，初始化价格和方向
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）'
            initial_price: 初始价格
            month: 可选，指定月份。如果不传，则更新该期货所有已知月份的价格。'
        """
        with self._lock:
            # ✅ 从 id_cache 获取 product 和 month
            info = self._get_params().get_instrument_meta(future_internal_id)
            if not info:
                logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
                return
            
            product = str(info.get('product', '')).upper()
            contract_month = info.get('year_month', month) or info.get('month', month)
            
            if contract_month:
                self._future_prices_by_month[product][contract_month] = initial_price
            else:
                # 如果没有指定月份，更新该期货下所有已注册月份的价格（模拟主力合约联动）'
                for m in self._months.get(future_internal_id, []):
                    self._future_prices_by_month[product][m] = initial_price
            
            self._future_prev_price[future_internal_id] = initial_price
            if future_internal_id not in self._future_rising:
                self._future_rising[future_internal_id] = False
            # 注册初值不代表已有方向信息；首次真实 tick 到来后再标记为已初始化
            self._future_initialized.setdefault(future_internal_id, False)

    def register_option(self, instrument_id: str, underlying_product: str,
                        month: str, strike_price: float, option_type: str,
                        initial_price: float,
                        underlying_future_id: Optional[int] = None,
                        internal_id: Optional[int] = None):
        """注册期权合约，记录基本信息
        
        Args:
            instrument_id: 合约代码（兼容入口）
            underlying_product: 期权产品代码（兼容展示入参，不参与关系推导）
            month: 月份
            strike_price: 行权价
            option_type: 期权类型
            initial_price: 初始价格
            underlying_future_id: 标的期货 internal_id
            internal_id: 系统内部代理键（优先使用）
        """
        # R24-P2-06修复: 入门前置防御性校验，避免异常数据进入锁内操作
        if not instrument_id:
            logging.warning("[register_option] instrument_id为空")
            return None
        if underlying_future_id is None:
            logging.warning("[register_option] underlying_future_id为空: inst=%s", instrument_id)
            return None
        try:
            underlying_future_id = int(underlying_future_id)
        except (TypeError, ValueError):
            logging.warning("[register_option] underlying_future_id非整数: inst=%s fid=%s", instrument_id, underlying_future_id)
            return None
        if underlying_future_id <= 0:
            logging.warning("[register_option] underlying_future_id非正: inst=%s fid=%s", instrument_id, underlying_future_id)
            return None
        if internal_id is not None:
            try:
                internal_id = int(internal_id)
            except (TypeError, ValueError):
                logging.warning("[register_option] internal_id非整数: inst=%s iid=%s", instrument_id, internal_id)
                return None
            if internal_id <= 0:
                logging.warning("[register_option] internal_id非正: inst=%s iid=%s", instrument_id, internal_id)
                return None
        if not month or not isinstance(month, str):
            logging.warning("[register_option] month为空或非法: inst=%s month=%s", instrument_id, month)
            return None
        if len(month) > 16:
            logging.warning("[register_option] month过长: inst=%s month=%s", instrument_id, month)
            return None
        try:
            strike_price = float(strike_price)
        except (TypeError, ValueError):
            logging.warning("[register_option] strike_price无法转float: inst=%s strike=%s", instrument_id, strike_price)
            return None
        if strike_price <= 0 or strike_price > 1e9:
            logging.warning("[register_option] strike_price越界: inst=%s strike=%s", instrument_id, strike_price)
            return None
        try:
            initial_price = float(initial_price)
        except (TypeError, ValueError):
            initial_price = 0.0
        if initial_price < 0 or initial_price > 1e9:
            initial_price = 0.0

        with self._lock:
            _reg_count = getattr(self.__class__, '_reg_option_count', 0)
            if _reg_count < 5:
                logging.info("[WidthStrengthCache] register_option called: %s, underlying_future_id=%s, internal_id=%s", instrument_id, underlying_future_id, internal_id)
                self.__class__._reg_option_count = _reg_count + 1
            instrument_id = self._normalize_instrument_id(instrument_id)
            if not self._get_future_product_by_underlying_id(underlying_future_id):
                logging.warning("[register_option] 无法解析标的期货产品: inst=%s fid=%s", instrument_id, underlying_future_id)
                return None
            opt_type_upper = self._normalize_option_type(option_type)
            if not opt_type_upper or opt_type_upper not in ('CALL', 'PUT'):
                logging.warning("[register_option] option_type无效: inst=%s type=%s", instrument_id, option_type)
                return None
            
            # 入口标准化：确定 internal_id
            if internal_id is None:
                internal_id = self._resolve_internal_id(instrument_id)
            
            # ✅ 序号126修复：删除CRC32兜底，统一使用ParamsService解析的internal_id
            if internal_id is None:
                logging.error(
                    "[WidthStrengthCache] internal_id未提供且无法从ParamsService解析: "
                    "instrument_id=%s. Please ensure contract is properly registered.",
                    instrument_id
                )
                return None
            
            # 维护双向映射
            self._instrument_id_to_internal_id[instrument_id] = internal_id
            self._instrument_id_timestamps[instrument_id] = time.time()
            self._internal_id_to_instrument_id[internal_id] = instrument_id

            existing_info = self._option_info.get(internal_id)
            existing_price = float(self._option_price.get(internal_id, 0.0) or 0.0)
            existing_status = self._current_status.get(internal_id, 'other')
            preserve_runtime_state = existing_info is not None and initial_price <= 0 < existing_price
            if existing_info is not None:
                old_future_id = existing_info.get('underlying_future_id')
                old_month = existing_info.get('month')
                old_type = existing_info['option_type']
                if (old_future_id, old_month, old_type) != (underlying_future_id, month, opt_type_upper):
                    self._decrement_status_count_for_option(internal_id, existing_info)
                    self._current_status.pop(internal_id, None)
                if old_future_id is not None:
                    old_list = self._options_by_future_type.get(old_future_id, {}).get(old_type, [])
                    if internal_id in old_list and old_future_id != underlying_future_id:
                        old_list[:] = [iid for iid in old_list if iid != internal_id]
            
            self._option_info[internal_id] = {
                'instrument_id': instrument_id,
                'internal_id': internal_id,
                'underlying_future_id': int(underlying_future_id) if underlying_future_id not in (None, '') else None,
                'month': month,
                'strike_price': float(strike_price),
                'option_type': opt_type_upper,
                # BUG-15 fix: record is_main_month per design report 4.4
                'is_main_month': self._check_is_main_month(underlying_future_id, month),
            }
            self._option_info_timestamps[internal_id] = time.time()
            self._enforce_cache_size_limit()
            _info_count = getattr(self.__class__, '_option_info_count', 0) + 1
            self.__class__._option_info_count = _info_count
            if _info_count <= 5 or _info_count % 500 == 0:
                logging.info(f"[WidthStrengthCache] _option_info[{internal_id}] = {instrument_id}, underlying_future_id={underlying_future_id}, total={_info_count}")
            # ✅ 使用 future_internal_id 作为键
            option_list = self._options_by_future_type[underlying_future_id][opt_type_upper]
            if internal_id not in option_list:
                option_list.append(internal_id)
            if month not in self._months[underlying_future_id]:
                self._months[underlying_future_id].append(month)

            if preserve_runtime_state:
                self._set_current_status(internal_id, self._option_info[internal_id], existing_status)
                return True

            # 注册时使用实际价格，如果为0则不初始化价格字段（等待第一个Tick）
            if initial_price > 0:
                self._option_price[internal_id] = initial_price
                self._option_prev_price[internal_id] = initial_price
                self._option_last_distinct_price[internal_id] = initial_price
            else:
                self._option_price.pop(internal_id, None)
                self._option_prev_price.pop(internal_id, None)
                self._option_last_distinct_price[internal_id] = None
            
            self._option_last_direction[internal_id] = None
            self._set_current_status(internal_id, self._option_info[internal_id], 'other')
            self._sync_flag[internal_id] = False
            return True

    # ✅ 接口唯一：WidthStrengthCache的on_*_tick为内部实现，TTypeService为唯一公开入口
    def on_future_tick(self, future_internal_id: int, price: float, month: str = None):
        """更新期货价格，自动判断涨跌方向并同步到指定月份
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）'
            price: 最新价格
            month: 可选，该 Tick 所属的具体月份（如 '2605'）。'
                   如果不提供，则从 id_cache 获取。
        """
        if price <= 0:
            if not hasattr(self, '_future_zero_price_count'):
                self._future_zero_price_count = 0
            self._future_zero_price_count += 1
            if self._future_zero_price_count <= 5:
                logging.warning("[FutureZeroPrice] on_future_tick price<=0: fid=%s price=%s", future_internal_id, price)
            return
        with self._lock:
            # ✅ 从 id_cache 获取 product 和 month
            info = self._get_params().get_instrument_meta(future_internal_id)
            if not info:
                logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
                return
            
            product = str(info.get('product', '')).upper()
            contract_month = info.get('year_month', month) or info.get('month', month)
            
            if not contract_month:
                logging.warning("[on_future_tick] month=None for fid=%s, price not updated", future_internal_id)
                return
            
            prev = self._future_prev_price.get(future_internal_id, price)
            was_initialized = self._future_initialized.get(future_internal_id, False)
            self._future_prev_price[future_internal_id] = price
            direction_changed = False
            if price != prev:
                new_rising = price > prev
                old_rising = self._future_rising.get(future_internal_id)
                self._future_rising[future_internal_id] = new_rising
                direction_changed = old_rising is not None and old_rising != new_rising
            self._future_initialized[future_internal_id] = True
            
            self._future_prices_by_month[product][contract_month] = price
            
            need_recalc = direction_changed or (not was_initialized and self._future_initialized[future_internal_id])
            if need_recalc:
                self._recalc_all_state_for_underlying(future_internal_id)
            
            if not was_initialized:
                self._classify_pending_options(future_internal_id)

    def on_future_instrument_tick(self, future_internal_id: int, price: float):
        """✅ 按期货 internal_id 更新价格，ID语义主入口。"""
        # FIX-R20: 首次调用诊断日志
        if not hasattr(self, '_on_future_instr_tick_count'):
            self._on_future_instr_tick_count = 0
        self._on_future_instr_tick_count += 1
        if self._on_future_instr_tick_count <= 2:
            logging.info("[WidthCache] on_future_instrument_tick called: fid=%s price=%s (count=%d)",
                        future_internal_id, price, self._on_future_instr_tick_count)
        info = self._get_params().get_instrument_meta(future_internal_id)
        if not info:
            if not hasattr(self, '_on_future_instr_meta_miss'):
                self._on_future_instr_meta_miss = 0
            self._on_future_instr_meta_miss += 1
            if self._on_future_instr_meta_miss <= 3:
                logging.warning(f"[TType] Unknown future internal_id: {future_internal_id} (miss count={self._on_future_instr_meta_miss})")
            return
        
        product = info.get('product', '').upper()
        month = info.get('year_month', '') or info.get('month', '')
        if not product:
            return
        self.on_future_tick(future_internal_id, price, month)

    def _get_current_trading_date(self) -> str:
        """获取当前交易日（P1-4.3修复：提取为独立方法，便于测试时mock）"""
        return datetime.now(CHINA_TZ).strftime('%Y%m%d')

    def on_option_tick(self, instrument_id: str, price: float, volume: float = 0):
        """更新期权价格，增量更新同步虚值计数
        
        入口先做 instrument_id -> internal_id 标准化。'
        """
        if not hasattr(self, '_oot_entry_count'):
            self._oot_entry_count = 0
        self._oot_entry_count += 1
        if self._oot_entry_count <= 3 or self._oot_entry_count % 100000 == 0:
            logging.info("[OOT-ENTRY] on_option_tick called: inst=%s price=%s vol=%s total_calls=%d", instrument_id, price, volume, self._oot_entry_count)
        with self._lock:
            instrument_id = self._normalize_instrument_id(instrument_id)
            # 入口标准化：instrument_id -> internal_id
            internal_id = self._resolve_internal_id(instrument_id)
            if internal_id is None:
                if not hasattr(self, '_oot_diag_no_iid'):
                    self._oot_diag_no_iid = 0
                self._oot_diag_no_iid += 1
                if self._oot_diag_no_iid <= 5:
                    logging.warning("[OOT-DIAG] internal_id=None: inst=%s price=%s", instrument_id, price)
                return
            
            if volume > 0:
                self._option_volume[internal_id] = self._option_volume.get(internal_id, 0) + volume
                today = self._get_current_trading_date()
                if today != self._current_trading_date:
                    self._option_daily_volume.clear()
                    self._current_trading_date = today
                self._option_daily_volume[internal_id] = self._option_daily_volume.get(internal_id, 0) + volume
            info = self._option_info.get(internal_id)
            if not info:
                if not hasattr(self, '_oot_diag_no_info'):
                    self._oot_diag_no_info = 0
                self._oot_diag_no_info += 1
                if self._oot_diag_no_info <= 5:
                    logging.warning("[OOT-DIAG] no _option_info: inst=%s iid=%s price=%s info_count=%d", instrument_id, internal_id, price, len(self._option_info))
                if self._should_trace_option_tick(instrument_id):
                    self._log_tracked_option_tick(
                        instrument_id,
                        None,
                        prev_price=0.0,
                        current_price=price,
                        note='unregistered',
                    )
                return
            
            # ✅ 使用 future_internal_id
            future_id = info.get('underlying_future_id')
            if future_id is None:
                logging.warning(f"[on_option_tick] Missing underlying_future_id for {instrument_id}")
                return
            month = info['month']
            opt_type = info['option_type']

            prev_price = self._option_price.get(internal_id)
            if prev_price is None:
                # FIX-P0-20: 首个tick price<=0时不进行分类，仅注册为'other'
                # 与on_future_tick price<=0守卫一致，避免基于无效价格的错误分类
                if price <= 0:
                    self._option_last_direction[internal_id] = None
                    self._option_info_timestamps[internal_id] = time.time()
                    self._set_current_status(internal_id, info, 'other')
                    self._sync_flag[internal_id] = False
                    self._enforce_cache_size_limit()
                    return
                self._option_price[internal_id] = price
                self._option_prev_price[internal_id] = price
                self._option_last_distinct_price[internal_id] = price
                self._option_last_direction[internal_id] = None
                self._option_info_timestamps[internal_id] = time.time()
                _fut_init = self._future_initialized.get(future_id, False)
                if not hasattr(self, '_oot_diag_first_tick'):
                    self._oot_diag_first_tick = 0
                self._oot_diag_first_tick += 1
                if self._oot_diag_first_tick <= 5:
                    logging.debug("[OOT-DIAG] first_tick: inst=%s iid=%s fid=%s fut_init=%s price=%s", instrument_id, internal_id, future_id, _fut_init, price)
                
                if _fut_init:
                    initial_status = self._classify_status(future_id, month, opt_type, price, price, option_direction=None)
                    self._set_current_status(internal_id, info, initial_status)
                    self._update_sort_bucket(internal_id, info, future_id, month, opt_type)
                else:
                    self._pending_classify_options[future_id].append({
                        'internal_id': internal_id,
                        'info': info,
                        'month': month,
                        'opt_type': opt_type,
                        'price': price,
                    })
                    self._set_current_status(internal_id, info, 'other')
                    if not hasattr(self, '_future_init_pending_count'):
                        self._future_init_pending_count = defaultdict(int)
                    self._future_init_pending_count[future_id] += 1
                    if self._future_init_pending_count[future_id] <= 5:
                        logging.warning("[FutureInitPending] Future %s not initialized; option=%s queued for reclassification", future_id, instrument_id)

                self._enforce_cache_size_limit()
                return
            
            self._option_prev_price[internal_id] = prev_price
            self._option_price[internal_id] = price

            last_direction = self._update_option_direction(internal_id, price)

            old_was_sync = self._sync_flag.get(internal_id, False)
            new_is_sync = self._compute_sync_flag(info, price, last_direction)
            if not hasattr(self, '_oot_diag_classify'):
                self._oot_diag_classify = 0
            self._oot_diag_classify += 1
            if self._oot_diag_classify <= 5:
                logging.debug("[OOT-DIAG] classify: inst=%s iid=%s fid=%s price=%s prev=%s dir=%s fut_init=%s", instrument_id, internal_id, future_id, price, prev_price, last_direction, self._future_initialized.get(future_id, False))
            new_status = self._classify_status(
                future_id,
                month,
                opt_type,
                price,
                prev_price,
                option_direction=last_direction,
            )
            self._set_current_status(internal_id, info, new_status)

            # ✅ 增量更新 _sync_otm_count（排序二原则核心）- 使用 future_internal_id
            if new_is_sync and not old_was_sync:
                self._sync_otm_count[future_id][month][opt_type] += 1
            elif not new_is_sync and old_was_sync:
                self._sync_otm_count[future_id][month][opt_type] = max(0, self._sync_otm_count[future_id][month][opt_type] - 1)

            self._sync_flag[internal_id] = new_is_sync

            # ✅ 分组D准备：增量更新排序桶 - 使用 future_internal_id
            self._update_sort_bucket(internal_id, info, future_id, month, opt_type)

            if self._should_trace_option_tick(instrument_id):
                self._log_tracked_option_tick(
                    instrument_id,
                    info,
                    prev_price=prev_price,
                    current_price=price,
                )
            self._option_info_timestamps[internal_id] = time.time()
            self._enforce_cache_size_limit()


    def _classify_pending_options(self, future_internal_id: int):
        """期货初始化时，分类所有待分类的期权"""
        pending = self._pending_classify_options.pop(future_internal_id, [])
        if not pending:
            return
        logging.info("[ClassifyPending] Reclassifying %d pending options for future %s", len(pending), future_internal_id)
        for item in pending:
            internal_id = item['internal_id']
            info = item['info']
            month = item['month']
            opt_type = item['opt_type']
            price = item['price']
            status = self._classify_status(future_internal_id, month, opt_type, price, price, option_direction=None)

            self._set_current_status(internal_id, info, status)
            self._update_sort_bucket(internal_id, info, future_internal_id, month, opt_type)

    def _recalc_all_state_for_underlying(self, future_internal_id: int):
        """期货方向变化时，重新计算该标的下所有期权的当前状态和同步计数，并同步更新排序桶。
        P1-18修复：改为全锁内执行，消除TOCTOU竞态。
        
        Args:
            future_internal_id: 期货合约 internal_id（主键）
        """
        info = self._get_params().get_instrument_meta(future_internal_id)
        if not info:
            logging.warning(f"[WidthStrengthCache] Unknown future_internal_id: {future_internal_id}")
            return
        
        with self._lock:
            _recalc_before_total = sum(
                sum(bucket.values())
                for fid_sc in self._status_counts.values()
                for month_sc in fid_sc.values()
                for opt_sc in month_sc.values()
                for bucket in [opt_sc]
            )
            
            target_internal_ids = []
            for internal_ids in self._options_by_future_type.get(future_internal_id, {}).values():
                target_internal_ids.extend(internal_ids)
            
            if future_internal_id in self._sort_buckets:
                self._sort_buckets[future_internal_id].clear()
            
            for iid in target_internal_ids:
                opt_info = self._option_info.get(iid)
                if not opt_info or opt_info.get('underlying_future_id') != future_internal_id:
                    continue
                
                month = opt_info['month']
                opt_type = opt_info['option_type']
                option_price = self._option_price.get(iid, 0.0)
                prev_price = self._option_prev_price.get(iid, 0.0)
                last_direction = self._option_last_direction.get(iid)
                
                status = self._classify_status(
                    future_internal_id, month, opt_type,
                    option_price, prev_price,
                    option_direction=last_direction,
                )
                
                is_sync = self._compute_sync_flag(opt_info, option_price, last_direction)
                
                self._sync_flag[iid] = is_sync
                self._set_current_status(iid, opt_info, status)
                self._do_update_sort_bucket(iid, opt_info, future_internal_id, month, opt_type)
            
            self._sync_otm_count.pop(future_internal_id, None)
            for iid in target_internal_ids:
                if self._sync_flag.get(iid, False):
                    opt_info = self._option_info.get(iid)
                    if opt_info:
                        self._sync_otm_count[future_internal_id][opt_info['month']][opt_info['option_type']] += 1
            
            _recalc_after_total = sum(
                sum(bucket.values())
                for fid_sc in self._status_counts.values()
                for month_sc in fid_sc.values()
                for opt_sc in month_sc.values()
                for bucket in [opt_sc]
            )
            if _recalc_before_total != _recalc_after_total:
                logging.warning("[RecalcAudit] total changed: before=%d after=%d fid=%d options=%d",
                               _recalc_before_total, _recalc_after_total, future_internal_id, len(target_internal_ids))

    # ========================================================================
    # ✅ 分组D准备：by_sort_bucket 索引维护方法
    # ========================================================================

    def _update_sort_bucket(self, internal_id: int, info: Dict, future_internal_id: int, month: str, opt_type: str):
        """
        ✅ 分组D准备：增量更新排序桶（P0-4.3修复：加锁保护，避免与select_from_sort_bucket数据竞争）'
        当期权状态/成交量变化时，更新对应的排序桶。'
        使用heapq维护前N个最优期权，避免全量扫描。
        
        Args:
            internal_id: internal_id
            info: 期权元数据
            future_internal_id: 期货合约 internal_id（主键）'
            month: 月份
            opt_type: CALL/PUT
        """
        with self._lock:
            self._do_update_sort_bucket(internal_id, info, future_internal_id, month, opt_type)

    def _do_update_sort_bucket(self, internal_id: int, info: Dict, future_internal_id: int, month: str, opt_type: str):
        """_update_sort_bucket实际逻辑（在锁内调用）"""
        try:
            future_info = self._get_params().get_instrument_meta(future_internal_id)
            if not future_info:
                logging.warning(f"[_update_sort_bucket] Unknown future_internal_id: {future_internal_id}")
                return
            
            # ✅ 获取期货最新价 - 使用 future_internal_id
            m_price = self._get_future_price_by_id_and_month(future_internal_id, month)
            if m_price <= 0:
                return  # 期货价格未就绪，跳过
            
            # 获取期权信息
            strike = info.get('strike_price', 0)
            volume = self._option_daily_volume.get(internal_id, self._option_volume.get(internal_id, 0))
            price = self._option_price.get(internal_id, 0)
            sync_flag = self._sync_flag.get(internal_id, False)
            instrument_id = info.get('instrument_id', self._internal_id_to_instrument_id.get(internal_id, str(internal_id)))
            
            # 只处理同步虚值期权（correct_rise）
            current_status = self._current_status.get(internal_id, 'other')
            if current_status not in ('correct_rise', 'correct_fall'):
                self._remove_from_sort_bucket(internal_id, future_internal_id, month, opt_type)
                return
            
            # 检查是否为虚值
            if not self._is_out_of_the_money(opt_type, m_price, strike):
                self._remove_from_sort_bucket(internal_id, future_internal_id, month, opt_type)
                return
            
            # 创建排序条目
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
                last_tick_ts=time.monotonic(),  # 单调时钟，保证时间新鲜度排序有效
            )
            
            # 插入排序桶 - 使用 future_internal_id 作为键
            bucket = self._sort_buckets[future_internal_id][month][opt_type]
            
            # 检查是否已存在
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
        """
        从排序桶中移除指定期权（P0-4.3修复：加锁保护，避免与select_from_sort_bucket数据竞争）'
        Args:
            internal_id: internal_id
            future_internal_id: 期货合约 internal_id（主键）
            month: 月份
            opt_type: CALL/PUT
        """
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


_WidthCacheStateMixin = WidthCacheStateService

