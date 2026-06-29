# MODULE_ID: M1-057
"""
width_cache_sort_mixin.py — 排序桶索引维护Mixin
包含: _WidthCacheSortMixin — 排序桶增量更新/移除/全量重算
"""
from __future__ import annotations

import logging
import heapq
import time
from typing import Any, Dict, List, Optional
from collections import defaultdict

from ali2026v3_trading.infra.shared_utils import to_float32, CHINA_TZ
from ali2026v3_trading.data.width_cache_types import (
    _NoOpDiagnosisProbeManager, SortEntry,
    MAX_OPTION_CACHE_SIZE, MAX_INSTRUMENT_ID_MAP_SIZE, MAX_FUTURE_CACHE_SIZE,
    MAX_STATUS_COUNTS_SIZE, MAX_SORT_BUCKETS_FUTURES, _CACHE_TTL_SECONDS,
)


class WidthCacheSortService:

    def __init__(self):
        self._facade = None

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")



    # ========================================================================
    # ✅ 分组D准备：by_sort_bucket 索引维护方法

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
            if current_status not in ('correct_rise', 'correct_fall', 'wrong_rise', 'wrong_fall'):
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


_WidthCacheSortMixin = WidthCacheSortService
