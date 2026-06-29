# MODULE_ID: M1-054
"""
width_cache_context_mixin.py — 上下文解析Mixin
包含: _WidthCacheContextMixin
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.infra.shared_utils import to_float32, CHINA_TZ
from ali2026v3_trading.data.width_cache_types import (
    _NoOpDiagnosisProbeManager, SortEntry,
    MAX_OPTION_CACHE_SIZE, MAX_INSTRUMENT_ID_MAP_SIZE, MAX_FUTURE_CACHE_SIZE,
    MAX_STATUS_COUNTS_SIZE, MAX_SORT_BUCKETS_FUTURES, _CACHE_TTL_SECONDS,
)


class WidthCacheContextMixin:

    def __init__(self):
        self._facade = None

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    # 方法唯一：委托shared_utils.normalize_instrument_id，不再独立实现
    @staticmethod
    def _normalize_instrument_id(instrument_id: str) -> str:
        from ali2026v3_trading.infra.shared_utils import normalize_instrument_id
        return normalize_instrument_id(instrument_id)

    @staticmethod
    def _normalize_option_type(opt_type: str) -> str:
        """ID唯一：委托shared_utils.normalize_option_type，不再独立实现"""
        from ali2026v3_trading.infra.shared_utils import normalize_option_type
        return normalize_option_type(opt_type)
    
    def _resolve_internal_id(self, instrument_id: str) -> Optional[int]:
        """
        instrument_id -> internal_id 入口转换
        
        本地缓存优先，ParamsService回退（仅配置文件外合约触发）'
        """
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
        """✅ 通过 underlying_future_id 从 id_cache 获取 product（替代原产品映射）"""
        if underlying_future_id is None or underlying_future_id == '':
            return ''
        # ✅ 防御性检查：underlying_future_id 必须是整数类型，字符串说明调用方有bug
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
        """按 future_internal_id + month 获取期货价格。"""
        if underlying_future_id is None:
            return 0.0
        future_product = self._get_future_product_by_underlying_id(underlying_future_id)
        if not future_product:
            return 0.0
        return float(self._future_prices_by_month.get(future_product, {}).get(month, 0.0) or 0.0)

    def _get_future_runtime_state(self, underlying_future_id: Optional[int], month: str) -> Dict[str, Any]:
        """统一读取期货运行态，避免混用 product / future_id 作为 key。"""
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
            'rising': self._future_rising.get(future_id) if future_id is not None else None,
            'price': self._get_future_price_by_id_and_month(future_id, month),
        }

    def _get_option_display_products(self, info: Optional[Dict[str, Any]]) -> Dict[str, str]:
        """仅在导出或诊断时派生产品显示字段，避免把它们作为关系主链缓存。"""
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
        """按 instrument_id 收敛期权元数据，优先走 WidthCache，其次 ParamsService。"""
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
        """通过 future_internal_id 获取标的期货最新价，彻底替代 underlying_symbol 字符串回退。"""
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
        # 用internal_id查缓存（缓存键已统一为int）
        internal_id = self._resolve_internal_id(instrument_id)
        iid = internal_id if internal_id is not None else instrument_id
        last_distinct_price = self._option_last_distinct_price.get(iid)
        direction = self._option_last_direction.get(iid)
        current_status = self._current_status.get(iid, 'other')
        direction_text = 'up' if direction is True else ('down' if direction is False else 'none')
        
        # PROBE: 检查期货初始化状态
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
        """获取当前交易日（P1-4.3修复：提取为独立方法，便于测试时mock）"""
        return datetime.now(CHINA_TZ).strftime('%Y%m%d')


    def _get_future_price_for_month(self, product: str, month: str) -> float:
        """✅ 获取指定产品在指定月份的期货价格（ID语义版本）"""
        # 注意：此方法需要product字符串，从调用方传入的应是已从underlying_future_id解析出的product
        return self._future_prices_by_month.get(product, {}).get(month, 0.0)

    def _is_out_of_the_money(self, option_type: str, underlying_price: float, strike: float) -> bool:
        """判断是否为虚值期权（P1 修复：修正 CALL 虚值条件）"""
        if option_type == 'CALL':
            return underlying_price < strike  # 看涨期权：标的低于行权价为虚值
        else:
            return underlying_price > strike  # 看跌期权：标的高于行权价为虚值


_WidthCacheContextMixin = WidthCacheContextMixin

