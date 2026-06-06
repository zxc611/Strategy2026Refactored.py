"""
order_risk_guard.py - OrderRiskGuard
Phase 2 (CC-P1-01): 从OrderService提取的风控/防护职责域

职责：
- 跨策略PnL相关性检查 (_compute_pnl_correlation, _check_cross_strategy_risk)
- 价格修正/对齐 (_correct_price, _get_tick_size)
- 市场价格获取 (_get_last_market_price)
- 滑点估算 (_estimate_slippage)
- 保证金释放 (_release_margin_reservation)
- 风控阻断 (check_risk_block, set_risk_block)
"""
from __future__ import annotations

import logging
import time
import numpy as np
from typing import Dict, Optional

from ali2026v3_trading.shared_utils import compute_slippage_bps


class OrderRiskGuard:
    """订单风控/防护 — 从OrderService提取的独立可测试组件"""

    _PNL_CORR_THRESHOLD = 0.7

    def __init__(self, order_service=None):
        self._order_service = order_service
        self._risk_block_until: Dict[str, float] = {}
        self._risk_block_timeout: float = 300.0

    def compute_pnl_correlation(self, pnl_series_a, pnl_series_b) -> float:
        try:
            a = np.array(pnl_series_a, dtype=np.float64)
            b = np.array(pnl_series_b, dtype=np.float64)
            min_len = min(len(a), len(b))
            if min_len < 5:
                return 0.0
            a = a[:min_len]
            b = b[:min_len]
            a_mean = np.mean(a)
            b_mean = np.mean(b)
            cov = np.mean((a - a_mean) * (b - b_mean))
            var_a = np.mean((a - a_mean) ** 2)
            var_b = np.mean((b - b_mean) ** 2)
            denom = np.sqrt(var_a * var_b)
            if denom < 1e-12:
                return 0.0
            return float(cov / denom)
        except Exception:
            return 0.0

    def check_cross_strategy_risk(self) -> float:
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            _eco = get_strategy_ecosystem()
            if _eco is None:
                return 0.0
            strategies = []
            for slot_name in ('master', 'reverse', 'other', 'spring'):
                strat = getattr(_eco, f'_{slot_name}', None)
                if strat and hasattr(strat, 'equity_curve') and len(strat.equity_curve) >= 10:
                    strategies.append((slot_name, strat.equity_curve))
            max_corr = 0.0
            for i in range(len(strategies)):
                for j in range(i + 1, len(strategies)):
                    pa = list(np.diff(strategies[i][1]))
                    pb = list(np.diff(strategies[j][1]))
                    corr = self.compute_pnl_correlation(pa, pb)
                    if abs(corr) > max_corr:
                        max_corr = abs(corr)
            if max_corr > self._PNL_CORR_THRESHOLD:
                logging.warning(
                    "[OrderRiskGuard] P0-CS-001: 跨策略PnL相关系数%.3f超过阈值%.1f",
                    max_corr, self._PNL_CORR_THRESHOLD,
                )
            return max_corr
        except Exception:
            return 0.0

    def correct_price(self, price: float, instrument_id: str) -> float:
        tick_size = self.get_tick_size(instrument_id)
        if tick_size <= 0:
            return price
        aligned = round(price / tick_size) * tick_size
        return max(aligned, tick_size)

    def get_tick_size(self, instrument_id: str) -> float:
        try:
            from ali2026v3_trading.params_service import get_params_service
            params_svc = get_params_service()
            meta = params_svc.get_instrument_meta_by_id(instrument_id)
            product = meta.get('product') if meta else instrument_id
            product_cache = params_svc.get_product_cache(product)
            if product_cache:
                tick_size = float(product_cache.get('tick_size', 0))
                if tick_size > 0:
                    return tick_size
        except Exception as e:
            logging.debug("[OrderRiskGuard.get_tick_size] Failed: %s", e)
        default_tick_sizes = {
            'IF': 0.2, 'IC': 0.2, 'IH': 0.2, 'IM': 0.2,
        }
        for prefix, tick in default_tick_sizes.items():
            if instrument_id.startswith(prefix):
                return tick
        return 1.0

    def get_last_market_price(self, instrument_id: str, orders_by_id: Dict = None) -> Optional[float]:
        try:
            from ali2026v3_trading.query_service import get_query_service
            qs = get_query_service()
            tick = qs.get_last_tick(instrument_id)
            if tick and isinstance(tick, dict):
                return tick.get('last_price') or tick.get('price')
        except Exception:
            pass
        if orders_by_id:
            try:
                for order in orders_by_id.values():
                    if order.get('instrument_id') == instrument_id and order.get('status') in ('FILLED', 'ALL_FILLED'):
                        return order.get('price')
            except Exception:
                pass
        return None

    def estimate_slippage(self, instrument_id: str, price: float, volume: int,
                          bid_ask_spread: float = 0.0, days_to_expiry: int = 999,
                          spread_quality: int = 1) -> float:
        _instrument_type = "ETF"
        _s = str(instrument_id).upper()
        if "50ETF" in _s or "300ETF" in _s:
            _instrument_type = "OPTION_ETF" if ("P" in _s or "C" in _s) else "ETF"
        elif _s.startswith("IO") or _s.startswith("MO"):
            _instrument_type = "OPTION_INDEX"
        elif any(k in _s for k in ("M", "Y", "A", "C", "SR", "CF", "TA", "RU", "CU", "AL", "ZN", "AU", "AG")):
            if len(_s) > 4 and any(c.isdigit() for c in _s):
                _instrument_type = "OPTION_COMMODITY"
            else:
                _instrument_type = "FUTURE"
        return compute_slippage_bps(
            price=price, bid_ask_spread=bid_ask_spread,
            base_slippage_bps=3.0, spread_quality=spread_quality,
            days_to_expiry=days_to_expiry, instrument_type=_instrument_type,
        )

    def release_margin_reservation(self, order_id: str) -> None:
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            rs.release_margin(order_id)
        except Exception as e:
            logging.debug("[R14-P0-BIZ-01] 释放保证金预留失败(可忽略): order_id=%s err=%s", order_id, e)

    def check_risk_block(self, instrument_id: str) -> bool:
        _until = self._risk_block_until.get(instrument_id, 0.0)
        if _until > 0 and time.time() < _until:
            return True
        if _until > 0 and time.time() >= _until:
            del self._risk_block_until[instrument_id]
            logging.info("[OrderRiskGuard] R24-P1-CF-03: 风控阻断自动恢复: %s", instrument_id)
        return False

    def set_risk_block(self, instrument_id: str, duration: float = None) -> None:
        _dur = duration if duration is not None else self._risk_block_timeout
        self._risk_block_until[instrument_id] = time.time() + _dur
        logging.warning("[OrderRiskGuard] R24-P1-CF-03: 风控阻断设置: %s, 持续%.0fs", instrument_id, _dur)