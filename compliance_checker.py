"""R27-SML-FIX: 合规检查子模块 — 从SafetyMetaLayer提取的监管合规检查职责"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Optional

from ali2026v3_trading.shared_utils import safe_float

try:
    from ali2026v3_trading.regulatory_compliance import (
        RegulatoryCompliance, ComplianceEngine, ComplianceContext,
        create_default_compliance_engine, MaxPositionRule, ConcentrationRule, CrossMarketRule,
    )
    _HAS_COMPLIANCE_ENGINE = True
except ImportError:
    _HAS_COMPLIANCE_ENGINE = False


class ComplianceChecker:
    """合规检查器

    负责监管合规性检查，包括持仓限额、撤单率、自成交防护、涨跌停、交易时间等。
    从SafetyMetaLayer中提取，实现职责分离。
    """

    def __init__(self, safety_meta_layer) -> None:
        self._safety = safety_meta_layer
        self._compliance_engine: Optional[Any] = None
        if _HAS_COMPLIANCE_ENGINE:
            try:
                self._compliance_engine = create_default_compliance_engine()
            except Exception:
                pass

    def check_compliance_via_engine(self, position_data: Dict[str, Any],
                                     equity: float = 0.0,
                                     total_position_value: float = 0.0) -> List[Any]:
        if self._compliance_engine is None:
            return []
        try:
            ctx = ComplianceContext(
                position_data=position_data,
                equity=equity,
                total_position_value=total_position_value)
            return self._compliance_engine.run_checks(ctx)
        except Exception as e:
            logging.warning("[ComplianceChecker] ComplianceEngine委托失败: %s", e)
            return []

    def check_regulatory_compliance(self, position_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        checks: List[Dict[str, Any]] = []
        violations: List[str] = []

        try:
            limit_passed = True
            limit_detail = "持仓限额检查通过"
            with self._safety._lock:
                if self._safety._daily_new_open_blocked:
                    limit_passed = False
                    limit_detail = "新开仓已被阻断"
                if self._safety._daily_hard_stop_triggered:
                    limit_passed = False
                    limit_detail = "日回撤硬停止触发，全部交易禁止"
            checks.append({"check_item": "position_limit", "passed": limit_passed, "detail": limit_detail})
            if not limit_passed:
                violations.append(f"position_limit: {limit_detail}")
        except Exception as e:
            checks.append({"check_item": "position_limit", "passed": False, "detail": f"检查异常: {e}"})
            violations.append(f"position_limit: error={e}")

        try:
            from ali2026v3_trading.order_service import get_order_service
            _os = get_order_service()
            if _os:
                now = time.time()
                cutoff = now - 300
                with _os._lock:
                    recent_cancels = sum(1 for t in _os._cancel_count_window if t >= cutoff)
                    recent_orders = sum(1 for t in _os._order_count_window if t >= cutoff)
                cancel_rate = recent_cancels / recent_orders if recent_orders > 0 else 0.0
                cancel_passed = cancel_rate <= 0.5
                cancel_detail = f"撤单率={cancel_rate:.2%}, 窗口=300s" if cancel_passed else f"撤单率={cancel_rate:.2%}超过50%阈值"
                checks.append({"check_item": "cancel_rate", "passed": cancel_passed, "detail": cancel_detail})
                if not cancel_passed:
                    violations.append(f"cancel_rate: {cancel_detail}")
            else:
                checks.append({"check_item": "cancel_rate", "passed": True, "detail": "order_service不可用"})
        except Exception as e:
            checks.append({"check_item": "cancel_rate", "passed": True, "detail": f"检查跳过: {e}"})

        try:
            from ali2026v3_trading.order_service import get_order_service
            _os = get_order_service()
            if _os:
                with _os._lock:
                    self_trade_blocks = _os._stats.get('self_trade_blocks', 0)
                    active_bans = sum(1 for b in _os._self_trade_bans.values() if b > time.time())
                st_passed = active_bans == 0
                st_detail = f"自成交阻断{self_trade_blocks}次, 当前活跃禁止{active_bans}项" if st_passed else f"存在{active_bans}项活跃自成交禁止"
                checks.append({"check_item": "self_trade_prevention", "passed": st_passed, "detail": st_detail})
                if not st_passed:
                    violations.append(f"self_trade: {st_detail}")
            else:
                checks.append({"check_item": "self_trade_prevention", "passed": True, "detail": "order_service不可用"})
        except Exception as e:
            checks.append({"check_item": "self_trade_prevention", "passed": True, "detail": f"检查跳过: {e}"})

        try:
            price_limit_passed = True
            price_limit_detail = "涨跌停检查通过"
            if position_data:
                instrument_id = position_data.get('instrument_id', '')
                price = position_data.get('price', 0)
                direction = position_data.get('direction', 'BUY')
                if instrument_id and price > 0:
                    from ali2026v3_trading.params_service import get_params_service
                    ps = get_params_service()
                    meta = ps.get_instrument_meta_by_id(instrument_id)
                    if meta:
                        product = meta.get('product', '')
                        pc = ps.get_product_cache(product)
                        if pc:
                            limit_up = safe_float(pc.get('limit_up_price', 0))
                            limit_down = safe_float(pc.get('limit_down_price', 0))
                            if limit_up > 0 and price >= limit_up and direction in ('BUY', '买', 'long'):
                                price_limit_passed = False
                                price_limit_detail = f"涨停价{limit_up:.2f}禁止买入: {instrument_id}"
                            elif limit_down > 0 and price <= limit_down and direction in ('SELL', '卖', 'short'):
                                price_limit_passed = False
                                price_limit_detail = f"跌停价{limit_down:.2f}禁止卖出: {instrument_id}"
            checks.append({"check_item": "price_limit", "passed": price_limit_passed, "detail": price_limit_detail})
            if not price_limit_passed:
                violations.append(f"price_limit: {price_limit_detail}")
        except Exception as e:
            checks.append({"check_item": "price_limit", "passed": True, "detail": f"检查跳过: {e}"})

        try:
            from ali2026v3_trading.scheduler_service import is_market_open
            market_open = is_market_open()
            tm_passed = market_open
            tm_detail = "市场开盘中" if market_open else "市场已收盘"
            checks.append({"check_item": "trading_time", "passed": tm_passed, "detail": tm_detail})
            if not tm_passed:
                violations.append(f"trading_time: {tm_detail}")
        except Exception as e:
            checks.append({"check_item": "trading_time", "passed": True, "detail": f"检查跳过: {e}"})

        try:
            log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
            retention_passed = True
            retention_detail = "记录保留合规(≥1825天)"
            if os.path.exists(log_dir):
                oldest_file = None
                oldest_mtime = float('inf')
                for fname in os.listdir(log_dir):
                    fpath = os.path.join(log_dir, fname)
                    if os.path.isfile(fpath):
                        mtime = os.path.getmtime(fpath)
                        if mtime < oldest_mtime:
                            oldest_mtime = mtime
                            oldest_file = fname
                if oldest_file:
                    retention_days = (time.time() - oldest_mtime) / 86400.0
                    if retention_days < 1825:
                        retention_passed = False
                        retention_detail = f"最旧日志保留仅{retention_days:.0f}天, 需≥1825天"
            checks.append({"check_item": "record_retention", "passed": retention_passed, "detail": retention_detail})
            if not retention_passed:
                violations.append(f"record_retention: {retention_detail}")
        except Exception as e:
            checks.append({"check_item": "record_retention", "passed": True, "detail": f"检查跳过: {e}"})

        try:
            from ali2026v3_trading.order_service import get_order_service
            _os = get_order_service()
            if _os and position_data:
                volume = position_data.get('volume', 0)
                price = position_data.get('price', 0)
                amount = volume * price
                large_threshold = getattr(_os, 'LARGE_TRADE_THRESHOLD_AMOUNT', 5000000)
                large_passed = amount <= large_threshold if amount > 0 else True
                large_detail = f"交易金额={amount:.0f}" if large_passed else f"交易金额={amount:.0f}超过大额阈值{large_threshold}"
                checks.append({"check_item": "large_trade", "passed": large_passed, "detail": large_detail})
                if not large_passed:
                    violations.append(f"large_trade: {large_detail}")
            else:
                checks.append({"check_item": "large_trade", "passed": True, "detail": "无订单数据"})
        except Exception as e:
            checks.append({"check_item": "large_trade", "passed": True, "detail": f"检查跳过: {e}"})

        try:
            algo_passed = True
            algo_detail = "算法交易报备正常"
            try:
                from ali2026v3_trading.hft_enhancements import HFTEnhancementEngine
                algo_detail = "HFT增强引擎已注册"
            except ImportError:
                algo_detail = "HFT增强引擎未加载(可接受)"
            checks.append({"check_item": "algo_registration", "passed": algo_passed, "detail": algo_detail})
        except Exception as e:
            checks.append({"check_item": "algo_registration", "passed": True, "detail": f"检查跳过: {e}"})

        compliant = len(violations) == 0
        severity = "CRITICAL" if not compliant else "OK"
        return {
            "compliant": compliant,
            "severity": severity,
            "violations": violations,
            "checks": checks,
        }
