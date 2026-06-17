# MODULE_ID: M1-227
from __future__ import annotations

import time
import logging
from typing import Dict, Any, Optional
from ali2026v3_trading.risk.risk_service import RiskCheckResponse, RiskLevel
from ali2026v3_trading.risk_engine.snapshot import RiskSnapshot
from ali2026v3_trading.risk_engine import input_builder, result_handler
from ali2026v3_trading.risk_engine import market_risk, counterparty_risk, operational_risk, regulatory_risk
from ali2026v3_trading.risk_engine.log_deduplicator import LogDeduplicator
from ali2026v3_trading.risk_engine.abnormal_trade_detector import AbnormalTradeDetector

try:
    from ali2026v3_trading.strategy_judgment.causal_chain_utils import CyclicDependencyGuard
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False


class RiskEngine:
    def __init__(self, risk_service: Any):
        self._risk_service = risk_service
        self._log_dedup = LogDeduplicator()
        self._abnormal_detector = risk_service._abnormal_detector

    def check_before_trade(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        _check_dedup_key = f"{signal.get('instrument_id', '')}_{signal.get('direction', '')}_{signal.get('action', '')}"
        _cyclic_guard = CyclicDependencyGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        result = self._validate_signal(signal, _check_dedup_key, _cyclic_guard)
        if result is not None:
            return result
        snapshot = input_builder.build_snapshot(signal, self._risk_service)
        with self._risk_service._risk_check_lock:
            try:
                safety_result = self._check_safety_meta_layer(signal)
                if safety_result.is_block:
                    return self._risk_service._record_result(safety_result)
                result = operational_risk.check_operational_risks(snapshot, self._risk_service)
                if result is not None:
                    return self._risk_service._record_result(result)
                result = market_risk.check_market_risks(snapshot, self._risk_service)
                if result is not None:
                    return self._risk_service._record_result(result)
                result = counterparty_risk.check_counterparty_risks(snapshot, self._risk_service)
                if result is not None:
                    return self._risk_service._record_result(result)
                result = regulatory_risk.check_regulatory_risks(snapshot, self._risk_service)
                if result is not None:
                    return self._risk_service._record_result(result)
            except (KeyError, AttributeError, TypeError, ValueError, RuntimeError) as e:
                _exc_type = type(e).__name__
                if isinstance(e, (KeyError, AttributeError)):
                    _block_reason = f"check_config_error:{_exc_type}"
                elif isinstance(e, (TypeError, ValueError)):
                    _block_reason = f"check_param_error:{_exc_type}"
                else:
                    _block_reason = f"check_error:{_exc_type}"
                logging.error("[RiskEngine] 风控检查异常(%s)，fail-safe阻断: %s, signal={symbol=%s, direction=%s, action=%s}", _exc_type, e, signal.get('symbol', ''), signal.get('direction', ''), signal.get('action', ''))
                signal['risk_check_result'] = {
                    'result': 'BLOCK',
                    'reason': f"{_block_reason}: {e}",
                    'level': 'CRITICAL',
                }
                return RiskCheckResponse.block_result("check_error", f"风控检查异常，采用fail-safe阻断: {e}", RiskLevel.CRITICAL)
            result = self._detect_abnormal_trading_step(signal)
            if result is not None:
                return result
            return self._risk_service._record_and_publish(signal, _check_dedup_key, _cyclic_guard)

    def _validate_signal(self, signal: Dict[str, Any], check_dedup_key: str, cyclic_guard: Any) -> Optional[RiskCheckResponse]:
        return self._risk_service._validate_signal(signal, check_dedup_key, cyclic_guard)

    def _check_safety_meta_layer(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        return self._risk_service._check_safety_meta_layer(signal)

    def _detect_abnormal_trading_step(self, signal: Dict[str, Any]) -> Optional[RiskCheckResponse]:
        _abnormal = self._risk_service.detect_abnormal_trading(
            instrument_id=signal.get('instrument_id', ''),
            direction=signal.get('direction', ''),
            price=signal.get('price', 0.0),
            volume=signal.get('volume', 0.0),
            market_price=signal.get('market_price', 0.0),
            avg_volume=signal.get('avg_volume', 0.0),
        )
        if _abnormal.get('action') == 'block':
            return RiskCheckResponse.block_result(
                "abnormal_trading_detected",
                f"异常交易行为: {_abnormal.get('anomaly_count', 0)}项",
                RiskLevel.CRITICAL,
            )
        self._abnormal_detector.record_trade(
            signal.get('instrument_id', ''),
            signal.get('direction', ''),
            signal.get('volume', 0.0),
            signal.get('price', 0.0),
        )
        return None

    @property
    def risk_service(self) -> Any:
        return self._risk_service

    @property
    def log_dedup(self) -> LogDeduplicator:
        return self._log_dedup

    @property
    def abnormal_detector(self) -> AbnormalTradeDetector:
        return self._abnormal_detector
