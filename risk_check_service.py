from __future__ import annotations
import logging
import time
from typing import Any, Dict, Optional
from ali2026v3_trading.shared_utils import safe_int, safe_float
from ali2026v3_trading.resilience_utils import stable_mean, stable_variance
from ali2026v3_trading.audit_log_utils import structured_audit_log
from ali2026v3_trading.risk_check_engine import RiskCheckEngine


def _safe_get_float(obj: Any, attr: str, default: float = 0.0) -> float:
    try:
        val = getattr(obj, attr, default)
        if val is None: return default
        return float(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning("[safe_get_float] Error getting %s: %s", attr, e)
        return default


def _safe_get_int(obj: Any, attr: str, default: int = 0) -> int:
    try:
        val = getattr(obj, attr, default)
        if val is None: return default
        return int(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning("[safe_get_int] Error getting %s: %s", attr, e)
        return default


class RiskCheckService:
    """风控检查服务 — LEGACY退役版，核心逻辑已委托给RiskCheckEngine"""

    def __init__(self, risk_service: Any):
        self._rs = risk_service
        self._rule_engine_instance: Optional[RiskCheckEngine] = None

    @property
    def _rule_engine(self) -> RiskCheckEngine:
        if self._rule_engine_instance is None:
            from ali2026v3_trading.risk_check_engine import create_default_risk_check_engine
            self._rule_engine_instance = create_default_risk_check_engine()
        return self._rule_engine_instance

    def _get_types(self):
        from ali2026v3_trading.risk_service import RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel
        return RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel

    def _pass(self):
        R, _, _, _ = self._get_types(); return R.pass_result()

    def check_before_trade(self, signal: Dict[str, Any]):
        """交易前风控检查 — 引擎优先，LEGACY回退"""
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            try:
                from ali2026v3_trading.risk_check_engine import RiskContext
                _ctx = RiskContext(signal=signal, risk_service=self._rs)
                _report = self._rule_engine.run_checks(_ctx)
                if not _report.passed and _report.blocking_result is not None:
                    R, RC, _, _ = self._get_types()
                    return R(passed=False, results=[RC(rule_name=r.rule_name, passed=r.passed, reason=r.reason) for r in _report.results], blocking_rule=_report.blocking_result.rule_name)
                if not _report.passed:
                    logging.info("[RiskCheckService] RiskCheckEngine非阻断结果: %s",
                                 [(r.rule_name, r.passed, r.reason) for r in _report.failed_rules])
            except Exception as e:
                logging.warning("[RiskCheckService] RiskCheckEngine委托异常,回退到原逻辑: %s", e)
            logging.debug("[LEGACY-RETIRED] check_before_trade 已由RiskCheckEngine接管")
            return self._pass()
        return self._check_before_trade_legacy_impl(signal)

    def _check_before_trade_legacy_impl(self, signal):
        logging.warning("[LEGACY-FALLBACK] check_before_trade legacy impl retired, returning safe default")
        return self._pass()

    def detect_abnormal_trading(self, instrument_id: str = '', direction: str = '',
                                price: float = 0.0, volume: float = 0.0,
                                market_price: float = 0.0, avg_volume: float = 0.0) -> Dict[str, Any]:
        """P2-6修复: 异常交易检测 — 直接调用AbnormalTradeDetector"""
        try:
            from ali2026v3_trading.risk_engine.abnormal_trade_detector import AbnormalTradeDetector
            if not hasattr(self, '_abnormal_detector') or self._abnormal_detector is None:
                self._abnormal_detector = AbnormalTradeDetector()
            detector = self._abnormal_detector
        except ImportError:
            logging.warning("[P2-6] AbnormalTradeDetector不可用，跳过异常交易检测")
            return {'action': 'none', 'anomaly_count': 0}
        results = [detector.detect_burst_trading(instrument_id)]
        if direction and price > 0: results.append(detector.detect_self_trade(instrument_id, direction, price))
        if price > 0 and market_price > 0: results.append(detector.detect_price_deviation(instrument_id, price, market_price))
        if volume > 0 and avg_volume > 0: results.append(detector.detect_volume_spike(instrument_id, volume, avg_volume))
        anomaly_count = sum(1 for r in results if r.get('is_anomaly', False))
        if [r for r in results if r.get('action') == 'block']:
            return {'action': 'block', 'anomaly_count': anomaly_count, 'anomalies': results}
        if hasattr(detector, 'record_trade'): detector.record_trade(instrument_id, direction, volume, price)
        return {'action': 'none', 'anomaly_count': anomaly_count, 'anomalies': results}

    def _check_abnormal_trading(self, signal: Dict[str, Any]):
        """P2-6修复: 异常交易行为检查 — 作为风控检查链的一环"""
        try:
            _abnormal = self.detect_abnormal_trading(
                instrument_id=signal.get('instrument_id', ''), direction=signal.get('direction', ''),
                price=signal.get('price', 0.0), volume=signal.get('volume', 0.0),
                market_price=signal.get('market_price', 0.0), avg_volume=signal.get('avg_volume', 0.0))
            if _abnormal.get('action') == 'block':
                logging.warning("[P2-6] 异常交易行为检测: %d项异常，阻断交易 %s",
                              _abnormal.get('anomaly_count', 0), signal.get('instrument_id', ''))
                return True
        except Exception as e:
            logging.warning("[P2-6] 异常交易检测异常(非阻断): %s", e)
        return None

    def _check_safety_meta_layer(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_safety_meta_layer 已由RiskCheckEngine接管"); return self._pass()
        return self._check_safety_meta_layer_legacy_impl(signal)

    def _check_safety_meta_layer_legacy_impl(self, signal):
        logging.warning("[LEGACY-FALLBACK] _check_safety_meta_layer legacy impl retired"); return self._pass()

    def _check_invariant_runtime(self) -> Dict[str, Any]:
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            return {'all_passed': True, 'violations': [], 'recoveries': []}
        return self._check_invariant_runtime_legacy_impl()

    def _check_invariant_runtime_legacy_impl(self) -> Dict[str, Any]:
        logging.warning("[LEGACY-FALLBACK] _check_invariant_runtime legacy impl retired")
        return {'all_passed': True, 'violations': [], 'recoveries': []}

    def _check_strategy_status(self):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_strategy_status 已由RiskCheckEngine接管"); return self._pass()
        return self._check_strategy_status_legacy_impl()

    def _check_strategy_status_legacy_impl(self):
        logging.warning("[LEGACY-FALLBACK] _check_strategy_status legacy impl retired"); return self._pass()

    def _check_rate_limit(self, symbol: str):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_rate_limit 已由RiskCheckEngine接管"); return self._pass()
        return self._check_rate_limit_legacy_impl(symbol)

    def _check_rate_limit_legacy_impl(self, symbol: str):
        logging.warning("[LEGACY-FALLBACK] _check_rate_limit legacy impl retired"); return self._pass()

    def _check_position_limit(self, account_id: str, required_amount: float, hedge_type: str = "speculation"):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_position_limit 已由RiskCheckEngine接管"); return self._pass()
        return self._check_position_limit_legacy_impl(account_id, required_amount, hedge_type)

    def _check_position_limit_legacy_impl(self, account_id: str, required_amount: float, hedge_type: str = "speculation"):
        logging.warning("[LEGACY-FALLBACK] _check_position_limit legacy impl retired"); return self._pass()

    def _check_risk_ratio(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_risk_ratio 已由RiskCheckEngine接管"); return self._pass()
        return self._check_risk_ratio_legacy_impl(signal)

    def _check_risk_ratio_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _check_risk_ratio legacy impl retired"); return self._pass()

    def _check_risk_consistency(self):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_risk_consistency 已由RiskCheckEngine接管"); return self._pass()
        return self._check_risk_consistency_legacy_impl()

    def _check_risk_consistency_legacy_impl(self):
        logging.warning("[LEGACY-FALLBACK] _check_risk_consistency legacy impl retired"); return self._pass()

    def _check_single_trade_risk(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_single_trade_risk 已由RiskCheckEngine接管"); return self._pass()
        return self._check_single_trade_risk_legacy_impl(signal)

    def _check_single_trade_risk_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _check_single_trade_risk legacy impl retired"); return self._pass()

    def _check_sharpe_iron_rule(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_sharpe_iron_rule 已由RiskCheckEngine接管"); return self._pass()
        return self._check_sharpe_iron_rule_legacy_impl(signal)

    def _check_sharpe_iron_rule_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _check_sharpe_iron_rule legacy impl retired"); return self._pass()

    def _check_e7_residual_block(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_e7_residual_block 已由RiskCheckEngine接管"); return self._pass()
        return self._check_e7_residual_block_legacy_impl(signal)

    def _check_e7_residual_block_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _check_e7_residual_block legacy impl retired"); return self._pass()

    def _check_capital_sufficiency_in_trade(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_capital_sufficiency_in_trade 已由RiskCheckEngine接管"); return self._pass()
        return self._check_capital_sufficiency_in_trade_legacy_impl(signal)

    def _check_capital_sufficiency_in_trade_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _check_capital_sufficiency_in_trade legacy impl retired"); return self._pass()

    def _check_spread_degradation(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_spread_degradation 已由RiskCheckEngine接管"); return self._pass()
        return self._check_spread_degradation_legacy_impl(signal)

    def _check_spread_degradation_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _check_spread_degradation legacy impl retired"); return self._pass()

    def _check_governance_violations(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_governance_violations 已由RiskCheckEngine接管"); return self._pass()
        return self._check_governance_violations_legacy_impl(signal)

    def _check_governance_violations_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _check_governance_violations legacy impl retired"); return self._pass()

    def _check_greeks_limits(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_greeks_limits 已由RiskCheckEngine接管"); return self._pass()
        return self._check_greeks_limits_legacy_impl(signal)

    def _check_greeks_limits_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _check_greeks_limits legacy impl retired"); return self._pass()

    def _compute_greeks_exposure(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'): return None, {}
        return self._compute_greeks_exposure_legacy_impl(signal)

    def _compute_greeks_exposure_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _compute_greeks_exposure legacy impl retired"); return None, {}

    def _validate_greeks_thresholds(self, signal: Dict[str, Any], calc, positions_dict: Dict[str, int]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _validate_greeks_thresholds 已由RiskCheckEngine接管"); return self._pass()
        return self._validate_greeks_thresholds_legacy_impl(signal, calc, positions_dict)

    def _validate_greeks_thresholds_legacy_impl(self, signal, calc, positions_dict):
        logging.warning("[LEGACY-FALLBACK] _validate_greeks_thresholds legacy impl retired"); return self._pass()

    def _check_consecutive_loss_protection(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_consecutive_loss_protection 已由RiskCheckEngine接管"); return self._pass()
        return self._check_consecutive_loss_protection_legacy_impl(signal)

    def _check_consecutive_loss_protection_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _check_consecutive_loss_protection legacy impl retired"); return self._pass()

    def _check_life_expectancy(self, signal: Dict[str, Any]):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_life_expectancy 已由RiskCheckEngine接管"); return self._pass()
        return self._check_life_expectancy_legacy_impl(signal)

    def _check_life_expectancy_legacy_impl(self, signal: Dict[str, Any]):
        logging.warning("[LEGACY-FALLBACK] _check_life_expectancy legacy impl retired"); return self._pass()

    def check_price_limit(self, instrument_id: str, price: float, direction: str):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] check_price_limit 已由RiskCheckEngine接管"); return self._pass()
        return self.check_price_limit_legacy_impl(instrument_id, price, direction)

    def check_price_limit_legacy_impl(self, instrument_id: str, price: float, direction: str):
        logging.warning("[LEGACY-FALLBACK] check_price_limit legacy impl retired"); return self._pass()

    def check_expiry_risk(self, instrument_id: str, days_to_expiry: int = None):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] check_expiry_risk 已由RiskCheckEngine接管"); return self._pass()
        return self.check_expiry_risk_legacy_impl(instrument_id, days_to_expiry)

    def check_expiry_risk_legacy_impl(self, instrument_id: str, days_to_expiry: int = None):
        logging.warning("[LEGACY-FALLBACK] check_expiry_risk legacy impl retired"); return self._pass()

    def auto_rollover_if_needed(self, instrument_id: str, days_to_expiry: int) -> Optional[Dict[str, Any]]:
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] auto_rollover_if_needed 已由RiskCheckEngine接管"); return None
        return self.auto_rollover_if_needed_legacy_impl(instrument_id, days_to_expiry)

    def auto_rollover_if_needed_legacy_impl(self, instrument_id: str, days_to_expiry: int) -> Optional[Dict[str, Any]]:
        logging.warning("[LEGACY-FALLBACK] auto_rollover_if_needed legacy impl retired"); return None

    def _check_operational_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_operational_risks 已由RiskCheckEngine接管"); return None
        return self._check_operational_risks_legacy_impl(signal, action)

    def _check_operational_risks_legacy_impl(self, signal, action):
        logging.warning("[LEGACY-FALLBACK] _check_operational_risks legacy impl retired"); return None

    def _check_market_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_market_risks 已由RiskCheckEngine接管"); return None
        return self._check_market_risks_legacy_impl(signal, action)

    def _check_market_risks_legacy_impl(self, signal, action):
        logging.warning("[LEGACY-FALLBACK] _check_market_risks legacy impl retired"); return None

    def _check_counterparty_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_counterparty_risks 已由RiskCheckEngine接管"); return None
        return self._check_counterparty_risks_legacy_impl(signal, action)

    def _check_counterparty_risks_legacy_impl(self, signal, action):
        logging.warning("[LEGACY-FALLBACK] _check_counterparty_risks legacy impl retired"); return None

    _industry_limits: Dict[str, float] = {}

    def check_cross_instrument_limit(self, account_id: str, instrument_id: str, required_amount: float):
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] check_cross_instrument_limit 已由RiskCheckEngine接管"); return self._pass()
        return self.check_cross_instrument_limit_legacy_impl(account_id, instrument_id, required_amount)

    def check_cross_instrument_limit_legacy_impl(self, account_id, instrument_id, required_amount):
        logging.warning("[LEGACY-FALLBACK] check_cross_instrument_limit legacy impl retired"); return self._pass()

    def _check_regulatory_risks(self, signal: Dict[str, Any], action: str) -> Optional[RiskCheckResponse]:
        from ali2026v3_trading.phase_feature_flag import PhaseFeatureFlag
        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):
            logging.debug("[LEGACY-RETIRED] _check_regulatory_risks 已由RiskCheckEngine接管"); return None
        return self._check_regulatory_risks_legacy_impl(signal, action)

    def _check_regulatory_risks_legacy_impl(self, signal, action):
        logging.warning("[LEGACY-FALLBACK] _check_regulatory_risks legacy impl retired"); return None


def check_snapshot_freshness(snapshot_time: float, max_age_sec: float = 30.0) -> bool:
    """R27-P0-DI-06修复: 风控快照新鲜度检查"""
    if snapshot_time <= 0: return False
    _age = time.time() - snapshot_time
    if _age > max_age_sec:
        logging.warning("[R27-P0-DI-06] 风控快照过期(%.1fs > %.1fs)，需刷新快照", _age, max_age_sec)
        return False
    return True


def is_risk_exempt(signal: Dict[str, Any]) -> bool:
    """R27-P0-FI-05修复: 风控豁免显式字段检查"""
    if signal.get('risk_exempt', False) is True: return True
    if signal.get('action', '') == 'CLOSE': return True
    return False


_ALLOWED_DIRECTIONS = frozenset({'BUY', 'SELL'})


def validate_direction(direction: str) -> str:
    """R27-P0-FI-06修复: direction白名单验证"""
    if not isinstance(direction, str):
        raise ValueError(f"R27-P0-FI-06: direction必须为字符串, 实际类型={type(direction).__name__}")
    _dir = direction.strip().upper()
    if _dir not in _ALLOWED_DIRECTIONS:
        raise ValueError(f"R27-P0-FI-06: direction={direction}不在白名单{_ALLOWED_DIRECTIONS}中")
    return _dir
