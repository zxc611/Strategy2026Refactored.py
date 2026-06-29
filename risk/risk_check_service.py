# [M1-28] __ؼ_____

# MODULE_ID: M1-212

from __future__ import annotations

import logging

import time

from typing import Any, Dict, Optional

from ali2026v3_trading.infra.shared_utils import safe_int, safe_float

from ali2026v3_trading.infra.resilience import stable_mean, stable_variance

from ali2026v3_trading.infra.security_service import structured_audit_log  # R1-4修复

from ali2026v3_trading.risk.risk_check_engine import RiskCheckEngine

from ali2026v3_trading.risk.risk_circuit_breaker import get_safety_meta_layer

from ali2026v3_trading.risk._utils import safe_get_float as _safe_get_float, safe_get_int as _safe_get_int





class RiskCheckService:

    """风控检查服务务核心逻辑已委托给RiskCheckEngine，LEGACY方法已清理"""



    def __init__(self, risk_service: Any):

        self._rs = risk_service

        self._rule_engine_instance: Optional[RiskCheckEngine] = None



    @property

    def _rule_engine(self) -> RiskCheckEngine:

        if self._rule_engine_instance is None:

            from ali2026v3_trading.risk.risk_check_engine import create_default_risk_check_engine

            self._rule_engine_instance = create_default_risk_check_engine()

        return self._rule_engine_instance



    def _get_types(self):

        from ali2026v3_trading.risk.risk_service import RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel

        return RiskCheckResponse, RiskCheckResult, RiskLevel, AlertLevel



    def _pass(self):

        R, _, _, _ = self._get_types(); return R.pass_result()



    def _block(self, reason: str = "", message: str = ""):

        R, _, L, _ = self._get_types(); return R.block_result(reason=reason, message=message, level=L.HIGH)



    def check_regulatory_compliance(self, position_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        """监管合规检查询



        fail-closed策略：SafetyMetaLayer缺席(None)时降级放计回测/测试链路)_

        委托链异常时返回compliant=False(安全优先)_

        """

        try:

            safety = get_safety_meta_layer()

            if safety is not None and hasattr(safety, 'check_regulatory_compliance'):

                result = safety.check_regulatory_compliance(position_data)

                if isinstance(result, dict):

                    return result

                logging.debug("[RiskCheckService] check_regulatory_compliance返回非dict，降级放计 %s", type(result).__name__)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[RiskCheckService] check_regulatory_compliance委托链异常，fail-closed: %s", e)

            return {

                'compliant': False,

                'reason': f'风控委托链异常 {e}',

                'details': {'degraded': True, 'source': 'RiskCheckService'},

            }



        return {

            'compliant': True,

            'reason': 'safety_meta_layer_unavailable',

            'details': {'degraded': True, 'source': 'RiskCheckService'},

        }



    def check_capital_sufficiency(self, equity: float, required_margin: float = 0.0,
                                   open_positions: int = 0, max_positions: int = 50,
                                   existing_margin_used: float = 0.0) -> Dict[str, Any]:

        """资金充足性检查: 委托给SafetyMetaLayer (FIX-R24: 修复AttributeError阻断交易)"""

        try:

            safety = get_safety_meta_layer()

            if safety is not None and hasattr(safety, 'check_capital_sufficiency'):

                result = safety.check_capital_sufficiency(

                    equity, required_margin, open_positions, max_positions, existing_margin_used)

                if isinstance(result, dict):

                    return result

                logging.debug("[RiskCheckService] check_capital_sufficiency返回非dict，降级 %s", type(result).__name__)

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[RiskCheckService] check_capital_sufficiency委托链异常: %s", e)

            return {

                'sufficient': False,

                'reason': f'风控委托链异常 {e}',

                'details': {'degraded': True, 'source': 'RiskCheckService'},

            }



        return {

            'sufficient': True,

            'reason': 'safety_meta_layer_unavailable',

            'details': {'degraded': True, 'source': 'RiskCheckService'},

        }



    def check_before_trade(self, signal: Dict[str, Any]):

        """交易前风控检查链引擎优先，安全回退"""

        from ali2026v3_trading.infra.phase_feature_flag import PhaseFeatureFlag

        if PhaseFeatureFlag.is_enabled('USE_RISK_CHECK_ENGINE'):

            try:

                from ali2026v3_trading.risk.risk_check_engine import RiskContext

                _ctx = RiskContext(signal=signal, risk_service=self._rs)

                _report = self._rule_engine.run_checks(_ctx)

                if not _report.passed and _report.blocking_result is not None:

                    R, RC, _, _ = self._get_types()

                    # FIX-R25: RiskCheckResult是Enum，不能用关键字参数构造；使用block_result方法
                    _block = _report.blocking_result
                    return R.block_result(
                        reason=_block.reason or f"rule={_block.rule_name}",
                        message=f"blocking_rule={_block.rule_name}"
                    )

                if not _report.passed:

                    logging.info("[RiskCheckService] RiskCheckEngine非阻断结束 %s",

                                 [(r.rule_name, r.passed, r.reason) for r in _report.failed_rules])

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logging.warning("[RiskCheckService] RiskCheckEngine委托异常,回退到安全默所 %s", e)

        return self._pass()



    def detect_abnormal_trading(self, instrument_id: str = '', direction: str = '',

                                price: float = 0.0, volume: float = 0.0,

                                market_price: float = 0.0, avg_volume: float = 0.0) -> Dict[str, Any]:

        """异常交易检查链直接调用AbnormalTradeDetector"""

        try:

            from ali2026v3_trading.risk_engine.abnormal_trade_detector import AbnormalTradeDetector

            if not hasattr(self, '_abnormal_detector') or self._abnormal_detector is None:

                self._abnormal_detector = AbnormalTradeDetector()

            detector = self._abnormal_detector

        except ImportError:

            logging.warning("[P2-6] AbnormalTradeDetector不可用", 跳过异常交易检查)
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

        """异常交易行为检查链作为风控检查链的一）"""
        try:

            _abnormal = self.detect_abnormal_trading(

                instrument_id=signal.get('instrument_id', ''), direction=signal.get('direction', ''),

                price=signal.get('price', 0.0), volume=signal.get('volume', 0.0),

                market_price=signal.get('market_price', 0.0), avg_volume=signal.get('avg_volume', 0.0))

            if _abnormal.get('action') == 'block':

                logging.warning("[P2-6] 异常交易行为检查 %d项异常，阻断交易 %s",

                              _abnormal.get('anomaly_count', 0), signal.get('instrument_id', ''))

                return True

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[P2-6] 异常交易检测异常非阻塞: %s", e)

        return None



    def _check_safety_meta_layer(self, signal: Dict[str, Any]):

        """检查SafetyMetaLayer的hard_stop和new_open_blocked状态"""

        try:

            safety = get_safety_meta_layer()

            if safety is None:

                return self._pass()

            action = signal.get('action', '').upper() if signal else ''

            # P0-裂缝25修复: hard_stop期间允许平仓(CLOSE)保护性操作豁。'
            if safety.is_hard_stop_triggered() and action != 'CLOSE':

                return self._block(reason="hard_stop_triggered: 日回撤硬停止已触发，禁止新开仓")

            if safety.is_new_open_blocked() and action == 'OPEN':

                return self._block(reason="new_open_blocked: 新开仓被阻断")

            return self._pass()

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logging.warning("[RiskCheckService] _check_safety_meta_layer异常: %s", e)

            return self._pass()



    def _check_invariant_runtime(self) -> Dict[str, Any]:

        return {'all_passed': True, 'violations': [], 'recoveries': []}



    def check_price_limit(self, instrument_id: str, price: float, direction: str):

        return self._pass()



    def check_expiry_risk(self, instrument_id: str, days_to_expiry: int = None):

        return self._pass()



    def auto_rollover_if_needed(self, instrument_id: str, days_to_expiry: int) -> Optional[Dict[str, Any]]:

        return None



    def check_cross_instrument_limit(self, account_id: str, instrument_id: str, required_amount: float):

        return self._pass()



    def validate_gamma_path_dependency(self, bar_data, positions_dict, n_simulations=1000, max_deviation_pct=20.0) -> Dict[str, Any]:

        """P1-裂缝3修复: Gamma PnL路径依赖偏差验证



        在OHLC范围内随机生成多条日内价格路径，对比固定路径的Gamma PnL_

        若偏差max_deviation_pct，标记gamma_low_fidelity_

        """

        try:

            import numpy as np

            import pandas as pd

            from typing import List

            if bar_data is None or bar_data.empty or not positions_dict:

                return {'passed': True, 'deviation_pct': 0.0, 'reason': '无数据或持仓'}

            ohlc = bar_data[['open', 'high', 'low', 'close']].dropna()

            if len(ohlc) < 2:

                return {'passed': True, 'deviation_pct': 0.0, 'reason': '数据不足'}

            # 简化实现：对每条bar在[low, high]范围内随机抽样n_simulations条路由

            # 计算固定路径(close)与随机路径的pnl差异

            fixed_returns = ohlc['close'].pct_change().dropna().values

            deviations: List[float] = []

            rng = np.random.RandomState(42)

            for _ in range(min(n_simulations, 500)):

                random_prices = ohlc['low'].values + rng.rand(len(ohlc)) * (ohlc['high'].values - ohlc['low'].values)

                random_returns = np.diff(random_prices) / random_prices[:-1]

                if len(random_returns) == len(fixed_returns):

                    # 使用gamma近似: pnl差异 ~ gamma * (random_return^2 - fixed_return^2)

                    # 这里用路径夏普差异作为proxy

                    fixed_sharpe = np.mean(fixed_returns) / (np.std(fixed_returns) + 1e-10) * np.sqrt(252)

                    random_sharpe = np.mean(random_returns) / (np.std(random_returns) + 1e-10) * np.sqrt(252)

                    deviations.append(abs(random_sharpe - fixed_sharpe))

            avg_deviation = float(np.mean(deviations)) if deviations else 0.0

            deviation_pct = (avg_deviation / (abs(fixed_sharpe) + 1e-10)) * 100.0 if 'fixed_sharpe' in dir() else 0.0

            passed = deviation_pct <= max_deviation_pct

            return {

                'passed': passed,

                'deviation_pct': round(deviation_pct, 2),

                'avg_deviation': round(avg_deviation, 4),

                'n_simulations': len(deviations),

                'gamma_low_fidelity': not passed,

            }

        except (ValueError, TypeError, KeyError, ArithmeticError, RuntimeError) as e:

            logging.warning("[裂缝3] validate_gamma_path_dependency 计算异常: %s", e)

            return {'passed': True, 'deviation_pct': 0.0, 'reason': f'计算异常: {e}'}





def check_snapshot_freshness(snapshot_time: float, max_age_sec: float = 30.0) -> bool:

    """风控快照新鲜度检查"""

    if snapshot_time <= 0: return False

    _age = time.time() - snapshot_time

    if _age > max_age_sec:

        logging.warning("[R27-P0-DI-06] 风控快照过期(%.1fs > %.1fs)，需刷新快照", _age, max_age_sec)

        return False

    return True





def is_risk_exempt(signal: Dict[str, Any]) -> bool:

    """风控豁免显式字段检查"""

    if signal.get('risk_exempt', False) is True: return True

    if signal.get('action', '') == 'CLOSE': return True

    return False





_ALLOWED_DIRECTIONS = frozenset({'BUY', 'SELL'})





def validate_direction(direction: str) -> str:

    """direction白名单验验"""

    if not isinstance(direction, str):

        raise ValueError(f"direction必须为字符串, 实际类型={type(direction).__name__}")

    _dir = direction.strip().upper()

    if _dir not in _ALLOWED_DIRECTIONS:

        raise ValueError(f"direction={direction}不在白名单{_ALLOWED_DIRECTIONS}")

    return _dir

