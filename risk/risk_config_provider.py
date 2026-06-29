# [M1-32] ________ṩ__

# MODULE_ID: M1-215

# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

from __future__ import annotations



import logging

import threading

from typing import Any, Dict



from ali2026v3_trading.infra.shared_utils import safe_int, safe_float

from ._utils import safe_get_float as _safe_get_float, safe_get_int as _safe_get_int





class RiskConfigProvider:

    """CC-03/AP-02修复: 风控配置参数提供者



    从RiskService提取的D类方法（配置/参数管理），

    统一管理风控参数的读取和默认值

    构造函数接受risk_service引用（用于访问共享状态）_

    """



    _CONTRACT_MULTIPLIER_CACHE: Dict[str, float] = {}

    _MULTIPLIER_CACHE_LOCK = threading.Lock()



    def __init__(self, risk_service: Any):

        self._rs = risk_service



    def _get_signal_cooldown(self) -> float:

        return _safe_get_float(self._rs.params, "signal_cooldown_sec", 60.0)



    def _get_max_signals_per_window(self) -> int:

        return _safe_get_int(self._rs.params, "max_signals_per_window", 10)



    def _get_rate_limit_window(self) -> float:

        return _safe_get_float(self._rs.params, "rate_limit_window_sec", 120.0)



    def _get_max_risk_ratio(self) -> float:

        """P1-57修复: 默认值从get_param_default获取"""

        from ali2026v3_trading.config.config_params import get_param_default

        default = get_param_default('max_risk_ratio')

        return _safe_get_float(self._rs.params, "max_risk_ratio", default if default is not None else 0.8)



    def _get_total_position_limit(self) -> float:

        return _safe_get_float(self._rs.params, "total_position_limit", 1000000.0)



    def _get_contract_multiplier(self, instrument_id: str) -> float:

        with RiskConfigProvider._MULTIPLIER_CACHE_LOCK:

            if instrument_id in RiskConfigProvider._CONTRACT_MULTIPLIER_CACHE:

                return RiskConfigProvider._CONTRACT_MULTIPLIER_CACHE[instrument_id]



        multiplier = 1.0



        try:

            calc = self._rs._get_greeks_calculator()

            if calc is not None and hasattr(calc, '_contract_multiplier'):

                info = calc._option_info_cache.get(instrument_id, {})

                future_product = info.get('future_product', '')

                if future_product and future_product in calc._contract_multiplier:

                    multiplier = calc._contract_multiplier[future_product]

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.warning("[R22-EP-P1] RiskService exception swallowed")

            pass



        if multiplier == 1.0:

            try:

                from ali2026v3_trading.config.params_service import get_params_service

                params_svc = get_params_service()

                meta = params_svc.get_instrument_meta_by_id(instrument_id)

                if meta:

                    product = meta.get('product', '')

                    product_cache = params_svc.get_product_cache(product)

                    if product_cache:

                        multiplier = safe_float(product_cache.get('contract_size', 1.0))

            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                logging.warning("[R22-EP-P1] RiskService exception swallowed")

                pass



        if multiplier == 1.0:

            try:

                from ali2026v3_trading.config.config_service import get_cached_params

                _params = get_cached_params()

                if instrument_id and ('C' in instrument_id or 'P' in instrument_id):

                    multiplier = safe_float(_params.get('option_contract_multiplier', 10000))

            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

                logging.warning("[R22-EP-P1] RiskService exception swallowed")

                pass



        with RiskConfigProvider._MULTIPLIER_CACHE_LOCK:

            RiskConfigProvider._CONTRACT_MULTIPLIER_CACHE[instrument_id] = multiplier

        return multiplier



    _MARGIN_RATIO_CACHE: Dict[str, float] = {}



    def _get_margin_ratio(self, instrument_id: str) -> float:

        if instrument_id in RiskConfigProvider._MARGIN_RATIO_CACHE:

            return RiskConfigProvider._MARGIN_RATIO_CACHE[instrument_id]

        margin_ratio = 0.1

        try:

            from ali2026v3_trading.config.params_service import get_params_service

            params_svc = get_params_service()

            meta = params_svc.get_instrument_meta_by_id(instrument_id)

            if meta:

                product = meta.get('product', '')

                product_cache = params_svc.get_product_cache(product)

                if product_cache:

                    margin_ratio = safe_float(product_cache.get('margin_ratio', 0.1))

        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:

            logging.warning("[P0-4-1] margin_ratio fallback to 0.1")

            pass

        if margin_ratio <= 0 or margin_ratio > 1.0:

            margin_ratio = 0.1

        RiskConfigProvider._MARGIN_RATIO_CACHE[instrument_id] = margin_ratio

        return margin_ratio



    def _get_circuit_breaker_pause_sec(self) -> float:

        return _safe_get_float(self._rs.params, "circuit_breaker_pause_sec", 180.0)



    def _get_circuit_breaker_calm_period_sec(self) -> float:

        return _safe_get_float(self._rs.params, "circuit_breaker_calm_period_sec", 600.0)



    def _get_daily_drawdown_multiplier(self) -> float:

        return _safe_get_float(self._rs.params, "daily_drawdown_multiplier", 2.0)



    def _get_hard_time_stop_minutes(self) -> float:

        hft_ms = _safe_get_float(self._rs.params, "hft_hard_time_stop_ms", 0)

        if hft_ms > 0:

            return hft_ms / 60000.0

        spring_sec = _safe_get_float(self._rs.params, "spring_hard_time_stop_sec", 0)

        if spring_sec > 0:

            return spring_sec / 60.0

        resonance_min = _safe_get_float(self._rs.params, "resonance_hard_time_stop_min", 0)

        if resonance_min > 0:

            return resonance_min

        box_min = _safe_get_float(self._rs.params, "box_hard_time_stop_min", 0)

        if box_min > 0:

            return box_min

        return 5.0



    def _get_min_profit_threshold(self) -> float:

        return _safe_get_float(self._rs.params, "min_profit_threshold", 0.002)



    def _get_stage1_minutes(self) -> float:

        _bar_period = _safe_get_float(self._rs.params, "bar_period", 1.0)

        if _bar_period <= 0:

            logging.warning("[R5-L-01] bar_period=%s<=0，使用默所.0", _bar_period)

            _bar_period = 1.0

        val = _safe_get_float(self._rs.params, "stage1_min_minutes", None)

        if val is not None and val > 0:

            return val * _bar_period

        _fallback = _safe_get_float(self._rs.params, "stage1_minutes", 90.0)

        if _fallback <= 0:

            logging.warning("[R5-L-01] stage1_minutes=%s<=0，使用默所0.0", _fallback)

            _fallback = 90.0

        return _fallback * _bar_period



    def _get_stage2_minutes(self) -> float:

        return _safe_get_float(self._rs.params, "stage2_minutes", 240.0)



    def _get_stage1_profit_threshold(self) -> float:

        return _safe_get_float(self._rs.params, "stage1_profit_threshold", 0.002)





    def _get_greeks_calculator(self):

        # P2-R3-D-18: GreeksCalculator不可用时跳过 _当greeks_calculator模块

        # 导入失败(ImportError)或初始化失败(RuntimeError)时返回None,

        # 所有依赖Greeks的功的仪表的限仓/Gamma路径验证)退化为无数据状态机

        # 已知限制: Greeks数据缺失时风险判断依赖其他维度补偿，但组合风险盲区增大。'
        # 建议在系统启动时预检GreeksCalculator可用性并输出WARNING级别告警警

        if self._rs.__class__._greeks_calc is None:

            with self._rs.__class__._greeks_calc_lock:

                if self._rs.__class__._greeks_calc is None:

                    try:

                        from ali2026v3_trading.governance.greeks_calculator import GreeksCalculator

                        self._rs.__class__._greeks_calc = GreeksCalculator()

                    except (ImportError, RuntimeError) as e:

                        logging.debug("[RiskService] GreeksCalculator init failed: %s", e)

                        return None

        return self._rs.__class__._greeks_calc





    def _get_life_estimator(self) -> Any:

        """懒加载行情寿命估计器



        R4-D-03修复: 懒加载单例防护。

        首次调用返回None时记录WARNING，后续调用不再重复尝试导入口

        使用例SENTINEL标记区分「未初始化」和「初始化失败」_

        R5-T-04修复: 添加线程安全锁，防止并发初始化化

        """

        if self._rs._life_estimator is not None:

            return self._rs._life_estimator

        # R4-D-03: 使用sentinel避免重复尝试已失败的初始化

        if getattr(self, '_life_estimator_init_failed', False):

            return None

        # R5-T-04修复: 线程安全初始化

        with self._rs._lock:

            # double-check

            if self._rs._life_estimator is not None:

                return self._rs._life_estimator

            if getattr(self, '_life_estimator_init_failed', False):

                return None

            try:

                from ali2026v3_trading.precompute._quantification_core import BayesianShrinkageLifeEstimator

                self._rs._life_estimator = BayesianShrinkageLifeEstimator()

                if self._rs._life_estimator is None:

                    logging.warning("[R4-D-03] BayesianShrinkageLifeEstimator构造返回None")

                    self._rs._life_estimator_init_failed = True

                return self._rs._life_estimator

            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

                logging.warning("[R4-D-03] BayesianShrinkageLifeEstimator不可逆后续不再重试): %s", e)

                # P2-R3-P-15: BayesianShrinkageLifeEstimator不可用时跳过 _寿命检查降级，D3维度评分退化为0.5

                self._rs._life_estimator_init_failed = True

                return None



    def confirm_alpha_decay_recovery(self):

        # R17重审计修复 重构后该方法丢失，补全实现

        # 1. 设置恢复确认标志

        self._rs._alpha_decay_recovery_confirmed = True

        logging.info("[R16-P1-11] Alpha衰减恢复已确认")

        # 2. 联动清除shadow_engine降级状态

        try:

            from ali2026v3_trading.strategy.shadow_strategy_facade import get_shadow_strategy_engine

            _se = get_shadow_strategy_engine()

            if _se is not None and hasattr(_se, 'clear_degradation'):

                _se.clear_degradation()

                logging.info("[R16-P1-11] ShadowStrategyEngine降级状态已联动清除")

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:

            logging.warning("[R16-P1-11] ShadowStrategyEngine降级清除失败(非阻塞: %s", _e)





