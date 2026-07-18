# [M1-88] TVF参数加载器
# MODULE_ID: M1-017

"""

TVF参数加载器器从参数池配置文件加载六维仓位调整因子参数



支持:

  - 从YAML配置文件加载参数

  - 参数校验 (权重和检查、阈值范围检查

  - 动态重复(运行时更新参数

  - 多模式支持(small/medium/large可配置不同参数

"""



import logging

import os

from typing import Any, Dict, Optional, Tuple



from infra._helpers import get_logger  # R9-5

from infra.serialization_utils import yaml_safe_load



try:

    import yaml

except ImportError:

    yaml = None



logger = get_logger(__name__)  # R9-5





TVF_DEFAULT_PARAMS = {

    "tvf_l1_weight": 0.40,

    "tvf_l2_weight": 0.35,

    "tvf_l3_weight": 0.25,

    "tvf_sortino_threshold": 1.5,

    "tvf_calmar_threshold": 0.8,

    "tvf_sharpe_threshold": 1.2,

    "tvf_gamma_threshold": 0.05,

    "tvf_theta_threshold": -0.02,

    "tvf_vega_threshold": 0.10,

    "tvf_sortino_scale": 0.50,

    "tvf_calmar_scale": 0.30,

    "tvf_sharpe_scale": 0.40,

    "tvf_ofi_scale": 3.00,

    "tvf_smf_scale": 2.00,

    "tvf_gamma_scale": 0.02,

    "tvf_theta_scale": 0.01,

    "tvf_vega_scale": 0.05,

    "tvf_l1_inner_sortino_weight": 0.40,

    "tvf_l1_inner_calmar_weight": 0.30,

    "tvf_l1_inner_sharpe_weight": 0.30,

    "tvf_l2_inner_ofi_weight": 0.40,

    "tvf_l2_inner_cvd_weight": 0.30,

    "tvf_l2_inner_smf_weight": 0.30,

    "tvf_l3_inner_delta_weight": 0.30,

    "tvf_l3_inner_gamma_weight": 0.25,

    "tvf_l3_inner_theta_weight": 0.25,

    "tvf_l3_inner_vega_weight": 0.20,

}





class TVFParamLoader:



    _params: Dict[str, Any] = {}

    _loaded: bool = False



    def __new__(cls):

        from infra.registry_service import SingletonRegistry

        _registry = SingletonRegistry.get_registry('tvf_param_loader')

        _inst = _registry.get('instance')

        if _inst is None:

            _inst = super().__new__(cls)

            _registry.set('instance', _inst)

        return _inst



    @classmethod

    def get_instance(cls) -> 'TVFParamLoader':

        from infra.registry_service import SingletonRegistry

        _registry = SingletonRegistry.get_registry('tvf_param_loader')

        _inst = _registry.get('instance')

        if _inst is None:

            _inst = cls()

        return _inst



    @classmethod

    def reset(cls):

        from infra.registry_service import SingletonRegistry

        _registry = SingletonRegistry.get_registry('tvf_param_loader')

        _registry.remove('instance')

        cls._params = {}

        cls._loaded = False



    def load(self, config_path: Optional[str] = None) -> Dict[str, Any]:

        """加载TVF参数配置文件



        Args:

            config_path: 配置文件路径，默认使用参数池目录下的tvf_params.yaml



        Returns:

            加载的参数字典
        """

        if config_path is None:

            _base_dir = os.path.dirname(os.path.abspath(__file__))

            _project_dir = os.path.dirname(_base_dir)

            _pool_path = os.path.join(_project_dir, "param_pool", "param_configs.yaml")

            _root_path = os.path.join(_base_dir, "param_configs.yaml")

            config_path = _pool_path if os.path.exists(_pool_path) else _root_path



        try:

            with open(config_path, 'r', encoding='utf-8') as f:

                _raw = yaml_safe_load(f)

            self._params = _raw.get('tvf_params', _raw) if isinstance(_raw, dict) else _raw

            self._loaded = True

            logger.info('[TVFParamLoader] 参数加载成功: %s', config_path)



            # 校验参数

            self._validate_params()



        except FileNotFoundError:

            logger.debug('[TVFParamLoader] 配置文件不存在 %s，使用默认值', config_path)

            self._params = self._default_params()

            self._loaded = True

        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:

            logger.error('[TVFParamLoader] 参数加载失败: %s', e)

            self._params = self._default_params()

            self._loaded = True



        return self._params



    def reload(self, config_path: Optional[str] = None) -> Dict[str, Any]:

        """重新加载参数（运行时热更新）"""

        logger.info('[TVFParamLoader] 重新加载参数')

        return self.load(config_path)



    def get_params(self) -> Dict[str, Any]:

        """获取当前加载的参数"""

        if not self._loaded:

            self.load()

        return self._params



    def apply_to_mode_config(self, config) -> 'ModeConfig':

        """将TVF参数应用到ModeConfig



        注意: ModeConfig是frozen的dataclass，需要创建新实例

        """

        from governance.mode_engine import ModeConfig



        params = self.get_params()



        new_kwargs = {

            'tvf_enabled': params.get('tvf_enabled', True),  # R26-P0-CD-02修复: 三源统一为True(YAML=True, Code=True)

            'tvf_l1_weight': params['layer_weights']['l1_risk_return'],

            'tvf_l2_weight': params['layer_weights']['l2_market_micro'],

            'tvf_l3_weight': params['layer_weights']['l3_option_greeks'],

            'tvf_sortino_threshold': params['l1_tri_validation']['sortino_tri_center'],

            'tvf_calmar_threshold': params['l1_tri_validation']['calmar_tri_center'],

            'tvf_sharpe_threshold': params['l1_tri_validation']['sharpe_tri_center'],

            'tvf_sortino_scale': params['l1_tri_validation']['sortino_tri_scale'],

            'tvf_calmar_scale': params['l1_tri_validation']['calmar_tri_scale'],

            'tvf_sharpe_scale': params['l1_tri_validation']['sharpe_tri_scale'],

            'tvf_ofi_scale': params['l2_order_flow']['ofi_scale'],

            'tvf_smf_scale': params['l2_order_flow']['smf_scale'],

            'tvf_gamma_threshold': params['l3_greeks']['gamma_threshold'],

            'tvf_theta_threshold': params['l3_greeks']['theta_threshold'],

            'tvf_vega_threshold': params['l3_greeks']['vega_threshold'],

            'tvf_gamma_scale': params['l3_greeks']['gamma_scale'],

            'tvf_theta_scale': params['l3_greeks']['theta_scale'],

            'tvf_vega_scale': params['l3_greeks']['vega_scale'],

            'tvf_l1_inner_sortino_weight': params['l1_tri_validation']['inner_weights']['sortino'],

            'tvf_l1_inner_calmar_weight': params['l1_tri_validation']['inner_weights']['calmar'],

            'tvf_l1_inner_sharpe_weight': params['l1_tri_validation']['inner_weights']['sharpe'],

            'tvf_l2_inner_ofi_weight': params['l2_order_flow']['inner_weights']['ofi'],

            'tvf_l2_inner_cvd_weight': params['l2_order_flow']['inner_weights']['cvd_divergence'],

            'tvf_l2_inner_smf_weight': params['l2_order_flow']['inner_weights']['smart_money_flow'],

            'tvf_l3_inner_delta_weight': params['l3_greeks']['inner_weights']['delta'],

            'tvf_l3_inner_gamma_weight': params['l3_greeks']['inner_weights']['gamma'],

            'tvf_l3_inner_theta_weight': params['l3_greeks']['inner_weights']['theta'],

            'tvf_l3_inner_vega_weight': params['l3_greeks']['inner_weights']['vega'],

        }



        import dataclasses

        original_fields = {f.name: getattr(config, f.name) for f in dataclasses.fields(config)}



        original_fields.update(new_kwargs)



        return ModeConfig(**original_fields)



    def _validate_params(self) -> None:

        """校验参数合法过"""

        params = self._params

        errors = []

        warnings = []



        # R24-P2-IV-04修复: 检查必要嵌套key是否存在，给出更明确的错误信号
        _REQUIRED_NESTED_KEYS = {

            'layer_weights': ['l1_risk_return', 'l2_market_micro', 'l3_option_greeks'],

            'l1_tri_validation': ['sortino_tri_scale', 'calmar_tri_scale', 'sharpe_tri_scale'],

            'l2_order_flow': ['ofi_scale', 'smf_scale'],

            'l3_greeks': ['delta_scale', 'gamma_scale', 'theta_scale', 'vega_scale'],

        }

        for _group, _keys in _REQUIRED_NESTED_KEYS.items():

            if _group not in params:

                errors.append(f'缺少必要嵌套key: {_group}')

            else:

                for _k in _keys:

                    if _k not in params[_group]:

                        warnings.append(f'嵌套key {_group}.{_k} 缺失，将使用默认证')



        # 1. 校验层间权重复
        lw = params.get('layer_weights', {})

        l1_w = lw.get('l1_risk_return', 0)

        l2_w = lw.get('l2_market_micro', 0)

        l3_w = lw.get('l3_option_greeks', 0)

        l4_w = lw.get('l4_divergence_reversal', 0)  # FIX-BB R12-4-2: 支持第四层权重

        layer_sum = l1_w + l2_w + l3_w + l4_w  # FIX-BB: 四层求和

        if abs(layer_sum - 1.0) > 1e-6:

            errors.append(f'层间权重和不等于1.0: {layer_sum}')



        # 2. 校验L1层内权重复
        l1_inner = params.get('l1_tri_validation', {}).get('inner_weights', {})

        l1_inner_sum = sum(l1_inner.values())

        if abs(l1_inner_sum - 1.0) > 1e-6:

            errors.append(f'L1层内权重和不等于1.0: {l1_inner_sum}')



        # 3. 校验L2层内权重复
        l2_inner = params.get('l2_order_flow', {}).get('inner_weights', {})

        l2_inner_sum = sum(l2_inner.values())

        if abs(l2_inner_sum - 1.0) > 1e-6:

            errors.append(f'L2层内权重和不等于1.0: {l2_inner_sum}')



        # 4. 校验L3层内权重复
        l3_inner = params.get('l3_greeks', {}).get('inner_weights', {})

        l3_inner_sum = sum(l3_inner.values())

        if abs(l3_inner_sum - 1.0) > 1e-6:

            errors.append(f'L3层内权重和不等于1.0: {l3_inner_sum}')



        # 5. 校验scale值必须为期
        scales = [

            ('sortino_tri_scale', params.get('l1_tri_validation', {}).get('sortino_tri_scale', 0)),

            ('calmar_tri_scale', params.get('l1_tri_validation', {}).get('calmar_tri_scale', 0)),

            ('sharpe_tri_scale', params.get('l1_tri_validation', {}).get('sharpe_tri_scale', 0)),

            ('ofi_scale', params.get('l2_order_flow', {}).get('ofi_scale', 0)),

            ('smf_scale', params.get('l2_order_flow', {}).get('smf_scale', 0)),

            ('gamma_scale', params.get('l3_greeks', {}).get('gamma_scale', 0)),

            ('theta_scale', params.get('l3_greeks', {}).get('theta_scale', 0)),

            ('vega_scale', params.get('l3_greeks', {}).get('vega_scale', 0)),

        ]

        for name, value in scales:

            if value <= 0:

                errors.append(f'{name}必须为正数 {value}')



        # 6. 校验阈值合理从
        if params.get('l1_tri_validation', {}).get('sortino_tri_center', 0) < 0:

            warnings.append('sortino_tri_center为负数，可能导致异常行为')



        if params.get('l3_greeks', {}).get('gamma_threshold', 0) < 0:

            warnings.append('gamma_threshold为负数，可能导致异常行为')



        # 输出校验结果

        if errors:

            for e in errors:

                logger.error('[TVFParamLoader] 参数校验错误: %s', e)

            raise ValueError(f'TVF参数校验失败: {errors}')



        if warnings:

            for w in warnings:

                logger.debug('[TVFParamLoader] 参数校验警告: %s', w)



        logger.info('[TVFParamLoader] 参数校验通过')



    @staticmethod

    def _default_params() -> Dict[str, Any]:

        p = TVF_DEFAULT_PARAMS

        return {

            'tvf_enabled': True,  # R26-P0-CD-02修复: 三源统一为True

            'layer_weights': {

                'l1_risk_return': p['tvf_l1_weight'],

                'l2_market_micro': p['tvf_l2_weight'],

                'l3_option_greeks': p['tvf_l3_weight'],

            },

            'l1_tri_validation': {

                'sortino_tri_center': p['tvf_sortino_threshold'],

                'calmar_tri_center': p['tvf_calmar_threshold'],

                'sharpe_tri_center': p['tvf_sharpe_threshold'],

                'sortino_tri_scale': p['tvf_sortino_scale'],

                'calmar_tri_scale': p['tvf_calmar_scale'],

                'sharpe_tri_scale': p['tvf_sharpe_scale'],

                'inner_weights': {

                    'sortino': p['tvf_l1_inner_sortino_weight'],

                    'calmar': p['tvf_l1_inner_calmar_weight'],

                    'sharpe': p['tvf_l1_inner_sharpe_weight'],

                },

            },

            'l2_order_flow': {

                'ofi_scale': p['tvf_ofi_scale'],

                'smf_scale': p['tvf_smf_scale'],

                'inner_weights': {

                    'ofi': p['tvf_l2_inner_ofi_weight'],

                    'cvd_divergence': p['tvf_l2_inner_cvd_weight'],

                    'smart_money_flow': p['tvf_l2_inner_smf_weight'],

                },

            },

            'l3_greeks': {

                'gamma_threshold': p['tvf_gamma_threshold'],

                'theta_threshold': p['tvf_theta_threshold'],

                'vega_threshold': p['tvf_vega_threshold'],

                'gamma_scale': p['tvf_gamma_scale'],

                'theta_scale': p['tvf_theta_scale'],

                'vega_scale': p['tvf_vega_scale'],

                'inner_weights': {

                    'delta': p['tvf_l3_inner_delta_weight'],

                    'gamma': p['tvf_l3_inner_gamma_weight'],

                    'theta': p['tvf_l3_inner_theta_weight'],

                    'vega': p['tvf_l3_inner_vega_weight'],

                },

            },

        }





def get_tvf_param_loader() -> TVFParamLoader:

    """获取TVF参数加载器单播"""

    return TVFParamLoader.get_instance()





def load_tvf_params(config_path: Optional[str] = None) -> Dict[str, Any]:

    """便捷函数：加载TVF参数"""

    loader = get_tvf_param_loader()

    return loader.load(config_path)


# ============================================================================
# 合并自 final_three_layer_config.py — 三层期权五态排序最终落地配置
# 依据文档：docs/audit/三层期权五态排序方案_最终落地方案_20260624.md 第九章
# ============================================================================

MONTH_WEIGHTS_5 = (0.35, 0.25, 0.20, 0.12, 0.08)

ASYMMETRIC_DECAY = {
    'financial':     {'cr': 600, 'cf': 600, 'wr': 120, 'wf': 120, 'other': 60},
    'black_metal':   {'cr': 300, 'cf': 300, 'wr': 60,  'wf': 60,  'other': 60},
    'agricultural':  {'cr': 180, 'cf': 180, 'wr': 60,  'wf': 60,  'other': 60},
    'energy_chem':   {'cr': 240, 'cf': 240, 'wr': 60,  'wf': 60,  'other': 60},
    'default':       {'cr': 300, 'cf': 300, 'wr': 60,  'wf': 60,  'other': 60},
}

DELTA_MIN = 0.15
DELTA_MAX = 0.85
GAMMA_MAX_FRONT_MONTH = 0.08
THETA_MAX_FRONT_MONTH_PCT = 0.015

GAMMA_BOOST_FACTOR = 1.2
GAMMA_BOOST_MIN = 0.05
GAMMA_BOOST_DELTA_RANGE = 0.15
VEGA_PENALTY_THRESHOLD = 0.10
VEGA_PENALTY_IV_CHANGE = 0.05
VEGA_PENALTY_FACTOR = 0.8
THETA_PENALTY_MAX = 0.15
THETA_PENALTY_GAMMA_MIN = 0.03

TIER1_WILSON_THRESHOLD = 0.50  # 默认值，运行时从 SORTER_CONFIG['tier1_wilson_threshold'] 覆盖
# FIX-5 RC-6: 降低TIER2_COVERAGE_THRESHOLD 0.40→0.20，避免只有1-2个月分类数据时coverage=0.2被永久拒入Tier2
# 根因: coverage=active_months/MAX_MONTHS_FOR_SCORING(=5)，1个月→0.20, 2个月→0.40
#       原阈值0.40要求≥2个月，冷启动期间1个月即可达到0.20阈值放行
TIER2_COVERAGE_THRESHOLD = 0.20  # 默认值，运行时从 SORTER_CONFIG['tier2_coverage_threshold'] 覆盖
TIER2_CORRECT_UP_THRESHOLD = 0.45  # 默认值，运行时从 SORTER_CONFIG['tier2_correct_up_threshold'] 覆盖
TIER3_CORRECT_UP_THRESHOLD = 0.35  # 默认值，运行时从 SORTER_CONFIG['tier3_correct_up_threshold'] 覆盖

LAYER1_SORT_MODE = 'lexicographic'
LAYER1_WEIGHT_WILSON = 0.70
LAYER1_WEIGHT_FRESHNESS = 0.20
LAYER1_WEIGHT_LIQUIDITY = 0.10
FILTER_RATE_ALERT_THRESHOLD = 0.30

CLUSTERING_METHOD = 'GMM'
CLUSTERING_FREQUENCY = 'quarterly'
CLUSTERING_LOOKBACK_DAYS = 60
CLUSTER_BIC_RANGE = (4, 6)
CLUSTER_CONFIDENCE_THRESHOLD = 0.60

RESONANCE_VETO_THRESHOLD = 0.80
RESONANCE_MODE = 'veto'

LAYER2_WEIGHT_SCORE1 = 0.70
LAYER2_WEIGHT_RESONANCE = 0.20
LAYER2_WEIGHT_REGIME = 0.10
LAYER2_DISPERSION_THRESHOLD = 0.15
LAYER2_DISPERSION_PENALTY = -0.05
LAYER2_CORRELATION_THRESHOLD = 0.80
LAYER2_CORRELATION_PENALTY = -0.05
LAYER2_SCORE_FLOOR = 0.60

CLUSTER_REGIME_STRONG_TREND_THRESHOLD = 0.60
CLUSTER_REGIME_CHOPPY_THRESHOLD = 0.40

LAYER3_WEIGHT_SCORE2 = 0.65
LAYER3_WEIGHT_RESONANCE = 0.20
LAYER3_WEIGHT_LIQUIDITY = 0.15
LAYER3_SCORE_FLOOR = 0.55

MARKET_ACCURACY_FLOOR_PERCENTILE = 20
MARKET_POSITION_CAP_PERCENTILES = {
    'p80': 1.00, 'p50': 0.75, 'p20': 0.50, 'p10': 0.25, 'below_p10': 0.00,
}

CIRCUIT_BREAKER_ACC_PERCENTILE = 10
CIRCUIT_BREAKER_VOL_PERCENTILE = 99
CIRCUIT_BREAKER_CONSECUTIVE_MIN = 5
CIRCUIT_BREAKER_POSITION_MULT = 0.5
CIRCUIT_BREAKER_TIER_FILTER = 1

LIQUIDITY_WEIGHT_VOLUME = 0.40
LIQUIDITY_WEIGHT_SPREAD = 0.30
LIQUIDITY_WEIGHT_DEPTH = 0.30
LIQUIDITY_VOLUME_THRESHOLD = 500
LIQUIDITY_SPREAD_PCT = 0.02
LIQUIDITY_DEPTH_THRESHOLD = 100
LIQUIDITY_WATCH_VOLUME = 100

MAX_SINGLE_PRODUCT_PCT = 0.15
MAX_SINGLE_CLUSTER_PCT = 0.40
DRIFT_LIMIT = 0.20

SIGNAL_SOURCE = 'C'

HARD_FILTER_ENABLED = False
PURE_MODE = False

ENABLE_RESONANCE_WEIGHTING = False
ENABLE_RESONANCE_VETO = False

ENABLE_GLOBAL_PERCENTILE = True
MARKET_FLOOR_MODE = 'percentile'

AB_TEST_ENABLED = False
AB_TEST_EXPERIMENT_ALLOC = 0.20
AB_TEST_CONTROL_ALLOC = 0.80
AB_TEST_MIN_DURATION_DAYS = 14
AB_TEST_DM_P_VALUE = 0.05
AB_TEST_MIN_CALMAR_LIFT = 0.1

DEGRADE_TIMEOUT_LAYER3 = 100
DEGRADE_TIMEOUT_LAYER2 = 100

ROLLBACK_NEW_PARAM_NEGATIVE_CALMAR_DAYS = 7
ROLLBACK_DAILY_DRAWDOWN_PCT = 0.05
ROLLBACK_LIVE_CALMAR_RATIO = 0.50

FRESHNESS_WEIGHTS = {
    'cr': 1.0, 'cf': 1.0, 'wr': 0.3, 'wf': 0.3, 'other': 0.0,
}

SIGNAL_DEDUP_WINDOW_SECONDS = 60

CLUSTER_TIER_THRESHOLDS: Dict = {}

CONSERVATIVE_DEFAULT_P50 = 0.40

CIRCUIT_BREAKER_USE_TIMESTAMP = True

SUGGESTED_LOTS_TIER1_BASE = 2
SUGGESTED_LOTS_TIER2_BASE = 1
SUGGESTED_LOTS_TIER3_BASE = 1
SUGGESTED_LOTS_LOW_CAP_THRESHOLD = 0.3

STRUCTURED_LOG_ENABLED = True

BYPASS_MODE_ENABLED = False
BYPASS_LOG_TABLE = 'bypass_observation_log'
BYPASS_SNAPSHOT_INTERVAL_SEC = 300
BYPASS_METRICS = [
    'filter_rate', 'top3_overlap_ratio', 'filtered_out_avg_rank',
    'filtered_out_delta_avg', 'path_a_calmar', 'path_b_calmar',
]


# ============================================================================
# v2.5-v2.8: 排序层配置接口 SORTER_CONFIG
# 依据文档：三层期权五态排序方案_最终落地方案V2_20260624.md
#   §15.2 (v2.5): rank_window_days, month_weights, enable_rank_normalize
#   §16.2 (v2.6): resonance_enabled, resonance_weight, resonance_dimensions
#   §17.2 (v2.7): resonance_weight=0.5 (等权), direction_conflict, cold start fallback
#   §21.3 (v2.8): output_mode, production_fields (生产环境瘦身)
#   §22   (v2.8): sort_trigger_bars, cooldown_bars (K线倍数时间约束)
# ============================================================================

# K线周期→秒数映射表（支持 tick/M1/M3/M5/M15/M30/H1/H4/D1）
# 高频策略: T1(tick) = 1个tick ≈ 0.5s(期货500ms推送)，用0表示事件驱动无法精确换算
KLINE_PERIOD_SECONDS = {
    'T1': 0,    # tick级: 1根K线=1个tick, 事件驱动无固定周期
    'M1': 60, 'M3': 180, 'M5': 300, 'M15': 900,
    'M30': 1800, 'H1': 3600, 'H4': 14400, 'D1': 86400,
}

# tick级默认参数: 高频模式下排序激发和冷却的实际时间约束
_HFT_DEFAULT_TRIGGER_SEC = 1    # tick级默认激发间隔1秒（约2个tick）
_HFT_DEFAULT_COOLDOWN_SEC = 0   # tick级默认无冷却


def kline_bars_to_seconds(bars: int, kline_style: str = 'M1') -> int:
    """将 K线倍数换算为秒数。

    对于 tick 级 (T1): 1根K线=1个tick，事件驱动无法精确换算，
    使用 _HFT_DEFAULT_TRIGGER_SEC 作为默认值。

    Args:
        bars: K线根数（1=1根K线, 2=2根K线, ...）
        kline_style: K线周期（'T1', 'M1', 'M5', 'M15' 等）

    Returns:
        对应的秒数，tick级返回 bars × _HFT_DEFAULT_TRIGGER_SEC，
        非 tick 级最小5秒（安全下限）
    """
    _style = str(kline_style or 'M1').upper()
    _sec_per_bar = KLINE_PERIOD_SECONDS.get(_style, 60)  # 未知周期默认M1=60s

    if _sec_per_bar == 0:
        # tick级: 1 bar = 1 tick, 用默认间隔换算
        # bars=1 → 1s, bars=2 → 2s, bars=5 → 5s...
        return max(1, int(bars) * _HFT_DEFAULT_TRIGGER_SEC)

    return max(5, int(bars) * _sec_per_bar)


SORTER_CONFIG = {
    # --- v2.5 核心参数 ---
    'rank_window_days': 20,                # Rank 标准化滚动窗口（默认 20 日）
    'month_weights': None,                 # None = 等权；传入向量 = 加权
    'enable_rank_normalize': True,         # 是否启用 Rank 标准化
    'rank_normalize_clip': (-3.0, 3.0),    # 截断范围
    'score_delta_threshold': 0.10,         # rank_confidence 显著 gap 阈值
    'output_candidates_max': 5,            # 多候选列表最大长度
    'rank_confidence_method': 'percentile', # percentile / softmax / gap_ratio / t_statistic

    # --- 排序时间约束（v2.8 K线倍数定义）---
    # 排序激发间隔: 每隔多少根K线触发一次排序（排序→选标的→下单）
    # 1=每根K线触发(默认), 2=每2根K线, 3=每3根K线...
    # 实际秒数 = sort_trigger_bars × K线周期秒数 (由 kline_bars_to_seconds 换算)
    # M1时: 1bar=60s, 2bar=120s; M5时: 1bar=300s, 2bar=600s
    'sort_trigger_bars': 1,                # 默认1根K线（M1=60s, M5=300s）
    # K线周期：与 config_params.kline_style 对齐
    'kline_style': 'M1',                   # 默认1分钟K线
    # 信号冷却期: 同一品种两次信号之间的最小K线根数间隔
    # 0=无冷却, 1=1根K线冷却(M1=60s), 2=2根K线冷却(M1=120s)
    # 排序结果变化不大时避免重复下单
    'cooldown_bars': 0,                    # 默认无冷却（排序驱动即可）

    # --- Tier 分类阈值（v2.8 参数化）---
    # 原 TIER1_WILSON_THRESHOLD 等硬编码常量移入此配置
    # Tier1=高置信(wilson≥阈值,coverage≥80%), Tier2=中置信, Tier3=低置信, Tier4=噪声
    'tier1_wilson_threshold': 0.50,        # Tier1: Wilson下界≥0.50
    'tier2_coverage_threshold': 0.20,      # FIX-N: 与TIER2_COVERAGE_THRESHOLD常量同步(0.40→0.20)
    'tier2_correct_up_threshold': 0.45,    # Tier2: 正确上涨比≥0.45
    'tier3_correct_up_threshold': 0.35,    # Tier3: 正确上涨比≥0.35

    # --- v2.6/v2.7 共振模块参数 ---
    'resonance_enabled': False,            # 默认关闭（安全闸门：Phase 2 完成前必须为 False）
    'resonance_weight': 0.50,              # v2.7: 等权融合（消除未经证明的先验）
    'resonance_weight_note': 'Equal-weight default. V7 walk-forward optimization required before production deployment.',
    'resonance_binary_mode': False,        # False=连续值加权, True=二值化
    'resonance_dimensions': ['D_term_structure', 'D_momentum', 'D_type_balance'],
    'resonance_min_active': 2,             # 最少活跃维度数
    'resonance_min_score': 0.30,           # 共振强度阈值
    'resonance_direction_conflict_zero': True,  # v2.7: 方向冲突时 final_score=0
    'resonance_cold_start_fallback': True, # v2.7: 冷启动权重重新分配

    # --- D_type_balance 参数 ---
    'type_balance_threshold': 0.50,        # Call/Put 占比 > 50% 才触发方向判定

    # --- D_momentum 参数 ---
    'momentum_period': 1,                  # v2.7: 当日动量（period=1）

    # --- v2.8 输出模式参数（§21.3 铁律二：生产环境必须瘦身）---
    'output_mode': 'research',             # 'research' = 全字段 / 'production' = 瘦身字段
    'production_fields': None,             # None = 按 scoring_scheme 自动选择字段集
    # v2.8 §21: 方案开关机制
    # scheme_1(实盘默认): product_score=net_score, 主信号=confidence
    # scheme_2: product_score=D, 主信号=D/Q正交分解
    # scheme_3: product_score=D, 主信号=entropy/concentration/direction_bias
    # all: 全特征输出(research模式)
    'scoring_scheme': 'scheme_1',          # 实盘默认方案一（稳妥）
}


def update_sorter_config_from_v7_recommendation(config_delta: Dict) -> Dict:
    """
    v2.5 §15.2: V7 离线模块通过以下接口回传建议（人工审核后采纳）。
    排序层不主动调用 V7、不嵌入回测逻辑、不读取 V7 数据库。
    单向数据流：V7 → 人工 → 排序层。

    使用示例：
        update_sorter_config_from_v7_recommendation({'rank_window_days': 15})
    人工审核后写入 SORTER_CONFIG，排序层不主动调用 V7。
    """
    if not isinstance(config_delta, dict) or not config_delta:
        return SORTER_CONFIG
    # 仅记录建议，不自动注入（需人工审核）
    logger.info(f"[SORTER_CONFIG] V7 recommendation received (pending manual review): {config_delta}")
    return SORTER_CONFIG

