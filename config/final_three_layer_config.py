# MODULE_ID: M1-016-TL
"""
final_three_layer_config.py — 三层期权五态排序最终落地配置

依据文档：docs/audit/三层期权五态排序方案_最终落地方案_20260624.md 第九章

三层架构：
  Layer 1: 单品种月份层（硬过滤 + 字典序/加权混合 + 非对称衰减 + Greeks修正）
  Layer 2: 联动品种综合层（GMM软聚类 + 共振否决/加权 + 簇状态评估）
  Layer 3: 全品种综合排序层（历史分位数门槛 + 动态风险预算 + 熔断器）

每层独立发出开仓信号，形成三个决策源，由 three_decision_source_router 统一路由。
"""
from __future__ import annotations

from typing import Dict

# =========================================================================
# Layer 1: 单品种月份层
# =========================================================================

# 月份权重（当月+下月+三季月，最多5个月份）
MONTH_WEIGHTS_5 = (0.35, 0.25, 0.20, 0.12, 0.08)

# 非对称状态时效衰减半衰期（秒），按行为簇配置
# P2-08: 五态键统一为小写无后缀 (cr/cf/wr/wf/other)
ASYMMETRIC_DECAY = {
    'financial':     {'cr': 600, 'cf': 600, 'wr': 120, 'wf': 120, 'other': 60},
    'black_metal':   {'cr': 300, 'cf': 300, 'wr': 60,  'wf': 60,  'other': 60},
    'agricultural':  {'cr': 180, 'cf': 180, 'wr': 60,  'wf': 60,  'other': 60},
    'energy_chem':   {'cr': 240, 'cf': 240, 'wr': 60,  'wf': 60,  'other': 60},
    'default':       {'cr': 300, 'cf': 300, 'wr': 60,  'wf': 60,  'other': 60},
}

# Greeks 硬过滤阈值
DELTA_MIN = 0.15
DELTA_MAX = 0.85
GAMMA_MAX_FRONT_MONTH = 0.08
THETA_MAX_FRONT_MONTH_PCT = 0.015  # 日损耗1.5%

# Greeks 软修正
GAMMA_BOOST_FACTOR = 1.2
GAMMA_BOOST_MIN = 0.05
GAMMA_BOOST_DELTA_RANGE = 0.15
VEGA_PENALTY_THRESHOLD = 0.10
VEGA_PENALTY_IV_CHANGE = 0.05
VEGA_PENALTY_FACTOR = 0.8
THETA_PENALTY_MAX = 0.15
THETA_PENALTY_GAMMA_MIN = 0.03

# Tier 分级阈值（核心9参数之一）
TIER1_WILSON_THRESHOLD = 0.50      # 范围 (0.50, 0.70)
TIER2_COVERAGE_THRESHOLD = 0.40    # 范围 (0.40, 0.70)
TIER2_CORRECT_UP_THRESHOLD = 0.45  # 范围 (0.45, 0.60)
TIER3_CORRECT_UP_THRESHOLD = 0.35  # 范围 (0.35, 0.50)

# Layer 1 排序模式
LAYER1_SORT_MODE = 'lexicographic'  # 'lexicographic' | 'weighted'

# Layer 1 加权混合权重（模式B）
LAYER1_WEIGHT_WILSON = 0.70
LAYER1_WEIGHT_FRESHNESS = 0.20
LAYER1_WEIGHT_LIQUIDITY = 0.10

# 过滤率监控
FILTER_RATE_ALERT_THRESHOLD = 0.30

# =========================================================================
# Layer 2: 联动品种综合层
# =========================================================================

CLUSTERING_METHOD = 'GMM'
CLUSTERING_FREQUENCY = 'quarterly'
CLUSTERING_LOOKBACK_DAYS = 60
CLUSTER_BIC_RANGE = (4, 6)
CLUSTER_CONFIDENCE_THRESHOLD = 0.60

# 共振否决机制（短期默认）
RESONANCE_VETO_THRESHOLD = 0.80
RESONANCE_MODE = 'veto'  # 'veto' | 'weighted'

# 共振加权混合机制（中期启用）
LAYER2_WEIGHT_SCORE1 = 0.70
LAYER2_WEIGHT_RESONANCE = 0.20
LAYER2_WEIGHT_REGIME = 0.10
LAYER2_DISPERSION_THRESHOLD = 0.15
LAYER2_DISPERSION_PENALTY = -0.05
LAYER2_CORRELATION_THRESHOLD = 0.80
LAYER2_CORRELATION_PENALTY = -0.05
LAYER2_SCORE_FLOOR = 0.60

# 簇状态评估阈值
CLUSTER_REGIME_STRONG_TREND_THRESHOLD = 0.60
CLUSTER_REGIME_CHOPPY_THRESHOLD = 0.40

# =========================================================================
# Layer 3: 全品种综合排序层
# =========================================================================

LAYER3_WEIGHT_SCORE2 = 0.65
LAYER3_WEIGHT_RESONANCE = 0.20
LAYER3_WEIGHT_LIQUIDITY = 0.15
LAYER3_SCORE_FLOOR = 0.55

# 历史分位数自适应门槛（全市场仓位联动表）
MARKET_ACCURACY_FLOOR_PERCENTILE = 20
MARKET_POSITION_CAP_PERCENTILES = {
    'p80': 1.00,
    'p50': 0.75,
    'p20': 0.50,
    'p10': 0.25,
    'below_p10': 0.00,
}

# 熔断器配置
CIRCUIT_BREAKER_ACC_PERCENTILE = 10
CIRCUIT_BREAKER_VOL_PERCENTILE = 99
CIRCUIT_BREAKER_CONSECUTIVE_MIN = 5
CIRCUIT_BREAKER_POSITION_MULT = 0.5
CIRCUIT_BREAKER_TIER_FILTER = 1

# 综合流动性评分
LIQUIDITY_WEIGHT_VOLUME = 0.40
LIQUIDITY_WEIGHT_SPREAD = 0.30
LIQUIDITY_WEIGHT_DEPTH = 0.30
LIQUIDITY_VOLUME_THRESHOLD = 500
LIQUIDITY_SPREAD_PCT = 0.02
LIQUIDITY_DEPTH_THRESHOLD = 100
LIQUIDITY_WATCH_VOLUME = 100

# 动态风险预算
MAX_SINGLE_PRODUCT_PCT = 0.15
MAX_SINGLE_CLUSTER_PCT = 0.40
DRIFT_LIMIT = 0.20

# =========================================================================
# 信号源选择配置（v2.0重构：并列信号源A/B/C）
# =========================================================================

# 信号源选择：'A' | 'B' | 'C' | 'auto'
# A=单品种月份排序(IntraProductSorter), B=联动品种簇排序(InterProductClusterSorter),
# C=全域品种排序(GlobalSorter), auto=自动选择
SIGNAL_SOURCE = 'C'

# 信号源A参数
HARD_FILTER_ENABLED = False
PURE_MODE = False

# 信号源B参数
ENABLE_RESONANCE_WEIGHTING = False
ENABLE_RESONANCE_VETO = False

# 信号源C参数
ENABLE_GLOBAL_PERCENTILE = True
MARKET_FLOOR_MODE = 'percentile'  # 'percentile' | 'fixed'

# A/B 测试配置
AB_TEST_ENABLED = False
AB_TEST_EXPERIMENT_ALLOC = 0.20  # 实验组仓位分配
AB_TEST_CONTROL_ALLOC = 0.80     # 对照组仓位分配
AB_TEST_MIN_DURATION_DAYS = 14
AB_TEST_DM_P_VALUE = 0.05
AB_TEST_MIN_CALMAR_LIFT = 0.1

# =========================================================================
# 降级与回滚
# =========================================================================

# 降级延迟上限（毫秒）
DEGRADE_TIMEOUT_LAYER3 = 100
DEGRADE_TIMEOUT_LAYER2 = 100

# 自动回滚触发器
ROLLBACK_NEW_PARAM_NEGATIVE_CALMAR_DAYS = 7
ROLLBACK_DAILY_DRAWDOWN_PCT = 0.05
ROLLBACK_LIVE_CALMAR_RATIO = 0.50

# =========================================================================
# P0/P1/P2 修复新增配置
# =========================================================================

# Freshness权重配置（与AsymmetricDecay共享半衰期）
# P2-08: 五态键统一为小写无后缀 (cr/cf/wr/wf/other)
FRESHNESS_WEIGHTS = {
    'cr': 1.0,
    'cf': 1.0,
    'wr': 0.3,
    'wf': 0.3,
    'other': 0.0,
}

# 信号去重窗口（秒）
SIGNAL_DEDUP_WINDOW_SECONDS = 60

# 簇差异化Tier阈值（按cluster_id索引，0=全局默认）
CLUSTER_TIER_THRESHOLDS: Dict = {}

# 无历史数据时保守模式默认分位数
CONSERVATIVE_DEFAULT_P50 = 0.40

# 熔断器时间戳模式（替代调用次数）
CIRCUIT_BREAKER_USE_TIMESTAMP = True

# 手数计算参数
SUGGESTED_LOTS_TIER1_BASE = 2
SUGGESTED_LOTS_TIER2_BASE = 1
SUGGESTED_LOTS_TIER3_BASE = 1
SUGGESTED_LOTS_LOW_CAP_THRESHOLD = 0.3

# 结构化日志开关
STRUCTURED_LOG_ENABLED = True

# =========================================================================
# 旁路观测通道配置（仅研究/回测环境启用）
# =========================================================================

BYPASS_MODE_ENABLED = False  # 生产环境必须为 False
BYPASS_LOG_TABLE = 'bypass_observation_log'  # DuckDB 表名
BYPASS_SNAPSHOT_INTERVAL_SEC = 300  # 每5分钟记录一次快照
BYPASS_METRICS = [
    'filter_rate',           # 硬过滤过滤率
    'top3_overlap_ratio',    # 过滤前后Top3重叠度
    'filtered_out_avg_rank', # 被过滤期权在路径B中的平均排名
    'filtered_out_delta_avg',# 被过滤期权的delta绝对值均值
    'path_a_calmar',         # 路径A（过滤）Calmar
    'path_b_calmar',         # 路径B（不过滤）Calmar
]
