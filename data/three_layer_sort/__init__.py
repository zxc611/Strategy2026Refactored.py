# MODULE_ID: M1-056-TL
"""
three_layer_sort — v2.8 期权五态 AlphaEngine 与多粒度视图层排序系统

依据文档：三层期权五态排序方案_最终落地方案V2_20260624.md

v2.5 架构（§15 Final-Approved）：
  AlphaEngine（核心打分引擎，原信号源A IntraProductSorter）
    ├──→ ClusterView（簇内排名视图层，原信号源B InterProductClusterSorter）
    └──→ GlobalView（全局排名视图层，原信号源C GlobalSorter）

v2.6/v2.7 扩展（§16-§17）：
  ResonanceEngine（可插拔共振模块，默认关闭）
  - D_term_structure: 近月/远月 net_score 同向性
  - D_momentum: 当日动量（period=1）
  - D_type_balance: Call/Put 类型共振
  - 方向一致性检查 + 冷启动回退

v2.8 扩展（§21）：
  - scoring_scheme 参数化: scheme_1(实盘默认)/scheme_2/scheme_3/all
  - 方案一(实盘默认): product_score 基于 net_score + confidence 修正版
  - 方案二: product_score 基于 D/Q 正交分解
  - 方案三: product_score 基于 D, 辅助 entropy/concentration/direction_bias
  - D/Q 四象限正交分解
  - confidence = participation × clarity
  - entropy/concentration/direction_bias 信息熵特征
  - output_mode 参数化（research/production）

核心原则：排序只做排序，风控归模块层。
AlphaEngine 为唯一打分引擎，ClusterView/GlobalView 为视图层，非独立信号源。

向后兼容：
  旧类名 IntraProductSorter / InterProductClusterSorter / GlobalSorter 保留为别名
"""
from ali2026v3_trading.data.three_layer_sort.signal_source_a import (
    # v2.5 新类名
    AlphaEngine,
    ClusterView,
    GlobalView,
    RankNormalizer,
    # v2.5 辅助函数
    compute_rank_confidence_default,
    compute_score_delta_to_next,
    # v2.8 核心函数（§21 四象限正交分解 + 置信度 + 信息熵特征）
    compute_orthogonal_decomposition,
    compute_confidence_v28,
    compute_entropy_features,
    # v2.0 旧类名（向后兼容别名）
    IntraProductSorter,
    AsymmetricDecay,
    GreeksHardFilter,
    InterProductClusterSorter,
    GlobalSorter,
)

from ali2026v3_trading.data.three_layer_sort.resonance_engine import (
    ResonanceEngine,
    compute_term_structure_direction,
    compute_type_resonance_direction,
    compute_momentum_direction,
    compute_resonance_with_fallback,
    compute_final_score,
)

__all__ = [
    # v2.5 新类名
    'AlphaEngine',
    'ClusterView',
    'GlobalView',
    'RankNormalizer',
    # v2.5 辅助函数
    'compute_rank_confidence_default',
    'compute_score_delta_to_next',
    # v2.8 核心函数
    'compute_orthogonal_decomposition',
    'compute_confidence_v28',
    'compute_entropy_features',
    # v2.6/v2.7 共振模块
    'ResonanceEngine',
    'compute_term_structure_direction',
    'compute_type_resonance_direction',
    'compute_momentum_direction',
    'compute_resonance_with_fallback',
    'compute_final_score',
    # v2.0 旧类名（向后兼容别名）
    'IntraProductSorter',
    'AsymmetricDecay',
    'GreeksHardFilter',
    'InterProductClusterSorter',
    'GlobalSorter',
]
