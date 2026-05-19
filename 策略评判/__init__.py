"""
策略评判系统 — 策略微观行为诊断基础设施 v1.5-production

从顶级量化基金（中高频Stat Arb和期权做市团队）视角构建，
区分"可上线策略"与"回测幻觉"的关键基础设施。

v1.5 升级：
  - 资金规模感知评判：CapitalScale(SMALL/MEDIUM/LARGE)
  - 小资金：盈亏比主导（盈亏比40%+盈利因子25%+恢复效率20%+连亏容忍15%）
  - 中资金：平衡模式（盈亏比+Sharpe+Calmar均衡权重）
  - 大资金：Sharpe主导（Sharpe30%+Calmar30%，机构标准）
  - 回测系统新增交易级追踪：_ClosedTrade + _compute_profit_loss_ratio_metrics
  - 所有run_backtest_*返回值新增盈亏比指标
  - 参数池适配器新增盈亏比指标映射
  - 阻塞项(Blocker)机制：风险/极端/行为一致性 → 一票否决
  - 阈值收紧：行为一致性≥0.70，风险预算≥0.70
  - 消除默认通过：未测试=不通过
  - 细粒度4子项拆解：信号衰减/仓位平滑/Greeks合理/PnL路径
  - 六大策略预设逻辑映射：high_freq/resonance/box/spring/arbitrage/market_making
  - Markdown审计报告
  - 覆盖六大策略完整状态：HFT/Resonance/Box/Spring/Arbitrage/MarketMaking/Ecosystem/Shadow/SafetyMeta
"""
from __future__ import annotations

from .turning_point_microscope import (
    TurningPointMicroscope,
    EnhancedBar,
    ExtremeRegion,
    MAAlignment,
)
from .resonance_turning_point_marker import (
    ResonanceTurningPointMarker,
    TurningPointType,
    TurningPointRecord,
)
from .market_snapshot_collector import (
    MarketSnapshotCollector,
    MarketSnapshot,
    SnapshotTrigger,
    StrategyStateSnapshot,
    HFTSpecificState,
    ResonanceSpecificState,
    BoxSpecificState,
    SpringSpecificState,
    ArbitrageSpecificState,
    MarketMakingSpecificState,
    EcosystemState,
    ShadowAlphaState,
    SafetyMetaState,
    CrossStrategyGreeks,
)
from .strategy_behavior_diagnosis import (
    StrategyBehaviorDiagnosis,
    BehaviorConsistencyScore,
    DiagnosisReport,
    SubItemScore,
)
from .strategy_judgment_engine import (
    StrategyJudgmentEngine,
    JudgmentVerdict,
    JudgmentReport,
    CapitalScale,
    DEFAULT_THRESHOLDS,
    DEFAULT_WEIGHTS,
    BLOCKING_DIMENSIONS_DEFAULT,
    STRATEGY_TYPE_WEIGHT_OVERRIDES,
    STRATEGY_TYPE_THRESHOLD_OVERRIDES,
    ECOSYSTEM_TO_JUDGMENT_TYPE_MAP,
    JUDGMENT_TO_ECOSYSTEM_TYPE_MAP,
    CAPITAL_SCALE_CONFIGS,
)
from .backtest_integration_hooks import (
    BacktestIntegrationHooks,
    HookConfig,
)
from .parameter_pool_adapter import (
    judge_backtest_result,
    judge_sweep_results,
    ecosystem_type_to_judgment_type,
)

__all__ = [
    "TurningPointMicroscope", "EnhancedBar", "ExtremeRegion", "MAAlignment",
    "ResonanceTurningPointMarker", "TurningPointType", "TurningPointRecord",
    "MarketSnapshotCollector", "MarketSnapshot", "SnapshotTrigger",
    "StrategyStateSnapshot", "HFTSpecificState", "ResonanceSpecificState",
    "BoxSpecificState", "SpringSpecificState", "ArbitrageSpecificState",
    "MarketMakingSpecificState", "EcosystemState",
    "ShadowAlphaState", "SafetyMetaState", "CrossStrategyGreeks",
    "StrategyBehaviorDiagnosis", "BehaviorConsistencyScore", "DiagnosisReport", "SubItemScore",
    "StrategyJudgmentEngine", "JudgmentVerdict", "JudgmentReport",
    "CapitalScale", "CAPITAL_SCALE_CONFIGS",
    "DEFAULT_THRESHOLDS", "DEFAULT_WEIGHTS", "BLOCKING_DIMENSIONS_DEFAULT",
    "STRATEGY_TYPE_WEIGHT_OVERRIDES", "STRATEGY_TYPE_THRESHOLD_OVERRIDES",
    "ECOSYSTEM_TO_JUDGMENT_TYPE_MAP", "JUDGMENT_TO_ECOSYSTEM_TYPE_MAP",
    "BacktestIntegrationHooks", "HookConfig",
    "judge_backtest_result", "judge_sweep_results", "ecosystem_type_to_judgment_type",
]

__version__ = "1.5.0"
