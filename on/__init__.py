from .cascade_judge import CascadeJudge, CascadeReport, GateReport, GateResult, BacktestMetrics, adapt_backtest_result

# NEW-P1-06修复: 引入模块加载状态管理
# P1-22: 删除fallback定义，直接从module_load_status导入
from ali2026v3_trading.infra.metrics_registry import mark_module_loaded, mark_module_failed

# CORE-DEPENDENCY: parameter_drift_detector是评判系统的核心组件
try:
    from .parameter_drift_detector import ParameterDriftDetector
    mark_module_loaded('evaluation_parameter_drift_detector')
except ImportError as _e:
    ParameterDriftDetector = None
    mark_module_failed('evaluation_parameter_drift_detector', _e)

# CORE-DEPENDENCY: chicory_eviction是评判系统的核心组件
try:
    from .chicory_eviction import ChicoryEvictionPolicy
    mark_module_loaded('evaluation_chicory_eviction')
except ImportError as _e:
    ChicoryEvictionPolicy = None
    mark_module_failed('evaluation_chicory_eviction', _e)

# CORE-DEPENDENCY: marquee_threshold是评判系统的核心组件
try:
    from .marquee_threshold import MarqueeThreshold
    mark_module_loaded('evaluation_marquee_threshold')
except ImportError as _e:
    MarqueeThreshold = None
    mark_module_failed('evaluation_marquee_threshold', _e)

# CORE-DEPENDENCY: violation_tracker是评判系统的核心组件
try:
    from .violation_tracker import StrategyViolationTracker
    mark_module_loaded('evaluation_violation_tracker')
except ImportError as _e:
    StrategyViolationTracker = None
    mark_module_failed('evaluation_violation_tracker', _e)

# CORE-DEPENDENCY: state_density_decay是评判系统的核心组件
try:
    from .state_density_decay import StateEDensityDecayTracker
    mark_module_loaded('evaluation_state_density_decay')
except ImportError as _e:
    StateEDensityDecayTracker = None
    mark_module_failed('evaluation_state_density_decay', _e)

# CORE-DEPENDENCY: activity_weighted_scorer是评判系统的核心组件
try:
    from .activity_weighted_scorer import ActivityWeightedScorer
    mark_module_loaded('evaluation_activity_weighted_scorer')
except ImportError as _e:
    ActivityWeightedScorer = None
    mark_module_failed('evaluation_activity_weighted_scorer', _e)
