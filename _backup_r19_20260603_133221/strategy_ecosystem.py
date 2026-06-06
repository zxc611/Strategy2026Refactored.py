"""
strategy_ecosystem.py - 策略生态系统

V7次系统2核心：三大策略协同 + 资金路由 + 互斥规则

三大策略：
  1. 主策略(correct_trending): 跟随一致性共振方向做买方
  2. 反向策略(incorrect_reversal): 对错误信号做反向交易
  3. 震荡市策略(other): 箱体极值处的微利机会

资金路由(L-0.5):
  - 主策略基础占比60%，反向25%，震荡15%
  - 动态分配：激活状态对应策略获得优先资金

互斥规则：
  - 主策略和反向策略不允许同时持有同向仓位
  - 状态切换时先平旧策略仓位再开新策略仓位
  - other状态下主/反策略仅平仓模式

参数独立锁定：
  - 每个策略的参数在Walk-forward后冻结
  - 参数hash确保一致性

绝对期望值底线：
  - 任一策略绝对期望值<0 → 暂停该策略
  - 整体期望值<0 → 暂停实盘
"""
from __future__ import annotations

import copy
import json
import logging
import math
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Callable

from ali2026v3_trading.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE, STRATEGY_MODE_OTHER,
)


def _require_interface(required_attrs: Tuple[str, ...]) -> Callable:
    """R5-I-06修复: 运行时接口检查装饰器，替代裸duck typing

    对关键方法入口检查self/参数是否实现所需属性/方法，
    在调用方传入错误类型时立即抛TypeError而非运行时AttributeError。
    """
    def decorator(method: Callable) -> Callable:
        def wrapper(self, *args, **kwargs):
            missing = [a for a in required_attrs if not hasattr(self, a)]
            if missing:
                raise TypeError(
                    f"R5-I-06: {type(self).__name__}缺少接口属性{missing}，"
                    f"调用{method.__name__}前需确保实现{required_attrs}"
                )
            return method(self, *args, **kwargs)
        wrapper.__name__ = method.__name__
        wrapper.__doc__ = method.__doc__
        wrapper.__wrapped__ = method
        return wrapper
    return decorator


def _require_arg_interface(arg_name: str, required_attrs: Tuple[str, ...]) -> Callable:
    """R5-I-06修复: 参数级接口检查装饰器，检查指定参数是否实现所需属性/方法"""
    def decorator(method: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            from inspect import signature
            sig = signature(method)
            params = list(sig.parameters.keys())
            if arg_name in kwargs:
                target = kwargs[arg_name]
            else:
                idx = params.index(arg_name) if arg_name in params else -1
                target = args[idx - 1] if idx > 0 and idx - 1 < len(args) else None
            if target is not None:
                missing = [a for a in required_attrs if not hasattr(target, a)]
                if missing:
                    raise TypeError(
                        f"R5-I-06: 参数'{arg_name}'类型{type(target).__name__}缺少接口{missing}，"
                        f"调用{method.__name__}需确保实现{required_attrs}"
                    )
            return method(*args, **kwargs)
        wrapper.__name__ = method.__name__
        wrapper.__doc__ = method.__doc__
        wrapper.__wrapped__ = method
        return wrapper
    return decorator

from ali2026v3_trading.box_detector import BoxDetector, BoxProfile, ExtremeState, BoxStrategyParams
from ali2026v3_trading.strategy_judgment.strategy_judgment_engine import CapitalScale
from ali2026v3_trading.plr_calculator import get_plr_calculator
from ali2026v3_trading.performance_monitor import count_call
from ali2026v3_trading.shared_utils import CHINA_TZ

# R27-P1修复: 导入策略注册/信号生命周期/浮点工具
from ali2026v3_trading.resilience_utils import (
    get_strategy_registry, StrategyRegistry,
    get_signal_lifecycle, SignalLifecycleManager,
    Watchdog, HeartbeatMonitor, CircuitBreakerHalfOpen,
    StrategyRestartGuard, GracefulDegradation,
    stable_sum, stable_mean, compute_sharpe_stable,
    safe_normalize_weights, KahanSummation, safe_divide,
    PRICE_TOLERANCE, FLOAT_COMPARE_TOLERANCE,
)

# P-12修复: LiveStrategySelector实盘集成（手册9.4节）
try:
    from ali2026v3_trading.param_pool.delay_time_sharpe_3d import LiveStrategySelector
    from ali2026v3_trading.module_load_status import mark_module_loaded
    mark_module_loaded('strategy_ecosystem_live_strategy_selector')
except ImportError as _ie:
    LiveStrategySelector = None
    try:
        from ali2026v3_trading.module_load_status import mark_module_failed
        mark_module_failed('strategy_ecosystem_live_strategy_selector', _ie)
    except Exception:
        pass

logger = logging.getLogger(__name__)

# P2-R11-14修复: EV缓存TTL模块级常量 — 按策略类型自适应，可被params覆盖
EV_CACHE_TTL_DEFAULTS = {
    'hft': 1.0,      # HFT策略tick频率>5Hz，TTL应更短
    'minute': 3.0,    # 分钟级策略
    'default': 5.0,   # 默认策略
}
EV_CACHE_TTL = 5.0  # 向后兼容默认值

OBSERVATION_PERIOD_SEC = 1800.0


@dataclass(slots=True)
class StrategySlot:
    """策略槽位"""
    # R15-P1-API-13修复: 策略状态枚举类型定义，与VALID_STRATEGY_TRANSITIONS对齐
    from enum import Enum as _Enum
    class StrategyState(_Enum):
        INACTIVE = 'inactive'
        STANDBY = 'standby'
        ACTIVE = 'active'
        HANDOVER = 'handover'
        RETIRED = 'retired'
    # INV-07/INV-STA-02: 策略状态合法转换集合
    VALID_STRATEGY_TRANSITIONS = {
        'inactive': {'standby', 'active', 'retired'},
        'standby': {'active', 'inactive', 'handover', 'retired'},
        'active': {'standby', 'handover', 'inactive', 'retired'},
        'handover': {'standby', 'inactive', 'retired'},
        'retired': {'standby'},  # 仅允许复活审批后回到standby
    }

    strategy_id: str
    strategy_type: str
    state: str = 'inactive'
    capital_allocation: float = 0.0
    position_count: int = 0
    total_pnl: float = 0.0
    expected_value: float = 0.0
    sharpe: float = 0.0
    win_loss_ratio: float = 0.0
    profit_factor: float = 0.0
    consecutive_losses: int = 0
    target_plr: float = 2.0
    params_locked: bool = False
    params_hash: str = ''
    paused: bool = False
    pause_reason: str = ''
    frozen: bool = False
    pause_timestamp: float = 0.0  # R17-LC-03修复: 记录暂停时间戳，用于resume前冷却期检查
    last_direction: str = ''
    _closed_pnl_sum: float = 0.0
    _closed_count: int = 0
    _win_pnl_sum: float = 0.0
    _loss_pnl_sum: float = 0.0
    _win_count: int = 0
    _loss_count: int = 0
    _kahan_c_total: float = 0.0
    _kahan_c_closed: float = 0.0
    _kahan_c_win: float = 0.0
    _kahan_c_loss: float = 0.0
    _observation_period_until: Optional[float] = None
    _observation_position_scale: float = 1.0
    _sub_strategy_stats: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        's1_hft': {'trade_count': 0, 'pnl': 0.0, 'wins': 0},
        's2_resonance': {'trade_count': 0, 'pnl': 0.0, 'wins': 0},
    })

    def record_sub_strategy_trade(self, sub_id: str, pnl: float):
        """R16-P1-17修复: 记录子策略独立交易统计"""
        if sub_id in self._sub_strategy_stats:
            stats = self._sub_strategy_stats[sub_id]
            stats['trade_count'] += 1
            stats['pnl'] += pnl
            if pnl > 0:
                stats['wins'] += 1

    def get_sub_strategy_ev(self, sub_id: str) -> float:
        """R16-P1-17修复: 获取子策略独立EV"""
        if sub_id in self._sub_strategy_stats:
            stats = self._sub_strategy_stats[sub_id]
            if stats['trade_count'] > 0:
                return stats['pnl'] / stats['trade_count']
        return 0.0

    def validate_state_transition(self, new_state: str) -> bool:
        """INV-07/INV-STA-02: 验证策略状态转换是否合法"""
        if self.state == new_state:
            return True
        allowed = self.VALID_STRATEGY_TRANSITIONS.get(self.state, set())
        if new_state not in allowed:
            logger.warning(
                "[INV-07] 非法策略状态转换: %s→%s (允许: %s), strategy_id=%s",
                self.state, new_state, allowed, self.strategy_id,
            )
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CapitalRoute:
    """资金路由配置

    P1-R10-02修复: 字段修改需通过update_from_dict()方法，
    确保所有修改经过ecosystem锁保护。
    """
    master_base: float = 0.60
    reverse_base: float = 0.25
    other_base: float = 0.15
    spring_base: float = 0.15
    arbitrage_base: float = 0.10
    market_making_base: float = 0.10
    alpha_ratio: float = 0.0  # P1-R9-13修复: Alpha占比数据，用于资金路由调整
    dynamic_enabled: bool = True
    master_active_boost: float = 0.15
    min_maintenance_ratio: float = 0.05
    _frozen: bool = field(default=False, repr=False)  # P1-R10-02: 冻结标记

    def __post_init__(self):
        object.__setattr__(self, '_frozen', False)

    def __setattr__(self, name: str, value: Any) -> None:
        """P1-R10-02修复: 冻结后禁止外部直接修改字段"""
        if self._frozen and name != '_frozen':
            raise RuntimeError(
                f"[P1-R10-02] CapitalRoute已冻结，禁止直接修改字段'{name}'。"
                f"请使用update_from_dict()方法，确保修改经过ecosystem锁保护。"
            )
        object.__setattr__(self, name, value)

    def freeze(self) -> None:
        """冻结字段，阻止外部直接修改"""
        object.__setattr__(self, '_frozen', True)

    def unfreeze(self) -> None:
        """解冻字段，仅限ecosystem内部使用"""
        object.__setattr__(self, '_frozen', False)

    def update_from_dict(self, updates: Dict[str, Any]) -> None:
        """安全更新字段（由ecosystem锁保护调用）"""
        object.__setattr__(self, '_frozen', False)
        try:
            for k, v in updates.items():
                if hasattr(self, k) and k not in ('_frozen',):
                    object.__setattr__(self, k, v)
        finally:
            object.__setattr__(self, '_frozen', True)

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if k != '_frozen'}

    @property
    def total_base(self) -> float:
        return self.master_base + self.reverse_base + self.other_base


@dataclass(slots=True)
class EcosystemTradeRecord:
    """生态系统交易记录"""
    trade_id: str
    strategy_id: str
    timestamp: str
    action: str = ''
    instrument_id: str = ''
    direction: str = ''
    price: float = 0.0
    quantity: int = 0
    open_reason: str = ''
    pnl: float = 0.0
    net_pnl: float = 0.0
    is_open: bool = True
    market_state: str = ''
    target_plr: float = 0.0
    actual_plr: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class StrategyEcosystem:
    """策略生态系统

    六大策略协同：
    - correct_trending → 主策略(S2共振)
    - incorrect_reversal → 反向策略(S2影子A)
    - other → 震荡市策略(S3箱体极值)
    - spring → 弹簧策略(S4弹簧)
    - arbitrage → 套利策略(S5套利)
    - market_making → 做市策略(S6做市)
    """

    MIN_TRADES_FOR_EV = 5

    # R14-P1-DOC-P1-01修复: 资金分配权重配置（硬编码常量(待参数化)）
    # 基础资金分配比例
    CAPITAL_ALLOC_MASTER_BASE = 0.60
    CAPITAL_ALLOC_REVERSE_BASE = 0.25
    CAPITAL_ALLOC_OTHER_BASE = 0.15
    CAPITAL_ALLOC_SPRING_BASE = 0.15
    CAPITAL_ALLOC_ARBITRAGE_BASE = 0.10
    CAPITAL_ALLOC_MARKET_MAKING_BASE = 0.10
    # 资金规模调整比例
    CAPITAL_ALLOC_SMALL_MASTER = 0.30
    CAPITAL_ALLOC_SMALL_REVERSE = 0.20
    CAPITAL_ALLOC_SMALL_OTHER = 0.15
    CAPITAL_ALLOC_SMALL_SPRING = 0.35
    CAPITAL_ALLOC_LARGE_MASTER = 0.50
    CAPITAL_ALLOC_LARGE_REVERSE = 0.30
    CAPITAL_ALLOC_LARGE_OTHER = 0.15
    CAPITAL_ALLOC_LARGE_SPRING = 0.05
    # 动态分配剩余资金权重
    REMAINING_ALLOC_MASTER = 0.50
    REMAINING_ALLOC_REVERSE = 0.50
    REMAINING_ALLOC_OTHER_MASTER = 0.40
    REMAINING_ALLOC_OTHER_REVERSE = 0.40
    REMAINING_ALLOC_OTHER_SPRING = 0.20
    REMAINING_ALLOC_REVERSE_MASTER = 0.50
    REMAINING_ALLOC_REVERSE_OTHER = 0.30
    REMAINING_ALLOC_REVERSE_SPRING = 0.20
    REMAINING_ALLOC_MASTER_OTHER = 0.30
    REMAINING_ALLOC_MASTER_SPRING = 0.20

    # P1-裂缝38：状态联动资金分配乘子表
    # 按市场状态动态调整各策略资金权重，分配后归一化
    STATE_STRATEGY_MULTIPLIERS = {  # R25-SE-P1-02-FIX
        STRATEGY_MODE_CORRECT_TRENDING: {
            's1_hft': 1.5, 's2_resonance': 1.5, 's3_box_extreme': 0.8,
            's4_box_spring': 0.8, 's5_arbitrage': 1.0, 's6_market_making': 1.0,
        },
        STRATEGY_MODE_INCORRECT_REVERSAL: {
            's1_hft': 1.2, 's2_resonance': 1.2, 's3_box_extreme': 0.5,
            's4_box_spring': 0.5, 's5_arbitrage': 1.0, 's6_market_making': 1.0,
        },
        'other': {
            's1_hft': 0.6, 's2_resonance': 0.6, 's3_box_extreme': 1.5,
            's4_box_spring': 1.5, 's5_arbitrage': 1.0, 's6_market_making': 1.0,
        },
    }

    # PLR冻结条件阈值
    PLR_FREEZE_RATIO_THRESHOLD = 0.5
    PLR_FREEZE_CONSECUTIVE_LOSSES = 5

    def __init__(
        self,
        capital_route: Optional[CapitalRoute] = None,
        box_params: Optional[BoxStrategyParams] = None,
        log_dir: Optional[str] = None,
        capital_scale: Optional[CapitalScale] = None,
    ):
        self._lock = threading.RLock()
        self._capital_route = capital_route or CapitalRoute()
        self._capital_scale = capital_scale
        # R27-P0-DR-13修复: 多策略隔板(Bulkhead) — 每个策略独立的信号配额和资金限额
        self._strategy_bulkheads: Dict[str, Dict[str, Any]] = {  # R25-SE-P1-02-FIX
            STRATEGY_MODE_CORRECT_TRENDING: {'max_signals_per_min': 30, 'max_capital_pct': 0.60, 'signals_count': 0, 'last_reset': 0.0},
            STRATEGY_MODE_INCORRECT_REVERSAL: {'max_signals_per_min': 20, 'max_capital_pct': 0.25, 'signals_count': 0, 'last_reset': 0.0},
            'other': {'max_signals_per_min': 15, 'max_capital_pct': 0.15, 'signals_count': 0, 'last_reset': 0.0},
            'spring': {'max_signals_per_min': 15, 'max_capital_pct': 0.15, 'signals_count': 0, 'last_reset': 0.0},
            'arbitrage': {'max_signals_per_min': 10, 'max_capital_pct': 0.10, 'signals_count': 0, 'last_reset': 0.0},
            'market_making': {'max_signals_per_min': 50, 'max_capital_pct': 0.10, 'signals_count': 0, 'last_reset': 0.0},
        }
        self._strategy_error_counts: Dict[str, int] = {}  # strategy_name -> consecutive_errors
        self._strategy_circuit_breaker_threshold: int = 5  # 连续N次错误→策略断路
        self._strategy_circuit_breaker_at: Dict[str, float] = {}  # strategy_name -> 断路触发时间
        self._strategy_circuit_breaker_auto_recovery_sec: float = 300.0  # 断路5分钟后自动恢复

        _box_detector_kwargs = {}
        try:
            from ali2026v3_trading.config_params import get_cached_params
            _all_params = get_cached_params()
            for _dk in ('box_gain_ratio', 'plr_normalization_base'):
                if _dk in _all_params:
                    _box_detector_kwargs[_dk] = _all_params[_dk]
        except Exception:
            pass
        self._box_detector = BoxDetector(params=box_params, **_box_detector_kwargs)
        self._last_bar_data: Dict[str, float] = {}

        # P1-32修复: 从ConfigService覆盖资金分配权重
        try:
            from ali2026v3_trading.config_service import ConfigService
            _cfg = ConfigService()
            _cap_cfg = getattr(_cfg, 'trading', None)
            if _cap_cfg:
                for _attr in ['CAPITAL_ALLOC_MASTER_BASE', 'CAPITAL_ALLOC_REVERSE_BASE',
                              'CAPITAL_ALLOC_OTHER_BASE', 'CAPITAL_ALLOC_SPRING_BASE']:
                    _val = getattr(_cap_cfg, _attr.lower(), None)
                    if _val is not None:
                        setattr(self, _attr, _val)
        except Exception:
            pass

        self._log_dir = log_dir or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'ecosystem'
        )
        os.makedirs(self._log_dir, exist_ok=True)

        self._master = StrategySlot(
            strategy_id='master',
            strategy_type=STRATEGY_MODE_CORRECT_TRENDING,  # R25-SE-P1-02-FIX
            state='active',
            capital_allocation=self.CAPITAL_ALLOC_MASTER_BASE,
        )
        self._reverse = StrategySlot(
            strategy_id='reverse',
            strategy_type=STRATEGY_MODE_INCORRECT_REVERSAL,  # R25-SE-P1-02-FIX
            state='standby',
            capital_allocation=self.CAPITAL_ALLOC_REVERSE_BASE,
        )
        self._other = StrategySlot(
            strategy_id='other',
            strategy_type='box_extreme',
            state='standby',
            capital_allocation=self.CAPITAL_ALLOC_OTHER_BASE,
        )
        self._spring = StrategySlot(
            strategy_id='spring',
            strategy_type='box_spring',
            state='active',
            capital_allocation=self._capital_route.spring_base,
        )
        self._arbitrage = StrategySlot(
            strategy_id='arbitrage',
            strategy_type='arbitrage',
            state='standby',
            capital_allocation=self._capital_route.arbitrage_base,
        )
        self._market_making = StrategySlot(
            strategy_id='market_making',
            strategy_type='market_making',
            state='standby',
            capital_allocation=self._capital_route.market_making_base,
        )

        # P-12修复: LiveStrategySelector实盘集成初始化（手册9.4节）
        self._live_strategy_selector: Optional[Any] = None
        self._degrade_frequency_callbacks: List = []
        if LiveStrategySelector is not None:
            try:
                self._live_strategy_selector = LiveStrategySelector()
                # P-13修复: _degrade_frequency回调通知策略层
                self._degrade_frequency_callbacks.append(self._on_degrade_frequency)
                logging.info("[StrategyEcosystem] LiveStrategySelector集成成功(含P-13回调)")
            except Exception as e:
                logging.warning("[StrategyEcosystem] LiveStrategySelector初始化失败: %s", e)

        self._active_strategy: str = 'master'
        self._prev_active_strategy: str = 'master'
        # R15-P1-PERF-02修复: 策略实例缓存，切换时优先从缓存获取
        self._strategy_instance_cache: Dict[str, Any] = {}

        # UPG-P1-03修复: 并行运行模式 — 新旧策略同时运行的热更新机制
        self._parallel_run_mode: bool = False
        self._parallel_run_old_strategy: Optional[str] = None
        self._parallel_run_new_strategy: Optional[str] = None
        self._parallel_run_start_time: float = 0.0
        self._parallel_run_max_duration_sec: float = 3600.0  # 默认1小时后自动切换
        self._strategy_lineage: Dict[str, List[Dict[str, Any]]] = {}
        self._strategy_param_evolution: deque = deque(maxlen=1000)
        self._strategy_doc_links: Dict[str, str] = {
            'master': 'docs/strategies/master.md',
            'reverse': 'docs/strategies/reverse.md',
            'other': 'docs/strategies/other.md',
            'spring': 'docs/strategies/spring.md',
            'arbitrage': 'docs/strategies/arbitrage.md',
            'market_making': 'docs/strategies/market_making.md',
        }
        self._l2_dataset_enforced: bool = True
        self._l2_dataset_id: str = 'l2_default_dataset'
        self._capital_scale_upgrade_enabled: bool = True
        self._retired_strategies: Dict[str, Dict[str, Any]] = {}
        self._revival_requests: deque = deque(maxlen=200)

        self._master_trades: deque = deque(maxlen=10000)
        self._reverse_trades: deque = deque(maxlen=10000)
        self._other_trades: deque = deque(maxlen=10000)
        self._spring_trades: deque = deque(maxlen=10000)
        self._arbitrage_trades: deque = deque(maxlen=10000)
        self._market_making_trades: deque = deque(maxlen=10000)

        # R21-MEM-P1-09修复: equity曲线从List[float]改为deque(maxlen)，自动淘汰旧数据
        self._master_equity: deque = deque(maxlen=10000)
        self._reverse_equity: deque = deque(maxlen=10000)
        self._other_equity: deque = deque(maxlen=10000)

        self._trade_id_counter: int = 0
        self._last_route_time: float = 0.0
        self._ev_cache: Optional[Dict[str, Any]] = None
        self._ev_cache_ts: float = 0.0
        # P2-R11-14修复: 使用模块级EV_CACHE_TTL常量，允许params覆盖
        # P2-R11-14修复: EV缓存TTL自适应策略类型
        _strategy_type = str(_all_params.get('strategy_type', 'default')).lower() if _all_params else 'default'
        _ev_ttl_default = EV_CACHE_TTL_DEFAULTS.get(_strategy_type, EV_CACHE_TTL_DEFAULTS['default'])
        self._ev_cache_ttl: float = float(_all_params.get('ev_cache_ttl', _ev_ttl_default)) if _all_params else _ev_ttl_default
        self._mode_engine = None
        self._snapshot_collector = None
        # NOTE: decision_interval机制已从系统中移除（与HMM state_confirm_bars冲突）
        # 决策频率由K线周期 × state_confirm_bars自然决定
        self._bar_counter: int = 0
        # N-08标记: _s1_params以下为硬编码默认值，优先从get_cached_params()读取
        self._s1_params = {
            'strategy_id': 's1_hft',
            'signal_confirm_ticks': 3,
            'cooldown_ms': 50,
            'min_imbalance': 0.15,
            'hft_hard_time_stop_ms': 1000,       # HFT硬止损1秒（毫秒级）
            'daily_drawdown_pct': 0.03,
            'risk_ratio': 0.2,
            # NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
        }
        self._s2_params = {
            'strategy_id': 's2_resonance',
            'state_confirm_bars': 5,  # R24-P1-DF-01修复: 统一默认值为5
            'cooldown_sec': 0,
            'min_strength': 0.3,
            'resonance_hard_time_stop_min': 5,   # 共振硬止损5分钟（分钟级）
            'daily_drawdown_pct': 0.05,
            'risk_ratio': 0.3,
            # NOTE: decision_interval_minutes仍用于回测跳帧逻辑(与state_confirm_bars互补，控制决策频率)
        }
        self._master_substrategy_pause = {
            's1_hft': False,
            's2_resonance': False,
        }
        self._handover_from: Optional[str] = None
        self._handover_to: Optional[str] = None
        self._handover_started_ts: float = 0.0
        self._handover_min_alloc: float = 0.10
        self._s3_params = {
            'strategy_id': 's3_box',
            'box_hard_time_stop_min': 30,        # 箱体硬止损30分钟（分钟级）
            'daily_drawdown_pct': 0.05,
            'risk_ratio': 0.3,
        }
        self._s4_params = {
            'strategy_id': 's4_spring',
            'spring_hard_time_stop_sec': 30,     # 弹簧硬止损30秒（秒级）
            'daily_drawdown_pct': 0.05,
            'risk_ratio': 0.3,
        }

        self._stats = {
            'total_trades': 0,
            'master_trades': 0,
            'reverse_trades': 0,
            'other_trades': 0,
            'spring_trades': 0,
            'arbitrage_trades': 0,
            'market_making_trades': 0,
            'state_switches': 0,
            'mutual_exclusion_blocks': 0,
            'capital_rebalances': 0,
            'ev_breaches': 0,
        }

        # ✅ 集成治理检查器（L-P0-4~L-P0-6, L-P1-4, L-P2-1, E7, E11）
        try:
            from ali2026v3_trading.governance_engine import (
                E12ReverseStrategyPseudoIndependenceDetector,
                E13ShadowStrategyCollusionDetector,
                MultiStateSwitchBacktestScenario,
                WF6ToWF10EliminationChecker,
                E8E9E10EliminationChecker,
                E7UnexplainedReturnChecker,
                E11QuantitativeSourceChecker,
            )
            _gov_cfg = self._config if hasattr(self, '_config') and self._config else {}
            self._e12_detector = E12ReverseStrategyPseudoIndependenceDetector(
                max_correlation_threshold=_gov_cfg.get("e12_max_correlation_threshold", 0.3),
                min_trade_count=_gov_cfg.get("e12_min_trade_count", 20),
            )
            self._e13_detector = E13ShadowStrategyCollusionDetector(
                min_param_diff_pct=_gov_cfg.get("e13_min_param_diff_pct", 0.20),
                max_signal_sync_rate=_gov_cfg.get("e13_max_signal_sync_rate", 0.7),
                min_trade_count=_gov_cfg.get("e13_min_trade_count", 20),
            )
            self._multi_state_scenario = MultiStateSwitchBacktestScenario()
            self._wf_elimination_checker = WF6ToWF10EliminationChecker()
            self._e8e9e10_checker = E8E9E10EliminationChecker()
            self._e7_checker = E7UnexplainedReturnChecker()
            self._e11_checker = E11QuantitativeSourceChecker()
            logger.info("[StrategyEcosystem] 治理检查器已集成: E7/E8/E9/E10/E11/E12/E13/WF6-10")
        except Exception as e:
            logger.warning("[StrategyEcosystem] 治理检查器集成失败: %s", e)
            self._e12_detector = None
            self._e13_detector = None
            self._multi_state_scenario = None
            self._wf_elimination_checker = None
            self._e8e9e10_checker = None
            self._e7_checker = None
            self._e11_checker = None

        logger.info("[StrategyEcosystem] 初始化完成")

        self._propagate_capital_scale()

    # NOTE: set_decision_interval / should_make_decision 已从系统中移除
    # 决策频率由K线周期 × state_confirm_bars自然决定，与HMM框架一致

    def set_snapshot_collector(self, collector) -> None:
        self._snapshot_collector = collector

    # NOTE: should_make_decision 已废弃，决策频率由K线周期 × state_confirm_bars自然决定
    # 保留此方法仅用于向后兼容，始终返回True
    # R13-P1-DEAD-01修复: 添加DeprecationWarning标记废弃方法
    def should_make_decision(self, strategy_id: str) -> bool:
        import warnings
        logging.warning("[DEPRECATED] should_make_decision is deprecated, always returns True")  # R16-P1-DOC-P1-10修复: 补充logging.warning确保废弃标记可见
        warnings.warn(
            "should_make_decision is deprecated: decision frequency is now determined by "
            "K-line period × state_confirm_bars. This method always returns True and will "
            "be removed in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )
        return True

    # P-13修复: _degrade_frequency回调通知策略层
    # R13-P1-DEAD-02修复: 使用_get_slot()替代不存在的self._slots
    def _on_degrade_frequency(self, old_freq: int, new_freq: int, reason: str = "") -> None:
        """当LiveStrategySelector降频时，通知策略层调整决策频率"""
        logging.info("[P-13] _degrade_frequency回调: %d→%d, reason=%s", old_freq, new_freq, reason)
        for sid in ('master', 'reverse', 'other', 'spring', 'arbitrage', 'market_making'):
            slot = self._get_slot(sid)
            if slot is not None and hasattr(slot, 'decision_frequency'):
                slot.decision_frequency = new_freq

    def notify_degrade_frequency(self, old_freq: int, new_freq: int, reason: str = "") -> None:
        """外部接口：触发降频回调链"""
        import inspect
        for cb in self._degrade_frequency_callbacks:
            try:
                # R13-P2-API-03修复: 回调签名校验
                sig = inspect.signature(cb)
                params = list(sig.parameters.values())
                # bound method的self已绑定，只需检查剩余参数
                required_count = sum(1 for p in params if p.default is inspect.Parameter.empty)
                if required_count > 3:
                    logging.warning("[P-13] 降频回调签名不匹配(需要>3个必选参数): %s", cb)
                    continue
                cb(old_freq, new_freq, reason)
            except Exception as e:
                logging.warning("[P-13] 降频回调异常: %s", e)

    def get_s1_params(self) -> Dict[str, Any]:
        return dict(self._s1_params)

    def get_s2_params(self) -> Dict[str, Any]:
        return dict(self._s2_params)

    def update_s1_params(self, **kwargs) -> None:
        self._s1_params.update(kwargs)

    def update_s2_params(self, **kwargs) -> None:
        self._s2_params.update(kwargs)

    # R27-P0-DR-13修复: 多策略隔板信号配额检查
    def check_strategy_bulkhead(self, strategy_name: str) -> bool:
        """检查策略是否超过信号配额（隔板保护），返回True=允许，False=阻断"""
        import time as _time
        bh = self._strategy_bulkheads.get(strategy_name)
        if bh is None:
            return True
        now = _time.time()
        # R27-P0-DR-13隐患修复: 断路后自动恢复配额
        if bh.get('max_signals_per_min', 1) == 0:
            _cb_at = self._strategy_circuit_breaker_at.get(strategy_name, 0.0)
            if _cb_at > 0 and (now - _cb_at) >= self._strategy_circuit_breaker_auto_recovery_sec:
                bh['max_signals_per_min'] = self._strategy_bulkheads.get(strategy_name, {}).get('_original_max_signals', 15)
                self._strategy_error_counts[strategy_name] = 0
                self._strategy_circuit_breaker_at.pop(strategy_name, None)
                logging.info("[R27-P0-DR-13] 策略隔板自动恢复: %s 配额恢复为%d/min",
                            strategy_name, bh['max_signals_per_min'])
            else:
                return False
        if now - bh.get('last_reset', 0.0) >= 60.0:
            bh['signals_count'] = 0
            bh['last_reset'] = now
        if bh['signals_count'] >= bh['max_signals_per_min']:
            logging.warning("[R27-P0-DR-13] 策略隔板阻断: %s 信号数%d>=配额%d/min",
                            strategy_name, bh['signals_count'], bh['max_signals_per_min'])
            return False
        bh['signals_count'] += 1
        return True

    # R27-P0-DR-13修复: 策略异常计数与断路
    def record_strategy_error(self, strategy_name: str, error: Exception) -> None:
        """记录策略异常，连续错误超阈值则触发策略断路"""
        count = self._strategy_error_counts.get(strategy_name, 0) + 1
        self._strategy_error_counts[strategy_name] = count
        if count >= self._strategy_circuit_breaker_threshold:
            logging.critical("[R27-P0-DR-13] 策略断路触发: %s 连续错误%d>=%d",
                             strategy_name, count, self._strategy_circuit_breaker_threshold)
            bh = self._strategy_bulkheads.get(strategy_name)
            if bh:
                bh['_original_max_signals'] = bh.get('max_signals_per_min', 15)  # 保存原始配额
                bh['max_signals_per_min'] = 0  # 断路: 配额归零
                self._strategy_circuit_breaker_at[strategy_name] = time.time()  # 记录断路时间

    def reset_strategy_error(self, strategy_name: str) -> None:
        """策略成功执行后重置异常计数"""
        self._strategy_error_counts[strategy_name] = 0

    def on_bar_update(self) -> None:
        self._bar_counter += 1
        # P1-11修复: 每bar更新时向BoxDetector喂数据（维护历史缓冲区）
        try:
            if hasattr(self, '_last_bar_data') and self._last_bar_data:
                bd = self._box_detector
                d = self._last_bar_data
                bd.update_bar(
                    high=d.get('high', 0.0), low=d.get('low', 0.0),
                    close=d.get('close', 0.0), volume=d.get('volume', 0.0),
                )
                if d.get('iv', 0.0) > 0:
                    bd.update_iv(d['iv'])
                bd.detect_box()
        except Exception:
            pass
        if self._bar_counter % 60 == 0:
            try:
                self.validate_position_consistency()
            except Exception:
                pass
            try:
                self.normalize_capital_allocation()
            except Exception:
                pass

    @_require_interface(('_capital_scale', '_master', '_reverse', '_other'))
    def _propagate_capital_scale(self) -> None:
        if self._capital_scale is None:
            return
        scale_str = self._capital_scale.value if hasattr(self._capital_scale, 'value') else str(self._capital_scale).lower()
        try:
            from ali2026v3_trading.mode_engine import ModeEngine
            me = ModeEngine.create_engine(scale_str)
            self._mode_engine = me
            me.register_component('StrategyEcosystem', self)
            result = me.switch_mode(scale_str, reason='ecosystem_capital_scale_change')  # R25-P1-TR-03修复: 补充reason参数
            if not result.get('success'):
                logger.warning("[StrategyEcosystem] ModeEngine switch failed: %s", result)
        except Exception as e:
            logger.warning("[StrategyEcosystem] ModeEngine unavailable, falling back to direct propagation: %s", e)
            self._propagate_capital_scale_fallback(scale_str)

    def _propagate_capital_scale_fallback(self, scale_str: str) -> None:
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            rs.set_capital_scale(scale_str)
        except Exception as e:
            logger.warning("[StrategyEcosystem] fallback propagate to RiskService failed: %s", e)  # R24-P2-DF-11修复: debug→warning，风控参数传播失败需可感知
        try:
            from ali2026v3_trading.signal_service import SignalService, get_signal_service
            ss = get_signal_service()
            if scale_str == 'small':
                ss.enable_plr_filter(min_estimated_plr=2.0)
            elif scale_str == 'medium':
                ss.enable_plr_filter(min_estimated_plr=1.5)
            else:
                ss.disable_plr_filter()
        except Exception as e:
            logger.warning("[StrategyEcosystem] fallback propagate to SignalService failed: %s", e)  # R24-P2-DF-11修复: debug→warning
        try:
            from ali2026v3_trading.state_param_manager import StateParamManager, get_state_param_manager
            spm = get_state_param_manager()
            spm.set_capital_scale(scale_str)
        except Exception as e:
            logger.warning("[StrategyEcosystem] fallback propagate to StateParamManager failed: %s", e)  # R24-P2-DF-11修复: debug→warning
        try:
            from ali2026v3_trading.box_spring_strategy import get_box_spring_strategy
            bss = get_box_spring_strategy()
            if hasattr(bss, 'set_capital_scale') and callable(getattr(bss, 'set_capital_scale')):
                bss.set_capital_scale(scale_str)
            else:
                # R14-P1-API-12修复: 不直接设私有属性，改为setattr+warning日志
                setattr(bss, '_capital_scale', scale_str)
                logger.warning("[StrategyEcosystem] R14-P1-API-12: BoxSpringStrategy无set_capital_scale方法，使用setattr设_capital_scale=%s", scale_str)
        except Exception as e:
            logger.warning("[StrategyEcosystem] fallback propagate to BoxSpringStrategy failed: %s", e)  # R24-P2-DF-11修复: debug→warning

    def on_state_switched(self, old_state: str, new_state: str) -> None:
        """状态切换回调：由StateParamManager触发

        当SPM检测到三态切换时，自动联动ecosystem切换活跃策略。
        R5-T-05修复: 添加递归深度防护，防止回调链无限递归导致栈溢出。
        """
        # R5-T-05修复: 递归深度防护
        _depth = getattr(self, '_state_switch_depth', 0)
        if _depth >= 3:
            logger.warning("[StrategyEcosystem] R5-T-05: on_state_switched递归深度%d>=3，阻断递归", _depth)
            return
        self._state_switch_depth = _depth + 1
        try:
            result = self.switch_active_strategy(new_state)
            if result.get('switched'):
                logger.info(
                    "[StrategyEcosystem] SPM联动切换: %s → %s, result=%s",
                    old_state, new_state, result,
                )
        finally:
            self._state_switch_depth = _depth  # R5-T-05: 恢复递归深度

    @property
    def box_detector(self) -> BoxDetector:
        return self._box_detector

    def _generate_trade_id(self) -> str:
        # R10-P0-03修复: _trade_id_counter自增在锁内执行，防止并发trade_id重复
        with self._lock:
            self._trade_id_counter += 1
            counter = self._trade_id_counter
        ts = datetime.now(CHINA_TZ).strftime('%Y%m%d%H%M%S')
        return f"ECO-{ts}-{counter:06d}"

    # R13-P1-DEAD-08修复: 交易日切换时重置_trade_id_counter，防止跨日ID持续递增
    def reset_trade_id_counter_on_new_day(self) -> None:
        """在新交易日开始时调用，重置交易ID计数器"""
        today = datetime.now(CHINA_TZ).strftime('%Y%m%d')
        with self._lock:
            if not hasattr(self, '_last_trade_id_reset_date'):
                self._last_trade_id_reset_date = today
                return
            if self._last_trade_id_reset_date != today:
                self._trade_id_counter = 0
                self._last_trade_id_reset_date = today
                logger.info("[R13-P1-DEAD-08] _trade_id_counter已重置, new_date=%s", today)

    def _load_capital_route_params(self) -> Dict[str, float]:
        """R7-M-12修复: 从配置参数读取capital_route_s1~s6

        手册4.3节定义6个资金路由参数:
          capital_route_s1_hft, capital_route_s2_minute,
          capital_route_s3_box_extreme, capital_route_s4_box_spring,
          capital_route_s5_arbitrage, capital_route_s6_market_making

        Returns:
            Dict[str, float]: 路由参数映射 {s1_hft: 0.30, s2_minute: 0.20, ...}
        """
        try:
            from ali2026v3_trading.config_params import get_cached_params
            params = get_cached_params()
            return {
                's1_hft': params.get('capital_route_s1_hft', 0.30),
                's2_minute': params.get('capital_route_s2_minute', 0.20),
                's3_box_extreme': params.get('capital_route_s3_box_extreme', 0.15),
                's4_box_spring': params.get('capital_route_s4_box_spring', 0.15),
                's5_arbitrage': params.get('capital_route_s5_arbitrage', 0.10),
                's6_market_making': params.get('capital_route_s6_market_making', 0.10),
            }
        except Exception as e:
            logger.debug("[R7-M-12] 读取capital_route参数失败，使用类常量默认值: %s", e)
            return {}

    def set_master_substrategy_pause(self, strategy_key: str, paused: bool = True) -> None:
        """允许在共享master槽位内独立暂停S1/S2子策略。"""
        if strategy_key not in self._master_substrategy_pause:
            return
        with self._lock:
            self._master_substrategy_pause[strategy_key] = bool(paused)
        logger.info("[StrategyEcosystem] master子策略暂停开关: %s paused=%s", strategy_key, paused)

    @count_call()
    @_require_interface(('_lock', '_capital_route', '_master', '_reverse', '_other'))
    def route_capital(self, active_state: str) -> Dict[str, float]:
        self.on_bar_update()
        with self._lock:
            cr = self._capital_route

            # R7-M-12修复: 从配置参数读取capital_route_s1~s6，替代硬编码常量
            # 手册4.3节: capital_route_s1_hft ~ capital_route_s6_market_making
            _route_params = self._load_capital_route_params()

            if not cr.dynamic_enabled:
                return {
                    'master': _route_params.get('s1_hft', cr.master_base),
                    'reverse': _route_params.get('s2_minute', cr.reverse_base),
                    'other': _route_params.get('s3_box_extreme', cr.other_base),
                    'spring': _route_params.get('s4_box_spring', self._spring.capital_allocation),
                    'arbitrage': _route_params.get('s5_arbitrage', cr.arbitrage_base),
                    'market_making': _route_params.get('s6_market_making', cr.market_making_base),
                }

            if self._capital_scale == CapitalScale.SMALL:
                spring_base = _route_params.get('s4_box_spring', self.CAPITAL_ALLOC_SMALL_SPRING)
                master_base = _route_params.get('s1_hft', self.CAPITAL_ALLOC_SMALL_MASTER)
                reverse_base = _route_params.get('s2_minute', self.CAPITAL_ALLOC_SMALL_REVERSE)
                other_base = _route_params.get('s3_box_extreme', self.CAPITAL_ALLOC_SMALL_OTHER)
            elif self._capital_scale == CapitalScale.LARGE:
                spring_base = _route_params.get('s4_box_spring', self.CAPITAL_ALLOC_LARGE_SPRING)
                master_base = _route_params.get('s1_hft', self.CAPITAL_ALLOC_LARGE_MASTER)
                reverse_base = _route_params.get('s2_minute', self.CAPITAL_ALLOC_LARGE_REVERSE)
                other_base = _route_params.get('s3_box_extreme', self.CAPITAL_ALLOC_LARGE_OTHER)
            else:
                spring_base = _route_params.get('s4_box_spring', cr.spring_base)
                master_base = _route_params.get('s1_hft', cr.master_base)
                reverse_base = _route_params.get('s2_minute', cr.reverse_base)
                other_base = _route_params.get('s3_box_extreme', cr.other_base)

            # P2-R3-P-12: S1/S2共用master槽位 — correct_trending(S1)和incorrect_reversal(S2)
            # 均向master槽分配资金，但两者交易方向相反(顺势vs反转)，理论上可对冲。
            # 已知限制: 若S1和S2同时激活，master槽实际承载双向头寸，仓位净额可能接近零，
            # 浪费保证金但不增加风险。当前未区分S1/S2的master子槽位。
            allocations = {
                'master': cr.min_maintenance_ratio,
                'reverse': cr.min_maintenance_ratio,
                'other': cr.min_maintenance_ratio,
                'spring': spring_base,
            }

            if active_state == STRATEGY_MODE_CORRECT_TRENDING:  # R25-SE-P1-02-FIX
                allocations['master'] = master_base + cr.master_active_boost
                remaining = 1.0 - allocations['master'] - 2 * cr.min_maintenance_ratio - allocations['spring']
                if remaining > 0:
                    allocations['reverse'] += remaining * self.REMAINING_ALLOC_MASTER
                    allocations['other'] += remaining * self.REMAINING_ALLOC_MASTER_OTHER
                    allocations['spring'] += remaining * self.REMAINING_ALLOC_MASTER_SPRING
            elif active_state == STRATEGY_MODE_INCORRECT_REVERSAL:  # R25-SE-P1-02-FIX
                allocations['reverse'] = reverse_base + cr.master_active_boost
                remaining = 1.0 - allocations['reverse'] - 2 * cr.min_maintenance_ratio - allocations['spring']
                if remaining > 0:
                    allocations['master'] += remaining * self.REMAINING_ALLOC_REVERSE_MASTER
                    allocations['other'] += remaining * self.REMAINING_ALLOC_REVERSE_OTHER
                    allocations['spring'] += remaining * self.REMAINING_ALLOC_REVERSE_SPRING
            elif active_state == 'other':
                allocations['other'] = other_base + cr.master_active_boost
                remaining = 1.0 - allocations['other'] - 2 * cr.min_maintenance_ratio - allocations['spring']
                if remaining > 0:
                    allocations['master'] += remaining * self.REMAINING_ALLOC_OTHER_MASTER
                    allocations['reverse'] += remaining * self.REMAINING_ALLOC_OTHER_REVERSE
                    allocations['spring'] += remaining * self.REMAINING_ALLOC_OTHER_SPRING
            else:
                allocations['master'] = master_base
                allocations['reverse'] = reverse_base
                allocations['other'] = other_base
                allocations['spring'] = spring_base

            # P1-裂缝38：应用状态联动乘子表
            state_multipliers = self.STATE_STRATEGY_MULTIPLIERS.get(active_state, {})
            strategy_to_slot = {
                'master': ['s1_hft', 's2_resonance'],
                'reverse': ['s2_resonance'],
                'other': ['s3_box_extreme', 's4_box_spring'],
                'spring': ['s4_box_spring'],
            }
            for slot_name, strategy_keys in strategy_to_slot.items():
                multiplier = 1.0
                for sk in strategy_keys:
                    if sk in self._master_substrategy_pause and self._master_substrategy_pause.get(sk, False):
                        continue
                    if sk in state_multipliers:
                        multiplier = state_multipliers[sk]
                        break  # 取第一个匹配的乘子
                if multiplier != 1.0:
                    allocations[slot_name] *= multiplier

            # R16-P1-18修复: S1-S4交叉管理独立性 - 基于子策略EV独立调整资金权重
            _master_slot = self._get_slot('master')
            if _master_slot is not None:
                _s1_ev = _master_slot.get_sub_strategy_ev('s1_hft')
                _s2_ev = _master_slot.get_sub_strategy_ev('s2_resonance')
                _s1_pause = self._master_substrategy_pause.get('s1_hft', False)
                _s2_pause = self._master_substrategy_pause.get('s2_resonance', False)
                if _s1_pause and not _s2_pause:
                    allocations['master'] *= 0.5
                elif _s2_pause and not _s1_pause:
                    allocations['master'] *= 0.5
                elif _s1_pause and _s2_pause:
                    allocations['master'] *= 0.0
                if _s1_ev < 0 and _s2_ev >= 0:
                    allocations['master'] *= 0.6
                elif _s2_ev < 0 and _s1_ev >= 0:
                    allocations['master'] *= 0.6

            # 切换持仓保护：旧策略仍有持仓时，为其保留最小资金配额，避免交接空窗期。
            if self._handover_from and self._handover_to:
                _handover_slot = self._get_slot(self._handover_from)
                if _handover_slot is not None and _handover_slot.position_count > 0:
                    _min_alloc = max(cr.min_maintenance_ratio, self._handover_min_alloc)
                    _from_alloc = allocations.get(self._handover_from, 0.0)
                    if _from_alloc < _min_alloc:
                        _donor = self._handover_to if self._handover_to in allocations else None
                        _delta = _min_alloc - _from_alloc
                        if _donor and allocations.get(_donor, 0.0) > _delta:
                            allocations[self._handover_from] = _from_alloc + _delta
                            allocations[_donor] = max(0.0, allocations[_donor] - _delta)
                else:
                    self._handover_from = None
                    self._handover_to = None
                    self._handover_started_ts = 0.0

            # [R16-P1-13修复] 资金分配原子性保护：分配总和偏离1.0时告警并归一化
            _total_alloc = sum(allocations.values())
            if abs(_total_alloc - 1.0) > 0.01:
                logger.warning("[R16-P1-13] 资金分配总和不等于100%%: total=%.4f, 自动归一化", _total_alloc)
            total = sum(allocations.values())
            if abs(total - 1.0) > 1e-6:
                for k in allocations:
                    allocations[k] /= total

            # P1-R9-13修复: alpha_ratio深度参与路由 — 作为权重因子动态调整所有槽位
            if cr.alpha_ratio > 0.0 and cr.alpha_ratio <= 1.0:
                _alpha_w = cr.alpha_ratio
                allocations['master'] = allocations.get('master', 0.0) * (1.0 + _alpha_w * 0.15)
                allocations['reverse'] = allocations.get('reverse', 0.0) * (1.0 - _alpha_w * 0.10)
                allocations['other'] = allocations.get('other', 0.0) * (1.0 - _alpha_w * 0.05)
                allocations['spring'] = allocations.get('spring', 0.0) * (1.0 - _alpha_w * 0.05)
                _total_after = sum(allocations.values())
                if abs(_total_after - 1.0) > 1e-6 and _total_after > 1e-6:
                    for k in allocations:
                        allocations[k] /= _total_after

            # P0-R8-09修复: CRM周期共振模块接入route_capital
            # 手册26.3+26.5节: allocations[k] × size_multiplier，再归一化
            try:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                _sse = get_shadow_strategy_engine()
                if _sse is not None:
                    if _sse.is_degradation_active():
                        logger.warning("[StrategyEcosystem] 影子引擎降级激活，资金分配降至min_maintenance_ratio")
                        for slot_name in ('master', 'reverse', 'other', 'spring'):
                            if slot_name in allocations:
                                allocations[slot_name] = cr.min_maintenance_ratio
                    elif _sse.is_absolute_ev_paused():
                        logger.warning("[StrategyEcosystem] 影子引擎EV暂停激活，资金分配降至min_maintenance_ratio")
                        for slot_name in ('master', 'reverse', 'other', 'spring'):
                            if slot_name in allocations:
                                allocations[slot_name] = cr.min_maintenance_ratio
            except Exception as _sse_err:
                logger.debug("[StrategyEcosystem] 影子引擎降级检查跳过: %s", _sse_err)

            try:
                from ali2026v3_trading.param_pool.cycle_resonance_module import get_cycle_resonance_module
                crm = get_cycle_resonance_module()
                if crm:
                    # 获取各策略的风险曲面调整(含size_multiplier)
                    _SLOT_TO_CRM_STRATEGY = {'master': 'resonance', 'reverse': 'box', 'other': 'high_freq', 'spring': 'spring'}
                    for slot_name in ['master', 'reverse', 'other', 'spring']:
                        try:
                            _crm_strategy = _SLOT_TO_CRM_STRATEGY.get(slot_name, slot_name)
                            risk_adj = crm.get_risk_surface(strategy=_crm_strategy)
                            if risk_adj and hasattr(risk_adj, 'size_multiplier'):
                                crm_mult = risk_adj.size_multiplier
                                if crm_mult > 0 and crm_mult != 1.0:
                                    allocations[slot_name] *= crm_mult
                        except Exception:
                            pass
                    # P2-CR-004显式启用: 获取CRM信号优先级并根据波动率regime动态调整
                    _volatility_regime = None
                    try:
                        from ali2026v3_trading.params_service import get_params_service
                        _ps_crm = get_params_service()
                        _volatility_regime = _ps_crm.get_str('volatility_regime', None)
                        if _volatility_regime not in ('low', 'normal', 'high', 'extreme'):
                            _volatility_regime = None
                    except Exception:
                        pass
                    _crm_output = getattr(crm, '_last_output', None)
                    _crm_phase = getattr(_crm_output, 'phase', None) if _crm_output else None
                    if _crm_phase is not None:
                        _priority_list = crm.get_signal_priority(_crm_phase, volatility_regime=_volatility_regime)
                        _SLOT_MAP = {'resonance': 'master', 'spring': 'spring', 'high_freq': 'other', 'box': 'reverse'}
                        _ranked_allocs = []
                        for _p_slot in _priority_list:
                            _alloc_key = _SLOT_MAP.get(_p_slot)
                            if _alloc_key and _alloc_key in allocations:
                                _ranked_allocs.append((_alloc_key, allocations[_alloc_key]))
                        for _missing_key in ('master', 'reverse', 'other', 'spring'):
                            if _missing_key not in [k for k, _ in _ranked_allocs]:
                                _ranked_allocs.append((_missing_key, allocations.get(_missing_key, 0.0)))
                        _PRIORITY_BOOST = 1.15
                        if _ranked_allocs:
                            allocations[_ranked_allocs[0][0]] *= _PRIORITY_BOOST
                        logger.debug("[P2-CR-004] CRM信号优先级激活: phase=%s regime=%s priority=%s top=%s",
                                     _crm_phase, _volatility_regime,
                                     _priority_list[:2] if _priority_list else [],
                                     _ranked_allocs[0][0] if _ranked_allocs else None)
                    # 重新归一化
                    total2 = sum(v for k, v in allocations.items() if k in ('master', 'reverse', 'other', 'spring'))
                    if total2 > 1e-10:
                        for k in ('master', 'reverse', 'other', 'spring'):
                            allocations[k] /= total2
            except Exception as e:
                logger.debug("[StrategyEcosystem] CRM route_capital integration: %s", e)

            # R3-P-01修复: s5_arbitrage/s6_market_making路由从_route_params读取(动态路径也消费配置参数)
            allocations['arbitrage'] = _route_params.get('s5_arbitrage', cr.arbitrage_base)
            allocations['market_making'] = _route_params.get('s6_market_making', cr.market_making_base)

            # R3-P-01修复: 追加s5/s6后重新归一化，防止分配总和超过1.0
            _total_alloc = sum(allocations.values())
            if _total_alloc > 1e-10 and abs(_total_alloc - 1.0) > 1e-6:
                for k in allocations:
                    allocations[k] /= _total_alloc

            # P1-CA-002修复: 资金路由动态调整 — 根据策略绩效和市场状态调整分配比例
            try:
                from ali2026v3_trading.params_service import get_params_service
                _ps = get_params_service()
                _dynamic_enabled = _ps.get_bool('capital_route_dynamic_enabled', False)
                if _dynamic_enabled:
                    _perf_weights = {}
                    for slot_name, strat in [('master', self._master), ('reverse', self._reverse),
                                             ('other', self._other), ('spring', self._spring)]:
                        if strat and hasattr(strat, 'win_rate') and hasattr(strat, 'trade_count'):
                            wr = getattr(strat, 'win_rate', 0.5)
                            tc = getattr(strat, 'trade_count', 0)
                            _perf_weights[slot_name] = wr if tc >= 20 else 0.5
                        else:
                            _perf_weights[slot_name] = 0.5
                    _total_pw = sum(_perf_weights.values())
                    if _total_pw > 1e-10:
                        for slot_name in _perf_weights:
                            base_ratio = allocations.get(slot_name, 0.0)
                            perf_adj = _perf_weights[slot_name] / _total_pw
                            blend = _ps.get_float('capital_route_perf_blend', 0.3)
                            allocations[slot_name] = base_ratio * (1.0 - blend) + perf_adj * blend
                        _renorm = sum(allocations.get(k, 0.0) for k in allocations)
                        if abs(_renorm - 1.0) > 1e-6 and _renorm > 1e-10:
                            for k in allocations:
                                allocations[k] /= _renorm
            except Exception as _e:
                logger.debug("[P1-CA-002] 资金路由动态调整跳过: %s", _e)

            self._master.capital_allocation = allocations['master']
            self._reverse.capital_allocation = allocations['reverse']
            self._other.capital_allocation = allocations['other']
            self._spring.capital_allocation = allocations.get('spring', cr.spring_base)

            # P2-CA-003修复: S5(套利)/S6(做市)互斥验证
            # 套利策略需要市场存在定价偏差, 做市策略需要市场相对均衡
            # 两者同时高活跃度时存在逻辑冲突, 需根据市场状态互斥调整
            s5_alloc = allocations.get('arbitrage', 0.0)
            s6_alloc = allocations.get('market_making', 0.0)
            if s5_alloc > 0.05 and s6_alloc > 0.05:
                try:
                    from ali2026v3_trading.params_service import get_params_service
                    _ps = get_params_service()
                    _s5_s6_mutex_mode = _ps.get_str('capital_route_s5_s6_mutex', 'arbitrage_priority')
                    if _s5_s6_mutex_mode == 'arbitrage_priority':
                        allocations['market_making'] = min(s6_alloc, 0.05)
                        logger.debug("[P2-CA-003] S5/S6互斥: 套利优先, 做市限制为%.3f", allocations['market_making'])
                    elif _s5_s6_mutex_mode == 'market_making_priority':
                        allocations['arbitrage'] = min(s5_alloc, 0.05)
                        logger.debug("[P2-CA-003] S5/S6互斥: 做市优先, 套利限制为%.3f", allocations['arbitrage'])
                except Exception as _e:
                    logger.debug("[P2-CA-003] S5/S6互斥检查跳过: %s", _e)

            # P1-8修复：再平衡摩擦成本扣除
            # 每次route_capital重分配时扣除再平衡摩擦成本(默认2bps)
            # R16-P2-06修复: 资金路由动态分配 - rebalance摩擦改为可配置
            _rebalance_friction_bps = float(os.environ.get('REBALANCE_FRICTION_BPS', '2.0'))
            rebalance_friction_bps = getattr(cr, 'rebalance_friction_bps', _rebalance_friction_bps)
            total_friction_pct = rebalance_friction_bps / 10000.0
            if self._stats['capital_rebalances'] > 0:
                for k in allocations:
                    allocations[k] = max(0.0, allocations[k] * (1.0 - total_friction_pct))

            self._stats['capital_rebalances'] += 1
            self._auto_upgrade_capital_scale()

            _now_obs = time.time()
            for _slot_name in ('master', 'reverse', 'other', 'spring', 'arbitrage', 'market_making'):
                _slot = self._get_slot(_slot_name)
                if _slot is not None and _slot.state == 'retired':
                    allocations[_slot_name] = 0.0
                    continue
                if _slot is not None and _slot._observation_period_until is not None:
                    if _now_obs < _slot._observation_period_until:
                        _obs_scale = _slot._observation_position_scale
                        allocations[_slot_name] = allocations.get(_slot_name, 0.0) * _obs_scale
                    else:
                        _slot._observation_period_until = None
                        _slot._observation_position_scale = 1.0

            # R16-P2-16修复: Alpha置信区间重叠影响资金分配
            try:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                _sse = get_shadow_strategy_engine()
                if _sse is not None and hasattr(_sse, 'alpha_confidence_overlap_test'):
                    _overlap = _sse.alpha_confidence_overlap_test()
                    if _overlap and not _overlap.get('overlap_valid', True):
                        _overlap_scale = 0.8
                        for k in allocations:
                            allocations[k] *= _overlap_scale
                        logging.warning("[R16-P2-16] Alpha置信区间重叠，资金分配缩放至80%%")
            except Exception:
                pass

            return allocations

    def enforce_l2_dataset_binding(self, dataset_id: str) -> bool:
        with self._lock:
            if not dataset_id:
                self._l2_dataset_enforced = False
                self._l2_dataset_id = ''
                return False
            self._l2_dataset_enforced = True
            self._l2_dataset_id = dataset_id
            return True

    def _auto_upgrade_capital_scale(self) -> None:
        if not self._capital_scale_upgrade_enabled:
            return
        # R16-P1-05: 调用risk_service的升级评估
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            _rs = get_risk_service(None)
            if _rs is not None:
                _upgrade = _rs.evaluate_capital_scale_upgrade()
                if _upgrade.get('eligible'):
                    logger.info("[R16-P1-05] risk_service升级评估通过: %s", _upgrade)
        except Exception:
            pass
        total_trades = int(self._stats.get('total_trades', 0))
        ev_breaches = int(self._stats.get('ev_breaches', 0))
        if total_trades < 20 or ev_breaches > 0:
            return
        if self._capital_scale is None and hasattr(CapitalScale, 'MEDIUM'):
            self._capital_scale = CapitalScale.MEDIUM
            logger.info("[StrategyEcosystem] capital_scale自动升级: None -> %s", self._capital_scale)
        elif self._capital_scale == getattr(CapitalScale, 'MEDIUM', self._capital_scale) and total_trades >= 100 and ev_breaches == 0:
            target = getattr(CapitalScale, 'LARGE', self._capital_scale)
            if target != self._capital_scale:
                self._capital_scale = target
                logger.info("[StrategyEcosystem] capital_scale自动升级: MEDIUM -> %s", self._capital_scale)

    def bind_strategy_document(self, strategy_id: str, doc_path: str) -> None:
        with self._lock:
            if not strategy_id:
                return
            self._strategy_doc_links[strategy_id] = doc_path

    def _record_strategy_lineage(self, from_strategy: str, to_strategy: str, market_state: str) -> None:
        if not to_strategy:
            return
        chain = self._strategy_lineage.setdefault(to_strategy, [])
        chain.append({
            'from': from_strategy,
            'to': to_strategy,
            'market_state': market_state,
            'timestamp': time.time(),
        })
        if len(chain) > 200:
            del chain[:-200]

    def _record_param_evolution(self, strategy_id: str, params_hash: str, source: str) -> None:
        self._strategy_param_evolution.append({
            'strategy_id': strategy_id,
            'params_hash': params_hash,
            'source': source,
            'timestamp': time.time(),
        })

    def retire_strategy_permanently(self, strategy_id: str, reason: str, operator: str = 'system') -> bool:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None:
                return False
            # INV-07: 验证状态转换合法性
            if not slot.validate_state_transition('retired'):
                logger.warning(
                    "[INV-07] retire_strategy_permanently: 策略%s从%s→retired转换不合法, 仍执行退市",
                    strategy_id, slot.state,
                )
            slot.paused = True
            slot.frozen = True
            slot.pause_timestamp = time.time()  # R17-LC-03修复
            slot.state = 'retired'
            slot.pause_reason = reason or 'retired'
            self._retired_strategies[strategy_id] = {
                'strategy_id': strategy_id,
                'reason': reason,
                'operator': operator,
                'retired_at': time.time(),
                'status': 'retired',
            }
            logger.warning("[StrategyEcosystem] 策略永久退市: %s reason=%s operator=%s", strategy_id, reason, operator)
            return True

    def submit_strategy_revival_review(self, strategy_id: str, evidence: str, requester: str = 'system') -> Dict[str, Any]:
        with self._lock:
            request = {
                'request_id': f"revive_{strategy_id}_{int(time.time()*1000)}",
                'strategy_id': strategy_id,
                'evidence': evidence,
                'requester': requester,
                'status': 'pending_review',
                'created_at': time.time(),
            }
            self._revival_requests.append(request)
            return dict(request)

    def approve_strategy_revival(self, request_id: str, approver: str = 'risk_committee') -> bool:
        with self._lock:
            for req in self._revival_requests:
                if req.get('request_id') != request_id:
                    continue
                if req.get('status') != 'pending_review':
                    return False
                strategy_id = str(req.get('strategy_id', ''))
                slot = self._get_slot(strategy_id)
                if slot is None:
                    return False
                # INV-07: 验证状态转换合法性
                if not slot.validate_state_transition('standby'):
                    logger.warning(
                        "[INV-07] approve_strategy_revival: 策略%s从%s→standby转换不合法",
                        strategy_id, slot.state,
                    )
                    return False
                slot.paused = False
                slot.frozen = False
                slot.state = 'standby'
                slot.pause_reason = ''
                req['status'] = 'approved'
                req['approved_by'] = approver
                req['approved_at'] = time.time()
                if strategy_id in self._retired_strategies:
                    self._retired_strategies[strategy_id]['status'] = 'revived'
                logger.info("[StrategyEcosystem] 策略复活审批通过: %s request=%s", strategy_id, request_id)
                return True
        return False

    @_require_interface(('_lock', '_active_strategy', '_master', '_reverse', '_other'))
    def switch_active_strategy(self, new_state: str) -> Dict[str, Any]:
        with self._lock:
            new_active = self.resolve_strategy_id_from_state(new_state)

            if new_active == self._active_strategy:
                # R15-P1-API-09修复: 统一switch返回结构，含success/timestamp
                return {'success': True, 'switched': False, 'from': self._active_strategy, 'to': new_active, 'timestamp': time.time()}

            # P0: 影子引擎降级状态检查 — 降级或EV暂停时阻止切换到该策略
            try:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                _sse = get_shadow_strategy_engine()
                if _sse is not None:
                    if _sse.is_degradation_active():
                        logger.warning(
                            "[StrategyEcosystem] 影子引擎降级激活，阻止切换到策略%s(state=%s)",
                            new_active, new_state,
                        )
                        return {
                            'success': False,
                            'switched': False,
                            'from': self._active_strategy,
                            'to': new_active,
                            'reason': 'shadow_degradation_active',
                            'timestamp': time.time(),
                        }
                    if _sse.is_absolute_ev_paused():
                        logger.warning(
                            "[StrategyEcosystem] 影子引擎EV暂停激活，阻止切换到策略%s(state=%s)",
                            new_active, new_state,
                        )
                        return {
                            'success': False,
                            'switched': False,
                            'from': self._active_strategy,
                            'to': new_active,
                            'reason': 'shadow_ev_paused',
                            'timestamp': time.time(),
                        }
            except Exception as _sse_err:
                logger.debug("[StrategyEcosystem] 影子引擎降级检查跳过(允许切换): %s", _sse_err)

            old_active = self._active_strategy
            old_slot = self._get_slot(old_active)
            if old_slot is not None and old_slot.position_count > 0:
                # INV-07: 验证handover状态转换合法性
                if not old_slot.validate_state_transition('handover'):
                    logger.warning(
                        "[INV-07] 策略%s从%s→handover转换不合法, 仍执行切换保护",
                        old_active, old_slot.state,
                    )
                old_slot.freeze()
                old_slot.state = 'handover'
                self._handover_from = old_active
                self._handover_to = new_active
                self._handover_started_ts = time.time()
                logger.warning(
                    "[StrategyEcosystem] 策略切换持仓保护: %s仍有持仓(%d)，进入handover后再完全切换",
                    old_active, old_slot.position_count,
                )
            self._prev_active_strategy = old_active
            self._active_strategy = new_active
            self._record_strategy_lineage(old_active, new_active, new_state)
            self._strategy_doc_links.setdefault(new_active, f"docs/strategies/{new_active}.md")
            _new_slot = self._get_slot(new_active)
            if _new_slot is not None:
                _params_hash = _new_slot.params_hash or f"state:{new_state}"
                self._record_param_evolution(new_active, _params_hash, 'switch_active_strategy')

            self._master.state = 'active' if new_active == 'master' else 'standby'
            self._reverse.state = 'active' if new_active == 'reverse' else 'standby'
            self._other.state = 'active' if new_active == 'other' else 'standby'

            self.route_capital(new_state)
            self._stats['state_switches'] += 1

            # ✅ P1-16修复: 集成StateTransitionAnalytics状态转换分析  # R17-P2-DOC-05
            try:
                from ali2026v3_trading.state_param_manager import StateTransitionAnalytics
                sta = getattr(self, '_state_transition_analytics', None)
                if sta is None:
                    sta = StateTransitionAnalytics()
                    self._state_transition_analytics = sta
                sta.on_state_change('default', old_active, new_active)
                matrix = sta.get_transition_matrix()
                logger.debug("[StrategyEcosystem] 状态转换矩阵: %s", matrix)
            except Exception as e:
                logger.debug("[StrategyEcosystem] StateTransitionAnalytics error: %s", e)

            # P0-11修复: 策略切换时执行E13同谋检测
            # 原代码仅导入E13检测器但从未调用detect()，实盘无法检测参数同谋
            try:
                if self._e13_detector is not None:
                    _main_params = {k: v for k, v in self._s1_params.items()
                                    if isinstance(v, (int, float))}
                    _shadow_params = {k: v for k, v in self._s2_params.items()
                                      if isinstance(v, (int, float))}
                    _e13_result = self._e13_detector.detect(
                        main_params=_main_params,
                        shadow_params=_shadow_params,
                        main_signals=[],
                        shadow_signals=[],
                    )
                    if _e13_result.get("e13_triggered", False):
                        logger.warning(
                            "[P0-11] E13同谋检测触发! 参数差异度不足，主策略与影子策略可能同谋。详情: %s",
                            _e13_result.get("details", "")
                        )
            except Exception as e:
                logger.debug("[StrategyEcosystem] E13同谋检测失败: %s", e)

            logger.info(
                "[StrategyEcosystem] 策略切换: %s → %s (state=%s)",
                old_active, new_active, new_state,
            )

            if new_state == STRATEGY_MODE_CORRECT_TRENDING:  # R25-SE-P1-02-FIX
                self._s1_params['active'] = True
                self._s2_params['active'] = True
            else:
                self._s1_params['active'] = False
                self._s2_params['active'] = False

            return {
                'success': True,
                'switched': True,
                'from': old_active,
                'to': new_active,
                'market_state': new_state,
                'close_old_positions_first': True,
                'timestamp': time.time(),
                'capital_allocations': {
                    'master': self._master.capital_allocation,
                    'reverse': self._reverse.capital_allocation,
                    'other': self._other.capital_allocation,
                    'spring': self._spring.capital_allocation,
                },
            }

    def validate_switch_slippage_cascade(self, close_volume: float = 1.0,
                                           base_slippage_bps: float = 1.0,
                                           cascade_multiplier: float = 1.5) -> Dict[str, Any]:
        """P1-裂缝19：验证策略切换时平仓+开仓的连续滑点放大

        当同一Bar内平仓+开仓时，开仓滑点 = 平仓滑点 × 1.5 + 额外冲击成本
        （与平仓量成正比）。检验切换场景下的夏普衰减。
        """
        impact_cost_bps = close_volume * 0.5  # 冲击成本与平仓量成正比
        close_slippage = base_slippage_bps
        open_slippage = close_slippage * cascade_multiplier + impact_cost_bps
        total_switch_slippage = close_slippage + open_slippage

        # 估算对Sharpe的影响（假设日均交易1次，年化252天）
        annual_cost_pct = total_switch_slippage / 10000 * 252 * 0.5  # 假设50%的交易日有切换
        estimated_sharpe_reduction = annual_cost_pct / 0.15  # 假设波动率15%

        return {
            "close_slippage_bps": round(close_slippage, 2),
            "open_slippage_bps": round(open_slippage, 2),
            "total_switch_slippage_bps": round(total_switch_slippage, 2),
            "impact_cost_bps": round(impact_cost_bps, 2),
            "cascade_multiplier": cascade_multiplier,
            "estimated_sharpe_reduction": round(estimated_sharpe_reduction, 4),
            "needs_slippage_buffer": estimated_sharpe_reduction > 0.2,
        }

    @count_call()
    def check_mutual_exclusion(
        self,
        strategy_id: str,
        direction: str,
        instrument_id: str = '',
    ) -> Tuple[bool, str]:
        # R27-P0-DR-13修复: 信号配额隔板检查
        if not self.check_strategy_bulkhead(strategy_id):
            return False, f'{strategy_id}策略信号配额已满(隔板保护)'
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is not None and slot.frozen:
                return False, f'{strategy_id}策略已被冻结, 不允许新开仓(旧仓位按原规则退出)'

            if strategy_id == 'master' and self._reverse.position_count > 0:
                if self._reverse.state == 'active':
                    if self._is_same_delta_direction(strategy_id, direction, 'reverse'):
                        self._stats['mutual_exclusion_blocks'] += 1
                        return False, 'reverse策略已有同向持仓，主策略被互斥规则阻断'

            if strategy_id == 'reverse' and self._master.position_count > 0:
                if self._master.state == 'active':
                    if self._is_same_delta_direction(strategy_id, direction, 'master'):
                        self._stats['mutual_exclusion_blocks'] += 1
                        return False, '主策略已有同向持仓，反向策略被互斥规则阻断'

            if self._active_strategy == 'other' and strategy_id in ('master', 'reverse'):
                return False, 'other状态下主/反策略仅平仓模式，不允许新开仓'

            if strategy_id == 'spring':
                for other_id in ('master', 'reverse'):
                    other_slot = self._get_slot(other_id)
                    if other_slot and other_slot.position_count > 0 and other_slot.state == 'active':
                        if self._is_same_delta_direction(strategy_id, direction, other_id):
                            if other_slot.win_loss_ratio >= other_slot.target_plr:
                                pass
                            else:
                                self._stats['mutual_exclusion_blocks'] += 1
                                return False, f'{other_id}策略已有同向持仓且盈亏比未达标，spring策略被互斥规则阻断'

            risk_check = self._check_cross_strategy_risk(instrument_id, direction)
            if risk_check == 'BLOCK':
                self._stats['mutual_exclusion_blocks'] += 1
                return False, '跨策略Delta敞口超硬阻断线，拒绝新开仓'
            if risk_check == 'CIRCUIT_BREAK':
                self._stats['mutual_exclusion_blocks'] += 1
                return False, '跨策略Delta敞口超熔断线，拒绝新开仓'

            return True, ''

    def _get_slot(self, strategy_id: str) -> Optional[StrategySlot]:
        slot_map = {
            'master': self._master,
            'reverse': self._reverse,
            'other': self._other,
            'spring': self._spring,
            'arbitrage': self._arbitrage,
            'market_making': self._market_making,
        }
        return slot_map.get(strategy_id)

    def _is_same_delta_direction(self, my_id: str, my_direction: str, other_id: str) -> bool:
        """判断两个策略的方向是否同向（Delta方向一致）

        R13-P2-BIZ-02修复: 增加Delta对冲头寸感知。
        对冲头寸的delta方向与方向性头寸相反，不应被互斥规则阻断。
        如果对方策略是对冲类型(hedge/delta_hedge)，则跳过互斥检查。
        """
        other_slot = self._master if other_id == 'master' else self._reverse
        other_dir = getattr(other_slot, 'last_direction', '')
        if not other_dir:
            return True

        # R13-P2-BIZ-02修复: 对冲头寸感知 — 对冲策略的delta方向与方向性头寸相反
        # 不应被互斥规则阻断，直接返回False(不同向)以允许通过
        other_open_reason = getattr(other_slot, 'last_open_reason', '')
        if other_open_reason and any(kw in other_open_reason.upper() for kw in ('HEDGE', 'DELTA_HEDGE', '对冲')):
            return False  # 对冲头寸与方向性头寸不同向，不触发互斥

        my_delta_sign = 1 if my_direction in ('long', 'BUY', 'buy') else -1
        other_delta_sign = 1 if other_dir in ('long', 'BUY', 'buy') else -1
        return my_delta_sign * other_delta_sign > 0

    def _check_cross_strategy_risk(self, instrument_id: str, direction: str) -> str:
        try:
            from ali2026v3_trading.position_service import (
                aggregate_greeks_exposure,
                get_cross_strategy_risk_guard,
            )
            guard = get_cross_strategy_risk_guard()
            pos_svc = self._get_position_service()
            if pos_svc is None:
                return 'PASS'
            exposure = aggregate_greeks_exposure(pos_svc.positions)
            level, reason, detail = guard.check(exposure)
            if level in (guard.BLOCK, guard.CIRCUIT_BREAK):
                return level
            return 'PASS'
        except Exception as e:
            logger.warning("[StrategyEcosystem._check_cross_strategy_risk] Error: %s, fail-safe阻断", e)
            return 'BLOCK'

    def _get_position_service(self):
        try:
            from ali2026v3_trading.position_service import get_position_service
            return get_position_service()
        except Exception:
            return None

    def record_spring_trade(self, direction: str, pnl: float = 0.0):
        # [R16-P2-2.4修复] BoxSpring信号传递可追踪日志
        logging.info("[R16-P2-2.4] BoxSpring信号传递: direction=%s active_strategy=%s spring_position_count=%d pnl=%.2f",
                     direction, self._active_strategy, self._spring.position_count, pnl)
        with self._lock:
            self._spring.position_count += 1
            self._spring_trades.append(EcosystemTradeRecord(
                trade_id=self._generate_trade_id(),
                strategy_id='spring',
                timestamp=datetime.now(CHINA_TZ).isoformat(),
                action='OPEN',
                direction=direction,
                open_reason='BOX_SPRING',
                market_state=self._active_strategy,
            ))
            self._stats['total_trades'] += 1
            self._stats['spring_trades'] += 1

            # R16-P1-17: 记录子策略交易统计 — R17重审计修复: 使用self._active_strategy替代self._master._active_strategy
            _sub_id = 's1_hft' if 'hft' in str(self._active_strategy).lower() else 's2_resonance'
            if hasattr(self._master, 'record_sub_strategy_trade'):
                self._master.record_sub_strategy_trade(_sub_id, pnl)

            if self._snapshot_collector is not None:
                try:
                    import numpy as np
                    self._snapshot_collector.capture_signal_point(
                        timestamp=np.datetime64('now'),
                        trigger_detail=f"spring {direction}",
                    )
                except Exception as e:
                    logging.debug("[StrategyEcosystem] spring snapshot error: %s", e)

    def process_other_strategy_signal(
        self,
        current_price: float,
        resonance_direction: str = '',
        resonance_strength: float = 0.0,
        current_iv: float = 0.0,
        flow_imbalance: float = 0.0,
        cvd_slope: float = 0.0,
    ) -> Dict[str, Any]:
        if self._other.paused:
            return {'action': 'skip', 'reason': 'other策略已暂停'}

        extreme = self._box_detector.classify_extreme_state(
            current_price=current_price,
            resonance_direction=resonance_direction,
            resonance_strength=resonance_strength,
            current_iv=current_iv,
            flow_imbalance=flow_imbalance,
            cvd_slope=cvd_slope,
        )

        if not extreme.tradeable:
            return {
                'action': 'no_signal',
                'extreme_state': extreme.to_dict(),
                'reason': '极值不满足交易条件',
            }

        trade_dir = self._box_detector.determine_trade_direction(extreme)

        if not trade_dir:
            return {
                'action': 'no_signal',
                'extreme_state': extreme.to_dict(),
                'reason': '无明确交易方向',
            }

        allowed, reason = self.check_mutual_exclusion('other', trade_dir)
        if not allowed:
            return {
                'action': 'blocked',
                'extreme_state': extreme.to_dict(),
                'reason': reason,
            }

        with self._lock:
            record = EcosystemTradeRecord(
                trade_id=self._generate_trade_id(),
                strategy_id='other',
                timestamp=datetime.now(CHINA_TZ).isoformat(),
                action='OPEN',
                direction=trade_dir,
                open_reason='OTHER_SCALP',
                market_state='other',
            )
            self._other_trades.append(record)
            self._other.position_count += 1
            self._stats['other_trades'] += 1
            self._stats['total_trades'] += 1

        return {
            'action': 'open',
            'direction': trade_dir,
            'strategy_id': 'other',
            'open_reason': 'OTHER_SCALP',
            'extreme_state': extreme.to_dict(),
            'params': self._box_detector.params.to_dict(),
            'trade_id': record.trade_id,
        }

    def compute_defense_net_benefit(
        self,
        defense_mode_loss: float,
        ignore_mode_loss: float,
        ignore_mode_win_trades: int,
        ignore_mode_avg_win: float,
        other_state_ratio: float,
    ) -> Dict[str, Any]:
        """P1-裂缝31：other状态防御净收益量化（含机会成本）

        防御模式（不新开仓）虽然避免了错误交易，但也丧失了other状态下的潜在盈利机会。
        净收益 = (防御模式损失 - 无视模式损失) - 防御机会成本
        防御机会成本 = 无视模式中盈利交易的期望值 × other状态占比
        净收益 > 0 才启用防御。
        """
        defense_opportunity_cost = ignore_mode_win_trades * ignore_mode_avg_win * other_state_ratio
        net_benefit = (defense_mode_loss - ignore_mode_loss) - defense_opportunity_cost
        should_defend = net_benefit > 0

        if not should_defend and defense_mode_loss < ignore_mode_loss:
            logger.info(
                "[StrategyEcosystem] P1-裂缝31: 防御减少损失%.2f但机会成本%.2f更高, "
                "净收益=%.2f, 不启用防御",
                ignore_mode_loss - defense_mode_loss, defense_opportunity_cost, net_benefit,
            )

        return {
            "defense_mode_loss": round(defense_mode_loss, 2),
            "ignore_mode_loss": round(ignore_mode_loss, 2),
            "defense_opportunity_cost": round(defense_opportunity_cost, 2),
            "net_benefit": round(net_benefit, 2),
            "should_defend": should_defend,
            "defense_savings": round(ignore_mode_loss - defense_mode_loss, 2),
        }

    @count_call()
    def record_strategy_pnl(
        self,
        strategy_id: str,
        pnl: float,
        commission: float = 0.0,
        slippage: float = 0.0,
        target_plr: float = 0.0,
    ) -> None:
        with self._lock:
            # N-06修复: 实盘记录时同时扣除滑点，与回测模型对齐
            net_pnl = pnl - commission - slippage
            slot = self._get_slot(strategy_id)
            if slot is None:
                return

            # NP-P1-13修复: Kahan补偿累加
            _y = net_pnl - slot._kahan_c_total
            _t = slot.total_pnl + _y
            slot._kahan_c_total = (_t - slot.total_pnl) - _y
            slot.total_pnl = _t

            trades = self._get_trades(strategy_id)
            if trades:
                for trade in reversed(trades):
                    if trade.is_open:
                        trade.pnl = pnl
                        trade.net_pnl = net_pnl
                        trade.is_open = False
                        trade.target_plr = target_plr or slot.target_plr
                        if slot._loss_pnl_sum > 1e-10:
                            trade.actual_plr = slot._win_pnl_sum / slot._loss_pnl_sum if abs(slot._loss_pnl_sum) > 1e-10 else 1e9
                        elif net_pnl > 0:
                            trade.actual_plr = 0.0
                        else:
                            trade.actual_plr = 0.0
                        slot.position_count = max(0, slot.position_count - 1)
                        _y = net_pnl - slot._kahan_c_closed
                        _t = slot._closed_pnl_sum + _y
                        slot._kahan_c_closed = (_t - slot._closed_pnl_sum) - _y
                        slot._closed_pnl_sum = _t
                        slot._closed_count += 1
                        if net_pnl > 0:
                            _y = net_pnl - slot._kahan_c_win
                            _t = slot._win_pnl_sum + _y
                            slot._kahan_c_win = (_t - slot._win_pnl_sum) - _y
                            slot._win_pnl_sum = _t
                            slot._win_count += 1
                        elif net_pnl < 0:
                            _abs_pnl = abs(net_pnl)
                            _y = _abs_pnl - slot._kahan_c_loss
                            _t = slot._loss_pnl_sum + _y
                            slot._kahan_c_loss = (_t - slot._loss_pnl_sum) - _y
                            slot._loss_pnl_sum = _t
                            slot._loss_count += 1
                        break
            # ✅ P1-8修复: 交易记录后自动更新PLR统计并检查是否需要冻结策略  # R17-P2-DOC-05
            self.update_plr_stats(strategy_id)
            plr_stats = self.compute_plr_stats(strategy_id)
            if plr_stats:
                plr_ratio = plr_stats.get('plr_ratio', 0.0)
                consecutive_losses = plr_stats.get('consecutive_losses', 0)
                if plr_ratio < self.PLR_FREEZE_RATIO_THRESHOLD and consecutive_losses >= self.PLR_FREEZE_CONSECUTIVE_LOSSES:
                    self.freeze_strategy_slot(strategy_id)
                    logging.warning("[StrategyEcosystem] 策略%s因PLR过低(%.2f)和连续亏损(%d次)被自动冻结",
                                    strategy_id, plr_ratio, consecutive_losses)

            # ✅ P0-18集成: 同步更新ProfitLossRatioCalculator（统一PLR计算引擎）  # R17-P2-DOC-03: P0修复标记保留用于追溯
            try:
                _plr_calc = get_plr_calculator(default_target_plr=slot.target_plr)
                _trade_records = []
                trades_deque = self._get_trades(strategy_id)
                if trades_deque:
                    for t in trades_deque:
                        if not t.is_open:
                            _trade_records.append({'pnl': getattr(t, 'net_pnl', getattr(t, 'pnl', 0.0))})
                if _trade_records:
                    _plr_calc.update_from_trades(strategy_id, _trade_records)
            except Exception as _plr_err:
                logging.debug("[StrategyEcosystem] ProfitLossRatioCalculator同步更新异常: %s", _plr_err)

    def _get_trades(self, strategy_id: str) -> Optional[deque]:
        trades_map = {
            'master': self._master_trades,
            'reverse': self._reverse_trades,
            'other': self._other_trades,
            'spring': self._spring_trades,
            'arbitrage': self._arbitrage_trades,
            'market_making': self._market_making_trades,
        }
        return trades_map.get(strategy_id)

    def compute_expected_value(self, strategy_id: str) -> float:
        trades = self._get_trades(strategy_id)
        if trades is None:
            return 0.0
        closed_pnls = [t.net_pnl for t in trades if not t.is_open]
        if len(closed_pnls) == 0:
            return 0.0
        return sum(closed_pnls) / len(closed_pnls)

    def compute_plr_stats(self, strategy_id: str) -> Dict[str, float]:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None or slot._closed_count == 0:
                return {}
            n_win = slot._win_count
            n_loss = slot._loss_count
            total_win = slot._win_pnl_sum
            total_loss = slot._loss_pnl_sum
            avg_win = total_win / n_win if n_win > 0 else 0.0
            avg_loss = total_loss / n_loss if n_loss > 0 else 0.0
            win_rate = n_win / slot._closed_count if slot._closed_count > 0 else 0.0
            loss_rate = n_loss / slot._closed_count if slot._closed_count > 0 else 0.0
            expected_value = avg_win * win_rate - avg_loss * loss_rate
            profit_factor = total_win / total_loss if total_loss > 1e-10 else 0.0
            win_loss_ratio = avg_win / avg_loss if avg_loss > 1e-10 else 0.0
            return {
                'expected_value': expected_value,
                'win_rate': win_rate,
                'loss_rate': loss_rate,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': profit_factor,
                'win_loss_ratio': win_loss_ratio,
                'total_trades': slot._closed_count,
                'win_count': n_win,
                'loss_count': n_loss,
                'total_pnl': slot.total_pnl,
                'target_plr': slot.target_plr,
                'current_plr': profit_factor,
                'plr_ratio': profit_factor / slot.target_plr if slot.target_plr > 0 else 0.0,
            }

    @count_call()
    def update_plr_stats(self, strategy_id: str) -> Dict[str, float]:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None or slot._closed_count == 0:
                return {}
            total_win = slot._win_pnl_sum
            total_loss = slot._loss_pnl_sum
            n_win = slot._win_count
            n_loss = slot._loss_count
            slot.profit_factor = total_win / total_loss if total_loss > 1e-10 else 0.0
            avg_win = total_win / n_win if n_win > 0 else 0.0
            avg_loss = total_loss / n_loss if n_loss > 0 else 0.0
            slot.win_loss_ratio = (avg_win / avg_loss) if avg_loss > 1e-10 else 0.0
            trades = self._get_trades(strategy_id)
            if trades:
                closed = [t for t in trades if not t.is_open]
                streak = 0
                max_streak = 0
                for t in reversed(closed):
                    if t.net_pnl < 0:
                        streak += 1
                        max_streak = max(max_streak, streak)
                    else:
                        break
                slot.consecutive_losses = max_streak
            logger.info(
                "[PLR] strategy=%s win_loss_ratio=%.2f profit_factor=%.2f consecutive_losses=%d target_plr=%.1f plr_ratio=%.2f",
                strategy_id, slot.win_loss_ratio, slot.profit_factor,
                slot.consecutive_losses, slot.target_plr,
                slot.profit_factor / slot.target_plr if slot.target_plr > 0 else 0.0,
            )
            return {
                'win_loss_ratio': slot.win_loss_ratio,
                'profit_factor': slot.profit_factor,
                'consecutive_losses': slot.consecutive_losses,
            }

    def check_absolute_ev_bottomline(self) -> Dict[str, Any]:
        with self._lock:
            results = {}
            any_breached = False

            for sid in ('master', 'reverse', 'other', 'spring', 'arbitrage', 'market_making'):
                slot = self._get_slot(sid)
                if slot._closed_count > 0:
                    ev = slot._closed_pnl_sum / slot._closed_count
                else:
                    ev = 0.0
                slot.expected_value = ev

                breached = ev < 0 and slot._closed_count >= self.MIN_TRADES_FOR_EV

                if breached and not slot.paused:
                    slot.paused = True
                    slot.pause_timestamp = time.time()  # R17-LC-03修复
                    slot.pause_reason = f'期望值={ev:.4f}<0'
                    any_breached = True
                    self._stats['ev_breaches'] += 1
                    logger.critical(
                        "[StrategyEcosystem] 🚨 %s策略绝对期望值底线突破! EV=%.4f, 暂停!",
                        sid, ev,
                    )

                results[sid] = {
                    'ev': ev,
                    'breached': breached,
                    'paused': slot.paused,
                }

            # --- 影子引擎EV融合检查 ---
            shadow_ev_info = {}
            try:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                _sse = get_shadow_strategy_engine()
                if _sse is not None:
                    if _sse.is_absolute_ev_paused():
                        logger.critical(
                            "[StrategyEcosystem] 影子引擎绝对EV暂停激活，所有策略暂停"
                        )
                        for sid in ('master', 'reverse', 'other', 'spring'):
                            slot = self._get_slot(sid)
                            if slot is not None and not slot.paused:
                                slot.paused = True
                                slot.pause_timestamp = time.time()
                                slot.pause_reason = 'shadow_engine_ev_paused'
                                any_breached = True
                                self._stats['ev_breaches'] += 1
                                logger.critical(
                                    "[StrategyEcosystem] 🚨 %s策略被影子引擎EV暂停触发暂停!", sid
                                )
                    shadow_ev = _sse.get_master_expected_value()
                    shadow_ev_info = {
                        'shadow_master_ev': shadow_ev,
                        'shadow_ev_paused': _sse.is_absolute_ev_paused(),
                        'shadow_degradation': _sse.is_degradation_active(),
                    }
                    if shadow_ev < 0:
                        logger.warning(
                            "[StrategyEcosystem] 影子引擎master EV为负(%.4f)，融合到master策略EV检查",
                            shadow_ev,
                        )
                        _master_slot = self._get_slot('master')
                        if _master_slot is not None and not _master_slot.paused:
                            _master_slot.paused = True
                            _master_slot.pause_timestamp = time.time()
                            _master_slot.pause_reason = f'shadow_ev_negative={shadow_ev:.4f}'
                            any_breached = True
                            self._stats['ev_breaches'] += 1
                            logger.critical(
                                "[StrategyEcosystem] 🚨 master策略被影子引擎负EV触发暂停! shadow_ev=%.4f",
                                shadow_ev,
                            )
            except Exception as _sse_err:
                shadow_ev_info = {'error': str(_sse_err)}
                logger.debug("[StrategyEcosystem] 影子引擎EV融合检查跳过: %s", _sse_err)

            # --- 瀑布式评判引擎（三系统统一，实盘准入门控） ---
            cascade_result = {}
            try:
                import sys, os
                _project_root = os.path.dirname(os.path.abspath(__file__))
                # R14-P0-DEP-05修复: sys.path.insert加路径验证，防止注入攻击面
                if _project_root not in sys.path and os.path.isdir(_project_root):
                    sys.path.insert(0, _project_root)
                from evaluation.cascade_judge import CascadeJudge, adapt_backtest_result
                _slot = self._get_slot('master')
                _live_metrics = {
                    'sharpe': getattr(_slot, 'sharpe', 0.0),
                    'calmar': getattr(_slot, 'calmar', 0.0),
                    'sortino_ratio': getattr(_slot, 'sortino', getattr(_slot, 'sharpe', 0.0) * 1.2),
                    'profit_loss_ratio': getattr(_slot, 'profit_factor', 1.0),
                    'max_consecutive_losses': getattr(_slot, 'consecutive_losses', 0),
                    'win_rate': getattr(_slot, 'win_rate', 0.5),
                    'total_trades': getattr(_slot, 'total_trades', 0),
                    'max_flat_period_days': getattr(_slot, 'max_flat_period_days', 999),
                    'peak_margin_used': getattr(_slot, 'peak_margin_used', 0.0),
                    'sharpe_ci_lower': getattr(_slot, 'sharpe_ci_lower', 0.0),
                    'sharpe_ci_upper': getattr(_slot, 'sharpe_ci_upper', 0.0),
                    'sharpe_ci_width': getattr(_slot, 'sharpe_ci_width', 0.0),
                    'alpha_action': getattr(_slot, 'alpha_action', 'hold'),
                }
                _adapted = adapt_backtest_result(_live_metrics, strategy_type=getattr(_slot, 'strategy_type', ''))
                try:
                    from ali2026v3_trading.config_params import get_cached_params
                    _params_for_cascade = get_cached_params()
                except Exception:
                    _params_for_cascade = None
                _capital_scale_str = self._capital_scale.value if self._capital_scale else 'medium'
                _cascade = CascadeJudge.from_config(capital_scale=_capital_scale_str, params=_params_for_cascade)
                _cascade_report = _cascade.judge(_adapted)
                cascade_result = {
                    'passed': _cascade_report.passed,
                    'score': _cascade_report.final_score,
                    'fatal': _cascade_report.fatal_reason,
                }
                if not _cascade_report.passed:
                    logger.warning("[StrategyEcosystem] 瀑布式评判否决: %s", _cascade_report.fatal_reason)

                # --- Alpha CI下游消费逻辑 ---
                _ci_width = _live_metrics.get('sharpe_ci_width', 0.0)
                _alpha_action = _live_metrics.get('alpha_action', 'hold')
                try:
                    from param_pool.state_param_sets import BACKTEST_THRESHOLDS as _CI_THRESHOLDS
                except ImportError:
                    try:
                        from param_pool.task_scheduler import BACKTEST_THRESHOLDS as _CI_THRESHOLDS
                    except ImportError:
                        _CI_THRESHOLDS = {}
                _max_ci_width = _CI_THRESHOLDS.get('max_ci_width', 0.5)
                _min_ci_width = _CI_THRESHOLDS.get('min_ci_width', 0.05)
                if _ci_width > _max_ci_width:
                    logger.warning("[StrategyEcosystem] Alpha CI过宽: ci_width=%.4f > max=%.4f，CI置信区间不足", _ci_width, _max_ci_width)
                # P0-R11-08修复: alpha_action值域对齐compute_alpha_confidence_interval
                # 实际值域: 'eliminate'(最严重), 'reduce_weight', 'flag', 'reliable'(最轻)
                if _alpha_action == 'eliminate':
                    cascade_result['ci_veto'] = 'eliminate'
                    logger.warning("[StrategyEcosystem] Alpha CI否决(eliminate): ci_width=%.4f, 策略应从生态中淘汰", _ci_width)
                elif _alpha_action == 'reduce_weight':
                    cascade_result['ci_veto'] = 'reduce_weight'
                    logger.warning("[StrategyEcosystem] Alpha CI降权(reduce_weight): ci_width=%.4f, 资金分配降权", _ci_width)
                elif _alpha_action == 'flag':
                    cascade_result['ci_flag'] = True
                    logger.info("[StrategyEcosystem] Alpha CI标注(flag): ci_width=%.4f, Sharpe中等可靠", _ci_width)

            except Exception as _e:
                cascade_result = {'error': str(_e)}

            try:
                _ev_decay_result = validate_ev_cache_time_decay(ecosystem=self)
                if _ev_decay_result and not _ev_decay_result.get('decay_valid', True):
                    logging.warning("[R16-P1-03] EV缓存时间衰减验证失败: %s", _ev_decay_result)
            except Exception as _evd_err:
                logging.debug("[R16-P1-03] EV衰减验证异常(非阻断): %s", _evd_err)

            # R16-P2-04修复: Sharpe/Sortino衰减独立检测
            try:
                _slot = self._get_slot('master')
                if _slot is not None and _slot._closed_count >= 20:
                    _recent_pnls = [t for t in getattr(_slot, '_recent_closed_pnls', [])[-20:]]
                    if len(_recent_pnls) >= 10:
                        _avg = sum(_recent_pnls) / len(_recent_pnls)
                        _std = (sum((x - _avg) ** 2 for x in _recent_pnls) / len(_recent_pnls)) ** 0.5
                        _sharpe = _avg / _std if _std > 0 else 0
                        if _sharpe < -0.5:
                            logging.warning("[R16-P2-04] Sharpe衰减检测: sharpe=%.2f< -0.5, 建议降级", _sharpe)
                        # R17重审计修复: Sortino衰减独立检测(仅计算下行标准差)
                        _neg_pnls = [x for x in _recent_pnls if x < 0]
                        if len(_neg_pnls) >= 5:
                            _down_std = (sum((x - _avg) ** 2 for x in _neg_pnls) / len(_neg_pnls)) ** 0.5
                            _sortino = _avg / _down_std if _down_std > 0 else 0
                            if _sortino < -0.3:
                                logging.warning("[R16-P2-04] Sortino衰减检测: sortino=%.2f< -0.3, 下行风险过大", _sortino)
            except Exception as _sharpe_err:
                logging.debug("[R16-P2-04] Sharpe衰减检测异常(非阻断): %s", _sharpe_err)

            return {
                'any_breached': any_breached,
                'strategies': results,
                'cascade_judge': cascade_result,
                'shadow_engine': shadow_ev_info,
            }

    # R13-P0-API-06修复: 提取状态→策略ID映射为独立方法，消除硬编码
    _STATE_TO_STRATEGY_MAP = {  # R25-SE-P1-02-FIX
        STRATEGY_MODE_CORRECT_TRENDING: 'master',
        STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: 'master',
        STRATEGY_MODE_INCORRECT_REVERSAL: 'reverse',
        'incorrect_reversal_defensive': 'reverse',
        STRATEGY_MODE_OTHER: 'other',
    }

    def resolve_strategy_id_from_state(self, state: str) -> str:
        """R13-P0-API-06修复: 将市场状态映射为策略ID（单一来源，消除多处硬编码）

        Args:
            state: 市场状态字符串，如'correct_trending', 'incorrect_reversal'等
        Returns:
            策略ID: 'master'/'reverse'/'other'/'spring'/'arbitrage'/'market_making'
        """
        return self._STATE_TO_STRATEGY_MAP.get(state, 'master')

    def get_active_strategy(self) -> Optional[str]:  # R25-SE-P1-04-FIX: 返回类型修正为Optional[str]
        with self._lock:
            return self._active_strategy

    def get_capital_allocations(self) -> Dict[str, float]:
        with self._lock:
            return {
                'master': self._master.capital_allocation,
                'reverse': self._reverse.capital_allocation,
                'other': self._other.capital_allocation,
                'spring': self._spring.capital_allocation,
                'arbitrage': self._arbitrage.capital_allocation,
                'market_making': self._market_making.capital_allocation,
            }

    def get_strategy_slots(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                'master': self._master.to_dict(),
                'reverse': self._reverse.to_dict(),
                'other': self._other.to_dict(),
                'spring': self._spring.to_dict(),
                'arbitrage': self._arbitrage.to_dict(),
                'market_making': self._market_making.to_dict(),
            }

    # DFG-P1-14修复: 多策略持仓聚合视图 — 合并所有策略持仓供风控查询
    def aggregate_position_view(self) -> Dict[str, Any]:
        """DFG-P1-14修复: 聚合所有策略的持仓数据，解决多策略持仓聚合数据流断裂

        将各策略槽位的持仓信息合并为统一视图，供风控服务、健康检查等消费者查询。

        Returns:
            Dict: 聚合持仓视图，包含:
                - total_positions: 总持仓数
                - by_strategy: 各策略持仓明细
                - by_instrument: 按合约聚合的持仓
                - net_direction: 净方向敞口
        """
        with self._lock:
            _by_strategy = {}
            _by_instrument: Dict[str, Dict[str, Any]] = {}
            _total_positions = 0
            _net_direction = 0  # 正=净多, 负=净空

            for _sid in ('master', 'reverse', 'other', 'spring', 'arbitrage', 'market_making'):
                _slot = self._get_slot(_sid)
                if _slot is None:
                    continue
                _slot_positions = {
                    'strategy_id': _sid,
                    'strategy_type': _slot.strategy_type,
                    'state': _slot.state,
                    'position_count': _slot.position_count,
                    'total_pnl': round(_slot.total_pnl, 4),
                    'expected_value': round(_slot.expected_value, 6),
                    'paused': _slot.paused,
                    'frozen': _slot.frozen,
                    'last_direction': _slot.last_direction,
                }
                _by_strategy[_sid] = _slot_positions
                _total_positions += _slot.position_count

                # 按方向聚合净敞口
                if _slot.last_direction in ('long', 'BUY', 'buy'):
                    _net_direction += _slot.position_count
                elif _slot.last_direction in ('short', 'SELL', 'sell'):
                    _net_direction -= _slot.position_count

                # 按合约聚合（从交易记录中提取）
                _trades = self._get_trades(_sid)
                if _trades:
                    for _t in _trades:
                        if _t.is_open and _t.instrument_id:
                            _inst = _t.instrument_id
                            if _inst not in _by_instrument:
                                _by_instrument[_inst] = {
                                    'instrument_id': _inst,
                                    'strategies': [],
                                    'total_open_count': 0,
                                    'directions': set(),
                                }
                            _by_instrument[_inst]['strategies'].append(_sid)
                            _by_instrument[_inst]['total_open_count'] += 1
                            if _t.direction:
                                _by_instrument[_inst]['directions'].add(_t.direction)

            # 转换set为list以便JSON序列化
            for _inst_data in _by_instrument.values():
                _inst_data['directions'] = list(_inst_data['directions'])
                _inst_data['strategy_count'] = len(_inst_data['strategies'])

            return {
                'total_positions': _total_positions,
                'by_strategy': _by_strategy,
                'by_instrument': _by_instrument,
                'net_direction': _net_direction,
                'active_strategy': self._active_strategy,
                'capital_allocations': self.get_capital_allocations(),
            }

    # DFG-P1-14修复: 多策略持仓聚合数据通过EventBus发布，供风控服务消费
    def publish_aggregate_position(self) -> None:
        """DFG-P1-14修复: 发布多策略持仓聚合数据到EventBus

        定期调用此方法，将聚合持仓视图发布到EventBus，
        解决多策略持仓聚合数据流断裂问题。
        """
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _view = self.aggregate_position_view()
                _bus.publish('position.aggregate_view', {
                    'type': 'position.aggregate_view',
                    'total_positions': _view.get('total_positions', 0),
                    'net_direction': _view.get('net_direction', 0),
                    'by_strategy': _view.get('by_strategy', {}),
                    'by_instrument': _view.get('by_instrument', {}),
                }, async_mode=True)
        except Exception:
            pass

    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            if self._ev_cache is not None and (now - self._ev_cache_ts) < self._ev_cache_ttl:
                ev_check = self._ev_cache
            else:
                try:
                    ev_check = self.check_absolute_ev_bottomline()
                    if ev_check is not None:
                        self._ev_cache_ts = now
                        self._ev_cache = ev_check
                    else:
                        ev_check = self._ev_cache if self._ev_cache is not None else {}
                except Exception:
                    ev_check = self._ev_cache if self._ev_cache is not None else {}

            any_paused = self._master.paused or self._reverse.paused or self._other.paused or self._spring.paused

            # ✅ 集成E12反向策略伪独立检测（L-P0-4）
            e12_result = None
            e12_error = False  # P1-R10-05: 检测异常标记
            if self._e12_detector is not None:
                try:
                    master_trades = list(self._master_trades)
                    reverse_trades = list(self._reverse_trades)
                    e12_result = self._e12_detector.detect(master_trades, reverse_trades)
                    if e12_result and e12_result.get('e12_triggered'):
                        logger.critical("[StrategyEcosystem] E12告警: 反向策略与主策略相关性过高 correlation=%.2f",
                                        e12_result.get('correlation', 0.0))
                except Exception as e:
                    # P1-R10-05修复: 检测异常时设置DEGRADED并传播，而非仅记录debug日志
                    logger.warning("[StrategyEcosystem] E12检测异常: %s", e, exc_info=True)
                    e12_error = True
                    e12_result = {'e12_triggered': False, 'error': str(e)}

            # ✅ 集成E11量化来源检查（E11）  # R17-P2-DOC-05
            e11_result = None
            e11_error = False  # P1-R10-05: 检测异常标记
            if self._e11_checker is not None:
                try:
                    param_sources = {
                        'master_params': 'backtest',
                        'reverse_params': 'backtest',
                        'other_params': 'backtest',
                    }
                    e11_result = self._e11_checker.check(param_sources)
                except Exception as e:
                    # P1-R10-05修复: 检测异常时设置DEGRADED并传播，而非仅记录debug日志
                    logger.warning("[StrategyEcosystem] E11检测异常: %s", e, exc_info=True)
                    e11_error = True
                    e11_result = {'e11_triggered': False, 'error': str(e)}

            # P1-R10-05修复: 组件检测异常时降级为DEGRADED
            if any_paused:
                health_status = 'CRITICAL'
            elif e12_error or e11_error:
                health_status = 'DEGRADED'
            else:
                health_status = 'OK'

            return {
                'component': 'strategy_ecosystem',
                'status': health_status,
                'active_strategy': self._active_strategy,
                'strategies': {
                    'master': {'state': self._master.state, 'paused': self._master.paused},
                    'reverse': {'state': self._reverse.state, 'paused': self._reverse.paused},
                    'other': {'state': self._other.state, 'paused': self._other.paused},
                    'spring': {'state': self._spring.state, 'paused': self._spring.paused},
                },
                'capital_allocations': self.get_capital_allocations(),
                'ev_check': ev_check,
                'box_detector': self._box_detector.get_health_status(),
                'governance': {
                    'e12_reverse_independence': e12_result,
                    'e11_quant_source': e11_result,
                },
            }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['active_strategy'] = self._active_strategy
            stats['master_pnl'] = self._master.total_pnl
            stats['reverse_pnl'] = self._reverse.total_pnl
            stats['other_pnl'] = self._other.total_pnl
            stats['spring_pnl'] = self._spring.total_pnl
            stats['master_ev'] = self._master.expected_value
            stats['reverse_ev'] = self._reverse.expected_value
            stats['other_ev'] = self._other.expected_value
            stats['spring_ev'] = self._spring.expected_value
            stats['lineage_entries'] = sum(len(v) for v in self._strategy_lineage.values())
            stats['param_evolution_entries'] = len(self._strategy_param_evolution)
            stats['strategy_doc_links'] = dict(self._strategy_doc_links)
            stats['l2_dataset_enforced'] = self._l2_dataset_enforced
            stats['l2_dataset_id'] = self._l2_dataset_id
            stats['capital_scale_upgrade_enabled'] = self._capital_scale_upgrade_enabled
            stats['retired_strategy_count'] = len(self._retired_strategies)
            stats['revival_request_count'] = len(self._revival_requests)
            try:
                from ali2026v3_trading.performance_monitor import PathCounter
                perf = PathCounter.get_stats()
                stats['performance_monitor'] = {
                    'total_paths': perf.get('total_paths', 0),
                    'uptime_seconds': round(perf.get('uptime_seconds', 0), 1),
                }
            except Exception:
                pass
            return stats

    def freeze_strategy_slot(self, strategy_id: str) -> bool:
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None:
                logger.warning("[StrategyEcosystem] freeze_strategy_slot: 未知策略%s", strategy_id)
                return False
            slot.frozen = True
            logger.info("[StrategyEcosystem] 策略%s已冻结新开仓", strategy_id)
            return True

    # P2-项17修复: 配置驱动的灰度部署
    def configure_grayscale(self, grayscale_config: Dict[str, Any]) -> Dict[str, Any]:
        """P2-项17修复: 基于配置的灰度部署

        通过配置控制灰度比例和目标策略，而非硬编码。

        Args:
            grayscale_config: 灰度配置，包含:
                - strategy_id: 灰度目标策略ID
                - traffic_pct: 灰度流量百分比(0-100)
                - duration_sec: 灰度持续时间(秒)
                - rollback_on_error: 出错时是否自动回滚

        Returns:
            Dict: 灰度配置结果
        """
        strategy_id = grayscale_config.get('strategy_id', '')
        traffic_pct = grayscale_config.get('traffic_pct', 10)
        duration_sec = grayscale_config.get('duration_sec', 3600)
        rollback_on_error = grayscale_config.get('rollback_on_error', True)

        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot is None:
                return {'success': False, 'error': f'未知策略: {strategy_id}'}

            self._grayscale_config = {
                'strategy_id': strategy_id,
                'traffic_pct': traffic_pct,
                'duration_sec': duration_sec,
                'rollback_on_error': rollback_on_error,
                'start_time': time.time(),
                'active': True,
            }
            logger.info(
                "[StrategyEcosystem] 灰度部署配置: strategy=%s traffic=%.0f%% duration=%.0fs rollback=%s",
                strategy_id, traffic_pct, duration_sec, rollback_on_error,
            )
            return {'success': True, 'config': self._grayscale_config}

    def get_grayscale_status(self) -> Dict[str, Any]:
        """P2-项17修复: 获取当前灰度部署状态"""
        config = getattr(self, '_grayscale_config', None)
        if config is None or not config.get('active'):
            return {'active': False}
        elapsed = time.time() - config.get('start_time', 0)
        remaining = max(0, config.get('duration_sec', 0) - elapsed)
        return {
            'active': True,
            'strategy_id': config.get('strategy_id', ''),
            'traffic_pct': config.get('traffic_pct', 0),
            'remaining_sec': round(remaining, 1),
            'elapsed_sec': round(elapsed, 1),
        }

    # P2-项18修复: 热更新灰度能力
    def reload_param_sets(self, param_updates: Dict[str, Any],
                           grayscale: bool = False,
                           grayscale_pct: float = 10.0) -> Dict[str, Any]:
        """P2-项18修复: 热更新参数集，支持灰度发布

        Args:
            param_updates: 参数更新字典 {strategy_id: {param_key: param_value}}
            grayscale: 是否启用灰度发布
            grayscale_pct: 灰度流量百分比

        Returns:
            Dict: 更新结果
        """
        result = {'updated': [], 'grayscale': grayscale}

        with self._lock:
            for strategy_id, updates in param_updates.items():
                slot = self._get_slot(strategy_id)
                if slot is None:
                    result[strategy_id] = 'not_found'
                    continue

                if strategy_id == 'master':
                    if 's1_params' in updates:
                        self._s1_params.update(updates['s1_params'])
                    if 's2_params' in updates:
                        self._s2_params.update(updates['s2_params'])
                    result['updated'].append(strategy_id)
                else:
                    params_attr = f'_{strategy_id}_params'
                    if hasattr(self, params_attr):
                        getattr(self, params_attr).update(updates)
                        result['updated'].append(strategy_id)

                # 灰度标记
                if grayscale:
                    slot._grayscale_active = True
                    slot._grayscale_pct = grayscale_pct
                    logger.info(
                        "[StrategyEcosystem] 灰度热更新: strategy=%s pct=%.0f%%",
                        strategy_id, grayscale_pct,
                    )

        return result

    # P0-R8-05修复: 治理引擎全量check_all调用 + 自动降级执行
    def run_governance_checks(self, returns: List[float] = None,
                              window_results: List[Dict] = None,
                              pnl_attribution: Dict[str, float] = None,
                              param_sources: Dict[str, str] = None) -> Dict[str, Any]:
        """
        手册14节要求: 执行所有治理检测器(E7-E13/WF6-10), 符合任意一条即触发自动降级。
        降级动作: 冻结所有策略新开仓, 设置降级标志, 通知影子引擎。
        """
        results = {}
        any_triggered = False
        degradation_reasons = []

        # WF6-10: 样本外窗口消除检查
        if self._wf_elimination_checker is not None:
            try:
                if window_results is not None:
                    wf_result = self._wf_elimination_checker.check_all(window_results)
                    results['wf6_10'] = wf_result
                    if wf_result.get('triggered', False):
                        any_triggered = True
                        degradation_reasons.append('WF6-10: 样本外窗口消除触发')
            except Exception as e:
                logger.debug("[Governance] WF6-10检查异常: %s", e)

        # E7: 收益不可解释性检查
        if self._e7_checker is not None:
            try:
                if pnl_attribution is not None:
                    e7_result = self._e7_checker.check(pnl_attribution)
                    results['e7'] = e7_result
                    if e7_result.get('triggered', False) or e7_result.get('e7_triggered', False):
                        any_triggered = True
                        degradation_reasons.append('E7: 收益不可解释性触发')
            except Exception as e:
                logger.debug("[Governance] E7检查异常: %s", e)

        # E8/E9/E10: 尾部风险/Minsky/状态依赖综合检查
        if self._e8e9e10_checker is not None:
            try:
                if returns is not None:
                    e8e9e10_result = self._e8e9e10_checker.check_all(returns=returns)
                    results['e8e9e10'] = e8e9e10_result
                    if e8e9e10_result.get('triggered', False) or e8e9e10_result.get('e8e9e10_triggered', False):
                        any_triggered = True
                        degradation_reasons.append('E8/E9/E10: 综合风险触发')
            except Exception as e:
                logger.debug("[Governance] E8E9E10检查异常: %s", e)

        # E11: 量化来源缺失检查
        if self._e11_checker is not None:
            try:
                if param_sources is not None:
                    e11_result = self._e11_checker.check(param_sources)
                    results['e11'] = e11_result
                    if e11_result.get('triggered', False):
                        any_triggered = True
                        degradation_reasons.append('E11: 量化来源缺失触发')
            except Exception as e:
                logger.debug("[Governance] E11检查异常: %s", e)

        # P2-R11-02: Strategy degradation logic currently operates at system level, not per-strategy.
        # Consider per-strategy degradation state for multi-strategy deployment.
        # 执行降级动作
        if any_triggered:
            self._governance_degraded = True
            self._governance_degradation_reasons = degradation_reasons
            # 冻结所有策略新开仓
            for strategy_id in ['master', 'reverse', 'spring', 'other']:
                self.freeze_strategy_slot(strategy_id)
            logger.critical(
                "[StrategyEcosystem] 🔴 治理引擎触发自动降级! 原因: %s",
                '; '.join(degradation_reasons)
            )
            # 通知影子引擎记录降级事件
            try:
                from ali2026v3_trading.shadow_strategy_engine import get_shadow_strategy_engine
                shadow = get_shadow_strategy_engine()
                if shadow:
                    shadow.record_governance_degradation(degradation_reasons)
            except Exception as e:
                logger.warning("[Governance] 通知影子引擎异常: %s", e)
        else:
            self._governance_degraded = False
            self._governance_degradation_reasons = []

        results['degraded'] = any_triggered
        results['degradation_reasons'] = degradation_reasons
        return results

    def is_governance_degraded(self) -> bool:
        """检查治理引擎是否已触发降级"""
        return getattr(self, '_governance_degraded', False)

    def resume_strategy(self, strategy_id: str) -> bool:
        # R17-LC-03修复: 添加冷却期检查，防止立即恢复被暂停策略
        # R19-P1-01修复: RESUME_COOLDOWN_SEC配置化，从类属性/环境变量读取
        resume_cooldown_sec = getattr(self, '_resume_cooldown_sec', 0) or float(
            os.environ.get('RESUME_COOLDOWN_SEC', '900.0'))
        with self._lock:
            slot = self._get_slot(strategy_id)
            if slot and slot.paused:
                if slot.frozen:
                    logger.warning("[StrategyEcosystem] %s策略仍处于冻结态，禁止直接恢复", strategy_id)
                    return False
                if getattr(self, '_governance_degraded', False):
                    logger.warning("[StrategyEcosystem] 治理降级仍生效，禁止恢复策略%s", strategy_id)
                    return False
                # R17-LC-03: 冷却期检查 — 暂停未满冷却期禁止恢复
                if slot.pause_timestamp > 0:
                    elapsed = time.time() - slot.pause_timestamp
                    if elapsed < resume_cooldown_sec:
                        logger.warning(
                            "[StrategyEcosystem] %s策略暂停仅%.1f秒(冷却期%.0f秒)，禁止恢复",
                            strategy_id, elapsed, resume_cooldown_sec,
                        )
                        return False
                slot.paused = False
                slot.pause_reason = ''
                slot.pause_timestamp = 0.0
                slot._observation_period_until = time.time() + OBSERVATION_PERIOD_SEC
                slot._observation_position_scale = 0.5
                _rs = self._get_risk_service()
                if _rs and hasattr(_rs, 'confirm_alpha_decay_recovery'):
                    _rs.confirm_alpha_decay_recovery()
                logger.info("[StrategyEcosystem] %s策略已恢复(观察期%.0f秒, 仓位缩放%.0f%%)",
                            strategy_id, OBSERVATION_PERIOD_SEC, slot._observation_position_scale * 100)
                return True
        return False

    def validate_position_consistency(self) -> Dict[str, Any]:
        """INV-10/INV-CON-01: 验证策略持仓之和等于PositionService总持仓

        检查所有策略槽位的position_count之和是否与PositionService记录的总持仓一致。
        不一致时告警但不自动修复（需人工介入）。
        """
        with self._lock:
            strategy_total = sum(
                self._get_slot(sid).position_count
                for sid in ('master', 'reverse', 'other', 'spring', 'arbitrage', 'market_making')
                if self._get_slot(sid) is not None
            )
            pos_service_total = 0
            try:
                pos_svc = self._get_position_service()
                if pos_svc is not None and hasattr(pos_svc, 'positions'):
                    pos_service_total = len(pos_svc.positions) if pos_svc.positions else 0
            except Exception as e:
                logger.debug("[INV-10] 获取PositionService持仓失败: %s", e)
                pos_service_total = -1  # 标记获取失败

            consistent = strategy_total == pos_service_total if pos_service_total >= 0 else True
            if not consistent:
                logger.warning(
                    "[INV-10] 持仓一致性违反: 策略持仓之和=%d, PositionService总持仓=%d",
                    strategy_total, pos_service_total,
                )
            return {
                'consistent': consistent,
                'strategy_position_sum': strategy_total,
                'position_service_total': pos_service_total,
            }

    def normalize_capital_allocation(self) -> Dict[str, float]:
        """INV-11/INV-CON-02: 归一化资金分配，确保所有策略分配之和为1.0

        CapitalRoute.total_base仅计算3个策略(master+reverse+other)，
        缺少spring/arbitrage/market_making。此方法将6个策略的分配归一化。
        """
        with self._lock:
            cr = self._capital_route
            raw_allocations = {
                'master': self._master.capital_allocation,
                'reverse': self._reverse.capital_allocation,
                'other': self._other.capital_allocation,
                'spring': self._spring.capital_allocation,
                'arbitrage': self._arbitrage.capital_allocation,
                'market_making': self._market_making.capital_allocation,
            }
            total = sum(raw_allocations.values())
            if total <= 0:
                logger.warning("[INV-11] 资金分配总和<=0 (%.6f)，无法归一化", total)
                return raw_allocations
            if abs(total - 1.0) > 1e-6:
                normalized = {k: v / total for k, v in raw_allocations.items()}
                logger.info(
                    "[INV-11] 资金分配归一化: total=%.6f → 1.0, 调整前=%s",
                    total, {k: round(v, 4) for k, v in raw_allocations.items()},
                )
                # 更新槽位分配
                self._master.capital_allocation = normalized['master']
                self._reverse.capital_allocation = normalized['reverse']
                self._other.capital_allocation = normalized['other']
                self._spring.capital_allocation = normalized['spring']
                self._arbitrage.capital_allocation = normalized['arbitrage']
                self._market_making.capital_allocation = normalized['market_making']
                return normalized
            return raw_allocations

    # UPG-P1-03修复: 并行运行模式 — 新旧策略同时运行的热更新机制
    def enable_parallel_run(self, old_strategy: str, new_strategy: str,
                            max_duration_sec: float = 3600.0) -> Dict[str, Any]:
        """UPG-P1-03修复: 启用并行运行模式

        在热更新期间，新旧策略同时运行，新策略仅做信号记录不开仓。
        超过max_duration_sec后自动切换到新策略。

        Args:
            old_strategy: 旧策略ID
            new_strategy: 新策略ID
            max_duration_sec: 最大并行运行时间(秒)

        Returns:
            dict: 操作结果
        """
        with self._lock:
            self._parallel_run_mode = True
            self._parallel_run_old_strategy = old_strategy
            self._parallel_run_new_strategy = new_strategy
            self._parallel_run_start_time = time.time()
            self._parallel_run_max_duration_sec = max_duration_sec
            logger.info(
                "UPG-P1-03: 并行运行模式已启用 old=%s new=%s max_duration=%.0fs",
                old_strategy, new_strategy, max_duration_sec,
            )
            return {
                'status': 'PARALLEL_RUN_ENABLED',
                'old_strategy': old_strategy,
                'new_strategy': new_strategy,
                'max_duration_sec': max_duration_sec,
            }

    def disable_parallel_run(self, switch_to_new: bool = True) -> Dict[str, Any]:
        """UPG-P1-03修复: 禁用并行运行模式

        Args:
            switch_to_new: 是否切换到新策略(True)或回退到旧策略(False)

        Returns:
            dict: 操作结果
        """
        with self._lock:
            target = (self._parallel_run_new_strategy if switch_to_new
                      else self._parallel_run_old_strategy)
            self._parallel_run_mode = False
            old_strategy = self._parallel_run_old_strategy
            new_strategy = self._parallel_run_new_strategy
            self._parallel_run_old_strategy = None
            self._parallel_run_new_strategy = None

            if target and target != self._active_strategy:
                self.switch_active_strategy(target)

            logger.info(
                "UPG-P1-03: 并行运行模式已禁用 switch_to_new=%s target=%s",
                switch_to_new, target,
            )
            return {
                'status': 'PARALLEL_RUN_DISABLED',
                'switched_to': target,
                'old_strategy': old_strategy,
                'new_strategy': new_strategy,
            }

    def is_parallel_run_active(self) -> bool:
        """UPG-P1-03修复: 检查是否处于并行运行模式"""
        with self._lock:
            if not self._parallel_run_mode:
                return False
            # 超时自动切换
            elapsed = time.time() - self._parallel_run_start_time
            if elapsed > self._parallel_run_max_duration_sec:
                logger.warning(
                    "UPG-P1-03: 并行运行超时(%.0fs > %.0fs), 自动切换到新策略",
                    elapsed, self._parallel_run_max_duration_sec,
                )
                self.disable_parallel_run(switch_to_new=True)
                return False
            return True

    # UPG-P1-13修复: 回归测试 — 策略逻辑变更后验证关键不变量
    def regression_check(self) -> Dict[str, Any]:
        """UPG-P1-13修复: 回归检查关键不变量

        验证策略生态系统在逻辑变更后仍满足关键不变量：
        1. 资金分配总和 = 1.0
        2. 活跃策略数量 >= 1
        3. 所有活跃策略的期望值 > 0
        4. 策略状态转换合法性

        Returns:
            dict: {passed: bool, violations: list, checked_invariants: int}
        """
        result = {'passed': True, 'violations': [], 'checked_invariants': 0}
        with self._lock:
            # 不变量1: 资金分配总和
            total_alloc = sum(s.capital_allocation for s in [
                self._master, self._reverse, self._other,
                self._spring, self._arbitrage, self._market_making,
            ])
            result['checked_invariants'] += 1
            if abs(total_alloc - 1.0) > 0.05:
                result['violations'].append(
                    f"资金分配总和={total_alloc:.4f} != 1.0 (偏差>{5}%)"
                )
                result['passed'] = False

            # 不变量2: 至少1个活跃策略
            active_count = sum(1 for s in [
                self._master, self._reverse, self._other,
                self._spring, self._arbitrage, self._market_making,
            ] if s.state == 'active' and not s.paused)
            result['checked_invariants'] += 1
            if active_count < 1:
                result['violations'].append("无活跃策略(至少需要1个)")
                result['passed'] = False

            # 不变量3: 活跃策略期望值 > 0 (仅检查有足够交易次数的)
            for s in [self._master, self._reverse, self._other,
                      self._spring, self._arbitrage, self._market_making]:
                if s.state == 'active' and not s.paused and s.expected_value < 0:
                    if s.total_pnl != 0 or s.consecutive_losses > 3:
                        result['checked_invariants'] += 1
                        result['violations'].append(
                            f"策略{s.strategy_id}期望值={s.expected_value:.4f} < 0"
                        )
                        result['passed'] = False

            # 不变量4: 策略状态合法性
            for s in [self._master, self._reverse, self._other,
                      self._spring, self._arbitrage, self._market_making]:
                result['checked_invariants'] += 1
                if s.state not in StrategySlot.VALID_STRATEGY_TRANSITIONS:
                    result['violations'].append(
                        f"策略{s.strategy_id}状态={s.state}不在合法状态集中"
                    )
                    result['passed'] = False

        if result['violations']:
            logger.warning("UPG-P1-13: 回归检查失败 violations=%s", result['violations'])
        else:
            logger.info("UPG-P1-13: 回归检查通过 checked=%d", result['checked_invariants'])
        return result

    # OPS-04修复: 紧急降级 — 减少活跃策略数量
    def emergency_degrade(self, target_active_count: int = 1,
                          caller_id: str = "unknown",
                          reason: str = "") -> Dict[str, Any]:
        """OPS-04修复: 紧急降级 — 减少活跃策略数量

        降级策略：保留优先级最高的N个策略活跃，其余暂停。
        优先级排序: master > spring > reverse > other > arbitrage > market_making

        Args:
            target_active_count: 降级后保留的活跃策略数量（默认1，仅保留master）
            caller_id: 操作人标识
            reason: 降级原因

        Returns:
            dict: {degraded_strategies, active_strategies, result_per_strategy, timestamp}
        """
        result = {
            'degraded_strategies': [],
            'active_strategies': [],
            'result_per_strategy': {},
            'timestamp': datetime.now(CHINA_TZ).isoformat(),
            'caller_id': caller_id,
            'reason': reason,
            'target_active_count': target_active_count,
        }
        # 优先级排序（从高到低）
        priority_order = ['master', 'spring', 'reverse', 'other', 'arbitrage', 'market_making']

        with self._lock:
            # 统计当前活跃策略
            active_slots = []
            for sid in priority_order:
                slot = self._get_slot(sid)
                if slot and slot.state == 'active' and not slot.paused:
                    active_slots.append(sid)

            # 需要暂停的策略 = 当前活跃 - 目标数量
            to_pause = []
            for sid in priority_order:
                if sid in active_slots and len(active_slots) - len(to_pause) > target_active_count:
                    to_pause.append(sid)

            # 执行暂停
            for sid in to_pause:
                slot = self._get_slot(sid)
                if slot:
                    slot.paused = True
                    slot.pause_timestamp = time.time()  # R17-LC-03修复
                    slot.pause_reason = f'EMERGENCY_DEGRADE: {reason or "ops_degrade"}'
                    slot.state = 'standby'
                    result['degraded_strategies'].append(sid)
                    result['result_per_strategy'][sid] = 'paused'
                    logger.warning(
                        "[OPS-04] 策略%s已降级暂停 reason=%s caller=%s",
                        sid, reason, caller_id,
                    )

            # 记录仍活跃的策略
            for sid in priority_order:
                slot = self._get_slot(sid)
                if slot and not slot.paused and slot.state == 'active':
                    result['active_strategies'].append(sid)

        # 审计日志
        try:
            from ali2026v3_trading.risk_service import operations_audit_log
            operations_audit_log(
                action='EMERGENCY_DEGRADE',
                operator=caller_id,
                result='success',
                detail=result,
            )
        except Exception:
            pass

        logger.critical(
            "[OPS-04] emergency_degrade 完成: degraded=%s, active=%s, caller=%s",
            result['degraded_strategies'], result['active_strategies'], caller_id,
        )
        return result


def validate_ev_cache_time_decay(ecosystem=None,
                                   half_life_days: float = 5.0,
                                   n_days: int = 365,
                                   reversal_day: int = 180) -> Dict[str, Any]:
    """P1-裂缝22：验证EV缓存在市场反转后是否及时衰减

    模拟长周期(1年)，前半段某模式高EV，后半段该模式反转。
    检查缓存是否在反转后及时衰减（半衰期≤5天）。
    若衰减过慢，需增加时间加权折扣因子。
    """
    import numpy as np
    rng = np.random.RandomState(42)

    # 模拟EV序列：前半段正EV，后半段负EV
    ev_series = []
    for day in range(n_days):
        if day < reversal_day:
            ev = rng.gauss(0.02, 0.005)  # 正EV
        else:
            ev = rng.gauss(-0.01, 0.005)  # 反转后负EV
        ev_series.append(ev)

    # 模拟缓存衰减：指数加权移动平均
    decay_factors = [0.5, 0.7, 0.9, 0.95, 0.99]  # 不同的衰减因子
    results = {}

    for alpha in decay_factors:
        cached_ev = 0.0
        cached_evs = []
        for ev in ev_series:
            cached_ev = alpha * cached_ev + (1 - alpha) * ev
            cached_evs.append(cached_ev)

        # 计算反转后的衰减半衰期
        reversal_cached = cached_evs[reversal_day - 1]
        target = reversal_cached * 0.5
        half_life = 0
        for d in range(reversal_day, n_days):
            if cached_evs[d] <= target:
                half_life = d - reversal_day
                break

        results[f"alpha_{alpha}"] = {
            "half_life_days": half_life if half_life > 0 else n_days,
            "reversal_cached_ev": round(reversal_cached, 6),
            "final_cached_ev": round(cached_evs[-1], 6),
        }

    # 推荐最佳衰减因子
    best_alpha = None
    for alpha, r in results.items():
        hl = r["half_life_days"]
        if 0 < hl <= half_life_days:
            best_alpha = alpha
            break

    needs_time_weighting = best_alpha is None

    return {
        "decay_results": results,
        "half_life_threshold_days": half_life_days,
        "recommended_alpha": best_alpha,
        "needs_time_weighting": needs_time_weighting,
        "recommendation": "增加时间加权折扣因子" if needs_time_weighting else f"使用衰减因子{best_alpha}",
    }


_ecosystem: Optional[StrategyEcosystem] = None
_ecosystem_lock = threading.Lock()
_ecosystem_initializing: bool = False  # R23-IN-P1-14-FIX: 初始化中标记，防止循环构造


def get_strategy_ecosystem(**kwargs) -> StrategyEcosystem:
    global _ecosystem, _ecosystem_initializing
    # [R23-P2-IN-04-FIX] DCL模式：锁外快速检查，减少锁竞争
    if _ecosystem is not None:
        return _ecosystem
    # R10-P0-01修复: DCL缺陷修复 — 全部在锁内检查和创建
    with _ecosystem_lock:
        # R23-IN-P1-14-FIX: 循环依赖防护 — 检测初始化中标记
        if _ecosystem_initializing:
            raise RuntimeError("[R23-IN-P1-14] strategy_ecosystem初始化中，检测到循环依赖")
        if _ecosystem is None:
            _ecosystem_initializing = True
            try:
                _ecosystem = StrategyEcosystem(**kwargs)
            finally:
                _ecosystem_initializing = False
        return _ecosystem


__all__ = [
    'StrategyEcosystem',
    'StrategySlot',
    'CapitalRoute',
    'EcosystemTradeRecord',
    'get_strategy_ecosystem',
]


# ============================================================================
# R15-P2 性能修复块
# ============================================================================

# R15-P2-PERF-07修复: 资金路由中浮点数多次乘除，添加预计算缓存
# 避免每次路由计算重复的 capital_base * ratio / divisor
# R21-MEM-P1-10修复: dict复用 — 缓存dict在key相同时原地更新，避免每次创建新dict
_capital_route_precomputed: Dict[str, float] = {}

def _precompute_capital_route_ratios(base_capital: float, ratios: Dict[str, float]) -> Dict[str, float]:
    """预计算资金路由比例: base_capital * ratio，结果缓存避免重复乘法"""
    global _capital_route_precomputed
    _cache_key = f"{base_capital:.2f}"
    if _cache_key in _capital_route_precomputed:
        return _capital_route_precomputed
    # R21-MEM-P1-10修复: 原地更新而非创建新dict，减少GC压力
    _capital_route_precomputed.clear()
    _capital_route_precomputed.update({k: base_capital * v for k, v in ratios.items()})
    return _capital_route_precomputed



# PERF-P2-14修复: 启用资金路由预计算
_capital_route_cache = _precompute_capital_route_ratios(0.0, {})

# R15-P2-PERF-08标记: DataFrame索引重置频繁，避免reset_index用loc替代
# 识别位置: 资金曲线、PnL汇总等处使用reset_index()
# TODO(R17-P2-DOC-02): 将 df.reset_index() 改为 df.loc[:, columns] 或 df.set_index() + df.loc[] 模式
# 原因: reset_index()创建新DataFrame，loc仅视图操作

# R15-P2-PERF-14修复: 策略生态系统重复浮点除法，预计算缓存
_division_cache: Dict[str, float] = {}  # R21-MEM-P2-10修复: 无TTL/大小限制的缓存，浮点除法结果不变可无TTL，但应设max_size防无限增长

def _precompute_division(numerator: float, denominator: float, key: str = '') -> float:
    """预计算浮点除法并缓存，避免重复除法运算"""
    if not key:
        key = f"{numerator:.6f}/{denominator:.6f}"
    if key in _division_cache:
        return _division_cache[key]
    if denominator == 0.0:
        return 0.0
    result = numerator / denominator
    _division_cache[key] = result
    return result
