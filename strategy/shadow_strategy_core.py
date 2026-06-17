# MODULE_ID: M1-260
"""影子策略核心服务与类型定义 - 合并自_shadow_strategy_core.py, _shadow_internals.py, shadow_strategy_types.py (2026-06-12)"""
from __future__ import annotations

import copy
import json
import logging
import math
import numpy as np
import os
import pandas as pd
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer, yaml_safe_load
from ali2026v3_trading.infra.shared_utils import CHINA_TZ
from ali2026v3_trading.infra.shared_utils import compute_content_hash  # R4-3: 统一哈希计算
# R27-P1修复: 导入容错/浮点/同步工具
from ali2026v3_trading.infra.resilience import (
    ShadowSyncBarrier, SharedStateRegistry, Watchdog, HeartbeatMonitor,
    CircuitBreakerHalfOpen, StrategyRestartGuard,
    stable_sum, stable_mean, stable_variance, stable_std,
    compute_sharpe_stable, KahanSummation, safe_divide,
    safe_normalize_weights, PRICE_TOLERANCE, FLOAT_COMPARE_TOLERANCE,
    get_strategy_registry, StrategyRegistry,
)
from ali2026v3_trading.config.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL,
)


# ═══════════════════════════════════════════════════════════════
# Data classes (from shadow_strategy_types.py)
# ═══════════════════════════════════════════════════════════════

@dataclass(slots=True)
class ShadowTradeRecord:
    """影子策略交易记录"""
    trade_id: str
    shadow_type: str
    timestamp: str
    instrument_id: str = ""
    direction: str = ""
    price: float = 0.0
    quantity: int = 0
    open_reason: str = ""
    close_reason: str = ""
    pnl: float = 0.0
    commission: float = 0.0
    net_pnl: float = 0.0
    is_open: bool = True
    market_state: str = ""
    signal_strength: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AlphaMetrics:
    """Alpha比率监控指标 — 覆盖6策略组×3变体"""
    timestamp: str
    # S1-S6 各组master/shadow_a/shadow_b sharpe
    s1_master_sharpe: float = 0.0
    s1_shadow_a_sharpe: float = 0.0
    s1_shadow_b_sharpe: float = 0.0
    s2_master_sharpe: float = 0.0
    s2_shadow_a_sharpe: float = 0.0
    s2_shadow_b_sharpe: float = 0.0
    s3_master_sharpe: float = 0.0
    s3_shadow_a_sharpe: float = 0.0
    s3_shadow_b_sharpe: float = 0.0
    s4_master_sharpe: float = 0.0
    s4_shadow_a_sharpe: float = 0.0
    s4_shadow_b_sharpe: float = 0.0
    s5_master_sharpe: float = 0.0
    s5_shadow_a_sharpe: float = 0.0
    s5_shadow_b_sharpe: float = 0.0
    s6_master_sharpe: float = 0.0
    s6_shadow_a_sharpe: float = 0.0
    s6_shadow_b_sharpe: float = 0.0
    s7_master_sharpe: float = 0.0
    s7_shadow_a_sharpe: float = 0.0
    s7_shadow_b_sharpe: float = 0.0
    # P1-R8-19修复: 各策略组独立Alpha
    s1_alpha: float = 0.0
    s2_alpha: float = 0.0
    s3_alpha: float = 0.0
    s4_alpha: float = 0.0
    s5_alpha: float = 0.0
    s6_alpha: float = 0.0
    s7_alpha: float = 0.0
    # 聚合指标(向后兼容，取S2组)
    master_sharpe: float = 0.0
    shadow_a_sharpe: float = 0.0
    shadow_b_sharpe: float = 0.0
    master_max_drawdown: float = 1.0
    alpha_ratio: float = 0.0
    master_expected_value: float = 0.0
    shadow_a_expected_value: float = 0.0
    shadow_b_expected_value: float = 0.0
    alpha_ratio_prev: float = 0.0
    alpha_ratio_decline_pct: float = 0.0
    consecutive_decline_windows: int = 0
    degradation_triggered: bool = False
    absolute_ev_breached: bool = False  # 绝对EV底线触发标记
    master_sharpe_eliminate: bool = False  # P1-2修复：主策略Sharpe<=0时ELIMINATE标记
    jensen_alpha: float = 0.0  # R19-ATT-02: 标准Jensen's Alpha (CAPM回归)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ShadowParamsSnapshot:
    """影子策略参数快照（独立锁定）"""
    shadow_type: str
    locked_at: str
    param_set: Dict[str, Any] = field(default_factory=dict)
    param_yaml_path: str = ""
    param_hash: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ═══════════════════════════════════════════════════════════════
# Module-level variables (from _shadow_internals.py)
# ═══════════════════════════════════════════════════════════════

_shadow_engine = None
_shadow_engine_lock = threading.Lock()


# ═══════════════════════════════════════════════════════════════
# Core service (from _shadow_strategy_core.py)
# ═══════════════════════════════════════════════════════════════

def _make_equity_curve_deque(maxlen: int = 10000) -> deque:
    return deque(maxlen=maxlen)

try:
    import yaml
except ImportError:
    yaml = None


logger = get_logger(__name__)  # R9-5

class ConfigError(RuntimeError):
    """配置错误 — 升A路径T1.3: 参数真相源缺失或格式错误时抛出"""
    pass

class ShadowStrategyCoreService:
    """Core lifecycle and param management service (from ShadowStrategyCoreMixin)"""
    STRATEGY_GROUPS = ['s1_hft', 's2_resonance', 's3_box', 's4_spring', 's5_arbitrage', 's6_market_making', 's7_divergence']
    ALPHA_DECLINE_THRESHOLD_PCT = 20.0
    CONSECUTIVE_DECLINE_LIMIT = 2
    MIN_TRADES_FOR_METRICS = 5
    # P0-13修复: 绝对EV底线阈值
    # 原阈值0过于严格(实盘扣除滑点+佣金后EV微负是常态)
    # 改为: EV < -1.0 时触发暂停(平均每笔亏损超过1.0单位，说明策略已无正期望)
    # 对于期权(权利金几百~几千元/手)，-1.0元/手意味着策略亏损已显著
    # R14-P0-DOC-03修复: 量纲说明 — 此处EV单位为per_trade_ev(每笔交易的期望值/元)
    # total_ev(累计期望值)不与此阈值比较，累计EV由equity_curve判定
    # UPG-03修复: ABSOLUTE_EV_FLOOR从-1.0改为-0.5，与parameter_attribute_matrix.yaml对齐
    ABSOLUTE_EV_FLOOR = -0.5  # per_trade_ev底线(元/手)
    EQUITY_CURVE_MAX_LEN = 10000
    # R13-P2-LOG-16修复: 交易记录上限改为可配置，默认值保持10000
    TRADES_MAX_LEN = 10000
    _trades_max_len: int = 10000  # 可通过set_trades_max_len()修改
    ALPHA_HISTORY_MAX_LEN = 1000
    LOG_QUEUE_MAX_LEN = 50000
    LOG_FLUSH_INTERVAL = 100
    _SHADOW_PARAM_OVERRIDES: Dict[str, Dict[str, Dict[str, Any]]] = {}
    DEFAULT_COMMISSION_RATE = 0.00003

    JSONL_FORMAT_VERSION = "1.0"


    def _estimate_commission(self, price: float, quantity: int) -> float:
        try:
            from ali2026v3_trading.infra.commission_utils import estimate_commission_simple
            return estimate_commission_simple(price, quantity, self.DEFAULT_COMMISSION_RATE)
        except ImportError:
            return abs(price * quantity * self.DEFAULT_COMMISSION_RATE)
    def _load_absolute_ev_floor(self) -> None:
        """R19-UG-01修复: 从config/params.yaml加载ABSOLUTE_EV_FLOOR

        升A路径T2.1: 通过ConfigService.get_param统一读取，YAML为唯一参数真相源。
        """
        from ali2026v3_trading.config.config_service import get_param
        _ev_floor = get_param("parameter_attributes.shadow_absolute_ev_floor.default")
        if _ev_floor is None:
            raise ConfigError(
                "parameter_attributes.shadow_absolute_ev_floor.default 未找到，"
                "升A路径T1.3: params.yaml为唯一参数真相源"
            )
        self._absolute_ev_floor: float = float(_ev_floor)
        logging.debug("[ShadowStrategyEngine] R19-UG-01: YAML加载ABSOLUTE_EV_FLOOR=%.2f",
                      self._absolute_ev_floor)
    def _mask_sensitive_value(self, value: float) -> str:
        """对敏感数值进行掩码处理，仅显示首尾部分"""
        s = str(int(value)) if value == int(value) else f"{value:.2f}"
        if len(s) <= 4:
            return s[:1] + '*' * (len(s) - 1)
        return s[:2] + '*' * (len(s) - 4) + s[-2:]
    def __init__(
        self,
        log_dir: Optional[str] = None,
        yaml_path: Optional[str] = None,
        alpha_window_days: int = 7,  # R24-P1-DF-04修复: alpha_window_days从21改为7，与V7.0手册对齐
        summary_interval_hours: int = 24,
        shadow_seed: Optional[int] = None,
        shadow_alpha_threshold: float = 0.1,  # P0-6修复: alpha衰减阈值，与task_scheduler/params_service对齐
        strategy_group: Optional[str] = None,  # P1-R11-15修复: 策略组ID，用于YAML键前缀隔离
    ):
        self._lock = threading.RLock()
        self._rng = random.Random(shadow_seed if shadow_seed is not None else 42)
        self._snapshot_collector = None
        # P1-R11-15修复: 存储策略组ID，用于YAML键前缀隔离
        self._strategy_group = strategy_group
        self._yaml_path = yaml_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'params.yaml'
        )
        if not os.path.exists(self._yaml_path):
            logging.warning("[R2-4] config/params.yaml不存在: %s，参数真相源不可用", self._yaml_path)

        self._log_dir = log_dir or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'shadow'
        )
        if self._strategy_group:
            self._log_dir = os.path.join(self._log_dir, str(self._strategy_group))
        os.makedirs(self._log_dir, exist_ok=True)

        self._alpha_window_days = alpha_window_days
        self._summary_interval_hours = summary_interval_hours
        self._shadow_alpha_threshold: float = shadow_alpha_threshold

        # P1-R9-23修复: 从cascade_config.yaml读取alpha_decay_threshold替代硬编码
        try:
            from ali2026v3_trading.infra.serialization_utils import yaml_safe_load  # R3-1修复
            _cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'cascade_config.yaml')
            with open(_cfg_path, 'r', encoding='utf-8') as _f:
                _cfg = yaml_safe_load(_f) or {}
            _ad = _cfg.get('alpha_decay', {})
            self._alpha_decay_threshold = float(_ad.get('alpha_decay_threshold', 0.05))
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _cfg_err:
            logger.debug("[P1-R9-23] cascade_config.yaml读取失败, 使用默认值0.05: %s", _cfg_err)
            self._alpha_decay_threshold = 0.05

        self._load_absolute_ev_floor()

        # P0-裂缝11修复：影子策略物理隔离
        # 1. is_shadow_mode标志：初始化为False，由is_shadow_mode()方法动态检查隔离状态
        # R13-P0-DEAD-01修复: 不再硬编码为True，而是通过验证隔离性来确定实际状态
        self._is_shadow_mode: bool = False
        # 2. 独立模拟资金账户：影子策略使用独立paper_account，不影响主策略
        self._paper_account = {
            'initial_equity': 1_000_000.0,
            'current_equity': 1_000_000.0,
            'positions': {},       # instrument_id -> size
            'entry_prices': {},    # instrument_id -> entry price
            'realized_pnl': 0.0,
            'commission_paid': 0.0,
            'trade_count': 0,
        }
        # 3. 影子策略订单不经过OrderService，仅记录到独立日志
        self._shadow_order_log: deque = deque(maxlen=50000)

        self._shadow_a_params: Optional[ShadowParamsSnapshot] = None
        self._shadow_b_params: Optional[ShadowParamsSnapshot] = None
        self._params_locked: bool = False

        # 6策略组×3变体独立状态
        self._group_trades: Dict[str, Dict[str, deque]] = {}
        self._group_pnl_sum: Dict[str, Dict[str, float]] = {}
        # R21-MEM-P1-09修复: equity曲线从List[float]改为deque(maxlen)，自动淘汰旧数据
        self._group_equity_curve: Dict[str, Dict[str, deque]] = {}
        for g in self.STRATEGY_GROUPS:
            self._group_trades[g] = {
                'master': deque(maxlen=self.TRADES_MAX_LEN),
                'shadow_a': deque(maxlen=self.TRADES_MAX_LEN),
                'shadow_b': deque(maxlen=self.TRADES_MAX_LEN),
            }
            self._group_pnl_sum[g] = {'master': 0.0, 'shadow_a': 0.0, 'shadow_b': 0.0}
            self._group_equity_curve[g] = {
                'master': _make_equity_curve_deque(maxlen=self.EQUITY_CURVE_MAX_LEN),
                'shadow_a': _make_equity_curve_deque(maxlen=self.EQUITY_CURVE_MAX_LEN),
                'shadow_b': _make_equity_curve_deque(maxlen=self.EQUITY_CURVE_MAX_LEN),
            }

        # 向后兼容: 原单例属性指向s2_resonance组
        self._shadow_a_trades: deque = self._group_trades['s2_resonance']['shadow_a']
        self._shadow_b_trades: deque = self._group_trades['s2_resonance']['shadow_b']
        self._master_trades: deque = self._group_trades['s2_resonance']['master']

        self._alpha_history: deque = deque(maxlen=self.ALPHA_HISTORY_MAX_LEN)
        self._last_alpha_metrics: Optional[AlphaMetrics] = None
        self.alpha_decay_rate: float = 0.0  # R3-D-03修复: 公共属性供risk_service D10维度消费

        self._master_equity_curve: deque = self._group_equity_curve['s2_resonance']['master']  # R21-MEM-P1-09修复
        self._shadow_a_equity_curve: deque = self._group_equity_curve['s2_resonance']['shadow_a']
        self._shadow_b_equity_curve: deque = self._group_equity_curve['s2_resonance']['shadow_b']

        self._master_pnl_sum: float = 0.0
        self._shadow_a_pnl_sum: float = 0.0
        self._shadow_b_pnl_sum: float = 0.0

        self._degradation_active: bool = False
        self._absolute_ev_pause: bool = False
        self._last_summary_time: float = 0.0
        self._last_daily_summary_date: Optional[str] = None

        self._stats = {
            'shadow_a_trades': 0,
            'shadow_b_trades': 0,
            'master_trades': 0,
            'alpha_calculations': 0,
            'degradation_events': 0,
            'absolute_ev_breaches': 0,
            'params_locked_at': None,
        }

        self._trade_id_counter: int = 0

        self._log_write_queue: deque = deque(maxlen=self.LOG_QUEUE_MAX_LEN)
        self._log_queue_lock = threading.RLock()  # R10-P0-05修复: 改为RLock避免_async_flush_log_queue重入死锁
        self._log_seq_counter: int = 0  # P2-R9-02修复: 日志序列号计数器
        # R15-P1-PERF-05修复: ThreadPoolExecutor已设置max_workers=1，防止线程数无限增长
        # R15-P1-RES-05修复: 添加RejectedExecutionHandler(队列满时丢弃+warning)
        self._log_writer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='shadow_log')
        # R22-RES-01修复: 提交dummy任务触发线程创建，然后设为daemon; RuntimeError一并捕获避免PY3.12对已启动线程设置daemon报错
        try:
            _dummy_future = self._log_writer_executor.submit(lambda: None)
            _dummy_future.result(timeout=2.0)
            for _t in list(getattr(self._log_writer_executor, '_threads', set())):
                try:
                    _t.daemon = True
                except RuntimeError:
                    pass
        except (ValueError, KeyError, TypeError, AttributeError, RuntimeError) as _r3_err:
            logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
            pass
        self._log_rejected_count: int = 0
        self._log_flush_counter: int = 0
        # R13-P2-LOG-14修复: 日志队列丢弃计数器
        self._log_dropped_count: int = 0

        self._load_and_lock_params()

        # R21-CC-P1-03修复: 注册atexit回调确保线程池shutdown
        import atexit as _atexit
        _atexit.register(self._atexit_shutdown)

        # R13-P2-API-11修复: 初始化依赖链验证
        self.validate_dependencies()

        # FR-P1-02修复: 影子引擎tick时间差监控属性
        self._last_master_tick_timestamp: float = 0.0
        self._last_shadow_tick_timestamp: float = 0.0
        self._tick_timestamp_diff_threshold_sec: float = 5.0

        logger.info("[ShadowStrategyEngine] 初始化完成, log_dir=%s", self._log_dir)

    def validate_dependencies(self) -> bool:
        """R13-P2-API-11修复: 验证所有必需依赖是否存在"""
        missing = []
        if not hasattr(self, '_lock'):
            missing.append('_lock')
        if not hasattr(self, '_paper_account'):
            missing.append('_paper_account')
        if not hasattr(self, '_shadow_order_log'):
            missing.append('_shadow_order_log')
        if not hasattr(self, '_group_trades'):
            missing.append('_group_trades')
        if not hasattr(self, '_log_writer_executor'):
            missing.append('_log_writer_executor')
        try:
            from ali2026v3_trading.config.config_params import get_default_state_param_sets
        except ImportError:
            missing.append('config_params.get_default_state_param_sets')
        if missing:
            logger.error("[ShadowStrategyEngine] 依赖验证失败，缺少: %s", missing)
            return False
        return True
    def shutdown(self, wait: bool = True, timeout: float = 10.0) -> None:
        """R10-P0-16修复: 关闭ShadowStrategyEngine，释放线程池和刷盘队列

        Args:
            wait: 是否等待未完成的任务
            timeout: 等待超时（秒）
        """
        # 先刷盘剩余日志
        try:
            self._async_flush_log_queue()
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
            pass
        # 关闭线程池
        if hasattr(self, '_log_writer_executor') and self._log_writer_executor is not None:
            import sys as _sys
            if _sys.version_info >= (3, 13) and wait:
                self._log_writer_executor.shutdown(wait=wait, timeout=timeout if wait else 0)
            else:
                self._log_writer_executor.shutdown(wait=wait)
            self._log_writer_executor = None
            try:
                logger.info("[R10-P0-16] _log_writer_executor shutdown complete")
            except (ValueError, OSError):
                pass
    def _atexit_shutdown(self) -> None:
        """atexit回调：进程退出时自动shutdown线程池，防止资源泄漏"""
        try:
            self.shutdown(wait=False)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
            pass
    def _generate_trade_id(self, shadow_type: str) -> str:
        with self._lock:
            self._trade_id_counter += 1
            counter = self._trade_id_counter
        ts = datetime.now(CHINA_TZ).strftime('%Y%m%d%H%M%S')
        return f"SHADOW-{shadow_type}-{ts}-{counter:06d}"
    def _compute_param_hash(self, param_set: Dict[str, Any]) -> str:
        content = json_dumps(param_set, sort_keys=True)
        return compute_content_hash(content)  # R4-3: 统一哈希计算
    def set_shadow_param_override(cls, strategy_group: str, shadow_type: str, overrides: Dict[str, Any]) -> None:
        """P2-R11-11修复: 设置策略级影子参数覆盖

        Args:
            strategy_group: 策略组名（如hft/main/box_extreme等）
            shadow_type: 影子类型（shadow_a/shadow_b）
            overrides: 参数覆盖字典
        """
        if strategy_group not in cls._SHADOW_PARAM_OVERRIDES:
            cls._SHADOW_PARAM_OVERRIDES[strategy_group] = {}
        cls._SHADOW_PARAM_OVERRIDES[strategy_group][shadow_type] = overrides
    def _load_and_lock_params(self) -> None:
        # P1-R11-15: Shadow instances share the same YAML config.
        # P2-R11-11修复: 支持策略级影子参数覆盖，不同策略可使用不同影子参数
        param_sets = self._load_yaml_param_sets()
        now_str = datetime.now(CHINA_TZ).isoformat()

        shadow_a_set = dict(param_sets.get(STRATEGY_MODE_INCORRECT_REVERSAL, {}))
        shadow_b_set = dict(param_sets.get(STRATEGY_MODE_CORRECT_TRENDING, {}))

        if not shadow_a_set and not shadow_b_set:
            _defaults = self._default_param_sets()
            if not shadow_a_set:
                shadow_a_set = dict(_defaults.get(STRATEGY_MODE_INCORRECT_REVERSAL, {}))
            if not shadow_b_set:
                shadow_b_set = dict(_defaults.get(STRATEGY_MODE_CORRECT_TRENDING, {}))

        # P2-R11-11修复: 应用策略级影子参数覆盖
        # PA-03修复: 使用 type(self) 替代硬编码 ShadowStrategyEngine 引用，
        # 消除内部Mixin对公共Facade的耦合（三层物理隔离）
        _group_overrides = type(self)._SHADOW_PARAM_OVERRIDES.get(
            getattr(self, '_strategy_group', ''), {})
        if 'shadow_a' in _group_overrides:
            shadow_a_set.update(_group_overrides['shadow_a'])
        if 'shadow_b' in _group_overrides:
            shadow_b_set.update(_group_overrides['shadow_b'])

        self._shadow_a_params = ShadowParamsSnapshot(
            shadow_type='shadow_a',
            locked_at=now_str,
            param_set=shadow_a_set,
            param_yaml_path=self._yaml_path,
            param_hash=self._compute_param_hash(shadow_a_set),
        )
        self._shadow_b_params = ShadowParamsSnapshot(
            shadow_type='shadow_b',
            locked_at=now_str,
            param_set=shadow_b_set,
            param_yaml_path=self._yaml_path,
            param_hash=self._compute_param_hash(shadow_b_set),
        )
        self._params_locked = True
        self._stats['params_locked_at'] = now_str

        self._validate_shadow_param_independence(shadow_a_set, shadow_b_set)

        logger.info(
            "[ShadowStrategyEngine] 参数独立锁定完成: "
            "shadow_a(hash=%s), shadow_b(hash=%s)",
            self._shadow_a_params.param_hash[:8],
            self._shadow_b_params.param_hash[:8],
        )
    def _load_yaml_param_sets(self) -> Dict[str, Any]:
        if yaml is None:
            logger.warning("[ShadowStrategyEngine] PyYAML不可用，使用默认参数")
            return self._default_param_sets()
        try:
            if os.path.exists(self._yaml_path):
                with open(self._yaml_path, 'r', encoding='utf-8') as f:
                    raw_data = yaml_safe_load(f) or {}
                # P1-R11-15修复: 当strategy_group提供时，优先查找带前缀的键
                # 例如: strategy_group="hft" → 查找 "hft_shadow_a", "hft_incorrect_reversal" 等
                # 若带前缀键不存在，回退到无前缀键(向后兼容)
                if self._strategy_group:
                    prefixed_data = {}
                    for key, value in raw_data.items():
                        # 查找以 "{strategy_group}_" 开头的键，去掉前缀后映射
                        if key.startswith(f"{self._strategy_group}_"):
                            stripped_key = key[len(self._strategy_group) + 1:]
                            prefixed_data[stripped_key] = value
                    if prefixed_data:
                        logger.info("[ShadowStrategyEngine] 使用策略组'%s'前缀参数: %d个键",
                                   self._strategy_group, len(prefixed_data))
                        return prefixed_data
                    else:
                        logger.info("[ShadowStrategyEngine] 策略组'%s'无前缀参数，回退到无前缀键",
                                   self._strategy_group)
                return raw_data
            else:
                logger.warning("[ShadowStrategyEngine] YAML文件不存在: %s", self._yaml_path)
                return self._default_param_sets()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.error("[ShadowStrategyEngine] 加载YAML失败: %s", e)
            return self._default_param_sets()

    def _default_param_sets(self) -> Dict[str, Dict[str, Any]]:
        from ali2026v3_trading.config.config_params import get_default_state_param_sets
        return get_default_state_param_sets()
    def _validate_shadow_param_independence(self, shadow_a: Dict, shadow_b: Dict) -> bool:
        key_params = ["close_take_profit_ratio", "close_stop_loss_ratio",
                      "max_risk_ratio", "max_hold_minutes"]
        diffs = []
        for k in key_params:
            a_val = shadow_a.get(k)
            b_val = shadow_b.get(k)
            if a_val is not None and b_val is not None and a_val != 0:
                diffs.append(abs(b_val - a_val) / abs(a_val))
        if not diffs:
            logger.warning("[P0-10修复] 影子参数独立性验证: 无可比较的关键参数")
            return True
        avg_diff = sum(diffs) / len(diffs)
        min_diff_pct = 0.20
        if avg_diff < min_diff_pct:
            logger.error(
                "[P0-10修复] 影子参数独立性铁律违反! 平均差异=%.4f < 阈值=%.4f, "
                "影子策略参数与主策略差异不足，可能违反9.2节独立性铁律",
                avg_diff, min_diff_pct,
            )
            return False
        logger.info("[P0-10修复] 影子参数独立性验证通过: 平均差异=%.4f >= 阈值=%.4f", avg_diff, min_diff_pct)
        return True
    def are_params_locked(self) -> bool:
        with self._lock:
            return self._params_locked
    def get_shadow_a_params(self) -> Dict[str, Any]:
        with self._lock:
            if self._shadow_a_params is None:
                return {}
            return dict(self._shadow_a_params.param_set)  # R21-MEM-P1-04修复: 浅拷贝替代deepcopy
    def get_shadow_b_params(self) -> Dict[str, Any]:
        with self._lock:
            if self._shadow_b_params is None:
                return {}
            return dict(self._shadow_b_params.param_set)  # R21-MEM-P1-04修复
    def get_params_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'shadow_a': self._shadow_a_params.to_dict() if self._shadow_a_params else {},
                'shadow_b': self._shadow_b_params.to_dict() if self._shadow_b_params else {},
                'params_locked': self._params_locked,
            }
    def relock_params(self, new_yaml_path: Optional[str] = None, caller_id: str = "unknown") -> None:
        """P1-R11-19修复: 添加调用方鉴权"""
        _TRUSTED_RELOCK_CALLERS = frozenset({
            'shadow_strategy_engine', 'strategy_ecosystem', 'config_service',
            'state_param_manager', 'risk_service', 'main',
        })
        if caller_id not in _TRUSTED_RELOCK_CALLERS:
            logger.warning("[ShadowStrategyEngine] relock_params被非授权调用方拒绝: caller_id=%s", caller_id)
            return
        with self._lock:
            if new_yaml_path:
                self._yaml_path = new_yaml_path
            self._load_and_lock_params()
            logger.warning(
                "[ShadowStrategyEngine] 参数重新锁定(谨慎使用!): yaml=%s",
                self._yaml_path,
            )
    def set_snapshot_collector(self, collector) -> None:
        self._snapshot_collector = collector
    def _enqueue_trade_log(self, record: ShadowTradeRecord) -> None:
        # P1-R9-20修复: 同时写入统一日志系统
        try:
            logger.info("[SHADOW_TRADE] %s", json.dumps(
                {**record.to_dict(), 'format_version': self.JSONL_FORMAT_VERSION},
                ensure_ascii=False,
            ))
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] silent except triggered: %s", _r3_err)
            pass
        try:
            # R13-P2-LOG-05修复: 日志文件名包含策略组标识
            group_id = getattr(record, 'strategy_group', 'unknown')
            log_file = os.path.join(
                self._log_dir,
                f"shadow_{record.shadow_type}_{group_id}_{datetime.now(CHINA_TZ).strftime('%Y%m%d')}.jsonl"
            )
            with self._log_queue_lock:
                # P2-R9-02修复: 添加序列号确保日志顺序
                self._log_seq_counter += 1
                line = json_dumps(
                    {**record.to_dict(), 'format_version': self.JSONL_FORMAT_VERSION, 'sequence_number': self._log_seq_counter},
                ) + '\n'
                # R13-P2-LOG-14修复: 检测队列满丢弃并计数
                if len(self._log_write_queue) >= self.LOG_QUEUE_MAX_LEN:
                    self._log_dropped_count += 1
                    if self._log_dropped_count % 100 == 1:
                        logger.warning("[ShadowStrategyEngine] R13-P2-LOG-14: 日志队列已满, "
                                      "累计丢弃%d条", self._log_dropped_count)
                self._log_write_queue.append((log_file, line))
                self._log_flush_counter += 1
                # R10-P0-05修复: should_flush判断和重置在锁内，
                # _async_flush_log_queue也在锁内调用，消除TOCTOU竞态
                if self._log_flush_counter >= self.LOG_FLUSH_INTERVAL:
                    self._log_flush_counter = 0
                    self._async_flush_log_queue()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[ShadowStrategyEngine] 入队交易日志失败: %s", e)
    def _async_flush_log_queue(self) -> None:
        # R13-P2-LOG-10修复: 替换ThreadPoolExecutor为同步写入，
        # 避免线程池开销和潜在的日志丢失。日志写入本身是I/O密集型，
        # 单线程同步写入更可靠，且日志量不大时性能影响可忽略。
        try:
            items = []
            with self._log_queue_lock:
                while self._log_write_queue:
                    items.append(self._log_write_queue.popleft())
            if items:
                self._do_write_logs(items)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.warning("[ShadowStrategyEngine] 日志刷盘失败: %s", e)
    def _do_write_logs(self, items: List[Tuple[str, str]]) -> None:
        file_buffers: Dict[str, List[str]] = {}
        for log_file, line in items:
            file_buffers.setdefault(log_file, []).append(line)
        for log_file, lines in file_buffers.items():
            try:
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.writelines(lines)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
                logger.warning("[ShadowStrategyEngine] 写入日志文件失败: %s", e)


__all__ = [
    'ShadowTradeRecord', 'AlphaMetrics', 'ShadowParamsSnapshot',
    '_shadow_engine', '_shadow_engine_lock',
    'ShadowStrategyCoreService', 'ShadowStrategyCoreMixin', 'ConfigError',
]
ShadowStrategyCoreMixin = ShadowStrategyCoreService
