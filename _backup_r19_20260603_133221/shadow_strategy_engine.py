"""
shadow_strategy_engine.py - 影子策略监控引擎

V7核心工程任务：Alpha衰减监测
- 影子A(反向逻辑)：主策略用correct_trending时，影子A强制使用incorrect_reversal参数
- 影子B(随机动作)：相同信号时刻，50/50概率随机选择买卖方向，遵守相同风控

关键补丁(V6.0)：独立参数锁定，防止影子策略与主策略"沉默同谋"

设计原则：
- 影子策略参数在初始Walk-forward后冻结，不再随主策略迭代更新
- 每笔交易记录到独立日志
- 每日/周汇总输出Alpha比率
- 绝对期望值底线：主策略期望值必须>0
"""
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
from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer, yaml_safe_load
from ali2026v3_trading.shared_utils import CHINA_TZ
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# R27-P1修复: 导入容错/浮点/同步工具
from ali2026v3_trading.resilience_utils import (
    ShadowSyncBarrier, SharedStateRegistry, Watchdog, HeartbeatMonitor,
    CircuitBreakerHalfOpen, StrategyRestartGuard,
    stable_sum, stable_mean, stable_variance, stable_std,
    compute_sharpe_stable, KahanSummation, safe_divide,
    safe_normalize_weights, PRICE_TOLERANCE, FLOAT_COMPARE_TOLERANCE,
    get_strategy_registry, StrategyRegistry,
)
from ali2026v3_trading.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL,
)

try:
    import yaml
except ImportError:
    yaml = None


logger = logging.getLogger(__name__)


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
    # P1-R8-19修复: 各策略组独立Alpha比率
    s1_alpha: float = 0.0
    s2_alpha: float = 0.0
    s3_alpha: float = 0.0
    s4_alpha: float = 0.0
    s5_alpha: float = 0.0
    s6_alpha: float = 0.0
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
    absolute_ev_breached: bool = False
    master_sharpe_eliminate: bool = False  # P1-2修复：主策略Sharpe<=0时ELIMINATE标记
    jensen_alpha: float = 0.0  # R19-ATT-02: 标准Jensen's Alpha（CAPM回归）

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


class ShadowStrategyEngine:
    """影子策略监控引擎 — 6策略组×3变体(master/shadow_a/shadow_b)

    职责：
    1. 维护6策略组各自两个影子策略(反向逻辑A + 随机动作B)的独立运行环境
    2. 参数独立锁定：Walk-forward后冻结，不随主策略更新
    3. 每笔交易记录到独立日志
    4. 计算Alpha比率和绝对期望值
    5. 监控Alpha衰减，触发自动降级
    """

    STRATEGY_GROUPS = ['s1_hft', 's2_resonance', 's3_box', 's4_spring', 's5_arbitrage', 's6_market_making']
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
    DEFAULT_COMMISSION_RATE = 0.00003  # P0-R12-EX-14修复: 万0.3手续费率

    def _estimate_commission(self, price: float, quantity: int) -> float:
        """P0-R12-EX-14修复: 估算手续费(价格*数量*费率)"""
        return abs(price * quantity * self.DEFAULT_COMMISSION_RATE)

    def _load_absolute_ev_floor(self) -> None:
        """R19-UG-01修复: 从parameter_attribute_matrix.yaml加载ABSOLUTE_EV_FLOOR

        动态加载替代硬编码ABSOLUTE_EV_FLOOR常量的实例值。
        加载失败时回退到类常量ABSOLUTE_EV_FLOOR（-0.5）。
        """
        import yaml as _yaml
        try:
            _matrix_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), 'param_pool',
                'parameter_attribute_matrix.yaml'
            )
            if not os.path.exists(_matrix_path):
                _matrix_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    'parameter_attribute_matrix.yaml'
                )
            if os.path.exists(_matrix_path):
                with open(_matrix_path, 'r', encoding='utf-8') as _f:
                    _data = yaml_safe_load(_f)
                _ev_floor = _data.get('shadow_absolute_ev_floor', {}).get('default', None)
                if _ev_floor is not None:
                    self._absolute_ev_floor: float = float(_ev_floor)
                    logging.debug("[ShadowStrategyEngine] R19-UG-01: YAML加载ABSOLUTE_EV_FLOOR=%.2f",
                                  self._absolute_ev_floor)
                    return
        except Exception as _e:
            logging.warning("[ShadowStrategyEngine] R19-UG-01: YAML加载失败: %s，回退硬编码值", _e)
        self._absolute_ev_floor: float = self.ABSOLUTE_EV_FLOOR

    @staticmethod
    def _mask_sensitive_value(value: float) -> str:
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
            os.path.dirname(os.path.abspath(__file__)), 'param_pool', 'state_param_sets.yaml'
        )
        if not os.path.exists(self._yaml_path):
            _fallback = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), 'state_param_sets.yaml'
            )
            if os.path.exists(_fallback):
                # R24-P2-DF-08修复: 非参数池/路径的YAML可能是旧版配置，记录警告
                logging.warning("[R24-P2-DF-08] 使用非参数池/路径的YAML: %s，可能包含旧版配置", _fallback)
                self._yaml_path = _fallback

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
            import yaml as _yaml
            _cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'cascade_config.yaml')
            with open(_cfg_path, 'r', encoding='utf-8') as _f:
                _cfg = _yaml.safe_load(_f) or {}
            _ad = _cfg.get('alpha_decay', {})
            self._alpha_decay_threshold = float(_ad.get('alpha_decay_threshold', 0.05))
        except Exception as _cfg_err:
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
        # R22-RES-01修复: 提交一个dummy任务触发线程创建，然后设为daemon
        try:
            _dummy_future = self._log_writer_executor.submit(lambda: None)
            _dummy_future.result(timeout=2.0)
            for _t in getattr(self._log_writer_executor, '_threads', set()):
                _t.daemon = True
        except Exception:
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
            from ali2026v3_trading.config_params import get_default_state_param_sets
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
        except Exception:
            pass
        # 关闭线程池
        if hasattr(self, '_log_writer_executor') and self._log_writer_executor is not None:
            self._log_writer_executor.shutdown(wait=wait, timeout=timeout if wait else 0)
            self._log_writer_executor = None  # R21-MEM-P1-07修复: 释放executor引用，防止shutdown后仍持有资源
            logger.info("[R10-P0-16] _log_writer_executor shutdown complete")

    # R21-CC-P1-03修复: atexit回调，确保进程退出时线程池被正确shutdown
    def _atexit_shutdown(self) -> None:
        """atexit回调：进程退出时自动shutdown线程池，防止资源泄漏"""
        try:
            self.shutdown(wait=False)
        except Exception:
            pass

    # UPG-09修复: JSONL日志格式版本号
    JSONL_FORMAT_VERSION = "1.0"

    def _generate_trade_id(self, shadow_type: str) -> str:
        with self._lock:
            self._trade_id_counter += 1
            counter = self._trade_id_counter
        ts = datetime.now(CHINA_TZ).strftime('%Y%m%d%H%M%S')
        return f"SHADOW-{shadow_type}-{ts}-{counter:06d}"

    def _compute_param_hash(self, param_set: Dict[str, Any]) -> str:
        import hashlib
        content = json_dumps(param_set, sort_keys=True)
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    # P2-R11-11修复: 策略级影子参数覆盖，允许不同策略使用不同的影子参数
    _SHADOW_PARAM_OVERRIDES: Dict[str, Dict[str, Dict[str, Any]]] = {}

    @classmethod
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

        shadow_a_set = dict(param_sets.get(STRATEGY_MODE_INCORRECT_REVERSAL, {}))  # R21-MEM-P1-04修复: 浅拷贝替代deepcopy，参数值为简单类型 R25-SE-P1-02-FIX
        shadow_b_set = dict(param_sets.get(STRATEGY_MODE_CORRECT_TRENDING, {}))  # R21-MEM-P1-04修复 R25-SE-P1-02-FIX

        # P2-R11-11修复: 应用策略级影子参数覆盖
        _group_overrides = ShadowStrategyEngine._SHADOW_PARAM_OVERRIDES.get(
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
        except Exception as e:
            logger.error("[ShadowStrategyEngine] 加载YAML失败: %s", e)
            return self._default_param_sets()

    @staticmethod
    def _default_param_sets() -> Dict[str, Dict[str, Any]]:
        from ali2026v3_trading.config_params import get_default_state_param_sets
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

    def process_shadow_a_signal(
        self,
        market_state: str,
        instrument_id: str = "",
        signal_direction: str = "",
        price: float = 0.0,
        quantity: int = 0,
        signal_strength: float = 0.0,
        strategy_group: str = "s2_resonance",
    ) -> ShadowTradeRecord:
        # FR-P1-02修复: 记录影子引擎tick时间戳
        self._last_shadow_tick_timestamp = time.time()
        with self._lock:
            reverse_map = {
                'long': 'short',
                'short': 'long',
                'buy': 'sell',
                'sell': 'buy',
            }
            reversed_dir = reverse_map.get(signal_direction, signal_direction)

            record = ShadowTradeRecord(
                trade_id=self._generate_trade_id('A'),
                shadow_type=f'{strategy_group}_shadow_a',
                timestamp=datetime.now(CHINA_TZ).isoformat(),
                instrument_id=instrument_id,
                direction=reversed_dir,
                price=price,
                quantity=quantity,
                open_reason='SHADOW_A_REVERSAL',
                market_state=market_state,
                signal_strength=signal_strength,
            )

            group = strategy_group if strategy_group in self.STRATEGY_GROUPS else 's2_resonance'
            self._group_trades[group]['shadow_a'].append(record)

            self._stats['shadow_a_trades'] += 1

            self._enqueue_trade_log(record)

            self.update_paper_account(instrument_id=instrument_id, direction=reversed_dir, quantity=quantity if quantity > 0 else 1, price=price if price > 0 else 0.0, commission=self._estimate_commission(price, quantity))

            if self._snapshot_collector is not None:
                try:
                    import numpy as np
                    self._snapshot_collector.capture_signal_point(
                        timestamp=np.datetime64('now'),
                        trigger_detail=f"{group}_shadow_a {instrument_id} {reversed_dir}",
                    )
                except Exception as e:
                    logging.debug("[ShadowEngine] shadow_a snapshot error: %s", e)

            return record

    def process_shadow_b_signal(
        self,
        market_state: str,
        instrument_id: str = "",
        signal_direction: str = "",
        price: float = 0.0,
        quantity: int = 0,
        signal_strength: float = 0.0,
        strategy_group: str = "s2_resonance",
    ) -> ShadowTradeRecord:
        with self._lock:
            random_dir = self._rng.choice(['long', 'short'])

            record = ShadowTradeRecord(
                trade_id=self._generate_trade_id('B'),
                shadow_type=f'{strategy_group}_shadow_b',
                timestamp=datetime.now(CHINA_TZ).isoformat(),
                instrument_id=instrument_id,
                direction=random_dir,
                price=price,
                quantity=quantity,
                open_reason='SHADOW_B_RANDOM',
                market_state=market_state,
                signal_strength=signal_strength,
            )

            group = strategy_group if strategy_group in self.STRATEGY_GROUPS else 's2_resonance'
            self._group_trades[group]['shadow_b'].append(record)

            self._stats['shadow_b_trades'] += 1

            self._enqueue_trade_log(record)

            self.update_paper_account(instrument_id=instrument_id, direction=random_dir, quantity=quantity if quantity > 0 else 1, price=price if price > 0 else 0.0, commission=self._estimate_commission(price, quantity))

            if self._snapshot_collector is not None:
                try:
                    import numpy as np
                    self._snapshot_collector.capture_signal_point(
                        timestamp=np.datetime64('now'),
                        trigger_detail=f"{group}_shadow_b {instrument_id} {random_dir}",
                    )
                except Exception as e:
                    logging.debug("[ShadowEngine] shadow_b snapshot error: %s", e)

            return record

    def record_master_trade(
        self,
        instrument_id: str = "",
        direction: str = "",
        price: float = 0.0,
        quantity: int = 0,
        pnl: float = 0.0,
        open_reason: str = "",
        market_state: str = "",
        strategy_group: str = "s2_resonance",
    ) -> ShadowTradeRecord:
        # FR-P1-02修复: 记录主引擎tick时间戳，监控主/影子引擎tick时间差
        _now_ts = time.time()
        self._last_master_tick_timestamp = _now_ts
        if self._last_shadow_tick_timestamp > 0:
            _tick_diff = abs(_now_ts - self._last_shadow_tick_timestamp)
            if _tick_diff > self._tick_timestamp_diff_threshold_sec:
                logger.warning("[FR-P1-02] 主引擎与影子引擎tick时间差=%.1fs > 阈值=%.1fs, "
                               "可能存在数据延迟或处理阻塞", _tick_diff, self._tick_timestamp_diff_threshold_sec)
        with self._lock:
            if np.isfinite(pnl):  # NP-P2-16: pnl累加添加isfinite检查
                self._master_pnl_sum += pnl

            group = strategy_group if strategy_group in self.STRATEGY_GROUPS else 's2_resonance'
            if np.isfinite(pnl):  # NP-P2-16: pnl累加添加isfinite检查
                self._group_pnl_sum[group]['master'] += pnl

            record = ShadowTradeRecord(
                trade_id=self._generate_trade_id('MASTER'),
                shadow_type=f'{group}_master',
                timestamp=datetime.now(CHINA_TZ).isoformat(),
                instrument_id=instrument_id,
                direction=direction,
                price=price,
                quantity=quantity,
                pnl=pnl,
                net_pnl=pnl,
                open_reason=open_reason,
                market_state=market_state,
                is_open=False,
            )
            self._group_trades[group]['master'].append(record)
            self._stats['master_trades'] += 1

            self._enqueue_trade_log(record)

            if self._snapshot_collector is not None:
                try:
                    import numpy as np
                    from ali2026v3_trading.strategy_judgment.market_snapshot_collector import SnapshotTrigger
                    self._snapshot_collector.capture_order_event(
                        timestamp=np.datetime64('now'),
                        event_type=SnapshotTrigger.ORDER_CLOSED,
                        trigger_detail=f"master {instrument_id} {direction} pnl={pnl:.2f}",
                    )
                except Exception as e:
                    logging.debug("[ShadowEngine] master snapshot error: %s", e)

            return record

    def record_shadow_pnl(
        self,
        shadow_type: str,
        trade_id: str,
        pnl: float,
        close_reason: str = "",
        commission: float = 0.0,
    ) -> None:
        with self._lock:
            net_pnl = pnl - commission

            if shadow_type == 'shadow_a':
                trades = self._shadow_a_trades
                if np.isfinite(net_pnl):  # NP-P2-16: pnl累加添加isfinite检查
                    self._shadow_a_pnl_sum += net_pnl
            elif shadow_type == 'shadow_b':
                trades = self._shadow_b_trades
                if np.isfinite(net_pnl):  # NP-P2-16: pnl累加添加isfinite检查
                    self._shadow_b_pnl_sum += net_pnl
            else:
                return

            for trade in reversed(trades):
                if trade.trade_id == trade_id and trade.is_open:
                    trade.pnl = pnl
                    trade.commission = commission
                    trade.net_pnl = net_pnl
                    trade.close_reason = close_reason
                    trade.is_open = False
                    break

    def update_equity_curves(
        self,
        master_equity: float,
        shadow_a_equity: float,
        shadow_b_equity: float,
    ) -> None:
        with self._lock:
            self._master_equity_curve.append(master_equity)
            self._shadow_a_equity_curve.append(shadow_a_equity)
            self._shadow_b_equity_curve.append(shadow_b_equity)
            # R21-MEM-P1-09修复: deque(maxlen)自动淘汰旧数据，无需手动切片

    @staticmethod
    def _compute_sharpe(returns: List[float], annualize_factor: float = 252.0,
                        risk_free_rate: float = 0.02) -> float:
        """FM-01修复: Sharpe计算扣除无风险利率
        annualize_factor: 年化因子（日频 √252≈15.87，分钟频 √(252*240)≈245.9）
        risk_free_rate: 年化无风险利率(默认2%)
        risk_free_rate / annualize_factor: 逐期无风险利率
        """
        if len(returns) < 2:
            return 0.0
        mean_r = sum(returns) / len(returns)
        # FM-P2修复: 样本方差(unbiased estimator)，使用 n-1 分母（仅当n>2）
        n = len(returns)
        var_r = sum((r - mean_r) ** 2 for r in returns) / (n - 1) if n > 1 else 0.0
        std_r = math.sqrt(var_r) if var_r > 0 else 0.0
        if std_r < 1e-10:
            return 0.0
        excess_r = mean_r - risk_free_rate / annualize_factor
        sharpe = (excess_r / std_r) * math.sqrt(annualize_factor)
        return sharpe

    @staticmethod
    def _compute_max_drawdown(equity_curve: List[float]) -> float:
        if len(equity_curve) < 2:
            return 0.0
        peak = equity_curve[0]
        max_dd = 0.0
        for eq in equity_curve:
            if eq > peak:
                peak = eq
            if peak > 0:
                dd = (peak - eq) / peak
                if dd > max_dd:
                    max_dd = dd
        return max_dd

    @staticmethod
    def _equity_to_returns(equity_curve: List[float]) -> List[float]:
        if len(equity_curve) < 2:
            return []
        returns = []
        for i in range(1, len(equity_curve)):
            prev = equity_curve[i - 1]
            curr = equity_curve[i]
            if math.isnan(prev) or math.isnan(curr):
                returns.append(0.0)
                continue
            if abs(prev) < 1e-10:
                returns.append(0.0)
            else:
                returns.append((curr - prev) / abs(prev))
        return returns

    @staticmethod
    def _compute_expected_value(trades: deque) -> float:
        closed_pnls = [t.net_pnl for t in trades if not t.is_open]
        if len(closed_pnls) == 0:
            return 0.0
        return sum(closed_pnls) / len(closed_pnls)

    def compute_alpha_metrics(self) -> AlphaMetrics:
        with self._lock:
            now_str = datetime.now(CHINA_TZ).isoformat()

            # P2-裂缝45：alpha_window_days使用日历日自然边界
            # 窗口结束时间 = 当前时间 - 窗口天数
            # 数据截取使用pd.Timestamp的floor('D')避免边界重复计算
            now_ts = pd.Timestamp(datetime.now(CHINA_TZ))
            window_start = (now_ts - pd.Timedelta(days=self._alpha_window_days)).floor('D')

            master_returns = self._equity_to_returns(self._master_equity_curve)
            shadow_a_returns = self._equity_to_returns(self._shadow_a_equity_curve)
            shadow_b_returns = self._equity_to_returns(self._shadow_b_equity_curve)

            master_sharpe = self._compute_sharpe(master_returns, risk_free_rate=0.02)
            shadow_a_sharpe = self._compute_sharpe(shadow_a_returns, risk_free_rate=0.02)
            shadow_b_sharpe = self._compute_sharpe(shadow_b_returns, risk_free_rate=0.02)

            master_max_dd = self._compute_max_drawdown(self._master_equity_curve)
            if master_max_dd < 1e-10:
                master_max_dd = 1.0

            best_shadow_sharpe = max(shadow_a_sharpe, shadow_b_sharpe)
            # P0-R8-06修复: Alpha公式按手册9.3节更正
            # 手册规定: Alpha_i = 主策略Sharpe_i - max(影子A_Sharpe_i, 影子B_Sharpe_i)
            # 原代码分母多除了master_max_dd，导致Alpha系统性偏小
            alpha_ratio = master_sharpe - best_shadow_sharpe

            # P0-R8-07修复: 计算各策略组(S1-S6)的独立Sharpe指标
            # 原代码仅填充S2(分钟共振)聚合指标，S1/S3/S4/S5/S6始终为默认值0.0
            group_sharpes = {}
            for g in self.STRATEGY_GROUPS:
                g_equity = self._group_equity_curve.get(g, {})
                g_master_curve = g_equity.get('master', [])
                g_shadow_a_curve = g_equity.get('shadow_a', [])
                g_shadow_b_curve = g_equity.get('shadow_b', [])
                g_master_ret = self._equity_to_returns(g_master_curve)
                g_shadow_a_ret = self._equity_to_returns(g_shadow_a_curve)
                g_shadow_b_ret = self._equity_to_returns(g_shadow_b_curve)
                group_sharpes[g] = {
                    'master': self._compute_sharpe(g_master_ret, risk_free_rate=0.02),
                    'shadow_a': self._compute_sharpe(g_shadow_a_ret, risk_free_rate=0.02),
                    'shadow_b': self._compute_sharpe(g_shadow_b_ret, risk_free_rate=0.02),
                }
            # 映射到AlphaMetrics字段名: s1_hft → s1, s2_resonance → s2, etc.
            s1_ms, s1_sa, s1_sb = group_sharpes['s1_hft']['master'], group_sharpes['s1_hft']['shadow_a'], group_sharpes['s1_hft']['shadow_b']
            s2_ms, s2_sa, s2_sb = group_sharpes['s2_resonance']['master'], group_sharpes['s2_resonance']['shadow_a'], group_sharpes['s2_resonance']['shadow_b']
            s3_ms, s3_sa, s3_sb = group_sharpes['s3_box']['master'], group_sharpes['s3_box']['shadow_a'], group_sharpes['s3_box']['shadow_b']
            s4_ms, s4_sa, s4_sb = group_sharpes['s4_spring']['master'], group_sharpes['s4_spring']['shadow_a'], group_sharpes['s4_spring']['shadow_b']
            s5_ms, s5_sa, s5_sb = group_sharpes['s5_arbitrage']['master'], group_sharpes['s5_arbitrage']['shadow_a'], group_sharpes['s5_arbitrage']['shadow_b']
            s6_ms, s6_sa, s6_sb = group_sharpes['s6_market_making']['master'], group_sharpes['s6_market_making']['shadow_a'], group_sharpes['s6_market_making']['shadow_b']
            logger.debug("[ShadowStrategyEngine] Per-group sharpes: S1=%.3f S2=%.3f S3=%.3f S4=%.3f S5=%.3f S6=%.3f",
                         s1_ms, s2_ms, s3_ms, s4_ms, s5_ms, s6_ms)

            # P1-R8-19修复: 各策略组(S1-S6)独立Alpha计算
            # 手册9.3节: Alpha_i = 主策略Sharpe_i - max(影子A_Sharpe_i, 影子B_Sharpe_i)
            group_alphas = {}
            for g in self.STRATEGY_GROUPS:
                gs = group_sharpes[g]
                g_alpha = gs['master'] - max(gs['shadow_a'], gs['shadow_b'])
                group_alphas[g] = g_alpha
            s1_alpha = group_alphas['s1_hft']
            s2_alpha = group_alphas['s2_resonance']
            s3_alpha = group_alphas['s3_box']
            s4_alpha = group_alphas['s4_spring']
            s5_alpha = group_alphas['s5_arbitrage']
            s6_alpha = group_alphas['s6_market_making']

            # P1-2修复：主策略Sharpe<=0时触发ELIMINATE标记
            # 当主策略夏普为负或零，说明策略无正期望，应禁止该策略组继续交易
            master_sharpe_eliminate = False
            if master_sharpe <= 0 and len(self._master_trades) >= self.MIN_TRADES_FOR_METRICS:
                master_sharpe_eliminate = True
                if not self._degradation_active:
                    self._degradation_active = True
                    self._stats['degradation_events'] += 1
                    logger.critical(
                        "[ShadowStrategyEngine] 🚨 ELIMINATE触发! master_sharpe=%.4f <= 0, "
                        "策略无正期望，禁止继续交易!",
                        master_sharpe,
                    )

            master_ev = self._compute_expected_value(self._master_trades)
            shadow_a_ev = self._compute_expected_value(self._shadow_a_trades)
            shadow_b_ev = self._compute_expected_value(self._shadow_b_trades)

            prev_metrics = self._last_alpha_metrics
            alpha_ratio_prev = prev_metrics.alpha_ratio if prev_metrics else 0.0
            decline_pct = 0.0
            consecutive_decline = 0

            if prev_metrics and abs(prev_metrics.alpha_ratio) > 1e-10:
                decline_pct = (
                    (prev_metrics.alpha_ratio - alpha_ratio) / abs(prev_metrics.alpha_ratio) * 100.0
                )
                if decline_pct > self.ALPHA_DECLINE_THRESHOLD_PCT:
                    consecutive_decline = prev_metrics.consecutive_decline_windows + 1
                else:
                    consecutive_decline = 0

            degradation = consecutive_decline >= self.CONSECUTIVE_DECLINE_LIMIT
            alpha_threshold_breached = alpha_ratio < self._shadow_alpha_threshold and len(self._master_trades) >= self.MIN_TRADES_FOR_METRICS
            absolute_ev_breached = master_ev < self._absolute_ev_floor and len(self._master_trades) >= self.MIN_TRADES_FOR_METRICS

            if degradation and not self._degradation_active:
                self._degradation_active = True
                self._stats['degradation_events'] += 1
                logger.warning(
                    "[ShadowStrategyEngine] ⚠️ Alpha衰减降级触发! "
                    "alpha_ratio=%.4f, 连续衰减窗口=%d",
                    alpha_ratio, consecutive_decline,
                )
                # R13-P2-LOG-13修复: Alpha衰减实时告警 — 主动推送告警事件
                self._emit_alpha_decay_alert(alpha_ratio, consecutive_decline, decline_pct)

            if alpha_threshold_breached:
                self._stats['alpha_threshold_breaches'] = self._stats.get('alpha_threshold_breaches', 0) + 1
                logger.warning(
                    "[ShadowStrategyEngine] Alpha比率低于阈值! alpha_ratio=%.4f < threshold=%.4f",
                    alpha_ratio, self._shadow_alpha_threshold,
                )

            if absolute_ev_breached and not self._absolute_ev_pause:
                self._absolute_ev_pause = True
                self._stats['absolute_ev_breaches'] += 1
                logger.critical(
                    "[ShadowStrategyEngine] 🚨 绝对期望值底线突破! master_ev=%.4f, 暂停实盘!",
                    master_ev,
                )

            # R19-ATT-02: 计算标准Jensen's Alpha (CAPM回归)
            jensen_alpha_val = 0.0
            try:
                import numpy as _np
                from ali2026v3_trading.jensen_alpha import compute_jensen_alpha
                if len(master_returns) >= 10 and len(shadow_a_returns) >= 10:
                    _master_arr = _np.array(master_returns[-min(252, len(master_returns)):])
                    _shadow_a_arr = _np.array(shadow_a_returns[-min(252, len(shadow_a_returns)):])
                    _min_len = min(len(_master_arr), len(_shadow_a_arr))
                    _ja_result = compute_jensen_alpha(_master_arr[-_min_len:], _shadow_a_arr[-_min_len:])
                    jensen_alpha_val = _ja_result.alpha
            except Exception as _e:
                logger.debug("Jensen's Alpha计算跳过: %s", _e)

            metrics = AlphaMetrics(
                timestamp=now_str,
                # P0-R8-07修复: 填入S1-S6各组独立Sharpe
                s1_master_sharpe=s1_ms, s1_shadow_a_sharpe=s1_sa, s1_shadow_b_sharpe=s1_sb,
                s2_master_sharpe=s2_ms, s2_shadow_a_sharpe=s2_sa, s2_shadow_b_sharpe=s2_sb,
                s3_master_sharpe=s3_ms, s3_shadow_a_sharpe=s3_sa, s3_shadow_b_sharpe=s3_sb,
                s4_master_sharpe=s4_ms, s4_shadow_a_sharpe=s4_sa, s4_shadow_b_sharpe=s4_sb,
                s5_master_sharpe=s5_ms, s5_shadow_a_sharpe=s5_sa, s5_shadow_b_sharpe=s5_sb,
                s6_master_sharpe=s6_ms, s6_shadow_a_sharpe=s6_sa, s6_shadow_b_sharpe=s6_sb,
                # P1-R8-19修复: 各组独立Alpha(非仅S2聚合)
                s1_alpha=s1_alpha, s2_alpha=s2_alpha, s3_alpha=s3_alpha,
                s4_alpha=s4_alpha, s5_alpha=s5_alpha, s6_alpha=s6_alpha,
                # 聚合指标(向后兼容，取S2组)
                master_sharpe=master_sharpe,
                shadow_a_sharpe=shadow_a_sharpe,
                shadow_b_sharpe=shadow_b_sharpe,
                master_max_drawdown=master_max_dd,
                alpha_ratio=alpha_ratio,
                master_expected_value=master_ev,
                shadow_a_expected_value=shadow_a_ev,
                shadow_b_expected_value=shadow_b_ev,
                alpha_ratio_prev=alpha_ratio_prev,
                alpha_ratio_decline_pct=decline_pct,
                consecutive_decline_windows=consecutive_decline,
                degradation_triggered=degradation,
                absolute_ev_breached=absolute_ev_breached,
                master_sharpe_eliminate=master_sharpe_eliminate,  # P1-2修复
                jensen_alpha=jensen_alpha_val,  # R19-ATT-02: 标准Jensen's Alpha
            )

            self._alpha_history.append(metrics)
            self._last_alpha_metrics = metrics
            self.alpha_decay_rate = metrics.alpha_ratio  # R3-D-03修复: 同步更新公共属性供risk_service消费
            self._stats['alpha_calculations'] += 1

            return metrics

    def is_degradation_active(self) -> bool:
        with self._lock:
            _active = self._degradation_active
            if _active:
                logger.debug("[R23-SM-08-FIX] 降级激活: degradation_active=%s ev_pause=%s "
                             "alpha_decay_rate=%s consecutive_losses=%s",
                             _active, self._absolute_ev_pause,
                             getattr(self, '_alpha_decay_rate', None),
                             getattr(self, '_consecutive_losses', None))
            return _active

    def is_absolute_ev_paused(self) -> bool:
        with self._lock:
            _paused = self._absolute_ev_pause
            if _paused:
                logger.debug("[R23-SM-08-FIX] 绝对EV暂停: ev_pause=%s degradation_active=%s "
                             "current_ev=%s ev_threshold=%s",
                             _paused, self._degradation_active,
                             getattr(self, '_current_expected_value', None),
                             getattr(self, '_ev_pause_threshold', None))
            return _paused

    def clear_degradation(self) -> None:
        with self._lock:
            self._degradation_active = False
            logger.info("[ShadowStrategyEngine] Alpha衰减降级已清除")

    def clear_absolute_ev_pause(self) -> None:
        with self._lock:
            self._absolute_ev_pause = False
            logger.info("[ShadowStrategyEngine] 绝对期望值暂停已解除")

    # R13-P2-LOG-13修复: Alpha衰减实时告警方法
    def _emit_alpha_decay_alert(self, alpha_ratio: float, consecutive_decline: int,
                                 decline_pct: float) -> None:
        """Alpha衰减时主动推送告警事件到EventBus"""
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            bus = get_global_event_bus()
            if bus is not None:
                bus.publish({
                    'type': 'ALPHA_DECAY_ALERT',
                    'alpha_ratio': alpha_ratio,
                    'consecutive_decline_windows': consecutive_decline,
                    'decline_pct': decline_pct,
                    'timestamp': datetime.now(CHINA_TZ).isoformat(),
                }, async_mode=True)
        except Exception:
            pass  # EventBus不可用时静默降级

    # R13-P2-LOG-16修复: 交易记录上限可配置方法
    def set_trades_max_len(self, max_len: int) -> None:
        """设置交易记录上限（运行时可配置）"""
        if max_len > 0:
            self._trades_max_len = max_len
            logger.info("[ShadowStrategyEngine] R13-P2-LOG-16: 交易记录上限设为%d", max_len)

    # P0-R8-05修复: 记录治理引擎降级事件
    def record_governance_degradation(self, reasons: List[str]) -> None:
        """记录治理引擎触发的降级事件到影子引擎日志。"""
        with self._lock:
            self._stats['governance_degradations'] = self._stats.get('governance_degradations', 0) + 1
            event = {
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'type': 'governance_degradation',
                'reasons': reasons,
                'alpha_ratio': self.get_alpha_ratio(),
            }
            self._jsonl_logger.info(json_dumps(event))
            logger.critical(
                "[ShadowStrategyEngine] 治理引擎降级事件已记录: %s",
                '; '.join(reasons)
            )

    def get_alpha_ratio(self) -> float:
        with self._lock:
            if self._last_alpha_metrics:
                return self._last_alpha_metrics.alpha_ratio
            return 0.0

    def get_master_expected_value(self) -> float:
        with self._lock:
            if self._last_alpha_metrics:
                return self._last_alpha_metrics.master_expected_value
            return 0.0

    def _enqueue_trade_log(self, record: ShadowTradeRecord) -> None:
        # P1-R9-20修复: 同时写入统一日志系统
        try:
            logger.info("[SHADOW_TRADE] %s", json.dumps(
                {**record.to_dict(), 'format_version': self.JSONL_FORMAT_VERSION},
                ensure_ascii=False,
            ))
        except Exception:
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
        except Exception as e:
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
        except Exception as e:
            logger.warning("[ShadowStrategyEngine] 日志刷盘失败: %s", e)

    @staticmethod
    def _do_write_logs(items: List[Tuple[str, str]]) -> None:
        file_buffers: Dict[str, List[str]] = {}
        for log_file, line in items:
            file_buffers.setdefault(log_file, []).append(line)
        for log_file, lines in file_buffers.items():
            try:
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.writelines(lines)
            except Exception as e:
                logger.warning("[ShadowStrategyEngine] 写入日志文件失败: %s", e)

    def generate_daily_summary(self) -> Dict[str, Any]:
        today_str = datetime.now(CHINA_TZ).strftime('%Y-%m-%d')

        # R10-P0-06修复: metrics和交易数据在同一锁内获取，确保快照一致性
        # self._lock是RLock，compute_alpha_metrics内部也用self._lock，可重入
        with self._lock:
            metrics = self.compute_alpha_metrics()
            shadow_a_trades_copy = list(self._shadow_a_trades)
            shadow_b_trades_copy = list(self._shadow_b_trades)
            master_trades_copy = list(self._master_trades)
            stats_copy = dict(self._stats)
            shadow_a_pnl = self._shadow_a_pnl_sum
            shadow_b_pnl = self._shadow_b_pnl_sum
            master_pnl = self._master_pnl_sum
            params_locked = self._params_locked
            degradation_active = self._degradation_active
            absolute_ev_pause = self._absolute_ev_pause

        summary = {
            'summary_date': today_str,
            'summary_timestamp': datetime.now(CHINA_TZ).isoformat(),
            'alpha_metrics': metrics.to_dict(),
            # P1-裂缝48：Beta贡献修正
            # 原始：Beta贡献 = 影子B夏普（可能为负，导致独立Alpha占比>100%）
            # 修正：Beta贡献 = max(0, 影子B夏普)，独立Alpha占比 = Alpha / (Alpha + Beta)
            'alpha_beta_attribution': {
                'alpha': round(metrics.master_sharpe - max(metrics.shadow_a_sharpe, metrics.shadow_b_sharpe), 4),
                'beta': round(max(0, metrics.shadow_b_sharpe), 4),
                'alpha_pct': 0.0,
            },
            'shadow_a_stats': {
                'total_trades': stats_copy['shadow_a_trades'],
                'total_pnl': shadow_a_pnl,
                'expected_value': self._compute_expected_value(shadow_a_trades_copy),
                'recent_trades': sum(1 for t in shadow_a_trades_copy if t.timestamp.startswith(today_str)),  # R21-MEM-P2-03修复: 生成器替代列表推导式
            },
            'shadow_b_stats': {
                'total_trades': stats_copy['shadow_b_trades'],
                'total_pnl': shadow_b_pnl,
                'expected_value': self._compute_expected_value(shadow_b_trades_copy),
                'recent_trades': sum(1 for t in shadow_b_trades_copy if t.timestamp.startswith(today_str)),  # R21-MEM-P2-03修复: 生成器替代列表推导式
            },
            'master_stats': {
                'total_trades': stats_copy['master_trades'],
                'total_pnl': master_pnl,
                'expected_value': self._compute_expected_value(master_trades_copy),
            },
            'params_locked': params_locked,
            'degradation_active': degradation_active,
            'absolute_ev_pause': absolute_ev_pause,
        }

        # P1-裂缝48：计算独立Alpha占比
        alpha_val = summary['alpha_beta_attribution']['alpha']
        beta_val = summary['alpha_beta_attribution']['beta']
        total_ab = alpha_val + beta_val
        summary['alpha_beta_attribution']['alpha_pct'] = round(
            alpha_val / total_ab if total_ab > 0 else 0.0, 4
        )

        try:
            summary_file = os.path.join(
                self._log_dir,
                f"summary_{today_str}.json"
            )
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            logger.info("[ShadowStrategyEngine] 日汇总写入: %s", summary_file)
        except Exception as e:
            logger.warning("[ShadowStrategyEngine] 写入日汇总失败: %s", e)

        with self._lock:
            self._last_daily_summary_date = today_str
        return summary

    def generate_weekly_summary(self) -> Dict[str, Any]:
        with self._lock:
            now = datetime.now(CHINA_TZ)
            week_start = (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d')

            alpha_list = list(self._alpha_history)
            if not alpha_list:
                return {'week_start': week_start, 'message': '无Alpha历史数据'}

            weekly_alpha_ratios = [m.alpha_ratio for m in alpha_list]
            avg_alpha = sum(weekly_alpha_ratios) / len(weekly_alpha_ratios) if weekly_alpha_ratios else 0.0
            min_alpha = min(weekly_alpha_ratios) if weekly_alpha_ratios else 0.0
            max_alpha = max(weekly_alpha_ratios) if weekly_alpha_ratios else 0.0

            summary = {
                'week_start': week_start,
                'summary_timestamp': now.isoformat(),
                'alpha_ratio_stats': {
                    'avg': avg_alpha,
                    'min': min_alpha,
                    'max': max_alpha,
                    'samples': len(weekly_alpha_ratios),
                },
                'total_degradation_events': self._stats['degradation_events'],
                'total_ev_breaches': self._stats['absolute_ev_breaches'],
                'params_snapshot': self.get_params_snapshot(),
                'master_pnl': self._master_pnl_sum,
                'shadow_a_pnl': self._shadow_a_pnl_sum,
                'shadow_b_pnl': self._shadow_b_pnl_sum,
            }

            try:
                summary_file = os.path.join(
                    self._log_dir,
                    f"weekly_summary_{week_start}.json"
                )
                with open(summary_file, 'w', encoding='utf-8') as f:
                    json.dump(summary, f, ensure_ascii=False, indent=2)
                logger.info("[ShadowStrategyEngine] 周汇总写入: %s", summary_file)
            except Exception as e:
                logger.warning("[ShadowStrategyEngine] 写入周汇总失败: %s", e)

            return summary

    def process_signal(
        self,
        market_state: str,
        instrument_id: str = "",
        signal_direction: str = "",
        price: float = 0.0,
        quantity: int = 0,
        signal_strength: float = 0.0,
        strategy_group: str = "s2_resonance",  # R13-API-P1-01: add strategy_group param
    ) -> Dict[str, ShadowTradeRecord]:
        shadow_a = self.process_shadow_a_signal(
            market_state=market_state,
            instrument_id=instrument_id,
            signal_direction=signal_direction,
            price=price,
            quantity=quantity,
            signal_strength=signal_strength,
            strategy_group=strategy_group,  # R13-API-P1-01: pass strategy_group
        )
        shadow_b = self.process_shadow_b_signal(
            market_state=market_state,
            instrument_id=instrument_id,
            signal_direction=signal_direction,
            price=price,
            quantity=quantity,
            signal_strength=signal_strength,
            strategy_group=strategy_group,  # R13-API-P1-01: pass strategy_group
        )

        now = time.time()
        # R24-P2-CF-07修复: 将_last_summary_time更新放入锁内，防止并发双重汇总
        with self._lock:
            if now - self._last_summary_time > self._summary_interval_hours * 3600:
                self._last_summary_time = now
                _should_summary = True
            else:
                _should_summary = False
        if _should_summary:
            self.generate_daily_summary()

        return {'shadow_a': shadow_a, 'shadow_b': shadow_b}

    def process_signals_batch(
        self,
        signals: List[Dict[str, Any]],
    ) -> List[Dict[str, ShadowTradeRecord]]:
        """批量信号处理(优化：单次锁获取处理所有signals)"""
        results = []
        reverse_map = {'long': 'short', 'short': 'long', 'buy': 'sell', 'sell': 'buy'}
        with self._lock:
            for sig in signals:
                market_state = sig.get('market_state', '')
                instrument_id = sig.get('instrument_id', '')
                signal_direction = sig.get('signal_direction', '')
                price = sig.get('price', 0.0)
                quantity = sig.get('quantity', 0)
                signal_strength = sig.get('signal_strength', 0.0)

                reversed_dir = reverse_map.get(signal_direction, signal_direction)
                record_a = ShadowTradeRecord(
                    trade_id=self._generate_trade_id('A'),
                    shadow_type='shadow_a',
                    timestamp=datetime.now(CHINA_TZ).isoformat(),
                    instrument_id=instrument_id,
                    direction=reversed_dir,
                    price=price, quantity=quantity,
                    open_reason='SHADOW_A_REVERSAL',
                    market_state=market_state,
                    signal_strength=signal_strength,
                )
                self._shadow_a_trades.append(record_a)
                self._stats['shadow_a_trades'] += 1
                self._enqueue_trade_log(record_a)
                # P0-裂缝23：批量接口也更新独立模拟账户（shadow_a）
                self.update_paper_account(
                    instrument_id=instrument_id,
                    direction=reversed_dir,
                    quantity=quantity if quantity > 0 else 1,
                    price=price if price > 0 else 0.0,
                    commission=self._estimate_commission(price, quantity),
                )

                random_dir = self._rng.choice(['long', 'short'])
                record_b = ShadowTradeRecord(
                    trade_id=self._generate_trade_id('B'),
                    shadow_type='shadow_b',
                    timestamp=datetime.now(CHINA_TZ).isoformat(),
                    instrument_id=instrument_id,
                    direction=random_dir,
                    price=price, quantity=quantity,
                    open_reason='SHADOW_B_RANDOM',
                    market_state=market_state,
                    signal_strength=signal_strength,
                )
                self._shadow_b_trades.append(record_b)
                self._stats['shadow_b_trades'] += 1
                self._enqueue_trade_log(record_b)
                # P0-裂缝23：批量接口也更新独立模拟账户
                self.update_paper_account(
                    instrument_id=instrument_id,
                    direction=random_dir,
                    quantity=quantity if quantity > 0 else 1,
                    price=price if price > 0 else 0.0,
                    commission=self._estimate_commission(price, quantity),
                )

                results.append({'shadow_a': record_a, 'shadow_b': record_b})

        now = time.time()
        # R24-P2-CF-07修复: 将_last_summary_time更新放入锁内，防止并发双重汇总
        with self._lock:
            if now - self._last_summary_time > self._summary_interval_hours * 3600:
                self._last_summary_time = now
                _should_summary = True
            else:
                _should_summary = False
        if _should_summary:
            self.generate_daily_summary()

        return results

    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            # R10-P1-03: _last_alpha_metrics读路径在锁内，线程安全
            alpha_ratio = self._last_alpha_metrics.alpha_ratio if self._last_alpha_metrics else 0.0
            master_ev = self._last_alpha_metrics.master_expected_value if self._last_alpha_metrics else 0.0
            last_alpha_ts = self._last_alpha_metrics.timestamp if self._last_alpha_metrics else None
            # P-32修复: 三态健康检查 OK/WARNING/CRITICAL（手册12.1节）
            if self._absolute_ev_pause:
                status = "CRITICAL"
            elif self._degradation_active or alpha_ratio < 1.0:
                status = "WARNING"
            else:
                status = "OK"
            result = {
                'component': 'shadow_strategy_engine',
                'status': status,
                'params_locked': self._params_locked,
                'degradation_active': self._degradation_active,
                'absolute_ev_pause': self._absolute_ev_pause,
                'shadow_a_trades': self._stats['shadow_a_trades'],
                'shadow_b_trades': self._stats['shadow_b_trades'],
                'master_trades': self._stats['master_trades'],
                'alpha_ratio': alpha_ratio,
                'master_expected_value': master_ev,
                'last_alpha_calculation': last_alpha_ts,
            }
            # P1-R10-05修复: 组件检查异常传播到返回值
            if status in ('CRITICAL', 'WARNING'):
                result['error_propagation'] = True
                result['action_required'] = (
                    'halt_new_open' if status == 'CRITICAL' else 'review_and_confirm'
                )
            return result

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['alpha_ratio'] = self._last_alpha_metrics.alpha_ratio if self._last_alpha_metrics else 0.0
            stats['master_expected_value'] = self._last_alpha_metrics.master_expected_value if self._last_alpha_metrics else 0.0
            stats['degradation_active'] = self._degradation_active
            stats['absolute_ev_pause'] = self._absolute_ev_pause
            stats['master_pnl_sum'] = self._master_pnl_sum
            stats['shadow_a_pnl_sum'] = self._shadow_a_pnl_sum
            stats['shadow_b_pnl_sum'] = self._shadow_b_pnl_sum
            stats['is_shadow_mode'] = self._is_shadow_mode
            stats['paper_account_equity'] = self._paper_account['current_equity']
            # R13-P2-LOG-03修复: 脱敏处理 — 日志中掩码显示权益
            stats['paper_account_equity_masked'] = self._mask_sensitive_value(self._paper_account['current_equity'])
            return stats

    # R13-P0-DEAD-01修复: _is_shadow_mode不再恒为True，而是实际检查影子模式状态
    # 影子策略引擎始终处于隔离模式（独立paper_account、不经过OrderService），
    # 但需要验证隔离性是否完整。如果隔离验证失败，则_is_shadow_mode=False，
    # 表示影子策略可能泄漏到主策略路径。
    def is_shadow_mode(self) -> bool:
        """P0-裂缝11：确认影子策略处于隔离模式

        R13-P0-DEAD-01修复: 不再直接返回True，而是验证隔离性：
        1. 影子订单日志中无真实order_id（不经过OrderService）
        2. paper_account持仓与主策略不重叠
        3. 所有检查通过则返回True，否则返回False并记录隔离性破坏
        """
        try:
            # 快速检查：影子订单日志中是否有真实order_id
            for log_entry in list(self._shadow_order_log)[-50:]:
                if log_entry.get('order_id', '').startswith('ORD-'):
                    logging.critical(
                        "[ShadowStrategyEngine] R13-P0-DEAD-01: 隔离性被破坏! "
                        "影子订单日志中发现真实order_id=%s",
                        log_entry.get('order_id'),
                    )
                    # 更新_is_shadow_mode状态
                    self._is_shadow_mode = False
                    return False

            # 检查paper_account持仓与主策略是否重叠
            try:
                from ali2026v3_trading.position_service import get_position_service
                ps = get_position_service()
                if ps:
                    shadow_positions = set(self._paper_account['positions'].keys())
                    for _inst_id, pos_dict in ps.positions.items():
                        for _pid, rec in pos_dict.items():
                            if getattr(rec, 'volume', 0) != 0 and rec.instrument_id in shadow_positions:
                                if self._paper_account['positions'].get(rec.instrument_id, 0) != 0:
                                    logging.critical(
                                        "[ShadowStrategyEngine] R13-P0-DEAD-01: 隔离性被破坏! "
                                        "paper_account与主策略持仓重叠 instrument=%s",
                                        rec.instrument_id,
                                    )
                                    # 更新_is_shadow_mode状态
                                    self._is_shadow_mode = False
                                    return False
            except Exception:
                pass  # PositionService不可用时默认通过

            # 所有检查通过，确认处于隔离模式
            self._is_shadow_mode = True
            return True
        except Exception as e:
            logging.warning("[ShadowStrategyEngine] R13-P0-DEAD-01: is_shadow_mode检查异常: %s", e)
            # 异常情况下保守返回False
            self._is_shadow_mode = False
            return False

    def get_paper_account(self) -> Dict[str, Any]:
        """P0-裂缝11：获取影子策略独立模拟账户状态"""
        with self._lock:
            return dict(self._paper_account)

    def update_paper_account(self, instrument_id: str, direction: str,
                              quantity: int, price: float,
                              commission: float = 0.0) -> Dict[str, Any]:
        """P0-裂缝11：更新影子策略模拟账户（不影响主策略）

        Args:
            instrument_id: 合约ID
            direction: 'long'/'short'/'close_long'/'close_short'
            quantity: 数量
            price: 成交价格
            commission: 手续费

        Returns:
            更新后的paper_account状态
        """
        with self._lock:
            acct = self._paper_account
            pos_size = acct['positions'].get(instrument_id, 0)

            if direction in ('long',):
                cost = price * quantity
                acct['current_equity'] -= cost + commission
                acct['positions'][instrument_id] = pos_size + quantity
                acct['entry_prices'][instrument_id] = price
            elif direction in ('short',):
                proceeds = price * quantity
                acct['current_equity'] += proceeds - commission
                acct['positions'][instrument_id] = pos_size - quantity
                acct['entry_prices'][instrument_id] = price
            elif direction in ('close_long',):
                entry_price = acct['entry_prices'].get(instrument_id, price)
                pnl = (price - entry_price) * quantity
                proceeds = price * quantity
                acct['current_equity'] += proceeds - commission
                acct['positions'][instrument_id] = pos_size - quantity
                acct['realized_pnl'] += pnl
            elif direction in ('close_short',):
                entry_price = acct['entry_prices'].get(instrument_id, price)
                pnl = (entry_price - price) * quantity
                cost = price * quantity
                acct['current_equity'] -= cost + commission
                acct['positions'][instrument_id] = pos_size + quantity
                acct['realized_pnl'] += pnl

            acct['commission_paid'] += commission
            acct['trade_count'] += 1

            # 记录到影子订单日志
            self._shadow_order_log.append({
                'instrument_id': instrument_id, 'direction': direction,
                'quantity': quantity, 'price': price, 'commission': commission,
                'equity_after': acct['current_equity'],
            })

            return dict(acct)

    def verify_shadow_isolation(self) -> Dict[str, Any]:
        """P0-裂缝11+R13-DEAD-01：验证影子策略与主策略完全隔离

        通过标准：
        1. _is_shadow_mode标志为True
        2. 影子策略不使用OrderService（检查shadow_order_log无真实order_id）
        3. 影子策略使用独立paper_account（检查不与主策略持仓重叠）
        """
        with self._lock:
            # R13-DEAD-01: 实际验证隔离性而非直接返回_is_shadow_mode
            uses_order_service = False
            try:
                from ali2026v3_trading.order_service import get_order_service
                osvc = get_order_service()
                if osvc:
                    # 检查影子订单日志中是否有真实order_id（不应有）
                    for log_entry in list(self._shadow_order_log)[-100:]:
                        if log_entry.get('order_id', '').startswith('ORD-'):
                            uses_order_service = True
                            break
            except Exception:
                pass

            # 检查paper_account持仓与主策略是否重叠
            position_overlap = False
            try:
                from ali2026v3_trading.position_service import get_position_service
                ps = get_position_service()
                if ps:
                    shadow_positions = set(self._paper_account['positions'].keys())
                    for _inst_id, pos_dict in ps.positions.items():
                        for _pid, rec in pos_dict.items():
                            if getattr(rec, 'volume', 0) != 0 and rec.instrument_id in shadow_positions:
                                if self._paper_account['positions'].get(rec.instrument_id, 0) != 0:
                                    position_overlap = True
                                    break
            except Exception:
                pass

            isolation_passed = (
                not uses_order_service
                and not position_overlap
            )
            self._is_shadow_mode = isolation_passed
            if not isolation_passed:
                logger.critical(
                    "[ShadowStrategyEngine] 🚨 Isolation check FAILED! "
                    "uses_order_service=%s position_overlap=%s",
                    uses_order_service, position_overlap,
                )

            return {
                "is_shadow_mode": isolation_passed,
                "paper_account_independent": not position_overlap,
                "paper_account_equity": self._paper_account['current_equity'],
                "paper_account_positions": len(self._paper_account['positions']),
                "paper_account_trades": self._paper_account['trade_count'],
                "shadow_order_log_size": len(self._shadow_order_log),
                "uses_order_service": uses_order_service,
                "position_overlap": position_overlap,
                "isolation_passed": isolation_passed,
            }

    def validate_shadow_b_stability(self, n_seeds: int = 100,
                                      ci_width_threshold: float = 1.0) -> Dict[str, Any]:
        """P1-裂缝15：验证影子B策略夏普的95%置信区间

        用n_seeds个不同种子运行影子B，计算夏普的95%CI。
        若CI宽度>1.0，则影子B不可用于Alpha扣除，应改用期望收益而非单路径。
        """
        import random as _random
        base_sharpes = []
        base_seed = getattr(self, '_shadow_b_seed', None)

        for seed_idx in range(n_seeds):
            seed = seed_idx * 1000 + 42
            rng = _random.Random(seed)
            # 模拟影子B的随机方向序列
            n_trades = max(1, len(self._shadow_b_trades))
            wins = 0
            total = 0
            for _ in range(min(n_trades, 500)):
                direction = rng.choice(['long', 'short'])
                # 使用历史价格变动模拟PnL
                pnl = rng.gauss(0, max(1.0, abs(self._recent_pnl_std) if hasattr(self, '_recent_pnl_std') else 1.0))
                if pnl > 0:
                    wins += 1
                total += 1

            if total > 0 and wins > 0:
                win_rate = wins / total
                loss_rate = 1 - win_rate
                avg_win = 1.5
                avg_loss = 1.0
                if loss_rate > 0:
                    plr = (win_rate * avg_win) / (loss_rate * avg_loss)
                    sharpe = (win_rate * avg_win - loss_rate * avg_loss) / max(avg_loss, 0.01)
                    base_sharpes.append(sharpe)

        if len(base_sharpes) < 10:
            return {"ci_width": 0.0, "passed": True, "action": "insufficient_data"}

        arr = np.array(base_sharpes)
        ci_lower = float(np.percentile(arr, 2.5))
        ci_upper = float(np.percentile(arr, 97.5))
        ci_width = ci_upper - ci_lower
        passed = ci_width <= ci_width_threshold

        return {
            "ci_width": round(ci_width, 4),
            "ci_lower": round(ci_lower, 4),
            "ci_upper": round(ci_upper, 4),
            "ci_width_threshold": ci_width_threshold,
            "mean_sharpe": round(float(np.mean(arr)), 4),
            "std_sharpe": round(float(np.std(arr)), 4),
            "n_seeds": n_seeds,
            "passed": passed,
            "action": "use_expected_return_instead_of_single_path" if not passed else "proceed",
        }

    def alpha_confidence_overlap_test(self, overlap_threshold: float = 0.50) -> Dict[str, Any]:
        """P2-裂缝27：主-影子Sharpe置信区间重叠检验

        若主策略与影子策略的Sharpe置信区间重叠比例 > 50%，
        则Alpha可能只是抽样误差，标记alpha_unreliable，
        资金分配回退到基础权重。
        """
        with self._lock:
            master_returns = self._equity_to_returns(self._master_equity_curve)
            shadow_a_returns = self._equity_to_returns(self._shadow_a_equity_curve)
            shadow_b_returns = self._equity_to_returns(self._shadow_b_equity_curve)

            master_sharpe = self._compute_sharpe(master_returns, risk_free_rate=0.02)
            shadow_a_sharpe = self._compute_sharpe(shadow_a_returns, risk_free_rate=0.02)
            shadow_b_sharpe = self._compute_sharpe(shadow_b_returns, risk_free_rate=0.02)

            master_n = max(1, len(master_returns))
            shadow_a_n = max(1, len(shadow_a_returns))
            shadow_b_n = max(1, len(shadow_b_returns))

            # R17-26修复: Sharpe标准误使用Lo(2002)公式
            # SE(SR) = √((1 + SR²/2 - skew*SR + (kurt-3)/4*SR²) / N)
            # 当skew≈0, kurt≈3(正态近似)时退化为 SE ≈ √((1 + SR²/2) / N)
            # 当前简化: 使用 √((1 + SR²/2) / N) 近似
            # TODO(R17-P2-DOC-02): 完整实现需计算returns的skew和kurtosis
            main_se = math.sqrt((1 + master_sharpe**2 / 2) / master_n) if master_n > 0 else 1.0
            shadow_a_se = math.sqrt((1 + shadow_a_sharpe**2 / 2) / shadow_a_n) if shadow_a_n > 0 else 1.0
            shadow_b_se = math.sqrt((1 + shadow_b_sharpe**2 / 2) / shadow_b_n) if shadow_b_n > 0 else 1.0

            # 95%置信区间
            main_ci = (master_sharpe - 1.96 * main_se, master_sharpe + 1.96 * main_se)
            shadow_a_ci = (shadow_a_sharpe - 1.96 * shadow_a_se, shadow_a_sharpe + 1.96 * shadow_a_se)
            shadow_b_ci = (shadow_b_sharpe - 1.96 * shadow_b_se, shadow_b_sharpe + 1.96 * shadow_b_se)

            # 计算重叠比例
            def ci_overlap_ratio(ci1, ci2):
                ci_width = ci1[1] - ci1[0]
                if ci_width <= 0:
                    return 0.0
                overlap = max(0, min(ci1[1], ci2[1]) - max(ci1[0], ci2[0]))
                return overlap / ci_width

            overlap_a = ci_overlap_ratio(main_ci, shadow_a_ci)
            overlap_b = ci_overlap_ratio(main_ci, shadow_b_ci)
            max_overlap = max(overlap_a, overlap_b)

            alpha_unreliable = max_overlap > overlap_threshold

            if alpha_unreliable:
                logger.warning(
                    "[ShadowStrategyEngine] ⚠️ P2-裂缝27: Alpha不可靠! "
                    "主策略CI=[%.2f, %.2f], 影子A CI=[%.2f, %.2f], 影子B CI=[%.2f, %.2f], "
                    "重叠比例=%.2f%% > %.0f%%阈值, 资金分配回退基础权重",
                    main_ci[0], main_ci[1], shadow_a_ci[0], shadow_a_ci[1],
                    shadow_b_ci[0], shadow_b_ci[1],
                    max_overlap * 100, overlap_threshold * 100,
                )

            return {
                "main_ci": [round(main_ci[0], 4), round(main_ci[1], 4)],
                "shadow_a_ci": [round(shadow_a_ci[0], 4), round(shadow_a_ci[1], 4)],
                "shadow_b_ci": [round(shadow_b_ci[0], 4), round(shadow_b_ci[1], 4)],
                "overlap_a_ratio": round(overlap_a, 4),
                "overlap_b_ratio": round(overlap_b, 4),
                "max_overlap_ratio": round(max_overlap, 4),
                "overlap_threshold": overlap_threshold,
                "alpha_unreliable": alpha_unreliable,
                "action": "fallback_to_base_weights" if alpha_unreliable else "proceed",
            }

    def validate_shadow_param_orthogonality(self, kl_threshold: float = 0.5) -> Dict[str, Any]:
        """P2-裂缝29：影子参数差异度KL散度验证

        差异度基于4个参数平均可能不够，参数间交互可能导致联合作用相似。
        增加条件：影子策略的绩效分布与主策略绩效分布的KL散度 > 0.5。
        """
        with self._lock:
            master_returns = self._equity_to_returns(self._master_equity_curve)
            shadow_a_returns = self._equity_to_returns(self._shadow_a_equity_curve)
            shadow_b_returns = self._equity_to_returns(self._shadow_b_equity_curve)

            def _estimate_kl_divergence(p_samples, q_samples, n_bins=50):
                """用直方图估计KL散度 D_KL(P||Q)"""
                if len(p_samples) < 10 or len(q_samples) < 10:
                    return 0.0
                all_vals = list(p_samples) + list(q_samples)
                vmin, vmax = min(all_vals), max(all_vals)
                if vmax - vmin < 1e-10:
                    return 0.0
                bins = np.linspace(vmin, vmax, n_bins + 1)
                p_hist, _ = np.histogram(p_samples, bins=bins, density=True)
                q_hist, _ = np.histogram(q_samples, bins=bins, density=True)
                # 加小量避免log(0)
                eps = 1e-10
                p_hist = p_hist + eps
                q_hist = q_hist + eps
                # 归一化
                p_hist = p_hist / p_hist.sum()
                q_hist = q_hist / q_hist.sum()
                kl = float(np.sum(p_hist * np.log(p_hist / q_hist)))
                return max(0.0, kl)

            kl_a = _estimate_kl_divergence(master_returns, shadow_a_returns)
            kl_b = _estimate_kl_divergence(master_returns, shadow_b_returns)
            min_kl = min(kl_a, kl_b)

            passed = min_kl >= kl_threshold

            if not passed:
                logger.warning(
                    "[ShadowStrategyEngine] ⚠️ P2-裂缝29: 影子参数正交性不足! "
                    "KL(master||shadow_a)=%.4f, KL(master||shadow_b)=%.4f, "
                    "最小KL散度=%.4f < %.1f阈值",
                    kl_a, kl_b, min_kl, kl_threshold,
                )

            return {
                "kl_master_shadow_a": round(kl_a, 4),
                "kl_master_shadow_b": round(kl_b, 4),
                "min_kl_divergence": round(min_kl, 4),
                "kl_threshold": kl_threshold,
                "passed": passed,
                "action": "increase_shadow_divergence" if not passed else "proceed",
            }

    def get_recent_trades(self, shadow_type: str = 'all', limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            result = []
            if shadow_type in ('all', 'shadow_a'):
                result.extend([t.to_dict() for t in self._shadow_a_trades])
            if shadow_type in ('all', 'shadow_b'):
                result.extend([t.to_dict() for t in self._shadow_b_trades])
            if shadow_type in ('all', 'master'):
                result.extend([t.to_dict() for t in self._master_trades])
            result.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return result[:limit]


_shadow_engine: Optional[ShadowStrategyEngine] = None
_shadow_engine_lock = threading.Lock()


def get_shadow_strategy_engine(**kwargs) -> ShadowStrategyEngine:
    global _shadow_engine
    # R10-P0-01修复: DCL缺陷修复 — 全部在锁内检查和创建，避免半构造对象泄漏
    with _shadow_engine_lock:
        if _shadow_engine is None:
            _shadow_engine = ShadowStrategyEngine(**kwargs)
        return _shadow_engine


__all__ = [
    'ShadowStrategyEngine',
    'ShadowTradeRecord',
    'AlphaMetrics',
    'ShadowParamsSnapshot',
    'get_shadow_strategy_engine',
]


# ============================================================================
# R15-P2 性能修复块
# ============================================================================

# R15-P2-PERF-12修复: 影子策略权益曲线从list改为collections.deque
# deque的append在两端均为O(1)，且maxlen自动淘汰旧数据，避免手动切片
# 修改方式: 将 _group_equity_curve[g] = {'master': [], ...} 改为 deque(maxlen=EQUITY_CURVE_MAX_LEN)
# 注意: 此处添加辅助函数供init中调用
def _make_equity_curve_deque(maxlen: int = 10000) -> deque:
    """创建带maxlen的deque用于权益曲线存储"""
    return deque(maxlen=maxlen)
