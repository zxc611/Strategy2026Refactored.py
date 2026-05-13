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
import os
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml
except ImportError:
    yaml = None


logger = logging.getLogger(__name__)


@dataclass
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


@dataclass
class AlphaMetrics:
    """Alpha比率监控指标"""
    timestamp: str
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

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
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
    """影子策略监控引擎

    职责：
    1. 维护两个影子策略(反向逻辑A + 随机动作B)的独立运行环境
    2. 参数独立锁定：Walk-forward后冻结，不随主策略更新
    3. 每笔交易记录到独立日志
    4. 计算Alpha比率和绝对期望值
    5. 监控Alpha衰减，触发自动降级
    """

    ALPHA_DECLINE_THRESHOLD_PCT = 20.0
    CONSECUTIVE_DECLINE_LIMIT = 2
    MIN_TRADES_FOR_METRICS = 5

    def __init__(
        self,
        log_dir: Optional[str] = None,
        yaml_path: Optional[str] = None,
        alpha_window_days: int = 7,
        summary_interval_hours: int = 24,
    ):
        self._lock = threading.RLock()
        self._yaml_path = yaml_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'state_param_sets.yaml'
        )

        self._log_dir = log_dir or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'shadow'
        )
        os.makedirs(self._log_dir, exist_ok=True)

        self._alpha_window_days = alpha_window_days
        self._summary_interval_hours = summary_interval_hours

        self._shadow_a_params: Optional[ShadowParamsSnapshot] = None
        self._shadow_b_params: Optional[ShadowParamsSnapshot] = None
        self._params_locked: bool = False

        self._shadow_a_trades: deque = deque(maxlen=10000)
        self._shadow_b_trades: deque = deque(maxlen=10000)
        self._master_trades: deque = deque(maxlen=10000)

        self._alpha_history: deque = deque(maxlen=1000)
        self._last_alpha_metrics: Optional[AlphaMetrics] = None

        self._master_equity_curve: List[float] = []
        self._shadow_a_equity_curve: List[float] = []
        self._shadow_b_equity_curve: List[float] = []

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

        self._log_write_queue: deque = deque(maxlen=50000)
        self._log_writer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='shadow_log')
        self._log_flush_counter: int = 0
        self._LOG_FLUSH_INTERVAL: int = 100

        self._load_and_lock_params()

        logger.info("[ShadowStrategyEngine] 初始化完成, log_dir=%s", self._log_dir)

    def _generate_trade_id(self, shadow_type: str) -> str:
        self._trade_id_counter += 1
        ts = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"SHADOW-{shadow_type}-{ts}-{self._trade_id_counter:06d}"

    def _compute_param_hash(self, param_set: Dict[str, Any]) -> str:
        import hashlib
        content = json.dumps(param_set, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    def _load_and_lock_params(self) -> None:
        param_sets = self._load_yaml_param_sets()
        now_str = datetime.now().isoformat()

        shadow_a_set = copy.deepcopy(param_sets.get('incorrect_reversal', {}))
        shadow_b_set = copy.deepcopy(param_sets.get('correct_trending', {}))

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
                    return yaml.safe_load(f) or {}
            else:
                logger.warning("[ShadowStrategyEngine] YAML文件不存在: %s", self._yaml_path)
                return self._default_param_sets()
        except Exception as e:
            logger.error("[ShadowStrategyEngine] 加载YAML失败: %s", e)
            return self._default_param_sets()

    @staticmethod
    def _default_param_sets() -> Dict[str, Dict[str, Any]]:
        return {
            'correct_trending': {
                'option_width_min_threshold': 2.0,
                'signal_cooldown_sec': 15,
                'close_take_profit_ratio': 2.5,
                'close_stop_loss_ratio': 0.4,
                'max_risk_ratio': 0.8,
                'max_signals_per_window': 10,
                'lots_min': 5,
            },
            'incorrect_reversal': {
                'option_width_min_threshold': 4.0,
                'signal_cooldown_sec': 120,
                'close_take_profit_ratio': 1.3,
                'close_stop_loss_ratio': 0.6,
                'max_risk_ratio': 0.3,
                'max_signals_per_window': 3,
                'lots_min': 2,
            },
            'other': {
                'option_width_min_threshold': 4.0,
                'signal_cooldown_sec': 300,
                'close_take_profit_ratio': 1.1,
                'close_stop_loss_ratio': 0.8,
                'max_risk_ratio': 0.2,
                'max_signals_per_window': 2,
                'lots_min': 1,
            },
        }

    def are_params_locked(self) -> bool:
        with self._lock:
            return self._params_locked

    def get_shadow_a_params(self) -> Dict[str, Any]:
        with self._lock:
            if self._shadow_a_params is None:
                return {}
            return copy.deepcopy(self._shadow_a_params.param_set)

    def get_shadow_b_params(self) -> Dict[str, Any]:
        with self._lock:
            if self._shadow_b_params is None:
                return {}
            return copy.deepcopy(self._shadow_b_params.param_set)

    def get_params_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'shadow_a': self._shadow_a_params.to_dict() if self._shadow_a_params else {},
                'shadow_b': self._shadow_b_params.to_dict() if self._shadow_b_params else {},
                'params_locked': self._params_locked,
            }

    def relock_params(self, new_yaml_path: Optional[str] = None) -> None:
        with self._lock:
            if new_yaml_path:
                self._yaml_path = new_yaml_path
            self._load_and_lock_params()
            logger.warning(
                "[ShadowStrategyEngine] 参数重新锁定(谨慎使用!): yaml=%s",
                self._yaml_path,
            )

    def process_shadow_a_signal(
        self,
        market_state: str,
        instrument_id: str = "",
        signal_direction: str = "",
        price: float = 0.0,
        quantity: int = 0,
        signal_strength: float = 0.0,
    ) -> ShadowTradeRecord:
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
                shadow_type='shadow_a',
                timestamp=datetime.now().isoformat(),
                instrument_id=instrument_id,
                direction=reversed_dir,
                price=price,
                quantity=quantity,
                open_reason='SHADOW_A_REVERSAL',
                market_state=market_state,
                signal_strength=signal_strength,
            )
            self._shadow_a_trades.append(record)
            self._stats['shadow_a_trades'] += 1

            self._enqueue_trade_log(record)
            return record

    def process_shadow_b_signal(
        self,
        market_state: str,
        instrument_id: str = "",
        signal_direction: str = "",
        price: float = 0.0,
        quantity: int = 0,
        signal_strength: float = 0.0,
    ) -> ShadowTradeRecord:
        with self._lock:
            random_dir = random.choice(['long', 'short'])

            record = ShadowTradeRecord(
                trade_id=self._generate_trade_id('B'),
                shadow_type='shadow_b',
                timestamp=datetime.now().isoformat(),
                instrument_id=instrument_id,
                direction=random_dir,
                price=price,
                quantity=quantity,
                open_reason='SHADOW_B_RANDOM',
                market_state=market_state,
                signal_strength=signal_strength,
            )
            self._shadow_b_trades.append(record)
            self._stats['shadow_b_trades'] += 1

            self._enqueue_trade_log(record)
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
    ) -> ShadowTradeRecord:
        with self._lock:
            self._master_pnl_sum += pnl

            record = ShadowTradeRecord(
                trade_id=self._generate_trade_id('MASTER'),
                shadow_type='master',
                timestamp=datetime.now().isoformat(),
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
            self._master_trades.append(record)
            self._stats['master_trades'] += 1

            self._enqueue_trade_log(record)
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
                self._shadow_a_pnl_sum += net_pnl
            elif shadow_type == 'shadow_b':
                trades = self._shadow_b_trades
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

            max_len = 10000
            if len(self._master_equity_curve) > max_len:
                self._master_equity_curve = self._master_equity_curve[-max_len:]
                self._shadow_a_equity_curve = self._shadow_a_equity_curve[-max_len:]
                self._shadow_b_equity_curve = self._shadow_b_equity_curve[-max_len:]

    @staticmethod
    def _compute_sharpe(returns: List[float], annualize_factor: float = 252.0) -> float:
        if len(returns) < 2:
            return 0.0
        mean_r = sum(returns) / len(returns)
        var_r = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        std_r = math.sqrt(var_r) if var_r > 0 else 0.0
        if std_r < 1e-10:
            return 0.0
        sharpe = (mean_r / std_r) * math.sqrt(annualize_factor)
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
            if abs(prev) < 1e-10:
                returns.append(0.0)
            else:
                returns.append((equity_curve[i] - prev) / abs(prev))
        return returns

    @staticmethod
    def _compute_expected_value(trades: deque) -> float:
        closed_pnls = [t.net_pnl for t in trades if not t.is_open]
        if len(closed_pnls) == 0:
            return 0.0
        return sum(closed_pnls) / len(closed_pnls)

    def compute_alpha_metrics(self) -> AlphaMetrics:
        with self._lock:
            now_str = datetime.now().isoformat()

            master_returns = self._equity_to_returns(self._master_equity_curve)
            shadow_a_returns = self._equity_to_returns(self._shadow_a_equity_curve)
            shadow_b_returns = self._equity_to_returns(self._shadow_b_equity_curve)

            master_sharpe = self._compute_sharpe(master_returns)
            shadow_a_sharpe = self._compute_sharpe(shadow_a_returns)
            shadow_b_sharpe = self._compute_sharpe(shadow_b_returns)

            master_max_dd = self._compute_max_drawdown(self._master_equity_curve)
            if master_max_dd < 1e-10:
                master_max_dd = 1.0

            best_shadow_sharpe = max(shadow_a_sharpe, shadow_b_sharpe)
            alpha_ratio = (master_sharpe - best_shadow_sharpe) / master_max_dd

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
            absolute_ev_breached = master_ev < 0 and len(self._master_trades) >= self.MIN_TRADES_FOR_METRICS

            if degradation and not self._degradation_active:
                self._degradation_active = True
                self._stats['degradation_events'] += 1
                logger.warning(
                    "[ShadowStrategyEngine] ⚠️ Alpha衰减降级触发! "
                    "alpha_ratio=%.4f, 连续衰减窗口=%d",
                    alpha_ratio, consecutive_decline,
                )

            if absolute_ev_breached and not self._absolute_ev_pause:
                self._absolute_ev_pause = True
                self._stats['absolute_ev_breaches'] += 1
                logger.critical(
                    "[ShadowStrategyEngine] 🚨 绝对期望值底线突破! master_ev=%.4f, 暂停实盘!",
                    master_ev,
                )

            metrics = AlphaMetrics(
                timestamp=now_str,
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
            )

            self._alpha_history.append(metrics)
            self._last_alpha_metrics = metrics
            self._stats['alpha_calculations'] += 1

            return metrics

    def is_degradation_active(self) -> bool:
        with self._lock:
            return self._degradation_active

    def is_absolute_ev_paused(self) -> bool:
        with self._lock:
            return self._absolute_ev_pause

    def clear_degradation(self) -> None:
        with self._lock:
            self._degradation_active = False
            logger.info("[ShadowStrategyEngine] Alpha衰减降级已清除")

    def clear_absolute_ev_pause(self) -> None:
        with self._lock:
            self._absolute_ev_pause = False
            logger.info("[ShadowStrategyEngine] 绝对期望值暂停已解除")

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
        try:
            log_file = os.path.join(
                self._log_dir,
                f"shadow_{record.shadow_type}_{datetime.now().strftime('%Y%m%d')}.jsonl"
            )
            line = json.dumps(record.to_dict(), ensure_ascii=False) + '\n'
            self._log_write_queue.append((log_file, line))
            self._log_flush_counter += 1
            if self._log_flush_counter >= self._LOG_FLUSH_INTERVAL:
                self._log_flush_counter = 0
                self._async_flush_log_queue()
        except Exception as e:
            logger.warning("[ShadowStrategyEngine] 入队交易日志失败: %s", e)

    def _async_flush_log_queue(self) -> None:
        try:
            items = []
            while self._log_write_queue:
                items.append(self._log_write_queue.popleft())
            if items:
                self._log_writer_executor.submit(self._do_write_logs, items)
        except Exception as e:
            logger.warning("[ShadowStrategyEngine] 异步刷盘提交失败: %s", e)

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
        with self._lock:
            today_str = datetime.now().strftime('%Y-%m-%d')

            metrics = self.compute_alpha_metrics()

            summary = {
                'summary_date': today_str,
                'summary_timestamp': datetime.now().isoformat(),
                'alpha_metrics': metrics.to_dict(),
                'shadow_a_stats': {
                    'total_trades': self._stats['shadow_a_trades'],
                    'total_pnl': self._shadow_a_pnl_sum,
                    'expected_value': self._compute_expected_value(self._shadow_a_trades),
                    'recent_trades': len([t for t in self._shadow_a_trades if t.timestamp.startswith(today_str)]),
                },
                'shadow_b_stats': {
                    'total_trades': self._stats['shadow_b_trades'],
                    'total_pnl': self._shadow_b_pnl_sum,
                    'expected_value': self._compute_expected_value(self._shadow_b_trades),
                    'recent_trades': len([t for t in self._shadow_b_trades if t.timestamp.startswith(today_str)]),
                },
                'master_stats': {
                    'total_trades': self._stats['master_trades'],
                    'total_pnl': self._master_pnl_sum,
                    'expected_value': self._compute_expected_value(self._master_trades),
                },
                'params_locked': self._params_locked,
                'degradation_active': self._degradation_active,
                'absolute_ev_pause': self._absolute_ev_pause,
            }

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

            self._last_daily_summary_date = today_str
            return summary

    def generate_weekly_summary(self) -> Dict[str, Any]:
        with self._lock:
            now = datetime.now()
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
    ) -> Dict[str, ShadowTradeRecord]:
        shadow_a = self.process_shadow_a_signal(
            market_state=market_state,
            instrument_id=instrument_id,
            signal_direction=signal_direction,
            price=price,
            quantity=quantity,
            signal_strength=signal_strength,
        )
        shadow_b = self.process_shadow_b_signal(
            market_state=market_state,
            instrument_id=instrument_id,
            signal_direction=signal_direction,
            price=price,
            quantity=quantity,
            signal_strength=signal_strength,
        )

        now = time.time()
        if now - self._last_summary_time > self._summary_interval_hours * 3600:
            self.generate_daily_summary()
            self._last_summary_time = now

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
                    timestamp=datetime.now().isoformat(),
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

                random_dir = random.choice(['long', 'short'])
                record_b = ShadowTradeRecord(
                    trade_id=self._generate_trade_id('B'),
                    shadow_type='shadow_b',
                    timestamp=datetime.now().isoformat(),
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

                results.append({'shadow_a': record_a, 'shadow_b': record_b})

        now = time.time()
        if now - self._last_summary_time > self._summary_interval_hours * 3600:
            self.generate_daily_summary()
            self._last_summary_time = now

        return results

    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            alpha_ratio = self._last_alpha_metrics.alpha_ratio if self._last_alpha_metrics else 0.0
            master_ev = self._last_alpha_metrics.master_expected_value if self._last_alpha_metrics else 0.0
            last_alpha_ts = self._last_alpha_metrics.timestamp if self._last_alpha_metrics else None
            return {
                'component': 'shadow_strategy_engine',
                'status': 'OK' if not self._absolute_ev_pause else 'CRITICAL',
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
            return stats

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
    if _shadow_engine is None:
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
