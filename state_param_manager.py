import os
import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, Optional

try:
    import yaml
except ImportError:
    yaml = None

_STATE_MAP = {
    'correct_rise': 'correct_trending',
    'correct_fall': 'correct_trending',
    'wrong_rise': 'incorrect_reversal',
    'wrong_fall': 'incorrect_reversal',
    'other': 'other',
}

_STATE_PRIORITY = {
    'correct_trending': 3,
    'incorrect_reversal': 2,
    'other': 1,
}


class StateParamManager:
    """状态驱动参数路由器

    将 width_cache 的五态分类 (correct_rise/wrong_rise/correct_fall/wrong_fall/other)
    映射为三种参数策略 (correct_trending/incorrect_reversal/other)，
    并从 state_param_sets.yaml 加载对应的参数集。

    三态切换机制(V7完善)：
    - state_confirm_bars: 新状态需连续确认N次才切换，防止单次噪声误切
    - state_check_interval_sec: 状态检查间隔（秒），控制三态判断频率
    - state_switch_position_policy: 切换时持仓处理策略
    - on_state_switch回调: 状态切换时通知策略生态系统

    切换连续性处理：
    - 切换瞬间不强制平仓已有头寸
    - 切换至 other 状态时，触发防御性减仓（止盈止损收紧至60%，暂停新开仓）
    """

    def __init__(
        self,
        yaml_path: Optional[str] = None,
        state_confirm_bars: int = 3,
        state_check_interval_sec: float = 180.0,
        state_switch_position_policy: str = 'keep_with_original_rules',
        non_other_ratio_threshold: float = 0.4,
        min_state_hold_seconds: float = 600.0,
    ):
        self._lock = threading.RLock()
        self._yaml_path = yaml_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'state_param_sets.yaml'
        )
        self._param_sets: Dict[str, Dict[str, Any]] = {}
        self._current_state: str = 'other'
        self._prev_state: str = 'other'
        self._state_enter_time: float = time.time()
        self._state_history: list = []
        self._width_cache_ref: Any = None
        self._last_switch_time: float = 0.0
        self._last_check_time: float = 0.0

        self._state_confirm_bars: int = max(1, state_confirm_bars)
        self._state_check_interval_sec: float = state_check_interval_sec
        self._state_switch_position_policy: str = state_switch_position_policy
        self._non_other_ratio_threshold: float = non_other_ratio_threshold
        self._min_state_hold_seconds: float = min_state_hold_seconds

        self._pending_state: Optional[str] = None
        self._pending_confirm_count: int = 0

        self._on_state_switch_callbacks: list = []

        self._last_resonance_strength: float = 0.0
        self._prev_resonance_strength: float = 0.0
        self._last_known_price: float = 0.0

        self._hft_transition_capture: Optional[StateTransitionCapture] = None
        try:
            self._hft_transition_capture = StateTransitionCapture()
            logging.info("[StateParamManager] HFT状态转换捕捉器已集成")
        except Exception as e:
            logging.warning("[StateParamManager] HFT状态转换捕捉器初始化失败: %s", e)

        self._stats = {
            'state_switches': 0,
            'correct_trending_count': 0,
            'incorrect_reversal_count': 0,
            'other_count': 0,
            'state_check_count': 0,
            'state_confirm_rejects': 0,
        }

        self._load_param_sets()

    def _load_param_sets(self) -> None:
        if yaml is None:
            logging.warning("[StateParamManager] PyYAML not installed, using default param sets")
            self._param_sets = self._default_param_sets()
            return

        try:
            if os.path.exists(self._yaml_path):
                with open(self._yaml_path, 'r', encoding='utf-8') as f:
                    self._param_sets = yaml.safe_load(f) or {}
                logging.info("[StateParamManager] Loaded %d param sets from %s",
                             len(self._param_sets), self._yaml_path)
            else:
                logging.warning("[StateParamManager] YAML file not found: %s, using defaults",
                                self._yaml_path)
                self._param_sets = self._default_param_sets()
        except Exception as e:
            logging.error("[StateParamManager] Failed to load YAML: %s, using defaults", e)
            self._param_sets = self._default_param_sets()

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

    def bind_width_cache(self, width_cache: Any) -> None:
        self._width_cache_ref = width_cache

    def update_market_context(self, resonance_strength: float, current_price: float) -> None:
        with self._lock:
            if resonance_strength > 0:
                self._prev_resonance_strength = self._last_resonance_strength
                self._last_resonance_strength = resonance_strength
            if current_price > 0:
                self._last_known_price = current_price

    def register_on_state_switch(self, callback: Callable[[str, str], None]) -> None:
        """注册状态切换回调。回调签名: callback(old_state, new_state)"""
        with self._lock:
            self._on_state_switch_callbacks.append(callback)

    def should_check_state(self) -> bool:
        """判断是否到达状态检查间隔"""
        now = time.time()
        if now - self._last_check_time >= self._state_check_interval_sec:
            self._last_check_time = now
            return True
        return False

    def update_state_from_width_cache(self) -> str:
        now = time.time()
        with self._lock:
            elapsed = now - self._last_check_time
        if elapsed < self._state_check_interval_sec:
            return self._current_state
        with self._lock:
            self._last_check_time = now

        if self._width_cache_ref is None:
            return self._current_state

        try:
            snapshot_method = getattr(self._width_cache_ref, 'get_status_counts_snapshot', None)
            if snapshot_method is not None:
                status_counts = snapshot_method()
            else:
                status_counts = getattr(self._width_cache_ref, '_status_counts', {})

            if not status_counts:
                return self._current_state

            total = 0
            state_tally: Dict[str, int] = {'correct_trending': 0, 'incorrect_reversal': 0, 'other': 0}

            for future_id, months_data in status_counts.items():
                if not isinstance(months_data, dict):
                    continue
                for month, types_data in months_data.items():
                    if not isinstance(types_data, dict):
                        continue
                    for opt_type, counts in types_data.items():
                        if not isinstance(counts, dict):
                            continue
                        for raw_status, count in counts.items():
                            if not isinstance(count, (int, float)):
                                continue
                            mapped = _STATE_MAP.get(raw_status, 'other')
                            state_tally[mapped] += count
                            total += count

            if total == 0:
                return self._current_state

            self._stats['state_check_count'] += 1

            non_other_total = state_tally.get('correct_trending', 0) + state_tally.get('incorrect_reversal', 0)
            if non_other_total > 0 and non_other_total / total >= self._non_other_ratio_threshold:
                candidates = {k: v for k, v in state_tally.items() if k != 'other'}
                best_state = max(candidates, key=lambda s: (candidates[s], _STATE_PRIORITY.get(s, 0)))
            else:
                best_state = 'other'

            if best_state != self._current_state:
                with self._lock:
                    hold_elapsed = time.time() - self._state_enter_time
                    if hold_elapsed < self._min_state_hold_seconds:
                        logging.debug(
                            "[StateParamManager] 状态最小持有期内，拒绝切换: %s → %s (持有%.0fs/%.0fs)",
                            self._current_state, best_state,
                            hold_elapsed, self._min_state_hold_seconds,
                        )
                        return self._current_state

                    if self._pending_state == best_state:
                        self._pending_confirm_count += 1
                    else:
                        self._pending_state = best_state
                        self._pending_confirm_count = 1

                    if self._pending_confirm_count >= self._state_confirm_bars:
                        self._switch_state(best_state)
                        self._pending_state = None
                        self._pending_confirm_count = 0
                    else:
                        logging.info(
                            "[StateParamManager] 状态确认中: %s → %s (%d/%d)",
                            self._current_state, best_state,
                            self._pending_confirm_count, self._state_confirm_bars,
                        )
            else:
                with self._lock:
                    if self._pending_state is not None:
                        self._stats['state_confirm_rejects'] += 1
                        logging.info(
                            "[StateParamManager] 状态确认中断: %s → %s 被拒绝(当前回到%s)",
                            self._current_state, self._pending_state, best_state,
                        )
                    self._pending_state = None
                    self._pending_confirm_count = 0

        except Exception as e:
            logging.warning("[StateParamManager.update_state_from_width_cache] Error: %s", e)

        return self._current_state

    def _switch_state(self, new_state: str,
                      resonance_strength: float = 0.0,
                      current_price: float = 0.0) -> None:
        effective_resonance = resonance_strength if resonance_strength > 0 else self._last_resonance_strength
        effective_price = current_price if current_price > 0 else self._last_known_price
        with self._lock:
            now = time.time()
            old_state = self._current_state
            self._prev_state = old_state
            self._current_state = new_state
            self._state_enter_time = now
            self._last_switch_time = now
            self._stats['state_switches'] += 1
            self._stats[f'{new_state}_count'] = self._stats.get(f'{new_state}_count', 0) + 1

            self._state_history.append({
                'from': self._prev_state,
                'to': new_state,
                'time': now,
                'confirm_bars': self._state_confirm_bars,
            })
            if len(self._state_history) > 100:
                self._state_history = self._state_history[-100:]

            logging.info(
                "[StateParamManager] 状态切换: %s → %s (确认%d次, correct=%d, incorrect=%d, other=%d)",
                self._prev_state, new_state, self._state_confirm_bars,
                self._stats.get('correct_trending_count', 0),
                self._stats.get('incorrect_reversal_count', 0),
                self._stats.get('other_count', 0),
            )

            if new_state == 'other':
                logging.warning("[StateParamManager] ⚠️ 进入other状态，触发防御性减仓模式")

            if self._hft_transition_capture:
                try:
                    self._hft_transition_capture.on_state_change(
                        old_state, new_state, effective_resonance, effective_price)
                except Exception as hft_e:
                    logging.debug("[StateParamManager] HFT状态转换捕捉异常: %s", hft_e)

            callbacks = list(self._on_state_switch_callbacks)

        for cb in callbacks:
            try:
                cb(old_state, new_state)
            except Exception as e:
                logging.warning("[StateParamManager] on_state_switch回调异常: %s", e)

    def get_params(self, state: Optional[str] = None) -> Dict[str, Any]:
        target = state or self._current_state
        with self._lock:
            return dict(self._param_sets.get(target, self._param_sets.get('other', {})))

    def get_current_state(self) -> str:
        with self._lock:
            return self._current_state

    def get_state_duration_sec(self) -> float:
        with self._lock:
            return time.time() - self._state_enter_time

    def is_defensive_mode(self) -> bool:
        with self._lock:
            return self._current_state == 'other'

    def get_state_distribution(self) -> Dict[str, float]:
        total = sum(self._stats.get(f'{s}_count', 0) for s in ('correct_trending', 'incorrect_reversal', 'other'))
        if total == 0:
            return {'correct_trending': 0.0, 'incorrect_reversal': 0.0, 'other': 1.0}
        return {
            s: self._stats.get(f'{s}_count', 0) / total
            for s in ('correct_trending', 'incorrect_reversal', 'other')
        }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['current_state'] = self._current_state
            stats['prev_state'] = self._prev_state
            stats['state_duration_sec'] = self.get_state_duration_sec()
            stats['distribution'] = self.get_state_distribution()
            stats['state_confirm_bars'] = self._state_confirm_bars
            stats['state_check_interval_sec'] = self._state_check_interval_sec
            stats['state_switch_position_policy'] = self._state_switch_position_policy
            stats['non_other_ratio_threshold'] = self._non_other_ratio_threshold
            stats['min_state_hold_seconds'] = self._min_state_hold_seconds
            stats['state_hold_elapsed_sec'] = time.time() - self._state_enter_time
            stats['pending_state'] = self._pending_state
            stats['pending_confirm_count'] = self._pending_confirm_count
            return stats

    def reload_param_sets(self) -> None:
        with self._lock:
            self._load_param_sets()


_state_param_manager: Optional[StateParamManager] = None
_state_param_manager_lock = threading.Lock()


def get_state_param_manager(yaml_path: Optional[str] = None, **kwargs) -> StateParamManager:
    global _state_param_manager
    if _state_param_manager is None:
        with _state_param_manager_lock:
            if _state_param_manager is None:
                _state_param_manager = StateParamManager(yaml_path=yaml_path, **kwargs)
    return _state_param_manager


__all__ = [
    'StateParamManager',
    'get_state_param_manager',
    'StateTransitionCapture',
    'StateTransitionEvent',
    'TransitionPoint',
]


class StateTransitionEvent(Enum):
    OTHER_TO_CORRECT = auto()
    OTHER_TO_INCORRECT = auto()
    CORRECT_TO_OTHER = auto()
    INCORRECT_TO_OTHER = auto()
    CORRECT_TO_INCORRECT = auto()
    INCORRECT_TO_CORRECT = auto()


@dataclass
class TransitionPoint:
    transition_id: str
    from_state: str
    to_state: str
    event_type: StateTransitionEvent
    timestamp: float
    resonance_strength: float
    price_at_transition: float
    is_consumed: bool = False


class StateTransitionCapture:
    def __init__(self, tight_stop_loss_pct: float = 0.15, quick_take_profit_ratio: float = 1.2,
                 max_hold_seconds: float = 300.0, entry_window_seconds: float = 10.0):
        self._tight_sl = tight_stop_loss_pct
        self._quick_tp = quick_take_profit_ratio
        self._max_hold = max_hold_seconds
        self._entry_window = entry_window_seconds
        self._transition_history: list = []
        self._pending_entries: Dict[str, TransitionPoint] = {}
        self._active_positions: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._stats = {
            'transitions_detected': 0, 'entries_triggered': 0, 'entries_expired': 0,
            'positions_opened': 0, 'positions_closed_tp': 0,
            'positions_closed_sl': 0, 'positions_closed_timeout': 0,
        }

    def on_state_change(self, old_state: str, new_state: str,
                        resonance_strength: float = 0.0, current_price: float = 0.0) -> Optional[Dict[str, Any]]:
        if old_state == new_state:
            return None
        event = self._classify_transition(old_state, new_state)
        if event is None:
            return None
        self._stats['transitions_detected'] += 1
        point = TransitionPoint(
            transition_id=f"TRANS_{int(time.time()*1000)}_{old_state}_{new_state}",
            from_state=old_state, to_state=new_state, event_type=event,
            timestamp=time.time(), resonance_strength=resonance_strength,
            price_at_transition=current_price,
        )
        with self._lock:
            self._transition_history.append(point)
            if len(self._transition_history) > 500:
                self._transition_history = self._transition_history[-500:]
            entry_signal = self._evaluate_entry(point)
            if entry_signal:
                self._stats['entries_triggered'] += 1
                self._pending_entries[point.transition_id] = point
        logging.info("[StateTransitionCapture] %s -> %s event=%s entry=%s strength=%.3f",
                     old_state, new_state, event.name, 'YES' if entry_signal else 'NO', resonance_strength)
        return entry_signal

    def _classify_transition(self, old_state: str, new_state: str) -> Optional[StateTransitionEvent]:
        mapping = {
            ('other', 'correct_trending'): StateTransitionEvent.OTHER_TO_CORRECT,
            ('other', 'incorrect_reversal'): StateTransitionEvent.OTHER_TO_INCORRECT,
            ('correct_trending', 'other'): StateTransitionEvent.CORRECT_TO_OTHER,
            ('incorrect_reversal', 'other'): StateTransitionEvent.INCORRECT_TO_OTHER,
            ('correct_trending', 'incorrect_reversal'): StateTransitionEvent.CORRECT_TO_INCORRECT,
            ('incorrect_reversal', 'correct_trending'): StateTransitionEvent.INCORRECT_TO_CORRECT,
        }
        return mapping.get((old_state, new_state))

    def _evaluate_entry(self, point: TransitionPoint) -> Optional[Dict[str, Any]]:
        high_value_transitions = {StateTransitionEvent.OTHER_TO_CORRECT, StateTransitionEvent.OTHER_TO_INCORRECT}
        if point.event_type not in high_value_transitions:
            return None
        if point.event_type == StateTransitionEvent.OTHER_TO_CORRECT:
            direction, reason = 'BUY', 'transition_other_to_correct'
        else:
            direction, reason = 'SELL', 'transition_other_to_incorrect'
        entry_price = point.price_at_transition
        if entry_price <= 0:
            return None
        if direction == 'BUY':
            stop_loss = entry_price * (1 - self._tight_sl)
            take_profit = entry_price * self._quick_tp
        else:
            stop_loss = entry_price * (1 + self._tight_sl)
            take_profit = entry_price / self._quick_tp
        return {
            'action': 'OPEN', 'direction': direction, 'price': entry_price, 'volume': 1,
            'stop_loss': stop_loss, 'take_profit': take_profit, 'reason': reason,
            'transition_id': point.transition_id, 'max_hold_seconds': self._max_hold,
            'event_type': point.event_type.name,
        }

    def check_position_exit(self, instrument_id: str, current_price: float) -> Optional[Dict[str, Any]]:
        with self._lock:
            pos = self._active_positions.get(instrument_id)
            if not pos:
                return None
            entry_price = pos['entry_price']
            direction = pos['direction']
            sl, tp = pos['stop_loss'], pos['take_profit']
            should_close, reason = False, ''
            if direction == 'BUY':
                if current_price >= tp:
                    should_close, reason = True, 'transition_take_profit'
                elif current_price <= sl:
                    should_close, reason = True, 'transition_stop_loss'
            else:
                if current_price <= tp:
                    should_close, reason = True, 'transition_take_profit'
                elif current_price >= sl:
                    should_close, reason = True, 'transition_stop_loss'
            hold_time = time.time() - pos['entry_time']
            if hold_time > self._max_hold:
                should_close, reason = True, 'transition_timeout'
            if should_close:
                del self._active_positions[instrument_id]
                if 'take_profit' in reason:
                    self._stats['positions_closed_tp'] += 1
                elif 'stop_loss' in reason:
                    self._stats['positions_closed_sl'] += 1
                else:
                    self._stats['positions_closed_timeout'] += 1
                close_direction = 'SELL' if direction == 'BUY' else 'BUY'
                pnl = (current_price - entry_price if direction == 'BUY' else entry_price - current_price)
                return {
                    'action': 'CLOSE', 'instrument_id': instrument_id, 'direction': close_direction,
                    'volume': pos['volume'], 'price': current_price, 'reason': reason, 'pnl': pnl,
                }
        return None

    def register_position(self, instrument_id: str, entry: Dict[str, Any]) -> None:
        with self._lock:
            self._active_positions[instrument_id] = {
                'entry_price': entry['price'], 'direction': entry['direction'],
                'volume': entry.get('volume', 1), 'stop_loss': entry['stop_loss'],
                'take_profit': entry['take_profit'], 'entry_time': time.time(),
                'transition_id': entry.get('transition_id', ''),
            }
            self._stats['positions_opened'] += 1

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            self._cleanup_expired_pending()
            return {
                'service_name': 'StateTransitionCapture', **self._stats,
                'active_positions': len(self._active_positions),
                'pending_entries': len(self._pending_entries),
                'transition_history_size': len(self._transition_history),
            }

    def _cleanup_expired_pending(self, ttl_seconds: float = 60.0) -> None:
        now = time.time()
        expired = [tid for tid, p in self._pending_entries.items()
                   if now - p.timestamp > ttl_seconds]
        for tid in expired:
            del self._pending_entries[tid]
            self._stats['entries_expired'] += 1


class StateTransitionAnalytics:
    """状态转换分析：首次切换记录+驻留时间+转换概率矩阵

    首次切换：记录每个品种首次从other→correct_trending的时刻，
    这是高频策略最有利可图的入场窗口。
    驻留时间：每个状态的停留时长分布，用于优化信号确认窗口。
    转换概率矩阵：P(state_t+1 | state_t)，用于预测状态切换概率。
    """

    STATES = ('other', 'correct_trending', 'incorrect_reversal')

    def __init__(self):
        self._first_switches: Dict[str, Dict[str, float]] = {}
        self._dwell_times: Dict[str, List[float]] = {s: [] for s in self.STATES}
        self._current_state_entry: Dict[str, float] = {}
        self._transition_counts: Dict[str, Dict[str, int]] = {
            s1: {s2: 0 for s2 in self.STATES} for s1 in self.STATES
        }
        self._stats = {'total_transitions': 0, 'first_switches': 0}

    def on_state_change(self, instrument_id: str,
                        old_state: str, new_state: str,
                        timestamp: float = 0.0) -> Dict[str, Any]:
        result = {}
        if timestamp <= 0:
            timestamp = time.time()

        if old_state != new_state:
            self._transition_counts.setdefault(old_state, {}).setdefault(new_state, 0)
            self._transition_counts[old_state][new_state] += 1
            self._stats['total_transitions'] += 1

        if old_state in self._current_state_entry:
            dwell = timestamp - self._current_state_entry[old_state]
            self._dwell_times.setdefault(old_state, []).append(dwell)
            result['dwell_time'] = dwell
        self._current_state_entry[new_state] = timestamp

        if old_state == 'other' and new_state == 'correct_trending':
            if instrument_id not in self._first_switches:
                self._first_switches[instrument_id] = {
                    'timestamp': timestamp, 'from': old_state, 'to': new_state,
                }
                self._stats['first_switches'] += 1
                result['is_first_switch'] = True

        return result

    def get_transition_matrix(self) -> Dict[str, Dict[str, float]]:
        matrix = {}
        for s1 in self.STATES:
            total = sum(self._transition_counts.get(s1, {}).get(s2, 0)
                        for s2 in self.STATES)
            matrix[s1] = {}
            for s2 in self.STATES:
                cnt = self._transition_counts.get(s1, {}).get(s2, 0)
                matrix[s1][s2] = round(cnt / max(1, total), 4)
        return matrix

    def get_avg_dwell_times(self) -> Dict[str, float]:
        return {
            s: round(sum(times) / max(1, len(times)), 2)
            for s, times in self._dwell_times.items() if times
        }

    def get_stats(self) -> Dict[str, Any]:
        return {
            'service_name': 'StateTransitionAnalytics', **self._stats,
            'first_switch_count': len(self._first_switches),
            'avg_dwell_times': self.get_avg_dwell_times(),
            'transition_matrix': self.get_transition_matrix(),
        }
