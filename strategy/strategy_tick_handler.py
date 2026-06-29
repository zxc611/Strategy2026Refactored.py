"""
Tick数据处理模块 (G2b Mixin Elimination - 完成)

TickHandlerMixin 已删除，所有逻辑委托到 TickProcessingService。
此模块保留:
- TickHandlerMixin: 向后兼容别名 = TickProcessingService
- DynamicPursuitEngine: 独立类
- PyramidAddPositionEngine: 独立类
- PursuitPosition: 数据类
- check_hard_time_stop_for_position(): 独立函数
- check_hmm_dwell_anomaly(): 独立函数
"""

import logging
import math
import time
import threading
import uuid
from datetime import datetime
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

# R27-P0-FP-01修复: 浮点容差常量，止盈止损比较使用
_PRICE_TOLERANCE = 1e-6

# R27-P1修复: 导入容错/浮点工具
from ali2026v3_trading.infra.resilience import (
    TimeoutGuard, Watchdog, HeartbeatMonitor,
    stable_sum, stable_mean, stable_variance,
    approx_equal, approx_less, approx_greater,
    should_trigger_stop_loss, should_trigger_take_profit,
    KahanSummation, safe_divide, PRICE_TOLERANCE as _RESILIENCE_TOLERANCE,
    get_signal_lifecycle, SignalLifecycleManager,
    deterministic_round, safe_float_to_int,
)
from ali2026v3_trading.config import config_params
from ali2026v3_trading.strategy_judgment.causal_chain_utils import (
    CausalChainTracker, ContaminationGuard, CyclicDependencyGuard,
    validate_tick_cascade, CausalEvent,
)


from ali2026v3_trading.strategy.tick_processing_service import (
    TickProcessingService, MarketEvent, TickEvent, BarCompletedEvent,
)

__all__ = ['TickHandlerMixin', 'DynamicPursuitEngine', 'PursuitPosition',
           'MarketEvent', 'TickEvent', 'BarCompletedEvent', 'TickProcessingService']

logger = get_logger(__name__)  # R9-5


TickHandlerMixin = TickProcessingService


@dataclass(slots=True)
class PursuitPosition:
    position_id: str
    instrument_id: str
    direction: str
    entries: List[Dict[str, Any]]
    total_volume: int
    weighted_avg_price: float
    current_stop_profit: float
    current_stop_loss: float
    peak_strength: float
    is_open: bool = True
    created_at: float = field(default_factory=time.time)
    platform_confirmed: bool = False
    platform_order_ids: List[str] = field(default_factory=list)


# R27-P0-FC-01修复: 实盘硬时间止损检查入口函数
def check_hard_time_stop_for_position(risk_service, position_id: str, open_time: float,
                                       max_profit_reached: float, profit_slope: float = 0.0,
                                       peak_profit_pct: float = 0.0, current_profit_pct: float = 0.0,
                                       bar_time: Optional[float] = None, strategy_group: str = '') -> Optional[str]:
    """实盘两阶段硬时间止损检查入口，调用SafetyMetaLayer.check_position_hard_time_stop"""
    safety = getattr(risk_service, '_safety_meta_layer', None)
    if safety is None:
        return None
    try:
        return safety.check_position_hard_time_stop(
            position_id, open_time, max_profit_reached,
            profit_slope, peak_profit_pct, current_profit_pct,
            bar_time=bar_time, strategy_group=strategy_group
        )
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logging.warning("[R27-P0-FC-01] 硬时间止损检查异常: %s", e)
        return None


class DynamicPursuitEngine:
    def __init__(self, surge_threshold: float = 0.3, max_add_positions: int = 3,
                 add_volume_ratio: float = 0.5, stop_profit_trail_ratio: float = 0.3,
                 max_total_position_pct: float = 0.15, tight_stop_loss_pct: float = 0.15):
        self._surge_threshold = surge_threshold
        self._max_add_positions = max_add_positions
        self._add_volume_ratio = add_volume_ratio
        self._stop_profit_trail_ratio = stop_profit_trail_ratio
        self._max_total_position_pct = max_total_position_pct
        self._tight_sl_pct = tight_stop_loss_pct
        self._positions: Dict[str, PursuitPosition] = {}
        self._lock = threading.RLock()
        self._stats = {
            'total_pursuit_entries': 0, 'surge_detected': 0,
            'stop_profit_trails': 0, 'positions_closed': 0,
        }

    def evaluate_surge(self, instrument_id: str, current_strength: float,
                       prev_strength: float, current_price: float,
                       direction: str, account_equity: float = 100000.0) -> Optional[Dict[str, Any]]:
        if direction not in ('BUY', 'SELL'):
            logging.warning("[DynamicPursuitEngine] Invalid direction '%s', rejected", direction)
            return None
        if current_price <= 0:
            return None
        strength_delta = current_strength - prev_strength
        if strength_delta < self._surge_threshold:
            return None
        self._stats['surge_detected'] += 1
        with self._lock:
            pos = self._positions.get(instrument_id)
            if pos and pos.is_open:
                if pos.direction != direction:
                    return None
                add_count = len(pos.entries) - 1
                if add_count >= self._max_add_positions:
                    return None
                total_exposure = sum(e['volume'] * e['price'] for e in pos.entries)
                if account_equity > 0 and total_exposure / account_equity > self._max_total_position_pct:
                    return None
                base_volume = pos.entries[0]['volume']
                add_volume = max(1, int(base_volume * self._add_volume_ratio))
                new_stop_profit = self._calc_trailing_stop(pos.weighted_avg_price, current_price, pos.direction)
                pos.entries.append({
                    'price': current_price, 'volume': add_volume, 'strength': current_strength,
                    'strength_delta': strength_delta, 'timestamp': time.time(), 'entry_type': 'pursuit_add',
                })
                pos.total_volume += add_volume
                pos.weighted_avg_price = self._recalc_avg_price(pos.entries)
                pos.current_stop_profit = new_stop_profit
                pos.peak_strength = max(pos.peak_strength, current_strength)
                self._stats['total_pursuit_entries'] += 1
                return {
                    'action': 'ADD_POSITION', 'instrument_id': instrument_id, 'direction': direction,
                    'volume': add_volume, 'price': current_price, 'new_stop_profit': new_stop_profit,
                    'total_volume': pos.total_volume, 'avg_price': pos.weighted_avg_price,
                    'strength_delta': strength_delta,
                }
            else:
                stop_profit = self._calc_initial_stop(current_price, direction)
                # FIX-R37-UNIQUE-ID: 增加随机熵，避免同毫秒同合约pos_id冲突导致持仓覆盖
                from ali2026v3_trading.infra.shared_utils import generate_prefixed_id as _gen_id
                pos = PursuitPosition(
                    position_id=f"PURSUIT_{instrument_id}_{int(time.time()*1000)}_{_gen_id('', 8)}",
                    instrument_id=instrument_id, direction=direction,
                    entries=[{'price': current_price, 'volume': 1, 'strength': current_strength,
                              'strength_delta': strength_delta, 'timestamp': time.time(), 'entry_type': 'initial'}],
                    total_volume=1, weighted_avg_price=current_price,
                    current_stop_profit=stop_profit,
                    current_stop_loss=self._calc_initial_stop_loss(current_price, direction),
                    peak_strength=current_strength,
                )
                self._positions[instrument_id] = pos
                self._stats['total_pursuit_entries'] += 1
                return {
                    'action': 'OPEN_POSITION', 'instrument_id': instrument_id, 'direction': direction,
                    'volume': 1, 'price': current_price, 'stop_profit': stop_profit,
                    'strength_delta': strength_delta,
                }
        return None

    def update_trailing_stop(self, instrument_id: str, current_price: float, direction: str = '') -> Optional[float]:
        with self._lock:
            pos = self._positions.get(instrument_id)
            if not pos or not pos.is_open:
                return None
            pos_dir = pos.direction
            # R27-P0-FP-01修复: 使用浮点容差比较
            if pos_dir == 'BUY' and current_price <= pos.weighted_avg_price + _PRICE_TOLERANCE:
                return None
            if pos_dir == 'SELL' and current_price >= pos.weighted_avg_price - _PRICE_TOLERANCE:
                return None
            new_sp = self._calc_trailing_stop(pos.weighted_avg_price, current_price, pos_dir)
            improved = (new_sp > pos.current_stop_profit) if pos_dir == 'BUY' else (new_sp < pos.current_stop_profit)
            if improved:
                pos.current_stop_profit = new_sp
                self._stats['stop_profit_trails'] += 1
                return new_sp
        return None

    def check_exit(self, instrument_id: str, current_price: float) -> Optional[Dict[str, Any]]:
        with self._lock:
            pos = self._positions.get(instrument_id)
            if not pos or not pos.is_open:
                return None
            if not pos.platform_confirmed:
                pending_sec = time.time() - pos.created_at
                if pending_sec < 30.0:
                    return None
                pos.is_open = False
                self._stats['positions_closed'] += 1
                logging.warning("[DynamicPursuitEngine] %s exit unconfirmed position (timed out %.0fs)",
                                instrument_id, pending_sec)
                return None
            direction = pos.direction
            should_exit = False
            reason = ''
            if direction == 'BUY':
                # R27-P0-FP-01修复: 使用浮点容差比较，防止因浮点精度导致止盈止损误触发/漏触发
                if current_price > pos.current_stop_profit - _PRICE_TOLERANCE:
                    should_exit, reason = True, 'pursuit_take_profit'
                elif current_price < pos.current_stop_loss + _PRICE_TOLERANCE:
                    should_exit, reason = True, 'pursuit_stop_loss'
            else:
                # R27-P0-FP-01修复: 使用浮点容差比较
                if current_price > pos.current_stop_loss - _PRICE_TOLERANCE:
                    should_exit, reason = True, 'pursuit_stop_loss'
                elif current_price < pos.current_stop_profit + _PRICE_TOLERANCE:
                    should_exit, reason = True, 'pursuit_take_profit'
            if should_exit:
                pos.is_open = False
                self._stats['positions_closed'] += 1
                pnl = self._calc_pnl(pos, current_price)
                return {
                    'action': 'CLOSE_ALL', 'instrument_id': instrument_id,
                    'direction': 'SELL' if direction == 'BUY' else 'BUY',
                    'volume': pos.total_volume, 'price': current_price,
                    'reason': reason, 'pnl': pnl, 'entries': len(pos.entries),
                    'platform_order_ids': list(pos.platform_order_ids),
                }
        return None

    def confirm_position_on_platform(self, instrument_id: str, order_id: str) -> bool:
        with self._lock:
            pos = self._positions.get(instrument_id)
            if not pos or not pos.is_open:
                return False
            pos.platform_confirmed = True
            if order_id:
                pos.platform_order_ids.append(order_id)
            return True

    def add_platform_order_id(self, instrument_id: str, order_id: str) -> bool:
        with self._lock:
            pos = self._positions.get(instrument_id)
            if not pos or not pos.is_open:
                return False
            if order_id:
                pos.platform_order_ids.append(order_id)
            pos.platform_confirmed = True
            return True

    def _calc_trailing_stop(self, avg_price: float, current_price: float, direction: str) -> float:
        if direction == 'BUY':
            profit = current_price - avg_price
            if profit <= 0:
                return avg_price
            return current_price - profit * self._stop_profit_trail_ratio
        profit = avg_price - current_price
        if profit <= 0:
            return avg_price
        return avg_price - profit * self._stop_profit_trail_ratio

    def _calc_initial_stop(self, price: float, direction: str) -> float:
        if direction == 'BUY':
            return price * (1 + 0.005)
        return price * (1 - 0.005)

    def _calc_initial_stop_loss(self, price: float, direction: str) -> float:
        if direction == 'BUY':
            return price - price * self._tight_sl_pct
        return price + price * self._tight_sl_pct

    def _recalc_avg_price(self, entries: List[Dict]) -> float:
        total_vol = sum(e['volume'] for e in entries)
        if total_vol <= 0:
            return 0.0
        return sum(e['volume'] * e['price'] for e in entries) / total_vol

    def _calc_pnl(self, pos: PursuitPosition, exit_price: float) -> float:
        if pos.direction == 'BUY':
            return (exit_price - pos.weighted_avg_price) * pos.total_volume
        return (pos.weighted_avg_price - exit_price) * pos.total_volume

    def _cleanup_closed_positions(self, max_closed: int = 50) -> None:
        closed_keys = [k for k, p in self._positions.items() if not p.is_open]
        if len(closed_keys) > max_closed:
            for k in closed_keys[:len(closed_keys) - max_closed]:
                del self._positions[k]

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            self._cleanup_closed_positions()
            return {
                'service_name': 'DynamicPursuitEngine', **self._stats,
                'active_positions': sum(1 for p in self._positions.values() if p.is_open),
            }


class PyramidAddPositionEngine:
    """金字塔加仓引擎：信号增强时逐级递减加仓

    原理：每次加仓量为前次的pyramid_ratio倍（如0.5），
    形成金字塔结构——底部仓位大、顶部仓位小。
    ATR自适应：加仓量与当前ATR反相关，高波动时减量。
    """

    def __init__(self, max_levels: int = 4,
                 pyramid_ratio: float = 0.5,
                 atr_adaptive: bool = True,
                 atr_reference: float = 0.02,
                 min_plr_for_add: float = 1.5):
        self._max_levels = max_levels
        self._pyramid_ratio = pyramid_ratio
        self._atr_adaptive = atr_adaptive
        self._atr_reference = atr_reference
        self._min_plr_for_add = min_plr_for_add
        self._positions: Dict[str, Dict] = {}
        self._stats = {'total_adds': 0, 'total_volume_added': 0, 'plr_blocked_adds': 0}

    def calc_add_volume(self, instrument_id: str, base_volume: int,
                        current_level: int, current_atr: float = 0.0,
                        current_plr: float = 0.0) -> int:
        if current_level >= self._max_levels:
            return 0
        if self._min_plr_for_add > 0 and current_plr > 0 and current_plr < self._min_plr_for_add:
            self._stats['plr_blocked_adds'] += 1
            return 0
        volume = int(base_volume * (self._pyramid_ratio ** current_level))
        if self._atr_adaptive and current_atr > 0 and self._atr_reference > 0:
            atr_scale = min(2.0, max(0.3, self._atr_reference / current_atr))
            volume = max(1, int(volume * atr_scale))
        self._stats['total_adds'] += 1
        self._stats['total_volume_added'] += volume
        return volume

    def get_stats(self) -> Dict[str, Any]:
        return {'service_name': 'PyramidAddPositionEngine', **self._stats}

    def _cleanup_closed_positions(self, max_closed: int = 50) -> None:
        closed_keys = [k for k, p in self._positions.items() if not p.get('is_open', True)]
        if len(closed_keys) > max_closed:
            for k in closed_keys[:len(closed_keys) - max_closed]:
                del self._positions[k]


# ============================================================================
# R15-P2 性能修复块
# ============================================================================

# R15-P2-PERF-05修复: tick热路径logging改用%格式化，避免f-string在未命中日志级别时的求值开销
# 使用示例: logger.debug("tick %s price=%.2f vol=%d", instrument_id, price, volume)
# 而非:    logger.debug(f"tick {instrument_id} price={price:.2f} vol={volume}")
# 已在此文件中逐步替换热路径(>1000calls/s)的f-string为%格式化
# 标记: 非热路径(<10calls/s)保留f-string可读性


def check_hmm_dwell_anomaly(handler_instance, current_state: str) -> None:
    """R23-SM-09-FIX: HMM状态驻留时间异常检测

    检测两种异常：
    1. 驻留时间过长(>_hmm_state_dwell_max_sec) — 状态卡死
    2. 切换频率过高(>_hmm_state_max_switches_per_window per _hmm_state_switch_window_sec) — 震荡
    """
    if not hasattr(handler_instance, '_hmm_state_entry_time'):
        return
    _now = time.time()
    # R23-SM-P1-03-FIX: HMM转移矩阵退化检测 — 某状态长期未被访问
    _all_states = set(getattr(handler_instance, '_hmm_state_entry_time', {}).keys())
    _all_states.add(current_state)
    for _s in _all_states:
        _last_visit = handler_instance._hmm_state_entry_time.get(_s, 0.0)
        if _last_visit > 0 and (_now - _last_visit) > handler_instance._hmm_state_dwell_max_sec and _s != current_state:
            logging.warning("[R23-SM-P1-03-FIX] HMM状态长期未被访问(退化): state=%s unvisited=%.1fs > max=%.1fs",
                           _s, _now - _last_visit, handler_instance._hmm_state_dwell_max_sec)
    _prev_state = getattr(handler_instance, '_hmm_last_state', None)
    if _prev_state is not None and _prev_state != current_state:
        _entry_time = handler_instance._hmm_state_entry_time.get(_prev_state, 0.0)
        if _entry_time > 0:
            _dwell = _now - _entry_time
            if _dwell > handler_instance._hmm_state_dwell_max_sec:
                logging.warning("[R23-SM-09-FIX] HMM状态驻留超限: state=%s dwell=%.1fs > max=%.1fs",
                               _prev_state, _dwell, handler_instance._hmm_state_dwell_max_sec)
            if _dwell < handler_instance._hmm_state_dwell_min_sec:
                logging.warning("[R23-SM-09-FIX] HMM状态切换过快: state=%s dwell=%.3fs < min=%.3fs",
                               _prev_state, _dwell, handler_instance._hmm_state_dwell_min_sec)
        _window = handler_instance._hmm_state_switch_window_sec
        _window_key = f"{_prev_state}_{int(_now // _window)}"
        handler_instance._hmm_state_switch_counts[_window_key] = handler_instance._hmm_state_switch_counts.get(_window_key, 0) + 1
        if handler_instance._hmm_state_switch_counts[_window_key] > handler_instance._hmm_state_max_switches_per_window:
            logging.warning("[R23-SM-09-FIX] HMM状态震荡: state=%s switches=%d > max=%d in %.0fs window",
                           _prev_state, handler_instance._hmm_state_switch_counts[_window_key],
                           handler_instance._hmm_state_max_switches_per_window, _window)
    handler_instance._hmm_state_entry_time[current_state] = _now
    handler_instance._hmm_last_state = current_state

# R15-P2-PERF-09标记: 循环中.append改为列表推导式(仅标记，不改逻辑)
# 识别位置: _shard_buffers、_probe_logged_instruments等append调用
# TODO(R17-P2-DOC-02): 将for循环中的.append改为列表推导式，例如:
#   results = [process(item) for item in items]  替代  results=[]; for item in items: results.append(process(item))

# R15-P2-PERF-10标记: 多次重复import移到模块顶部
# 识别位置: 函数内 from ali2026v3_trading.infra.shared_utils import ... 重复调用
# TODO: 将函数内延迟import移到模块顶部，仅在解决循环依赖时保留函数内import  # R17-P2-DEP-03: 模块级/函数内import混用标记
