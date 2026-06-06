import os
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, Optional

from ali2026v3_trading.serialization_utils import yaml_safe_load
from ali2026v3_trading.config_params import (
    STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL,
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE, STRATEGY_MODE_OTHER,
)

try:
    import yaml
except ImportError:
    yaml = None

# R10-P0-19修复: 五态分类映射，回测与实盘统一使用相同的五态判定逻辑
# correct_rise → correct_trending (方向一致+上涨，强信号)
# correct_fall → correct_trending_defensive (方向一致+下跌，防御性信号)
# wrong_rise → incorrect_reversal (方向背离+上涨，反转信号)
# wrong_fall → incorrect_reversal_defensive (方向背离+下跌，防御性反转)
# other → other
STRATEGY_MODE_INCORRECT_REVERSAL_DEFENSIVE = 'incorrect_reversal_defensive'  # R10-P0-19修复: 提取为常量
_STATE_MAP = {
    'correct_rise': STRATEGY_MODE_CORRECT_TRENDING,  # R25-SE-P1-02-FIX
    'correct_fall': STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE,  # R25-SE-P1-02-FIX
    'wrong_rise': STRATEGY_MODE_INCORRECT_REVERSAL,  # R25-SE-P1-02-FIX
    'wrong_fall': STRATEGY_MODE_INCORRECT_REVERSAL_DEFENSIVE,  # R10-P0-19修复: 使用常量替代硬编码字符串
    'other': STRATEGY_MODE_OTHER,  # R25-SE-P1-02-FIX
}

_STATE_PRIORITY = {
    STRATEGY_MODE_CORRECT_TRENDING: 5,  # R25-SE-P1-02-FIX
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: 4,  # R25-SE-P1-02-FIX
    STRATEGY_MODE_INCORRECT_REVERSAL: 3,  # R25-SE-P1-02-FIX
    STRATEGY_MODE_INCORRECT_REVERSAL_DEFENSIVE: 2,  # R10-P0-19修复: 使用常量替代硬编码字符串
    STRATEGY_MODE_OTHER: 1,  # R25-SE-P1-02-FIX
}

_THREE_STATE_MAP = {
    'correct_rise': 'correct_trending',
    'correct_fall': 'correct_trending',
    'wrong_rise': 'incorrect_reversal',
    'wrong_fall': 'incorrect_reversal',
    'other': 'other',
    STRATEGY_MODE_CORRECT_TRENDING: 'correct_trending',
    STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: 'correct_trending',
    STRATEGY_MODE_INCORRECT_REVERSAL: 'incorrect_reversal',
    'incorrect_reversal_defensive': 'incorrect_reversal',
    STRATEGY_MODE_OTHER: 'other',
}


def five_to_three_state(five_state: str) -> str:
    """P0-R9-09修复: 五态到三态映射
    手册3.1节要求:
    {correct_rise, correct_fall} -> correct_trending
    {wrong_rise, wrong_fall} -> incorrect_reversal
    {other} -> other
    """
    return _THREE_STATE_MAP.get(five_state, 'other')


class StateParamManager:
    __slots__ = (
        '_lock', '_strategy_id', '_history_max_len', '_yaml_path',
        '_runtime_env', '_param_sets', '_capital_scale', '_capital_scale_overrides',
        '_current_state', '_prev_state', '_state_enter_time', '_state_history',
        '_width_cache_ref', '_last_switch_time', '_last_check_time',
        '_state_confirm_bars', '_state_check_interval_sec',
        '_state_switch_position_policy', '_non_other_ratio_threshold',
        '_min_state_hold_seconds', '_pending_state', '_pending_confirm_count',
        '_on_state_switch_callbacks', '_dynamic_param_overrides',
        '_dynamic_param_bridge_keys', '_last_resonance_strength',
        '_prev_resonance_strength', '_last_known_price',
        '_hft_transition_capture', '_stats', '_css_premium_threshold',
        '_entry_button', '_cycle_detection_window',
    )
    """状态驱动参数路由器

    将 width_cache 的五态分类 (correct_rise/wrong_rise/correct_fall/wrong_fall/other)
    映射为五种参数策略 (correct_trending/correct_trending_defensive/
    incorrect_reversal/incorrect_reversal_defensive/other)，
    并从 state_param_sets.yaml 加载对应的参数集。

    R10-P0-19修复: 五态切换机制(V7完善)：
    - state_confirm_bars: 新状态需连续确认N次才切换，防止单次噪声误切
    - state_check_interval_sec: 状态检查间隔（秒），控制三态判断频率
    - state_switch_position_policy: 切换时持仓处理策略
    - on_state_switch回调: 状态切换时通知策略生态系统

    切换连续性处理：
    - 切换瞬间不强制平仓已有头寸
    - 切换至 other 状态时，触发防御性减仓（止盈止损收紧至60%，暂停新开仓）
    """

    # R13-P2-LOG-15修复: 历史上限改为类属性，可被子类或构造参数覆盖
    _DEFAULT_HISTORY_MAX_LEN: int = 100  # 状态切换历史最大条目数
    MAX_TRANSITION_HISTORY = 500  # 状态转换捕捉历史最大条目数
    PENDING_ENTRY_TTL_SEC = 60.0  # 待处理入场信号TTL（秒）

    # P2-R11-01修复: 策略级状态参数覆盖字典
    # 结构: {strategy_id: {state_key: {param_key: param_value}}}
    _strategy_state_overrides: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def __init__(
        self,
        yaml_path: Optional[str] = None,
        strategy_id: Optional[str] = None,  # P2-R11-01修复: 策略级参数隔离
        state_confirm_bars: int = 5,
        state_check_interval_sec: float = 60.0,  # [R23-P1-05-FIX] 从180s降至60s，与config_params.CACHE_TTL对齐
        state_switch_position_policy: str = 'keep_with_original_rules',
        non_other_ratio_threshold: float = 0.65,
        min_state_hold_seconds: float = 600.0,
        history_max_len: Optional[int] = None,  # R13-P2-LOG-15修复: 可配置历史上限
    ):
        self._lock = threading.RLock()
        # P2-R11-01修复: 保存策略ID，用于策略级参数隔离
        self._strategy_id = strategy_id or "default"
        # R13-P2-LOG-15修复: 实例级历史上限，优先使用构造参数，否则使用类属性
        self._history_max_len: int = history_max_len if history_max_len is not None else StateParamManager._DEFAULT_HISTORY_MAX_LEN
        self._yaml_path = yaml_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'param_pool', 'state_param_sets.yaml'
        )
        self._runtime_env: str = (
            os.environ.get('TRADING_ENV')
            or os.environ.get('INFINITRADER_ENV')
            or 'default'
        ).strip().lower()
        self._param_sets: Dict[str, Dict[str, Any]] = {}
        self._capital_scale: Optional[str] = None
        self._capital_scale_overrides: Dict[str, Dict[str, Any]] = {}
        self._current_state: str = 'other'
        self._prev_state: str = 'other'
        self._state_enter_time: float = time.time()
        self._state_history: deque = deque(maxlen=self._history_max_len)  # R31-P2-08修复: list→deque(maxlen)避免切片截断O(n)开销
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
        self._dynamic_param_overrides: Dict[str, Any] = {}
        # R17-P1-PERF-15修复: 参数快照缓存
        self._params_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_dirty: bool = True
        self._dynamic_param_bridge_keys = frozenset([
            'close_take_profit_ratio', 'close_stop_loss_ratio',
            'option_buy_lots_min', 'option_buy_lots_max',
            'max_risk_ratio', 'signal_cooldown_sec',
        ])

        self._last_resonance_strength: float = 0.0
        self._prev_resonance_strength: float = 0.0
        self._last_known_price: float = 0.0

        self._hft_transition_capture: Optional[StateTransitionCapture] = None
        try:
            self._hft_transition_capture = StateTransitionCapture()
            logging.info("[StateParamManager] HFT状态转换捕捉器已集成")
        except Exception as e:
            logging.warning("[StateParamManager] HFT状态转换捕捉器初始化失败: %s", e)

        # R10-P0-19修复: 统计包含五态
        self._stats = {
            'state_switches': 0,
            f'{STRATEGY_MODE_CORRECT_TRENDING}_count': 0,  # R25-SE-P1-02-FIX
            f'{STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE}_count': 0,  # R25-SE-P1-02-FIX
            f'{STRATEGY_MODE_INCORRECT_REVERSAL}_count': 0,  # R25-SE-P1-02-FIX
            'incorrect_reversal_defensive_count': 0,
            'other_count': 0,
            'state_check_count': 0,
            'state_confirm_rejects': 0,
            'state_cycles_detected': 0,
        }

        self._css_premium_threshold: float = 0.5  # P1-1修复: 从config_params覆盖
        self._entry_button: float = 0.7  # P1-2修复: 从config_params覆盖
        self._cycle_detection_window: int = 5  # P2-4修复: 循环检测窗口大小
        try:
            from ali2026v3_trading.config_params import get_param
            self._css_premium_threshold = float(get_param('css_premium_threshold', 0.5))
            self._entry_button = float(get_param('entry_button', 0.7))
        except Exception:
            pass

        self._load_param_sets()
        self._apply_yaml_overrides()
        self._refresh_dynamic_param_overrides()

        # 将config_params运行时更新桥接到StateParamManager，避免两套缓存割裂。
        try:
            from ali2026v3_trading.config_params import register_param_change_callback
            register_param_change_callback(self._on_global_param_change)
        except Exception as e:
            logging.debug("[StateParamManager] 注册参数变更回调失败: %s", e)

    def _refresh_dynamic_param_overrides(self) -> None:
        try:
            from ali2026v3_trading.config_params import get_cached_params
            params = get_cached_params() or {}
            self._dynamic_param_overrides = {
                k: params[k] for k in self._dynamic_param_bridge_keys if k in params
            }
        except Exception as e:
            logging.debug("[StateParamManager] 刷新动态参数覆盖失败: %s", e)

    def _on_global_param_change(self, event: Dict[str, Any]) -> None:
        with self._lock:
            self._refresh_dynamic_param_overrides()
        logging.info(
            "[StateParamManager] 收到参数变更通知 source=%s keys=%s",
            (event or {}).get('source', 'unknown'),
            ','.join((event or {}).get('changed_keys', [])),
        )

    def _load_param_sets(self) -> None:
        try:
            from ali2026v3_trading.config_params import get_cached_params
            _cached = get_cached_params()
            if 'state_param_sets' in _cached and _cached['state_param_sets']:
                self._param_sets.clear()
                self._param_sets.update(_cached['state_param_sets'])
                logging.info("[StateParamManager] R16-P1-005: Loaded %d param sets from cached params",
                             len(self._param_sets))
                return
        except Exception:
            pass

        if yaml is None:
            logging.warning("[StateParamManager] PyYAML not installed, using default param sets")
            self._param_sets = self._default_param_sets()
            return

        try:
            if os.path.exists(self._yaml_path):
                with open(self._yaml_path, 'r', encoding='utf-8') as f:
                    all_data = yaml_safe_load(f) or {}
                self._capital_scale_overrides = all_data.pop('capital_scale_overrides', {}) or {}
                # R21-MEM-P1-11修复: 配置对象复用 — 原地更新_param_sets而非替换引用，减少GC压力
                self._param_sets.clear()
                self._param_sets.update(all_data)
                logging.info("[StateParamManager] Loaded %d param sets from %s (capital_scale_overrides: %s)",
                             len(self._param_sets), self._yaml_path,
                             list(self._capital_scale_overrides.keys()))
            else:
                logging.warning("[StateParamManager] YAML file not found: %s, using defaults",
                                self._yaml_path)
                self._param_sets = self._default_param_sets()
        except (OSError, ValueError) as e:  # R31-P2-10修复: 缩窄异常捕获范围，仅捕获IO和解析错误
            logging.error("[StateParamManager] Failed to load YAML: %s, using defaults", e)
            self._param_sets = self._default_param_sets()

    def _apply_yaml_overrides(self) -> None:
        sw = self._param_sets.get('state_switch', {})
        if not isinstance(sw, dict):
            return
        if 'state_confirm_bars' in sw:
            self._state_confirm_bars = max(1, int(sw['state_confirm_bars']))
        if 'state_check_interval_sec' in sw:
            self._state_check_interval_sec = float(sw['state_check_interval_sec'])
        if 'state_switch_position_policy' in sw:
            self._state_switch_position_policy = str(sw['state_switch_position_policy'])
        if 'non_other_ratio_threshold' in sw:
            self._non_other_ratio_threshold = float(sw['non_other_ratio_threshold'])
        if 'min_state_hold_seconds' in sw:
            self._min_state_hold_seconds = float(sw['min_state_hold_seconds'])
        logging.info(
            "[StateParamManager] YAML→实例覆盖生效: state_confirm_bars=%d, state_check_interval_sec=%.1f",
            self._state_confirm_bars, self._state_check_interval_sec,
        )

    @staticmethod
    def _default_param_sets() -> Dict[str, Dict[str, Any]]:
        from ali2026v3_trading.config_params import get_default_state_param_sets
        logging.debug("[P2-API] _default_param_sets委托链: state_param_manager→config_params.get_default_state_param_sets")
        return get_default_state_param_sets()

    def _compute_life_score(self, base_score: float, css_premium: float) -> float:
        """P1-1修复: 计算生命状态评分，css_premium_threshold控制CSS权利金加权"""
        if css_premium <= 0:
            return base_score
        css_weight = min(css_premium / self._css_premium_threshold, 1.0) * 0.3
        return base_score * (1.0 - css_weight) + css_premium * css_weight

    def check_entry_button(self, signal_strength: float) -> bool:
        """P1-2修复: 信号入场确认按钮，信号强度需超过entry_button阈值"""
        return signal_strength >= self._entry_button

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
        import inspect
        try:
            sig = inspect.signature(callback)
            params = list(sig.parameters.values())
            required_count = sum(1 for p in params if p.default is inspect.Parameter.empty)
            if required_count > 2:
                logging.warning("[StateParamManager] 回调签名不匹配(需要>2个必选参数): %s", callback)
                return
        except (ValueError, TypeError) as e:
            logging.warning("[StateParamManager] 回调签名检查失败: %s", e)
        # R13-P2-API-04修复: 回调签名校验
        with self._lock:
            self._on_state_switch_callbacks.append(callback)

    def should_check_state(self) -> bool:
        """判断是否到达状态检查间隔（纯检查，不修改时间戳，避免TOCTOU）"""
        with self._lock:
            now = time.time()
            return now - self._last_check_time >= self._state_check_interval_sec

    def update_state_from_width_cache(self) -> str:
        with self._lock:
            now = time.time()
            elapsed = now - self._last_check_time
            if elapsed < self._state_check_interval_sec:
                return self._current_state
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
                # R10-P0-19修复: 五态统计，与_STATE_MAP五态分类一致
                state_tally: Dict[str, int] = {
                    STRATEGY_MODE_CORRECT_TRENDING: 0, STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE: 0,  # R25-SE-P1-02-FIX
                    STRATEGY_MODE_INCORRECT_REVERSAL: 0, 'incorrect_reversal_defensive': 0,  # R25-SE-P1-02-FIX
                    STRATEGY_MODE_OTHER: 0,  # R25-SE-P1-02-FIX
                }

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
                                # R23-P2-20修复: 增强numpy类型处理
                                try:
                                    count_float = float(count)
                                except (TypeError, ValueError):
                                    continue
                                mapped = _STATE_MAP.get(raw_status, STRATEGY_MODE_OTHER)  # R25-SE-P1-02-FIX
                                state_tally[mapped] += count_float
                                total += count_float

                if total == 0:
                    return self._current_state

                self._stats['state_check_count'] += 1

                # R10-P0-19修复: non_other_total包含所有四类非other状态
                non_other_total = (state_tally.get(STRATEGY_MODE_CORRECT_TRENDING, 0)  # R25-SE-P1-02-FIX
                                   + state_tally.get(STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE, 0)
                                   + state_tally.get(STRATEGY_MODE_INCORRECT_REVERSAL, 0)
                                   + state_tally.get('incorrect_reversal_defensive', 0))
                if non_other_total > 0 and non_other_total / total >= self._non_other_ratio_threshold:
                    candidates = {k: v for k, v in state_tally.items() if k != STRATEGY_MODE_OTHER}  # R25-SE-P1-02-FIX
                    best_state = max(candidates, key=lambda s: (candidates[s], _STATE_PRIORITY.get(s, 0)))
                else:
                    best_state = STRATEGY_MODE_OTHER  # R25-SE-P1-02-FIX

                if best_state != self._current_state:
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
        need_freeze = False
        with self._lock:
            # R17-P1-PERF-15修复: 状态切换时缓存失效
            self._cache_dirty = True; self._params_cache.clear()
            now = time.time()
            old_state = self._current_state
            # P2-4修复: 循环转换检测 — 检测最近N次切换中的A↔B振荡
            if len(self._state_history) >= self._cycle_detection_window:
                recent = list(self._state_history)[-self._cycle_detection_window:]
                transitions = [(h['from'], h['to']) for h in recent]
                # 检测A→B和B→A交替出现>=2次
                pair_counts = {}
                for src, dst in transitions:
                    pair = (src, dst)
                    reverse_pair = (dst, src)
                    if reverse_pair in pair_counts:
                        pair_counts[reverse_pair] = pair_counts[reverse_pair] + 1
                    else:
                        pair_counts[pair] = pair_counts.get(pair, 0) + 1
                for pair, count in pair_counts.items():
                    if count >= 2:
                        self._stats['state_cycles_detected'] = self._stats.get('state_cycles_detected', 0) + 1
                        logging.warning(
                            "[StateParamManager] P2-4: 检测到状态循环振荡 %s↔%s (窗口内%d次), 考虑调整state_confirm_bars或min_state_hold_seconds",
                            pair[0], pair[1], count
                        )
                        break
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
            # R31-P2-08修复: deque(maxlen)自动截断，无需手动切片。仅在接近满时记录warning
            if len(self._state_history) >= self._history_max_len and len(self._state_history) % self._history_max_len == 0:
                logging.warning(
                    "[StateParamManager] 状态切换历史deque已满(maxlen=%d), 自动淘汰最旧记录 # R31-P2-08修复",
                    self._history_max_len,
                )

            logging.info(
                "[StateParamManager] 状态切换: %s → %s (确认%d次, correct=%d, incorrect=%d, other=%d)",
                self._prev_state, new_state, self._state_confirm_bars,
                self._stats.get(f'{STRATEGY_MODE_CORRECT_TRENDING}_count', 0),  # R25-SE-P1-02-FIX
                self._stats.get(f'{STRATEGY_MODE_INCORRECT_REVERSAL}_count', 0),  # R25-SE-P1-02-FIX
                self._stats.get('other_count', 0),
            )

            if new_state == STRATEGY_MODE_OTHER:  # R25-SE-P1-02-FIX
                logging.warning("[StateParamManager] ⚠️ 进入other状态，触发防御性减仓模式")
                need_freeze = True

            if self._hft_transition_capture:
                try:
                    self._hft_transition_capture.on_state_change(
                        old_state, new_state, effective_resonance, effective_price)
                except Exception as hft_e:
                    logging.debug("[StateParamManager] HFT状态转换捕捉异常: %s", hft_e)

            callbacks = list(self._on_state_switch_callbacks)
            # R10-P1-01: 快照语义——回调执行期间新注册的回调不会被当前轮触发
            # 这是刻意设计：避免回调中注册回调导致无限递归

        # P1-R8-02修复: 根据state_switch_position_policy处理持仓
        # 三种策略: exit_all(全部平仓) | keep_with_original_rules(保持原规则) | migrate_to_new_rules(迁移到新规则)
        position_policy = self._state_switch_position_policy
        if position_policy == 'exit_all':
            logging.warning("[StateParamManager] P1-R8-02: state_switch_position_policy=exit_all, 状态切换时撤销所有挂单并冻结开仓")
            try:
                from ali2026v3_trading.order_service import get_order_service
                osvc = get_order_service()
                if osvc:
                    cancelled = osvc.cancel_all_pending()
                    logging.info("[StateParamManager] P1-R8-02: exit_all — 已撤销%d个挂单", cancelled)
            except Exception as _exit_all_e:
                logging.warning("[StateParamManager] P1-R8-02: exit_all撤销挂单异常: %s", _exit_all_e)
            need_freeze = True  # exit_all时同时冻结新旧开仓
        elif position_policy == 'migrate_to_new_rules':
            logging.info("[StateParamManager] P1-R8-02: state_switch_position_policy=migrate_to_new_rules, 持仓参数迁移到新状态")
            self._refresh_dynamic_param_overrides()
            need_freeze = False  # migrate模式下不冻结，而是迁移参数
        # keep_with_original_rules: 默认行为，保持原规则（走原有freeze逻辑）

        # R10-P0-07修复: 冻结策略和回调均在锁外执行，避免死锁
        # 锁序约定: SPM._lock → ecosystem._lock（A→B）
        # 禁止在持有SPM._lock时获取ecosystem._lock，反之亦然
        if need_freeze:
            self._freeze_old_strategy_opening('master')
            self._freeze_old_strategy_opening('reverse')

        for cb in callbacks:
            try:
                cb(old_state, new_state)
            except Exception as e:
                logging.warning("[StateParamManager] on_state_switch回调异常: %s", e)

        # R30-ARCH-03修复: 状态切换通过EventBus发布
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None and not getattr(_bus, '_shutdown', False):
                _bus.publish('spm.state_switched', {
                    'old_state': old_state, 'new_state': new_state,
                    'resonance': effective_resonance, 'price': effective_price,
                })
        except Exception:
            pass

    def _freeze_old_strategy_opening(self, strategy_id: str):
        try:
            from ali2026v3_trading.strategy_ecosystem import get_strategy_ecosystem
            eco = get_strategy_ecosystem()
            freeze_fn = getattr(eco, 'freeze_strategy_slot', None)
            if callable(freeze_fn):
                freeze_fn(strategy_id)
                logging.info("[StateParamManager] 冻结策略%s新开仓(保留已有仓位按原规则退出)", strategy_id)
            else:
                slot = getattr(eco, f'_{strategy_id}', None)
                if slot is None:
                    logging.warning("[StateParamManager] 策略生态系统无_%s属性，跳过冻结", strategy_id)
                    return
                try:
                    slot.frozen = True
                except AttributeError:
                    logging.warning("[StateParamManager] 策略槽位_%s无frozen属性，无法冻结", strategy_id)
                    return
                logging.info("[StateParamManager] 冻结策略%s新开仓(保留已有仓位按原规则退出)", strategy_id)
        except ImportError:
            logging.warning("[StateParamManager] strategy_ecosystem模块不可用，跳过冻结%s", strategy_id)
        except Exception as e:
            logging.warning("[StateParamManager._freeze_old_strategy_opening] 冻结%s失败: %s", strategy_id, e)

    def get_params(self, state: Optional[str] = None) -> Dict[str, Any]:
        target = state or self._current_state
        with self._lock:
            # R17-P1-PERF-15修复: 缓存命中直接返回
            if not self._cache_dirty and target in self._params_cache:
                return dict(self._params_cache[target])
            base_params = dict(self._param_sets.get(target, self._param_sets.get('other', {})))
            if self._capital_scale and self._capital_scale in self._capital_scale_overrides:
                scale_overrides = self._capital_scale_overrides[self._capital_scale]
                if target in scale_overrides:
                    base_params.update(scale_overrides[target])
            if self._dynamic_param_overrides:
                base_params.update(self._dynamic_param_overrides)
            # R17-P1-PERF-15修复: 更新缓存
            self._params_cache[target] = dict(base_params)
            if self._cache_dirty and len(self._params_cache) >= len(self._param_sets):
                self._cache_dirty = False
            return base_params

    def get_params_for_state(self, state_key: str) -> Dict[str, Any]:
        """P2-R11-01修复: 获取指定状态的参数，支持策略级参数隔离

        优先级: 策略级覆盖 > 资金规模覆盖 > 动态参数覆盖 > 基础参数集

        Args:
            state_key: 状态键名(如correct_trending/incorrect_reversal/other等)

        Returns:
            Dict[str, Any]: 合并后的参数字典
        """
        with self._lock:
            result = self.get_params(state=state_key)
            # P2-R11-01修复: 策略级状态参数隔离
            _overrides = StateParamManager._strategy_state_overrides.get(self._strategy_id, {})
            if _overrides and state_key in _overrides:
                result.update(_overrides[state_key])
            return result

    def set_capital_scale(self, capital_scale: str) -> None:
        with self._lock:
            self._capital_scale = capital_scale
            logging.info("[StateParamManager] Capital scale set to: %s", capital_scale)

    def get_capital_scale(self) -> Optional[str]:
        with self._lock:
            return self._capital_scale

    def get_capital_scale_overrides(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._capital_scale_overrides)

    def get_current_state(self) -> str:
        with self._lock:
            return self._current_state

    def get_runtime_env(self) -> str:
        with self._lock:
            return self._runtime_env

    def get_state_duration_sec(self) -> float:
        with self._lock:
            return time.time() - self._state_enter_time

    def is_defensive_mode(self) -> bool:
        with self._lock:
            return self._current_state == STRATEGY_MODE_OTHER  # R25-SE-P1-02-FIX

    # R10-P0-19修复: 五态分布统计
    _FIVE_STATES = (STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE,  # R25-SE-P1-02-FIX
                    STRATEGY_MODE_INCORRECT_REVERSAL, STRATEGY_MODE_INCORRECT_REVERSAL_DEFENSIVE, STRATEGY_MODE_OTHER)  # R10-P0-19修复: 全部使用常量

    def get_state_distribution(self) -> Dict[str, float]:
        total = sum(self._stats.get(f'{s}_count', 0) for s in self._FIVE_STATES)
        if total == 0:
            return {s: 0.0 for s in self._FIVE_STATES[:-1]} | {'other': 1.0}
        return {
            s: self._stats.get(f'{s}_count', 0) / total
            for s in self._FIVE_STATES
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
            stats['runtime_env'] = self._runtime_env
            return stats

    def _migrate_param_format(self, param_sets: Dict[str, Any]) -> Dict[str, Any]:
        """UPG-P1-02修复: 参数格式迁移

        处理参数重命名、类型变更等格式迁移。
        当前迁移规则：
        - close_take_profit_ratio → take_profit_ratio (保留旧键兼容)
        - close_stop_loss_ratio → stop_loss_ratio (保留旧键兼容)
        - 数值型字符串自动转为float

        Args:
            param_sets: 原始参数集

        Returns:
            迁移后的参数集
        """
        migrated = {}
        for state_key, params in param_sets.items():
            if not isinstance(params, dict):
                migrated[state_key] = params
                continue
            new_params = dict(params)
            # 迁移规则1: 旧键名映射到新键名（保留旧键向后兼容）
            _KEY_MIGRATIONS = {
                'close_take_profit_ratio': 'take_profit_ratio',
                'close_stop_loss_ratio': 'stop_loss_ratio',
            }
            for old_key, new_key in _KEY_MIGRATIONS.items():
                if old_key in new_params and new_key not in new_params:
                    new_params[new_key] = new_params[old_key]
                    logging.info(
                        "UPG-P1-02: 参数迁移 %s → %s (state=%s, value=%s)",
                        old_key, new_key, state_key, new_params[new_key],
                    )
            # 迁移规则2: 数值型字符串自动转float
            for k, v in list(new_params.items()):
                if isinstance(v, str):
                    try:
                        new_params[k] = float(v)
                    except (ValueError, TypeError):
                        pass
            migrated[state_key] = new_params
        return migrated

    def _check_param_compatibility(self, new_param_sets: Dict[str, Any],
                                    old_param_sets: Dict[str, Any]) -> List[str]:
        """UPG-P1-09修复: 参数兼容性检查

        检查新参数集是否与旧参数集兼容：
        1. 关键参数不能缺失
        2. 数值型参数不能从有效值变为None
        3. 安全关键参数不能越界

        Args:
            new_param_sets: 新参数集
            old_param_sets: 旧参数集

        Returns:
            List[str]: 兼容性问题列表，空列表表示兼容
        """
        issues = []
        _CRITICAL_PARAMS = [
            'close_take_profit_ratio', 'take_profit_ratio',
            'close_stop_loss_ratio', 'stop_loss_ratio',
        ]
        _SAFETY_RANGES = {
            'close_take_profit_ratio': (0.5, 10.0),
            'take_profit_ratio': (0.5, 10.0),
            'close_stop_loss_ratio': (0.01, 0.99),
            'stop_loss_ratio': (0.01, 0.99),
        }

        for state_key in set(list(new_param_sets.keys()) + list(old_param_sets.keys())):
            new_params = new_param_sets.get(state_key, {})
            old_params = old_param_sets.get(state_key, {})
            if not isinstance(new_params, dict) or not isinstance(old_params, dict):
                continue

            # 检查关键参数缺失
            for cp in _CRITICAL_PARAMS:
                if cp in old_params and cp not in new_params:
                    issues.append(f"state={state_key}: 关键参数{cp}在新参数集中缺失")

            # 检查安全范围
            for param_key, (lo, hi) in _SAFETY_RANGES.items():
                val = new_params.get(param_key)
                if val is not None:
                    try:
                        fval = float(val)
                        if not (lo <= fval <= hi):
                            issues.append(
                                f"state={state_key}: {param_key}={fval}越界[{lo},{hi}]"
                            )
                    except (ValueError, TypeError):
                        issues.append(f"state={state_key}: {param_key}={val}无法转为数值")

        if issues:
            logging.warning("UPG-P1-09: 参数兼容性检查发现%d个问题: %s",
                            len(issues), '; '.join(issues[:5]))
        return issues

    def reload_param_sets(self) -> None:
        """UPG-05/UPG-06修复: 热更新参数集

        UPG-05: 使用双缓冲模式，先加载到临时变量，再原子交换，
        避免其他线程在reload期间读到空参数。

        UPG-06: 加载失败时恢复当前有效值，而非回退到硬编码默认值。

        UPG-P1-02: 加载后执行参数格式迁移。

        UPG-P1-07/12: 加载后执行兼容性检查，不兼容时自动回滚。
        """
        with self._lock:
            # UPG-06: 保存当前有效参数作为回退
            _prev_param_sets = dict(self._param_sets)
            _prev_capital_scale_overrides = dict(self._capital_scale_overrides)

            try:
                # UPG-05: 双缓冲 — 先加载到临时变量
                _new_param_sets = {}
                _new_capital_scale_overrides = {}

                if yaml is None:
                    logging.warning("[StateParamManager] PyYAML not installed, keeping current params")
                    return

                if os.path.exists(self._yaml_path):
                    with open(self._yaml_path, 'r', encoding='utf-8') as f:
                        all_data = yaml_safe_load(f) or {}
                    _new_capital_scale_overrides = all_data.pop('capital_scale_overrides', {}) or {}
                    _new_param_sets = all_data
                    logging.info("[StateParamManager] Reloaded %d param sets from %s (capital_scale_overrides: %s)",
                                 len(_new_param_sets), self._yaml_path,
                                 list(_new_capital_scale_overrides.keys()))
                else:
                    logging.warning("[StateParamManager] YAML file not found: %s, keeping current params",
                                    self._yaml_path)
                    return

                # UPG-P1-02: 参数格式迁移
                _new_param_sets = self._migrate_param_format(_new_param_sets)

                # UPG-P1-09: 参数兼容性检查
                compatibility_issues = self._check_param_compatibility(_new_param_sets, _prev_param_sets)
                if compatibility_issues:
                    # UPG-P1-07/12: 兼容性问题严重时自动回滚
                    _critical_issues = [i for i in compatibility_issues if '越界' in i or '缺失' in i]
                    if _critical_issues:
                        logging.error(
                            "UPG-P1-07: 参数兼容性检查发现%d个严重问题，自动回滚: %s",
                            len(_critical_issues), '; '.join(_critical_issues[:3]),
                        )
                        self._param_sets = _prev_param_sets
                        self._capital_scale_overrides = _prev_capital_scale_overrides
                        return
                    else:
                        logging.warning(
                            "UPG-P1-09: 参数兼容性检查发现%d个非严重问题，继续加载: %s",
                            len(compatibility_issues), '; '.join(compatibility_issues[:3]),
                        )

                # UPG-05: 原子交换 — 一次性替换，中间无空窗期
                self._param_sets = _new_param_sets
                self._capital_scale_overrides = _new_capital_scale_overrides

            except Exception as e:
                # UPG-06: 恢复当前有效值，而非使用_default_param_sets()
                logging.error("[StateParamManager] Failed to reload YAML: %s, restoring current effective params", e)
                self._param_sets = _prev_param_sets
                self._capital_scale_overrides = _prev_capital_scale_overrides

    def check_plr_decline_trigger(self, strategy_id: str, current_plr: float, target_plr: float) -> bool:
        with self._lock:
            if target_plr <= 0:
                return False
            plr_ratio = current_plr / target_plr
            if plr_ratio < 0.3 and self._current_state != 'other':
                # R30-P1-15修复: PLR侧信道绕过确认机制
                # 原问题: 直接设置_current_state='other'，绕过_state_confirm_bars和min_state_hold_seconds
                # 修复: 仍尊重最小持有期约束，若在持有期内则仅设置pending_state
                _now = time.time()
                _hold_elapsed = _now - self._state_enter_time
                if _hold_elapsed < self._min_state_hold_seconds:
                    logging.warning(
                        "[StateParamManager] R30-P1-15: PLR触发切换到other，但最小持有期未满(%.1fs/%.1fs)，"
                        "设置pending_state而非直接切换",
                        _hold_elapsed, self._min_state_hold_seconds,
                    )
                    self._pending_state = 'other'
                    self._pending_confirm_count = 0
                    return False
                logging.info(
                    "[StateParamManager] PLR严重下降触发状态切换: %s plr=%.2f/target=%.2f ratio=%.2f, state=%s->other(需确认)",
                    strategy_id, current_plr, target_plr, plr_ratio, self._current_state
                )
                # R33-P1-15修复: PLR触发也必须经过_state_confirm_bars确认窗口
                # 原R30修复仅尊重min_state_hold_seconds，但持有期过后仍直接切换绕过确认
                # 修复: 统一走pending_state+确认计数路径，与正常状态切换对齐
                self._pending_state = 'other'
                self._pending_confirm_count = 1  # PLR触发算1次确认
                if self._pending_confirm_count >= self._state_confirm_bars:
                    # 确认次数已满足，执行切换
                    self._prev_state = self._current_state
                    self._current_state = 'other'
                    self._state_enter_time = time.time()
                    self._pending_state = None
                    self._pending_confirm_count = 0
                    return True
                else:
                    logging.info(
                        "[StateParamManager] R33-P1-15: PLR触发pending_state=other, 确认%d/%d",
                        self._pending_confirm_count, self._state_confirm_bars,
                    )
                    return False
            return False


_state_param_manager: Optional[StateParamManager] = None
_state_param_manager_lock = threading.Lock()


def get_state_param_manager(yaml_path: Optional[str] = None, **kwargs) -> StateParamManager:
    global _state_param_manager
    # R10-P0-01修复: DCL缺陷修复 — 全部在锁内检查和创建
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


@dataclass(slots=True)
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
    MAX_TRANSITION_HISTORY = 500
    PENDING_ENTRY_TTL_SEC = 60.0

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
            if len(self._transition_history) > self.MAX_TRANSITION_HISTORY:
                self._transition_history = self._transition_history[-self.MAX_TRANSITION_HISTORY:]
            entry_signal = self._evaluate_entry(point)
            if entry_signal:
                self._stats['entries_triggered'] += 1
                self._pending_entries[point.transition_id] = point
        logging.info("[StateTransitionCapture] %s -> %s event=%s entry=%s strength=%.3f",
                     old_state, new_state, event.name, 'YES' if entry_signal else 'NO', resonance_strength)
        return entry_signal

    def _classify_transition(self, old_state: str, new_state: str) -> Optional[StateTransitionEvent]:
        mapping = {  # R25-SE-P1-02-FIX
            (STRATEGY_MODE_OTHER, STRATEGY_MODE_CORRECT_TRENDING): StateTransitionEvent.OTHER_TO_CORRECT,
            (STRATEGY_MODE_OTHER, STRATEGY_MODE_INCORRECT_REVERSAL): StateTransitionEvent.OTHER_TO_INCORRECT,
            (STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_OTHER): StateTransitionEvent.CORRECT_TO_OTHER,
            (STRATEGY_MODE_INCORRECT_REVERSAL, STRATEGY_MODE_OTHER): StateTransitionEvent.INCORRECT_TO_OTHER,
            (STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_INCORRECT_REVERSAL): StateTransitionEvent.CORRECT_TO_INCORRECT,
            (STRATEGY_MODE_INCORRECT_REVERSAL, STRATEGY_MODE_CORRECT_TRENDING): StateTransitionEvent.INCORRECT_TO_CORRECT,
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

    def _cleanup_expired_pending(self, ttl_seconds: float = None) -> None:
        if ttl_seconds is None:
            ttl_seconds = self.PENDING_ENTRY_TTL_SEC
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

    # R10-P0-19修复: 五态转换分析
    STATES = (STRATEGY_MODE_OTHER, STRATEGY_MODE_CORRECT_TRENDING, STRATEGY_MODE_CORRECT_TRENDING_DEFENSIVE,  # R25-SE-P1-02-FIX
              STRATEGY_MODE_INCORRECT_REVERSAL, 'incorrect_reversal_defensive')

    def __init__(self):
        self._first_switches: Dict[str, Dict[str, float]] = {}
        self._dwell_times: Dict[str, List[float]] = {s: [] for s in self.STATES}
        self._current_state_entry: Dict[str, float] = {}
        self._transition_counts: Dict[str, Dict[str, int]] = {
            s1: {s2: 0 for s2 in self.STATES} for s1 in self.STATES
        }
        self._stats = {'total_transitions': 0, 'first_switches': 0}
        # R33-P2-4: 循环转换检测历史（按instrument_id存储最近N次转换）
        self._cycle_detect_window: int = 10
        self._recent_transitions: Dict[str, List[tuple]] = {}

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

            # R33-P2-4: A→B→A循环转换检测
            if old_state != new_state:
                history = self._recent_transitions.setdefault(instrument_id, [])
                history.append((old_state, new_state))
                if len(history) > self._cycle_detect_window:
                    history.pop(0)
                # 检测 from→to→from 模式（最近3次转换中出现 A→B, B→A 即为循环）
                if len(history) >= 2:
                    prev_from, prev_to = history[-2]
                    curr_from, curr_to = history[-1]
                    if prev_from == curr_to and prev_to == curr_from:
                        import logging
                        logging.warning(
                            "[R33-P2-4] 状态循环检测: instrument=%s 检测到 %s→%s→%s 循环转换，"
                            "最近%d次转换历史=%s",
                            instrument_id, prev_from, prev_to, curr_to,
                            len(history), history[-min(len(history), 5):]
                        )

        if old_state in self._current_state_entry:
            dwell = timestamp - self._current_state_entry[old_state]
            self._dwell_times.setdefault(old_state, []).append(dwell)
            result['dwell_time'] = dwell
        self._current_state_entry[new_state] = timestamp

        if old_state == STRATEGY_MODE_OTHER and new_state == STRATEGY_MODE_CORRECT_TRENDING:  # R25-SE-P1-02-FIX
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


# ============================================================================
# R15-P2 性能修复块
# ============================================================================

# R15-P2-PERF-02修复: StateParamManager添加__slots__声明，减少hasattr反射开销
# 已集成: __slots__已在类定义中声明
def _get_state_param_slots():
    """已集成: __slots__已在类定义中声明"""
    return (
        '_lock', '_strategy_id', '_history_max_len', '_yaml_path',
        '_runtime_env', '_param_sets', '_capital_scale', '_capital_scale_overrides',
        '_current_state', '_prev_state', '_state_enter_time', '_state_history',
        '_width_cache_ref', '_last_switch_time', '_last_check_time',
        '_state_confirm_bars', '_state_check_interval_sec',
        '_state_switch_position_policy', '_non_other_ratio_threshold',
        '_min_state_hold_seconds', '_pending_state', '_pending_confirm_count',
        '_on_state_switch_callbacks', '_dynamic_param_overrides',
        '_dynamic_param_bridge_keys', '_last_resonance_strength',
        '_prev_resonance_strength', '_last_known_price',
        '_hft_transition_capture', '_stats', '_css_premium_threshold',
        '_entry_button', '_cycle_detection_window',
    )
