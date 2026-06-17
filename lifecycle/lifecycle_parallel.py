# MODULE_ID: M1-125
"""lifecycle_parallel/统一并行操作模块 — 合并自lifecycle_parallel + lifecycle_parallel_ops"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from ali2026v3_trading.lifecycle.lifecycle_state_machine import StrategyState, _state_is


# ──────────────────────────────────────────────────────────────
# Section 1: 并行操作核心 (原 lifecycle_parallel.py)
# ──────────────────────────────────────────────────────────────

class LifecycleParallel:
    def __init__(self, provider):
        self._provider = provider
        self._parallel_mode = False
        self._parallel_results: Dict[str, Any] = {}
        self._parallel_lock = threading.Lock()
        self._shadow_strategy: Optional[Any] = None
        self._parallel_start_time: float = 0.0
        self._parallel_duration_sec: float = 0.0
        self._promotion_result: Optional[Dict[str, Any]] = None

    def enter_parallel_running(self, shadow_strategy: Any = None,
                               duration_sec: float = 3600.0) -> None:
        with self._parallel_lock:
            self._parallel_mode = True
            self._parallel_results.clear()
            self._shadow_strategy = shadow_strategy
            self._parallel_start_time = time.time()
            self._parallel_duration_sec = duration_sec
            self._promotion_result = None
        logging.info("[LifecycleParallel] 进入并行模式, duration=%.0fs", duration_sec)

    def exit_parallel_running(self, promote_new: bool = False) -> bool:
        with self._parallel_lock:
            was_parallel = self._parallel_mode
            self._parallel_mode = False
            shadow = self._shadow_strategy
            self._shadow_strategy = None
            if was_parallel and promote_new and shadow is not None:
                self._promotion_result = {
                    'promoted': True,
                    'shadow_strategy': shadow,
                    'timestamp': time.time(),
                }
                logging.info("[LifecycleParallel] 退出并行模式, promote_new=True")
                return True
        if was_parallel:
            logging.info("[LifecycleParallel] 退出并行模式, promote_new=False")
        return False

    def record_parallel_result(self, key: str, result: Any) -> None:
        with self._parallel_lock:
            self._parallel_results[key] = result

    def compare_parallel_results(self) -> Dict[str, Any]:
        with self._parallel_lock:
            results = dict(self._parallel_results)
        if len(results) < 2:
            return {'comparable': False, 'reason': 'insufficient_results', 'count': len(results)}
        keys = list(results.keys())
        comparison = {'comparable': True, 'keys': keys, 'count': len(results)}
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                k1, k2 = keys[i], keys[j]
                v1, v2 = results[k1], results[k2]
                if isinstance(v1, dict) and isinstance(v2, dict):
                    diff_keys = set(v1.keys()) ^ set(v2.keys())
                    common_keys = set(v1.keys()) & set(v2.keys())
                    value_diffs = {}
                    for ck in common_keys:
                        if isinstance(v1[ck], (int, float)) and isinstance(v2[ck], (int, float)):
                            value_diffs[ck] = v2[ck] - v1[ck]
                    comparison[f'{k1}_vs_{k2}'] = {
                        'key_diff': list(diff_keys),
                        'value_diffs': value_diffs,
                    }
        return comparison

    @property
    def is_parallel_running(self) -> bool:
        return self._parallel_mode

    @property
    def parallel_elapsed_sec(self) -> float:
        if not self._parallel_mode:
            return 0.0
        return time.time() - self._parallel_start_time

    @property
    def should_exit_parallel(self) -> bool:
        if not self._parallel_mode:
            return False
        return self.parallel_elapsed_sec >= self._parallel_duration_sec

    @property
    def promotion_result(self) -> Optional[Dict[str, Any]]:
        return self._promotion_result

    @property
    def shadow_strategy(self) -> Optional[Any]:
        with self._parallel_lock:
            return self._shadow_strategy


# ──────────────────────────────────────────────────────────────
# Section 2: 并行操作辅助 (原 lifecycle_parallel_ops.py)
# ──────────────────────────────────────────────────────────────

class LifecycleParallelOps:
    def __init__(self, provider):
        self.p = provider

    def enter_parallel_running(self, shadow_strategy: Any = None,
                                comparison_callback: Optional[Callable] = None,
                                duration_sec: float = 3600.0) -> bool:
        p = self.p
        if not _state_is(p._state, StrategyState.RUNNING):
            logging.warning(
                "UPG-P1-03: 无法进入并行运行期，当前状态=%s (需要RUNNING)",
                p._state.value if hasattr(p._state, 'value') else p._state,
            )
            return False
        p._parallel_running_config = {
            'shadow_strategy': shadow_strategy,
            'comparison_callback': comparison_callback,
            'duration_sec': duration_sec,
            'entered_at': time.time(),
            'comparison_count': 0,
            'mismatch_count': 0,
        }
        if shadow_strategy is not None:
            try:
                if hasattr(shadow_strategy, 'initialize'):
                    shadow_strategy.initialize(params=getattr(p, 'params', None))
                if hasattr(shadow_strategy, 'start'):
                    shadow_strategy.start()
                logging.info("UPG-P1-03: 影子策略已启动")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("UPG-P1-03: 影子策略启动失败: %s", e)
        success = p.transition_to(StrategyState.PARALLEL_RUNNING)
        if success:
            try:
                p._lifecycle_parallel.enter_parallel_running()
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _lpar_err:
                logging.debug("[LifecycleParallel] enter_parallel_running 委托失败: %s", _lpar_err)
            logging.info("UPG-P1-03: 已进入并行运行期 (duration=%.0fs)", duration_sec)
            p._publish_event('StrategyParallelRunning', {
                'strategy_id': p.strategy_id,
                'duration_sec': duration_sec,
            })
            if duration_sec > 0:
                def _parallel_timeout():
                    time.sleep(duration_sec)
                    if _state_is(p._state, StrategyState.PARALLEL_RUNNING):
                        logging.info("UPG-P1-03: 并行运行期超时，自动退出")
                        p.exit_parallel_running(promote_new=True)
                threading.Thread(
                    target=_parallel_timeout,
                    name=f"parallel-timeout[strategy:{p.strategy_id}]",
                    daemon=True,
                ).start()
        return success

    def exit_parallel_running(self, promote_new: bool = False) -> bool:
        p = self.p
        if not _state_is(p._state, StrategyState.PARALLEL_RUNNING):
            logging.warning("UPG-P1-03: 当前不在并行运行期，无法退出")
            return False
        config = getattr(p, '_parallel_running_config', {})
        shadow = config.get('shadow_strategy')
        comparison_count = config.get('comparison_count', 0)
        mismatch_count = config.get('mismatch_count', 0)
        logging.info(
            "UPG-P1-03: 退出并行运行期 (comparisons=%d, mismatches=%d, promote_new=%s)",
            comparison_count, mismatch_count, promote_new,
        )
        if shadow is not None:
            try:
                if hasattr(shadow, 'stop'):
                    shadow.stop()
                logging.info("UPG-P1-03: 影子策略已停止")
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("UPG-P1-03: 影子策略停止失败: %s", e)
        p._parallel_running_config = {}
        try:
            p._lifecycle_parallel.exit_parallel_running()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _lpar_err:
            logging.debug("[LifecycleParallel] exit_parallel_running 委托失败: %s", _lpar_err)
        success = p.transition_to(StrategyState.RUNNING)
        if success:
            p._publish_event('StrategyParallelRunningExited', {
                'strategy_id': p.strategy_id,
                'promote_new': promote_new,
                'comparison_count': comparison_count,
                'mismatch_count': mismatch_count,
            })
        return success

    def compare_parallel_results(self, old_result: Any, new_result: Any) -> Dict[str, Any]:
        p = self.p
        config = getattr(p, '_parallel_running_config', {})
        config['comparison_count'] = config.get('comparison_count', 0) + 1
        comparison_callback = config.get('comparison_callback')
        if comparison_callback:
            try:
                result = comparison_callback(old_result, new_result)
                if not result.get('match', True):
                    config['mismatch_count'] = config.get('mismatch_count', 0) + 1
                    logging.warning(
                        "UPG-P1-03: 新旧策略结果不匹配 #%d: %s",
                        config['mismatch_count'], result.get('details', ''),
                    )
                return result
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logging.warning("UPG-P1-03: 比较回调执行失败: %s", e)
        match = old_result == new_result
        if not match:
            config['mismatch_count'] = config.get('mismatch_count', 0) + 1
            logging.warning("UPG-P1-03: 新旧策略结果不匹配 #%d", config['mismatch_count'])
        return {'match': match, 'details': '' if match else '结果不一致'}

    def record_parallel_result(self, key: str, result: Any) -> None:
        p = self.p
        try:
            p._lifecycle_parallel.record_parallel_result(key, result)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _lpar_err:
            logging.debug("[LifecycleParallel] record_parallel_result 委托失败: %s", _lpar_err)

    def get_parallel_results(self) -> Dict[str, Any]:
        p = self.p
        try:
            return p._lifecycle_parallel.compare_parallel_results()
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _lpar_err:
            logging.debug("[LifecycleParallel] compare_parallel_results 委托失败: %s", _lpar_err)
            return {}

    def get_parallel_running_status(self) -> Dict[str, Any]:
        p = self.p
        config = getattr(p, '_parallel_running_config', {})
        if not config:
            return {
                'in_parallel': False,
                'state': p._state.value if hasattr(p._state, 'value') else str(p._state),
            }
        elapsed = time.time() - config.get('entered_at', time.time())
        remaining = max(0, config.get('duration_sec', 0) - elapsed)
        return {
            'in_parallel': _state_is(p._state, StrategyState.PARALLEL_RUNNING),
            'elapsed_sec': elapsed,
            'remaining_sec': remaining,
            'comparison_count': config.get('comparison_count', 0),
            'mismatch_count': config.get('mismatch_count', 0),
            'has_shadow': config.get('shadow_strategy') is not None,
        }
