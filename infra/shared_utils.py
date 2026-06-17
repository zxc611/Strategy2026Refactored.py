# [M1-61] 统一工具模块
"""shared_utils - Facade模块 (R27-CP-04-FIX: 拆分为5个子模块，本文件保留向后兼容re-export)

✅ ID唯一 / 方法唯一 / 传递渠道唯一：跨模块共享的工具函数统一定义在此
禁止在其他模块中重复定义这些函数，必须从此模块导入。

子模块拆分 (R27-CP-04-FIX):
- shared_utils_sql: SQL安全相关 (sanitize_sql_identifier, sanitize_sql_value)
- shared_utils_types: 类型转换相关 (to_float32, to_native_type, safe_int, safe_float, safe_price_check)
- shared_utils_instrument: 合约工具相关 (normalize_option_type, normalize_instrument_id, extract_product_code, extract_strike_price, normalize_year_month, CHINA_TZ)
- shared_utils_contracts: 契约编程相关 (requires_phase, require_precondition, ensure_postcondition, PreconditionError, PostconditionError, OperationTimeoutError, with_timeout)
- shared_utils_infra: 基础设施相关 (RingBuffer, ShardRouter, get_shard_router, ThreadLifecycleManager, InitPhase, InitializationError, InitStateMachine, IdempotencyGuard, AutoRollbackContext, atomic_replace_file, cleanup_atomic_backup, check_version_sync, check_shared_utils_invariants)
"""

import logging
import math
import time
import threading
import warnings as _warnings
from dataclasses import dataclass, field
from datetime import timezone, timedelta
from typing import Any, Callable, Dict, List, Optional

# ============================================================================
# R27-CP-04-FIX: 从子模块re-export所有公开API（向后兼容）
# ============================================================================

from ali2026v3_trading.infra.shared_utils_sql import (
    sanitize_sql_identifier,
    sanitize_sql_value,
)

from ali2026v3_trading.infra.shared_utils_types import (
    to_float32,
    to_native_type,
    safe_int,
    safe_float,
    safe_price_check,
)

from ali2026v3_trading.infra.shared_utils_instrument import (
    CHINA_TZ,
    normalize_option_type,
    normalize_instrument_id,
    extract_product_code,
    extract_strike_price,
    normalize_year_month,
)

from ali2026v3_trading.infra.shared_utils_contracts import (
    requires_phase,
    require_precondition,
    ensure_postcondition,
    PreconditionError,
    PostconditionError,
    OperationTimeoutError,
    with_timeout,
)

from ali2026v3_trading.infra.shared_utils_infra import (
    RingBuffer,
    ShardRouter,
    DEFAULT_SHARD_COUNT,
    get_shard_router,
    ThreadLifecycleManager,
    InitPhase,
    InitializationError,
    InitStateMachine,
    IdempotencyGuard,
    AutoRollbackContext,
    atomic_replace_file,
    cleanup_atomic_backup,
    check_version_sync,
    check_shared_utils_invariants,
)

# R27-P1修复: 导入浮点精度工具供全项目共享
from ali2026v3_trading.resilience_utils import (
    stable_sum, stable_mean, stable_variance, stable_std,
    approx_equal, approx_less, approx_greater,
    should_trigger_stop_loss, should_trigger_take_profit,
    KahanSummation, safe_divide, compute_sharpe_stable,
    safe_normalize_weights, PRICE_TOLERANCE, FLOAT_COMPARE_TOLERANCE,
)


# ============================================================================
# 以下为未归入子模块的剩余定义，暂保留在facade中
# ============================================================================

def unified_ts_ms() -> int:
    """[R22-TIME-P1-15] 统一毫秒精度时间戳，用作缓存键，消除毫秒/微秒混用"""
    return int(time.time() * 1000)


# ============================================================================
# FM-01修复: 无风险利率常量（与greeks_calculator.DEFAULT_RISK_FREE_RATE对齐）
# ============================================================================
DEFAULT_RISK_FREE_RATE: float = 0.02
TRADING_DAYS_PER_YEAR_CHINA: float = 242.0

# ============================================================================
# R17-12修复: 年化因子常量 — 统一管理Sharpe/Calmar等年化计算
# ============================================================================
ANNUALIZE_FACTOR_DAILY = 252.0
ANNUALIZE_FACTOR_MINUTE = 252.0 * 240  # 假设: equity_curve逐分钟采样, 每天240根Bar


# ========================================================================
# 回撤开仓（Pullback Entry） — 通用基础设施
# ========================================================================

@dataclass(slots=True)
class PendingPullbackSignal:
    signal_id: str
    strategy_type: str
    instrument_id: str
    direction: str
    peak_price: float
    signal_price: float
    signal_bar_index: int
    peak_bar_index: int = 0
    bars_elapsed: int = 0
    ticks_elapsed: int = 0  # P0-2修复：逐tick计数器，支持HFT逐tick跟踪模式
    is_expired: bool = False
    dynamic_wait_bars: int = 0
    dynamic_retrace_pct: float = 0.0
    dynamic_wait_ticks: int = 0  # P0-2修复：逐tick模式下的等待tick数
    ref_price: float = 0.0
    extra: Dict[str, Any] = field(default_factory=dict)

    def check_retrace(self, current_price: float,
                      retrace_pct: float,
                      wait_bars: int,
                      min_retrace_abs: float = 0.0,
                      wait_ticks: int = 0) -> bool:
        if self.is_expired:
            return False
        self.bars_elapsed += 1
        effective_wait = self.dynamic_wait_bars if self.dynamic_wait_bars > 0 else wait_bars
        effective_retrace = self.dynamic_retrace_pct if self.dynamic_retrace_pct > 0.0 else retrace_pct
        if self.bars_elapsed > effective_wait:
            self.is_expired = True
            return False
        # P0-裂缝10修复：禁止在回撤判断中动态上移peak_price
        ref = self.ref_price if self.ref_price > 0.0 else self.peak_price
        if ref <= 0:
            return False
        retrace_abs = ref - current_price
        retrace_pct_actual = retrace_abs / ref
        if retrace_pct_actual < 0:
            return False
        if min_retrace_abs > 0.0 and retrace_abs < min_retrace_abs:
            return False
        return retrace_pct_actual >= effective_retrace

    def check_retrace_tick(self, current_price: float,
                           retrace_pct: float,
                           wait_ticks: int,
                           min_retrace_abs: float = 0.0) -> bool:
        """P0-2修复：逐tick实时跟踪模式 — 每个tick即时判断回撤"""
        if self.is_expired:
            return False
        self.ticks_elapsed += 1
        effective_wait_ticks = self.dynamic_wait_ticks if self.dynamic_wait_ticks > 0 else wait_ticks
        if effective_wait_ticks > 0 and self.ticks_elapsed > effective_wait_ticks:
            self.is_expired = True
            return False
        ref = self.ref_price if self.ref_price > 0.0 else self.peak_price
        if ref <= 0:
            return False
        retrace_abs = ref - current_price
        retrace_pct_actual = retrace_abs / ref
        if retrace_pct_actual < 0:
            return False
        if min_retrace_abs > 0.0 and retrace_abs < min_retrace_abs:
            return False
        return retrace_pct_actual >= retrace_pct


class PullbackManager:
    """通用回撤开仓管理器 — 非高频策略共用"""

    REF_MODES = frozenset({'peak', 'atr', 'vwap', 'delta'})

    def __init__(self, params: Dict[str, Any] = None):
        p = params or {}
        self.enabled: bool = p.get('pullback_enabled', False)
        self.wait_bars: int = p.get('pullback_wait_bars', 5)
        self.retrace_pct: float = p.get('pullback_retrace_pct', 0.15)
        self.iv_min_pct: float = p.get('pullback_iv_min_percentile', 20.0)
        self.iv_max_pct: float = p.get('pullback_iv_max_percentile', 80.0)
        self.ref_mode: str = p.get('pullback_ref_mode', 'peak')
        if self.ref_mode not in self.REF_MODES:
            logging.warning("[Pullback] Unknown ref_mode='%s', falling back to 'peak'", self.ref_mode)
            self.ref_mode = 'peak'
        self.atr_wait_multiplier: float = p.get('pullback_atr_wait_multiplier', 0.0)
        self.retrace_pct_call: Optional[float] = p.get('pullback_retrace_pct_call', 0.38)
        self.retrace_pct_put: Optional[float] = p.get('pullback_retrace_pct_put', 0.42)
        self.theta_decay_accel: float = p.get('pullback_theta_decay_accel', 0.0)
        self.min_retrace_abs: float = p.get('pullback_min_retrace_abs', 0.0)
        # P0-2修复：逐tick实时跟踪模式
        self.tick_tracking: bool = p.get('pullback_tick_tracking', False)
        self.wait_ticks: int = p.get('pullback_wait_ticks', self.wait_bars * 240)
        self._pending: Dict[str, PendingPullbackSignal] = {}
        self._bar_counter: int = 0
        self._lock = threading.RLock()
        self._stats = {
            'signals_deferred': 0,
            'retrace_entries': 0,
            'retrace_timeouts': 0,
            'iv_rejected': 0,
            'atr_wait_extended': 0,
            'theta_shortened': 0,
            'tick_retrace_entries': 0,
        }

    def _compute_directional_retrace(self, direction: str) -> float:
        if direction.upper() in ('CALL', 'BUY', 'LONG'):
            return self.retrace_pct_call if self.retrace_pct_call is not None else self.retrace_pct
        return self.retrace_pct_put if self.retrace_pct_put is not None else self.retrace_pct

    def _compute_dynamic_wait(self, atr_pct: float = 0.0,
                              days_to_expiry: float = 999.0) -> int:
        wait = self.wait_bars
        if self.atr_wait_multiplier > 0.0 and atr_pct > 0.0:
            atr_ext = int(atr_pct * self.atr_wait_multiplier)
            wait += atr_ext
            if atr_ext > 0:
                with self._lock:
                    self._stats['atr_wait_extended'] += 1
        if self.theta_decay_accel > 0.0 and days_to_expiry < 30.0:
            theta_shrink = int(self.theta_decay_accel * max(0.0, 30.0 - days_to_expiry))
            wait = max(1, wait - theta_shrink)
            if theta_shrink > 0:
                with self._lock:
                    self._stats['theta_shortened'] += 1
        return max(1, wait)

    def _compute_ref_price(self, current_price: float,
                           atr_value: float = 0.0,
                           vwap: float = 0.0,
                           delta_anchor: float = 0.0) -> float:
        if self.ref_mode == 'atr' and atr_value > 0.0:
            return current_price + atr_value
        elif self.ref_mode == 'vwap' and vwap > 0.0:
            return vwap
        elif self.ref_mode == 'delta' and abs(delta_anchor) > 0.001:
            return current_price * (1.0 + abs(delta_anchor) * 0.1)
        return current_price

    def should_defer(self, signal_id: str, strategy_type: str,
                     instrument_id: str, direction: str,
                     current_price: float,
                     iv_percentile: float = 50.0,
                     atr_pct: float = 0.0,
                     days_to_expiry: float = 999.0,
                     atr_value: float = 0.0,
                     vwap: float = 0.0,
                     delta_anchor: float = 0.0,
                     extra: Dict[str, Any] = None) -> bool:
        if not self.enabled:
            return False
        if iv_percentile < self.iv_min_pct or iv_percentile > self.iv_max_pct:
            with self._lock:
                self._stats['iv_rejected'] += 1
            return True
        dynamic_wait = self._compute_dynamic_wait(atr_pct, days_to_expiry)
        dynamic_retrace = self._compute_directional_retrace(direction)
        ref_price = self._compute_ref_price(current_price, atr_value, vwap, delta_anchor)
        pending = PendingPullbackSignal(
            signal_id=signal_id,
            strategy_type=strategy_type,
            instrument_id=instrument_id,
            direction=direction,
            peak_price=current_price,
            signal_price=current_price,
            signal_bar_index=self._bar_counter,
            dynamic_wait_bars=dynamic_wait,
            dynamic_retrace_pct=dynamic_retrace,
            dynamic_wait_ticks=self.wait_ticks if self.tick_tracking else 0,
            ref_price=ref_price,
            extra=extra or {},
        )
        with self._lock:
            self._pending[signal_id] = pending
            self._stats['signals_deferred'] += 1
        logging.info(
            "[Pullback] DEFERRED: %s type=%s dir=%s price=%.4f wait=%d retrace=%.0f%% ref=%s",
            signal_id, strategy_type, direction, current_price,
            dynamic_wait, dynamic_retrace * 100, self.ref_mode,
        )
        return True

    def process_bar(self, current_prices: Dict[str, float]) -> List[PendingPullbackSignal]:
        if not self.enabled or not self._pending:
            return []
        self._bar_counter += 1
        ready = []
        expired_keys = []
        with self._lock:
            for sig_id, pending in self._pending.items():
                price = current_prices.get(pending.instrument_id, 0.0)
                if price <= 0:
                    continue
                if pending.check_retrace(price, self.retrace_pct, self.wait_bars, self.min_retrace_abs):
                    ready.append(pending)
                    expired_keys.append(sig_id)
                    self._stats['retrace_entries'] += 1
                    logging.info(
                        "[Pullback] RETRACE ENTRY: %s type=%s bars=%d retrace=%.1f%%",
                        sig_id, pending.strategy_type, pending.bars_elapsed,
                        (pending.peak_price - price) / pending.peak_price * 100
                        if pending.peak_price > 0 else 0,
                    )
                elif pending.is_expired:
                    expired_keys.append(sig_id)
                    self._stats['retrace_timeouts'] += 1
                    logging.info(
                        "[Pullback] TIMEOUT: %s type=%s bars=%d",
                        sig_id, pending.strategy_type, pending.bars_elapsed,
                    )
            for k in expired_keys:
                del self._pending[k]
        return ready

    def process_tick(self, instrument_id: str, tick_price: float) -> List[PendingPullbackSignal]:
        """P0-2修复：逐tick实时跟踪模式"""
        if not self.enabled or not self._pending or not self.tick_tracking:
            return []
        ready = []
        expired_keys = []
        with self._lock:
            for sig_id, pending in self._pending.items():
                if pending.instrument_id != instrument_id:
                    continue
                if tick_price <= 0:
                    continue
                if pending.check_retrace_tick(tick_price, self.retrace_pct,
                                              self.wait_ticks, self.min_retrace_abs):
                    ready.append(pending)
                    expired_keys.append(sig_id)
                    self._stats['tick_retrace_entries'] += 1
                    logging.info(
                        "[Pullback-Tick] RETRACE ENTRY: %s type=%s ticks=%d retrace=%.1f%%",
                        sig_id, pending.strategy_type, pending.ticks_elapsed,
                        (pending.peak_price - tick_price) / pending.peak_price * 100
                        if pending.peak_price > 0 else 0,
                    )
                elif pending.is_expired:
                    expired_keys.append(sig_id)
                    self._stats['retrace_timeouts'] += 1
            for k in expired_keys:
                del self._pending[k]
        return ready

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                **self._stats,
                'pending_count': len(self._pending),
                'pullback_enabled': self.enabled,
                'pullback_wait_bars': self.wait_bars,
                'pullback_retrace_pct': self.retrace_pct,
                'pullback_ref_mode': self.ref_mode,
                'pullback_atr_wait_multiplier': self.atr_wait_multiplier,
                'pullback_retrace_pct_call': self.retrace_pct_call,
                'pullback_retrace_pct_put': self.retrace_pct_put,
                'pullback_theta_decay_accel': self.theta_decay_accel,
                'pullback_min_retrace_abs': self.min_retrace_abs,
            }


# ============================================================================
# R5-E-05修复: 带最大重试次数的重试辅助函数
# ============================================================================
def retry_with_limit(func, max_retries: int = 3, base_delay: float = 1.0,
                     backoff_factor: float = 2.0, logger=None, max_delay: float = 60.0,
                     cancel_event: 'threading.Event|None' = None):
    """R5-E-05修复: 带最大重试次数和指数退避的重试辅助函数"""
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                if logger:
                    logger.warning(
                        "[R5-E-05] %s第%d次失败(共%d次重试)，%.1fs后重试: %s",
                        getattr(func, '__name__', 'func'), attempt + 1, max_retries, delay, e
                    )
                if cancel_event is not None:
                    if cancel_event.wait(timeout=delay):
                        raise InterruptedError("R5-E-05: 重试被cancel_event中断")
                else:
                    time.sleep(delay)
            else:
                if logger:
                    logger.error(
                        "[R5-E-05] %s全部%d次重试失败: %s",
                        getattr(func, '__name__', 'func'), max_retries, e
                    )
    raise last_exception


# ============================================================================
# UPG-P1-04修复: API版本标记装饰器
# ============================================================================

def api_version(version: str, changelog: str = ""):
    """UPG-P1-04修复: API版本标记装饰器"""
    def decorator(func):
        func._api_version = version
        func._api_changelog = changelog
        func._api_versioned = True
        return func
    return decorator


def get_api_version(func) -> Optional[str]:
    """UPG-P1-04修复: 获取函数的API版本号"""
    return getattr(func, '_api_version', None)


def check_api_compatibility(func, min_version: str = "") -> bool:
    """UPG-P1-04修复: 检查API版本兼容性"""
    current = getattr(func, '_api_version', None)
    if current is None:
        return True
    if not min_version:
        return True
    return current >= min_version


# ============================================================================
# UPG-P1-05修复: 废弃API装饰器
# ============================================================================

def deprecated(reason: str = "", migration_guide: str = "", removal_version: str = ""):
    """UPG-P1-05修复: 废弃API装饰器"""
    def decorator(func):
        func._deprecated = True
        func._deprecated_reason = reason
        func._deprecated_migration_guide = migration_guide
        func._deprecated_removal_version = removal_version

        def wrapper(*args, **kwargs):
            msg_parts = [f"{func.__name__} is deprecated"]
            if reason:
                msg_parts.append(f"原因: {reason}")
            if migration_guide:
                msg_parts.append(f"迁移指南: {migration_guide}")
            if removal_version:
                msg_parts.append(f"计划在版本 {removal_version} 中移除")
            msg = ". ".join(msg_parts) + "."
            _warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper.__wrapped__ = func
        # 保留装饰器元数据
        wrapper._deprecated = True
        wrapper._deprecated_reason = reason
        wrapper._deprecated_migration_guide = migration_guide
        wrapper._deprecated_removal_version = removal_version
        # 保留api_version元数据（如果有的话）
        for attr in ('_api_version', '_api_changelog', '_api_versioned'):
            val = getattr(func, attr, None)
            if val is not None:
                setattr(wrapper, attr, val)
        return wrapper
    return decorator


# ============================================================================
# UPG-P1-13修复: 策略逻辑回归测试框架
# ============================================================================

class StrategyRegressionTest:
    """UPG-P1-13修复: 策略逻辑回归测试框架"""

    def __init__(self, test_name: str):
        self._test_name = test_name
        self._test_cases: List[Dict[str, Any]] = []

    def register(self, case_name: str, input_data: Any,
                 expected_output: Any, tolerance: float = 0.0,
                 comparator: Optional[Callable[[Any, Any], bool]] = None) -> None:
        self._test_cases.append({
            'name': case_name,
            'input': input_data,
            'expected': expected_output,
            'tolerance': tolerance,
            'comparator': comparator,
        })

    def run_all(self, func: Callable) -> Dict[str, Any]:
        results = []
        passed = 0
        failed = 0

        for case in self._test_cases:
            case_result = {
                'name': case['name'],
                'passed': False,
                'error': '',
                'actual': None,
            }
            try:
                actual = func(case['input'])
                case_result['actual'] = actual

                if case['comparator']:
                    case_result['passed'] = case['comparator'](actual, case['expected'])
                elif case['tolerance'] > 0 and isinstance(actual, (int, float)):
                    case_result['passed'] = abs(actual - case['expected']) <= case['tolerance']
                else:
                    case_result['passed'] = actual == case['expected']

                if case_result['passed']:
                    passed += 1
                else:
                    failed += 1
                    case_result['error'] = f"输出不匹配: expected={case['expected']}, actual={actual}"
            except Exception as e:
                failed += 1
                case_result['error'] = str(e)

            results.append(case_result)

        report = {
            'test_name': self._test_name,
            'passed': passed,
            'failed': failed,
            'total': len(self._test_cases),
            'details': results,
        }

        if failed > 0:
            logging.warning(
                "UPG-P1-13: 回归测试[%s] %d/%d 通过, %d 失败",
                self._test_name, passed, len(self._test_cases), failed,
            )
        else:
            logging.info(
                "UPG-P1-13: 回归测试[%s] 全部通过 (%d/%d)",
                self._test_name, passed, len(self._test_cases),
            )

        return report


# ============================================================================
# CS-03修复: 基于波动率缩放的执行延迟滑点计算
# ============================================================================

def compute_execution_delay_slippage_bps(
    price: float,
    bar_high: float,
    bar_low: float,
    bar_duration_sec: float = 60.0,
    exec_delay_ms: float = 50.0,
    z_score: float = 1.0,
) -> float:
    """基于波动率缩放的执行延迟滑点（布朗运动理论）"""
    if exec_delay_ms <= 0 or price <= 0:
        return 0.0

    # bar内波动率（bps）
    bar_range = (bar_high - bar_low) if bar_high > bar_low else price * 0.002
    bar_vol_bps = bar_range / price * 10000.0

    # 延迟占bar时长的比例，sqrt缩放（布朗运动假设）
    delay_ratio = min(exec_delay_ms / 1000.0 / max(bar_duration_sec, 1.0), 1.0)
    vol_scaled = bar_vol_bps * math.sqrt(delay_ratio)

    # z_score个标准差的不利变动
    delay_slippage_bps = vol_scaled * z_score

    return delay_slippage_bps


# ============================================================================
# CS-01: 统一滑点模型（生产/回测/评判三系统共享）
# ============================================================================

# 统一到期日滑点倍增系数
UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS = {
    1: 20.0,
    2: 10.0,
    3: 5.0,
    5: 3.0,
    7: 2.0,
    10: 1.5,
    14: 1.2,
}

# 统一品种滑点乘数
UNIFIED_INSTRUMENT_SLIPPAGE_MULTIPLIER = {
    "ETF": 1.0,
    "FUTURE": 1.2,
    "OPTION_ETF": 1.5,
    "OPTION_INDEX": 2.0,
    "OPTION_COMMODITY": 2.5,
}

# 统一quality_scale映射
UNIFIED_QUALITY_SCALE_MAP = {1: 0.75, 2: 0.5, 3: 0.3}

# 统一流动性层级
UNIFIED_LIQUIDITY_TIER_MAP = {
    'future_main': 1.0,
    'future_sub': 1.2,
    'option_atm': 1.0,
    'option_otm': 1.5,
    'option_far': 2.0,
}


def compute_slippage_bps(
    price: float,
    bid_ask_spread: float = 0.0,
    base_slippage_bps: float = 3.0,
    spread_quality: int = 1,
    days_to_expiry: int = 999,
    instrument_type: str = "ETF",
    liquidity_tier: str = "future_main",
    backtest_premium_bps: float = 0.0,
) -> float:
    """统一滑点计算函数（生产/回测/评判三系统共享）"""
    if price <= 0:
        return base_slippage_bps + backtest_premium_bps

    # 到期日倍增
    expiry_mult = 1.0
    if days_to_expiry >= 0:
        for threshold_days, multiplier in sorted(UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS.items()):
            if days_to_expiry <= threshold_days:
                expiry_mult = multiplier
                break

    # spread退化处理
    if bid_ask_spread <= 0 or spread_quality == 0:
        return base_slippage_bps * expiry_mult + backtest_premium_bps

    # spread转为bps
    spread_bps = bid_ask_spread / price * 10000.0

    # quality缩放
    quality_scale = UNIFIED_QUALITY_SCALE_MAP.get(spread_quality, 0.75)

    # 流动性层级
    liquidity_mult = UNIFIED_LIQUIDITY_TIER_MAP.get(liquidity_tier, 1.2)

    # 品种乘数
    instrument_mult = UNIFIED_INSTRUMENT_SLIPPAGE_MULTIPLIER.get(instrument_type, 1.0)

    # 基础滑点 = max(固定base, spread动态)
    base = max(base_slippage_bps, spread_bps * quality_scale * liquidity_mult) * instrument_mult

    return base * expiry_mult + backtest_premium_bps


# ============================================================================
# 结构性补充: 模型版本管理（回滚框架）
# ============================================================================

class ModelVersionManager:
    """模型版本管理器 — 支持运行时切换和回滚"""

    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self._versions = {
            'slippage_model': 'v2',
            'position_model': 'v2',
            'execution_delay_model': 'v2',
        }
        self._rollback_history = []

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_version(self, model_name: str) -> str:
        return self._versions.get(model_name, 'v1')

    def is_v2(self, model_name: str) -> bool:
        return self._versions.get(model_name, 'v1') == 'v2'

    def rollback(self, model_name: str, reason: str = ""):
        """回滚到旧版本"""
        if model_name not in self._versions:
            raise ValueError(f"Unknown model: {model_name}")
        old_version = self._versions[model_name]
        self._versions[model_name] = 'v1'
        self._rollback_history.append({
            'model': model_name,
            'from_version': old_version,
            'to_version': 'v1',
            'reason': reason,
            'timestamp': time.time(),
        })
        logging.warning("[ModelVersion] ROLLBACK: %s %s → v1, reason=%s",
                       model_name, old_version, reason)

    def get_rollback_history(self) -> list:
        return list(self._rollback_history)


# ============================================================================
# 结构性补充: 运行时监控指标
# ============================================================================

class RuntimeMonitor:
    """运行时监控 — 跟踪关键指标偏差"""

    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self._metrics = {
            'slippage_model_deviation': [],
            'position_size_ratio': [],
            'execution_delay_actual': [],
            'signal_latency_total': [],
            'strategy_eviction_rate': [],
            'model_version_consistency': True,
        }

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def record_slippage_deviation(self, estimated_bps: float, actual_bps: float):
        """记录滑点偏差"""
        if actual_bps > 0:
            deviation = abs(estimated_bps - actual_bps) / actual_bps
            self._metrics['slippage_model_deviation'].append(deviation)
            if len(self._metrics['slippage_model_deviation']) > 100:
                self._metrics['slippage_model_deviation'] = self._metrics['slippage_model_deviation'][-100:]
            recent = self._metrics['slippage_model_deviation'][-10:]
            if len(recent) >= 10 and all(d > 0.5 for d in recent):
                ModelVersionManager.get_instance().rollback(
                    'slippage_model',
                    f'连续10笔滑点偏差>50%, 最近偏差={recent}'
                )

    def record_position_ratio(self, production_lots: float, backtest_lots: float):
        """记录仓位比值"""
        if backtest_lots > 0:
            ratio = production_lots / backtest_lots
            self._metrics['position_size_ratio'].append(ratio)
            if len(self._metrics['position_size_ratio']) > 100:
                self._metrics['position_size_ratio'] = self._metrics['position_size_ratio'][-100:]

    def record_execution_delay(self, delay_ms: float):
        """记录执行延迟"""
        self._metrics['execution_delay_actual'].append(delay_ms)
        if len(self._metrics['execution_delay_actual']) > 1000:
            self._metrics['execution_delay_actual'] = self._metrics['execution_delay_actual'][-1000:]

    def record_signal_latency(self, latency_ms: float):
        """记录信号延迟"""
        self._metrics['signal_latency_total'].append(latency_ms)
        if len(self._metrics['signal_latency_total']) > 1000:
            self._metrics['signal_latency_total'] = self._metrics['signal_latency_total'][-1000:]

    def get_summary(self) -> Dict[str, Any]:
        """获取监控摘要"""
        import statistics
        summary = {}
        for key, values in self._metrics.items():
            if isinstance(values, list) and values:
                summary[key] = {
                    'count': len(values),
                    'p50': statistics.median(values) if values else 0,
                    'p99': sorted(values)[int(len(values) * 0.99)] if len(values) > 1 else values[0] if values else 0,
                    'latest': values[-1] if values else None,
                }
            else:
                summary[key] = values
        return summary


# ============================================================================
# SIG-P2-01: 交易动作与方向枚举（消除字符串比对残留）
# ============================================================================

class TradeAction(str):
    """交易动作枚举 — 继承str以保持序列化兼容性"""
    OPEN = 'OPEN'
    CLOSE = 'CLOSE'

class TradeDirection(str):
    """交易方向枚举 — 继承str以保持序列化兼容性"""
    BUY = 'BUY'
    SELL = 'SELL'

class SignalType(str):
    """信号类型枚举 — 继承str以保持序列化兼容性"""
    BUY = 'BUY'
    SELL = 'SELL'
    CLOSE_LONG = 'CLOSE_LONG'
    CLOSE_SHORT = 'CLOSE_SHORT'

# 合法值集合（用于校验）
VALID_TRADE_ACTIONS = {TradeAction.OPEN, TradeAction.CLOSE}
VALID_TRADE_DIRECTIONS = {TradeDirection.BUY, TradeDirection.SELL}
VALID_SIGNAL_TYPES = {SignalType.BUY, SignalType.SELL, SignalType.CLOSE_LONG, SignalType.CLOSE_SHORT}
OPEN_SIGNAL_TYPES = {SignalType.BUY, SignalType.SELL}
CLOSE_SIGNAL_TYPES = {SignalType.CLOSE_LONG, SignalType.CLOSE_SHORT}


# ============================================================================
# __all__ — 保持与原模块完全一致的公开API
# ============================================================================

__all__ = [
    'CHINA_TZ', 'sanitize_sql_identifier', 'sanitize_sql_value', 'unified_ts_ms',
    'DEFAULT_RISK_FREE_RATE', 'TRADING_DAYS_PER_YEAR_CHINA',
    'ANNUALIZE_FACTOR_DAILY', 'ANNUALIZE_FACTOR_MINUTE',
    'normalize_option_type', 'to_float32', 'normalize_instrument_id',
    'extract_product_code', 'extract_strike_price', 'normalize_year_month',
    'safe_int', 'safe_float', 'safe_price_check', 'RingBuffer', 'ShardRouter',
    'DEFAULT_SHARD_COUNT', 'get_shard_router', 'ThreadLifecycleManager',
    'InitPhase', 'InitializationError', 'InitStateMachine', 'requires_phase',
    'PendingPullbackSignal', 'PullbackManager', 'retry_with_limit',
    'PreconditionError', 'PostconditionError', 'OperationTimeoutError',
    'require_precondition', 'ensure_postcondition', 'with_timeout',
    'IdempotencyGuard', 'to_native_type', 'api_version', 'get_api_version',
    'check_api_compatibility', 'deprecated', 'atomic_replace_file',
    'cleanup_atomic_backup', 'AutoRollbackContext', 'check_version_sync',
    'check_shared_utils_invariants', 'StrategyRegressionTest',
    'stable_sum', 'stable_mean', 'stable_variance', 'stable_std',
    'approx_equal', 'approx_less', 'approx_greater',
    'should_trigger_stop_loss', 'should_trigger_take_profit',
    'KahanSummation', 'safe_divide', 'compute_sharpe_stable',
    'safe_normalize_weights', 'PRICE_TOLERANCE', 'FLOAT_COMPARE_TOLERANCE',
    'compute_slippage_bps', 'UNIFIED_EXPIRY_SLIPPAGE_MULTIPLIERS',
    'UNIFIED_INSTRUMENT_SLIPPAGE_MULTIPLIER', 'UNIFIED_QUALITY_SCALE_MAP',
    'UNIFIED_LIQUIDITY_TIER_MAP',
    'compute_execution_delay_slippage_bps',
    'ModelVersionManager',
    'RuntimeMonitor',
    'TradeAction', 'TradeDirection', 'SignalType',
    'VALID_TRADE_ACTIONS', 'VALID_TRADE_DIRECTIONS', 'VALID_SIGNAL_TYPES',
    'OPEN_SIGNAL_TYPES', 'CLOSE_SIGNAL_TYPES',
]
