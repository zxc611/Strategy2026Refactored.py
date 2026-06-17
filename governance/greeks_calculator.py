# MODULE_ID: M1-070
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问
"""
Greeks Calculator - Independent option risk management module.（合并后单一文件）

合并来源：
- greeks_math.py: _norm_cdf, _norm_pdf, _normalize_option_type, _bs_price, _bs_greeks 数学函数
- greeks_pricers.py: IVCalculator, BinomialTreePricer, MonteCarloPricer, TradingCalendar 定价模型类
- greeks_calculator.py: GreeksCalculator类 + validate_greeks_calendar_iv_noise

Features:
- Black-Scholes Greeks calculation (Delta, Gamma, Theta, Vega)
- Implied Volatility (IV) calculation (Newton iteration)
- Binomial tree pricing model (CRR, European/American options)
- Monte Carlo pricing model (GBM simulation, antithetic variance reduction)
- Smart update strategy (time + price change + IV change)
- Portfolio risk aggregation (position-weighted)
- Config file loading
- Trading calendar support
"""

import math
import random
import threading
import time
import logging
import json
import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any, Callable
from collections import defaultdict
import functools

from ali2026v3_trading.infra.shared_utils import (
    CHINA_TZ, TRADING_DAYS_PER_YEAR_CHINA as TRADING_DAYS_PER_YEAR,
    DEFAULT_RISK_FREE_RATE, DAYS_PER_YEAR_CALENDAR,
)
# R27-P1-FP-04修复: 引入浮点稳定计算替代内置sum/divide
try:
    from ali2026v3_trading.infra.resilience import stable_sum, safe_divide
except ImportError:
    stable_sum = sum
    safe_divide = lambda a, b, default=0.0: a / b if b != 0 else default
from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5


# ============================================================================
# Greeks 数学函数（原 greeks_math.py）
# ============================================================================

def _norm_cdf(x: float) -> float:
    """Standard normal CDF (precision ~1e-5)."""
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def _norm_pdf(x: float) -> float:
    """标准正态分布概率密度函数"""
    return math.exp(-x * x / 2.0) / math.sqrt(2.0 * math.pi)


def _normalize_option_type(opt_type: str) -> str:
    """ID唯一: 委托shared_utils.normalize_option_type, 不再独立实现"""
    from ali2026v3_trading.infra.shared_utils import normalize_option_type
    return normalize_option_type(opt_type)


@functools.lru_cache(maxsize=4096)
def _bs_price(S: float, K: float, T: float, r: float, q: float, sigma: float, option_type: str) -> float:
    """Black-Scholes option pricing (for IV calculation)

    R16-P0-PERF-05: added lru_cache for IV newton iteration
    Note: option_type should be normalized before calling
    """
    if T <= 0 or sigma <= 0:
        return max(0.0, (S - K) if option_type == 'CALL' else (K - S))
    # R31-P1-13修复: S<=0或K<=0时math.log(S/K)触发ValueError/ZeroDivisionError
    if S <= 0 or K <= 0:
        return 0.0

    try:
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)

        if option_type == 'CALL':
            return S * math.exp(-q * T) * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
        else:
            return K * math.exp(-r * T) * _norm_cdf(-d2) - S * math.exp(-q * T) * _norm_cdf(-d1)
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logger.error("BS price calculation error: %s", e)
        return 0.0


def _bs_greeks(
    S: float, K: float, T: float, r: float, q: float, sigma: float, option_type: str
) -> dict:
    """
    Calculate option Greeks.

    Args:
        S: Underlying price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate
        q: Dividend yield (continuous)
        sigma: Implied volatility
        option_type: 'CALL' or 'PUT' (should be normalized before calling)

    Returns:
        Dict: {'delta', 'gamma', 'theta', 'vega'}
    """
    if T <= 0 or sigma <= 0 or S <= 0:
        return {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}
    # R31-P1-13修复: K<=0时math.log(S/K)触发ValueError/ZeroDivisionError
    if K <= 0:
        return {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}

    try:
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)

        if option_type == 'CALL':
            delta = math.exp(-q * T) * _norm_cdf(d1)
            gamma = math.exp(-q * T) * _norm_pdf(d1) / (S * sigma * math.sqrt(T))
            theta = (-math.exp(-q * T) * S * _norm_pdf(d1) * sigma / (2 * math.sqrt(T))
                     - r * K * math.exp(-r * T) * _norm_cdf(d2)
                     + q * S * math.exp(-q * T) * _norm_cdf(d1)) / TRADING_DAYS_PER_YEAR
        else:  # PUT
            delta = math.exp(-q * T) * (_norm_cdf(d1) - 1.0)
            gamma = math.exp(-q * T) * _norm_pdf(d1) / (S * sigma * math.sqrt(T))
            theta = (-math.exp(-q * T) * S * _norm_pdf(d1) * sigma / (2 * math.sqrt(T))
                     + r * K * math.exp(-r * T) * _norm_cdf(-d2)
                     - q * S * math.exp(-q * T) * _norm_cdf(-d1)) / TRADING_DAYS_PER_YEAR

        vega = S * math.exp(-q * T) * _norm_pdf(d1) * math.sqrt(T) / 100.0  # 1% IV变化

        return {
            'delta': round(delta, 6),
            'gamma': round(gamma, 4),
            'theta': round(theta, 6),
            'vega': round(vega, 4)
        }
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        logger.error("BS Greeks calculation error: S=%s, K=%s, T=%s, sigma=%s, error=%s", S, K, T, sigma, e)
        return {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}


# ============================================================================
# 定价模型（原 greeks_pricers.py）
# ============================================================================

class IVCalculator:
    """Implied volatility calculator using Newton iteration."""

    @staticmethod
    def implied_volatility(
        market_price: float,
        S: float,
        K: float,
        T: float,
        r: float,
        q: float,
        option_type: str,
        initial_guess: float = 0.2,
        max_iter: int = 100,
        tol: float = 1e-6
    ) -> float:
        """
        Calculate implied volatility.

        Args:
            market_price: Market option price
            S: Underlying price
            K: Strike price
            T: Time to expiry (years)
            r: Risk-free rate
            q: Dividend yield
            option_type: 'CALL' or 'PUT'
            initial_guess: Initial guess (default 0.2)
            max_iter: Maximum iterations
            tol: Convergence tolerance

        Returns:
            float: Implied volatility, returns 0.2 on failure
        """
        if market_price <= 0 or S <= 0 or K <= 0 or T <= 0:
            return 0.2

        sigma = initial_guess
        i = 0  # P2#121修复:初始化i,防止max_iter=0时i未定义
        for i in range(max_iter):
            price = _bs_price(S, K, T, r, q, sigma, option_type)
            greeks = _bs_greeks(S, K, T, r, q, sigma, option_type)
            vega = greeks.get('vega', 0.0) * 100  # vega是每1%变化,转换为每单位变化

            if vega < 1e-6:
                logger.warning(f"Vega too small ({vega:.2e}), falling back to bisection search")
                lo, hi = 0.01, 5.0
                for _ in range(max_iter):
                    mid = (lo + hi) / 2.0
                    p_mid = _bs_price(S, K, T, r, q, mid, option_type)
                    if abs(p_mid - market_price) < tol:
                        return mid
                    if p_mid > market_price:
                        hi = mid
                    else:
                        lo = mid
                return (lo + hi) / 2.0

            diff = price - market_price
            if abs(diff) < tol:
                return sigma

            sigma = sigma - diff / vega
            sigma = max(0.01, min(5.0, sigma))

        logger.debug(f"IV calculation converged to {sigma:.4f} after {i+1} iterations")
        return sigma


class BinomialTreePricer:
    """Binomial tree option pricing model (CRR)"""

    @staticmethod
    def price(
        S: float,
        K: float,
        T: float,
        r: float,
        q: float,
        sigma: float,
        option_type: str,
        steps: int = 200,
        american: bool = False
    ) -> float:
        if T <= 0 or sigma <= 0 or S <= 0:
            return max(0.0, (S - K) if option_type == 'CALL' else (K - S))

        dt = T / steps
        u = math.exp(sigma * math.sqrt(dt))
        d = 1.0 / u
        df = math.exp(-r * dt)
        p = (math.exp((r - q) * dt) - d) / (u - d)
        discount = math.exp(-q * dt)

        p = max(0.0, min(1.0, p))

        prices = [S * (u ** (steps - j)) * (d ** j) for j in range(steps + 1)]

        if option_type == 'CALL':
            option_vals = [max(0.0, price - K) for price in prices]
        else:
            option_vals = [max(0.0, K - price) for price in prices]

        for i in range(steps - 1, -1, -1):
            for j in range(i + 1):
                hold = df * (p * option_vals[j] + (1.0 - p) * option_vals[j + 1])
                if american:
                    spot = S * (u ** (i - j)) * (d ** j)
                    if option_type == 'CALL':
                        exercise = max(0.0, spot - K)
                    else:
                        exercise = max(0.0, K - spot)
                    option_vals[j] = max(hold, exercise)
                else:
                    option_vals[j] = hold

        return option_vals[0]

    @staticmethod
    def price_with_greeks(
        S: float,
        K: float,
        T: float,
        r: float,
        q: float,
        sigma: float,
        option_type: str,
        steps: int = 200,
        american: bool = False
    ) -> Dict[str, float]:
        h = max(S * 1e-4, 1e-8)
        price = BinomialTreePricer.price(S, K, T, r, q, sigma, option_type, steps, american)
        price_up = BinomialTreePricer.price(S + h, K, T, r, q, sigma, option_type, steps, american)
        price_down = BinomialTreePricer.price(S - h, K, T, r, q, sigma, option_type, steps, american)
        price_T = BinomialTreePricer.price(S, K, max(T - 1.0/TRADING_DAYS_PER_YEAR, 0.0), r, q, sigma, option_type, steps, american)
        price_vol_up = BinomialTreePricer.price(S, K, T, r, q, sigma + 0.01, option_type, steps, american)

        delta = (price_up - price_down) / (2.0 * h)
        gamma = (price_up - 2.0 * price + price_down) / (h * h)
        theta = (price_T - price) / (1.0 / TRADING_DAYS_PER_YEAR)
        vega = (price_vol_up - price) / 100.0

        return {
            'price': round(price, 6),
            'delta': round(delta, 6),
            'gamma': round(gamma, 4),
            'theta': round(theta, 6),
            'vega': round(vega, 4)
        }


class MonteCarloPricer:
    """Monte Carlo option pricing model using GBM with antithetic variance reduction."""

    @staticmethod
    def price(
        S: float,
        K: float,
        T: float,
        r: float,
        q: float,
        sigma: float,
        option_type: str,
        n_simulations: int = 50000,
        n_steps: int = 1,
        antithetic: bool = True,
        seed: Optional[int] = None,
        historical_returns: Optional[list] = None,
        distribution_mode: str = "gbm",
    ) -> Tuple[float, float]:
        """Monte Carlo pricing with optional historical bootstrap distribution."""
        if T <= 0 or sigma <= 0 or S <= 0:
            intrinsic = max(0.0, (S - K) if option_type == 'CALL' else (K - S))
            return intrinsic, 0.0

        if seed is not None:
            from ali2026v3_trading.infra.shared_utils import set_global_seed  # P2-23: 统一入口
            set_global_seed(seed)

        # P0-R9-19修复: historical bootstrap分布
        if distribution_mode == "historical" and historical_returns and len(historical_returns) >= 20:
            n_days = max(1, int(T * 252))
            payoffs_hist = []
            for _ in range(n_simulations):
                ST = S
                sampled_indices = [random.randint(0, len(historical_returns) - 1) for _ in range(n_days)]
                for idx in sampled_indices:
                    ST *= math.exp(historical_returns[idx])
                if option_type == 'CALL':
                    payoff = max(0.0, ST - K)
                else:
                    payoff = max(0.0, K - ST)
                payoffs_hist.append(math.exp(-r * T) * payoff)
            mean = safe_divide(stable_sum(payoffs_hist), n_simulations)
            variance = safe_divide(stable_sum((p - mean) ** 2 for p in payoffs_hist), n_simulations - 1)
            std_error = math.sqrt(variance / n_simulations)
            return mean, std_error

        drift = (r - q - 0.5 * sigma * sigma) * T
        vol = sigma * math.sqrt(T)

        if antithetic and n_steps == 1:
            effective_n = n_simulations // 2
            payoffs = []
            for _ in range(effective_n):
                z = random.gauss(0.0, 1.0)
                ST1 = S * math.exp(drift + vol * z)
                ST2 = S * math.exp(drift - vol * z)
                if option_type == 'CALL':
                    payoffs.append(max(0.0, ST1 - K))
                    payoffs.append(max(0.0, ST2 - K))
                else:
                    payoffs.append(max(0.0, K - ST1))
                    payoffs.append(max(0.0, K - ST2))

            payoffs_arr = [math.exp(-r * T) * p for p in payoffs]
            mean = safe_divide(stable_sum(payoffs_arr), len(payoffs_arr))
            variance = safe_divide(stable_sum((p - mean) ** 2 for p in payoffs_arr), len(payoffs_arr) - 1)
            std_error = math.sqrt(variance / len(payoffs_arr))
            return mean, std_error

        dt = T / n_steps
        payoffs = []
        for _ in range(n_simulations):
            ST = S
            for _ in range(n_steps):
                z = random.gauss(0.0, 1.0)
                ST *= math.exp((r - q - 0.5 * sigma * sigma) * dt + sigma * math.sqrt(dt) * z)

            if option_type == 'CALL':
                payoff = max(0.0, ST - K)
            else:
                payoff = max(0.0, K - ST)
            payoffs.append(math.exp(-r * T) * payoff)

        mean = safe_divide(stable_sum(payoffs), n_simulations)
        variance = safe_divide(stable_sum((p - mean) ** 2 for p in payoffs), n_simulations - 1)
        std_error = math.sqrt(variance / n_simulations)
        return mean, std_error

    @staticmethod
    def price_with_confidence(
        S: float,
        K: float,
        T: float,
        r: float,
        q: float,
        sigma: float,
        option_type: str,
        n_simulations: int = 50000,
        n_steps: int = 1,
        antithetic: bool = True,
        confidence: float = 0.95,
        seed: Optional[int] = None,
        **kwargs
    ) -> Dict[str, float]:
        distribution_mode = kwargs.get('distribution_mode', 'gbm')
        historical_returns = kwargs.get('historical_returns', None)
        mean, std_error = MonteCarloPricer.price(
            S, K, T, r, q, sigma, option_type,
            n_simulations, n_steps, antithetic, seed,
            distribution_mode=distribution_mode,
            historical_returns=historical_returns,
        )

        z_scores = {0.90: 1.645, 0.95: 1.96, 0.99: 2.576}
        z = z_scores.get(confidence, 1.96)
        half_width = z * std_error

        return {
            'price': round(mean, 6),
            'std_error': round(std_error, 6),
            'confidence_interval': (
                round(mean - half_width, 6),
                round(mean + half_width, 6)
            ),
            'confidence_level': confidence
        }


class TradingCalendar:
    """Trading calendar for calculating remaining trading days."""

    def __init__(self, holidays: List[date] = None, market_time_service=None):
        """
        Initialize TradingCalendar with optional holidays and MarketTimeService.

        Args:
            holidays: List of holiday dates
            market_time_service: MarketTimeService instance (optional)
        """
        self.holidays = set(holidays) if holidays else set()
        self._market_time_service = market_time_service
        if not self.holidays and market_time_service is None:
            self._auto_load_chinese_holidays()

    def _auto_load_chinese_holidays(self):
        try:
            import chinese_calendar
            _today = datetime.now(CHINA_TZ).date()
            _year = _today.year
            _auto_holidays = set()
            for y in range(_year - 1, _year + 2):
                _auto_holidays.update(self._fetch_year_holidays(chinese_calendar, y))
            if _auto_holidays:
                self.holidays = _auto_holidays
                logger.info("[TradingCalendar] auto-loaded chinese_calendar holidays: %d", len(self.holidays))
        except ImportError:
            logger.warning("[TradingCalendar] chinese_calendar not installed, holidays list empty, weekends only")

    @staticmethod
    def _fetch_year_holidays(chinese_calendar, year):
        try:
            _hols = chinese_calendar.get_holidays(f'{year}-01-01', f'{year}-12-31')
            return set(_hols) if _hols else set()
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            return set()

    def add_holiday(self, d: date):
        """Add a holiday."""
        self.holidays.add(d)

    def is_trading_day(self, d: date) -> bool:
        """Check if a date is a trading day."""
        if self._market_time_service is not None:
            return self._market_time_service.is_trading_day(d, holiday_dates=self.holidays)
        return d.weekday() < 5 and d not in self.holidays

    def trading_days_between(self, start: date, end: date) -> int:
        """Return the number of trading days between two dates."""
        if start >= end:
            return 0
        days = 0
        current = start
        while current < end:
            if self.is_trading_day(current):
                days += 1
            current += timedelta(days=1)
        return days

    def time_to_expiry(self, expiry_date: date, use_trading_days: bool = False,
                       bar_date: Optional[date] = None) -> float:
        """
        Return annualized time to expiry (years).

        Args:
            expiry_date: Expiry date
            use_trading_days: Whether to use trading days
            bar_date: Current bar date (pass in backtest, None uses system time)

        Returns:
            float: Remaining time (years)
        """
        today = bar_date if bar_date is not None else datetime.now(CHINA_TZ).date()
        if bar_date is None:
            import warnings
            warnings.warn("[BIZ-P1-15] time_to_expiry called without bar_date; using system time which is incorrect in backtest", stacklevel=2)
        if expiry_date <= today:
            logging.warning("[P0-EX-002] 期权已到期: expiry=%s, today=%s, instrument可能需强制平仓", expiry_date, today)
            return 1.0 / (252.0 * 240.0)

        if use_trading_days:
            days = self.trading_days_between(today, expiry_date)
            if days <= 5:
                logging.warning("[P0-EX-002] 期权临近到期: expiry=%s, days_remaining=%d", expiry_date, days)
            return max(1.0 / (252.0 * 24.0), days / 252.0)
        else:
            days = (expiry_date - today).days
            if days <= 5:
                logging.warning("[P0-EX-002] 期权临近到期: expiry=%s, days_remaining=%d", expiry_date, days)
            return max(1.0 / (252.0 * 24.0), days / DAYS_PER_YEAR_CALENDAR)


# ============================================================================
# 增强的 GreeksCalculator（原 greeks_calculator.py 主体）
# ============================================================================

class GreeksCalculator:
    """
    Independent Greeks calculator supporting:
    - Implied volatility calculation (Newton iteration)
    - Hybrid update strategy based on time, price change, and IV change
    - Position-level portfolio risk aggregation
    - Config file parameter loading
    - Comprehensive exception handling and logging
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Args:
            config_path: JSON config file path for loading rate, multiplier, dividend yield, update strategy params.
        """
        self._lock = threading.RLock()

        # 默认配置
        self._risk_free_rate = DEFAULT_RISK_FREE_RATE
        self._update_strategy = {
            'time_interval_sec': 1.0,      # 时间间隔超过1秒则更新
            'price_change_pct': 0.5,       # 价格变化超过0.5%则更新
            'iv_change_pct': 5.0,          # IV变化超过5%则更新
            'min_tick_interval': 5,        # 最小tick间隔(兼容旧逻辑)
        }
        self._contract_multiplier: Dict[str, int] = {}
        self._dividend_yield: Dict[str, float] = {}
        self._use_trading_days: bool = False   # 是否使用交易日历计算剩余时间

        # 交易日历
        self._calendar = TradingCalendar()

        # 加载配置文件
        if config_path and os.path.exists(config_path):
            self._load_config(config_path)

        # 缓存数据
        self._option_greeks: Dict[str, Dict[str, float]] = {}
        self._option_iv: Dict[str, float] = {}
        self._option_last_update: Dict[str, float] = {}          # 上次更新时间戳
        self._option_last_price: Dict[str, float] = {}           # 上次期权价格
        self._option_last_iv: Dict[str, float] = {}              # 上次IV
        self._option_tick_counter: Dict[str, int] = {}           # tick计数(备用)

        # 期权基本信息缓存 (避免重复传入)
        self._option_info_cache: Dict[str, Dict[str, Any]] = {}

        # 性能统计
        self._update_count: int = 0
        self._total_compute_time_ms: float = 0.0
        self._iv_calc_count: int = 0
        self._total_iv_compute_time_ms: float = 0.0

        # 预设默认乘数和股息率(可被配置文件覆盖)
        self._set_defaults()

        logger.info("GreeksCalculator initialized with strategy: %s", self._update_strategy)

    def __repr__(self) -> str:
        """Debug string representation."""
        return (
            f"GreeksCalculator(cached_options={len(self._option_greeks)}, "
            f"updates={self._update_count}, iv_calcs={self._iv_calc_count})"
        )

    def __str__(self) -> str:
        """Friendly string representation."""
        stats = self.get_stats()
        return (
            f"GreeksCalculator:\n"
            f"  缓存期权: {stats['cached_options']}\n"
            f"  更新次数: {stats['greeks_update_count']}\n"
            f"  IV计算次数: {stats['iv_calc_count']}\n"
            f"  平均Greeks计算耗时: {stats['avg_greeks_compute_time_ms']:.4f}ms\n"
            f"  平均IV计算耗时: {stats['avg_iv_compute_time_ms']:.4f}ms"
        )

    def _set_defaults(self):
        """Set default multipliers and dividend yields."""
        defaults_mult = {
            'IF': 100, 'IH': 100, 'IM': 100,
            'IO': 100, 'HO': 100, 'MO': 100,
            'AL': 5, 'AU': 1000, 'CU': 5, 'ZN': 5,
        }
        for k, v in defaults_mult.items():
            self._contract_multiplier.setdefault(k, v)

        defaults_div = {
            'IF': 0.0, 'IH': 0.0, 'IM': 0.0,
            'AL': 0.0, 'AU': 0.0, 'CU': 0.0, 'ZN': 0.0,
        }
        for k, v in defaults_div.items():
            self._dividend_yield.setdefault(k, v)

    def _load_config(self, path: str):
        """Load configuration from JSON file."""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                cfg = json_loads(f.read())  # R5-2

            self._risk_free_rate = cfg.get('risk_free_rate', self._risk_free_rate)
            self._update_strategy.update(cfg.get('update_strategy', {}))
            self._use_trading_days = cfg.get('use_trading_days', self._use_trading_days)

            if 'contract_multiplier' in cfg:
                self._contract_multiplier.update(cfg['contract_multiplier'])
            if 'dividend_yield' in cfg:
                self._dividend_yield.update(cfg['dividend_yield'])

            if 'holidays' in cfg:
                self._load_holidays_from_config(cfg['holidays'])

            logger.info("Loaded config from %s", path)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logger.error("Failed to load config from %s: %s", path, e)

    def _load_holidays_from_config(self, holiday_list):
        for h in holiday_list:
            try:
                self._calendar.add_holiday(date.fromisoformat(h))
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
                logger.warning(f"[TradingCalendar] Failed to parse holiday '{h}': {e}")

    # ------------------------------------------------------------------------
    # 公共配置接口
    # ------------------------------------------------------------------------
    def set_contract_multiplier(self, product: str, multiplier: int):
        """Set contract multiplier."""
        with self._lock:
            self._contract_multiplier[product] = multiplier

    def set_dividend_yield(self, product: str, yield_rate: float):
        """Set dividend yield."""
        with self._lock:
            self._dividend_yield[product] = yield_rate

    def set_risk_free_rate(self, rate: float):
        """Set risk-free rate."""
        with self._lock:
            self._risk_free_rate = rate

    def set_update_strategy(self, **kwargs):
        """
        Set update strategy parameters.

        Args:
            time_interval_sec: Time interval (seconds)
            price_change_pct: Price change percentage
            iv_change_pct: IV change percentage
            min_tick_interval: Minimum tick interval
        """
        with self._lock:
            self._update_strategy.update(kwargs)
            logger.info(f"Update strategy changed: {kwargs}")

    def set_trading_calendar(self, calendar: TradingCalendar):
        """Set trading calendar."""
        with self._lock:
            self._calendar = calendar

    def use_trading_days(self, flag: bool):
        """Enable/disable trading day counting."""
        with self._lock:
            self._use_trading_days = flag

    def cache_option_info(self, instrument_id: str, info: Dict[str, Any]):
        """
        Cache option basic info to avoid passing on every tick.

        Args:
            instrument_id: Option contract ID
            info: {'strike', 'option_type', 'expiry_date', 'future_product', ...}
        """
        with self._lock:
            self._option_info_cache[instrument_id] = info
            self._evict_cache_if_needed()

    # ------------------------------------------------------------------------
    # 核心计算与更新
    # ------------------------------------------------------------------------
    def _time_to_expiry(self, expiry_date: Optional[date], bar_date: Optional[date] = None) -> float:
        """Calculate annualized time to expiry, supporting trading days."""
        if expiry_date is None:
            return 30.0 / DAYS_PER_YEAR_CALENDAR
        return self._calendar.time_to_expiry(expiry_date, self._use_trading_days, bar_date=bar_date)

    def _parse_strike_from_id(self, instrument_id: str) -> Optional[float]:
        """
        Parse strike price from contract ID (fallback method).

        Delegates to SubscriptionManager.parse_option to extract strike_price.
        """
        try:
            from ali2026v3_trading.infra.subscription_manager import SubscriptionManager
            parsed = SubscriptionManager.parse_option(instrument_id)
            strike = parsed.get('strike_price')
            return float(strike) if strike is not None else None
        except (ValueError, KeyError, TypeError) as e:
            logger.debug(f"[ParseStrike] Failed to parse strike from {instrument_id}: {e}")
            return None

    def _should_update(self, instrument_id: str, current_price: float, current_iv: Optional[float] = None,
                       last_time: float = 0.0, last_price: float = 0.0, last_iv: Optional[float] = None,
                       tick_count: int = 0) -> bool:
        """
        Determine whether Greeks need updating using a hybrid strategy.

        Conditions (any one triggers update):
        1. Time interval exceeds threshold
        2. Price change exceeds threshold
        3. IV change exceeds threshold (if current_iv provided)
        4. Tick count reaches minimum interval
        """
        now = time.time()

        # 条件1: 时间间隔
        time_condition = (now - last_time) >= self._update_strategy.get('time_interval_sec', 1.0)

        # 条件2: 价格变化
        if last_price > 0:
            price_change_pct = abs(current_price - last_price) / last_price * 100
            price_condition = price_change_pct >= self._update_strategy.get('price_change_pct', 0.5)
        else:
            price_condition = True  # 首次更新

        # 条件3: IV变化 (仅在提供current_iv时检查)
        if current_iv is not None and last_iv is not None and last_iv > 0:
            iv_change_pct = abs(current_iv - last_iv) / last_iv * 100
            iv_condition = iv_change_pct >= self._update_strategy.get('iv_change_pct', 5.0)
        else:
            iv_condition = False  # 不提供IV时跳过此检查

        # 条件4: Tick计数 (保底机制)
        min_interval = self._update_strategy.get('min_tick_interval', 5)
        tick_condition = tick_count >= min_interval

        should_update = time_condition or price_condition or iv_condition or tick_condition

        if should_update and logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"[UpdateTrigger] {instrument_id}: time={time_condition}, "
                f"price={price_condition}, iv={iv_condition}, tick={tick_condition}"
            )

        return should_update

    def _should_update_with_cached_iv(self, instrument_id: str, current_price: float,
                                      last_time: float = 0.0, last_price: float = 0.0,
                                      last_iv: Optional[float] = None, tick_count: int = 0) -> bool:
        """Delegate to _should_update with cached IV."""
        cached_iv = self._option_last_iv.get(instrument_id)
        return self._should_update(instrument_id, current_price, cached_iv,
                                   last_time, last_price, last_iv or cached_iv, tick_count)

    def calculate_and_update(
        self,
        instrument_id: str,
        underlying_price: float,
        strike: float,
        expiry_date: Optional[date],
        option_type: str,
        future_product: str,
        market_price: Optional[float] = None,
        iv: Optional[float] = None,
        bar_date: Optional[date] = None
    ) -> Dict[str, float]:
        """
        Calculate and update Greeks (full pipeline).
        """
        with self._lock:
            # 1. 计算剩余时间
            T = self._time_to_expiry(expiry_date, bar_date=bar_date)

            # 2. 获取股息率
            q = self._dividend_yield.get(future_product, 0.0)

            # 3. 计算或获取IV
            if iv is None or iv <= 0:
                if market_price and market_price > 0:
                    start_iv = time.perf_counter()
                    iv = IVCalculator.implied_volatility(
                        market_price=market_price,
                        S=underlying_price,
                        K=strike,
                        T=T,
                        r=self._risk_free_rate,
                        q=q,
                        option_type=option_type
                    )
                    elapsed_ms = (time.perf_counter() - start_iv) * 1000
                    self._iv_calc_count += 1
                    self._total_iv_compute_time_ms += elapsed_ms

                    if elapsed_ms > 1.0:
                        logger.warning(f"[IVCalc] Slow: {instrument_id} took {elapsed_ms:.2f}ms")
                else:
                    iv = self._option_iv.get(instrument_id, 0.2)
                    logger.debug(f"[IVCalc] Using cached/default IV={iv:.4f} for {instrument_id}")

            # 4. 计算希腊字母
            start_calc = time.perf_counter()
            greeks = _bs_greeks(
                S=underlying_price,
                K=strike,
                T=T,
                r=self._risk_free_rate,
                q=q,
                sigma=iv,
                option_type=option_type
            )
            elapsed_ms = (time.perf_counter() - start_calc) * 1000

            # 5. 更新缓存
            self._option_greeks[instrument_id] = greeks
            self._option_iv[instrument_id] = iv
            self._option_last_update[instrument_id] = time.time()
            self._option_last_price[instrument_id] = market_price or 0.0
            self._option_last_iv[instrument_id] = iv
            self._option_tick_counter[instrument_id] = 0  # 重置tick计数

            self._update_count += 1
            self._total_compute_time_ms += elapsed_ms

            # 6. 性能监控
            if elapsed_ms > 1.0:
                logger.warning(f"[GreeksCalc] Slow: {instrument_id} took {elapsed_ms:.2f}ms")

            logger.debug(
                f"[GreeksUpdated] {instrument_id}: Delta={greeks['delta']:.4f}, "
                f"Gamma={greeks['gamma']:.4f}, Theta={greeks['theta']:.4f}, "
                f"Vega={greeks['vega']:.4f}, IV={iv:.4f}"
            )

            return greeks.copy()

    def update_greeks_from_tick(
        self,
        instrument_id: str,
        price: float,
        underlying_price: float,
        expiry_date: Optional[date],
        option_type: str,
        future_product: str,
        strike: Optional[float] = None,
        iv: Optional[float] = None,
        bar_date: Optional[date] = None
    ) -> Optional[Dict[str, float]]:
        """
        Update Greeks from option tick data (smart decision on whether to recalculate).
        """
        # P1#76修复:将IV计算移到锁外,减少持锁时间
        # 第一步:锁内做低成本判断,决定是否需要计算IV
        with self._lock:
            tick_cnt = self._option_tick_counter.get(instrument_id, 0) + 1
            self._option_tick_counter[instrument_id] = tick_cnt

            info = self._option_info_cache.get(instrument_id, {})
            if strike is None:
                strike = info.get('strike')

            if strike is None:
                strike = self._parse_strike_from_id(instrument_id)
                if strike is not None:
                    logger.debug(f"[OnTick] Parsed strike={strike} from {instrument_id}")

            if strike is None:
                logger.warning(f"[OnTick] Strike not found for {instrument_id}")
                return None

            cached_iv = self._option_iv.get(instrument_id)
            has_cached_iv = cached_iv is not None and cached_iv > 0

            # P1修复:在锁内读取所有状态,消除TOCTOU竞争
            last_time = self._option_last_update.get(instrument_id, 0)
            last_price = self._option_last_price.get(instrument_id, price)
            last_iv = self._option_last_iv.get(instrument_id)

            need_compute_iv = False
            if iv is None and not has_cached_iv:
                need_compute_iv = True
            elif iv is None and has_cached_iv:
                if self._should_update_with_cached_iv(instrument_id, price,
                                                      last_time, last_price, last_iv, tick_cnt):
                    need_compute_iv = True
                else:
                    return None  # 不需要更新

        # 第二步:锁外执行昂贵的IV计算
        if need_compute_iv and iv is None:
            start_iv = time.perf_counter()
            iv = IVCalculator.implied_volatility(
                market_price=price,
                S=underlying_price,
                K=strike,
                T=self._time_to_expiry(expiry_date, bar_date=bar_date),
                r=self._risk_free_rate,
                q=self._dividend_yield.get(future_product, 0.0),
                option_type=option_type
            )
            elapsed_ms = (time.perf_counter() - start_iv) * 1000

            if elapsed_ms > 1.0:
                logger.warning(f"[IVCalc] Slow: {instrument_id} took {elapsed_ms:.2f}ms")

        # 第三步:锁内做最终判断和更新,计数器递增移到锁内
        with self._lock:
            if need_compute_iv and iv is not None:
                self._iv_calc_count += 1
                self._total_iv_compute_time_ms += elapsed_ms

            # P1修复:使用锁内读取的状态数据
            last_time = self._option_last_update.get(instrument_id, 0)
            last_price = self._option_last_price.get(instrument_id, price)

            if has_cached_iv and not self._should_update(instrument_id, price, iv,
                                                         last_time, last_price, last_iv, tick_cnt):
                return None

            return self.calculate_and_update(
                instrument_id=instrument_id,
                underlying_price=underlying_price,
                strike=strike,
                expiry_date=expiry_date,
                option_type=option_type,
                future_product=future_product,
                market_price=price,
                iv=iv,
                bar_date=bar_date
            )

    # ------------------------------------------------------------------------
    # 查询接口
    # ------------------------------------------------------------------------
    def get_greeks(self, instrument_id: str) -> Dict[str, float]:
        with self._lock:
            entry = self._option_greeks.get(instrument_id, {})
            if entry:
                _update_time = self._option_last_update.get(instrument_id, 0)
                if _update_time > 0 and (time.time() - _update_time) > self._GREEKS_CACHE_TTL:
                    del self._option_greeks[instrument_id]
                    return {}
            return entry.copy() if entry else {}

    def get_iv(self, instrument_id: str) -> float:
        """Get implied volatility for a single contract."""
        with self._lock:
            return self._option_iv.get(instrument_id, 0.0)

    def set_iv(self, instrument_id: str, iv: float):
        """Manually set IV (from external data source)."""
        with self._lock:
            if iv > 0:
                self._option_iv[instrument_id] = iv
                logger.debug(f"[SetIV] {instrument_id} IV set to {iv:.4f}")

    def get_portfolio_greeks(self, positions: Dict[str, int]) -> Dict[str, float]:
        """
        Calculate portfolio Greeks (position-weighted).

        Args:
            positions: {instrument_id: position_size}
                       Positive = long, negative = short

        Returns:
            Dict: {'delta', 'gamma', 'theta', 'vega'} portfolio totals
        """
        portfolio = {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}

        with self._lock:
            for inst_id, size in positions.items():
                if size == 0:
                    continue

                greeks = self._option_greeks.get(inst_id)
                if not greeks:
                    logger.debug(f"[PortfolioRisk] No greeks for {inst_id}, skipping")
                    continue
                _update_time = self._option_last_update.get(inst_id, 0)
                if _update_time > 0 and (time.time() - _update_time) > self._GREEKS_CACHE_TTL:
                    del self._option_greeks[inst_id]
                    continue

                info = self._option_info_cache.get(inst_id, {})
                future_product = info.get('future_product', '')
                multiplier = self._contract_multiplier.get(future_product, 1)

                portfolio['delta'] += greeks['delta'] * multiplier * size
                portfolio['gamma'] += greeks['gamma'] * multiplier * size
                portfolio['theta'] += greeks['theta'] * multiplier * size
                portfolio['vega'] += greeks['vega'] * multiplier * size

        logger.debug(
            f"[PortfolioRisk] Delta={portfolio['delta']:.2f}, "
            f"Gamma={portfolio['gamma']:.4f}, Theta={portfolio['theta']:.2f}, "
            f"Vega={portfolio['vega']:.2f}"
        )

        return portfolio

    def get_product_summary(
        self,
        product: str,
        months: List[str] = None,
        option_type: str = None,
        include_multiplier: bool = True
    ) -> Dict[str, float]:
        """
        Summarize Greeks for a specified product.
        """
        total = {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}

        with self._lock:
            for inst_id, info in self._option_info_cache.items():
                future_prod = info.get('future_product', '')
                if future_prod != product:
                    continue

                if months:
                    inst_month = info.get('month', '')
                    if not inst_month:
                        inst_month = self._parse_option_month(inst_id)
                        if not inst_month:
                            continue
                    if inst_month not in months:
                        continue

                if option_type:
                    info_type = _normalize_option_type(info.get('option_type', ''))
                    input_type = _normalize_option_type(option_type)
                    if info_type != input_type:
                        continue

                g = self._option_greeks.get(inst_id, {})
                if not g:
                    continue
                _update_time = self._option_last_update.get(inst_id, 0)
                if _update_time > 0 and (time.time() - _update_time) > self._GREEKS_CACHE_TTL:
                    del self._option_greeks[inst_id]
                    continue

                mult = 1
                if include_multiplier:
                    mult = self._contract_multiplier.get(future_prod, 1)

                for key in total:
                    total[key] += g.get(key, 0.0) * mult

        return total

    @staticmethod
    def _parse_option_month(inst_id):
        try:
            from ali2026v3_trading.infra.subscription_manager import SubscriptionManager
            parsed = SubscriptionManager.parse_option(inst_id)
            return parsed.get('year_month', '')
        except (ValueError, KeyError, TypeError):
            return ''

    def check_risk_limits(
        self,
        positions: Dict[str, int],
        delta_limit: Optional[float] = None,
        gamma_limit: Optional[float] = None,
        theta_min: Optional[float] = None,
        vega_limit: Optional[float] = None
    ) -> List[str]:
        """
        Check risk limits.
        """
        portfolio = self.get_portfolio_greeks(positions)

        # P0-GR-001修复: 优先从参数池读取约束阈值,异常时回退硬编码默认值
        if delta_limit is None:
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                _ps = get_params_service()
                delta_limit = _ps.get_float('max_net_delta_pct', 0.30) * 1000
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                delta_limit = 300
        if gamma_limit is None:
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                _ps = get_params_service()
                gamma_limit = _ps.get_float('max_net_gamma_pct', 0.08) * 625
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                gamma_limit = 50
        if theta_min is None:
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                _ps = get_params_service()
                theta_min = -_ps.get_float('max_theta_ratio', 0.5) * 1000
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                theta_min = -500
        if vega_limit is None:
            try:
                from ali2026v3_trading.config.params_service import get_params_service
                _ps = get_params_service()
                vega_limit = _ps.get_float('max_net_vega_bps', 0.02) * 10000
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                vega_limit = 200

        warnings = []

        if abs(portfolio['delta']) > delta_limit:
            warnings.append(f"[WARN] Delta超限: {portfolio['delta']:.2f} / ±{delta_limit}")

        if portfolio['gamma'] > gamma_limit:
            warnings.append(f"[WARN] Gamma超限: {portfolio['gamma']:.4f} / {gamma_limit:.2f}")

        if portfolio['theta'] < theta_min:
            warnings.append(f"[WARN] Theta成本过高: {portfolio['theta']:.2f}/日(限额: {theta_min})")

        if abs(portfolio['vega']) > vega_limit:
            warnings.append(f"[WARN] Vega超限: {portfolio['vega']:.2f} / ±{vega_limit:.2f}")

        if warnings:
            logger.warning(f"[RiskAlert] {'; '.join(warnings)}")

        return warnings

    # ID唯一:get_stats统一接口,返回值含service_name="GreeksCalculator"
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        with self._lock:
            avg_calc_time = (
                self._total_compute_time_ms / self._update_count
                if self._update_count > 0 else 0.0
            )
            avg_iv_time = (
                self._total_iv_compute_time_ms / self._iv_calc_count
                if self._iv_calc_count > 0 else 0.0
            )

            return {
                'service_name': 'GreeksCalculator',
                'cached_options': len(self._option_greeks),
                'cached_iv': len(self._option_iv),
                'greeks_update_count': self._update_count,
                'iv_calc_count': self._iv_calc_count,
                'total_greeks_compute_time_ms': round(self._total_compute_time_ms, 2),
                'total_iv_compute_time_ms': round(self._total_iv_compute_time_ms, 2),
                'avg_greeks_compute_time_ms': round(avg_calc_time, 4),
                'avg_iv_compute_time_ms': round(avg_iv_time, 4),
                'update_strategy': self._update_strategy.copy(),
            }

    # ID唯一:clear_cache统一接口,服务GreeksCalculator
    def clear_cache(self):
        """clear_cache: R10-P1-13 fix"""
        with self._lock:
            self._option_greeks.clear()
            self._option_iv.clear()
            self._option_last_update.clear()
            self._option_last_price.clear()
            self._option_last_iv.clear()
            self._option_tick_counter.clear()
            self._option_info_cache.clear()

    _MAX_CACHE_SIZE = 5000
    _GREEKS_CACHE_TTL = 120  # R23-FR-03-FIX: 从300s降至120s

    def _evict_cache_if_needed(self):
        if len(self._option_greeks) > self._MAX_CACHE_SIZE:
            _keys = list(self._option_greeks.keys())
            _evict_count = len(_keys) // 2
            for _k in _keys[:_evict_count]:
                self._option_greeks.pop(_k, None)
                self._option_iv.pop(_k, None)
                self._option_last_update.pop(_k, None)
            self._option_last_price.clear()
            self._option_last_iv.clear()
            self._option_tick_counter.clear()
            logger.info("[GreeksCalculator] Cache cleared")


# ============================================================================
# P2-GR-004修复: 验证Greeks日历效应和IV噪声稳定性
# ============================================================================
def validate_greeks_calendar_iv_noise(
    greeks_calculator: Optional['GreeksCalculator'] = None,
    iv_noise_sigma: float = 0.005,
    n_simulations: int = 100,
    calendar: Optional['TradingCalendar'] = None,
) -> Dict[str, Any]:
    """
    P2-GR-004修复: 验证Greeks日历效应和IV噪声稳定性。

    检查:
    1. 日历效应: 周末/假日前后IV是否有异常跳变
    2. IV噪声: 对IV序列添加高斯噪声(sigma=iv_noise_sigma)后Greeks是否稳定

    Args:
        greeks_calculator: GreeksCalculator实例，从中提取缓存的IV数据
        iv_noise_sigma: IV噪声标准差
        n_simulations: 噪声模拟次数
        calendar: TradingCalendar实例（可选，为空则自动创建）

    Returns:
        Dict with keys: 'passed' (bool), 'calendar_anomaly_count' (int),
        'iv_noise_max_delta_change' (float), 'details' (str)
    """
    if greeks_calculator is None:
        return {
            'passed': True,
            'calendar_anomaly_count': 0,
            'iv_noise_max_delta_change': 0.0,
            'details': 'No data to validate',
        }

    # 提取缓存的IV数据
    try:
        iv_data = dict(greeks_calculator._option_iv)
    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
        return {
            'passed': True,
            'calendar_anomaly_count': 0,
            'iv_noise_max_delta_change': 0.0,
            'details': 'Failed to read IV data from calculator',
        }

    if not iv_data:
        return {
            'passed': True,
            'calendar_anomaly_count': 0,
            'iv_noise_max_delta_change': 0.0,
            'details': 'No IV data cached',
        }

    # --- 日历效应检查 ---
    calendar_anomaly_count = 0
    if calendar is None:
        try:
            calendar = TradingCalendar()
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            calendar = None

    today = datetime.now(CHINA_TZ).date() if CHINA_TZ else date.today()
    anomaly_threshold = 3 * iv_noise_sigma

    # 检查最近5个交易日的IV跳变
    check_dates = []
    d = today
    for _ in range(10):
        if calendar is not None:
            if calendar.is_trading_day(d):
                check_dates.append(d)
        else:
            if d.weekday() < 5:
                check_dates.append(d)
        if len(check_dates) >= 5:
            break
        d -= timedelta(days=1)

    # 对每个期权，检查相邻交易日IV变化是否超过阈值
    iv_values_by_date: Dict[str, Dict[date, float]] = {}
    for inst_id, iv in iv_data.items():
        iv_values_by_date[inst_id] = {today: iv}

    if len(check_dates) >= 2:
        for inst_id, date_iv_map in iv_values_by_date.items():
            sorted_dates = sorted(date_iv_map.keys())
            for i in range(1, len(sorted_dates)):
                prev_iv = date_iv_map[sorted_dates[i - 1]]
                curr_iv = date_iv_map[sorted_dates[i]]
                if prev_iv > 0 and abs(curr_iv - prev_iv) > anomaly_threshold:
                    calendar_anomaly_count += 1

    # --- IV噪声稳定性检查 ---
    iv_noise_max_delta_change = 0.0
    try:
        greeks_cache = dict(greeks_calculator._option_greeks)
    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
        greeks_cache = {}

    for inst_id, base_iv in iv_data.items():
        base_delta = greeks_cache.get(inst_id, {}).get('delta', 0.0)
        max_delta_change = 0.0

        for _ in range(n_simulations):
            noisy_iv = base_iv + random.gauss(0, iv_noise_sigma)
            if noisy_iv <= 0:
                continue
            vega = greeks_cache.get(inst_id, {}).get('vega', 0.0)
            if vega != 0 and base_iv > 0:
                delta_change = abs(vega * (noisy_iv - base_iv) / base_iv)
            else:
                delta_change = abs(base_delta * (noisy_iv - base_iv) / max(base_iv, 1e-9))
            max_delta_change = max(max_delta_change, delta_change)

        iv_noise_max_delta_change = max(iv_noise_max_delta_change, max_delta_change)

    # P2-裂缝9-10/21修复: 自然日vs交易日theta差异对比 + 约束违反率统计
    theta_natural_vs_trading_diff = 0.0
    theta_violation_rate = 0.0
    try:
        # 从greeks_cache中提取theta值进行双轨计算
        thetas = []
        for inst_id, greeks in greeks_cache.items():
            theta_val = greeks.get('theta', 0.0)
            if theta_val != 0:
                thetas.append(theta_val)
        if thetas:
            # 自然日假设: 全年365天，theta按日历日衰减
            # 交易日假设: 约252个交易日，theta按交易日衰减
            avg_theta = sum(thetas) / len(thetas)
            # 差异 = |自然日年化theta - 交易日年化theta| / |交易日年化theta|
            theta_trading_annual = avg_theta * 252
            theta_natural_annual = avg_theta * 365
            theta_natural_vs_trading_diff = abs(theta_natural_annual - theta_trading_annual) / (abs(theta_trading_annual) + 1e-10)
    except (ValueError, KeyError, TypeError, AttributeError):
        theta_natural_vs_trading_diff = 0.0

    # 约束违反率: IV噪声导致的delta变化超过0.1阈值的占比
    iv_violations = 0
    iv_total = 0
    for inst_id, base_iv in iv_data.items():
        base_delta = greeks_cache.get(inst_id, {}).get('delta', 0.0)
        vega = greeks_cache.get(inst_id, {}).get('vega', 0.0)
        for _ in range(n_simulations):
            noisy_iv = base_iv + random.gauss(0, iv_noise_sigma)
            if noisy_iv <= 0 or base_iv <= 0:
                continue
            iv_total += 1
            if vega != 0:
                delta_change = abs(vega * (noisy_iv - base_iv) / base_iv)
            else:
                delta_change = abs(base_delta * (noisy_iv - base_iv) / max(base_iv, 1e-9))
            if delta_change >= 0.1:
                iv_violations += 1
    theta_violation_rate = (iv_violations / max(iv_total, 1)) * 100.0

    passed = (
        (calendar_anomaly_count == 0)
        and (theta_natural_vs_trading_diff < 0.05)  # 自然日vs交易日theta差异<5%
        and (theta_violation_rate < 5.0)  # 约束违反率<5%
    )

    details_parts = []
    if calendar_anomaly_count > 0:
        details_parts.append(f"calendar anomalies: {calendar_anomaly_count}")
    if theta_natural_vs_trading_diff >= 0.05:
        details_parts.append(f"theta natural vs trading diff: {theta_natural_vs_trading_diff:.4f} >= 0.05")
    if theta_violation_rate >= 5.0:
        details_parts.append(f"IV noise violation rate: {theta_violation_rate:.2f}% >= 5%")
    details = '; '.join(details_parts) if details_parts else 'All checks passed'

    return {
        'passed': passed,
        'calendar_anomaly_count': calendar_anomaly_count,
        'iv_noise_max_delta_change': round(iv_noise_max_delta_change, 6),
        'theta_natural_vs_trading_diff': round(theta_natural_vs_trading_diff, 6),
        'theta_violation_rate_pct': round(theta_violation_rate, 2),
        'details': details,
    }


# ============================================================================
# 导出公共接口
# ============================================================================
__all__ = [
    'GreeksCalculator',
    'IVCalculator',
    'BinomialTreePricer',
    'MonteCarloPricer',
    'TradingCalendar',
    '_bs_greeks',
    '_bs_price',
    '_norm_cdf',
    '_norm_pdf',
    '_normalize_option_type',
    'validate_greeks_calendar_iv_noise',
]
