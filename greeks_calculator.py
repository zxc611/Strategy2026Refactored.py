"""
希腊字母计算器模块 - 独立的期权风险管理工具

功能:
- Black-Scholes希腊字母计算 (Delta, Gamma, Theta, Vega)
- 隐含波动率(IV)计算 (牛顿迭代法)
- 二叉树定价模型 (CRR, 支持欧式/美式期权)
- 蒙特卡洛定价模型 (GBM模拟, 对偶变量方差缩减)
- 智能更新策略 (时间+价格变化+IV变化)
- 组合风险汇总 (支持持仓加权)
- 配置文件加载
- 交易日历支持

使用示例:
    calculator = GreeksCalculator(config_path='greeks_config.json')

    # Black-Scholes (适用于欧式期权)
    greeks = calculator.update_greeks_from_tick(
        instrument_id='IF2603-C-4500',
        price=150.0, underlying_price=4500.0,
        expiry_date=date(2026, 3, 20), option_type='CALL'
    )
    portfolio_risk = calculator.get_portfolio_greeks(positions)

    # 二叉树定价 (适用于美式期权)
    bt_price = BinomialTreePricer.price(
        S=4500, K=4500, T=0.25, r=0.02, q=0.0,
        sigma=0.2, option_type='CALL', steps=200, american=True
    )

    # 蒙特卡洛定价 (适用于路径依赖期权)
    mc_price, mc_std = MonteCarloPricer.price(
        S=4500, K=4500, T=0.25, r=0.02, q=0.0,
        sigma=0.2, option_type='CALL', n_simulations=50000
    )
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

# ----------------------------------------------------------------------------
# 配置日志
# ----------------------------------------------------------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# ----------------------------------------------------------------------------
# 辅助数学函数
# ----------------------------------------------------------------------------
def _norm_cdf(x: float) -> float:
    """标准正态分布累积分布函数 (精度 ~1e-5)"""
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

def _norm_pdf(x: float) -> float:
    """标准正态分布概率密度函数"""
    return math.exp(-x * x / 2.0) / math.sqrt(2.0 * math.pi)

def _normalize_option_type(opt_type: str) -> str:
    """✅ ID唯一：委托shared_utils.normalize_option_type，不再独立实现"""
    from ali2026v3_trading.shared_utils import normalize_option_type
    return normalize_option_type(opt_type)

def _bs_price(S: float, K: float, T: float, r: float, q: float, sigma: float, option_type: str) -> float:
    """Black-Scholes 期权价格 (用于 IV 计算)
    
    注意：option_type 应在调用前已标准化，避免重复标准化
    """
    if T <= 0 or sigma <= 0:
        # option_type 已在入口标准化，直接使用
        return max(0.0, (S - K) if option_type == 'CALL' else (K - S))
    
    try:
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        
        # option_type 已在入口标准化，直接使用
        if option_type == 'CALL':
            return S * math.exp(-q * T) * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
        else:
            return K * math.exp(-r * T) * _norm_cdf(-d2) - S * math.exp(-q * T) * _norm_cdf(-d1)
    except Exception as e:
        logger.error(f"BS price calculation error: {e}")
        return 0.0

def _bs_greeks(
    S: float, K: float, T: float, r: float, q: float, sigma: float, option_type: str
) -> Dict[str, float]:
    """
    计算期权希腊字母
    
    Args:
        S: 标的价格
        K: 行权价
        T: 剩余时间 (年)
        r: 无风险利率
        q: 股息率 (连续复利)
        sigma: 隐含波动率
        option_type: 'CALL' 或 'PUT' (应在调用前已标准化)
    
    Returns:
        Dict: {'delta', 'gamma', 'theta', 'vega'}
              - delta: 方向性风险
              - gamma: Delta变化速度
              - theta: 每日时间衰减
              - vega: 每1% IV变化的影响
    
    注意：option_type 应在调用前已标准化，避免重复标准化
    """
    if T <= 0 or sigma <= 0 or S <= 0:
        return {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}
    
    try:
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)

        # option_type 已在入口标准化，直接使用
        if option_type == 'CALL':
            delta = math.exp(-q * T) * _norm_cdf(d1)
            gamma = math.exp(-q * T) * _norm_pdf(d1) / (S * sigma * math.sqrt(T))
            theta = (-math.exp(-q * T) * S * _norm_pdf(d1) * sigma / (2 * math.sqrt(T))
                     - r * K * math.exp(-r * T) * _norm_cdf(d2)
                     + q * S * math.exp(-q * T) * _norm_cdf(d1)) / 365.0
        else:  # PUT
            delta = math.exp(-q * T) * (_norm_cdf(d1) - 1.0)
            gamma = math.exp(-q * T) * _norm_pdf(d1) / (S * sigma * math.sqrt(T))
            theta = (-math.exp(-q * T) * S * _norm_pdf(d1) * sigma / (2 * math.sqrt(T))
                     + r * K * math.exp(-r * T) * _norm_cdf(-d2)
                     - q * S * math.exp(-q * T) * _norm_cdf(-d1)) / 365.0
        
        vega = S * math.exp(-q * T) * _norm_pdf(d1) * math.sqrt(T) / 100.0  # 每1% IV变化
        
        return {
            'delta': round(delta, 6),
            'gamma': round(gamma, 4),
            'theta': round(theta, 6),
            'vega': round(vega, 4)
        }
    except Exception as e:
        logger.error(f"BS Greeks calculation error: S={S}, K={K}, T={T}, sigma={sigma}, error={e}")
        return {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}


# ----------------------------------------------------------------------------
# IV 计算器 (牛顿迭代法)
# ----------------------------------------------------------------------------
class IVCalculator:
    """隐含波动率计算器，使用牛顿迭代法"""

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
        计算隐含波动率
        
        Args:
            market_price: 市场期权价格
            S: 标的价格
            K: 行权价
            T: 剩余时间 (年)
            r: 无风险利率
            q: 股息率
            option_type: 'CALL' 或 'PUT'
            initial_guess: 初始猜测值 (默认0.2)
            max_iter: 最大迭代次数
            tol: 收敛容差
        
        Returns:
            float: 隐含波动率，如果失败返回0.2
        """
        if market_price <= 0 or S <= 0 or K <= 0 or T <= 0:
            return 0.2

        sigma = initial_guess
        i = 0  # ✅ P2#121修复：初始化i，防止max_iter=0时i未定义
        for i in range(max_iter):
            # 计算当前 sigma 下的期权价格和 Vega
            price = _bs_price(S, K, T, r, q, sigma, option_type)
            greeks = _bs_greeks(S, K, T, r, q, sigma, option_type)
            vega = greeks.get('vega', 0.0) * 100  # vega是每1%变化，转换为每单位变化

            if vega < 1e-10:
                logger.warning(f"Vega too small ({vega:.2e}), IV calculation cannot converge")
                # ✅ P1#77修复：vega接近0时返回初始猜测值，而非未收敛的sigma
                return initial_guess

            diff = price - market_price
            if abs(diff) < tol:
                return sigma

            # 牛顿迭代：sigma_new = sigma - diff / vega
            sigma = sigma - diff / vega
            
            # 限制波动率范围 [0.01, 5.0]
            sigma = max(0.01, min(5.0, sigma))

        logger.debug(f"IV calculation converged to {sigma:.4f} after {i+1} iterations")
        return sigma


# ----------------------------------------------------------------------------
# 二叉树定价模型 (CRR - Cox-Ross-Rubinstein)
# ----------------------------------------------------------------------------
class BinomialTreePricer:
    """二叉树期权定价模型 (CRR参数化)

    支持欧式和美式期权，通过反向归纳计算期权价格。

    CRR参数化:
        u = exp(sigma * sqrt(dt))
        d = 1 / u
        p = (exp((r - q) * dt) - d) / (u - d)

    特点:
    - 美式期权可提前行权（每步比较内在价值）
    - 步数越多精度越高，建议>=100步
    - 时间复杂度 O(steps²)，空间复杂度 O(steps)
    """

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

        # 确保概率在有效范围内
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
        h = S * 0.001
        price = BinomialTreePricer.price(S, K, T, r, q, sigma, option_type, steps, american)
        price_up = BinomialTreePricer.price(S + h, K, T, r, q, sigma, option_type, steps, american)
        price_down = BinomialTreePricer.price(S - h, K, T, r, q, sigma, option_type, steps, american)
        price_T = BinomialTreePricer.price(S, K, max(T - 1.0/365.0, 0.0), r, q, sigma, option_type, steps, american)
        price_vol_up = BinomialTreePricer.price(S, K, T, r, q, sigma + 0.01, option_type, steps, american)

        delta = (price_up - price_down) / (2.0 * h)
        gamma = (price_up - 2.0 * price + price_down) / (h * h)
        theta = (price_T - price) / (1.0 / 365.0)
        vega = (price_vol_up - price) / 100.0

        return {
            'price': round(price, 6),
            'delta': round(delta, 6),
            'gamma': round(gamma, 4),
            'theta': round(theta, 6),
            'vega': round(vega, 4)
        }


# ----------------------------------------------------------------------------
# 蒙特卡洛定价模型 (GBM模拟 + 对偶变量方差缩减)
# ----------------------------------------------------------------------------
class MonteCarloPricer:
    """蒙特卡洛期权定价模型

    使用几何布朗运动(GBM)模拟标的资产路径，通过大量随机抽样估计期权价格。
    支持对偶变量法(Antithetic Variates)进行方差缩减。

    GBM:
        S_T = S * exp((r - q - sigma²/2) * T + sigma * sqrt(T) * Z)
        其中 Z ~ N(0, 1)

    特点:
    - 适用于路径依赖期权（亚式、障碍等）
    - 对偶变量法减少一半方差
    - 时间复杂度 O(n_simulations * n_steps)
    - 标准误差随 1/sqrt(n_simulations) 收敛
    """

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
        seed: Optional[int] = None
    ) -> Tuple[float, float]:
        if T <= 0 or sigma <= 0 or S <= 0:
            intrinsic = max(0.0, (S - K) if option_type == 'CALL' else (K - S))
            return intrinsic, 0.0

        if seed is not None:
            random.seed(seed)

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
            mean = sum(payoffs_arr) / len(payoffs_arr)
            variance = sum((p - mean) ** 2 for p in payoffs_arr) / (len(payoffs_arr) - 1)
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

        mean = sum(payoffs) / n_simulations
        variance = sum((p - mean) ** 2 for p in payoffs) / (n_simulations - 1)
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
        seed: Optional[int] = None
    ) -> Dict[str, float]:
        mean, std_error = MonteCarloPricer.price(
            S, K, T, r, q, sigma, option_type,
            n_simulations, n_steps, antithetic, seed
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


# ----------------------------------------------------------------------------
# 交易日历辅助类 (简化版，支持工作日计算)
# ----------------------------------------------------------------------------
class TradingCalendar:
    """交易日历，用于精确计算剩余交易日"""

    def __init__(self, holidays: List[date] = None, market_time_service=None):
        """
        ✅ P2修复：通过构造函数注入MarketTimeService，避免延迟导入
        
        Args:
            holidays: 节假日列表
            market_time_service: MarketTimeService实例（可选）
        """
        self.holidays = set(holidays) if holidays else set()
        self._market_time_service = market_time_service

    def add_holiday(self, d: date):
        """添加节假日"""
        self.holidays.add(d)

    # ✅ P2修复：使用注入的MarketTimeService，避免延迟导入
    def is_trading_day(self, d: date) -> bool:
        """判断是否为交易日"""
        if self._market_time_service is not None:
            return self._market_time_service.is_trading_day(d, holiday_dates=self.holidays)
        
        # 降级方案：如果没有注入service，则仅检查节假日
        return d.weekday() < 5 and d not in self.holidays

    def trading_days_between(self, start: date, end: date) -> int:
        """返回两个日期之间的交易日天数"""
        if start >= end:
            return 0
        
        days = 0
        current = start
        while current < end:
            if self.is_trading_day(current):
                days += 1
            current += timedelta(days=1)
        return days

    def time_to_expiry(self, expiry_date: date, use_trading_days: bool = False) -> float:
        """
        返回剩余年化时间 (年)
        
        Args:
            expiry_date: 到期日
            use_trading_days: 是否使用交易日计算
        
        Returns:
            float: 剩余时间 (年)
        """
        today = datetime.now().date()
        if expiry_date <= today:
            return 1.0 / (252.0 * 240.0)
        
        if use_trading_days:
            days = self.trading_days_between(today, expiry_date)
            return max(1.0 / (252.0 * 24.0), days / 252.0)
        else:
            days = (expiry_date - today).days
            return max(1.0 / (252.0 * 24.0), days / 365.0)


# ----------------------------------------------------------------------------
# 增强版 GreeksCalculator
# ----------------------------------------------------------------------------
class GreeksCalculator:
    """
    独立的希腊字母计算器，支持：
    - 隐含波动率计算 (牛顿迭代)
    - 基于时间、价格变化、IV变化的混合更新策略
    - 持仓级别的组合风险汇总
    - 配置文件加载参数
    - 完整的异常处理和日志
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Args:
            config_path: JSON 配置文件路径，用于加载利率、乘数、股息率、更新策略参数等
        """
        self._lock = threading.RLock()

        # 默认配置
        self._risk_free_rate = 0.02
        self._update_strategy = {
            'time_interval_sec': 1.0,      # 时间间隔超过1秒则更新
            'price_change_pct': 0.5,       # 价格变化超过0.5%则更新
            'iv_change_pct': 5.0,          # IV变化超过5%则更新
            'min_tick_interval': 5,        # 最小tick间隔（兼容旧逻辑）
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
        self._option_tick_counter: Dict[str, int] = {}           # tick计数（备用）
        
        # 期权基本信息缓存 (避免重复传入)
        self._option_info_cache: Dict[str, Dict[str, Any]] = {}

        # 性能统计
        self._update_count: int = 0
        self._total_compute_time_ms: float = 0.0
        self._iv_calc_count: int = 0
        self._total_iv_compute_time_ms: float = 0.0

        # 预设默认乘数和股息率（可被配置文件覆盖）
        self._set_defaults()

        logger.info("GreeksCalculator initialized with strategy: %s", self._update_strategy)

    def __repr__(self) -> str:
        """调试用字符串表示"""
        return (
            f"GreeksCalculator(cached_options={len(self._option_greeks)}, "
            f"updates={self._update_count}, iv_calcs={self._iv_calc_count})"
        )

    def __str__(self) -> str:
        """友好字符串表示"""
        stats = self.get_stats()
        return (
            f"GreeksCalculator:\n"
            f"  缓存期权数: {stats['cached_options']}\n"
            f"  更新次数: {stats['greeks_update_count']}\n"
            f"  IV计算次数: {stats['iv_calc_count']}\n"
            f"  平均Greeks计算耗时: {stats['avg_greeks_compute_time_ms']:.4f}ms\n"
            f"  平均IV计算耗时: {stats['avg_iv_compute_time_ms']:.4f}ms"
        )

    def _set_defaults(self):
        """设置默认的乘数和股息率"""
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
        """从 JSON 文件加载配置"""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
            
            self._risk_free_rate = cfg.get('risk_free_rate', self._risk_free_rate)
            self._update_strategy.update(cfg.get('update_strategy', {}))
            self._use_trading_days = cfg.get('use_trading_days', self._use_trading_days)

            # 加载乘数和股息率
            if 'contract_multiplier' in cfg:
                self._contract_multiplier.update(cfg['contract_multiplier'])
            if 'dividend_yield' in cfg:
                self._dividend_yield.update(cfg['dividend_yield'])

            # 加载节假日（可选）
            if 'holidays' in cfg:
                for h in cfg['holidays']:
                    try:
                        self._calendar.add_holiday(date.fromisoformat(h))
                    except Exception as e:
                        logger.warning(f"[TradingCalendar] Failed to parse holiday '{h}': {e}")

            logger.info("Loaded config from %s", path)
        except Exception as e:
            logger.error(f"Failed to load config from {path}: {e}")

    # ------------------------------------------------------------------------
    # 公共配置接口
    # ------------------------------------------------------------------------
    def set_contract_multiplier(self, product: str, multiplier: int):
        """设置合约乘数"""
        with self._lock:
            self._contract_multiplier[product] = multiplier

    def set_dividend_yield(self, product: str, yield_rate: float):
        """设置股息率"""
        with self._lock:
            self._dividend_yield[product] = yield_rate

    def set_risk_free_rate(self, rate: float):
        """设置无风险利率"""
        with self._lock:
            self._risk_free_rate = rate

    def set_update_strategy(self, **kwargs):
        """
        设置更新策略参数
        
        Args:
            time_interval_sec: 时间间隔 (秒)
            price_change_pct: 价格变化百分比
            iv_change_pct: IV变化百分比
            min_tick_interval: 最小tick间隔
        """
        with self._lock:
            self._update_strategy.update(kwargs)
            logger.info(f"Update strategy changed: {kwargs}")

    def set_trading_calendar(self, calendar: TradingCalendar):
        """设置交易日历"""
        with self._lock:
            self._calendar = calendar

    def use_trading_days(self, flag: bool):
        """启用/禁用交易日计算"""
        with self._lock:
            self._use_trading_days = flag

    def cache_option_info(self, instrument_id: str, info: Dict[str, Any]):
        """
        缓存期权基本信息 (避免每次tick都传入)
        
        Args:
            instrument_id: 期权合约ID
            info: {'strike', 'option_type', 'expiry_date', 'future_product', ...}
        """
        with self._lock:
            self._option_info_cache[instrument_id] = info

    # ------------------------------------------------------------------------
    # 核心计算与更新
    # ------------------------------------------------------------------------
    def _time_to_expiry(self, expiry_date: Optional[date]) -> float:
        """计算剩余年化时间，支持交易日历
        
        Note: 此方法在持有锁的情况下被调用，因此内部不再加锁。
        TradingCalendar.time_to_expiry 使用 datetime.now() 是线程安全的。
        """
        if expiry_date is None:
            return 30.0 / 365.0
        
        return self._calendar.time_to_expiry(expiry_date, self._use_trading_days)

    def _parse_strike_from_id(self, instrument_id: str) -> Optional[float]:
        """
        从合约ID解析行权价（降级方案）
        
        ✅ 方法唯一修复：委托SubscriptionManager.parse_option提取strike_price，不再内联正则
        """
        try:
            from ali2026v3_trading.subscription_manager import SubscriptionManager
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
        ✅ P1修复：消除TOCTOU竞态，所有参数由调用方在锁内传入
        
        混合策略：判断是否需要更新希腊字母
        
        条件 (满足任一即更新):
        1. 时间间隔超过阈值
        2. 价格变化超过阈值
        3. IV变化超过阈值 (如果提供current_iv)
        4. Tick计数达到最小间隔
        
        Args:
            instrument_id: 合约ID
            current_price: 当前价格
            current_iv: 当前IV (可选，如果不提供则跳过IV检查)
            last_time: 上次更新时间（调用方在锁内读取）
            last_price: 上次价格（调用方在锁内读取）
            last_iv: 上次IV（调用方在锁内读取）
            tick_count: Tick计数（调用方在锁内读取）
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
        """✅ P1修复：委托_should_update并传入缓存IV，所有参数由调用方在锁内读取。"""
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
        iv: Optional[float] = None
    ) -> Dict[str, float]:
        """
        计算并更新希腊字母 (完整流程)
        
        Args:
            instrument_id: 期权合约ID
            underlying_price: 标的价格
            strike: 行权价
            expiry_date: 到期日
            option_type: 'CALL' 或 'PUT'
            future_product: 期货产品代码 (用于获取乘数和股息率)
            market_price: 期权市场价格 (用于计算IV，可选)
            iv: 隐含波动率 (如果提供则直接使用，否则从market_price计算)
        
        Returns:
            Dict: 希腊字母 {'delta', 'gamma', 'theta', 'vega'}
        """
        with self._lock:
            # 1. 计算剩余时间
            T = self._time_to_expiry(expiry_date)
            
            # 2. 获取股息率
            q = self._dividend_yield.get(future_product, 0.0)
            
            # 3. 计算或获取IV
            if iv is None or iv <= 0:
                if market_price and market_price > 0:
                    # 计算IV
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
                    # 使用缓存的IV或默认值
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
        iv: Optional[float] = None
    ) -> Optional[Dict[str, float]]:
        """
        根据期权tick数据更新希腊字母（智能决定是否重新计算）
        
        注意：此方法不是tick处理器，而是希腊字母计算引擎。
        应在 strategy_tick_handler.on_tick() 中提取参数后调用。
        
        优化：先判断是否需要更新，再执行昂贵的IV计算
        
        Args:
            instrument_id: 期权合约ID
            price: 期权最新价
            underlying_price: 标的价格
            expiry_date: 到期日
            option_type: 'CALL' 或 'PUT'
            future_product: 期货产品代码
            strike: 行权价 (可选，优先从缓存读取)
            iv: 隐含波动率 (可选，如不提供则从price计算)
        
        Returns:
            Optional[Dict]: 如果更新了则返回希腊字母，否则返回None
        """
        # ✅ P1#76修复：将IV计算移到锁外，减少持锁时间
        # 第一步：锁内做低成本判断，决定是否需要计算IV
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
            
            # ✅ P1修复：在锁内读取所有状态，消除TOCTOU竞态
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
        
        # 第二步：锁外执行昂贵的IV计算
        if need_compute_iv and iv is None:
            start_iv = time.perf_counter()
            iv = IVCalculator.implied_volatility(
                market_price=price,
                S=underlying_price,
                K=strike,
                T=self._time_to_expiry(expiry_date),
                r=self._risk_free_rate,
                q=self._dividend_yield.get(future_product, 0.0),
                option_type=option_type
            )
            elapsed_ms = (time.perf_counter() - start_iv) * 1000
            
            if elapsed_ms > 1.0:
                logger.warning(f"[IVCalc] Slow: {instrument_id} took {elapsed_ms:.2f}ms")
        
        # 第三步：锁内做最终判断和更新，计数器递增移到锁内
        with self._lock:
            if need_compute_iv and iv is not None:
                self._iv_calc_count += 1
                self._total_iv_compute_time_ms += elapsed_ms
            
            # ✅ P1修复：使用锁内读取的状态数据
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
                iv=iv
            )

    # ------------------------------------------------------------------------
    # 查询接口
    # ------------------------------------------------------------------------
    def get_greeks(self, instrument_id: str) -> Dict[str, float]:
        """获取单个合约的希腊字母"""
        with self._lock:
            return self._option_greeks.get(instrument_id, {}).copy()

    def get_iv(self, instrument_id: str) -> float:
        """获取单个合约的隐含波动率"""
        with self._lock:
            return self._option_iv.get(instrument_id, 0.0)

    def set_iv(self, instrument_id: str, iv: float):
        """手动设置IV (外部数据源提供)"""
        with self._lock:
            if iv > 0:
                self._option_iv[instrument_id] = iv
                logger.debug(f"[SetIV] {instrument_id} IV set to {iv:.4f}")

    def get_portfolio_greeks(self, positions: Dict[str, int]) -> Dict[str, float]:
        """
        计算组合希腊字母 (按持仓加权)
        
        Args:
            positions: {instrument_id: position_size}
                      正数=多头，负数=空头
        
        Returns:
            Dict: {'delta', 'gamma', 'theta', 'vega'} 组合总值
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
                
                # 获取合约乘数
                info = self._option_info_cache.get(inst_id, {})
                future_product = info.get('future_product', '')
                multiplier = self._contract_multiplier.get(future_product, 1)
                
                # 加权累加
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
        汇总指定产品的希腊字母总值
        
        Args:
            product: 产品代码 (如 'IF')
            months: 月份过滤 (如 ['2603', '2604'])
            option_type: 类型过滤 ('CALL'/'PUT')
            include_multiplier: 是否包含合约乘数
        
        Returns:
            Dict: {'delta', 'gamma', 'theta', 'vega'}
        """
        total = {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0}
        
        with self._lock:
            for inst_id, info in self._option_info_cache.items():
                # 过滤产品 - 统一使用 future_product
                future_prod = info.get('future_product', '')
                if future_prod != product:
                    continue
                
                # 过滤月份 - 从缓存中读取 month 字段
                if months:
                    inst_month = info.get('month', '')
                    if not inst_month:
                        try:
                            from ali2026v3_trading.subscription_manager import SubscriptionManager
                            parsed = SubscriptionManager.parse_option(inst_id)
                            inst_month = parsed.get('year_month', '')
                        except (ValueError, KeyError, TypeError):
                            continue
                    if not inst_month or inst_month not in months:
                        continue
                
                # 过滤类型
                if option_type:
                    info_type = _normalize_option_type(info.get('option_type', ''))
                    input_type = _normalize_option_type(option_type)
                    if info_type != input_type:
                        continue
                
                # 累加希腊字母
                g = self._option_greeks.get(inst_id, {})
                if not g:
                    continue
                
                mult = 1
                if include_multiplier:
                    mult = self._contract_multiplier.get(future_prod, 1)
                
                for key in total:
                    total[key] += g.get(key, 0.0) * mult
        
        return total

    def check_risk_limits(
        self,
        positions: Dict[str, int],
        delta_limit: Optional[float] = None,
        gamma_limit: Optional[float] = None,
        theta_min: Optional[float] = None,
        vega_limit: Optional[float] = None
    ) -> List[str]:
        """
        检查风险限额
        
        Args:
            positions: 持仓字典
            delta_limit: Delta限额 (默认1000)
            gamma_limit: Gamma限额 (默认50)
            theta_min: Theta最小值 (默认-500，负值表示时间衰减成本)
            vega_limit: Vega限额 (默认200)
        
        Returns:
            List[str]: 警告列表
        
        Note:
            所有限额参数均为绝对值，用户需根据策略风险偏好明确设置。
            不建议使用动态计算，因为不同策略的风险承受能力差异巨大。
        """
        portfolio = self.get_portfolio_greeks(positions)
        
        # 使用明确的默认值（不再动态计算）
        if delta_limit is None:
            delta_limit = 1000
        if gamma_limit is None:
            gamma_limit = 50
        if theta_min is None:
            theta_min = -500
        if vega_limit is None:
            vega_limit = 200
        
        warnings = []
        
        if abs(portfolio['delta']) > delta_limit:
            warnings.append(
                f"[WARN] Delta超限: {portfolio['delta']:.2f} / ±{delta_limit}"
            )
        
        if portfolio['gamma'] > gamma_limit:
            warnings.append(
                f"[WARN] Gamma超限: {portfolio['gamma']:.4f} / {gamma_limit:.2f}"
            )
        
        if portfolio['theta'] < theta_min:
            warnings.append(
                f"[WARN] Theta成本过高: {portfolio['theta']:.2f}/天 (限额: {theta_min})"
            )
        
        if abs(portfolio['vega']) > vega_limit:
            warnings.append(
                f"[WARN] Vega超限: {portfolio['vega']:.2f} / ±{vega_limit:.2f}"
            )
        
        if warnings:
            logger.warning(f"[RiskAlert] {'; '.join(warnings)}")
        
        return warnings

    # ✅ ID唯一：get_stats统一接口，返回值含service_name="GreeksCalculator"
    def get_stats(self) -> Dict[str, Any]:
        """获取性能统计信息"""
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
                'service_name': 'GreeksCalculator',  # ✅ ID唯一：统一标识服务来源
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

    # ✅ ID唯一：clear_cache统一接口，服务=GreeksCalculator
    def clear_cache(self):
        """清空所有缓存"""
        with self._lock:
            self._option_greeks.clear()
            self._option_iv.clear()
            self._option_last_update.clear()
            self._option_last_price.clear()
            self._option_last_iv.clear()
            self._option_tick_counter.clear()
            logger.info("[GreeksCalculator] Cache cleared")


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
]
