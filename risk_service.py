"""
risk_service.py - 风控服务

合并来源：08_position.py (PositionManagerMixin) + 10_gate.py (GateLayer3)
合并策略：提取风控检查、限额管理、风险计算等功能

重构目标：
- 旧架构：08_position.py (~600行) + 10_gate.py (~200行风控相关) = ~700行
- 新架构：risk_service.py (~400行)
- 减少：~43%

核心改进：
1. 单一职责：专注于风险控制与限额管理
2. 统一风控接口：整合持仓限额、信号限频、策略状态检查
3. 线程安全：支持并发检查
4. 可配置性：灵活的风控参数配置

作者：CodeArts 代码智能体
版本：v1.0
生成时间：2026-03-16
"""

from __future__ import annotations

import copy
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
from enum import Enum, auto


# ============================================================================
# 枚举与数据结构
# ============================================================================

class RiskLevel(Enum):
    """风险等级"""
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    CRITICAL = auto()


class RiskCheckResult(Enum):
    """风控检查结果"""
    PASS = auto()       # 通过
    BLOCK = auto()      # 阻断
    WARNING = auto()    # 警告（可继续）
    DEGRADED = auto()   # 降级


@dataclass
class RiskCheckResponse:
    """风控检查响应"""
    result: RiskCheckResult = RiskCheckResult.PASS
    level: RiskLevel = RiskLevel.LOW
    message: str = ""
    reason: str = ""
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def is_pass(self) -> bool:
        return self.result == RiskCheckResult.PASS

    @property
    def is_block(self) -> bool:
        return self.result == RiskCheckResult.BLOCK

    @classmethod
    def pass_result(cls, message: str = "") -> "RiskCheckResponse":
        return cls(result=RiskCheckResult.PASS, message=message)

    @classmethod
    def block_result(cls, reason: str, message: str = "",
                     level: RiskLevel = RiskLevel.HIGH) -> "RiskCheckResponse":
        return cls(result=RiskCheckResult.BLOCK, level=level,
                   reason=reason, message=message)

    @classmethod
    def warning_result(cls, reason: str, message: str = "") -> "RiskCheckResponse":
        return cls(result=RiskCheckResult.WARNING, level=RiskLevel.MEDIUM,
                   reason=reason, message=message)


@dataclass
class PositionLimit:
    """持仓限额配置"""
    account_id: str = ""
    limit_amount: float = 0.0
    effective_until: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.now)

    def is_valid(self) -> bool:
        """检查限额是否有效"""
        if self.effective_until and datetime.now() > self.effective_until:
            return False
        return self.limit_amount > 0


@dataclass
class RiskMetrics:
    """风险指标"""
    total_exposure: float = 0.0       # 总敞口
    max_single_position: float = 0.0  # 最大单持仓
    position_count: int = 0           # 持仓数量
    risk_ratio: float = 0.0           # 风险比率
    timestamp: datetime = field(default_factory=datetime.now)


# ============================================================================
# 风控服务
# ============================================================================

class RiskService:
    """
    风控服务 - 统一的风险控制接口

    职责：
    1. 持仓限额检查
    2. 信号限频控制
    3. 策略状态检查
    4. 风险指标计算

    使用方式：
        service = RiskService(params, position_manager)
        result = service.check_before_trade(signal)
    """

    def __init__(self, params: Any, position_manager: Any = None,
                 strategy: Any = None):
        """
        初始化风控服务

        Args:
            params: 参数配置对象
            position_manager: 持仓管理器（可选）
            strategy: 策略实例（可选）
        """
        self.params = params
        self.position_manager = position_manager
        self.strategy = strategy

        # 线程安全
        self._lock = threading.RLock()

        # 持仓限额配置
        self._position_limits: Dict[str, PositionLimit] = {}

        # 限频控制
        self._signal_times: Dict[str, deque] = {}  # 注：插入时统一使用maxlen=1000
        # ✅ 修复：删除_last_signal_time，统一为_signal_times滑动窗口

        # 风控统计
        self._stats = {
            "total_checks": 0,
            "blocked": 0,
            "warnings": 0,
            "passed": 0,
        }

        # 缓存
        self._strategy_status_cache: Tuple[float, RiskCheckResponse] = (0.0, RiskCheckResponse.pass_result())
        self._cache_ttl = 1.0  # 缓存有效期（秒）
        self._last_greeks_dashboard: Dict[str, Any] = {}

    # ========================================================================
    # 主检查接口
    # ========================================================================

    def check_before_trade(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        try:
            safety_result = self._check_safety_meta_layer(signal)
            if safety_result.is_block:
                return self._record_result(safety_result)

            with self._lock:
                status_result = self._check_strategy_status()
                if status_result.is_block:
                    return self._record_result(status_result)

                if not signal.get("is_valid", True):
                    return self._record_result(
                        RiskCheckResponse.block_result("signal_invalid", "信号无效")
                    )

                symbol = signal.get("symbol", "")
                rate_result = self._check_rate_limit(symbol)
                if rate_result.is_block:
                    return self._record_result(rate_result)

                account_id = signal.get("account_id", "default")
                amount = signal.get("amount", 0)
                limit_result = self._check_position_limit(account_id, amount)
                if limit_result.is_block:
                    return self._record_result(limit_result)

                risk_result = self._check_risk_ratio(signal)
                if risk_result.is_block:
                    return self._record_result(risk_result)

                greeks_result = self._check_greeks_limits(signal)
                if greeks_result.is_block:
                    return self._record_result(greeks_result)

                self._record_signal_time(symbol)

            return self._record_result(RiskCheckResponse.pass_result("风控检查通过"))

        except Exception as e:
            logging.error(f"[RiskService] 风控检查异常，采用fail-safe阻断: {e}")
            return RiskCheckResponse.block_result("check_error", f"风控检查异常，采用fail-safe阻断: {e}", RiskLevel.CRITICAL)

    def _check_safety_meta_layer(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        try:
            safety = get_safety_meta_layer(self.params)

            paused, reason = safety.is_trading_paused()
            if paused:
                return RiskCheckResponse.block_result(
                    "safety_circuit_breaker", reason, RiskLevel.CRITICAL
                )

            if safety.is_hard_stop_triggered():
                return RiskCheckResponse.block_result(
                    "safety_daily_hard_stop",
                    "日回撤会话周期硬终止：所有交易禁止（含平仓），需人工confirm_daily_resume()恢复",
                    RiskLevel.CRITICAL
                )

            action = signal.get("action", "")
            if action != "CLOSE" and safety.is_new_open_blocked():
                return RiskCheckResponse.block_result(
                    "safety_daily_drawdown",
                    "日最大回撤硬停止：仅允许平仓，禁止新开仓",
                    RiskLevel.CRITICAL
                )

        except Exception as e:
            logging.warning(f"[RiskService._check_safety_meta_layer] SafetyMetaLayer check error: {e}")

        return RiskCheckResponse.pass_result()

    def _record_result(self, result: RiskCheckResponse) -> RiskCheckResponse:
        """记录检查结果"""
        with self._lock:
            self._stats["total_checks"] += 1
            if result.is_block:
                self._stats["blocked"] += 1
            elif result.result == RiskCheckResult.WARNING:
                self._stats["warnings"] += 1
            else:
                self._stats["passed"] += 1
        return result

    # ========================================================================
    # 策略状态检查
    # ========================================================================

    def _check_strategy_status(self) -> RiskCheckResponse:
        """检查策略运行状态"""
        if self.strategy is None:
            return RiskCheckResponse.pass_result()

        now = time.time()

        with self._lock:
            # 检查缓存
            ts, cached = self._strategy_status_cache
            if now - ts < self._cache_ttl:
                return cached

            # 检查策略状态
            if not getattr(self.strategy, "my_is_running", True):
                result = RiskCheckResponse.block_result(
                    "strategy_stopped", "策略未运行", RiskLevel.HIGH
                )
            elif getattr(self.strategy, "my_is_paused", False):
                result = RiskCheckResponse.block_result(
                    "trading_paused", "策略已暂停", RiskLevel.MEDIUM
                )
            elif getattr(self.strategy, "my_destroyed", False):
                result = RiskCheckResponse.block_result(
                    "strategy_destroyed", "策略已销毁", RiskLevel.CRITICAL
                )
            elif getattr(self.strategy, "my_trading", True) is False:
                result = RiskCheckResponse.block_result(
                    "trading_disabled", "交易开关关闭", RiskLevel.MEDIUM
                )
            else:
                result = RiskCheckResponse.pass_result()

            # 缓存结果
            self._strategy_status_cache = (now, result)
            return result

    # ========================================================================
    # 限频检查
    # ========================================================================

    def _check_rate_limit(self, symbol: str) -> RiskCheckResponse:
        """检查信号限频"""
        cooldown = self._get_signal_cooldown()
        max_signals = self._get_max_signals_per_window()
        window_sec = self._get_rate_limit_window()

        with self._lock:
            now = time.time()

            # ✅ 修复：统一使用_signal_times滑动窗口检查，删除_last_signal_time
            if symbol not in self._signal_times:
                self._signal_times[symbol] = deque(maxlen=max_signals * 2)

            window = self._signal_times[symbol]
            cutoff = now - window_sec

            # 清理旧记录
            while window and window[0] < cutoff:
                window.popleft()

            # 冷却检查：基于滑动窗口最后一个信号时间
            if window:
                last_time = window[-1]
                if now - last_time < cooldown:
                    return RiskCheckResponse.block_result(
                        "rate_limit_cooldown",
                        f"{symbol} 信号冷却中，剩余 {cooldown - (now - last_time):.1f} 秒",
                        RiskLevel.MEDIUM
                    )

            if len(window) >= max_signals:
                return RiskCheckResponse.block_result(
                    "rate_limit_exceeded",
                    f"{symbol} 窗口内信号数超限: {len(window)}/{max_signals}",
                    RiskLevel.MEDIUM
                )

        return RiskCheckResponse.pass_result()

    def _record_signal_time(self, symbol: str) -> None:
        """记录信号时间 - 统一使用_signal_times滑动窗口"""
        with self._lock:
            now = time.time()
            # ✅ 修复：删除_last_signal_time更新，仅使用_signal_times
            if symbol not in self._signal_times:
                max_signals = self._get_max_signals_per_window()
                self._signal_times[symbol] = deque(maxlen=max_signals * 2)
            self._signal_times[symbol].append(now)

    # ========================================================================
    # 持仓限额检查
    # ========================================================================

    def _check_position_limit(self, account_id: str, required_amount: float) -> RiskCheckResponse:
        """检查持仓限额"""
        if required_amount <= 0:
            return RiskCheckResponse.pass_result()

        with self._lock:
            expired_keys = []
            for aid in list(self._position_limits.keys()):
                if not self._position_limits[aid].is_valid():
                    expired_keys.append(aid)
            for aid in expired_keys:
                self._position_limits.pop(aid, None)

            if account_id not in self._position_limits:
                return RiskCheckResponse.pass_result()

            limit = self._position_limits[account_id]

            current_exposure = 0.0
            if self.position_manager is not None:
                try:
                    import copy as _copy
                    positions_raw = getattr(self.position_manager, "positions", {})
                    positions = _copy.deepcopy(positions_raw) if positions_raw else {}
                    for inst_map in positions.values():
                        for pos in inst_map.values():
                            vol = getattr(pos, "volume", 0)
                            price = getattr(pos, "open_price", 0)
                            current_exposure += abs(vol) * price
                except Exception as e:
                    # ✅ P0修复：限额检查异常时返回WARNING而非静默跳过
                    logging.warning(f"[RiskService._check_position_limit] Failed to calculate exposure: {e}")
                    return RiskCheckResponse.block_result(
                        "position_calculation_error",
                        f"持仓计算异常: {e}",
                        RiskLevel.WARNING
                    )

            if limit is not None and limit.limit_amount < (current_exposure + required_amount):
                return RiskCheckResponse.block_result(
                    "position_limit_exceeded",
                    f"持仓限额不足: 当前{current_exposure:.2f}+需要{required_amount:.2f}={current_exposure + required_amount:.2f}, 限额 {limit.limit_amount:.2f}",
                    RiskLevel.HIGH
                )

        return RiskCheckResponse.pass_result()

    def set_position_limit(self, account_id: str, limit_amount: float,
                           effective_until: Optional[datetime] = None) -> None:
        """设置持仓限额 - 统一为PositionService入口，RiskService仅内部调用
        
        ✅ 修复标准：委托给PositionService.set_position_limit（唯一权威入口）
        """
        # ✅ 统一为PositionService入口
        if self.position_manager and hasattr(self.position_manager, 'set_position_limit'):
            self.position_manager.set_position_limit(account_id, limit_amount, effective_until)
        else:
            logging.warning("[RiskService.set_position_limit] PositionService not available")

    def get_position_limit(self, account_id: str) -> Optional[PositionLimit]:
        """获取持仓限额 - ✅ P1修复：统一数据源，委托给PositionService"""
        # ✅ 统一为PositionService入口
        if self.position_manager and hasattr(self.position_manager, 'get_position_limit'):
            return self.position_manager.get_position_limit(account_id)
        else:
            logging.warning("[RiskService.get_position_limit] PositionService not available, using fallback")
            with self._lock:
                limit = self._position_limits.get(account_id)
                if limit and limit.is_valid():
                    return limit
            return None

    # ========================================================================
    # 风险比率检查
    # ========================================================================

    def _check_risk_ratio(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """检查风险比率"""
        max_risk_ratio = self._get_max_risk_ratio()
        if max_risk_ratio <= 0:
            return RiskCheckResponse.pass_result()

        # 计算当前风险比率
        metrics = self.calculate_risk_metrics()
        if metrics.risk_ratio >= max_risk_ratio:
            return RiskCheckResponse.block_result(
                "risk_ratio_exceeded",
                f"风险比率超限: {metrics.risk_ratio:.2%} >= {max_risk_ratio:.2%}",
                RiskLevel.HIGH
            )

        return RiskCheckResponse.pass_result()

    _greeks_calc = None
    _greeks_calc_lock = threading.Lock()

    def _get_greeks_calculator(self):
        if RiskService._greeks_calc is None:
            with RiskService._greeks_calc_lock:
                if RiskService._greeks_calc is None:
                    try:
                        from ali2026v3_trading.greeks_calculator import GreeksCalculator
                        RiskService._greeks_calc = GreeksCalculator()
                    except Exception:
                        return None
        return RiskService._greeks_calc

    def get_greeks_dashboard(self) -> Dict[str, Any]:
        """V7核心工程：实时希腊字母风险仪表盘

        输出结构化数据，包含组合Greeks、跳空模拟、PnL归因、安全元层状态。
        每次 check_before_trade() 时更新仪表盘缓存。
        如 greeks_calculator 导入失败，仪表盘字段设为 null 并记录警告，不阻断交易。
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.") + f"{datetime.now().microsecond // 1000:03d}"

        portfolio_section: Dict[str, Any] = {}
        stress_section: Dict[str, Any] = {}
        attribution_section: Dict[str, Any] = {}
        safety_section: Dict[str, Any] = {}

        try:
            calc = self._get_greeks_calculator()
            if calc is None:
                portfolio_section = {k: None for k in (
                    "net_delta", "max_net_delta", "delta_usage_pct",
                    "net_gamma", "max_net_gamma", "gamma_usage_pct",
                    "net_vega", "max_net_vega", "vega_usage_pct",
                    "net_theta", "max_theta_ratio", "theta_ratio",
                )}
                logging.warning("[GreekDashboard] GreeksCalculator unavailable, fields set to null")
            else:
                positions_dict = {}
                if self.position_manager:
                    positions_raw = getattr(self.position_manager, "positions", {})
                    for inst_map in positions_raw.values():
                        for inst_id, pos in inst_map.items():
                            vol = getattr(pos, "volume", 0)
                            if vol != 0:
                                positions_dict[inst_id] = vol

                max_delta_pct = safe_get_float(self.params, "max_net_delta_pct", 0.30)
                max_gamma_pct = safe_get_float(self.params, "max_net_gamma_pct", 0.08)
                max_vega_bps = safe_get_float(self.params, "max_net_vega_bps", 0.02)
                max_theta_ratio = safe_get_float(self.params, "max_theta_ratio", 0.5)

                if positions_dict:
                    pg = calc.get_portfolio_greeks(positions_dict)

                    net_delta = pg.get("delta", 0.0)
                    net_gamma = pg.get("gamma", 0.0)
                    net_vega = pg.get("vega", 0.0)
                    net_theta = pg.get("theta", 0.0)

                    max_delta_abs = max_delta_pct * 1000
                    max_gamma_abs = max_gamma_pct * 50
                    max_vega_abs = max_vega_bps * 200

                    portfolio_section = {
                        "net_delta": round(net_delta, 4),
                        "max_net_delta": round(max_delta_abs, 2),
                        "delta_usage_pct": round(abs(net_delta) / max_delta_abs * 100, 2) if max_delta_abs > 0 else 0.0,
                        "net_gamma": round(net_gamma, 6),
                        "max_net_gamma": round(max_gamma_abs, 4),
                        "gamma_usage_pct": round(abs(net_gamma) / max_gamma_abs * 100, 2) if max_gamma_abs > 0 else 0.0,
                        "net_vega": round(net_vega, 6),
                        "max_net_vega": round(max_vega_abs, 4),
                        "vega_usage_pct": round(abs(net_vega) / max_vega_abs * 100, 2) if max_vega_abs > 0 else 0.0,
                        "net_theta": round(net_theta, 6),
                        "max_theta_ratio": max_theta_ratio,
                        "theta_ratio": round(abs(net_theta) / max_theta_ratio, 4) if max_theta_ratio > 0 else 0.0,
                    }

                    stress_section = self._compute_stress_test(calc, positions_dict, net_delta, net_gamma, net_vega, net_theta)
                    attribution_section = self._compute_pnl_attribution(calc, positions_dict, net_delta, net_gamma, net_vega, net_theta)
                else:
                    portfolio_section = {k: 0.0 for k in (
                        "net_delta", "max_net_delta", "delta_usage_pct",
                        "net_gamma", "max_net_gamma", "gamma_usage_pct",
                        "net_vega", "max_net_vega", "vega_usage_pct",
                        "net_theta", "max_theta_ratio", "theta_ratio",
                    )}
                    stress_section = {"jump_1sigma_pnl": 0.0, "jump_2sigma_pnl": 0.0, "L1_trigger_prob": 0.0}
                    attribution_section = {
                        "delta_contrib": 0.0, "gamma_contrib": 0.0,
                        "vega_contrib": 0.0, "theta_contrib": 0.0,
                        "unexplained": 0.0, "unexplained_pct": 0.0,
                    }
        except Exception as e:
            logging.warning("[GreekDashboard] Computation error: %s", e)
            portfolio_section = portfolio_section or {}
            stress_section = stress_section or {}
            attribution_section = attribution_section or {}

        try:
            safety = get_safety_meta_layer(self.params)
            paused, _ = safety.is_trading_paused()
            safety_section = {
                "circuit_breaker_triggered": paused,
                "trading_paused": paused,
                "new_open_blocked": safety.is_new_open_blocked(),
            }
        except Exception:
            safety_section = {"circuit_breaker_triggered": False, "trading_paused": False, "new_open_blocked": False}

        dashboard = {
            "timestamp": now_str,
            "portfolio": portfolio_section,
            "stress_test": stress_section,
            "pnl_attribution": attribution_section,
            "safety_meta": safety_section,
        }

        with self._lock:
            self._last_greeks_dashboard = dashboard

        return dashboard

    def _compute_stress_test(self, calc, positions_dict, net_delta, net_gamma, net_vega, net_theta):
        """跳空模拟：1σ和2σ标的价格跳变下的PnL影响"""
        try:
            jump_1sigma_pnl = 0.0
            jump_2sigma_pnl = 0.0

            with calc._lock:
                for inst_id, size in positions_dict.items():
                    if size == 0:
                        continue
                    greeks = calc._option_greeks.get(inst_id)
                    if not greeks:
                        continue
                    info = calc._option_info_cache.get(inst_id, {})
                    future_product = info.get('future_product', '')
                    multiplier = calc._contract_multiplier.get(future_product, 1)

                    underlying_price = 0.0
                    iv = calc._option_iv.get(inst_id, 0.2)
                    if iv <= 0:
                        iv = 0.2
                    try:
                        from ali2026v3_trading.data_service import get_data_service
                        ds = get_data_service()
                        if ds and ds.realtime_cache:
                            underlying_id = info.get('underlying_future_id')
                            if underlying_id:
                                underlying_price = ds.realtime_cache.get_latest_price(str(underlying_id)) or 0.0
                    except Exception:
                        pass

                    if underlying_price <= 0:
                        continue

                    sigma_1 = underlying_price * iv * 0.01
                    sigma_2 = sigma_1 * 2

                    delta_contrib = greeks.get('delta', 0.0) * multiplier * size
                    gamma_contrib = 0.5 * greeks.get('gamma', 0.0) * multiplier * size

                    jump_1sigma_pnl += delta_contrib * sigma_1 + gamma_contrib * sigma_1 * sigma_1
                    jump_2sigma_pnl += delta_contrib * sigma_2 + gamma_contrib * sigma_2 * sigma_2

            l1_prob = 0.05 if abs(jump_2sigma_pnl) > abs(jump_1sigma_pnl) * 3 else 0.01

            return {
                "jump_1sigma_pnl": round(jump_1sigma_pnl, 2),
                "jump_2sigma_pnl": round(jump_2sigma_pnl, 2),
                "L1_trigger_prob": l1_prob,
            }
        except Exception as e:
            logging.warning("[GreekDashboard._compute_stress_test] Error: %s", e)
            return {"jump_1sigma_pnl": 0.0, "jump_2sigma_pnl": 0.0, "L1_trigger_prob": 0.0}

    def _compute_pnl_attribution(self, calc, positions_dict, net_delta, net_gamma, net_vega, net_theta):
        """PnL归因：按Delta/Gamma/Vega/Theta分解收益贡献"""
        try:
            delta_contrib = 0.0
            gamma_contrib = 0.0
            vega_contrib = 0.0
            theta_contrib = 0.0

            with calc._lock:
                for inst_id, size in positions_dict.items():
                    if size == 0:
                        continue
                    greeks = calc._option_greeks.get(inst_id)
                    if not greeks:
                        continue
                    info = calc._option_info_cache.get(inst_id, {})
                    future_product = info.get('future_product', '')
                    multiplier = calc._contract_multiplier.get(future_product, 1)

                    iv = calc._option_iv.get(inst_id, 0.2)
                    if iv <= 0:
                        iv = 0.2

                    delta_contrib += greeks.get('delta', 0.0) * multiplier * size * 0.01
                    gamma_contrib += 0.5 * greeks.get('gamma', 0.0) * multiplier * size * 0.0001
                    vega_contrib += greeks.get('vega', 0.0) * multiplier * size * (iv * 0.01)
                    theta_contrib += greeks.get('theta', 0.0) * multiplier * size

            total_explained = delta_contrib + gamma_contrib + vega_contrib + theta_contrib
            total_pnl = abs(net_delta * 0.01) + abs(net_gamma * 0.0001) + abs(net_vega * 0.01) + abs(net_theta)
            unexplained = total_pnl - abs(total_explained) if total_pnl > 0 else 0.0
            unexplained_pct = (unexplained / total_pnl * 100) if total_pnl > 0 else 0.0

            if unexplained_pct > 15.0:
                logging.warning("[E7_PNL_RESIDUAL] PnL归因残差%.1f%%>15%%阈值，触发E7告警", unexplained_pct)

            return {
                "delta_contrib": round(delta_contrib, 2),
                "gamma_contrib": round(gamma_contrib, 2),
                "vega_contrib": round(vega_contrib, 2),
                "theta_contrib": round(theta_contrib, 2),
                "unexplained": round(unexplained, 2),
                "unexplained_pct": round(unexplained_pct, 2),
            }
        except Exception as e:
            logging.warning("[GreekDashboard._compute_pnl_attribution] Error: %s", e)
            return {
                "delta_contrib": 0.0, "gamma_contrib": 0.0,
                "vega_contrib": 0.0, "theta_contrib": 0.0,
                "unexplained": 0.0, "unexplained_pct": 0.0,
            }

    def log_greeks_dashboard_periodic(self) -> None:
        """每分钟周期性输出仪表盘汇总日志，标签[GreekDashboard]"""
        try:
            dashboard = self.get_greeks_dashboard()
            portfolio = dashboard.get("portfolio", {})
            stress = dashboard.get("stress_test", {})
            attribution = dashboard.get("pnl_attribution", {})
            safety = dashboard.get("safety_meta", {})

            delta_usage = portfolio.get("delta_usage_pct", 0)
            gamma_usage = portfolio.get("gamma_usage_pct", 0)
            vega_usage = portfolio.get("vega_usage_pct", 0)
            unexplained_pct = attribution.get("unexplained_pct", 0)

            usage_warning = ""
            if delta_usage > 100 or gamma_usage > 100 or vega_usage > 100:
                usage_warning = " ⚠️ USAGE>100%!"
            if unexplained_pct > 15:
                usage_warning += " ⚠️ RESIDUAL>15%!"

            logging.info(
                "[GreekDashboard] Δ=%.2f(%.0f%%) Γ=%.4f(%.0f%%) V=%.4f(%.0f%%) Θ=%.4f "
                "| 1σPnL=%.0f 2σPnL=%.0f "
                "| attr: D=%.0f G=%.0f V=%.0f T=%.0f unexp=%.1f%% "
                "| safety: CB=%s blocked=%s%s",
                portfolio.get("net_delta", 0), delta_usage,
                portfolio.get("net_gamma", 0), gamma_usage,
                portfolio.get("net_vega", 0), vega_usage,
                portfolio.get("net_theta", 0),
                stress.get("jump_1sigma_pnl", 0), stress.get("jump_2sigma_pnl", 0),
                attribution.get("delta_contrib", 0), attribution.get("gamma_contrib", 0),
                attribution.get("vega_contrib", 0), attribution.get("theta_contrib", 0),
                unexplained_pct,
                safety.get("circuit_breaker_triggered", False),
                safety.get("new_open_blocked", False),
                usage_warning,
            )
        except Exception as e:
            logging.warning("[GreekDashboard] Periodic log error: %s", e)

    def _check_greeks_limits(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """希腊字母硬约束检查 — 订单生成前必须执行

        任一超限则拒绝新开仓信号，只允许减仓或对冲。
        V7增强：每次检查时更新仪表盘缓存。
        """
        action = signal.get("action", "")
        if action == "CLOSE":
            return RiskCheckResponse.pass_result()

        try:
            calc = self._get_greeks_calculator()
            if calc is None:
                return RiskCheckResponse.pass_result()

            positions_dict = {}
            if self.position_manager:
                positions_raw = getattr(self.position_manager, "positions", {})
                for inst_map in positions_raw.values():
                    for inst_id, pos in inst_map.items():
                        vol = getattr(pos, "volume", 0)
                        if vol != 0:
                            positions_dict[inst_id] = vol

            if not positions_dict:
                return RiskCheckResponse.pass_result()

            max_delta = safe_get_float(self.params, "max_net_delta_pct", 0.30) * 1000
            max_gamma = safe_get_float(self.params, "max_net_gamma_pct", 0.08) * 50
            max_vega = safe_get_float(self.params, "max_net_vega_bps", 0.02) * 200

            warnings = calc.check_risk_limits(
                positions_dict,
                delta_limit=max_delta,
                gamma_limit=max_gamma,
                vega_limit=max_vega,
            )

            if warnings:
                return RiskCheckResponse.block_result(
                    "greeks_limit_exceeded",
                    f"希腊字母硬约束超限: {'; '.join(warnings)}",
                    RiskLevel.HIGH
                )

            try:
                self.get_greeks_dashboard()
            except Exception:
                pass

        except ImportError:
            logging.debug("[RiskService._check_greeks_limits] GreeksCalculator not available, skip")
        except Exception as e:
            logging.warning("[RiskService._check_greeks_limits] Check error: %s", e)

        return RiskCheckResponse.pass_result()

    def calculate_risk_metrics(self) -> RiskMetrics:
        """计算风险指标"""
        try:
            if self.position_manager is None:
                return RiskMetrics()

            # ✅ P1修复：将deepcopy移到锁外，减少持锁时间
            # 先读取引用，再在锁外执行deepcopy
            positions_raw = getattr(self.position_manager, "positions", {})
            
            # 在锁外执行deepcopy，避免长时间持锁
            import copy
            positions = copy.deepcopy(positions_raw) if positions_raw else {}
            
            total_exposure = 0.0
            max_position = 0.0
            count = 0

            for inst_map in positions.values():
                for pos in inst_map.values():
                    volume = getattr(pos, "volume", 0)
                    price = getattr(pos, "open_price", 0)
                    exposure = abs(volume) * price
                    total_exposure += exposure
                    max_position = max(max_position, exposure)
                    if volume != 0:
                        count += 1

            # 计算风险比率
            limit = self._get_total_position_limit()
            risk_ratio = total_exposure / limit if limit > 0 else 0.0

            return RiskMetrics(
                total_exposure=total_exposure,
                max_single_position=max_position,
                position_count=count,
                risk_ratio=risk_ratio
            )

        except Exception as e:
            logging.error(f"[RiskService.calculate_risk_metrics] Error: {e}")
            return RiskMetrics()

    # ========================================================================
    # 配置参数获取
    # ========================================================================

    def _get_signal_cooldown(self) -> float:
        """获取信号冷却时间（秒）"""
        return safe_get_float(self.params, "signal_cooldown_sec", 1.0)

    def _get_max_signals_per_window(self) -> int:
        """获取窗口内最大信号数"""
        return safe_get_int(self.params, "max_signals_per_window", 10)

    def _get_rate_limit_window(self) -> float:
        """获取限频窗口（秒）"""
        return safe_get_float(self.params, "rate_limit_window_sec", 60.0)

    def _get_max_risk_ratio(self) -> float:
        """获取最大风险比率"""
        return safe_get_float(self.params, "max_risk_ratio", 0.8)

    def _get_total_position_limit(self) -> float:
        """获取总持仓限额"""
        return safe_get_float(self.params, "total_position_limit", 1000000.0)

    # ========================================================================
    # 状态与统计
    # ========================================================================

    # ✅ ID唯一：get_stats统一接口，返回值含service_name="RiskService"
    def get_stats(self) -> Dict[str, Any]:
        """获取风控统计"""
        with self._lock:
            stats = dict(self._stats)
            stats['service_name'] = 'RiskService'  # ✅ ID唯一：统一标识服务来源
            return stats

    def reset_stats(self) -> None:
        """重置统计"""
        with self._lock:
            self._stats = {
                "total_checks": 0,
                "blocked": 0,
                "warnings": 0,
                "passed": 0,
            }

    # ✅ ID唯一：clear_cache统一接口，服务=RiskService
    def clear_cache(self) -> None:
        """清空缓存"""
        with self._lock:
            self._strategy_status_cache = (0.0, RiskCheckResponse.pass_result())
            self._signal_times.clear()
        logging.info('[RiskService] Cache cleared')
            # ✅ 修复：删除_last_signal_time.clear()


# ============================================================================
# 辅助函数
# ============================================================================

def safe_get(obj: Any, attr: str, default: Any = None, target_type: type = float) -> Any:
    """统一安全属性获取入口（方法唯一修复：公开为safe_get，并行接口为便捷别名）"""
    try:
        val = getattr(obj, attr, default)
        if val is None:
            return default
        return target_type(val) if target_type == int else float(val)
    except (ValueError, TypeError, AttributeError) as e:
        logging.warning(f"[safe_get_{target_type.__name__}] Error getting {attr}: {e}")
        return default


def safe_get_float(obj: Any, attr: str, default: float = 0.0) -> float:
    return safe_get(obj, attr, default, float)


def safe_get_int(obj: Any, attr: str, default: int = 0) -> int:
    return safe_get(obj, attr, default, int)


# ============================================================================
# L-1 安全元层 (SafetyMetaLayer)
# ============================================================================

class SafetyMetaLayer:
    """L-1安全元层 — 独立于策略模型的账户级生存保障

    三条硬规则（最高权限，不可被策略参数覆盖）：
    1. 速率断路器：1分钟内权益回撤超过滚动3σ，暂停交易。
       熔断冷静期(circuit_breaker_calm_period_sec)：首次触发后N秒内不再二次触发，防止极端行情中频繁"抽搐"
    2. 持仓时间硬止损：开仓后max_hold_minutes_hard分钟内浮盈从未达到min_profit_threshold，
       则在max_hold_minutes_hard+30分钟强制平仓
    3. 日最大回撤硬停止：当日累计回撤超过前5日平均日收益的daily_drawdown_multiplier倍，
       立即平掉所有仓位，禁止当日任何后续交易，直到下个交易日人工确认后恢复
    """

    def __init__(self, params: Any = None):
        self._params = params
        self._lock = threading.RLock()

        self._equity_series: deque = deque(maxlen=60)
        self._equity_timestamps: deque = deque(maxlen=60)

        self._trading_paused_until: float = 0.0
        self._pause_reason: str = ""
        self._circuit_breaker_calm_until: float = 0.0

        self._daily_start_equity: Optional[float] = None
        self._daily_peak_equity: float = 0.0
        self._daily_drawdown: float = 0.0
        self._prev_5day_avg_profit: float = 0.0
        self._daily_new_open_blocked: bool = False
        self._daily_hard_stop_triggered: bool = False
        self._current_date: Optional[str] = None

        self._stats = {
            "circuit_breaker_triggers": 0,
            "circuit_breaker_calm_rejects": 0,
            "hard_time_stop_triggers": 0,
            "daily_drawdown_triggers": 0,
            "daily_hard_stop_triggers": 0,
            "total_equity_updates": 0,
        }

    def on_equity_update(self, equity: float) -> None:
        now = time.time()
        today = datetime.now().strftime("%Y-%m-%d")

        with self._lock:
            self._stats["total_equity_updates"] += 1

            if self._current_date != today:
                self._current_date = today
                self._daily_start_equity = equity
                self._daily_peak_equity = equity
                self._daily_drawdown = 0.0
                self._daily_new_open_blocked = False
                if self._daily_hard_stop_triggered:
                    logging.warning(
                        "[SafetyMetaLayer] ⚠️ 新交易日(%s)，但日回撤硬停止尚未人工确认恢复，"
                        "交易仍被禁止。请调用confirm_daily_resume()确认恢复。",
                        today,
                    )

            self._daily_peak_equity = max(self._daily_peak_equity, equity)
            if self._daily_start_equity and self._daily_start_equity > 0:
                self._daily_drawdown = (self._daily_peak_equity - equity) / self._daily_start_equity

            self._equity_series.append(equity)
            self._equity_timestamps.append(now)

            if not self._daily_hard_stop_triggered:
                self._check_circuit_breaker(now)
                self._check_daily_drawdown()

    def _check_circuit_breaker(self, now: float) -> None:
        if len(self._equity_series) < 10:
            return

        recent = list(self._equity_series)
        if len(recent) < 3:
            return

        current = recent[-1]
        one_min_ago_idx = max(0, len(recent) - 6)
        one_min_ago_val = recent[one_min_ago_idx]

        if one_min_ago_val <= 0:
            return

        drop_pct = (one_min_ago_val - current) / one_min_ago_val

        import statistics
        if len(recent) >= 10:
            diffs = [recent[i] - recent[i - 1] for i in range(1, len(recent))]
            mean_diff = statistics.mean(diffs)
            if len(diffs) >= 2:
                std_diff = statistics.stdev(diffs)
            else:
                std_diff = abs(mean_diff) if mean_diff != 0 else 1.0
        else:
            std_diff = 1.0

        threshold = 2.5 * std_diff / one_min_ago_val if one_min_ago_val > 0 else 0.05

        if drop_pct > max(threshold, 0.02):
            if now < self._circuit_breaker_calm_until:
                self._stats["circuit_breaker_calm_rejects"] += 1
                logging.info(
                    "[SafetyMetaLayer] 熔断冷静期内，忽略二次触发 (冷静期剩余%.0fs)",
                    self._circuit_breaker_calm_until - now,
                )
                return

            pause_duration = self._get_circuit_breaker_pause_sec()
            self._trading_paused_until = now + pause_duration
            self._pause_reason = f"速率断路器: 1min回撤{drop_pct:.2%} > 2.5σ阈值{threshold:.2%}"
            self._stats["circuit_breaker_triggers"] += 1

            calm_period = self._get_circuit_breaker_calm_period_sec()
            self._circuit_breaker_calm_until = now + pause_duration + calm_period

            logging.warning(
                "[SafetyMetaLayer] ⚡ 速率断路器触发！1min回撤=%.2f%%, 阈值=%.2f%%, 暂停%.0f秒, 冷静期%.0f秒",
                drop_pct * 100, threshold * 100, pause_duration, calm_period
            )

            self._cancel_pending_on_circuit_breaker()

    def _check_daily_drawdown(self) -> None:
        if self._daily_hard_stop_triggered:
            return

        multiplier = self._get_daily_drawdown_multiplier()
        if multiplier <= 0:
            return

        triggered = False

        if self._prev_5day_avg_profit <= 0:
            if self._daily_start_equity and self._daily_start_equity > 0:
                max_dd = 0.05
                if self._daily_drawdown >= max_dd:
                    triggered = True
                    self._stats["daily_drawdown_triggers"] += 1
                    logging.warning(
                        "[SafetyMetaLayer] 🛑 日最大回撤硬停止触发！回撤=%.2f%% >= 硬限5%%",
                        self._daily_drawdown * 100
                    )
        else:
            max_daily_loss = self._prev_5day_avg_profit * multiplier
            current_loss = self._daily_peak_equity - list(self._equity_series)[-1] if self._equity_series else 0

            if current_loss >= max_daily_loss:
                triggered = True
                self._stats["daily_drawdown_triggers"] += 1
                logging.warning(
                    "[SafetyMetaLayer] 🛑 日最大回撤硬停止触发！当前亏损=%.2f >= 5日均值×%.1f=%.2f",
                    current_loss, multiplier, max_daily_loss
                )

        if triggered:
            self._daily_hard_stop_triggered = True
            self._daily_new_open_blocked = True
            self._stats["daily_hard_stop_triggers"] += 1
            logging.critical(
                "[SafetyMetaLayer] 🚫 会话周期硬终止！所有交易禁止，需人工调用confirm_daily_resume()恢复"
            )

    def check_position_hard_time_stop(self, position_id: str, open_time: float,
                                       max_profit_reached: float) -> Optional[str]:
        hard_limit_min = self._get_hard_time_stop_minutes()
        min_profit_pct = self._get_min_profit_threshold()

        now = time.time()
        elapsed_min = (now - open_time) / 60.0

        if elapsed_min >= hard_limit_min and max_profit_reached < min_profit_pct:
            self._stats["hard_time_stop_triggers"] += 1
            logging.warning(
                "[SafetyMetaLayer] ⏰ 持仓时间硬止损触发！position=%s, 已持%.0fmin, "
                "最高浮盈=%.2f%% < 最低要求=%.2f%%",
                position_id, elapsed_min, max_profit_reached * 100, min_profit_pct * 100
            )
            return f"HardTimeStop@{elapsed_min:.0f}min(profit<{min_profit_pct:.1%})"

        return None

    def is_trading_paused(self) -> Tuple[bool, str]:
        with self._lock:
            if time.time() < self._trading_paused_until:
                remaining = self._trading_paused_until - time.time()
                return True, f"{self._pause_reason}, 剩余{remaining:.0f}秒"
            return False, ""

    def is_new_open_blocked(self) -> bool:
        with self._lock:
            return self._daily_new_open_blocked

    def is_hard_stop_triggered(self) -> bool:
        with self._lock:
            return self._daily_hard_stop_triggered

    def confirm_daily_resume(self) -> bool:
        with self._lock:
            if not self._daily_hard_stop_triggered:
                logging.info("[SafetyMetaLayer] confirm_daily_resume: 未处于硬停止状态，无需恢复")
                return False
            self._daily_hard_stop_triggered = False
            self._daily_new_open_blocked = False
            self._daily_drawdown = 0.0
            logging.critical(
                "[SafetyMetaLayer] ✅ 人工确认恢复交易！日回撤硬停止已解除，交易恢复正常"
            )
            return True

    def set_prev_5day_avg_profit(self, avg_profit: float) -> None:
        with self._lock:
            self._prev_5day_avg_profit = avg_profit

    def _get_circuit_breaker_pause_sec(self) -> float:
        return safe_get_float(self._params, "circuit_breaker_pause_sec", 180.0)

    def _get_circuit_breaker_calm_period_sec(self) -> float:
        return safe_get_float(self._params, "circuit_breaker_calm_period_sec", 600.0)

    def _cancel_pending_on_circuit_breaker(self) -> None:
        try:
            from ali2026v3_trading.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                count = osvc.cancel_all_pending()
                if count > 0:
                    logging.warning("[SafetyMetaLayer] 断路器触发，已撤销 %d 笔未成交订单", count)
        except Exception as e:
            logging.warning("[SafetyMetaLayer] cancel_pending error: %s", e)

    def _get_daily_drawdown_multiplier(self) -> float:
        return safe_get_float(self._params, "daily_drawdown_multiplier", 2.0)

    def _get_hard_time_stop_minutes(self) -> float:
        return safe_get_float(self._params, "hard_time_stop_minutes", 90.0)

    def _get_min_profit_threshold(self) -> float:
        return safe_get_float(self._params, "min_profit_threshold", 0.002)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['trading_paused'] = time.time() < self._trading_paused_until
            stats['new_open_blocked'] = self._daily_new_open_blocked
            stats['hard_stop_triggered'] = self._daily_hard_stop_triggered
            stats['daily_drawdown_pct'] = self._daily_drawdown
            return stats


_safety_meta_layer: Optional[SafetyMetaLayer] = None
_safety_meta_layer_lock = threading.Lock()


def get_safety_meta_layer(params: Any = None) -> SafetyMetaLayer:
    global _safety_meta_layer
    if _safety_meta_layer is None:
        with _safety_meta_layer_lock:
            if _safety_meta_layer is None:
                _safety_meta_layer = SafetyMetaLayer(params=params)
    return _safety_meta_layer


# ============================================================================
# 模块导出
# ============================================================================

__all__ = [
    'RiskService',
    'RiskLevel',
    'RiskCheckResult',
    'RiskCheckResponse',
    'PositionLimit',
    'RiskMetrics',
    'SafetyMetaLayer',
    'get_safety_meta_layer',
]
