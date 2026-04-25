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

    # ========================================================================
    # 主检查接口
    # ========================================================================

    def check_before_trade(self, signal: Dict[str, Any]) -> RiskCheckResponse:
        """
        交易前风控检查（主入口）

        Args:
            signal: 信号字典，包含 symbol, exchange, is_valid 等

        Returns:
            RiskCheckResponse: 检查结果
        """
        try:
            # 1. 策略状态检查
            status_result = self._check_strategy_status()
            if status_result.is_block:
                return self._record_result(status_result)

            # 2. 信号有效性检查
            if not signal.get("is_valid", True):
                return self._record_result(
                    RiskCheckResponse.block_result("signal_invalid", "信号无效")
                )

            # 3. 限频检查
            symbol = signal.get("symbol", "")
            rate_result = self._check_rate_limit(symbol)
            if rate_result.is_block:
                return self._record_result(rate_result)

            # 4. 持仓限额检查
            account_id = signal.get("account_id", "default")
            amount = signal.get("amount", 0)
            limit_result = self._check_position_limit(account_id, amount)
            if limit_result.is_block:
                return self._record_result(limit_result)

            # 5. 风险比率检查
            risk_result = self._check_risk_ratio(signal)
            if risk_result.is_block:
                return self._record_result(risk_result)

            # 记录信号时间
            self._record_signal_time(symbol)

            return self._record_result(RiskCheckResponse.pass_result("风控检查通过"))

        except Exception as e:
            logging.error(f"[RiskService] 风控检查异常，采用fail-safe阻断: {e}")
            return RiskCheckResponse.block_result("check_error", f"风控检查异常，采用fail-safe阻断: {e}", RiskLevel.CRITICAL)

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

            if limit is not None and limit.limit_amount < required_amount:
                return RiskCheckResponse.block_result(
                    "position_limit_exceeded",
                    f"持仓限额不足: 需要 {required_amount:.2f}, 限额 {limit.limit_amount:.2f}",
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
        """获取持仓限额"""
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

    def calculate_risk_metrics(self) -> RiskMetrics:
        """计算风险指标"""
        try:
            if self.position_manager is None:
                return RiskMetrics()

            # 获取持仓信息
            # M22-Bug1修复：使用copy.deepcopy避免嵌套dict并发修改
            positions_raw = getattr(self.position_manager, "positions", {})
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
# 模块导出
# ============================================================================

__all__ = [
    'RiskService',
    'RiskLevel',
    'RiskCheckResult',
    'RiskCheckResponse',
    'PositionLimit',
    'RiskMetrics',
]
