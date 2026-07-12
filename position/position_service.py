# MODULE_ID: M1-207
"""Position Service - Facade编排 (CC-09拆分重构)

从2609行瘦身为Facade，委托调用子服务:
- position_command_service.py: on_trade/on_tick/_add/_reduce/_trigger_close/retry (7方法)
- position_check_service.py: 风控检查联动 (6方法)
- position_pnl_service.py: PnL计算+盈亏统计 (8方法)
- position_persistence.py: 持仓持久化+快照 (5方法)

向后兼容: from position_service import PositionService 仍可用
"""
from __future__ import annotations

import json
import logging
from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer
from ali2026v3_trading.infra.metrics_registry import count_call
from ali2026v3_trading.config import config_params

try:
    from ali2026v3_trading.strategy_judgment.causal_chain_utils import (
        CyclicDependencyGuard, ContaminationGuard,
    )
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False

from ali2026v3_trading.infra.resilience import (
    ExponentialBackoff, BoundedRetry, Watchdog, HeartbeatMonitor,
    CircuitBreakerHalfOpen, SlowQueryDetector, DataStalenessDetector,
    RateLimitedLogger, ResourceLeakDetector, safe_callback_wrapper,
    get_process_health, ProcessHealthState,
    stable_sum, stable_mean, stable_variance, stable_std,
    approx_equal, approx_less, approx_greater, approx_less_equal, approx_greater_equal,
    should_trigger_stop_loss, should_trigger_take_profit,
    KahanSummation, safe_divide, compute_sharpe_stable, safe_normalize_weights,
    PRICE_TOLERANCE,
)
import os
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple

from ali2026v3_trading.infra.shared_utils import CHINA_TZ as _CHINA_TZ  # P2-13: 统一CHINA_TZ

from ali2026v3_trading.infra.shared_utils import api_version  # P1-21: 统一从shared_utils导入
from ali2026v3_trading.risk.risk_support import RiskBridgeAdapter, PositionBridgeAdapter, BridgeRiskLevel

try:
    from ali2026v3_trading.infra.security_service import structured_audit_log as _structured_audit_log  # R1-4修复
except ImportError:
    _structured_audit_log = None

# CP-06修复: 以下import从函数体内延迟import提升为模块级import（共享常量模块无循环依赖风险）
from ali2026v3_trading.infra.commission_utils import REASON_MULTIPLIERS  # R27-CP-01-FIX

# CP-06注释: 以下延迟import因懒初始化模式保留在函数体内（非循环依赖，而是服务按需初始化）：
# - position_command_service/position_check_service/position_pnl_service/position_persistence — 服务懒初始化
# - order_service (get_order_service) — 服务懒初始化
# - event_bus (get_global_event_bus) — 服务懒初始化

try:
    from ali2026v3_trading.data.t_type_service import get_t_type_service
    _HAS_T_TYPE = True
except ImportError as e:
    import logging
    logging.warning("[PositionService] Failed to import TTypeService: %s", e)
    _HAS_T_TYPE = False
    get_t_type_service = None

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False
    np = None
if _HAS_NUMPY and np is None:
    _HAS_NUMPY = False

_HAS_RISK_SERVICE = False
RiskService = None


@dataclass(slots=True)
class PositionRecord(object):
    """持仓记录"""
    position_id: str
    instrument_id: str
    exchange: str
    volume: int
    direction: str
    open_price: float
    open_time: datetime
    open_date: datetime.date
    position_type: str
    stop_profit_price: float = 0.0
    stop_loss_price: float = 0.0
    chase_count: int = 0
    open_reason: str = ''
    order_id: str = ''
    signal_id: str = ''  # CHAIN-BUG-5 fix: 贯通开仓signal_id至持仓，平仓订单可回溯开仓信号
    strategy_group: str = ''  # 策略组归属，由open_reason自动映射
    close_reason: str = ''    # 平仓原因，在_trigger_close_position时设置
    realized_pnl: float = 0.0  # 已实现盈亏，在平仓时计算
    target_plr: float = 0.0
    current_plr: float = 0.0
    plr_status: str = ''
    _closing: bool = False
    closing_order_id: str = ''
    open_signal_snapshot: str = ''
    close_method: str = ''
    _max_profit_pct: float = 0.0
    _profit_history: List[float] = None
    profit_slope: float = 0.0
    stage1_passed: bool = False  # P0-1修复: 两阶段止损stage1状态标记
    current_price: float = 0.0
    option_premium: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        # FIX-R37-UNIQUE-UPDATE(B2): 序列化全部字段，确保重启后恢复完整持仓状态
        # 原 to_dict 漏掉 _max_profit_pct/profit_slope/stage1_passed/option_premium/_closing/closing_order_id
        # 导致重启后两阶段止损/利润斜率/期权权利金等状态丢失
        return {
            'position_id': self.position_id, 'instrument_id': self.instrument_id,
            'exchange': self.exchange, 'volume': self.volume, 'direction': self.direction,
            'open_price': self.open_price, 'open_time': self.open_time.isoformat(),
            'open_date': self.open_date.isoformat(), 'position_type': self.position_type,
            'stop_profit_price': self.stop_profit_price, 'stop_loss_price': self.stop_loss_price,
            'chase_count': self.chase_count, 'open_reason': self.open_reason,
            'order_id': self.order_id, 'signal_id': self.signal_id, 'strategy_group': self.strategy_group,
            'close_reason': self.close_reason, 'realized_pnl': self.realized_pnl,
            'target_plr': self.target_plr,
            'current_plr': self.current_plr, 'plr_status': self.plr_status,
            'current_price': self.current_price,
            'open_signal_snapshot': getattr(self, 'open_signal_snapshot', ''),
            'close_method': getattr(self, 'close_method', ''),
            # FIX-R37-UNIQUE-UPDATE(B2): 补充遗漏字段
            '_max_profit_pct': getattr(self, '_max_profit_pct', 0.0),
            'profit_slope': getattr(self, 'profit_slope', 0.0),
            'stage1_passed': getattr(self, 'stage1_passed', False),
            'option_premium': getattr(self, 'option_premium', 0.0),
            '_closing': getattr(self, '_closing', False),
            'closing_order_id': getattr(self, 'closing_order_id', ''),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PositionRecord:
        # FIX-R37-UNIQUE-UPDATE(B2): 恢复时读取全部字段
        return cls(
            position_id=data['position_id'], instrument_id=data['instrument_id'],
            exchange=data['exchange'], volume=data['volume'], direction=data['direction'],
            open_price=data['open_price'], open_time=datetime.fromisoformat(data['open_time']),
            open_date=datetime.fromisoformat(data['open_date']).date(), position_type=data['position_type'],
            stop_profit_price=data.get('stop_profit_price', 0.0), stop_loss_price=data.get('stop_loss_price', 0.0),
            chase_count=data.get('chase_count', 0), open_reason=data.get('open_reason', ''),
            order_id=data.get('order_id', ''), signal_id=data.get('signal_id', ''),
            strategy_group=data.get('strategy_group', ''), close_reason=data.get('close_reason', ''),
            realized_pnl=data.get('realized_pnl', 0.0), target_plr=data.get('target_plr', 0.0),
            current_plr=data.get('current_plr', 0.0), plr_status=data.get('plr_status', ''),
            current_price=data.get('current_price', 0.0), _closing=data.get('_closing', False),
            open_signal_snapshot=data.get('open_signal_snapshot', ''),
            close_method=data.get('close_method', ''),
            # FIX-R37-UNIQUE-UPDATE(B2): 恢复遗漏字段
            _max_profit_pct=data.get('_max_profit_pct', 0.0),
            profit_slope=data.get('profit_slope', 0.0),
            stage1_passed=data.get('stage1_passed', False),
            option_premium=data.get('option_premium', 0.0),
            closing_order_id=data.get('closing_order_id', ''),
        )


@dataclass(slots=True)
class PositionLimitConfig(object):
    """持仓限额配置"""
    limit_amount: float
    account_id: str
    limit_volume: int = 0
    instrument_id: str = ""
    position_type: str = "SPECULATIVE"
    effective_until: Optional[datetime] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(_CHINA_TZ))

    def is_valid(self) -> bool:
        if self.limit_amount <= 0:
            return False
        if self.effective_until is not None:
            # [R27-AUDIT] P1修复: 确保时区感知比较，避免naive vs aware TypeError
            _eff = self.effective_until
            if _eff.tzinfo is None and isinstance(_eff, datetime):
                from datetime import timezone as _tz, timedelta as _td
                _eff = _eff.replace(tzinfo=_tz(_td(hours=8)))
            if datetime.now(_CHINA_TZ) > _eff:
                return False
        return True


class PositionService(object):
    """持仓服务 - Facade编排 (CC-09拆分后)"""

    DEFAULT_TP_RATIO = 1.8
    DEFAULT_SL_RATIO = 0.30
    TP_SL_REASON_DEFAULTS = {
        'CORRECT_RESONANCE': (1.5, 0.50),
        'DIVERGENCE_REVERSAL': (1.3, 0.50),
        'OTHER_SCALP': (1.1, 0.30), 'BOX_SPRING': (1.3, 0.40),  # [FIX-20260712-S4] tp 2.0→1.3(匹配弹簧短持仓特性, 原FIX-20260711-P2: 5.0→2.0)
        'BOX_EXTREME': (1.2, 0.40), 'ARBITRAGE': (1.2, 0.30), 'MARKET_MAKING': (1.1, 0.20),
    }
    OPTION_DELTA_PER_LOT_CALL = 0.5
    OPTION_DELTA_PER_LOT_PUT = -0.5
    OPTION_VEGA_PER_LOT = 0.15
    OPTION_GAMMA_PER_LOT = 0.05
    PLR_RATIO_EXCELLENT = 1.5
    PLR_RATIO_GOOD = 1.0
    PLR_RATIO_POOR = 0.5
    PLR_RATIO_WARNING = 0.8
    PLR_HOLD_MULTIPLIER_EXCELLENT = 1.5
    PLR_HOLD_MULTIPLIER_GOOD = 1.2
    PLR_HOLD_MULTIPLIER_POOR = 0.6
    PLR_HOLD_MULTIPLIER_WARNING = 0.8
    TRAILING_STOP_ACTIVATION_PCT = 0.5
    TRAILING_STOP_RETRACEMENT_PCT = 0.2
    CLOSE_RETRY_MAX_ATTEMPTS = 3
    CLOSE_RETRY_BASE_DELAY_SEC = 0.1
    CLOSE_RETRY_MAX_THREADS = 10
    _MAX_CLOSE_RETRY_THREADS = CLOSE_RETRY_MAX_THREADS
    DEFAULT_MAX_HOLD_MINUTES = 120
    EOD_CLOSE_HOUR = 14
    EOD_CLOSE_MINUTE = 55
    NIGHT_EOD_CLOSE_HOUR = 2
    NIGHT_EOD_CLOSE_MINUTE = 30
    DEFAULT_VALID_HOURS = 24
    MAX_VALID_HOURS = 720
    CRM_SL_CLIP_LOWER = 0.01
    CRM_SL_CLIP_UPPER = 0.99
    DEFAULT_TARGET_PLR = 2.0
    DAILY_DRAWDOWN_CIRCUIT_BREAK_PCT = 3.0
    RISK_TIER_CIRCUIT_BREAK_MULTIPLIER = 2.0
    RISK_TIER_BLOCK_MULTIPLIER = 1.5
    RISK_TIER_REDUCE_MULTIPLIER = 1.2
    _close_retry_executor = None

    def __init__(self, risk_service: Any = None):
        """R17-P1-DOC-P1-03修复: PositionService初始化。Args: risk_service: 风控服务实例(可选)。初始化self_trade_detector(自成交检测器)/network_retry_manager(网络重试)/partial_fill_handler(部分成交处理器)/structured_logger(结构化日志)"""
        self._data_service_ready: bool = False
        self.positions: Dict[str, Dict[str, PositionRecord]] = {}
        # FIX-R37-REALIZED-PNL: 服务级已实现盈亏累加器，持仓记录删除后仍可追踪
        self._total_realized_pnl: float = 0.0
        self._position_snapshot_time: float = 0.0
        self._position_snapshot_ttl: float = 300.0
        self.config_file = "option_buy_limits.json"
        self.position_locks: Dict[str, threading.RLock] = {}
        self.global_lock = threading.RLock()
        self._risk_service = risk_service
        self._risk_bridge = RiskBridgeAdapter(self._risk_service) if self._risk_service else None
        self._position_state_audit_log: List[Dict[str, Any]] = []
        self._position_last_update_time: Dict[str, float] = {}

        try:
            from ali2026v3_trading.order.order_persistence import SelfTradeDetector, NetworkRetryManager, PartialFillHandler
            self.self_trade_detector = SelfTradeDetector()
            self.network_retry_manager = NetworkRetryManager(max_retries=3, base_interval_sec=2.0)
            self.partial_fill_handler = PartialFillHandler(timeout_sec=300.0)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.warning("[PositionService] 订单安全功能集成失败: %s", e)
            self.self_trade_detector = None
            self.network_retry_manager = None
            self.partial_fill_handler = None

        try:
            from ali2026v3_trading.infra.health_monitor import StructuredJsonlLogger
            from ali2026v3_trading.config._params_canary_env import DEFAULT_LOG_DIR
            self._structured_logger = StructuredJsonlLogger(log_dir=DEFAULT_LOG_DIR)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            self._structured_logger = None

        self.t_type_service = get_t_type_service() if _HAS_T_TYPE else None
        from ali2026v3_trading.config._params_canary_env import DEFAULT_LOG_DIR as _DEFAULT_LOG_DIR2
        self._position_state_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), _DEFAULT_LOG_DIR2, 'position_state.jsonl')
        self._position_state_lock = threading.Lock()
        self._cross_shard_lock = threading.RLock()
        self._position_watchdog = Watchdog(timeout_sec=config_params.WATCHDOG_TIMEOUT_POSITION_SEC, name='position_service')
        self._position_heartbeat = HeartbeatMonitor(heartbeat_interval_sec=10.0, missed_threshold=3)
        self._position_cb = CircuitBreakerHalfOpen(failure_threshold=5, open_duration_sec=60.0)
        self._position_slow_query = SlowQueryDetector(slow_threshold_sec=0.5)
        self._position_staleness = DataStalenessDetector(staleness_threshold_sec=30.0)
        self._position_leak_detector = ResourceLeakDetector(name='position_service')
        self._process_health = get_process_health()

        # CC-09: 初始化子服务
        from ali2026v3_trading.position.position_command_service import PositionCommandService
        from ali2026v3_trading.position.position_check_service import PositionCheckService
        from ali2026v3_trading.position.position_pnl_service import PositionPnlService
        from ali2026v3_trading.position.position_persistence import PositionPersistenceService
        self._command_svc = PositionCommandService(self)
        self._check_svc = PositionCheckService(self)
        self._pnl_svc = PositionPnlService(self)
        self._persistence_svc = PositionPersistenceService(self)

        self._persistence_svc._recover_position_state()
        self._load_position_configs()
        self._snapshot_collector = None

    def set_snapshot_collector(self, collector) -> None:
        self._snapshot_collector = collector

        try:
            from ali2026v3_trading.config.config_params import register_param_change_callback
            register_param_change_callback(self._on_config_param_change)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.subscribe_weak('order.partial_fill', self._on_partial_fill_event)
                _bus.subscribe_weak('ParamChangedEvent', self._on_param_changed_event)
                _bus.subscribe_weak('tick_dropped', self.on_tick_dropped)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass

    def _update_tp_sl_from_params(self) -> None:
        """R1-5修复: 提取公共方法，从配置更新止盈止损比率"""
        try:
            from ali2026v3_trading.config.config_service import get_cached_params
            params = get_cached_params() or {}
            tp = params.get('close_take_profit_ratio', self.DEFAULT_TP_RATIO)
            sl = params.get('close_stop_loss_ratio', self.DEFAULT_SL_RATIO)
            tp_val, sl_val = float(tp), float(sl)
            if tp_val > 0 and 0 < sl_val < 1:
                self.DEFAULT_TP_RATIO = tp_val
                self.DEFAULT_SL_RATIO = sl_val
                self._FALLBACK_TP_SL = (tp_val, sl_val)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass

    def _on_config_param_change(self, event: Dict[str, Any]) -> None:
        self._update_tp_sl_from_params()

    def _on_partial_fill_event(self, event: Any) -> None:
        # FIX-R37-UNIQUE-UPDATE(B1): 原 _on_partial_fill_event 访问 _rec.traded_volume/status，
        # 但 PositionRecord(slots=True) 没有这两个字段，导致整个方法成为死代码。
        # 改为基于 _closing 标志和 closing_order_id 判断部分平仓状态。
        try:
            _inst = getattr(event, 'instrument_id', '')
            _filled = getattr(event, 'traded_volume', 0)
            if _inst and _filled > 0:
                with self._get_instrument_lock(_inst):
                    _pos_map = self.positions.get(_inst, {})
                    for _pid, _rec in _pos_map.items():
                        # FIX-R37-UNIQUE-UPDATE(B1): 使用 slots 中实际存在的字段
                        # 部分平仓期间 _closing=True 且 closing_order_id 以 PENDING_ 开头
                        _closing_oid = getattr(_rec, 'closing_order_id', '')
                        if getattr(_rec, '_closing', False) and _closing_oid:
                            # 标记为部分平仓中，便于下游识别
                            if not _closing_oid.startswith('PARTIAL_'):
                                _rec.closing_order_id = f"PARTIAL_{_closing_oid}"
                            break
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass

    def _on_param_changed_event(self, event: Any) -> None:
        """P2-26修复: 与审on_config_param_change功能完全相同，统一委托到。update_tp_sl_from_params"""
        self._update_tp_sl_from_params()

    @staticmethod
    def _get_platform_attr(obj: Any, *attr_names: str, default: Any = None) -> Any:
        """P1-55修复: 优先委托到shared_utils.get_tick_field，回退到原始逻辑"""
        try:
            from ali2026v3_trading.infra.shared_utils import get_tick_field, TICK_FIELD_NAMES
            # 查找attr_names是否匹配某个field_name的候选列表
            for field_name, candidates in TICK_FIELD_NAMES.items():
                if any(a in candidates for a in attr_names):
                    return get_tick_field(obj, field_name, default)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        # 回退：原始逻辑
        for attr in attr_names:
            val = getattr(obj, attr, None)
            if val is not None and val != '':
                return val
        return default

    def _get_instrument_lock(self, instrument_id: str) -> threading.RLock:
        with self.global_lock:
            if instrument_id not in self.position_locks:
                self.position_locks[instrument_id] = threading.RLock()
            return self.position_locks[instrument_id]

    # ========== Facade委托方法 ==========
    def _append_position_state(self, instrument_id: str, position_id: str, action: str, detail: dict = None, signal_id: str = ''):
        return self._persistence_svc._append_position_state(instrument_id, position_id, action, detail, signal_id)
    def _recover_position_state(self):
        return self._persistence_svc._recover_position_state()
    def _recover_positions(self):
        return self._persistence_svc._recover_positions()

    @count_call()
    @api_version("1.0")
    def on_trade(self, trade: Any) -> None:
        return self._command_svc.on_trade(trade)

    def on_tick(self, tick: Any) -> None:
        return self._command_svc.on_tick(tick)

    def on_tick_dropped(self, event_data: dict) -> None:
        try:
            import time as _time_mod
            instrument_id = event_data.get('instrument_id', '')
            if not instrument_id:
                return
            if not hasattr(self, '_tick_drop_timestamps'):
                self._tick_drop_timestamps: dict = {}
            self._tick_drop_timestamps[instrument_id] = event_data.get('timestamp', _time_mod.time())
            _drop_count = len(self._tick_drop_timestamps)
            if _drop_count <= 5 or _drop_count % 100 == 0:
                logging.warning("[R31-P0-04] tick断流: instrument=%s reason=%s (累计%d合约断流)",
                              instrument_id, event_data.get('reason', '?'), _drop_count)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            logging.debug("[PositionService.on_tick_dropped] Error: %s", e)

    def set_position_limit(self, account_id: str, limit_amount: float, valid_hours: Optional[int] = None, force_set: bool = False) -> bool:
        try:
            if not account_id:
                return False
            if valid_hours is None:
                valid_hours = self.DEFAULT_VALID_HOURS
            if not 1 <= valid_hours <= self.MAX_VALID_HOURS:
                return False
            until = datetime.now(_CHINA_TZ) + timedelta(hours=valid_hours)
            if self._risk_bridge is not None:
                try:
                    self._risk_bridge.set_position_limit(account_id=account_id, limit_amount=limit_amount, effective_until=until)
                    return True
                except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                    return False
            return False
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            return False

    def _total_volume(self, instrument_id: str) -> int:
        """P2-32修复: 提取volume求和，消除3处重复sum(rec.volume for rec in ...)"""
        if instrument_id not in self.positions:
            return 0
        return sum(rec.volume for rec in self.positions[instrument_id].values())

    def get_position(self, instrument_id: str) -> Dict[str, Any]:
        import time as _t
        _now = _t.time()
        if self._position_snapshot_time > 0 and (_now - self._position_snapshot_time) > self._position_snapshot_ttl:
            logging.warning("[FR-P1-05-FIX] 持仓快照过期: inst=%s", instrument_id)
        self._position_snapshot_time = _now
        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                return {"volume": 0, "average_price": 0, "positions": []}
            positions = list(self.positions[instrument_id].values())
            total_volume = self._total_volume(instrument_id)  # P2-32: 使用提取的方法
            if total_volume == 0:
                return {"volume": 0, "average_price": 0, "positions": []}
            weighted_sum = sum(Decimal(str(abs(rec.volume))) * Decimal(str(rec.open_price)) for rec in positions)
            average_price = float(weighted_sum / Decimal(str(abs(total_volume))))
            return {"volume": total_volume, "average_price": average_price, "positions": positions}

    def get_net_position(self, instrument_id: str) -> int:
        with self._get_instrument_lock(instrument_id):
            return self._total_volume(instrument_id)  # P2-32: 使用提取的方法

    def validate_net_position_consistency(self, instrument_id: str) -> bool:
        return self._check_svc.validate_net_position_consistency(instrument_id)

    def has_position(self, instrument_id: str, check_nonzero: bool = True) -> bool:
        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                return False
            pos_dict = self.positions[instrument_id]
            if not pos_dict:
                return False
            return any(r.volume != 0 for r in pos_dict.values()) if check_nonzero else len(pos_dict) > 0

    def check_position_limit(self, account_id: str, required_amount: float) -> bool:
        return self._check_svc.check_position_limit(account_id, required_amount)

    def calculate_position_risk(self, instrument_id: str) -> float:
        return self._check_svc.calculate_position_risk(instrument_id)

    def get_position_info(self) -> List[Dict[str, Any]]:
        with self.global_lock:
            result = []
            current_date = datetime.now(_CHINA_TZ).date()
            for inst_map in self.positions.values():
                for record in inst_map.values():
                    if record.volume != 0:
                        r_open_date = record.open_date
                        if isinstance(r_open_date, datetime):
                            r_open_date = r_open_date.date()
                        days_held = (current_date - r_open_date).days
                        result.append({
                            "仓位 ID": record.position_id, "合约": record.instrument_id,
                            "开仓价": f"{record.open_price:.2f}", "持仓量": record.volume,
                            "方向": "多头" if record.direction == "long" else "空头",
                            "性质": record.position_type, "开仓日期": r_open_date.strftime("%Y-%m-%d"),
                            "持仓天数": days_held, "开仓超过3天": days_held >= 3,
                            "止盈价": f"{record.stop_profit_price:.2f}", "追单次数": record.chase_count
                        })
            return result

    def _add_position(self, exchange: str, instrument_id: str, volume: int, price: float, open_reason: str = '', order_id: str = '', signal_id: str = '') -> None:
        return self._command_svc._add_position(exchange, instrument_id, volume, price, open_reason, order_id, signal_id)
    def _reduce_position(self, exchange: str, instrument_id: str, volume: int, is_buy: bool, price: float) -> None:
        return self._command_svc._reduce_position(exchange, instrument_id, volume, is_buy, price)
    def _trigger_close_position(self, record, reason: str, current_price: float = 0.0) -> None:
        return self._command_svc._trigger_close_position(record, reason, current_price)
    # FIX-R37-UNIQUE-CLOSE: 统一回滚入口，TradeRollbackManager必须通过此方法操作持仓
    def _rollback_position(self, instrument_id: str, position_id: str, reason: str = 'rollback') -> bool:
        return self._command_svc._rollback_position(instrument_id, position_id, reason)
    def _restore_position_state(self, position_id: str, instrument_id: str, snapshot) -> None:
        return self._command_svc._restore_position_state(position_id, instrument_id, snapshot)
    def _schedule_close_retry(self, record, price: float) -> None:
        return self._command_svc._schedule_close_retry(record, price)

    _HARDCODED_REASON_DEFAULTS = {
        'CORRECT_RESONANCE': (1.5, 0.50), 'HIGH_FREQ': (1.8, 0.30),
        'DIVERGENCE_REVERSAL': (1.3, 0.50),
        'OTHER_SCALP': (1.1, 0.30),
        'BOX_SPRING': (1.3, 0.40),  # [FIX-20260712-S4] tp 2.0→1.3(匹配弹簧短持仓特性)
        'ARBITRAGE': (1.2, 0.30), 'MARKET_MAKING': (1.1, 0.20), 'BOX_EXTREME': (1.2, 0.40),
        'MANUAL': (1.5, 0.50), '': (1.5, 0.50),
    }
    _FALLBACK_TP_SL = (1.8, 0.30)

    def _get_tp_sl_ratios_by_reason(self, open_reason: str) -> Tuple[float, float]:
        try:
            from ali2026v3_trading.config.state_param import get_state_param_manager
            spm = get_state_param_manager()
            reason_params = spm.get_params(open_reason)
            tp = reason_params.get('close_take_profit_ratio')
            sl = reason_params.get('close_stop_loss_ratio')
            if tp is not None and sl is not None:
                tp_val, sl_val = float(tp), float(sl)
                if tp_val > 0 and 0 < sl_val < 1:
                    return (tp_val, sl_val)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        return self._HARDCODED_REASON_DEFAULTS.get(open_reason, self._FALLBACK_TP_SL)

    def _verify_tp_sl_alignment_with_backtest(self, open_reason: str, tp: float, sl: float) -> None:
        try:
            bt_defaults = dict(getattr(self.__class__, 'TP_SL_REASON_DEFAULTS', {}) or {})
            if open_reason in bt_defaults:
                bt_tp, bt_sl = bt_defaults[open_reason]
                if abs(tp - bt_tp) > 0.05 or abs(sl - bt_sl) > 0.05:
                    logging.warning("[P0-2修复] 生产/回测TP-SL分叉: reason=%s prod=(%s,%s) bt=(%s,%s)", open_reason, tp, sl, bt_tp, bt_sl)
            else:
                bt_mult = REASON_MULTIPLIERS.get(open_reason)
                if bt_mult is not None:
                    base_tp = float(getattr(self, 'DEFAULT_TP_RATIO', 1.8))
                    base_sl = float(getattr(self, 'DEFAULT_SL_RATIO', 0.30))
                    bt_tp = base_tp * bt_mult.get('tp_mult', 1.0)
                    bt_sl = base_sl * bt_mult.get('sl_mult', 1.0)
                    if abs(tp - bt_tp) > 0.05 or abs(sl - bt_sl) > 0.05:
                        logging.warning("[P0-2修复] 生产/回测TP-SL分叉(回退路径): reason=%s prod=(%s,%s) bt=(%s,%s)", open_reason, tp, sl, bt_tp, bt_sl)
        except (ImportError, AttributeError):
            pass

    def _apply_crm_stop_loss_adjustment(self, sl_ratio: float, open_reason: str) -> float:
        try:
            import importlib
            crm_module = importlib.import_module('ali2026v3_trading.param_pool.optimization.cycle_sharpe')
            crm = crm_module.get_cycle_resonance_module()
            strategy = _REASON_STRATEGY_MAP.get(open_reason, 'high_freq')  # 五唯一性修复：直接使用字典查找
            rs = crm.get_risk_surface(strategy)
            adjusted = sl_ratio * rs.stop_loss_multiplier
            return float(np.clip(adjusted, self.CRM_SL_CLIP_LOWER, self.CRM_SL_CLIP_UPPER)) if _HAS_NUMPY else max(self.CRM_SL_CLIP_LOWER, min(self.CRM_SL_CLIP_UPPER, adjusted))
        except (ImportError, ModuleNotFoundError):
            return sl_ratio
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            return sl_ratio

    def _map_reason_to_strategy(self, open_reason: str) -> str:
        """向后兼容包装：映射开仓理由到策略组。五唯一性修复：直接使用_REASON_STRATEGY_MAP字典。"""
        return _REASON_STRATEGY_MAP.get(open_reason, 'high_freq')

    def _get_open_reason_from_order(self, instrument_id: str, order_id: str = '') -> str:
        try:
            from ali2026v3_trading.order.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                if order_id:
                    order = osvc.get_order(order_id)
                    if order:
                        return order.get('open_reason', '')
                orders = osvc.get_orders_by_instrument(instrument_id)
                for o in orders:
                    if o.get('action') == 'OPEN' and o.get('status') in ('SUBMITTED', 'FILLED', 'ALL_FILLED', '全成', '全部成交', '部成部撤'):
                        return o.get('open_reason', '')
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass
        return ''

    def _get_signal_id_from_order(self, instrument_id: str, order_id: str = '') -> str:
        # CHAIN-BUG-5 fix: 通过order_id回查开仓signal_id，贯通至PositionRecord
        try:
            from ali2026v3_trading.order.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                if order_id:
                    order = osvc.get_order(order_id)
                    if order:
                        return order.get('signal_id', '')
                orders = osvc.get_orders_by_instrument(instrument_id)
                for o in orders:
                    if o.get('action') == 'OPEN' and o.get('status') in ('SUBMITTED', 'FILLED', 'ALL_FILLED', '全成', '全部成交', '部成部撤'):
                        return o.get('signal_id', '')
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
        return ''

    def _compute_option_premium(self, instrument_id: str, price: float) -> float:
        _is_option = any(k in instrument_id.upper() for k in ['-C-', '-P-', '_C_', '_P_'])
        return price if _is_option and price > 0 else 0.0

    # 更多Facade委托
    def _check_stop_profit(self, record, current_price: float) -> None:
        return self._pnl_svc._check_stop_profit(record, current_price)
    def _check_stop_loss(self, record, current_price: float) -> None:
        return self._pnl_svc._check_stop_loss(record, current_price)
    def _check_option_expiry(self, instrument_id: str) -> None:
        return self._pnl_svc._check_option_expiry(instrument_id)
    def _calc_days_to_expiry(self, instrument_id: str) -> Optional[int]:
        return self._pnl_svc._calc_days_to_expiry(instrument_id)
    def check_trailing_stop(self, record) -> Optional[str]:
        return self._check_svc.check_trailing_stop(record)
    def check_all_positions(self) -> None:
        return self._check_svc.check_all_positions()
    def _validate_pnl_equity_consistency(self) -> None:
        return self._check_svc._validate_pnl_equity_consistency()
    def _check_time_stop(self, record, now: datetime = None) -> None:
        return self._pnl_svc._check_time_stop(record, now)
    def _check_two_stage_stop(self, record, now: datetime = None) -> None:
        return self._pnl_svc._check_two_stage_stop(record, now)
    def _check_option_expiry_force_close(self) -> None:
        return self._pnl_svc._check_option_expiry_force_close()
    def _check_eod_close(self, now: datetime = None) -> None:
        return self._pnl_svc._check_eod_close(now)
    def _load_position_configs(self) -> None:
        return self._persistence_svc._load_position_configs()
    def _save_position_configs(self) -> None:
        return self._persistence_svc._save_position_configs()

    @classmethod
    def _cleanup_close_retry_executor(cls):
        pass  # 已委托到。command_svc

    def __del__(self):
        try:
            if hasattr(self, '_command_svc') and self._command_svc:
                self._command_svc._cleanup_close_retry_executor()
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass

    def get_status(self) -> str:
        with self.global_lock:
            total_instruments = len(self.positions)
            total_records = sum(len(v) for v in self.positions.values())
            total_configs = len(getattr(self._risk_bridge._risk_service, '_position_limits', {})) if self._risk_bridge else 0
            return f"PositionService: Tracking {total_instruments} instruments, {total_records} positions, {total_configs} limits"


_position_service_instances: Dict[str, PositionService] = {}
_position_service_lock = threading.Lock()


def get_position_service(risk_service: Any = None, scope_id: Optional[str] = None) -> PositionService:
    scope = str(scope_id or "global")
    with _position_service_lock:
        if scope not in _position_service_instances:
            _position_service_instances[scope] = PositionService(risk_service=risk_service)
        instance = _position_service_instances[scope]
        if risk_service is not None and instance._risk_service is None:
            instance._risk_service = risk_service
        return instance


def reset_position_service(scope_id: Optional[str] = None) -> None:
    with _position_service_lock:
        if scope_id is None:
            _position_service_instances.clear()
        else:
            _position_service_instances.pop(str(scope_id), None)


# CC-09: 辅助函数从position_utils.py导入(向后兼容)
from ali2026v3_trading.position.position_utils import (
    _calc_max_volume_from_capital, _cancel_and_resend,
    _check_available_amount, reconcile_positions_with_exchange,
)

# CC-09: Greeks/风控守卫从position_greeks.py导入(向后兼容)
from ali2026v3_trading.position.position_greeks import (
    GreeksExposure, CrossStrategyRiskGuard, aggregate_greeks_exposure,
    _is_option_instrument, _estimate_option_delta, _estimate_option_vega,
    _estimate_option_gamma, _REASON_STRATEGY_MAP,
    get_cross_strategy_risk_guard,
)
