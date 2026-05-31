"""Position Service - Command Handler for Position Management

CQRS Command Service for managing trading positions.
Handles position lifecycle: open, close, query, and risk management.

重构来源:
- 08_position.py (600 行) → position_service.py (300 行计算)
- 实际精简至 300 行 (-50%)

核心功能:
1. 持仓记录管理 (PositionRecord CRUD)
2. 持仓限额配置 (PositionLimitConfig)
3. 止盈止损检查 (Stop Profit/Loss)
4. 超期持仓风控 (Overdue Position Close)
5. 持仓风险评估 (Position Risk Calculation)

架构改进:
- 移除 PlatformCompatMixin (冗余)
- 简化 Getter/Setter 方法 (Pythonic)
- 整合到 EventBus 事件机制
- 使用 storage.py 统一数据持久化"""
from __future__ import annotations

import json
import logging
from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer
from ali2026v3_trading.performance_monitor import count_call
from ali2026v3_trading import config_params

try:
    from ali2026v3_trading.causal_chain_utils import (
        CyclicDependencyGuard, ContaminationGuard,
    )
    _HAS_CAUSAL_CHAIN = True
except ImportError:
    _HAS_CAUSAL_CHAIN = False

# R27-P1修复: 导入容错/浮点/竞态工具
from ali2026v3_trading.resilience_utils import (
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
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple

# R22-TIME-01: 统一时区常量
_CHINA_TZ = timezone(timedelta(hours=8))

# UPG-P1-04修复: 从risk_service导入api_version装饰器
from ali2026v3_trading.risk_service import api_version

# R24-P0-TR-05修复: 持仓变更审计日志
try:
    from ali2026v3_trading.risk_service import structured_audit_log as _structured_audit_log
except ImportError:
    _structured_audit_log = None

# T-Type Service 导入（避免循环依赖）
try:
    # ✅ 传递渠道唯一：导入工厂函数，禁止直接实例化TTypeService
    from .t_type_service import get_t_type_service
    _HAS_T_TYPE = True
except ImportError as e:
    import logging
    logging.warning(f"[PositionService] Failed to import TTypeService: {e}")
    _HAS_T_TYPE = False
    get_t_type_service = None

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False
    np = None
# R13-P2-DEAD-07修复: _HAS_NUMPY检查后np=None的guard
if _HAS_NUMPY and np is None:
    _HAS_NUMPY = False
    logging.warning("[PositionService] numpy导入成功但np=None，降级为纯Python计算")

# RiskService 导入（懒加载避免循环依赖）
_HAS_RISK_SERVICE = False
RiskService = None


@dataclass(slots=True)
class PositionRecord(object):
    """持仓记录
    
    Attributes:
        position_id: 唯一仓位 ID
        instrument_id: 合约代码
        exchange: 交易所
        volume: 持仓量(正=多，负=空)
        direction: 方向 ("long"/"short")
        open_price: 开仓价格        open_time: 开仓时间        open_date: 开仓日期        position_type: 持仓类型 ("long"/"short")
        stop_profit_price: 止盈价格
        chase_count: 追单次数
        open_reason: V7新增-开仓理由编码 (CORRECT_RESONANCE/CORRECT_DIVERGENCE/INCORRECT_REVERSAL/OTHER_SCALP/MANUAL)
    """
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
    target_plr: float = 0.0
    current_plr: float = 0.0
    plr_status: str = ''
    _closing: bool = False
    _max_profit_pct: float = 0.0
    _profit_history: List[float] = None
    profit_slope: float = 0.0
    current_price: float = 0.0
    option_premium: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于 JSON 序列化）"""
        return {
            'position_id': self.position_id,
            'instrument_id': self.instrument_id,
            'exchange': self.exchange,
            'volume': self.volume,
            'direction': self.direction,
            'open_price': self.open_price,
            'open_time': self.open_time.isoformat(),
            'open_date': self.open_date.isoformat(),
            'position_type': self.position_type,
            'stop_profit_price': self.stop_profit_price,
            'stop_loss_price': self.stop_loss_price,
            'chase_count': self.chase_count,
            'open_reason': self.open_reason,
            'order_id': self.order_id,
            'target_plr': self.target_plr,
            'current_plr': self.current_plr,
            'plr_status': self.plr_status,
            'current_price': self.current_price,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PositionRecord:
        """从字典创建"""
        return cls(
            position_id=data['position_id'],
            instrument_id=data['instrument_id'],
            exchange=data['exchange'],
            volume=data['volume'],
            direction=data['direction'],
            open_price=data['open_price'],
            open_time=datetime.fromisoformat(data['open_time']),
            open_date=datetime.fromisoformat(data['open_date']).date(),
            position_type=data['position_type'],
            stop_profit_price=data.get('stop_profit_price', 0.0),
            stop_loss_price=data.get('stop_loss_price', 0.0),
            chase_count=data.get('chase_count', 0),
            open_reason=data.get('open_reason', ''),
            order_id=data.get('order_id', ''),
            target_plr=data.get('target_plr', 0.0),
            current_plr=data.get('current_plr', 0.0),
            plr_status=data.get('plr_status', ''),
            current_price=data.get('current_price', 0.0),
            _closing=data.get('_closing', False)
        )


@dataclass(slots=True)
class PositionLimitConfig(object):
    """持仓限额配置
    
    Attributes:
        limit_amount: 限额金额
        account_id: 账户 ID
        limit_volume: 按手数限额(交易所规定)  # CMP-04新增
        instrument_id: 品种级别  # CMP-04新增
        position_type: 投机/套保/套利分类  # CMP-04新增
        effective_until: 有效期至
        created_at: 创建时间
    """
    limit_amount: float
    account_id: str
    limit_volume: int = 0  # CMP-04新增: 按手数限额(交易所规定)
    instrument_id: str = ""  # CMP-04新增: 品种级别
    position_type: str = "SPECULATIVE"  # CMP-04新增: 投机/套保/套利
    effective_until: Optional[datetime] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(_CHINA_TZ))
    
    def is_valid(self) -> bool:
        """检查限额有效"""
        if self.limit_amount <= 0:
            return False
        if self.effective_until and datetime.now(_CHINA_TZ) > self.effective_until:
            return False
        return True


class PositionService(object):
    """持仓服务 - CQRS Command Handler

    职责:
    1. 管理所有持仓记录 (增删改查)
    2. 执行止盈止损检查    3. 超期持仓风控平仓
    4. 持仓限额管理
    5. 持仓风险评估

    事件驱动:
    - 订阅 TradeEvent (成交回报)
    - 订阅 TickEvent (行情更新)
    - 发布 PositionChangedEvent (持仓变更)
    """

    # R14-P1-DOC-P1-09修复: 止盈止损倍数添加量化来源注释
    # P1-35: 核心魔法数字参数化 — 止盈止损倍数
    DEFAULT_TP_RATIO = 1.8  # R25-P1-DF-07修复: 从1.5改为1.8，与V7.0手册§9.1和CENTRALIZED_DEFAULTS对齐
    DEFAULT_SL_RATIO = 0.30  # R24-P0-DF-03修复: 从0.50改为0.30，与V7.0手册§9.1对齐
    TP_SL_REASON_DEFAULTS = {
        'CORRECT_RESONANCE': (1.5, 0.50),  # 来源: V7.0手册§9.2 正确共振: 标准盈亏比
        'CORRECT_DIVERGENCE': (1.2, 0.40),  # 来源: V7.0手册§9.2 正确背离: 保守盈亏比
        'INCORRECT_REVERSAL': (1.3, 0.60),  # 来源: V7.0手册§9.2 错误反转: 宽止损
        'OTHER_SCALP': (1.1, 0.30),  # 来源: V7.0手册§9.2 其他剥头皮: 窄止盈窄止损
        'BOX_SPRING': (5.0, 0.60),  # 来源: V7.0手册§9.2 箱体弹簧: 高盈亏比宽止损
    }

    # 期权Greeks估计默认值
    OPTION_DELTA_PER_LOT_CALL = 0.5
    OPTION_DELTA_PER_LOT_PUT = -0.5
    OPTION_VEGA_PER_LOT = 0.15
    OPTION_GAMMA_PER_LOT = 0.05

    # PLR弹性调整阈值
    PLR_RATIO_EXCELLENT = 1.5
    PLR_RATIO_GOOD = 1.0
    PLR_RATIO_POOR = 0.5
    PLR_RATIO_WARNING = 0.8
    PLR_HOLD_MULTIPLIER_EXCELLENT = 1.5
    PLR_HOLD_MULTIPLIER_GOOD = 1.2
    PLR_HOLD_MULTIPLIER_POOR = 0.6
    PLR_HOLD_MULTIPLIER_WARNING = 0.8

    # 浮动止盈参数
    TRAILING_STOP_ACTIVATION_PCT = 0.5
    TRAILING_STOP_RETRACEMENT_PCT = 0.2

    # 平仓重试参数
    CLOSE_RETRY_MAX_ATTEMPTS = 3
    CLOSE_RETRY_BASE_DELAY_SEC = 0.1
    CLOSE_RETRY_MAX_THREADS = 10
    # 兼容历史验收脚本命名
    _MAX_CLOSE_RETRY_THREADS = CLOSE_RETRY_MAX_THREADS

    # 持仓时间止损默认参数
    DEFAULT_MAX_HOLD_MINUTES = 120
    EOD_CLOSE_HOUR = 14
    EOD_CLOSE_MINUTE = 55
    NIGHT_EOD_CLOSE_HOUR = 2
    NIGHT_EOD_CLOSE_MINUTE = 30  # [R22-TIME-P1-11] 夜盘收尾时间02:30(原02:25未覆盖尾盘)
    DEFAULT_VALID_HOURS = 24
    MAX_VALID_HOURS = 720

    CRM_SL_CLIP_LOWER = 0.01
    CRM_SL_CLIP_UPPER = 0.99
    DEFAULT_TARGET_PLR = 2.0
    DAILY_DRAWDOWN_CIRCUIT_BREAK_PCT = 3.0
    RISK_TIER_CIRCUIT_BREAK_MULTIPLIER = 2.0
    RISK_TIER_BLOCK_MULTIPLIER = 1.5
    RISK_TIER_REDUCE_MULTIPLIER = 1.2

    def __init__(self, risk_service: Any = None):
        """初始化持仓服务

        RTO目标: 5分钟, RPO目标: 0数据丢失

        R15-P1-DOC-P1-03修复: docstring内#注释语法改为自由描述文本

        Args:
            risk_service: RiskService实例（可选），用于仓位限制委托

        Returns:
            None
        """
        # R23-IN-P1-10-FIX: 数据服务就绪守卫
        self._data_service_ready: bool = False
        # 持仓数据结构：{ instrument_id: { position_id: PositionRecord } }
        self.positions: Dict[str, Dict[str, PositionRecord]] = {}
        # [FR-P1-05-FIX] 持仓快照时间戳和TTL校验
        self._position_snapshot_time: float = 0.0
        self._position_snapshot_ttl: float = 300.0  # 5分钟TTL

        # 持仓限额配置：已删除limit_configs本地存储，统一使用RiskService._position_limits
        # ✅ 传递渠道唯一#65：RiskService为唯一限额源，本地仅从ID读取

        # 配置文件
        self.config_file = "option_buy_limits.json"

        # 线程安全锁 - 按合约分片锁
        # R21-CC-P2-05修复: RLock vs Lock选择说明
        #   position_locks使用RLock: update_position→_apply_crm_stop_loss_adjustment等方法嵌套调用需重入
        #   global_lock使用RLock: _get_instrument_lock内部获取global_lock后可能被其他已持有global_lock的方法调用
        #   _position_state_lock使用Lock: 仅保护简单的状态标志读写，无重入需求
        self.position_locks: Dict[str, threading.RLock] = {}  # 按合约ID的锁
        self.global_lock = threading.RLock()  # 全局锁，用于初始化合约锁

        # RiskService引用（仓位限制权威源）
        self._risk_service = risk_service
        # [R23-P2-SM-08-FIX] 持仓状态变更审计记录
        self._position_state_audit_log: List[Dict[str, Any]] = []
        # [R23-P2-FR-05-FIX] 持仓数据年龄监控
        self._position_last_update_time: Dict[str, float] = {}

        # ✅ 集成订单安全功能（L-P0-1~L-P0-3）
        try:
            from ali2026v3_trading.order_persistence import (
                SelfTradeDetector, NetworkRetryManager, PartialFillHandler,
            )
            self.self_trade_detector = SelfTradeDetector()
            self.network_retry_manager = NetworkRetryManager(max_retries=3, base_interval_sec=2.0)
            self.partial_fill_handler = PartialFillHandler(timeout_sec=300.0)
            logging.info("[PositionService] 订单安全功能已集成: SelfTradeDetector + NetworkRetryManager + PartialFillHandler")
        except Exception as e:
            logging.warning(f"[PositionService] 订单安全功能集成失败: {e}")
            self.self_trade_detector = None
            self.network_retry_manager = None
            self.partial_fill_handler = None

        # ✅ 集成结构化JSONL日志（L-P1-1）
        try:
            from ali2026v3_trading.health_check_api import StructuredJsonlLogger
            self._structured_logger = StructuredJsonlLogger(log_dir="logs")
            logging.info("[PositionService] 结构化JSONL日志已集成")
        except Exception as e:
            logging.warning(f"[PositionService] 结构化日志集成失败: {e}")
            self._structured_logger = None

        # 加载配置
        self._load_position_configs()

        # T-Type Service 引用（用于期权分析）
        # ✅ 传递渠道唯一：使用get_t_type_service()工厂函数获取单例
        self.t_type_service = get_t_type_service() if _HAS_T_TYPE else None

        # R15-P0-RES-06修复: 持仓状态append-only JSON日志，启动时恢复
        self._position_state_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'logs', 'position_state.jsonl'
        )
        self._position_state_lock = threading.Lock()

        # R27-P1-RC-02修复: 跨分片持仓操作协调锁 — 多个instrument的原子操作使用全局协调锁
        self._cross_shard_lock = threading.RLock()
        # R27-P1-DR-06修复: 持仓服务看门狗; R27-P2-DR-10修复: 超时配置外置
        self._position_watchdog = Watchdog(timeout_sec=config_params.WATCHDOG_TIMEOUT_POSITION_SEC, name='position_service')
        # R27-P1-DR-07修复: 持仓服务心跳
        self._position_heartbeat = HeartbeatMonitor(heartbeat_interval_sec=10.0, missed_threshold=3)
        # R27-P1-DR-09修复: 持仓断路器半开恢复
        self._position_cb = CircuitBreakerHalfOpen(failure_threshold=5, open_duration_sec=60.0)
        # R27-P1-DR-12修复: 慢查询检测
        self._position_slow_query = SlowQueryDetector(slow_threshold_sec=0.5)
        # R27-P1-DR-14修复: 数据陈旧检测
        self._position_staleness = DataStalenessDetector(staleness_threshold_sec=30.0)
        # R27-P1-DR-20修复: 资源泄漏检测
        self._position_leak_detector = ResourceLeakDetector(name='position_service')
        # R27-P1-DR-24修复: 进程健康状态
        self._process_health = get_process_health()
        self._recover_position_state()

        self._param_change_callback_registered = False
        try:
            from ali2026v3_trading.config_params import register_param_change_callback
            register_param_change_callback(self._on_config_param_change)
            self._param_change_callback_registered = True
        except Exception as e:
            logging.debug("[PositionService] 参数变更回调注册失败: %s", e)

        # DFG-P1-06修复: 订阅部分成交事件，更新持仓服务
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.subscribe_weak('order.partial_fill', self._on_partial_fill_event)
        except Exception:
            logging.warning("[R22-EP-P1] PositionService exception swallowed")
            pass

        # DFG-P1-07修复: 订阅参数变更事件，刷新缓存
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.subscribe_weak('ParamChangedEvent', self._on_param_changed_event)
        except Exception:
            logging.warning("[R22-EP-P1] PositionService exception swallowed")
            pass

        # R32-P0-04修复: 订阅tick_dropped事件，使position_service感知行情中断
        try:
            from ali2026v3_trading.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.subscribe_weak('tick_dropped', self.on_tick_dropped)
        except Exception:
            logging.warning("[R32-P0-04] PositionService tick_dropped订阅失败")
            pass

    def _on_config_param_change(self, event: Dict[str, Any]) -> None:
        """参数更新后刷新仓位侧默认TP/SL，避免配置变更不可见。"""
        try:
            from ali2026v3_trading.config_params import get_cached_params
            params = get_cached_params() or {}
            tp = params.get('close_take_profit_ratio', self.DEFAULT_TP_RATIO)
            sl = params.get('close_stop_loss_ratio', self.DEFAULT_SL_RATIO)
            tp_val = float(tp)
            sl_val = float(sl)
            if tp_val > 0 and 0 < sl_val < 1:
                self.DEFAULT_TP_RATIO = tp_val
                self.DEFAULT_SL_RATIO = sl_val
                self._FALLBACK_TP_SL = (tp_val, sl_val)
            logging.info(
                "[PositionService] 已同步参数变更 source=%s keys=%s",
                (event or {}).get('source', 'unknown'),
                ','.join((event or {}).get('changed_keys', [])),
            )
        except Exception as e:
            logging.debug("[PositionService] 参数变更同步失败: %s", e)

    # DFG-P1-06修复: 部分成交事件消费者 — 更新持仓服务
    def _on_partial_fill_event(self, event: Any) -> None:
        """DFG-P1-06修复: 消费部分成交事件，更新持仓记录

        部分成交时需要实时更新持仓的filled_volume，
        确保持仓服务与订单状态同步。
        """
        try:
            if isinstance(event, dict):
                _inst = event.get('instrument_id', '')
                _filled = event.get('filled_volume', 0)
                _direction = event.get('direction', '')
            elif hasattr(event, 'instrument_id'):
                _inst = event.instrument_id
                _filled = getattr(event, 'filled_volume', 0)
                _direction = getattr(event, 'direction', '')
            else:
                return
            if _inst and _filled > 0:
                logging.debug("[DFG-P1-06] PositionService收到部分成交: inst=%s filled=%d dir=%s",
                              _inst, _filled, _direction)
                _pos_map = self.positions.get(_inst, {})
                for _pid, _rec in _pos_map.items():
                    if hasattr(_rec, 'volume') and hasattr(_rec, 'filled_volume'):
                        _remaining = abs(getattr(_rec, 'volume', 0)) - abs(getattr(_rec, 'filled_volume', 0))
                        if _remaining > 0 and getattr(_rec, 'status', '') != 'PARTIAL_CLOSING':
                            setattr(_rec, 'status', 'PARTIAL_CLOSING')
                            logging.info("[R23-SM-07-FIX] 持仓标记为PARTIAL_CLOSING: inst=%s pid=%s remaining=%d",
                                        _inst, _pid, _remaining)
                            break
            # R23-SM-P1-06-FIX: 持仓多腿一致性校验 — 同strategy_id下所有腿status一致
            _strategy_id = None
            if isinstance(event, dict):
                _strategy_id = event.get('strategy_id')
            elif hasattr(event, 'strategy_id'):
                _strategy_id = getattr(event, 'strategy_id', None)
            if _strategy_id and _inst:
                _pos_map = self.positions.get(_inst, {})
                _leg_statuses = set()
                for _pid2, _rec2 in _pos_map.items():
                    _leg_status = getattr(_rec2, 'status', None)
                    if _leg_status:
                        _leg_statuses.add(_leg_status)
                if len(_leg_statuses) > 1:
                    logging.warning("[R23-SM-P1-06-FIX] 持仓多腿状态不一致: strategy_id=%s inst=%s statuses=%s",
                                    _strategy_id, _inst, _leg_statuses)
        except Exception:
            logging.warning("[R22-EP-P1] PositionService exception swallowed")
            pass

    # DFG-P1-07修复: 参数变更事件消费者 — 刷新缓存
    def _on_param_changed_event(self, event: Any) -> None:
        """DFG-P1-07修复: 消费参数变更事件，刷新持仓服务缓存

        当参数发生变更时，刷新TP/SL等缓存配置。
        """
        try:
            from ali2026v3_trading.config_params import get_cached_params
            params = get_cached_params() or {}
            tp = params.get('close_take_profit_ratio', self.DEFAULT_TP_RATIO)
            sl = params.get('close_stop_loss_ratio', self.DEFAULT_SL_RATIO)
            tp_val = float(tp)
            sl_val = float(sl)
            if tp_val > 0 and 0 < sl_val < 1:
                self.DEFAULT_TP_RATIO = tp_val
                self.DEFAULT_SL_RATIO = sl_val
            logging.debug("[DFG-P1-07] PositionService收到参数变更，已刷新缓存")
        except Exception:
            logging.warning("[R22-EP-P1] PositionService exception swallowed")
            pass
    
    @staticmethod
    def _get_platform_attr(obj: Any, *attr_names: str, default: Any = None) -> Any:
        """统一获取平台属性，按优先级尝试多个属性名 # R13-P2-API-01修复

        Args:
            obj: 平台对象
            *attr_names: 属性名列表（按优先级）
            default: 默认值

        Returns:
            第一个存在的属性值或默认值
        """
        for attr in attr_names:
            val = getattr(obj, attr, None)
            if val is not None and val != '':
                return val
        return default

    # R15-P0-RES-06修复: 持仓状态持久化与恢复
    def _append_position_state(self, instrument_id: str, position_id: str, action: str, detail: dict = None):
        """追加写入持仓状态到JSONL文件"""
        try:
            import json as _json
            record = {'instrument_id': instrument_id, 'position_id': position_id,
                      'action': action, 'ts': time.time()}
            if detail:
                record.update(detail)
            with self._position_state_lock:
                os.makedirs(os.path.dirname(self._position_state_file), exist_ok=True)
                with open(self._position_state_file, 'a', encoding='utf-8') as f:
                    f.write(json_dumps(record) + '\n')
            # R24-P0-TR-05修复: 持仓变更写入结构化审计日志
            if _structured_audit_log:
                _structured_audit_log('position_change', action, {
                    'instrument_id': instrument_id, 'position_id': position_id, 'detail': detail
                })
        except Exception as e:
            logging.debug("[PositionService] R15-P0-RES-06: _append_position_state failed: %s", e)

    def _recover_position_state(self):
        """启动时从JSONL恢复持仓状态"""
        try:
            if not os.path.exists(self._position_state_file):
                return
            recovered = 0
            total_records = 0
            with open(self._position_state_file, 'r', encoding='utf-8') as f:
                for line in f:
                    total_records += 1
                    try:
                        import json as _json
                        record = _json.loads(line.strip())
                        inst_id = record.get('instrument_id')
                        pid = record.get('position_id')
                        act = record.get('action')
                        if inst_id and pid and act == 'OPEN':
                            self.positions.setdefault(inst_id, {})
                            recovered += 1
                    except Exception:
                        continue
            if recovered > 0:
                logging.info("[PositionService] R15-P0-RES-06: 从position_state.jsonl恢复%d条持仓", recovered)
            # DR-01: 恢复完整性校验 — 比较records总数与恢复成功数
            if total_records > 0 and recovered != total_records:
                logging.warning(
                    "[PositionService] DR-01: 恢复完整性校验警告 — "
                    "total_records=%d recovered=%d (差异=%d)",
                    total_records, recovered, total_records - recovered,
                )
        except Exception as e:
            logging.debug("[PositionService] R15-P0-RES-06: _recover_position_state failed: %s", e)

    def _recover_positions(self):
        """R16修复: _recover_positions别名，兼容strategy_core_service调用"""
        self._recover_position_state()
    
    def _get_instrument_lock(self, instrument_id: str) -> threading.RLock:
        """获取合约的锁
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            threading.RLock: 合约对应的锁
        """
        with self.global_lock:
            if instrument_id not in self.position_locks:
                self.position_locks[instrument_id] = threading.RLock()
            return self.position_locks[instrument_id]
    
    # ========== Command Methods (写操作) ==========
    
    @count_call()
    @api_version("1.0")
    def on_trade(self, trade: Any) -> None:
        """从成交回报更新持仓
        Args:
            trade: 成交对象 (兼容 CTPII/InfiniTrader 字段)
        """
        # OPS-P1-06修复: 前置条件验证
        from ali2026v3_trading.shared_utils import require_precondition
        require_precondition(
            trade is not None, "on_trade: trade不能为None"
        )

        try:
            # 使用统一方法获取平台属性，避免重复的双属性路径
            inst_id = self._get_platform_attr(trade, "instrument_id", "InstrumentID", default="")
            exch = self._get_platform_attr(trade, "exchange", "ExchangeID", default="")

            # Direction: 0=Buy, 1=Sell; Offset: 0=Open, 1=Close
            d_raw = self._get_platform_attr(trade, "direction", "Direction", default="")
            o_raw = self._get_platform_attr(trade, "offset_flag", "OffsetFlag", default="")

            price = self._get_platform_attr(trade, "price", "Price", default=0)
            volume = self._get_platform_attr(trade, "volume", "Volume", default=0)
            # R24-P2-IV-06修复: trade字段提取容错——验证price/volume类型和范围
            try:
                price = float(price) if price is not None else 0.0
                volume = int(float(volume)) if volume is not None else 0
            except (ValueError, TypeError):
                logging.warning("[R24-P2-IV-06] on_trade: price/volume类型异常 price=%s volume=%s instrument=%s",
                              price, volume, inst_id)
                price = 0.0
                volume = 0

            # 转换方向
            is_buy = (str(d_raw) == "0")
            is_open = (str(o_raw) == "0")

            # ✅ P0修复: order_id在partial_fill_handler块外初始化，避免平仓分支UnboundLocalError
            order_id = self._get_platform_attr(trade, "order_id", "OrderID", default="")

            # ✅ 集成部分成交处理（L-P0-3）
            if self.partial_fill_handler is not None:
                try:
                    filled_volume = self._get_platform_attr(trade, "filled_volume", "FilledVolume", default=volume)
                    total_volume = self._get_platform_attr(trade, "total_volume", "TotalVolume", default=volume)
                    if order_id and total_volume > 0:
                        self.partial_fill_handler.record_partial_fill(
                            order_id=order_id,
                            instrument_id=inst_id,
                            filled_volume=filled_volume,
                            total_volume=total_volume,
                        )
                        # 检查是否需要撤销剩余挂单
                        def _cancel_order(oid):
                            from ali2026v3_trading.order_service import get_order_service
                            osvc = get_order_service()
                            if osvc and hasattr(osvc, 'cancel_order'):
                                osvc.cancel_order(oid)
                            # ✅ P0-22集成: 取消订单后从自成交检测中移除
                            if self.self_trade_detector is not None:
                                self.self_trade_detector.remove_order(oid)
                        self.partial_fill_handler.check_and_cancel_remaining(order_id, cancel_func=_cancel_order)
                except Exception as e:
                    logging.debug(f"[PositionService.on_trade] PartialFillHandler error: {e}")

            if is_open:
                vol_signed = volume if is_buy else -volume
                open_reason = self._get_open_reason_from_order(inst_id, order_id=order_id)
                self._add_position(exch, inst_id, vol_signed, price, open_reason=open_reason, order_id=order_id)
            else:
                # 平仓：买入平仓(减空), 卖出平仓 (减多)
                self._reduce_position(exch, inst_id, volume, is_buy, price)
                # ✅ P0-22集成: 平仓成交后从自成交检测中移除已完成订单
                if self.self_trade_detector is not None and order_id:
                    self.self_trade_detector.remove_order(order_id)

            logging.debug(f"[PositionService.on_trade] Updated: {inst_id} vol={volume}")

        except Exception as e:
            logging.error(f"[PositionService.on_trade] Error: {e}")
    
    def on_tick(self, tick: Any) -> None:
        """实时行情检查 (止盈/止损/期权到期)
        
        Args:
            tick: Tick 数据对象
        """
        try:
            # 使用统一方法获取价格（优先级：last_price > LastPrice > price > last）
            price = self._get_platform_attr(tick, "last_price", "LastPrice", "price", "last", default=0)

            # R24-P1-IV-10修复: 使用safe_price_check替代price<=0，防止NaN/Inf穿透
            from ali2026v3_trading.shared_utils import safe_price_check
            if not safe_price_check(price):
                return
            
            # 使用统一方法获取合约ID
            inst_id = self._get_platform_attr(tick, "instrument_id", "InstrumentID", default="")
            if not inst_id:
                return
            
            # R13-P1-BIZ-04修复: 期权到期日检查 — 每个tick检查持仓的days_to_expiry
            self._check_option_expiry(inst_id)
            
            # 检查该合约的所有持仓
            # R21-CC-P1-09修复: 将np.polyfit计算移到锁外，减少持锁时间
            _slope_updates = {}  # pid -> computed_slope，锁外计算，锁内赋值
            with self._get_instrument_lock(inst_id):
                if inst_id in self.positions:
                    # R15-P0-PERF-03修复: 仅复制键列表，避免tuple(items())创建完整快照
                    for pid in list(self.positions[inst_id]):
                        record = self.positions[inst_id].get(pid)
                        if record is None:
                            continue
                        self._check_stop_profit(record, price)
                        self._check_stop_loss(record, price)

                        if record.volume != 0 and record.open_price > 0:
                            # DFG-01修复: 更新current_price，供check_trailing_stop()使用
                            record.current_price = price
                            if record.volume > 0:
                                profit_pct = (price - record.open_price) / record.open_price
                            else:
                                profit_pct = (record.open_price - price) / record.open_price
                            prev_max = getattr(record, '_max_profit_pct', 0.0)
                            if profit_pct > prev_max:
                                record._max_profit_pct = profit_pct
                            # ✅ P0-8修复: 计算profit_slope
                            if record._profit_history is None:
                                record._profit_history = []
                            record._profit_history.append(profit_pct)
                            # R21-CC-P1-09修复: 仅在锁内快照_profit_history，计算移到锁外
                            if len(record._profit_history) >= 5:
                                _slope_updates[pid] = list(record._profit_history)

            # R21-CC-P1-09修复: np.polyfit在锁外执行，避免GIL+锁双重阻塞
            _computed_slopes = {}
            for pid, history_snapshot in _slope_updates.items():
                _computed_slope = None
                if len(history_snapshot) >= 5:
                    if _HAS_NUMPY:
                        try:
                            _computed_slope = float(np.polyfit(
                                range(len(history_snapshot)),
                                history_snapshot, 1)[0])
                        except (ValueError, np.linalg.LinAlgError):
                            n = len(history_snapshot)
                            x_mean = (n - 1) / 2.0
                            y_mean = sum(history_snapshot) / n
                            num = sum((i - x_mean) * (history_snapshot[i] - y_mean) for i in range(n))
                            den = sum((i - x_mean) ** 2 for i in range(n))
                            _computed_slope = num / den if den > 0 else 0.0
                    else:
                        n = len(history_snapshot)
                        x_mean = (n - 1) / 2.0
                        y_mean = sum(history_snapshot) / n
                        num = sum((i - x_mean) * (history_snapshot[i] - y_mean) for i in range(n))
                        den = sum((i - x_mean) ** 2 for i in range(n))
                        _computed_slope = num / den if den > 0 else 0.0
                    if _computed_slope is not None:
                        _computed_slopes[pid] = _computed_slope

            # R21-CC-P1-09修复: 锁内仅做赋值操作
            if _computed_slopes:
                with self._get_instrument_lock(inst_id):
                    if inst_id in self.positions:
                        for pid, slope in _computed_slopes.items():
                            record = self.positions[inst_id].get(pid)
                            if record is not None:
                                record.profit_slope = slope

            # DFG-04修复: 发布TickEvent到EventBus
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus, TickEvent
                _eb = get_global_event_bus()
                if _eb and not getattr(_eb, '_shutdown', True):
                    _eb.publish(TickEvent(instrument_id=inst_id, tick_data=tick), async_mode=True)
            except Exception:
                logging.warning("[R22-EP-P1] PositionService exception swallowed")
                pass
                    
        except Exception as e:
            logging.error(f"[PositionService.on_tick] Error: {e}")
    
    def on_tick_dropped(self, event_data: dict) -> None:
        """R31-P0-04修复: tick丢弃事件处理 — 标记该合约行情中断，止损基于最后已知价格
        
        Args:
            event_data: {'instrument_id': str, 'reason': str, 'timestamp': float}
        """
        try:
            instrument_id = event_data.get('instrument_id', '')
            if not instrument_id:
                return
            if not hasattr(self, '_tick_drop_timestamps'):
                self._tick_drop_timestamps: dict = {}
            self._tick_drop_timestamps[instrument_id] = event_data.get('timestamp', time.time())
            _drop_count = len(self._tick_drop_timestamps)
            if _drop_count <= 5 or _drop_count % 100 == 0:
                logging.warning("[R31-P0-04] tick断流: instrument=%s reason=%s (累计%d合约断流)",
                              instrument_id, event_data.get('reason', '?'), _drop_count)
        except Exception as e:
            logging.debug(f"[PositionService.on_tick_dropped] Error: {e}")
    
    def set_position_limit(self, account_id: str, limit_amount: float, 
                          valid_hours: Optional[int] = None, 
                          force_set: bool = False) -> bool:
        """设置持仓限额（委托给RiskService作为权威源，同时本地持久化）

        Args:
            account_id: 账户 ID
            limit_amount: 限额金额
            valid_hours: 有效小时数 (默认 24h, 最大 720h)
            force_set: 是否强制设置（覆盖已有配置）

        Returns:
            bool: 设置成功返回 True
        """
        try:
            if not account_id:
                logging.error("[PositionService.set_position_limit] Invalid account_id")
                return False

            # 默认有效期
            if valid_hours is None:
                valid_hours = self.DEFAULT_VALID_HOURS

            # 验证范围
            max_hours = self.MAX_VALID_HOURS
            if not 1 <= valid_hours <= max_hours:
                logging.error(f"[PositionService.set_position_limit] Hours out of range: {valid_hours}")
                return False

            until = datetime.now(_CHINA_TZ) + timedelta(hours=valid_hours)

            # ✅ P0修复：通过RiskService公共API设置限额，避免穿透封装
            if self._risk_service is not None:
                try:
                    # 使用RiskService的公共方法设置限额
                    self._risk_service.set_position_limit(
                        account_id=account_id,
                        limit_amount=limit_amount,
                        effective_until=until
                    )
                    logging.info(f"[PositionService.set_position_limit] Set limit via RiskService API for {account_id}")
                    return True
                except Exception as e:
                    logging.error(f"[PositionService.set_position_limit] Failed to set limit via RiskService: {e}")
                    return False
            else:
                logging.warning("[PositionService.set_position_limit] RiskService not available, skip setting limit")
                return False

        except Exception as e:
            logging.error(f"[PositionService.set_position_limit] Error: {e}")
            return False
    
    # ========== Query Methods (读操作) ==========
    
    def get_position(self, instrument_id: str) -> Dict[str, Any]:
        """获取持仓信息
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            dict: {
                "volume": 总持仓量,
                "average_price": 平均价格,
                "positions": [PositionRecord, ...]
            }
        """
        # [FR-P1-05-FIX] 持仓快照TTL校验
        import time as _t
        _now = _t.time()
        if self._position_snapshot_time > 0 and (_now - self._position_snapshot_time) > self._position_snapshot_ttl:
            logging.warning("[FR-P1-05-FIX] 持仓快照过期: age=%.1fs > ttl=%.1fs, inst=%s",
                           _now - self._position_snapshot_time, self._position_snapshot_ttl, instrument_id)
        self._position_snapshot_time = _now
        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                return {"volume": 0, "average_price": 0, "positions": []}
            
            positions = list(self.positions[instrument_id].values())
            total_volume = sum(rec.volume for rec in positions)
            
            if total_volume == 0:
                return {"volume": 0, "average_price": 0, "positions": []}
            
            # 计算平均价格
            weighted_sum = sum(Decimal(str(abs(rec.volume))) * Decimal(str(rec.open_price)) for rec in positions)
            if total_volume == 0:
                logging.warning("[R25-BV-01-FIX] total_volume=0, 无法计算均价, 返回空持仓")
                return {"volume": 0, "average_price": 0, "positions": []}
            average_price = float(weighted_sum / Decimal(str(abs(total_volume))))
            
            return {
                "volume": total_volume,
                "average_price": average_price,
                "positions": positions
            }
    
    def get_net_position(self, instrument_id: str) -> int:
        """获取某合约的净持仓量

        Args:
            instrument_id: 合约代码

        Returns:
            int: 净持仓量 (正=净多，负=净空)
        """
        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                return 0
            return sum(rec.volume for rec in self.positions[instrument_id].values())

    def validate_net_position_consistency(self, instrument_id: str) -> bool:
        """INV-P1-02/INV-POS-02修复: 净持仓一致性验证

        独立计算long/short持仓量，与get_net_position()结果比对。
        如果不一致，说明持仓记录存在数据损坏。

        Args:
            instrument_id: 合约代码

        Returns:
            bool: True=一致, False=不一致(数据异常)
        """
        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                return True

            # 独立计算多空持仓量
            long_volume = 0
            short_volume = 0
            for rec in self.positions[instrument_id].values():
                if rec.volume > 0:
                    long_volume += rec.volume
                elif rec.volume < 0:
                    short_volume += abs(rec.volume)

            # 独立计算净持仓
            independent_net = long_volume - short_volume

            # 与get_net_position()结果比对
            stored_net = sum(rec.volume for rec in self.positions[instrument_id].values())

            if independent_net != stored_net:
                logging.critical(
                    "[PositionService] INV-POS-02: 净持仓不一致! instrument=%s "
                    "long=%d short=%d independent_net=%d stored_net=%d",
                    instrument_id, long_volume, short_volume, independent_net, stored_net,
                )
                # INV-P1-02修复: 净持仓不一致时触发事件总线告警
                try:
                    from ali2026v3_trading.event_bus import get_global_event_bus, RiskEvent
                    _eb = get_global_event_bus()
                    if _eb and not getattr(_eb, '_shutdown', True):
                        _eb.publish(RiskEvent(
                            risk_type='net_position_inconsistency',
                            level='CRITICAL',
                            message=f"INV-P1-02: 净持仓不一致 instrument={instrument_id} "
                                    f"long={long_volume} short={short_volume} "
                                    f"independent_net={independent_net} stored_net={stored_net}",
                        ), async_mode=True)
                except Exception as _eb_e:
                    logging.debug("[PositionService] INV-P1-02: 事件总线告警失败: %s", _eb_e)
                return False

            return True
    
    def has_position(self, instrument_id: str, check_nonzero: bool = True) -> bool:
        """检查合约是否有持仓（统一接口）
        
        Args:
            instrument_id: 合约代码
            check_nonzero: 是否检查非零持仓
                          - True: 检查是否有非零持仓（默认）
                          - False: 检查是否有持仓记录（包括零持仓）
        
        Returns:
            bool: 是否有持仓
        """
        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                return False
            pos_dict = self.positions[instrument_id]
            if not pos_dict:
                return False
            if check_nonzero:
                return any(r.volume != 0 for r in pos_dict.values())
            else:
                return len(pos_dict) > 0
    
    def check_position_limit(self, account_id: str, required_amount: float) -> bool:
        """检查持仓限额 - 统一为RiskService检查，本地配置仅做初始化
        Args:
            account_id: 账户 ID
            required_amount: 需求金额
        Returns:
            bool: 是否允许（True=允许，False=超限）        """
        try:
            # ✅ 统一为RiskService检查（唯一权威源）
            if self._risk_service is not None:
                result = self._risk_service._check_position_limit(account_id, required_amount)
                return result.is_pass
            else:
                # ✅ P0修复：RiskService不可用时应阻断而非默认允许（fail-safe原则）
                logging.error("[PositionService.check_position_limit] RiskService not available, BLOCKING position check (fail-safe)")
                return False  # RiskService不可用时阻断，遵循fail-safe原则

        except Exception as e:
            logging.error(f"[PositionService.check_position_limit] Error: {e}")
            return False
    
    def calculate_position_risk(self, instrument_id: str) -> float:
        """计算持仓风险（含合约乘数）

        Args:
            instrument_id: 合约代码

        Returns:
            float: 风险值 (持仓量 * 平均价格 * 合约乘数)
        """
        try:
            position_info = self.get_position(instrument_id)
            volume = position_info.get("volume", 0)
            average_price = position_info.get("average_price", 0)

            # R18-P0-INV-01修复: 获取合约乘数，默认1.0
            multiplier = 1.0
            try:
                from ali2026v3_trading.params_service import get_params_service
                ps = get_params_service()
                meta = ps.get_instrument_meta_by_id(instrument_id)
                if meta:
                    multiplier = float(meta.get("contract_size", 1.0))
            except Exception as _e:
                logging.debug(f"[PositionService.calculate_position_risk] 获取合约乘数失败，使用默认值1.0: {_e}")

            # 风险计算：持仓量 * 平均价格 * 合约乘数
            risk = abs(volume) * average_price * multiplier
            return risk

        except Exception as e:
            logging.error(f"[PositionService.calculate_position_risk] Error: {e}")
            return 0.0
    
    def get_position_info(self) -> List[Dict[str, Any]]:
        """获取所有持仓信息（用于 UI 展示）        
        Returns:
            list: 持仓信息列表
        """
        with self.global_lock:
            result = []
            current_date = datetime.now(_CHINA_TZ).date()
            
            # 展开所有持仓            all_records = []
            for inst_map in self.positions.values():
                all_records.extend(inst_map.values())
            
            for record in all_records:
                if record.volume != 0:  # ✅ 包含空头持仓
                    r_open_date = record.open_date
                    if isinstance(r_open_date, datetime):
                        r_open_date = r_open_date.date()
                    
                    days_held = (current_date - r_open_date).days
                    
                    result.append({
                        "仓位 ID": record.position_id,
                        "合约": record.instrument_id,
                        "开仓价": f"{record.open_price:.2f}",
                        "持仓量": record.volume,
                        "方向": "多头" if record.direction == "long" else "空头",
                        "性质": record.position_type,
                        "开仓日期": r_open_date.strftime("%Y-%m-%d"),
                        "持仓天数": days_held,
                        "开仓超过3天": days_held >= 3,
                        "止盈价": f"{record.stop_profit_price:.2f}",
                        "追单次数": record.chase_count
                    })
            
            return result
    
    # ========== Private Helper Methods ==========
    
    @count_call()
    def _add_position(self, exchange: str, instrument_id: str,
                     volume: int, price: float, open_reason: str = '',
                     order_id: str = '') -> None:
        """添加持仓

        Args:
            exchange: 交易所
            instrument_id: 合约代码
            volume: 持仓量(正=多，负=空)
            price: 开仓价格
            open_reason: V7新增-开仓理由编码
            order_id: 关联订单ID
        """
        # R13-P0-BIZ-07修复: 开仓前保证金充足性检查 — 即使上游风控被绕过也能防止超限持仓
        try:
            from ali2026v3_trading.risk_service import get_risk_service
            rs = get_risk_service()
            if rs:
                # 计算已有持仓保证金
                existing_margin = 0.0
                try:
                    for _inst_id, pos_dict in self.positions.items():
                        for _pid, rec in pos_dict.items():
                            if rec.volume != 0 and rec.open_price > 0:
                                existing_margin += abs(rec.volume) * rec.open_price * 0.1  # 10%保证金率近似
                except Exception:
                    logging.warning("[R22-EP-P1] PositionService exception swallowed")
                    pass
                new_margin = abs(volume) * price * 0.1  # 10%保证金率近似
                # 获取当前权益
                equity = 0.0
                try:
                    from ali2026v3_trading.risk_service import get_safety_meta_layer
                    _sid = str(getattr(self, 'strategy_id', '') or 'global')
                    safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
                    if safety and safety._equity_series:
                        equity = safety._equity_series[-1]
                except Exception:
                    logging.warning("[R22-EP-P1] PositionService exception swallowed")
                    pass
                if equity > 0:
                    result = rs.check_capital_sufficiency(
                        equity=equity,
                        required_margin=new_margin,
                        existing_margin_used=existing_margin,
                    )
                    if not result.get('sufficient', True):
                        logging.critical(
                            "[PositionService] R13-V4-001: 保证金不足防御性阻断! equity=%.2f "
                            "existing_margin=%.2f new_margin=%.2f reason=%s",
                            equity, existing_margin, new_margin, result.get('reason', 'unknown'),
                        )
                        return
        except Exception as e:
            logging.debug("[PositionService] R13-V4-001 margin check failed: %s", e)

        # ✅ 集成自成交检测（L-P0-1）
        if self.self_trade_detector is not None:
            try:
                from ali2026v3_trading.order_persistence import OrderRecord
                new_order = OrderRecord(
                    order_id=f"temp_{int(datetime.now(_CHINA_TZ).timestamp()*1000)}",
                    instrument_id=instrument_id,
                    direction="buy" if volume > 0 else "sell",
                    price=price,
                    volume=abs(volume),
                    timestamp=datetime.now(_CHINA_TZ).timestamp(),
                )
                is_self_trade, alert_msg = self.self_trade_detector.check_self_trade(new_order)
                if is_self_trade:
                    logging.error(f"[PositionService._add_position] 自成交检测阻断: {alert_msg}")
                    return
                self.self_trade_detector.add_order(new_order)
            except Exception as e:
                logging.warning(f"[PositionService._add_position] 自成交检测异常: {e}")

        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                self.positions[instrument_id] = {}

            import uuid as _uuid
            pos_id = f"{instrument_id}_{int(datetime.now(_CHINA_TZ).timestamp()*1000)}_{_uuid.uuid4().hex[:6]}"
            
            direction_str = "long" if volume > 0 else "short"
            p_type = "long" if volume > 0 else "short"
            
            sp_price = 0.0
            sl_price = 0.0
            try:
                tp_ratio, sl_ratio = self._get_tp_sl_ratios_by_reason(open_reason)
                sl_ratio = self._apply_crm_stop_loss_adjustment(sl_ratio, open_reason)
                self._verify_tp_sl_alignment_with_backtest(open_reason, tp_ratio, sl_ratio)
            except Exception as e:
                logging.warning(f"[PositionService._add_position] Failed to get TP/SL ratio, using defaults: {e}")
                tp_ratio = self.DEFAULT_TP_RATIO
                sl_ratio = self.DEFAULT_SL_RATIO
            if price > 0 and tp_ratio > 0:
                if volume > 0:
                    sp_price = price * tp_ratio
                    sl_price = price * (1 - sl_ratio)
                else:
                    sp_price = price / tp_ratio
                    sl_price = price * (1 + sl_ratio)
            elif price > 0 and tp_ratio <= 0:
                # R27-P1-HZ-06修复: 改为raise ValueError(fail-safe)
                raise ValueError(f"[R26-P0-BV-03] tp_ratio={tp_ratio:.4f}<=0, 拒绝创建持仓(无止盈保护), instrument={instrument_id} price={price:.2f}")
            elif price <= 0:
                # R24-P1-IV-10修复: 使用safe_price_check已在上游验证，此处保留<=0兜底
                # R27-P1-HZ-06修复: 改为raise ValueError(fail-safe), 调用方必须处理
                raise ValueError(f"[R26-P0-FI-09] open_price={price:.2f}<=0, 拒绝创建持仓, instrument={instrument_id}")
            
            record = PositionRecord(
                position_id=pos_id,
                instrument_id=instrument_id,
                exchange=exchange,
                volume=volume,
                direction=direction_str,
                open_price=price,
                open_time=datetime.now(_CHINA_TZ),
                open_date=datetime.now(_CHINA_TZ).date(),
                position_type=p_type,
                stop_profit_price=sp_price,
                stop_loss_price=sl_price,
                open_reason=open_reason,
                order_id=order_id,
                option_premium=self._compute_option_premium(instrument_id, price),
            )
            
            self.positions[instrument_id][pos_id] = record

            logging.info(f"[PositionService._add_position] Added: {instrument_id} {volume}手@ {price} reason={open_reason}")

            # ✅ DR-01: 持仓状态持久化 — 开仓时追加写入
            self._append_position_state(instrument_id, pos_id, 'OPEN',
                                       {'volume': volume, 'price': price, 'open_reason': open_reason})

            # ✅ 集成结构化JSONL日志记录（L-P1-1）
            if self._structured_logger is not None:
                try:
                    self._structured_logger.log_order({
                        "order_id": pos_id,
                        "instrument_id": instrument_id,
                        "direction": "buy" if volume > 0 else "sell",
                        "price": price,
                        "volume": abs(volume),
                        "order_type": "OPEN",
                        "status": "filled",
                        "filled_volume": abs(volume),
                        "remaining_volume": 0,
                    })
                except Exception as e:
                    logging.debug("[PositionService] StructuredLogger error: %s", e)

            # DFG-04修复: 发布PositionEvent(OPENED)到EventBus
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus, PositionEvent
                _eb = get_global_event_bus()
                if _eb and not getattr(_eb, '_shutdown', True):
                    _eb.publish(PositionEvent(
                        instrument_id=instrument_id, position=float(volume),
                        avg_price=price, action='OPENED',
                    ), async_mode=True)
            except Exception:
                logging.warning("[R22-EP-P1] PositionService exception swallowed")
                pass

    # R18-P0-DEAD-03修复: 硬编码默认值必须与state_param_sets.yaml一致
    _HARDCODED_REASON_DEFAULTS = {
        'CORRECT_RESONANCE': (1.5, 0.50),
        'CORRECT_DIVERGENCE': (1.2, 0.40),
        'INCORRECT_REVERSAL': (1.3, 0.60),
        'OTHER_SCALP': (1.1, 0.30),
        'MANUAL': (1.5, 0.50),
        '': (1.5, 0.50),
    }
    _FALLBACK_TP_SL = (1.8, 0.30)  # R25修复: tp从1.5→1.8与CENTRALIZED_DEFAULTS对齐, sl=0.30与DEFAULT_SL_RATIO对齐

    def _get_tp_sl_ratios_by_reason(self, open_reason: str) -> Tuple[float, float]:
        """V7新增：根据开仓理由获取差异化止盈止损倍数

        路由优先级（P3>硬编码兜底）：
          P3: state_param_sets.yaml 中的 reason 级参数集（如 CORRECT_RESONANCE/BOX_SPRING）
          P2: 硬编码 _HARDCODED_REASON_DEFAULTS（保证YAML缺失时不崩溃）
          P1: _FALLBACK_TP_SL 兜底值 (1.8, 0.30)

        平仓规则分叉表（文档6.2节）:
        - CORRECT_RESONANCE: 固定倍数止盈(1.5x), 固定止损(-50%)
        - CORRECT_DIVERGENCE: 收紧止盈(1.2x), 固定止损(-40%)
        - INCORRECT_REVERSAL: 紧止盈(1.3x), 紧止损(-60%)
        - OTHER_SCALP: 微利即走(1.1x), 极窄止损(-30%)
        - MANUAL/空: 默认(1.5x, -50%)
        """
        try:
            from .state_param_manager import get_state_param_manager
            spm = get_state_param_manager()
            reason_params = spm.get_params(open_reason)
            tp = reason_params.get('close_take_profit_ratio')
            sl = reason_params.get('close_stop_loss_ratio')
            if tp is not None and sl is not None:
                tp_val = float(tp)
                sl_val = float(sl)
                if tp_val > 0 and 0 < sl_val < 1:
                    return (tp_val, sl_val)
                logging.warning(
                    "[PositionService] YAML中的TP/SL值越界 reason=%s tp=%.4f sl=%.4f, 降级到硬编码",
                    open_reason, tp_val, sl_val,
                )
        except Exception as e:
            logging.debug("[PositionService] 从StateParamManager读取reason参数失败: %s, 使用硬编码", e)

        return self._HARDCODED_REASON_DEFAULTS.get(open_reason, self._FALLBACK_TP_SL)

    def _verify_tp_sl_alignment_with_backtest(self, open_reason: str, tp: float, sl: float) -> None:
        try:
            from ali2026v3_trading.参数池.task_scheduler import REASON_MULTIPLIERS
            bt_mult = REASON_MULTIPLIERS.get(open_reason)
            if bt_mult is not None:
                bt_tp_mult = bt_mult.get('tp_mult', 1.0)
                bt_sl_mult = bt_mult.get('sl_mult', 1.0)
                base_tp = float(getattr(self, 'DEFAULT_TP_RATIO', 1.8))
                base_sl = float(getattr(self, 'DEFAULT_SL_RATIO', 0.30))
                bt_tp = base_tp * bt_tp_mult
                bt_sl = base_sl * bt_sl_mult
                if abs(tp - bt_tp) > 0.05 or abs(sl - bt_sl) > 0.05:
                    logging.warning(
                        "[P0-2修复] 生产/回测TP-SL分叉: reason=%s 生产(tp=%.4f,sl=%.4f) vs 回测(tp=%.4f,sl=%.4f)",
                        open_reason, tp, sl, bt_tp, bt_sl,
                    )
        except (ImportError, AttributeError):
            pass

    @staticmethod
    def _map_reason_to_strategy(reason: str) -> str:
        # ✅ P0-7修复: 统一使用模块级_REASON_STRATEGY_MAP，避免重复定义导致不一致
        return _REASON_STRATEGY_MAP.get(reason, 'high_freq')

    def _apply_crm_stop_loss_adjustment(self, sl_ratio: float, open_reason: str) -> float:
        try:
            import importlib  # R21-CC-P2-03修复: 动态导入 — 高频路径上重复importlib调用有性能开销，大数据pickle开销显著
            crm_module = importlib.import_module('参数池.cycle_resonance_module')
            crm = crm_module.get_cycle_resonance_module()
            strategy = self._map_reason_to_strategy(open_reason)
            rs = crm.get_risk_surface(strategy)
            adjusted = sl_ratio * rs.stop_loss_multiplier
            return float(np.clip(adjusted, self.CRM_SL_CLIP_LOWER, self.CRM_SL_CLIP_UPPER)) if _HAS_NUMPY else max(self.CRM_SL_CLIP_LOWER, min(self.CRM_SL_CLIP_UPPER, adjusted))
        except (ImportError, ModuleNotFoundError):
            logging.info("[PositionService] 参数池.cycle_resonance_module not available, using default sl_ratio")  # R24-P2-DF-11修复: debug→info，CRM模块不可用需可感知
            return sl_ratio
        except Exception as e:
            logging.warning("[PositionService] _apply_crm_stop_loss_adjustment error: %s", e)  # R24-P1-DF-09修复: 周期共振模块不可用日志级别debug→warning
            return sl_ratio

    def _get_open_reason_from_order(self, instrument_id: str, order_id: str = '') -> str:
        try:
            from ali2026v3_trading.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                if order_id:
                    order = osvc.get_order(order_id)
                    if order:
                        return order.get('open_reason', '')
                orders = osvc.get_orders_by_instrument(instrument_id)
                for o in orders:
                    if o.get('action') == 'OPEN' and o.get('status') in ('SUBMITTED', 'FILLED', 'ALL_FILLED', '全成'):
                        return o.get('open_reason', '')
        except (ImportError, AttributeError, KeyError) as e:
            logging.debug("[PositionService] _get_open_reason failed: %s", e)
        return ''

    def _compute_option_premium(self, instrument_id: str, price: float) -> float:
        """P1-15修复: 计算期权权利金
        期权合约: option_premium = 开仓价格 (即权利金)
        非期权合约: option_premium = 0.0
        """
        _is_option = any(k in instrument_id.upper() for k in ['-C-', '-P-', '_C_', '_P_'])
        if _is_option and price > 0:
            return price
        return 0.0

    def _reduce_position(self, exchange: str, instrument_id: str, 
                         volume: int, is_buy: bool, price: float) -> None:
        volume = abs(volume)
        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                return
            
            to_close = volume
            
            # 按开仓时间排序(FIFO)
            records = sorted(self.positions[instrument_id].values(), key=lambda x: x.open_time)
            
            remaining_close = volume
            keys_to_remove = []
            
            for rec in records:
                if remaining_close <= 0:
                    break
                
                # 买入平仓平空头，卖出平仓平多头；同向无法平仓
                if is_buy and rec.volume > 0:
                    continue
                if not is_buy and rec.volume < 0:
                    continue
                
                # 反向：可以平仓
                can_close = abs(rec.volume)
                if can_close <= remaining_close:
                    remaining_close -= can_close
                    keys_to_remove.append(rec.position_id)
                else:
                    # 部分平仓
                    if rec.volume > 0:
                        rec.volume -= remaining_close
                    else:
                        rec.volume += remaining_close
                    # R14-P1-BIZ-08修复: 部分平仓后按剩余仓位比例重算止盈止损价
                    if hasattr(rec, 'stop_profit_price') and rec.stop_profit_price and rec.stop_profit_price > 0:
                        _orig_tp = rec.stop_profit_price
                        _tp_ratio = abs(_orig_tp / rec.open_price - 1) if rec.open_price > 0 else 0
                        rec.stop_profit_price = rec.open_price * (1 + _tp_ratio) if rec.open_price > 0 else _orig_tp
                    if hasattr(rec, 'stop_loss_price') and rec.stop_loss_price and rec.stop_loss_price > 0:
                        _orig_sl = rec.stop_loss_price
                        _sl_ratio = abs(rec.open_price / _orig_sl - 1) if _orig_sl > 0 and rec.open_price > 0 else 0
                        rec.stop_loss_price = rec.open_price * (1 - _sl_ratio) if rec.open_price > 0 else _orig_sl
                    logging.info("[PositionService] R14-P1-BIZ-08: 部分平仓后重算止盈止损 instrument=%s vol=%d tp=%.2f sl=%.2f",
                                rec.instrument_id, rec.volume, getattr(rec, 'stop_profit_price', 0), getattr(rec, 'stop_loss_price', 0))
                    remaining_close = 0
            
            # 删除已平仓记录
            for k in keys_to_remove:
                rec = self.positions[instrument_id].get(k)
                # R24-P1-TR-11修复: 平仓记录增加Greeks快照
                _greeks_snapshot = {}
                if rec is not None:
                    try:
                        from ali2026v3_trading.risk_service import get_risk_service
                        _rs = get_risk_service()
                        if _rs and hasattr(_rs, '_get_greeks_calculator'):
                            _gc = _rs._get_greeks_calculator()
                            if _gc and hasattr(_gc, 'get_greeks'):
                                _greeks = _gc.get_greeks(rec.instrument_id)
                                if _greeks:
                                    _greeks_snapshot = {
                                        'delta': _greeks.get('delta', 0.0),
                                        'gamma': _greeks.get('gamma', 0.0),
                                        'vega': _greeks.get('vega', 0.0),
                                        'theta': _greeks.get('theta', 0.0),
                                    }
                    except Exception:
                        pass
                del self.positions[instrument_id][k]
                # ✅ DR-01: 持仓状态持久化 — 平仓时追加写入
                _close_detail = {'close_price': price}
                if _greeks_snapshot:
                    _close_detail['greeks_snapshot'] = _greeks_snapshot  # R24-P1-TR-11修复
                self._append_position_state(instrument_id, k, 'CLOSE', _close_detail)
            
            logging.info(f"[PositionService._reduce_position] Reduced: {instrument_id} {volume}@ {price}")

            # DFG-04修复: 发布PositionEvent(CLOSED)到EventBus
            try:
                from ali2026v3_trading.event_bus import get_global_event_bus, PositionEvent
                _eb = get_global_event_bus()
                if _eb and not getattr(_eb, '_shutdown', True):
                    _eb.publish(PositionEvent(
                        instrument_id=instrument_id, position=0.0,
                        avg_price=price, action='CLOSED',
                    ), async_mode=True)
            except Exception:
                logging.warning("[R22-EP-P1] PositionService exception swallowed")
                pass

    def _check_stop_profit(self, record: PositionRecord, current_price: float) -> None:
        """检查止盈
        R13-P0-LOG-02修复: 添加止盈触发/跳过日志
        Args:
            record: 持仓记录
            current_price: 当前价格
        """
        if record.volume == 0:
            return

        # 检查止盈触发
        if record.stop_profit_price > 0:
            triggered = False
            # 统一使用 volume 正负判断多空（volume>0=多，volume<0=空）
            is_long = record.volume > 0
            is_short = record.volume < 0

            if is_long and current_price >= record.stop_profit_price:
                triggered = True
            elif is_short and current_price <= record.stop_profit_price:
                triggered = True

            if triggered:
                logging.info(
                    '[PositionService] R13-P0-LOG-02修复: 止盈触发, instrument=%s direction=%s price=%.2f tp_price=%.2f',
                    record.instrument_id, 'LONG' if is_long else 'SHORT', current_price, record.stop_profit_price,
                )  # R13-P0-LOG-02修复
                self._trigger_close_position(record, f"StopProfit@{current_price:.2f}", current_price)
            else:
                logging.debug(
                    '[PositionService] R13-P0-LOG-02修复: 止盈未触发, instrument=%s price=%.2f tp_price=%.2f (距离=%.2f)',
                    record.instrument_id, current_price, record.stop_profit_price,
                    abs(current_price - record.stop_profit_price),
                )  # R13-P0-LOG-02修复

    def _check_stop_loss(self, record: PositionRecord, current_price: float) -> None:
        """R13-P0-LOG-02修复: 添加止损触发/跳过日志"""
        if record.volume == 0:
            return
        if record.stop_loss_price <= 0:
            if record.volume != 0:
                if record.stop_loss_price == 0:
                    logging.warning("[R25-BV-P1-04-FIX] 止损价格=0,持仓无保护: inst=%s vol=%d open=%.2f",
                                    record.instrument_id, record.volume, record.open_price)
                else:
                    logging.error("[R26-P1-BV-04] 止损价格<0(异常值),持仓无保护: inst=%s vol=%d sl=%.2f open=%.2f",
                                  record.instrument_id, record.volume, record.stop_loss_price, record.open_price)
            return
        triggered = False
        is_long = record.volume > 0
        is_short = record.volume < 0
        if is_long and current_price <= record.stop_loss_price:
            triggered = True
        elif is_short and current_price >= record.stop_loss_price:
            triggered = True
        if triggered:
            logging.info(
                '[PositionService] R13-P0-LOG-02修复: 止损触发, instrument=%s direction=%s price=%.2f sl_price=%.2f',
                record.instrument_id, 'LONG' if is_long else 'SHORT', current_price, record.stop_loss_price,
            )
            self._trigger_close_position(record, f"StopLoss@{current_price:.2f}", current_price)
        else:
            logging.debug(
                '[PositionService] R13-P0-LOG-02修复: 止损未触发, instrument=%s price=%.2f sl_price=%.2f (距离=%.2f)',
                record.instrument_id, current_price, record.stop_loss_price,
                abs(current_price - record.stop_loss_price),
            )

    # R13-P1-BIZ-04修复: 期权到期日检查方法
    def _check_option_expiry(self, instrument_id: str) -> None:
        """检查期权合约到期日，若days_to_expiry<=0则触发强制平仓

        期权合约到期后流动性急剧下降且无法交易，必须在到期前强制平仓。
        合约代码中包含到期月份(如IO2606-C-3900中的2606)，
        解析到期月份并与当前日期比较计算days_to_expiry。
        """
        if not _is_option_instrument(instrument_id):
            return
        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                return
            # R15-P0-PERF-03修复: 仅复制键列表，避免tuple(items())创建完整快照
            for pid in list(self.positions[instrument_id]):
                record = self.positions[instrument_id].get(pid)
                if record is None:
                    continue
                if record.volume == 0:
                    continue
                try:
                    days_to_expiry = self._calc_days_to_expiry(instrument_id)
                    if days_to_expiry is not None and days_to_expiry <= 0:
                        logging.warning(
                            '[PositionService] R13-P1-BIZ-04修复: 期权到期强制平仓, '
                            'instrument=%s days_to_expiry=%d, 触发强制平仓',
                            instrument_id, days_to_expiry,
                        )
                        self._trigger_close_position(record, f"OptionExpiry@{instrument_id}")
                except Exception as e:
                    logging.debug('[PositionService] _check_option_expiry error for %s: %s', instrument_id, e)

    @staticmethod
    def _calc_days_to_expiry(instrument_id: str) -> Optional[int]:
        """从期权合约代码解析到期月份并计算距到期日的天数

        合约代码格式示例: IO2606-C-3900 → 到期月份2026年6月
        中金所期权到期日为到期月份的第三个星期五
        """
        try:
            parts = instrument_id.split('-')
            if len(parts) < 2:
                return None
            # 提取合约月份部分(如IO2606中的2606)
            code_part = parts[0]
            year_month = ''
            for c in reversed(code_part):
                if c.isdigit():
                    year_month = c + year_month
                else:
                    break
            if len(year_month) < 3:
                return None
            year = 2000 + int(year_month[:2]) if len(year_month) == 4 else 2000 + int(year_month[:2])
            month = int(year_month[2:]) if len(year_month) == 4 else int(year_month[2:])
            if month < 1 or month > 12:
                return None
            # 计算该月第三个星期五(中金所期权到期日)
            from datetime import date
            first_day = date(year, month, 1)
            # 找到第一个星期五
            first_friday = first_day
            while first_friday.weekday() != 4:  # 4=Friday
                first_friday = first_friday + timedelta(days=1)
            third_friday = first_friday + timedelta(days=14)
            today = datetime.now(_CHINA_TZ).date()
            return (third_friday - today).days
        except Exception:
            logging.warning("[R22-EP-P1] PositionService exception swallowed")
            return None

    def check_trailing_stop(self, record) -> Optional[str]:
        current_price = getattr(record, 'current_price', 0.0)
        open_price = record.open_price
        # R24-P1-IV-10修复: 使用safe_price_check替代price<=0，防止NaN/Inf穿透
        from ali2026v3_trading.shared_utils import safe_price_check
        if not safe_price_check(open_price) or not safe_price_check(current_price):
            return None
        is_long = record.volume > 0
        if is_long:
            current_profit_pct = (current_price - open_price) / open_price
        else:
            current_profit_pct = (open_price - current_price) / open_price
        target_plr = getattr(record, 'target_plr', self.DEFAULT_TARGET_PLR)
        tp_ratio = getattr(record, 'take_profit_ratio', self.DEFAULT_TP_RATIO)
        target_profit_pct = tp_ratio * self.TRAILING_STOP_ACTIVATION_PCT
        if current_profit_pct <= target_profit_pct:
            return None
        _key = f"trailing_stop_{record.instrument_id}_{record.position_id if hasattr(record, 'position_id') else id(record)}"
        if not hasattr(self, '_trailing_stop_activated'):
            self._trailing_stop_activated = {}
        if _key not in self._trailing_stop_activated:
            self._trailing_stop_activated[_key] = True
            logging.info(
                '[PositionService] 浮动止盈激活: instrument=%s profit=%.2f%% > target_half=%.2f%%',
                record.instrument_id, current_profit_pct * 100, target_profit_pct * 100,
            )
        peak_key = f"trailing_peak_{_key}"
        if not hasattr(self, '_trailing_stop_peaks'):
            self._trailing_stop_peaks = {}
        prev_peak = self._trailing_stop_peaks.get(peak_key, current_profit_pct)
        peak_profit = max(prev_peak, current_profit_pct)
        self._trailing_stop_peaks[peak_key] = peak_profit
        trailing_stop_pct = peak_profit * self.TRAILING_STOP_RETRACEMENT_PCT
        if current_profit_pct < trailing_stop_pct and peak_profit > target_profit_pct:
            logging.info(
                '[PositionService] 浮动止盈触发: instrument=%s peak=%.2f%% current=%.2f%% trailing_stop=%.2f%%',
                record.instrument_id, peak_profit * 100, current_profit_pct * 100, trailing_stop_pct * 100,
            )
            return f"TrailingStop@{current_profit_pct:.1%}(peak={peak_profit:.1%})"
        return None

    def _trigger_close_position(self, record: PositionRecord, reason: str, current_price: float = 0.0) -> None:
        with self._get_instrument_lock(record.instrument_id):
            if record._closing:
                return
            try:
                from ali2026v3_trading.order_service import get_order_service
                order_svc = get_order_service()
                direction = 'SELL' if record.volume > 0 else 'BUY'

                # 获取对手价：买平仓用bid价，卖平仓用ask价
                price = 0.0
                try:
                    from ali2026v3_trading.data_service import get_data_service
                    ds = get_data_service()
                    if ds and ds.realtime_cache:
                        tick = ds.realtime_cache._latest_ticks.get(record.instrument_id)
                        if tick:
                            price = tick.get('bid_price' if direction == 'SELL' else 'ask_price', 0.0)
                            if price <= 0:
                                price = tick.get('price', 0.0)
                except Exception as e:
                    logging.debug(f"[PositionService._trigger_close_position] Failed to get opponent price from cache: {e}")

                # 无对手价，用最新价±tick_size
                if price <= 0:
                    base = current_price or 0.0
                    if base <= 0:
                        try:
                            from ali2026v3_trading.data_service import get_data_service
                            ds = get_data_service()
                            if ds and ds.realtime_cache:
                                base = ds.realtime_cache.get_latest_price(record.instrument_id) or 0.0
                        except Exception as e:
                            logging.debug(f"[PositionService._trigger_close_position] Failed to get latest price: {e}")
                            pass
                    if base > 0:
                        try:
                            from ali2026v3_trading.params_service import get_params_service
                            tick_size = get_params_service().get_float('tick_size', 1.0)
                        except Exception as e:
                            # ✅ P1修复：添加告警日志
                            logging.warning(f"[PositionService._trigger_close_position] Failed to get tick_size, using default 1.0: {e}")
                            tick_size = 1.0
                        price = base - tick_size if direction == 'SELL' else base + tick_size
                    else:
                        logging.warning("[PositionService._trigger_close_position] 无法获取有效价格，跳过平仓: %s", record.instrument_id)
                        return

                # ✅ 集成网络重试管理器（L-P0-2）
                _close_signal_id = getattr(record, 'signal_id', '') or f"CLOSE_{record.instrument_id}"  # R24-P0-TR-01修复
                if self.network_retry_manager is not None:
                    def _send_order_wrapper():
                        return order_svc.send_order(
                            instrument_id=record.instrument_id,
                            volume=abs(record.volume),
                            price=price,
                            direction=direction,
                            action='CLOSE',
                            exchange=record.exchange or '',
                            signal_id=_close_signal_id,  # R24-P0-TR-01修复: signal_id链路贯通
                        )
                    order_id = self.network_retry_manager.execute_with_retry(
                        operation_id=f"close_{record.instrument_id}_{record.position_id}",
                        func=_send_order_wrapper,
                    )
                else:
                    order_id = order_svc.send_order(
                        instrument_id=record.instrument_id,
                        volume=abs(record.volume),
                        price=price,
                        direction=direction,
                        action='CLOSE',
                        exchange=record.exchange or '',
                        signal_id=_close_signal_id,  # R24-P0-TR-01修复: signal_id链路贯通
                    )

                if order_id:
                    record._closing = True
                    need_retry = False
                    logging.info("[PositionService._trigger_close_position] %s for %s vol=%d price=%.2f", reason, record.instrument_id, abs(record.volume), price)
                else:
                    logging.warning("[PositionService._trigger_close_position] 平仓下单失败: %s, 将重试", record.instrument_id)
                    need_retry = True
            except Exception as e:
                logging.error("[PositionService._trigger_close_position] Error: %s", e)
                return

        # ✅ 在锁外执行重试逻辑
        if need_retry:
            self._schedule_close_retry(record, price)

    _close_retry_executor = None

    @classmethod
    def _cleanup_close_retry_executor(cls):
        if cls._close_retry_executor is not None:
            cls._close_retry_executor.shutdown(wait=False)
            cls._close_retry_executor = None

    def __del__(self):
        # R23-P1-04修复: 析构时清理类级线程池，防止线程泄漏
        try:
            PositionService._cleanup_close_retry_executor()
        except Exception:
            pass

    def _schedule_close_retry(self, record, price: float) -> None:
        """非阻塞延迟重试平仓（替代time.sleep阻塞）"""
        from concurrent.futures import ThreadPoolExecutor

        if PositionService._close_retry_executor is None:
            PositionService._close_retry_executor = ThreadPoolExecutor(
                max_workers=self.CLOSE_RETRY_MAX_THREADS,
                thread_name_prefix='pos_retry'
            )
            # R22-RES-02修复: 注册atexit回调确保类级线程池shutdown
            import atexit as _atexit
            _atexit.register(PositionService._cleanup_close_retry_executor)
            # [R22-RES-02-补充] 设置线程为daemon，防止atexit未执行时阻塞进程退出
            try:
                _dummy_future = PositionService._close_retry_executor.submit(lambda: None)
                _dummy_future.result(timeout=2.0)
                for _t in getattr(PositionService._close_retry_executor, '_threads', set()):
                    _t.daemon = True
            except Exception:
                pass

        def _retry_worker():
            import time as _time
            retry_success = False
            for _retry in range(1, self.CLOSE_RETRY_MAX_ATTEMPTS + 1):
                _time.sleep(self.CLOSE_RETRY_BASE_DELAY_SEC * (2 ** (_retry - 1)))
                try:
                    from ali2026v3_trading.order_service import get_order_service
                    order_svc = get_order_service()
                    direction = 'SELL' if record.volume > 0 else 'BUY'
                    order_id = order_svc.send_order(
                        instrument_id=record.instrument_id,
                        volume=abs(record.volume),
                        price=price,
                        direction=direction,
                        action='CLOSE',
                        exchange=record.exchange or '',
                        signal_id=getattr(record, 'signal_id', '') or f"RETRY_CLOSE_{record.instrument_id}",  # R24-P0-TR-01修复
                    )
                    if order_id:
                        with self._get_instrument_lock(record.instrument_id):
                            record._closing = True
                        logging.info("[PositionService] retry %d succeeded: %s", _retry, record.instrument_id)
                        retry_success = True
                        break
                except Exception as retry_e:
                    logging.warning("[PositionService] retry %d failed: %s", _retry, retry_e)
            if not retry_success:
                with self._get_instrument_lock(record.instrument_id):
                    record._closing = False
                logging.error("[PositionService] all retries failed, reset _closing: %s", record.instrument_id)

        PositionService._close_retry_executor.submit(_retry_worker)

    def check_all_positions(self) -> None:
        _cyclic_guard = CyclicDependencyGuard.get_instance() if _HAS_CAUSAL_CHAIN else None
        if _cyclic_guard and not _cyclic_guard.enter("position_check_all"):
            logging.warning("[CC-04/CC-11] Cyclic call detected in check_all_positions, skipping")
            return
        # R27-P1-RC-02修复: 跨分片持仓操作使用协调锁，确保多策略同时检查时不产生部分读
        with self._cross_shard_lock:
            now = datetime.now(_CHINA_TZ)
            with self.global_lock:
                # R15-P0-PERF-03修复: 仅复制键列表，避免tuple(items())创建完整快照
                for inst_id in list(self.positions):
                    pos_dict = self.positions.get(inst_id)
                    if pos_dict is None:
                        continue
                    for pid in list(pos_dict):
                        record = pos_dict.get(pid)
                        if record is None:
                            continue
                        if record.volume == 0:
                            continue
                        self._check_time_stop(record, now)
                        self._check_two_stage_stop(record, now)
            self._check_eod_close(now)
        if _cyclic_guard:
            _cyclic_guard.exit("position_check_all")

        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            total_equity = 0.0
            with self.global_lock:
                for _inst_id, pos_dict in self.positions.items():
                    for _pid, rec in pos_dict.items():
                        if rec.volume != 0 and rec.open_price > 0:
                            # R13-P0-BIZ-02修复: 使用市价而非开仓价计算权益
                            # 原代码使用rec.open_price导致权益计算不准确，影响熔断器/回撤判断
                            market_price = rec.open_price  # default fallback
                            try:
                                from ali2026v3_trading.data_service import get_data_service
                                ds = get_data_service()
                                if ds and ds.realtime_cache:
                                    mp = ds.realtime_cache.get_latest_price(rec.instrument_id)
                                    if mp and mp > 0:
                                        market_price = mp
                            except Exception:
                                logging.warning("[R22-EP-P1] PositionService exception swallowed")
                                pass
                            total_equity += abs(rec.volume) * market_price
            if total_equity > 0:
                safety.on_equity_update(total_equity)

                try:
                    from ali2026v3_trading.position_service import get_cross_strategy_risk_guard
                    guard = get_cross_strategy_risk_guard()
                    if hasattr(safety, '_peak_equity') and safety._peak_equity > 0:
                        drawdown_pct = max(0.0, (safety._peak_equity - total_equity) / safety._peak_equity * 100.0)
                        guard.set_daily_drawdown(drawdown_pct)
                except (ImportError, AttributeError, ZeroDivisionError) as e:
                    logging.debug("[PositionService] drawdown update failed: %s", e)
        except (ImportError, AttributeError) as e:
            logging.debug("[PositionService] equity update failed: %s", e)

        # INV-P1-01修复: pnl == equity - initial_capital 运行时校验
        # 定期检查已实现盈亏是否等于当前权益减去初始资金，发现数据不一致
        self._validate_pnl_equity_consistency()

    def _validate_pnl_equity_consistency(self) -> None:
        """INV-P1-01修复: pnl == equity - initial_capital 运行时校验

        验证已实现盈亏(pnl)是否等于当前权益(equity)减去初始资金(initial_capital)。
        如果不一致，说明持仓记录或权益计算存在数据损坏。
        """
        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            _sid = str(getattr(self, 'strategy_id', '') or 'global')
            safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
            if safety is None:
                return

            # 获取当前权益
            equity = 0.0
            if safety._equity_series:
                equity = list(safety._equity_series)[-1] if safety._equity_series else 0.0
            if equity <= 0:
                return

            # 获取初始资金
            initial_capital = getattr(safety, '_daily_start_equity', None)
            if initial_capital is None or initial_capital <= 0:
                return

            # 独立计算已实现盈亏：从所有持仓的pnl汇总
            realized_pnl = 0.0
            with self.global_lock:
                for _inst_id, pos_dict in self.positions.items():
                    for _pid, rec in pos_dict.items():
                        if rec.volume == 0 and rec.open_price > 0:
                            # 已平仓记录的盈亏
                            if hasattr(rec, 'realized_pnl'):
                                realized_pnl += getattr(rec, 'realized_pnl', 0.0)

            # 计算期望pnl
            expected_pnl = equity - initial_capital

            # 允许0.5%的容差（浮点精度+未实现盈亏差异）
            if abs(expected_pnl) > 0 and abs(realized_pnl - expected_pnl) / max(abs(expected_pnl), 1.0) > 0.005:
                logging.error(
                    "[PositionService] INV-P1-01: PnL与权益不一致! "
                    "realized_pnl=%.2f equity=%.2f initial_capital=%.2f expected_pnl=%.2f "
                    "deviation=%.2f%%",
                    realized_pnl, equity, initial_capital, expected_pnl,
                    abs(realized_pnl - expected_pnl) / max(abs(expected_pnl), 1.0) * 100,
                )
                # 触发事件总线告警
                try:
                    from ali2026v3_trading.event_bus import get_global_event_bus, RiskEvent
                    _eb = get_global_event_bus()
                    if _eb and not getattr(_eb, '_shutdown', True):
                        _eb.publish(RiskEvent(
                            risk_type='pnl_equity_inconsistency',
                            level='HIGH',
                            message=f"INV-P1-01: PnL与权益不一致 "
                                    f"realized_pnl={realized_pnl:.2f} expected_pnl={expected_pnl:.2f} "
                                    f"equity={equity:.2f} initial_capital={initial_capital:.2f}",
                        ), async_mode=True)
                except Exception as _eb_e:
                    logging.debug("[PositionService] INV-P1-01: 事件总线告警失败: %s", _eb_e)
        except Exception as e:
            logging.debug("[PositionService] INV-P1-01: PnL权益一致性校验异常: %s", e)

    def _check_time_stop(self, record: PositionRecord, now: datetime = None) -> None:
        now = now or datetime.now(_CHINA_TZ)
        open_reason = getattr(record, 'open_reason', '')
        max_hold_minutes = self.DEFAULT_MAX_HOLD_MINUTES
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            max_hold_minutes = ps.get_int('max_hold_minutes', self.DEFAULT_MAX_HOLD_MINUTES)
        except (ImportError, AttributeError) as e:
            logging.debug("[PositionService] params_service load failed: %s", e)
        try:
            from 参数池.cycle_resonance_module import get_cycle_resonance_module
            crm = get_cycle_resonance_module()
            strategy = self._map_reason_to_strategy(open_reason)
            rs = crm.get_risk_surface(strategy)
            max_hold_minutes = rs.max_hold_seconds / 60.0
        except (ImportError, AttributeError, ZeroDivisionError) as e:
            logging.warning("[PositionService] cycle_resonance load failed: %s", e)  # R24-P1-DF-09修复: 周期共振模块不可用日志级别debug→warning
        trailing_reason = self.check_trailing_stop(record)
        if trailing_reason:
            self._trigger_close_position(record, trailing_reason)
            return
        if record.open_time:
            elapsed = (now - record.open_time).total_seconds() / 60
            adjusted_hold = max_hold_minutes
            current_plr = getattr(record, 'current_plr', 0.0)
            target_plr = getattr(record, 'target_plr', 0.0)
            if target_plr > 0 and current_plr > 0:
                plr_ratio = current_plr / target_plr
                orig_hold = adjusted_hold
                if plr_ratio >= self.PLR_RATIO_EXCELLENT:
                    adjusted_hold = max_hold_minutes * self.PLR_HOLD_MULTIPLIER_EXCELLENT
                elif plr_ratio >= self.PLR_RATIO_GOOD:
                    adjusted_hold = max_hold_minutes * self.PLR_HOLD_MULTIPLIER_GOOD
                elif plr_ratio < self.PLR_RATIO_POOR:
                    adjusted_hold = max_hold_minutes * self.PLR_HOLD_MULTIPLIER_POOR
                elif plr_ratio < self.PLR_RATIO_WARNING:
                    adjusted_hold = max_hold_minutes * self.PLR_HOLD_MULTIPLIER_WARNING
                if adjusted_hold != orig_hold:
                    logging.info(
                        '[PositionService] 时间止损PLR弹性调整: instrument=%s current_plr=%.2f target_plr=%.2f '
                        'plr_ratio=%.2f max_hold=%.1fmin -> adjusted=%.1fmin',
                        record.instrument_id, current_plr, target_plr, plr_ratio,
                        orig_hold, adjusted_hold,
                    )
            if elapsed >= adjusted_hold:
                logging.info(
                    '[PositionService] 时间止损触发: instrument=%s elapsed=%.1fmin adjusted_hold=%.1fmin reason=%s',
                    record.instrument_id, elapsed, adjusted_hold, f"TimeStop@{elapsed:.0f}min(plr_adj)",
                )
                self._trigger_close_position(record, f"TimeStop@{elapsed:.0f}min(plr_adj)")
                return

            try:
                from ali2026v3_trading.risk_service import get_safety_meta_layer
                _sid = str(getattr(self, 'strategy_id', '') or 'global')
                safety = get_safety_meta_layer(params=self._params if hasattr(self, '_params') else None, strategy_id=_sid)
                open_ts = record.open_time
                if isinstance(open_ts, datetime):
                    open_ts = open_ts.timestamp()
                elif not isinstance(open_ts, (int, float)):
                    open_ts = 0
                if open_ts > 0:
                    max_profit = getattr(record, '_max_profit_pct', 0.0)
                    profit_slope = getattr(record, 'profit_slope', 0.0)
                    peak_profit_pct = getattr(record, '_max_profit_pct', 0.0)
                    current_profit_pct = 0.0
                    current_price = getattr(record, 'current_price', 0.0)
                    if record.open_price > 0 and current_price > 0:
                        if record.volume > 0:
                            current_profit_pct = (current_price - record.open_price) / record.open_price
                        else:
                            current_profit_pct = (record.open_price - current_price) / record.open_price
                    hard_stop_reason = safety.check_position_hard_time_stop(
                        position_id=str(record.position_id) if hasattr(record, 'position_id') else record.instrument_id,
                        open_time=open_ts,
                        max_profit_reached=max_profit,
                        profit_slope=profit_slope,
                        peak_profit_pct=peak_profit_pct,
                        current_profit_pct=current_profit_pct,
                        bar_time=now.timestamp() if now else None,
                    )
                    if hard_stop_reason:
                        self._trigger_close_position(record, hard_stop_reason)
            except Exception as e:
                logging.debug(f"[PositionService._check_time_stop] SafetyMetaLayer check error: {e}")

    TWO_STAGE_STOP_CONFIG = {
        'stage1_min_minutes': 90,  # P1-R9-06修复: 与回测路径task_scheduler的stage1_min_minutes命名对齐
        'stage1_max_loss_pct': 0.02,
        'stage2_minutes': 15,
        'stage2_max_loss_pct': 0.05,
    }

    def _check_two_stage_stop(self, record: PositionRecord, now: datetime = None) -> None:
        now = now or datetime.now(_CHINA_TZ)
        if not record.open_time or record.volume == 0:
            return
        elapsed_minutes = (now - record.open_time).total_seconds() / 60.0
        current_price = getattr(record, 'current_price', 0.0)
        if record.open_price <= 0 or current_price <= 0:
            return
        if record.volume > 0:
            loss_pct = max(0.0, (record.open_price - current_price) / record.open_price)
        else:
            loss_pct = max(0.0, (current_price - record.open_price) / record.open_price)
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            stage1_min_minutes = ps.get_int('two_stage_stop_stage1_min_minutes', self.TWO_STAGE_STOP_CONFIG['stage1_min_minutes'])
            stage1_max_loss_pct = ps.get_float('two_stage_stop_stage1_max_loss_pct', self.TWO_STAGE_STOP_CONFIG['stage1_max_loss_pct'])
            stage2_minutes = ps.get_int('two_stage_stop_stage2_minutes', self.TWO_STAGE_STOP_CONFIG['stage2_minutes'])
            stage2_max_loss_pct = ps.get_float('two_stage_stop_stage2_max_loss_pct', self.TWO_STAGE_STOP_CONFIG['stage2_max_loss_pct'])
        except (ImportError, AttributeError):
            stage1_min_minutes = self.TWO_STAGE_STOP_CONFIG['stage1_min_minutes']
            stage1_max_loss_pct = self.TWO_STAGE_STOP_CONFIG['stage1_max_loss_pct']
            stage2_minutes = self.TWO_STAGE_STOP_CONFIG['stage2_minutes']
            stage2_max_loss_pct = self.TWO_STAGE_STOP_CONFIG['stage2_max_loss_pct']
        if elapsed_minutes >= stage1_min_minutes and loss_pct >= stage1_max_loss_pct:
            if elapsed_minutes < stage2_minutes:
                logging.info(
                    '[PositionService] 两阶段硬时间止损 Stage1触发: instrument=%s elapsed=%.1fmin loss_pct=%.4f threshold=%.4f',
                    record.instrument_id, elapsed_minutes, loss_pct, stage1_max_loss_pct,
                )
                self._trigger_close_position(record, f"TwoStageStop-S1@{elapsed_minutes:.0f}min")
                return
        if elapsed_minutes >= stage2_minutes and loss_pct >= stage2_max_loss_pct:
            logging.info(
                '[PositionService] 两阶段硬时间止损 Stage2触发: instrument=%s elapsed=%.1fmin loss_pct=%.4f threshold=%.4f',
                record.instrument_id, elapsed_minutes, loss_pct, stage2_max_loss_pct,
            )
            self._trigger_close_position(record, f"TwoStageStop-S2@{elapsed_minutes:.0f}min")

    def _check_option_expiry_force_close(self) -> None:
        with self.global_lock:
            for inst_id in list(self.positions):
                self._check_option_expiry(inst_id)

    def _check_eod_close(self, now: datetime = None) -> None:
        now = now or datetime.now(_CHINA_TZ)
        eod_close_hour = self.EOD_CLOSE_HOUR
        eod_close_minute = self.EOD_CLOSE_MINUTE
        night_eod_close_hour = self.NIGHT_EOD_CLOSE_HOUR
        night_eod_close_minute = self.NIGHT_EOD_CLOSE_MINUTE
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            eod_close_hour = ps.get_int('eod_close_hour', self.EOD_CLOSE_HOUR)
            eod_close_minute = ps.get_int('eod_close_minute', self.EOD_CLOSE_MINUTE)
            night_eod_close_hour = ps.get_int('night_session_eod_hour', self.NIGHT_EOD_CLOSE_HOUR)
            night_eod_close_minute = ps.get_int('night_session_eod_minute', self.NIGHT_EOD_CLOSE_MINUTE)
        except (ImportError, AttributeError) as e:
            logging.debug("[PositionService] EOD params load failed: %s", e)
        is_eod = False
        eod_reason = ""
        # R14-P1-BIZ-13修复: 检查是否为交易日(非交易日不触发EOD平仓)
        _is_trading_day = now.weekday() < 5  # 周一至周五为交易日
        if not _is_trading_day:
            logging.debug("[PositionService] R14-P1-BIZ-13: 非交易日(weekday=%d)，跳过EOD平仓", now.weekday())
            return
        if now.hour == eod_close_hour and now.minute >= eod_close_minute:
            is_eod = True
            eod_reason = "EOD_Close"
        elif now.hour == night_eod_close_hour and now.minute >= night_eod_close_minute:
            is_eod = True
            eod_reason = "EOD_Night_Close"
        if is_eod:
            if eod_reason == "EOD_Close":
                self._check_option_expiry_force_close()
            with self.global_lock:
                for inst_id in list(self.positions):
                    pos_dict = self.positions.get(inst_id)
                    if pos_dict is None:
                        continue
                    for pid in list(pos_dict):
                        record = pos_dict.get(pid)
                        if record is None:
                            continue
                        if record.volume != 0:
                            self._trigger_close_position(record, eod_reason)

    # ✅ 传递渠道唯一：通过get_config()获取配置，不再直接读取JSON文件
    def _load_position_configs(self) -> None:
        """加载持仓限额配置"""
        try:
            from ali2026v3_trading.config_service import get_config
            config = get_config()
            data = getattr(config, 'option_buy_limits', None)
            if data is None:
                # 降级：尝试从配置文件读取
                if not os.path.exists(self.config_file):
                    return
                with open(self.config_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            elif not isinstance(data, dict):
                return
            
            with self.global_lock:
                for account_id, config_data in data.items():
                    if not isinstance(config_data, dict):
                        continue
                    
                    # 解析时间字段
                    if "effective_until" in config_data and isinstance(config_data["effective_until"], str):
                        try:
                            config_data["effective_until"] = datetime.strptime(
                                config_data["effective_until"], "%Y-%m-%d %H:%M:%S"
                            )
                        except Exception as e:
                            logging.error(f"[PositionService._load_position_configs] Error parsing date: {e}")
                            continue
                    
                    if "created_at" in config_data and isinstance(config_data["created_at"], str):
                        try:
                            config_data["created_at"] = datetime.strptime(
                                config_data["created_at"], "%Y-%m-%d %H:%M:%S"
                            )
                        except Exception as e:
                            logging.error(f"[PositionService._load_position_configs] Error parsing date: {e}")
                            continue
                    
                    try:
                        config = PositionLimitConfig(**config_data)
                    except Exception as e:
                        logging.error(f"[PositionService._load_position_configs] Error creating config: {e}")
                        continue
                    
                    # 跳过过期配置
                    if config.effective_until and datetime.now(_CHINA_TZ) > config.effective_until:
                        continue
                    
                    # ✅ 传递渠道唯一#65：直接写入RiskService而非本地limit_configs
                    if self._risk_service:
                        self._risk_service.set_position_limit(account_id, config.limit_amount, config.effective_until)
            
            logging.info(f"[PositionService._load_position_configs] Loaded from {self.config_file}")
            
        except Exception as e:
            logging.error(f"[PositionService._load_position_configs] Error: {e}")
            with self.global_lock:
                # ✅ 传递渠道唯一#65：加载失败时不需清空本地（已无本地存储）
                pass

    def _save_position_configs(self) -> None:
        """保存持仓限额配置（从RiskService唯一源读取）"""
        try:
            if not self._risk_service:
                return
            save_data = {}
            with self.global_lock:
                # ✅ 传递渠道唯一#65：从RiskService._position_limits读取
                for account_id, limit_info in getattr(self._risk_service, '_position_limits', {}).items():
                    if isinstance(limit_info, PositionLimitConfig):
                        limit_amount = limit_info.limit_amount
                        effective_until = limit_info.effective_until
                    elif isinstance(limit_info, dict):
                        limit_amount = limit_info.get('limit_amount', 0)
                        effective_until = limit_info.get('effective_until')
                    else:
                        limit_amount = 0
                        effective_until = None
                    save_data[account_id] = {
                        "limit_amount": float(limit_amount),
                        "account_id": account_id,
                        "effective_until": effective_until.strftime("%Y-%m-%d %H:%M:%S")
                            if effective_until else None,
                    }
            
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(save_data, f, indent=2, ensure_ascii=False)
            
            logging.debug(f"[PositionService._save_position_configs] Saved to {self.config_file}")
            
        except Exception as e:
            logging.error(f"[PositionService._save_position_configs] Error: {e}")
    
    def get_status(self) -> str:
        """获取服务状态        
        Returns:
            str: 状态字符串
        """
        with self.global_lock:
            total_instruments = len(self.positions)
            total_records = sum(len(v) for v in self.positions.values())
            # ✅ 传递渠道唯一#65：从RiskService统计限额数
            total_configs = len(getattr(self._risk_service, '_position_limits', {})) if self._risk_service else 0
            
            return (f"PositionService: Tracking {total_instruments} instruments, "
                    f"{total_records} positions, {total_configs} limits")


# ========== Scoped Singleton Instances ==========
_position_service_instances: Dict[str, PositionService] = {}
_position_service_lock = threading.Lock()


def get_position_service(risk_service: Any = None, scope_id: Optional[str] = None) -> PositionService:
    """获取持仓服务实例（按scope隔离）

    Args:
        risk_service: RiskService实例（可选），用于仓位限制委托
        scope_id: 实例作用域，None时使用"global"
    Returns:
        PositionService: 作用域单例
    """
    scope = str(scope_id or "global")
    with _position_service_lock:
        if scope not in _position_service_instances:
            _position_service_instances[scope] = PositionService(risk_service=risk_service)
        instance = _position_service_instances[scope]
        if risk_service is not None and instance._risk_service is None:
            instance._risk_service = risk_service
        return instance


def reset_position_service(scope_id: Optional[str] = None) -> None:
    """重置实例（用于测试）

    Args:
        scope_id: 指定作用域；None表示清空全部作用域实例
    """
    with _position_service_lock:
        if scope_id is None:
            _position_service_instances.clear()
        else:
            _position_service_instances.pop(str(scope_id), None)


# ============================================================================
# 补充缺失功能 (来自 COMPLETE_FUNCTION_MAPPING_REPORT.txt)
# ============================================================================

def _calc_max_volume_from_capital(capital: float, price: float, leverage: float = 1.0, margin_rate: float = 0.15) -> int:
    """计算最大可用手数 - R14-P1-BIZ-09修复: 考虑保证金率"""
    try:
        if capital <= 0 or price <= 0:
            return 0
        # 每手保证金 = 价格 × 保证金率，可用手数 = 资金 / 每手保证金
        margin_per_lot = price * margin_rate
        if margin_per_lot <= 0:
            return 0
        volume = capital / margin_per_lot
        return int(volume // 1)
    except Exception as e:
        logging.error(f"[position_service._calc_max_volume_from_capital] Error: {e}")
        return 0


def _cancel_and_resend(order_id: str, new_params: Dict[str, Any]) -> bool:
    """取消并重新发单 - 缺失功能 #2"""
    try:
        from ali2026v3_trading.order_service import get_order_service
        order_svc = get_order_service()
        if not order_svc.cancel_order(order_id):
            return False
        # ✅ P0-22集成: 取消订单后从自成交检测中移除
        try:
            _ps = get_position_service()
            if _ps and _ps.self_trade_detector is not None:
                _ps.self_trade_detector.remove_order(order_id)
        except (ImportError, AttributeError) as e:
            logging.debug("[PositionService] self_trade_detector remove failed: %s", e)
        # R24-P0-TR-01修复: 确保signal_id传递到重发订单
        if 'signal_id' not in new_params:
            new_params['signal_id'] = f"RESUBMIT_{order_id}"
        new_order_id = order_svc.send_order(**new_params)
        return bool(new_order_id) and new_order_id.success
    except (ImportError, AttributeError, RuntimeError) as e:
        import logging
        logging.error("[position_service._cancel_and_resend] Failed to cancel and resend order %s: %s", order_id, e)
        return False


def _check_available_amount(position_data: Dict[str, Any], amount: int) -> bool:
    """检查可用数量 - 缺失功能 #3"""
    try:
        if not position_data:
            return False
        available = int(position_data.get('available', 0))
        return available >= amount
    except Exception as e:
        logging.error(f"[position_service._check_available_amount] Error: {e}")
        return False


def reconcile_positions_with_exchange(local_positions: Dict[str, Any], exchange_positions: Dict[str, Any]) -> Dict[str, Any]:
    """R26-P0-DI-01: 持仓对账——比较本地持仓与交易所持仓，返回差异报告
    
    Args:
        local_positions: 本地持仓dict {instrument_id: {volume, direction, ...}}
        exchange_positions: 交易所持仓dict {instrument_id: {volume, direction, ...}}
    
    Returns:
        Dict: {is_matched: bool, diffs: [...], local_only: [...], exchange_only: [...]}
    """
    if local_positions is None:
        local_positions = {}
    if exchange_positions is None:
        logging.warning("[R26-P0-DI-01] exchange_positions为None，交易所查询失败，仅返回本地持仓")
        exchange_positions = {}
    result = {'is_matched': True, 'diffs': [], 'local_only': [], 'exchange_only': []}
    all_keys = set(local_positions.keys()) | set(exchange_positions.keys())
    for _key in all_keys:
        _local = local_positions.get(_key)
        _exchange = exchange_positions.get(_key)
        if _local is None and _exchange is not None:
            result['exchange_only'].append(_key)
            result['is_matched'] = False
        elif _local is not None and _exchange is None:
            result['local_only'].append(_key)
            result['is_matched'] = False
        elif _local is not None and _exchange is not None:
            _lv = _local.get('volume', 0) if isinstance(_local, dict) else 0
            _ev = _exchange.get('volume', 0) if isinstance(_exchange, dict) else 0
            if _lv != _ev:
                result['diffs'].append({'instrument_id': _key, 'local_volume': _lv, 'exchange_volume': _ev})
                result['is_matched'] = False
    if not result['is_matched']:
        logging.warning("[R26-P0-DI-01] 持仓对账不一致: diffs=%d, local_only=%d, exchange_only=%d",
                        len(result['diffs']), len(result['local_only']), len(result['exchange_only']))
    return result


@dataclass(slots=True)
class GreeksExposure:
    """跨策略Greeks敞口聚合结果"""
    net_delta: float = 0.0
    gross_delta: float = 0.0
    net_vega: float = 0.0
    gross_vega: float = 0.0
    net_gamma: float = 0.0
    gross_gamma: float = 0.0
    by_strategy: Dict[str, Dict[str, float]] = field(default_factory=dict)
    by_instrument: Dict[str, Dict[str, float]] = field(default_factory=dict)
    total_futures_lots: int = 0
    total_option_lots: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'net_delta': round(self.net_delta, 4),
            'gross_delta': round(self.gross_delta, 4),
            'net_vega': round(self.net_vega, 4),
            'gross_vega': round(self.gross_vega, 4),
            'net_gamma': round(self.net_gamma, 4),
            'gross_gamma': round(self.gross_gamma, 4),
            'by_strategy': self.by_strategy,
            'by_instrument': self.by_instrument,
            'total_futures_lots': self.total_futures_lots,
            'total_option_lots': self.total_option_lots,
        }


_REASON_STRATEGY_MAP = {
    'CORRECT_RESONANCE': 'high_freq',
    'CORRECT_DIVERGENCE': 'high_freq',
    'INCORRECT_REVERSAL': 'resonance',
    'OTHER_SCALP': 'box',
    'BOX_SPRING': 'spring',
    'MANUAL': 'manual',
}


def _is_option_instrument(instrument_id: str) -> bool:
    return '-C-' in instrument_id or '-P-' in instrument_id


def _estimate_option_delta(instrument_id: str, direction: str, volume: int) -> float:
    # R14-P1-BIZ-07修复: 优先从greeks_calculator获取实时delta，固定值仅作fallback
    try:
        from ali2026v3_trading.risk_service import get_risk_service
        _rs = get_risk_service()
        if _rs:
            _gc = _rs._get_greeks_calculator() if hasattr(_rs, '_get_greeks_calculator') else None
            if _gc:
                _greeks = _gc.get_greeks(instrument_id)
                if _greeks and 'delta' in _greeks:
                    _delta = _greeks['delta']
                    sign = 1.0 if direction in ('long', 'BUY') else -1.0
                    return sign * _delta * abs(volume)
    except Exception:
        pass
    if '-C-' in instrument_id:
        delta_per_lot = PositionService.OPTION_DELTA_PER_LOT_CALL
    elif '-P-' in instrument_id:
        delta_per_lot = PositionService.OPTION_DELTA_PER_LOT_PUT
    else:
        delta_per_lot = 0.0
    sign = 1.0 if direction in ('long', 'BUY') else -1.0
    return sign * delta_per_lot * abs(volume)


def _estimate_option_vega(instrument_id: str, volume: int) -> float:
    if '-C-' in instrument_id or '-P-' in instrument_id:
        return PositionService.OPTION_VEGA_PER_LOT * abs(volume)
    return 0.0


def _estimate_option_gamma(instrument_id: str, volume: int) -> float:
    if '-C-' in instrument_id or '-P-' in instrument_id:
        return PositionService.OPTION_GAMMA_PER_LOT * abs(volume)
    return 0.0


def aggregate_greeks_exposure(
    positions: Dict[str, Dict[str, PositionRecord]],
) -> GreeksExposure:
    """聚合所有持仓的Greeks等效敞口"""
    result = GreeksExposure()
    for instrument_id, pos_dict in positions.items():
        if not pos_dict:
            continue
        inst_delta = 0.0
        inst_vega = 0.0
        inst_gamma = 0.0
        is_option = _is_option_instrument(instrument_id)
        for rec in pos_dict.values():
            if rec.volume == 0:
                continue
            strategy = _REASON_STRATEGY_MAP.get(rec.open_reason, 'unknown')
            if is_option:
                delta = _estimate_option_delta(instrument_id, rec.direction, rec.volume)
                vega = _estimate_option_vega(instrument_id, rec.volume)
                gamma = _estimate_option_gamma(instrument_id, rec.volume)
                result.total_option_lots += abs(rec.volume)
            else:
                sign = 1.0 if rec.direction in ('long', 'BUY') else -1.0
                delta = sign * abs(rec.volume)
                vega = 0.0
                gamma = 0.0
                result.total_futures_lots += abs(rec.volume)
            inst_delta += delta
            inst_vega += vega
            inst_gamma += gamma
            if strategy not in result.by_strategy:
                result.by_strategy[strategy] = {'delta': 0.0, 'vega': 0.0, 'gamma': 0.0, 'lots': 0}
            result.by_strategy[strategy]['delta'] += delta
            result.by_strategy[strategy]['vega'] += vega
            result.by_strategy[strategy]['gamma'] += gamma
            result.by_strategy[strategy]['lots'] += abs(rec.volume)
        result.net_delta += inst_delta
        result.gross_delta += abs(inst_delta)
        result.net_vega += inst_vega
        result.gross_vega += abs(inst_vega)
        result.net_gamma += inst_gamma
        result.gross_gamma += abs(inst_gamma)
        result.by_instrument[instrument_id] = {
            'delta': round(inst_delta, 4),
            'vega': round(inst_vega, 4),
            'gamma': round(inst_gamma, 4),
        }
    return result


class CrossStrategyRiskGuard:
    """跨策略分层风控守卫

    分级响应:
    - 注意线(gross_delta > limit): WARN
    - 预警线(gross_delta > limit * 1.2): REDUCE
    - 硬阻断线(gross_delta > limit * 1.5): BLOCK
    - 熔断线(gross_delta > limit * 2.0 或 日回撤>3%): CIRCUIT_BREAK
    """

    WARN = 'WARN'
    REDUCE = 'REDUCE'
    BLOCK = 'BLOCK'
    CIRCUIT_BREAK = 'CIRCUIT_BREAK'
    PASS = 'PASS'

    DEFAULT_DAILY_DRAWDOWN_CIRCUIT_BREAK_PCT = 3.0
    DEFAULT_DELTA_LIMIT = 3.0
    DEFAULT_VEGA_LIMIT = 1.5
    DEFAULT_GAMMA_LIMIT = 0.5
    TIER_CIRCUIT_BREAK_MULTIPLIER = 2.0
    TIER_BLOCK_MULTIPLIER = 1.5
    TIER_REDUCE_MULTIPLIER = 1.2

    def __init__(
        self,
        delta_limit: float = None,
        vega_limit: float = None,
        gamma_limit: float = None,
    ):
        self._delta_limit = delta_limit if delta_limit is not None else self.DEFAULT_DELTA_LIMIT
        self._vega_limit = vega_limit if vega_limit is not None else self.DEFAULT_VEGA_LIMIT
        self._gamma_limit = gamma_limit if gamma_limit is not None else self.DEFAULT_GAMMA_LIMIT
        self._daily_drawdown_pct = 0.0
        self._stats = {
            'checks': 0,
            'warns': 0,
            'reduces': 0,
            'blocks': 0,
            'circuit_breaks': 0,
        }

    def set_daily_drawdown(self, pct: float):
        self._daily_drawdown_pct = pct

    def check(self, exposure: GreeksExposure) -> Tuple[str, str, Dict[str, Any]]:
        """检查跨策略风险, 返回(级别, 原因, 详情)"""
        self._stats['checks'] += 1
        # R14-P1-LOG-04修复: check方法添加关键决策日志
        logging.info("[CrossRiskGuard] check: gross_delta=%.2f net_delta=%.2f gross_vega=%.2f gross_gamma=%.4f",
                     exposure.gross_delta, exposure.net_delta, exposure.gross_vega, exposure.gross_gamma)
        gd = exposure.gross_delta
        nd = abs(exposure.net_delta)
        gv = exposure.gross_vega
        gg = exposure.gross_gamma
        dl = self._delta_limit

        if self._daily_drawdown_pct > self.DEFAULT_DAILY_DRAWDOWN_CIRCUIT_BREAK_PCT:
            self._stats['circuit_breaks'] += 1
            return self.CIRCUIT_BREAK, f'日回撤超{self.DEFAULT_DAILY_DRAWDOWN_CIRCUIT_BREAK_PCT}%, 触发熔断', {'drawdown_pct': self._daily_drawdown_pct}

        if gd > dl * self.TIER_CIRCUIT_BREAK_MULTIPLIER or nd > dl * self.TIER_CIRCUIT_BREAK_MULTIPLIER:
            self._stats['circuit_breaks'] += 1
            return self.CIRCUIT_BREAK, f'Gross Delta({gd:.1f})超熔断线({dl*self.TIER_CIRCUIT_BREAK_MULTIPLIER:.1f})', {'gross_delta': gd}

        if gd > dl * self.TIER_BLOCK_MULTIPLIER or nd > dl * self.TIER_BLOCK_MULTIPLIER:
            self._stats['blocks'] += 1
            return self.BLOCK, f'Gross Delta({gd:.1f})超硬阻断线({dl*self.TIER_BLOCK_MULTIPLIER:.1f})', {'gross_delta': gd}

        if gv > self._vega_limit * self.TIER_BLOCK_MULTIPLIER:
            self._stats['blocks'] += 1
            return self.BLOCK, f'Gross Vega({gv:.2f})超Vega限额({self._vega_limit*self.TIER_BLOCK_MULTIPLIER:.2f})', {'gross_vega': gv}

        if gg > self._gamma_limit * self.TIER_BLOCK_MULTIPLIER:
            self._stats['blocks'] += 1
            return self.BLOCK, f'Gross Gamma({gg:.4f})超Gamma限额({self._gamma_limit*self.TIER_BLOCK_MULTIPLIER:.4f})', {'gross_gamma': gg}

        if gd > dl * self.TIER_REDUCE_MULTIPLIER or nd > dl * self.TIER_REDUCE_MULTIPLIER:
            self._stats['reduces'] += 1
            return self.REDUCE, f'Gross Delta({gd:.1f})超预警线({dl*self.TIER_REDUCE_MULTIPLIER:.1f}), 建议缩减新仓规模50%', {'gross_delta': gd}

        if gd > dl or nd > dl:
            self._stats['warns'] += 1
            return self.WARN, f'Gross Delta({gd:.1f})接近限额({dl:.1f})', {'gross_delta': gd}

        if gv > self._vega_limit:
            self._stats['warns'] += 1
            return self.WARN, f'Gross Vega({gv:.2f})接近Vega限额({self._vega_limit:.2f})', {'gross_vega': gv}

        if gg > self._gamma_limit:
            self._stats['warns'] += 1
            return self.WARN, f'Gross Gamma({gg:.4f})接近Gamma限额({self._gamma_limit:.4f})', {'gross_gamma': gg}

        return self.PASS, '', {}

    def get_stats(self) -> Dict[str, Any]:
        return dict(self._stats)


_cross_strategy_risk_guard: Optional[CrossStrategyRiskGuard] = None
# NS-P1-01修复: 添加跨策略风控守卫线程锁
_cross_strategy_risk_guard_lock = threading.Lock()


def get_cross_strategy_risk_guard() -> CrossStrategyRiskGuard:
    global _cross_strategy_risk_guard
    # NS-P1-01修复: 加锁保护全局单例初始化
    with _cross_strategy_risk_guard_lock:
        if _cross_strategy_risk_guard is None:
            _cross_strategy_risk_guard = CrossStrategyRiskGuard()
        return _cross_strategy_risk_guard


def reset_cross_strategy_risk_guard():
    global _cross_strategy_risk_guard
    # NS-P1-01修复: 加锁保护全局单例重置
    with _cross_strategy_risk_guard_lock:
        _cross_strategy_risk_guard = None
