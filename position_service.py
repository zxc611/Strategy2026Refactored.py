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
from ali2026v3_trading.performance_monitor import count_call
import os
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple

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

# RiskService 导入（懒加载避免循环依赖）
_HAS_RISK_SERVICE = False
RiskService = None


@dataclass
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
    target_plr: float = 0.0
    current_plr: float = 0.0
    plr_status: str = ''
    _closing: bool = False
    _max_profit_pct: float = 0.0
    _profit_history: List[float] = None
    profit_slope: float = 0.0
    
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
            'target_plr': self.target_plr,
            'current_plr': self.current_plr,
            'plr_status': self.plr_status,
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
            target_plr=data.get('target_plr', 0.0),
            current_plr=data.get('current_plr', 0.0),
            plr_status=data.get('plr_status', ''),
            _closing=data.get('_closing', False)
        )


@dataclass
class PositionLimitConfig(object):
    """持仓限额配置
    
    Attributes:
        limit_amount: 限额金额
        account_id: 账户 ID
        effective_until: 有效期至
        created_at: 创建时间
    """
    limit_amount: float
    account_id: str
    effective_until: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.now)
    
    def is_valid(self) -> bool:
        """检查限额有效"""
        if self.limit_amount <= 0:
            return False
        if self.effective_until and datetime.now() > self.effective_until:
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

    # P1-35: 核心魔法数字参数化 — 止盈止损倍数
    DEFAULT_TP_RATIO = 1.5
    DEFAULT_SL_RATIO = 0.50
    TP_SL_REASON_DEFAULTS = {
        'CORRECT_RESONANCE': (1.5, 0.50),
        'CORRECT_DIVERGENCE': (1.2, 0.40),
        'INCORRECT_REVERSAL': (1.3, 0.60),
        'OTHER_SCALP': (1.1, 0.30),
        'BOX_SPRING': (5.0, 0.60),
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

    # 持仓时间止损默认参数
    DEFAULT_MAX_HOLD_MINUTES = 120
    EOD_CLOSE_HOUR = 14
    EOD_CLOSE_MINUTE = 55
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
        Args:
            risk_service: RiskService实例（可选），用于仓位限制委托        """
        # 持仓数据结构：{ instrument_id: { position_id: PositionRecord } }
        self.positions: Dict[str, Dict[str, PositionRecord]] = {}

        # 持仓限额配置：已删除limit_configs本地存储，统一使用RiskService._position_limits
        # ✅ 传递渠道唯一#65：RiskService为唯一限额源，本地仅从ID读取

        # 配置文件
        self.config_file = "option_buy_limits.json"

        # 线程安全锁 - 按合约分片锁
        self.position_locks: Dict[str, threading.RLock] = {}  # 按合约ID的锁
        self.global_lock = threading.RLock()  # 全局锁，用于初始化合约锁

        # RiskService引用（仓位限制权威源）
        self._risk_service = risk_service

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
    
    @staticmethod
    def _get_platform_attr(obj: Any, *attr_names: str, default: Any = None) -> Any:
        """统一获取平台属性，按优先级尝试多个属性名
        
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
    def on_trade(self, trade: Any) -> None:
        """从成交回报更新持仓
        Args:
            trade: 成交对象 (兼容 CTPII/InfiniTrader 字段)
        """
        try:
            # 使用统一方法获取平台属性，避免重复的双属性路径
            inst_id = self._get_platform_attr(trade, "instrument_id", "InstrumentID", default="")
            exch = self._get_platform_attr(trade, "exchange", "ExchangeID", default="")

            # Direction: 0=Buy, 1=Sell; Offset: 0=Open, 1=Close
            d_raw = self._get_platform_attr(trade, "direction", "Direction", default="")
            o_raw = self._get_platform_attr(trade, "offset_flag", "OffsetFlag", default="")

            price = self._get_platform_attr(trade, "price", "Price", default=0)
            volume = self._get_platform_attr(trade, "volume", "Volume", default=0)

            # 转换方向
            is_buy = (str(d_raw) == "0")
            is_open = (str(o_raw) == "0")

            # ✅ 集成部分成交处理（L-P0-3）
            if self.partial_fill_handler is not None:
                try:
                    order_id = self._get_platform_attr(trade, "order_id", "OrderID", default="")
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
                open_reason = self._get_open_reason_from_order(inst_id)
                self._add_position(exch, inst_id, vol_signed, price, open_reason=open_reason)
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
        """实时行情检查 (止盈/止损)
        
        Args:
            tick: Tick 数据对象
        """
        try:
            # 使用统一方法获取价格（优先级：last_price > LastPrice > price > last）
            price = self._get_platform_attr(tick, "last_price", "LastPrice", "price", "last", default=0)
            
            if price <= 0:
                return
            
            # 使用统一方法获取合约ID
            inst_id = self._get_platform_attr(tick, "instrument_id", "InstrumentID", default="")
            if not inst_id:
                return
            
            # 检查该合约的所有持仓
            with self._get_instrument_lock(inst_id):
                if inst_id in self.positions:
                    for pid, record in list(self.positions[inst_id].items()):
                        self._check_stop_profit(record, price)
                        self._check_stop_loss(record, price)

                        if record.volume != 0 and record.open_price > 0:
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
                            if len(record._profit_history) >= 5:
                                if _HAS_NUMPY:
                                    try:
                                        record.profit_slope = float(np.polyfit(
                                            range(len(record._profit_history)),
                                            record._profit_history, 1)[0])
                                    except (ValueError, np.linalg.LinAlgError):
                                        n = len(record._profit_history)
                                        x_mean = (n - 1) / 2.0
                                        y_mean = sum(record._profit_history) / n
                                        num = sum((i - x_mean) * (record._profit_history[i] - y_mean) for i in range(n))
                                        den = sum((i - x_mean) ** 2 for i in range(n))
                                        record.profit_slope = num / den if den > 0 else 0.0
                                else:
                                    n = len(record._profit_history)
                                    x_mean = (n - 1) / 2.0
                                    y_mean = sum(record._profit_history) / n
                                    num = sum((i - x_mean) * (record._profit_history[i] - y_mean) for i in range(n))
                                    den = sum((i - x_mean) ** 2 for i in range(n))
                                    record.profit_slope = num / den if den > 0 else 0.0
                    
        except Exception as e:
            logging.error(f"[PositionService.on_tick] Error: {e}")
    
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

            until = datetime.now() + timedelta(hours=valid_hours)

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
        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                return {"volume": 0, "average_price": 0, "positions": []}
            
            positions = list(self.positions[instrument_id].values())
            total_volume = sum(rec.volume for rec in positions)
            
            if total_volume == 0:
                return {"volume": 0, "average_price": 0, "positions": []}
            
            # 计算平均价格
            weighted_sum = sum(Decimal(str(abs(rec.volume))) * Decimal(str(rec.open_price)) for rec in positions)
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
        """计算持仓风险
        
        Args:
            instrument_id: 合约代码
            
        Returns:
            float: 风险值 (持仓量 * 平均价格)
        """
        try:
            position_info = self.get_position(instrument_id)
            volume = position_info.get("volume", 0)
            average_price = position_info.get("average_price", 0)
            
            # 简单风险计算：持仓量 * 平均价格
            risk = abs(volume) * average_price
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
            current_date = datetime.now().date()
            
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
                     volume: int, price: float, open_reason: str = '') -> None:
        """添加持仓

        Args:
            exchange: 交易所
            instrument_id: 合约代码
            volume: 持仓量(正=多，负=空)
            price: 开仓价格
            open_reason: V7新增-开仓理由编码
        """
        # ✅ 集成自成交检测（L-P0-1）
        if self.self_trade_detector is not None:
            try:
                from ali2026v3_trading.order_persistence import OrderRecord
                new_order = OrderRecord(
                    order_id=f"temp_{int(datetime.now().timestamp()*1000)}",
                    instrument_id=instrument_id,
                    direction="buy" if volume > 0 else "sell",
                    price=price,
                    volume=abs(volume),
                    timestamp=datetime.now().timestamp(),
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
            pos_id = f"{instrument_id}_{int(datetime.now().timestamp()*1000)}_{_uuid.uuid4().hex[:6]}"
            
            direction_str = "long" if volume > 0 else "short"
            p_type = "long" if volume > 0 else "short"
            
            sp_price = 0.0
            sl_price = 0.0
            try:
                tp_ratio, sl_ratio = self._get_tp_sl_ratios_by_reason(open_reason)
                sl_ratio = self._apply_crm_stop_loss_adjustment(sl_ratio, open_reason)
            except Exception as e:
                logging.warning(f"[PositionService._add_position] Failed to get TP/SL ratio, using defaults: {e}")
                tp_ratio = self.DEFAULT_TP_RATIO
                sl_ratio = self.DEFAULT_SL_RATIO
            if price > 0:
                if volume > 0:
                    sp_price = price * tp_ratio
                    sl_price = price * (1 - sl_ratio)
                else:
                    sp_price = price / tp_ratio
                    sl_price = price * (1 + sl_ratio)
            
            record = PositionRecord(
                position_id=pos_id,
                instrument_id=instrument_id,
                exchange=exchange,
                volume=volume,
                direction=direction_str,
                open_price=price,
                open_time=datetime.now(),
                open_date=datetime.now().date(),
                position_type=p_type,
                stop_profit_price=sp_price,
                stop_loss_price=sl_price,
                open_reason=open_reason,
            )
            
            self.positions[instrument_id][pos_id] = record

            logging.info(f"[PositionService._add_position] Added: {instrument_id} {volume}手@ {price} reason={open_reason}")

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

    _HARDCODED_REASON_DEFAULTS = TP_SL_REASON_DEFAULTS
    _FALLBACK_TP_SL = (DEFAULT_TP_RATIO, DEFAULT_SL_RATIO)

    def _get_tp_sl_ratios_by_reason(self, open_reason: str) -> Tuple[float, float]:
        """V7新增：根据开仓理由获取差异化止盈止损倍数

        路由优先级（P3>硬编码兜底）：
          P3: state_param_sets.yaml 中的 reason 级参数集（如 CORRECT_RESONANCE/BOX_SPRING）
          P2: 硬编码 _HARDCODED_REASON_DEFAULTS（保证YAML缺失时不崩溃）
          P1: _FALLBACK_TP_SL 兜底值 (1.5, 0.50)

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

    @staticmethod
    def _map_reason_to_strategy(reason: str) -> str:
        # ✅ P0-7修复: 统一使用模块级_REASON_STRATEGY_MAP，避免重复定义导致不一致
        return _REASON_STRATEGY_MAP.get(reason, 'high_freq')

    def _apply_crm_stop_loss_adjustment(self, sl_ratio: float, open_reason: str) -> float:
        try:
            import importlib
            crm_module = importlib.import_module('参数池.cycle_resonance_module')
            crm = crm_module.get_cycle_resonance_module()
            strategy = self._map_reason_to_strategy(open_reason)
            rs = crm.get_risk_surface(strategy)
            adjusted = sl_ratio * rs.stop_loss_multiplier
            return float(np.clip(adjusted, self.CRM_SL_CLIP_LOWER, self.CRM_SL_CLIP_UPPER)) if _HAS_NUMPY else max(self.CRM_SL_CLIP_LOWER, min(self.CRM_SL_CLIP_UPPER, adjusted))
        except (ImportError, ModuleNotFoundError):
            logging.debug("[PositionService] 参数池.cycle_resonance_module not available, using default sl_ratio")
            return sl_ratio
        except Exception as e:
            logging.debug("[PositionService] _apply_crm_stop_loss_adjustment error: %s", e)
            return sl_ratio

    def _get_open_reason_from_order(self, instrument_id: str) -> str:
        """V7新增：从订单服务获取开仓理由"""
        try:
            from ali2026v3_trading.order_service import get_order_service
            osvc = get_order_service()
            if osvc:
                orders = osvc.get_orders_by_instrument(instrument_id)
                for o in orders:
                    if o.get('action') == 'OPEN' and o.get('status') in ('SUBMITTED', 'FILLED', 'ALL_FILLED', '全成'):
                        return o.get('open_reason', '')
        except Exception:
            pass
        return ''

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
                    remaining_close = 0
            
            # 删除已平仓记录
            for k in keys_to_remove:
                del self.positions[instrument_id][k]
            
            logging.info(f"[PositionService._reduce_position] Reduced: {instrument_id} {volume}@ {price}")
    
    def _check_stop_profit(self, record: PositionRecord, current_price: float) -> None:
        """检查止盈        
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
                self._trigger_close_position(record, f"StopProfit@{current_price:.2f}", current_price)

    def _check_stop_loss(self, record: PositionRecord, current_price: float) -> None:
        if record.volume == 0:
            return
        if record.stop_loss_price > 0:
            triggered = False
            # 统一使用 volume 正负判断多空（volume>0=多，volume<0=空）
            is_long = record.volume > 0
            is_short = record.volume < 0
            if is_long and current_price <= record.stop_loss_price:
                triggered = True
            elif is_short and current_price >= record.stop_loss_price:
                triggered = True
            if triggered:
                self._trigger_close_position(record, f"StopLoss@{current_price:.2f}", current_price)

    def check_trailing_stop(self, record) -> Optional[str]:
        current_price = getattr(record, 'current_price', 0.0)
        open_price = record.open_price
        if open_price <= 0 or current_price <= 0:
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
                if self.network_retry_manager is not None:
                    def _send_order_wrapper():
                        return order_svc.send_order(
                            instrument_id=record.instrument_id,
                            volume=abs(record.volume),
                            price=price,
                            direction=direction,
                            action='CLOSE',
                            exchange=record.exchange or '',
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

    def _schedule_close_retry(self, record, price: float) -> None:
        """非阻塞延迟重试平仓（替代time.sleep阻塞）"""
        from concurrent.futures import ThreadPoolExecutor

        if PositionService._close_retry_executor is None:
            PositionService._close_retry_executor = ThreadPoolExecutor(
                max_workers=self.CLOSE_RETRY_MAX_THREADS,
                thread_name_prefix='pos_retry'
            )

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
        now = datetime.now()
        with self.global_lock:
            for inst_id, pos_dict in list(self.positions.items()):
                for pid, record in list(pos_dict.items()):
                    if record.volume == 0:
                        continue
                    self._check_time_stop(record, now)
        self._check_eod_close(now)

        try:
            from ali2026v3_trading.risk_service import get_safety_meta_layer
            safety = get_safety_meta_layer()
            total_equity = 0.0
            with self.global_lock:
                for _inst_id, pos_dict in self.positions.items():
                    for _pid, rec in pos_dict.items():
                        if rec.volume != 0 and rec.open_price > 0:
                            total_equity += abs(rec.volume) * rec.open_price
            if total_equity > 0:
                safety.on_equity_update(total_equity)

                try:
                    from ali2026v3_trading.position_service import get_cross_strategy_risk_guard
                    guard = get_cross_strategy_risk_guard()
                    if hasattr(safety, '_peak_equity') and safety._peak_equity > 0:
                        drawdown_pct = max(0.0, (safety._peak_equity - total_equity) / safety._peak_equity * 100.0)
                        guard.set_daily_drawdown(drawdown_pct)
                except Exception:
                    pass
        except Exception:
            pass

    def _check_time_stop(self, record: PositionRecord, now: datetime = None) -> None:
        now = now or datetime.now()
        open_reason = getattr(record, 'open_reason', '')
        max_hold_minutes = self.DEFAULT_MAX_HOLD_MINUTES
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            max_hold_minutes = ps.get_int('max_hold_minutes', self.DEFAULT_MAX_HOLD_MINUTES)
        except Exception:
            pass
        try:
            from 参数池.cycle_resonance_module import get_cycle_resonance_module
            crm = get_cycle_resonance_module()
            strategy = self._map_reason_to_strategy(open_reason)
            rs = crm.get_risk_surface(strategy)
            max_hold_minutes = rs.max_hold_seconds / 60.0
        except Exception:
            pass
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
                safety = get_safety_meta_layer()
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
                    )
                    if hard_stop_reason:
                        self._trigger_close_position(record, hard_stop_reason)
            except Exception as e:
                logging.debug(f"[PositionService._check_time_stop] SafetyMetaLayer check error: {e}")

    def _check_eod_close(self, now: datetime = None) -> None:
        now = now or datetime.now()
        eod_close_hour = self.EOD_CLOSE_HOUR
        eod_close_minute = self.EOD_CLOSE_MINUTE
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            eod_close_hour = ps.get_int('eod_close_hour', self.EOD_CLOSE_HOUR)
            eod_close_minute = ps.get_int('eod_close_minute', self.EOD_CLOSE_MINUTE)
        except Exception:
            pass
        if now.hour == eod_close_hour and now.minute >= eod_close_minute:
            with self.global_lock:
                for inst_id, pos_dict in list(self.positions.items()):
                    for pid, record in list(pos_dict.items()):
                        if record.volume != 0:
                            self._trigger_close_position(record, "EOD_Close")

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
                    if config.effective_until and datetime.now() > config.effective_until:
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


# ========== Singleton Instance ==========
_global_position_service: Optional[PositionService] = None
_position_service_lock = threading.Lock()


def get_position_service(risk_service: Any = None) -> PositionService:
    """获取全局持仓服务实例

    Args:
        risk_service: RiskService实例（可选），用于仓位限制委托
    Returns:
        PositionService: 全局单例
    """
    global _global_position_service
    if _global_position_service is None:
        with _position_service_lock:
            if _global_position_service is None:
                _global_position_service = PositionService(risk_service=risk_service)
    if risk_service is not None and _global_position_service is not None and _global_position_service._risk_service is None:
        with _position_service_lock:
            if _global_position_service._risk_service is None:
                _global_position_service._risk_service = risk_service
    return _global_position_service
def reset_position_service() -> None:
    """重置全局实例（用于测试）"""
    global _global_position_service
    _global_position_service = None


# ============================================================================
# 补充缺失功能 (来自 COMPLETE_FUNCTION_MAPPING_REPORT.txt)
# ============================================================================

def _calc_max_volume_from_capital(capital: float, price: float, leverage: float = 1.0) -> int:
    """计算最大可用手数 - 缺失功能 #1"""
    try:
        if capital <= 0 or price <= 0:
            return 0
        volume = (capital * leverage) / price
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
        except Exception:
            pass
        new_order_id = order_svc.send_order(**new_params)
        return new_order_id is not None
    except Exception as e:
        import logging
        logging.error(f"[position_service._cancel_and_resend] Failed to cancel and resend order {order_id}: {e}")
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


@dataclass
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


def get_cross_strategy_risk_guard() -> CrossStrategyRiskGuard:
    global _cross_strategy_risk_guard
    if _cross_strategy_risk_guard is None:
        _cross_strategy_risk_guard = CrossStrategyRiskGuard()
    return _cross_strategy_risk_guard


def reset_cross_strategy_risk_guard():
    global _cross_strategy_risk_guard
    _cross_strategy_risk_guard = None
