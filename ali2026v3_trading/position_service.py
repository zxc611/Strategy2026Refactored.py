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
import os
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any

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

# RiskService 导入（避免循环依赖）
try:
    from .risk_service import RiskService
    _HAS_RISK_SERVICE = True
except ImportError as e:
    logging.warning(f"[PositionService] Failed to import RiskService: {e}")
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
    _closing: bool = False
    
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
            'chase_count': self.chase_count
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
    
    def __init__(self, risk_service: Any = None):
        """初始化持仓服务
        Args:
            risk_service: RiskService实例（可选），用于仓位限制委托        """
        # 持仓数据结构：{ instrument_id: { position_id: PositionRecord } }
        self.positions: Dict[str, Dict[str, PositionRecord]] = {}

        # 持仓限额配置：{ account_id: PositionLimitConfig }
        self.limit_configs: Dict[str, PositionLimitConfig] = {}

        # 配置文件
        self.config_file = "option_buy_limits.json"

        # 线程安全锁 - 按合约分片锁
        self.position_locks: Dict[str, threading.RLock] = {}  # 按合约ID的锁
        self.global_lock = threading.RLock()  # 全局锁，用于初始化合约锁

        # RiskService引用（仓位限制权威源）        self._risk_service = risk_service

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

        logging.info("[PositionService] Initialized")
    
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
            
            if is_open:
                # 开仓：买入 (多), 卖出 (空)
                vol_signed = volume if is_buy else -volume
                self._add_position(exch, inst_id, vol_signed, price)
            else:
                # 平仓：买入平仓(减空), 卖出平仓 (减多)
                self._reduce_position(exch, inst_id, volume, is_buy, price)
            
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
                valid_hours = 24

            # 验证范围
            max_hours = 720
            if not 1 <= valid_hours <= max_hours:
                logging.error(f"[PositionService.set_position_limit] Hours out of range: {valid_hours}")
                return False

            until = datetime.now() + timedelta(hours=valid_hours)

            # ✅ 以RiskService为唯一限额源，本地仅缓存读取
            # 注意：避免循环依赖，不委托给RiskService.set_position_limit（它会回调position_manager）
            # 而是直接更新RiskService内部的_position_limits字典
            if self._risk_service is not None:
                with self._risk_service._lock:
                    from ali2026v3_trading.risk_service import PositionLimit
                    self._risk_service._position_limits[account_id] = PositionLimit(
                        account_id=account_id,
                        limit_amount=limit_amount,
                        effective_until=until
                    )
                logging.info(f"[PositionService.set_position_limit] Updated RiskService._position_limits for {account_id}")
                return True
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
                logging.warning("[PositionService.check_position_limit] RiskService not available, default allow")
                return True  # RiskService不可用时默认允许

        except Exception as e:
            logging.error(f"[PositionService.check_position_limit] Error: {e}")
            return True
    
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
    
    def _add_position(self, exchange: str, instrument_id: str, 
                     volume: int, price: float) -> None:
        """添加持仓
        
        Args:
            exchange: 交易所
            instrument_id: 合约代码
            volume: 持仓量(正=多，负=空)
            price: 开仓价格        """
        with self._get_instrument_lock(instrument_id):
            if instrument_id not in self.positions:
                self.positions[instrument_id] = {}
            
            pos_id = f"{instrument_id}_{int(datetime.now().timestamp()*1000)}"
            
            direction_str = "long" if volume > 0 else "short"
            p_type = "long" if volume > 0 else "short"
            
            # 计算止盈价格
            sp_price = 0.0
            sl_price = 0.0
            try:
                from ali2026v3_trading.params_service import get_params_service
                ps = get_params_service()
                tp_ratio = ps.get_float('close_take_profit_ratio', 1.5)
                sl_ratio = ps.get_float('close_stop_loss_ratio', 0.5)
            except Exception:
                tp_ratio = 1.5
                sl_ratio = 0.5
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
                stop_loss_price=sl_price
            )
            
            self.positions[instrument_id][pos_id] = record
            
            logging.info(f"[PositionService._add_position] Added: {instrument_id} {volume}手@ {price}")
    
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
                except Exception:
                    pass
                
                # 无对手价，用最新价±tick_size
                if price <= 0:
                    base = current_price or 0.0
                    if base <= 0:
                        try:
                            from ali2026v3_trading.data_service import get_data_service
                            ds = get_data_service()
                            if ds and ds.realtime_cache:
                                base = ds.realtime_cache.get_latest_price(record.instrument_id) or 0.0
                        except Exception:
                            pass
                    if base > 0:
                        try:
                            from ali2026v3_trading.params_service import get_params_service
                            tick_size = get_params_service().get_float('tick_size', 1.0)
                        except Exception:
                            tick_size = 1.0
                        price = base - tick_size if direction == 'SELL' else base + tick_size
                    else:
                        logging.warning("[PositionService._trigger_close_position] 无法获取有效价格，跳过平仓: %s", record.instrument_id)
                        return
                
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
                    logging.info("[PositionService._trigger_close_position] %s for %s vol=%d price=%.2f", reason, record.instrument_id, abs(record.volume), price)
                else:
                    # ✅ #87修复：平仓下单失败时重试（指数退避，最多3次）
                    logging.warning("[PositionService._trigger_close_position] 平仓下单失败: %s, 将重试", record.instrument_id)
                    # ✅ 释放锁后sleep，避免阻塞同合约其他操作
                    need_retry = True
            except Exception as e:
                logging.error("[PositionService._trigger_close_position] Error: %s", e)
                return
        
        # ✅ 在锁外执行重试逻辑
        if need_retry:
            import time as _time
            for _retry in range(1, 4):
                _time.sleep(0.1 * (2 ** (_retry - 1)))  # 0.1s, 0.2s, 0.4s
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
                        logging.info("[PositionService._trigger_close_position] 重试第%d次成功: %s", _retry, record.instrument_id)
                        break
                except Exception as retry_e:
                    logging.warning("[PositionService._trigger_close_position] 重试第%d次异常: %s", _retry, retry_e)

    def check_all_positions(self) -> None:
        now = datetime.now()
        with self.global_lock:  # ✅ 加锁访问positions
            for inst_id, pos_dict in list(self.positions.items()):
                for pid, record in list(pos_dict.items()):
                    if record.volume == 0:
                        continue
                    self._check_time_stop(record, now)
        self._check_eod_close(now)

    def _check_time_stop(self, record: PositionRecord, now: datetime = None) -> None:
        now = now or datetime.now()
        try:
            from ali2026v3_trading.params_service import get_params_service
            ps = get_params_service()
            max_hold_minutes = ps.get_int('max_hold_minutes', 120)
        except Exception:
            max_hold_minutes = 120
        if record.open_time:
            elapsed = (now - record.open_time).total_seconds() / 60
            if elapsed >= max_hold_minutes:
                self._trigger_close_position(record, f"TimeStop@{elapsed:.0f}min")

    def _check_eod_close(self, now: datetime = None) -> None:
        now = now or datetime.now()
        if now.hour == 14 and now.minute >= 55:
            with self.global_lock:  # ✅ 加锁访问positions
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
                    
                    self.limit_configs[account_id] = config
            
            logging.info(f"[PositionService._load_position_configs] Loaded from {self.config_file}")
            
        except Exception as e:
            logging.error(f"[PositionService._load_position_configs] Error: {e}")
            with self.global_lock:
                self.limit_configs = {}
    
    def _save_position_configs(self) -> None:
        """保存持仓限额配置"""
        try:
            save_data = {}
            
            with self.global_lock:
                for account_id, config in self.limit_configs.items():
                    if not config.is_valid():
                        continue
                    
                    save_data[account_id] = {
                        "limit_amount": float(config.limit_amount),
                        "account_id": config.account_id,
                        "effective_until": config.effective_until.strftime("%Y-%m-%d %H:%M:%S")
                            if config.effective_until else None,
                        "created_at": config.created_at.strftime("%Y-%m-%d %H:%M:%S")
                            if config.created_at else None,
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
            total_configs = len(self.limit_configs)
            
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
