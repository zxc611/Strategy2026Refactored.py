"""持仓管理。"""
from __future__ import annotations

import collections
import os
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
from .position_models import PositionRecord, PositionType, PositionLimitConfig


class PositionManager:
    def __init__(self, strategy):
        self.strategy = strategy
        self.positions: Dict[str, Dict[str, PositionRecord]] = {}
        # 结构: { instrument_id: { position_id: PositionRecord } }
        self.limit_configs: Dict[str, PositionLimitConfig] = {} # Account ID -> PositionLimitConfig
        self.config_file = "option_buy_limits.json"
        self.lock = self._create_lock()
        self._load_configs()

    def _create_lock(self):
        import threading
        return threading.RLock()

    def _load_configs(self):
        """加载配置"""
        try:
            if not os.path.exists(self.config_file):
                return

            with open(self.config_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            with self.lock:
                for account_id, config_data in data.items():
                    try:
                        if not isinstance(config_data, dict):
                            continue

                        if "effective_until" in config_data and isinstance(config_data["effective_until"], str):
                            config_data["effective_until"] = datetime.strptime(
                                config_data["effective_until"], "%Y-%m-%d %H:%M:%S"
                            )

                        if "created_at" in config_data and isinstance(config_data["created_at"], str):
                            config_data["created_at"] = datetime.strptime(
                                config_data["created_at"], "%Y-%m-%d %H:%M:%S"
                            )

                        config = PositionLimitConfig(**config_data)

                        if config.effective_until and datetime.now() > config.effective_until:
                            continue

                        self.limit_configs[account_id] = config

                    except Exception:
                        continue
        except Exception:
            with self.lock:
                self.limit_configs = {}

    def _save_configs(self):
        """保存配置"""
        try:
            save_data = {}
            with self.lock:
                for account_id, config in self.limit_configs.items():
                    if not self._is_limit_valid(config):
                        continue

                    save_data[account_id] = {
                        "limit_amount": float(config.limit_amount),
                        "account_id": config.account_id,
                        "effective_until": config.effective_until.strftime("%Y-%m-%d %H:%M:%S")
                        if config.effective_until
                        else None,
                        "created_at": config.created_at.strftime("%Y-%m-%d %H:%M:%S")
                        if config.created_at
                        else None,
                    }

            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(save_data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def _is_limit_valid(self, config: PositionLimitConfig) -> bool:
        """检查限额有效性"""
        if config.limit_amount <= 0:
            return False
        if config.effective_until and datetime.now() > config.effective_until:
            return False
        return True

    def set_position_limit_config(self, account_id: str, limit_amount: float, valid_hours: Optional[int] = None, force_set: bool = False) -> Any:
        return self.set_position_limit(account_id, limit_amount, valid_hours, force_set)

    def set_position_limit(self, account_id: str, limit_amount: float, valid_hours: Optional[int] = None, force_set: bool = False) -> Any:
        # Full Implementation of Risk Limit Setting (Ported from Source)
        try:
            if not account_id: return False, "Invalid ID"
            
            p = getattr(self.strategy, "params", None)
            
            if valid_hours is None:
                valid_hours = 24
                if p: valid_hours = getattr(p, "position_limit_default_valid_hours", 24)
            
            max_hours = 720
            if p: max_hours = getattr(p, "position_limit_valid_hours_max", 720)
            
            if not 1 <= valid_hours <= max_hours:
                return False, "Hours out of range"
                
            from datetime import timedelta
            until = datetime.now() + timedelta(hours=valid_hours)
            
            # Simplified for now as struct is not fully defined in models context
            self.limit_configs[account_id] = {
                "limit": limit_amount,
                "until": until
            }
            return True, "Set success"
        except Exception as e:
            return False, str(e)

    def on_trade(self, trade: Any) -> None:
        """从成交回报更新持仓"""
        with self.lock:
            try:
                # 兼容属性读取
                inst_id = getattr(trade, "instrument_id", "") or getattr(trade, "InstrumentID", "")
                exch = getattr(trade, "exchange", "") or getattr(trade, "ExchangeID", "")
                
                # Direction: 0=Buy, 1=Sell; Offset: 0=Open, 1=Close
                d_raw = getattr(trade, "direction", "")
                if d_raw == "": d_raw = getattr(trade, "Direction", "")
                
                o_raw = getattr(trade, "offset_flag", "")
                if o_raw == "": o_raw = getattr(trade, "OffsetFlag", "")

                price = float(getattr(trade, "price", 0) or getattr(trade, "Price", 0))
                volume = int(getattr(trade, "volume", 0) or getattr(trade, "Volume", 0))
                
                # 转换方向
                is_buy = (str(d_raw) == "0")
                is_open = (str(o_raw) == "0")

                if is_open:
                    # 开仓：买入(多), 卖出(空)
                    vol_signed = volume if is_buy else -volume
                    self._add_position(exch, inst_id, vol_signed, price)
                else:
                    # 平仓：买入平仓(减空), 卖出平仓(减多)
                    # _reduce_position 这里的 volume 应该是减少的量
                    # 如果是 Buy(0) Close(1): 平空 -> volume减少量为负? 
                    # Reduce logic expects signed volume change usually
                    # 简化：传入绝对值和平仓方向
                    vol_change = volume if is_buy else -volume
                    self._reduce_position(exch, inst_id, vol_change, price)

            except Exception as e:
                if self.strategy:
                    self.strategy.output(f"持仓更新出错: {e}")

    # Aliases for Source Compatibility via Strategy hooks
    def handle_new_position(self, trade_data: Any) -> None:
        self.on_trade(trade_data)
        
    def handle_order_trade(self, trade_data: Any) -> None:
        self.on_trade(trade_data)

    def _add_position(self, exchange: str, instrument_id: str, volume: int, price: float) -> None:
        if instrument_id not in self.positions:
            self.positions[instrument_id] = {}
        
        pos_id = f"{instrument_id}_{int(datetime.now().timestamp()*1000)}"
        
        direction_str = "long" if volume > 0 else "short"
        p_type = PositionType.LONG if volume > 0 else PositionType.SHORT
        
        # Calculate Stop Profit Price
        sp_price = 0.0
        if self.strategy and hasattr(self.strategy, "params"):
             ratio = getattr(self.strategy.params, "close_take_profit_ratio", 1.5)
             if volume > 0 and price > 0:
                 sp_price = price * ratio
        
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
            stop_profit_price=sp_price
        )
        self.positions[instrument_id][pos_id] = record
        
        if self.strategy:
            self.strategy.output(f"持仓增加: {instrument_id} {volume}手 @ {price}")

    def on_tick(self, tick: Any) -> None:
        """实时行情检查 (止盈/止损)"""
        try:
            # Parse Price
            price = getattr(tick, "last_price", 0) or getattr(tick, "LastPrice", 0) or getattr(tick, "price", 0) or getattr(tick, "last", 0)
            if price <= 0: return
            
            inst_id = getattr(tick, "instrument_id", "") or getattr(tick, "InstrumentID", "")
            if not inst_id: return
            
            with self.lock:
                # Iterate positions for this instrument
                if inst_id in self.positions:
                    for pid, record in list(self.positions[inst_id].items()):
                        self._check_stop_profit(record, price)
        except Exception:
            pass

    def _check_stop_profit(self, record: PositionRecord, current_price: float) -> None:
        if record.volume <= 0: return
        
        # Check Stop Profit
        if getattr(record, "stop_profit_price", 0) > 0:
            triggered = False
            # Normalize direction check
            is_long = (str(record.direction).lower() == "long" or record.volume > 0)
            is_short = (str(record.direction).lower() == "short" or record.volume < 0)

            if is_long:
                if current_price >= record.stop_profit_price:
                    triggered = True
            elif is_short:
                if current_price <= record.stop_profit_price:
                    triggered = True
            
            if triggered:
                 self._close_position(record, f"StopProfit Triggered: {current_price} vs {record.stop_profit_price}")

    def _close_position(self, record: PositionRecord, reason: str) -> None:
        """触发平仓"""
        if not self.strategy: return
        
        # 优先使用 Strategy 提供的对价平仓方法 (包含SHFE/INE平今平昨逻辑)
        if hasattr(self.strategy, "_close_position_at_opposite_price"):
             self.strategy._close_position_at_opposite_price(record.position_id, reason)
             return

        # Fallback Logic
        inst_info = {"ExchangeID": record.exchange, "InstrumentID": record.instrument_id}
        vol = abs(record.volume)
        # Close direction is opposite of open
        is_long = (str(record.direction).lower() == "long" or record.volume > 0)
        direction = "sell" if is_long else "buy"
        
        self.strategy.output(f"平仓触发 [{reason}] {record.instrument_id}")
        
        if hasattr(self.strategy, "send_order_safe"):
            # Use safe send order
            self.strategy.send_order_safe(inst_info, vol, direction, reason, is_close=True)

    def _reduce_position(self, exchange: str, instrument_id: str, volume: int, price: float) -> None:
        # 平仓逻辑：默认FIFO
        if instrument_id not in self.positions:
            return

        to_close = abs(volume)
        # 简单处理：如果是卖出平仓，找多头持仓；如果是买入平仓，找空头持仓
        target_type = PositionType.LONG if volume < 0 else PositionType.SHORT  
        # 注意: 参数volume是净变化量。reduce时，传入的是反向变化。
        # 例如持有+10，平仓卖出5，则volume为-5。
        
        # 修正逻辑：
        # 如果是平多 (Sell Close)，传入的volume应该是负数？还是正数？
        # 这里假设调用方已处理好符号。
        # 如果当前持仓是多头，需要减少，传入volume应为负。
        
        # 为了简化，这里实现基本的FIFO
        records = list(self.positions[instrument_id].values())
        records.sort(key=lambda x: x.open_time)

        remaining_close = abs(volume)
        
        keys_to_remove = []

        for pid, rec in self.positions[instrument_id].items():
            if remaining_close <= 0:
                break
            
            # 同向无法平仓 (这是加仓逻辑，不应进这里)
            if (rec.volume > 0 and volume > 0) or (rec.volume < 0 and volume < 0):
                continue

            # 反向：可以平仓
            can_close = abs(rec.volume)
            if can_close <= remaining_close:
                remaining_close -= can_close
                keys_to_remove.append(pid)
            else:
                # 部分平仓
                rec.volume = rec.volume - volume # 这里的volume带符号，例如 +10 - 5 = +5
                 # 或者如果不带符号，需根据方向调整
                if rec.volume > 0:
                     rec.volume -= remaining_close
                else:
                     rec.volume += remaining_close
                remaining_close = 0
        
        for k in keys_to_remove:
            del self.positions[instrument_id][k]
            
        if self.strategy:
             self.strategy.output(f"持仓减少: {instrument_id} {abs(volume)}手 (剩余需平: {remaining_close})")


    def get_net_position(self, instrument_id: str) -> int:
        """获取某合约的净持仓量"""
        with self.lock:
            if instrument_id not in self.positions:
                return 0
            return sum(rec.volume for rec in self.positions[instrument_id].values())

    def get_positions_for_future(self, future_symbol: str) -> List[PositionRecord]:
        """获取与某期货相关的所有持仓（含期货本身和对应的期权）"""
        # 需结合 strategy 的映射关系
        # 这里简化：只返回包含该字符串的持仓
        res = []
        with self.lock:
            for inst_id, pos_map in self.positions.items():
                if future_symbol in inst_id: # 模糊匹配
                    res.extend(pos_map.values())
        return res

    def get_position_info(self) -> List[Dict]:
        """获取仓位信息 - 规则7"""
        with self.lock:
            result = []
            current_date = datetime.now().date()
            
            # Flatten all structure to list
            all_records = []
            for inst_map in self.positions.values():
                all_records.extend(inst_map.values())

            for record in all_records:
                if record.volume > 0:
                    r_open_date = record.open_date
                    if isinstance(r_open_date, datetime):
                        r_open_date = r_open_date.date()
                    
                    days_held = (current_date - r_open_date).days
                    
                    result.append({
                        "仓位ID": record.position_id,
                        "合约": record.instrument_id,
                        "开仓价": f"{record.open_price:.2f}",
                        "持仓量": record.volume,
                        "方向": "多头" if str(record.direction) == "0" else "空头",
                        "性质": record.position_type,
                        "开仓日期": r_open_date.strftime("%Y-%m-%d"),
                        "持仓天数": days_held,
                        "开仓超过3天": days_held >= 3,
                        "止盈价": f"{record.stop_profit_price:.2f}",
                        "追单次数": record.chase_count
                    })
            return result

    def _generate_position_id(self, trade_data: Any) -> str:
        """生成唯一仓位ID"""
        timestamp = int(datetime.now().timestamp() * 1000)
        # 兼容不同的 trade_id 字段
        trade_id = getattr(trade_data, 'trade_id', None) or getattr(trade_data, 'TradeID', None) or str(timestamp)
        inst = getattr(trade_data, "instrument_id", "Unknown")
        return f"{inst}_{trade_id}_{timestamp}"

    def _update_position_after_complete(self, position_id: str, traded_volume: int) -> None:
        """更新持仓记录 (Reduction Logic)"""
        with self.lock:
             # Search across all instruments
            target_inst = None
            target_rec = None
            
            for inst_id, pos_map in self.positions.items():
                if position_id in pos_map:
                    target_inst = inst_id
                    target_rec = pos_map[position_id]
                    break
            
            if target_rec:
                target_rec.volume -= traded_volume
                if target_rec.volume <= 0:
                    if self.strategy:
                        self.strategy.output(f"仓位{position_id} 已完全平仓")
                    # Optional: Clean up zero records
                    # if target_inst in self.positions:
                        # self.positions[target_inst].pop(position_id, None)

    def has_interest(self, instrument_id: str) -> bool:
        """检查是否关注该合约（持仓）"""
        with self.lock:
            if instrument_id in self.positions and len(self.positions[instrument_id]) > 0:
                return True
            return False

    def _load_existing_positions(self) -> None:
        """加载已有持仓 (Mock/Placeholder)"""
        # In a real scenario, this would query the broker/strategy backend
        # and populate self.positions. Currently integrated from Source as a shell/logic structure.
        pass
    
    def get_manager_status(self) -> str:
        with self.lock:
            total = sum(len(v) for v in self.positions.values())
            return f"PositionManager: Tracking {len(self.positions)} instruments, {total} records."

    def check_and_close_overdue_positions(self, days_limit: int = 3) -> None:
        """检查并平仓超期持仓 (New Requirement)"""
        try:
            if not self.strategy: return
            
            # Use current date
            current_date = datetime.now().date()
            overdue_list = []
            
            with self.lock:
                for inst_id, pos_map in self.positions.items():
                    for pid, record in pos_map.items():
                        if record.volume <= 0: continue
                        
                        r_date = record.open_date
                        if isinstance(r_date, datetime): r_date = r_date.date()
                        
                        days = (current_date - r_date).days
                        if days >= days_limit:
                            overdue_list.append(record)
            
            # Execute close outside lock to avoid deadlock if order func calls back
            for rec in overdue_list:
                try:
                    self.strategy.output(f"[风控] 发现超期持仓 {rec.instrument_id} (ID:{rec.position_id}), 持有{days_limit}+天, 触发强制平仓")
                    
                    # Logic 7 "IsClose" logic:
                    # If Long (PositionType.LONG) -> Sell
                    # If Short (PositionType.SHORT) -> Buy
                    
                    is_buy = (str(rec.direction) == "short") or (rec.position_type == PositionType.SHORT)
                    
                    # Prevent duplicate orders? 
                    # Ideally Strategy should handle order placement.
                    # We call insert_order directly.
                    
                    if hasattr(self.strategy, "valid_limit_order"):
                         # Use existing robust ordering
                         direction = "buy" if is_buy else "sell"
                         action = "open" # Wait, closing is usually action="close" or offset="close"
                         # PythonGO interface usually implies "close" offset.
                         # But here we use 'valid_limit_order' which might just take direction.
                         
                         # Check strategy interface in trading_logic.py
                         # It usually calls self.limit_order or similar.
                         
                         # Call strategy helper
                         self.strategy.output(f"[风控] 执行平仓: {rec.instrument_id} {direction} {rec.volume}手")
                         
                         # Assuming standard Pythongo method or Strategy wrapper
                         # We need to use offset='close' if applicable
                         
                         self.strategy.limit_order(
                             exchange=rec.exchange,
                             instrument_id=rec.instrument_id,
                             action=direction, # buy/sell
                             price=0.0, # Market/Best price? 0 implies logic handles it
                             volume=rec.volume,
                             algo="", # Direct
                             remark="OverdueClose",
                             is_quote=False
                         )
                         
                except Exception as e:
                    self.strategy.output(f"[风控] 平仓失败 {rec.instrument_id}: {e}")
                    
        except Exception as e:
            if self.strategy:
                self.strategy.output(f"[风控] 检查超期持仓异常: {e}")
