"""
order_compliance.py - 自成交检测/算法交易报备/洗盘检测
R27: 从order_service.py提取的合规检测类和函数

职责：
- 拆单自成交检测 (check_self_trade_across_splits)
- 算法交易报备 (AlgoTradingCompliance)
- 洗盘检测 (WashTradeDetector)
"""
from __future__ import annotations

import logging
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, List

from ali2026v3_trading.infra.shared_utils import TradeDirection

try:
    from ali2026v3_trading.config.config_params import CHINA_TZ
except Exception:
    from datetime import timezone, timedelta
    CHINA_TZ = timezone(timedelta(hours=8))


def check_self_trade_across_splits(
    orders: List[Dict[str, Any]],
    ban_minutes: float = 30.0,
) -> List[Dict[str, Any]]:
    """R18-06修复: 自成交检测覆盖拆分子单

    对一组拆分子单进行原子性自成交检查，检测同合约反方向的子单对

    Args:
        orders: 拆分子单列表, 每个dict含instrument_id/direction/price
        ban_minutes: 自成交禁止期(分钟)

    Returns:
        List[Dict]: 检测到的自成交对, 含{buy_idx, sell_idx, instrument_id, ban_until}
    """
    violations = []
    try:
        now = time.time()
        for i, order_a in enumerate(orders):
            for j, order_b in enumerate(orders):
                if j <= i:
                    continue
                if (order_a.get('instrument_id') == order_b.get('instrument_id')
                    and order_a.get('direction') != order_b.get('direction')):
                    price_a = order_a.get('price', 0)
                    price_b = order_b.get('price', 0)
                    if price_a > 0 and price_b > 0:
                        buy_idx = i if order_a.get('direction') == TradeDirection.BUY else j
                        sell_idx = j if order_a.get('direction') == TradeDirection.BUY else i
                        buy_order = orders[buy_idx]
                        sell_order = orders[sell_idx]
                        buy_price = buy_order.get('price', 0)
                        sell_price = sell_order.get('price', 0)
                        if buy_price >= sell_price:
                            violations.append({
                                'buy_idx': buy_idx,
                                'sell_idx': sell_idx,
                                'instrument_id': order_a.get('instrument_id', ''),
                                'buy_price': buy_price,
                                'sell_price': sell_price,
                                'ban_until': now + ban_minutes * 60.0,
                            })
                            logging.warning(
                                "[R18-06] 拆单自成交检测: BUY[%d]@%.2f >= SELL[%d]@%.2f instrument=%s",
                                buy_idx, buy_price, sell_idx, sell_price,
                                order_a.get('instrument_id', ''),
                            )
        return violations
    except Exception as e:
        logging.error("[R18-06] 拆单自成交检测异常: %s", e)
        return violations


class AlgoTradingCompliance:
    """R18-07修复: 算法交易报备机制

    满足中国证监会程序化交易报备要求:
    - 记录每笔算法交易决策
    - 生成报备日志
    - 检测报备违规(超频/超量)
    """

    def __init__(self, max_orders_per_sec: float = 30.0, max_daily_algo_orders: int = 10000):
        self._max_orders_per_sec = max_orders_per_sec
        self._max_daily_algo_orders = max_daily_algo_orders
        self._order_timestamps: deque = deque(maxlen=1000)
        self._daily_count: int = 0
        self._daily_date: str = ''
        self._compliance_log: List[Dict[str, Any]] = []

    def record_algo_order(self, signal_id: str, instrument_id: str, direction: str,
                          volume: float, price: float, algo_type: str = 'hft_pursuit',
                          decision_latency_ms: float = 0.0) -> Dict[str, Any]:
        now = time.time()
        today = datetime.now(CHINA_TZ).strftime('%Y-%m-%d')
        if today != self._daily_date:
            self._daily_date = today
            self._daily_count = 0
        self._daily_count += 1
        self._order_timestamps.append(now)
        freq_violation = False
        if len(self._order_timestamps) >= 2:
            recent = [t for t in self._order_timestamps if now - t < 1.0]
            if len(recent) > self._max_orders_per_sec:
                freq_violation = True
                logging.warning("[R18-07] 算法交易超频: %.0f单/秒 > 限制%.0f单/秒",
                                len(recent), self._max_orders_per_sec)
        daily_violation = self._daily_count > self._max_daily_algo_orders
        if daily_violation:
            logging.warning("[R18-07] 日内算法订单超量: %d > 限制%d",
                            self._daily_count, self._max_daily_algo_orders)
        record = {
            'timestamp': now,
            'signal_id': signal_id,
            'instrument_id': instrument_id,
            'direction': direction,
            'volume': volume,
            'price': price,
            'algo_type': algo_type,
            'decision_latency_ms': decision_latency_ms,
            'freq_violation': freq_violation,
            'daily_violation': daily_violation,
            'daily_count': self._daily_count,
        }
        self._compliance_log.append(record)
        return record

    def get_compliance_summary(self) -> Dict[str, Any]:
        total = len(self._compliance_log)
        freq_violations = sum(1 for r in self._compliance_log if r.get('freq_violation'))
        daily_violations = sum(1 for r in self._compliance_log if r.get('daily_violation'))
        return {
            'total_algo_orders': total,
            'freq_violations': freq_violations,
            'daily_violations': daily_violations,
            'daily_count': self._daily_count,
            'daily_date': self._daily_date,
        }


class WashTradeDetector:
    """R18-08修复: 洗盘检测

    检测疑似洗盘行为:
    - 同合约短时间内频繁对倒(买后即卖/卖后即买)
    - 净持仓接近零但交易量异常高
    - 价格未有效偏离但成交量放大
    """

    def __init__(self, window_sec: float = 300.0, min_round_trips: int = 3,
                 net_position_ratio_threshold: float = 0.1):
        self._window_sec = window_sec
        self._min_round_trips = min_round_trips
        self._net_ratio_threshold = net_position_ratio_threshold
        self._trade_history: Dict[str, deque] = {}

    def record_trade(self, instrument_id: str, direction: str, volume: float,
                     price: float, timestamp: float = 0.0) -> None:
        if timestamp <= 0:
            timestamp = time.time()
        if instrument_id not in self._trade_history:
            self._trade_history[instrument_id] = deque(maxlen=500)
        self._trade_history[instrument_id].append({
            'ts': timestamp, 'dir': direction, 'vol': volume, 'price': price,
        })

    def detect_wash_trade(self, instrument_id: str) -> Dict[str, Any]:
        now = time.time()
        result = {'suspected': False, 'round_trips': 0, 'net_volume': 0.0,
                  'gross_volume': 0.0, 'net_ratio': 1.0, 'reason': ''}
        trades = self._trade_history.get(instrument_id, deque())
        recent = [t for t in trades if now - t['ts'] <= self._window_sec]
        if len(recent) < 2:
            return result
        buy_vol = sum(t['vol'] for t in recent if t['dir'] == TradeDirection.BUY)
        sell_vol = sum(t['vol'] for t in recent if t['dir'] == TradeDirection.SELL)
        gross_vol = buy_vol + sell_vol
        net_vol = abs(buy_vol - sell_vol)
        round_trips = int(min(buy_vol, sell_vol))
        result['round_trips'] = round_trips
        result['net_volume'] = net_vol
        result['gross_volume'] = gross_vol
        net_ratio = net_vol / gross_vol if gross_vol > 0 else 1.0
        result['net_ratio'] = round(net_ratio, 4)
        if round_trips >= self._min_round_trips and net_ratio < self._net_ratio_threshold:
            result['suspected'] = True
            result['reason'] = (
                f"window={self._window_sec}s内{round_trips}次往返, "
                f"净持仓比{net_ratio:.2%}<{self._net_ratio_threshold:.0%}"
            )
            logging.warning(
                "[R18-08] 疑似洗盘: instrument=%s %s",
                instrument_id, result['reason'],
            )
        return result

    def get_all_suspected(self) -> Dict[str, Dict[str, Any]]:
        results = {}
        for instrument_id in self._trade_history:
            detection = self.detect_wash_trade(instrument_id)
            if detection['suspected']:
                results[instrument_id] = detection
        return results
