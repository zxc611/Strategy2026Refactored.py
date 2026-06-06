"""
order_market_impact.py - 市场冲击模型与成交概率
R27: 从order_service.py提取的市场冲击/成交概率计算函数

职责：
- Almgren-Chriss市场冲击成本模型 (almgren_chriss_impact)
- 成交概率建模 (estimate_fill_probability)
"""
from __future__ import annotations

import logging
import math
from typing import Dict


def almgren_chriss_impact(
    volume: float,
    avg_daily_volume: float,
    daily_volatility: float,
    participation_rate: float = 0.1,
    permanent_impact_factor: float = 0.1,
    temporary_impact_factor: float = 0.15,
    risk_aversion: float = 1.0,
) -> Dict[str, float]:
    """R18-04修复: Almgren-Chriss市场冲击成本模型

    计算最优执行轨迹下的临时冲击和永久冲击成本
    参考: Almgren & Chriss (2001), "Optimal Execution of Portfolio Transactions"

    Args:
        volume: 待执行订单量(手)
        avg_daily_volume: 日均成交量(手)
        daily_volatility: 日波动率
        participation_rate: 参与率 volume/avg_daily_volume
        permanent_impact_factor: 永久冲击系数(sigma)
        temporary_impact_factor: 临时冲击系数(alpha)
        risk_aversion: 风险厌恶系数(lambda)

    Returns:
        Dict: {total_impact_bps, permanent_impact_bps, temporary_impact_bps,
               optimal_horizon_sec, half_life_sec}
    """
    try:
        if avg_daily_volume <= 0 or volume <= 0 or daily_volatility <= 0:
            return {'total_impact_bps': 0.0, 'permanent_impact_bps': 0.0,
                    'temporary_impact_bps': 0.0, 'optimal_horizon_sec': 0.0,
                    'half_life_sec': 0.0}
        sigma = daily_volatility
        X = volume
        V = avg_daily_volume
        actual_participation = X / V if V > 0 else participation_rate
        permanent_impact_bps = permanent_impact_factor * sigma * math.sqrt(actual_participation) * 10000
        temporary_impact_bps = temporary_impact_factor * sigma * (actual_participation ** 0.6) * 10000
        total_impact_bps = permanent_impact_bps + temporary_impact_bps
        if risk_aversion > 0 and sigma > 0 and temporary_impact_factor > 0:
            optimal_horizon_sec = math.sqrt(
                (risk_aversion * sigma ** 2 * X) / (2 * temporary_impact_factor * (V / 6.5))
            ) * 3600 if V > 0 else 0.0
        else:
            optimal_horizon_sec = 0.0
        half_life_sec = optimal_horizon_sec / 2.0 if optimal_horizon_sec > 0 else 0.0
        return {
            'total_impact_bps': round(total_impact_bps, 4),
            'permanent_impact_bps': round(permanent_impact_bps, 4),
            'temporary_impact_bps': round(temporary_impact_bps, 4),
            'optimal_horizon_sec': round(optimal_horizon_sec, 2),
            'half_life_sec': round(half_life_sec, 2),
        }
    except Exception as e:
        logging.error("[R18-04] Almgren-Chriss计算异常: %s", e)
        return {'total_impact_bps': 0.0, 'permanent_impact_bps': 0.0,
                'temporary_impact_bps': 0.0, 'optimal_horizon_sec': 0.0,
                'half_life_sec': 0.0}


def estimate_fill_probability(
    order_price: float,
    best_opposite_price: float,
    spread_bps: float,
    volume: float,
    avg_daily_volume: float,
    time_horizon_sec: float = 60.0,
    queue_position: int = 0,
) -> float:
    """R18-05修复: 成交概率建模

    基于价格距离、参与率、队列位置的三维成交概率估计

    Args:
        order_price: 挂单价格
        best_opposite_price: 对盘最优价(买入用ask, 卖出用bid)
        spread_bps: 价差基点
        volume: 挂单量
        avg_daily_volume: 日均成交量
        time_horizon_sec: 时间窗口(秒)
        queue_position: 队列位置(0=队首)

    Returns:
        float: 成交概率 [0, 1]
    """
    try:
        if avg_daily_volume <= 0 or time_horizon_sec <= 0:
            return 1.0
        participation_rate = volume / avg_daily_volume
        queue_factor = 1.0 / (1.0 + queue_position * 0.1)
        if best_opposite_price > 0 and order_price > 0:
            price_distance_bps = abs(order_price - best_opposite_price) / best_opposite_price * 10000
            if price_distance_bps <= 0:
                price_factor = 1.0
            elif price_distance_bps <= spread_bps:
                price_factor = math.exp(-0.5 * price_distance_bps / max(spread_bps, 0.01))
            else:
                price_factor = math.exp(-2.0 * (price_distance_bps - spread_bps) / max(spread_bps, 0.01))
        else:
            price_factor = 0.5
        time_factor = 1.0 - math.exp(-time_horizon_sec / 60.0)
        participation_penalty = 1.0 / (1.0 + participation_rate * 10.0)
        fill_prob = price_factor * queue_factor * time_factor * participation_penalty
        return max(0.0, min(1.0, fill_prob))
    except Exception as e:
        logging.error("[R18-05] 成交概率计算异常: %s", e)
        return 0.5
