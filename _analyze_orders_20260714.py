#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""分析2026-07-14日的模拟下单数据"""

import json
import re
from collections import defaultdict
from pathlib import Path

LOGS_DIR = Path(r"c:\Users\xu\AppData\Roaming\InfiniTrader_QhZijintianfengPythonX64\logs")
ORDERS_FILE = LOGS_DIR / "orders.jsonl"
STRATEGY_LOG = LOGS_DIR / "strategy.log"

# 策略名称映射
STRATEGY_MAP = {
    "high_freq": "S1-高频策略",
    "resonance": "S2-共振策略",
    "spring": "S3-Spring策略",
    "box": "S4-箱体策略",
    "arbitrage": "S5-套利策略",
    "market_making": "S6-做市策略",
    "divergence_reversal": "S7-反转策略",
}

def parse_orders():
    """解析orders.jsonl"""
    stats = defaultdict(lambda: {"OPEN": 0, "CLOSE": 0})
    
    if not ORDERS_FILE.exists():
        print(f"警告: {ORDERS_FILE} 不存在")
        return stats
    
    total_lines = 0
    matched_lines = 0
    
    with open(ORDERS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            total_lines += 1
            try:
                order = json.loads(line.strip())
                timestamp = order.get('timestamp', '')
                if '2026-07-14' not in timestamp:
                    continue
                
                matched_lines += 1
                order_id = order.get('order_id', '')
                order_type = order.get('order_type', 'UNKNOWN')
                
                # 从order_id中提取策略名
                # 格式: instrument_strategy_timestamp_hash (如: fu2608P2900_high_freq_1783997248808743_1406860e)
                # 注意：策略名可能包含下划线（如high_freq, divergence_reversal）
                parts = order_id.split('_')
                strategy_key = "unknown"
                
                # 检查单部分策略名
                for i in range(len(parts)):
                    if parts[i] in ['resonance', 'spring', 'box', 'arbitrage', 'market_making']:
                        strategy_key = parts[i]
                        break
                    # 检查两部分组合的策略名
                    if i + 1 < len(parts):
                        combined = f"{parts[i]}_{parts[i+1]}"
                        if combined in ['high_freq', 'divergence_reversal']:
                            strategy_key = combined
                            break
                
                stats[strategy_key][order_type] += 1
            except Exception as e:
                print(f"解析错误: {e}")
                continue
    
    print(f"\n日志解析完成: orders.jsonl共{total_lines}行, 14日匹配{matched_lines}条")
    return stats

def parse_shadow_trades():
    """解析strategy.log中的SHADOW_TRADE"""
    shadow_stats = defaultdict(lambda: {"shadow_a": {"OPEN": 0, "CLOSE": 0}, "shadow_b": {"OPEN": 0, "CLOSE": 0}})
    
    if not STRATEGY_LOG.exists():
        print(f"警告: {STRATEGY_LOG} 不存在")
        return shadow_stats
    
    # SHADOW_TRADE日志格式: [SHADOW_TRADE] {"shadow_type": "s2_resonance_shadow_a", "is_open": true/false, ...}
    shadow_pattern = re.compile(r'\[SHADOW_TRADE\]\s+(\{.*\})')
    
    with open(STRATEGY_LOG, 'r', encoding='utf-8') as f:
        for line in f:
            if '2026-07-14' not in line:
                continue
            if 'SHADOW_TRADE' not in line:
                continue
            
            match = shadow_pattern.search(line)
            if match:
                try:
                    trade_json = json.loads(match.group(1))
                    shadow_type = trade_json.get('shadow_type', '')
                    is_open = trade_json.get('is_open', False)
                    
                    # 判断是shadow_a还是shadow_b
                    if 'shadow_a' in shadow_type:
                        shadow_id = 'shadow_a'
                    elif 'shadow_b' in shadow_type:
                        shadow_id = 'shadow_b'
                    else:
                        continue
                    
                    action = "OPEN" if is_open else "CLOSE"
                    shadow_stats['resonance'][shadow_id][action] += 1
                except Exception as e:
                    continue
    
    return shadow_stats

def main():
    print("=" * 80)
    print("2026-07-14 7策略模拟下单数据统计")
    print("=" * 80)
    
    # 解析订单
    order_stats = parse_orders()
    
    # 解析影子交易
    shadow_stats = parse_shadow_trades()
    
    # 打印统计结果
    print("\n【一、普通模拟下单统计】")
    print("-" * 60)
    print(f"{'策略':<20} {'开仓(OPEN)':<15} {'平仓(CLOSE)':<15} {'合计':<10}")
    print("-" * 60)
    
    total_open = 0
    total_close = 0
    
    for strategy_key in ['high_freq', 'resonance', 'spring', 'box', 'arbitrage', 'market_making', 'divergence_reversal']:
        strategy_name = STRATEGY_MAP.get(strategy_key, strategy_key)
        open_count = order_stats.get(strategy_key, {}).get('OPEN', 0)
        close_count = order_stats.get(strategy_key, {}).get('CLOSE', 0)
        total = open_count + close_count
        
        total_open += open_count
        total_close += close_count
        
        print(f"{strategy_name:<20} {open_count:<15} {close_count:<15} {total:<10}")
    
    print("-" * 60)
    print(f"{'合计':<20} {total_open:<15} {total_close:<15} {total_open + total_close:<10}")
    
    # 影子交易统计
    print("\n【二、影子模拟下单统计 (S2-共振策略)】")
    print("-" * 60)
    print(f"{'影子类型':<15} {'开仓(OPEN)':<15} {'平仓(CLOSE)':<15} {'合计':<10}")
    print("-" * 60)
    
    shadow_total_open = 0
    shadow_total_close = 0
    
    for shadow_id in ['shadow_a', 'shadow_b']:
        open_count = shadow_stats['resonance'][shadow_id]['OPEN']
        close_count = shadow_stats['resonance'][shadow_id]['CLOSE']
        total = open_count + close_count
        
        shadow_total_open += open_count
        shadow_total_close += close_count
        
        print(f"{shadow_id:<15} {open_count:<15} {close_count:<15} {total:<10}")
    
    print("-" * 60)
    print(f"{'影子合计':<15} {shadow_total_open:<15} {shadow_total_close:<15} {shadow_total_open + shadow_total_close:<10}")
    
    # 总计
    print("\n【三、全部模拟下单统计】")
    print("-" * 60)
    grand_total_open = total_open + shadow_total_open
    grand_total_close = total_close + shadow_total_close
    print(f"普通模拟: 开仓 {total_open}, 平仓 {total_close}, 合计 {total_open + total_close}")
    print(f"影子模拟: 开仓 {shadow_total_open}, 平仓 {shadow_total_close}, 合计 {shadow_total_open + shadow_total_close}")
    print(f"全部合计: 开仓 {grand_total_open}, 平仓 {grand_total_close}, 总计 {grand_total_open + grand_total_close}")
    print("=" * 80)

if __name__ == '__main__':
    main()