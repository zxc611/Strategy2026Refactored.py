"""
期权合约加载诊断脚本

检查：
1. option_instruments表是否有数据
2. 配置中的option_products参数
3. SubscriptionManager是否尝试加载期权
"""

import duckdb
import os
import sys
from datetime import datetime

db_path = r"C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\data\ticks.duckdb"

def diagnose_option_loading():
    print("=" * 80)
    print("期权合约加载诊断报告")
    print("=" * 80)
    print(f"诊断时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在: {db_path}")
        print("   策略可能尚未启动或数据路径错误")
        return False
    
    conn = duckdb.connect(db_path)
    
    # 1. 检查option_instruments表
    print("【检查1】option_instruments表")
    try:
        count = conn.execute("SELECT COUNT(*) FROM option_instruments").fetchone()[0]
        print(f"  记录数: {count}")
        
        if count > 0:
            print("  ✅ 期权合约元数据已加载")
            
            # 按产品统计
            products = conn.execute("""
                SELECT product, COUNT(*) as cnt 
                FROM option_instruments 
                GROUP BY product 
                ORDER BY cnt DESC
            """).fetchall()
            
            print("  按产品分布:")
            for prod, cnt in products:
                print(f"    - {prod}: {cnt}个合约")
            
            # 显示样例
            sample = conn.execute("""
                SELECT instrument_id, product, strike_price, option_type, month
                FROM option_instruments 
                LIMIT 5
            """).fetchall()
            
            print("  样例:")
            for row in sample:
                print(f"    - {row[0]} (strike={row[2]}, type={row[3]}, month={row[4]})")
        else:
            print("  ❌ 期权表为空！")
            print("  可能原因:")
            print("    1. 配置中option_products参数为空")
            print("    2. SubscriptionManager加载失败")
            print("    3. 平台MarketCenter返回空列表")
    except Exception as e:
        print(f"  ❌ 查询失败: {e}")
        return False
    
    # 2. 检查futures_instruments表（对比）
    print("\n【检查2】futures_instruments表（对比）")
    try:
        fut_count = conn.execute("SELECT COUNT(*) FROM futures_instruments").fetchone()[0]
        print(f"  记录数: {fut_count}")
        
        if fut_count > 0:
            print("  ✅ 期货合约元数据已加载")
            
            products = conn.execute("""
                SELECT product, COUNT(*) as cnt 
                FROM futures_instruments 
                GROUP BY product 
                ORDER BY cnt DESC
            """).fetchall()
            
            print("  按产品分布:")
            for prod, cnt in products[:5]:  # 只显示前5个
                print(f"    - {prod}: {cnt}个合约")
        else:
            print("  ⚠️  期货表也为空")
    except Exception as e:
        print(f"  ❌ 查询失败: {e}")
    
    # 3. 检查是否有期权Tick数据
    print("\n【检查3】期权Tick数据")
    try:
        # 先检查ticks_raw表是否存在
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        if 'ticks_raw' not in table_names:
            print("  ⚠️  ticks_raw表不存在")
        else:
            tick_count = conn.execute("SELECT COUNT(*) FROM ticks_raw").fetchone()[0]
            print(f"  Tick总数: {tick_count}")
            
            if tick_count > 0 and count > 0:
                # 检查是否有任何期权Tick
                opt_tick_count = conn.execute("""
                    SELECT COUNT(DISTINCT t.instrument_id)
                    FROM ticks_raw t
                    INNER JOIN option_instruments o ON t.instrument_id = o.instrument_id
                """).fetchone()[0]
                
                if opt_tick_count > 0:
                    print(f"  ✅ 有{opt_tick_count}个不同期权合约的Tick数据")
                else:
                    print("  ❌ 没有期权Tick数据（虽然有期权合约元数据）")
                    print("  可能原因: 期权合约未被订阅")
            elif tick_count > 0 and count == 0:
                print("  ⚠️  有Tick数据但无期权合约元数据")
    except Exception as e:
        print(f"  ❌ 查询失败: {e}")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("诊断完成")
    print("=" * 80)
    
    # 给出建议
    print("\n📋 建议操作:")
    if count == 0:
        print("1. 检查config_service.py中的option_products配置")
        print("2. 查看策略启动日志中SubscriptionManager的加载过程")
        print("3. 确认平台MarketCenter支持配置的期权品种")
    else:
        print("1. 期权合约元数据正常")
        print("2. 如无期权Tick，检查订阅逻辑")
    
    return True

if __name__ == '__main__':
    success = diagnose_option_loading()
    sys.exit(0 if success else 1)
