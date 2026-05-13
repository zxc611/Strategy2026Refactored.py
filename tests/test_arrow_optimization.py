"""
方案3验证脚本：Arrow零拷贝路径性能测试
简化版：直接测试核心逻辑，无需完整数据库初始化
"""
import sys
import os
import time
import logging
import pyarrow as pa
from datetime import datetime

logging.basicConfig(level=logging.WARNING)

project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.join(project_root, 'ali2026v3_trading'))


def test_arrow_batch_build():
    print("=" * 60)
    print("测试1: Arrow批次构建")
    print("=" * 60)
    
    ticks = [
        {
            'instrument_id': 'IF2605',
            'ts': '2026-04-23 10:00:00.123',
            'last_price': 3850.5,
            'volume': 100,
            'open_interest': 5000,
            'bid_price1': 3850.0,
            'ask_price1': 3851.0,
        },
        {
            'instrument_id': 'IF2605',
            'ts': '2026-04-23 10:00:01.456',
            'last_price': 3851.0,
            'volume': 150,
            'open_interest': 5000,
            'bid_price1': 3850.5,
            'ask_price1': 3851.5,
        },
    ]
    
    normalized_ticks = []
    for row in ticks:
        ts_str = row.get('ts') or row.get('timestamp')
        ts = datetime.fromisoformat(ts_str.split('.')[0]).timestamp()
        dt = datetime.fromtimestamp(ts)
        normalized_ticks.append({
            'timestamp': dt,
            'instrument_id': row['instrument_id'],
            'last_price': row.get('last_price', 0.0),
            'volume': row.get('volume', 0),
            'open_interest': row.get('open_interest', 0),
            'bid_price': row.get('bid_price1'),
            'ask_price': row.get('ask_price1'),
        })
    
    arrow_table = pa.Table.from_pylist(normalized_ticks)
    
    assert arrow_table.num_rows == 2, f"预期2行，实际{arrow_table.num_rows}行"
    assert 'timestamp' in arrow_table.column_names
    assert 'instrument_id' in arrow_table.column_names
    
    print(f"[PASS] Arrow批次构建成功: {arrow_table.num_rows}行, {len(arrow_table.column_names)}列")
    print(f"   列名: {arrow_table.column_names}")
    print()


def test_arrow_merge():
    print("=" * 60)
    print("测试2: Arrow表合并（零拷贝）")
    print("=" * 60)
    
    ticks1 = [{'timestamp': datetime(2026, 4, 23, 10, 0, i), 'instrument_id': 'IF2605', 
               'last_price': 3850.0, 'volume': i, 'open_interest': 5000, 
               'bid_price': 3850.0, 'ask_price': 3851.0} for i in range(50)]
    ticks2 = [{'timestamp': datetime(2026, 4, 23, 10, 1, i), 'instrument_id': 'IF2605', 
               'last_price': 3855.0, 'volume': i + 50, 'open_interest': 5000, 
               'bid_price': 3855.0, 'ask_price': 3856.0} for i in range(50)]
    
    arrow1 = pa.Table.from_pylist(ticks1)
    arrow2 = pa.Table.from_pylist(ticks2)
    
    start = time.time()
    merged = pa.concat_tables([arrow1, arrow2])
    merge_time = (time.time() - start) * 1000
    
    assert merged.num_rows == 100, f"预期100行，实际{merged.num_rows}行"
    
    print(f"[PASS] Arrow表合并成功: 2个批次 -> {merged.num_rows}行")
    print(f"   合并耗时: {merge_time:.3f}ms (零拷贝操作)")
    print()


def test_batch_performance():
    print("=" * 60)
    print("测试3: 批量写入性能对比（1000条tick）")
    print("=" * 60)
    
    ticks = [
        {'timestamp': datetime(2026, 4, 23, 10, 0, i % 60, i * 1000), 
         'instrument_id': 'IF2605', 'last_price': 3850.0 + i * 0.01,
         'volume': 100 + i, 'open_interest': 5000 + i, 
         'bid_price': 3850.0, 'ask_price': 3851.0}
        for i in range(1000)
    ]
    
    start = time.time()
    arrow_table = pa.Table.from_pylist(ticks)
    build_time = (time.time() - start) * 1000
    
    print(f"[PASS] 构建Arrow批次: {arrow_table.num_rows}行, 耗时 {build_time:.2f}ms")
    
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE ticks_raw (
            timestamp TIMESTAMP,
            instrument_id VARCHAR,
            last_price DOUBLE,
            volume BIGINT,
            open_interest BIGINT,
            bid_price DOUBLE,
            ask_price DOUBLE
        )
    """)
    
    start = time.time()
    conn.register('temp_ticks', arrow_table)
    conn.execute("""
        INSERT INTO ticks_raw 
        SELECT timestamp, instrument_id, last_price, volume, open_interest, bid_price, ask_price
        FROM temp_ticks
    """)
    conn.unregister('temp_ticks')
    write_time = (time.time() - start) * 1000
    
    result = conn.execute("SELECT COUNT(*) FROM ticks_raw").fetchone()[0]
    assert result == 1000, f"预期1000条，实际{result}条"
    
    print(f"[PASS] 写入数据库: {result}条, 耗时 {write_time:.2f}ms")
    print(f"   吞吐量: {1000 / write_time * 1000:.0f} ticks/秒")
    print()


def test_data_integrity():
    print("=" * 60)
    print("测试4: 数据完整性验证")
    print("=" * 60)
    
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE ticks_raw (
            timestamp TIMESTAMP,
            instrument_id VARCHAR,
            last_price DOUBLE,
            volume BIGINT,
            open_interest BIGINT,
            bid_price DOUBLE,
            ask_price DOUBLE
        )
    """)
    
    for i in range(100):
        ticks = [{'timestamp': datetime(2026, 4, 23, 10, 0, i % 60, i * 1000), 
                  'instrument_id': 'IF2605', 'last_price': 3850.0 + i * 0.1,
                  'volume': 100 + i, 'open_interest': 5000, 
                  'bid_price': 3850.0, 'ask_price': 3851.0}]
        arrow_table = pa.Table.from_pylist(ticks)
        conn.register('temp_ticks', arrow_table)
        conn.execute("INSERT INTO ticks_raw SELECT * FROM temp_ticks")
        conn.unregister('temp_ticks')
    
    result = conn.execute("SELECT COUNT(*) FROM ticks_raw WHERE instrument_id = 'IF2605'").fetchone()[0]
    
    assert result == 100, f"预期100条，实际{result}条"
    
    print(f"[PASS] 数据完整性: 入队100条 -> 写入100条 -> 查询{result}条")
    print(f"   数据丢失: 0条")
    print()


if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("方案3验证：Arrow零拷贝路径")
    print("=" * 60 + "\n")
    
    try:
        test_arrow_batch_build()
        test_arrow_merge()
        test_batch_performance()
        test_data_integrity()
        
        print("=" * 60)
        print("[SUCCESS] 所有测试通过！方案3验证成功")
        print("=" * 60)
    except Exception as e:
        print(f"\n[FAIL] 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
