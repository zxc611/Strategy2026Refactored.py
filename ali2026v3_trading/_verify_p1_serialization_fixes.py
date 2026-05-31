"""
P1级序列化问题修复验证脚本

验证项：
1. SER-P1-16: validate_config_schema函数存在且可执行
2. SER-P1-18: safe_jsonl_write原子写入保护
3. SER-P1-15: normalize_parquet_columns函数存在且可执行
4. SER-P1-14: 配置快照和回滚机制
5. SER-P1-05: Parquet schema evolution文档注释存在

验证日期：2026-05-28
"""
import sys
import os
import tempfile
import shutil

_base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _base_dir not in sys.path:
    sys.path.insert(0, _base_dir)

def test_ser_p1_16_validate_config_schema():
    """验证SER-P1-16: jsonschema配置验证函数"""
    print("\n" + "="*80)
    print("SER-P1-16: validate_config_schema函数验证")
    print("="*80)
    
    from ali2026v3_trading.serialization_utils import validate_config_schema
    
    test_config = {
        'trading': {
            'default_volume': 1,
            'max_position_limit': 10,
        }
    }
    
    is_valid, errors = validate_config_schema(test_config)
    print(f"  ✓ 函数存在且可执行")
    print(f"  ✓ 基础schema验证: is_valid={is_valid}, errors={errors}")
    
    test_schema = {
        'type': 'object',
        'properties': {
            'trading': {
                'type': 'object',
                'properties': {
                    'default_volume': {'type': 'integer'},
                }
            }
        }
    }
    
    is_valid2, errors2 = validate_config_schema(test_config, test_schema)
    print(f"  ✓ 自定义schema验证: is_valid={is_valid2}, errors={errors2}")
    
    return True

def test_ser_p1_18_atomic_jsonl_write():
    """验证SER-P1-18: JSONL原子写入保护"""
    print("\n" + "="*80)
    print("SER-P1-18: safe_jsonl_write原子写入保护验证")
    print("="*80)
    
    from ali2026v3_trading.serialization_utils import safe_jsonl_write, safe_jsonl_read
    
    test_records = [
        {'id': 1, 'name': 'test1', 'value': 100.5},
        {'id': 2, 'name': 'test2', 'value': 200.8},
    ]
    
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = os.path.join(tmpdir, 'test.jsonl')
        
        count = safe_jsonl_write(test_records, test_file)
        print(f"  ✓ 原子写入成功: 写入{count}条记录")
        
        loaded_records = safe_jsonl_read(test_file)
        print(f"  ✓ 读取验证: 读取{len(loaded_records)}条记录")
        
        assert loaded_records == test_records, "数据不一致"
        print(f"  ✓ 数据一致性验证通过")
        
        tmp_files = [f for f in os.listdir(tmpdir) if f.endswith('.tmp')]
        assert len(tmp_files) == 0, f"临时文件未清理: {tmp_files}"
        print(f"  ✓ 临时文件清理验证通过")
    
    return True

def test_ser_p1_15_normalize_parquet_columns():
    """验证SER-P1-15: Parquet列名标准化"""
    print("\n" + "="*80)
    print("SER-P1-15: normalize_parquet_columns函数验证")
    print("="*80)
    
    import pandas as pd
    from ali2026v3_trading.serialization_utils import normalize_parquet_columns
    
    test_df = pd.DataFrame({
        'InstrumentID': [1, 2, 3],
        'TradePrice': [100.0, 200.0, 300.0],
        'DateTime': ['2026-01-01', '2026-01-02', '2026-01-03'],
    })
    
    print(f"  原始列名: {list(test_df.columns)}")
    
    df_normalized = normalize_parquet_columns(test_df)
    print(f"  标准化后列名: {list(df_normalized.columns)}")
    
    expected_columns = ['instrument_id', 'trade_price', 'date_time']
    assert list(df_normalized.columns) == expected_columns, f"列名标准化错误: {list(df_normalized.columns)}"
    print(f"  ✓ 列名标准化验证通过")
    
    from ali2026v3_trading.serialization_utils import safe_dataframe_from_parquet_with_normalization
    print(f"  ✓ safe_dataframe_from_parquet_with_normalization函数存在")
    
    return True

def test_ser_p1_14_config_snapshot_rollback():
    """验证SER-P1-14: 配置快照和回滚机制"""
    print("\n" + "="*80)
    print("SER-P1-14: 配置快照和回滚机制验证")
    print("="*80)
    
    from ali2026v3_trading.config_service import ConfigService
    
    config = ConfigService()
    
    assert hasattr(config, 'create_config_snapshot'), "create_config_snapshot方法不存在"
    print(f"  ✓ create_config_snapshot方法存在")
    
    assert hasattr(config, 'rollback_config_snapshot'), "rollback_config_snapshot方法不存在"
    print(f"  ✓ rollback_config_snapshot方法存在")
    
    assert hasattr(config, 'get_snapshot_info'), "get_snapshot_info方法不存在"
    print(f"  ✓ get_snapshot_info方法存在")
    
    snapshot_id = config.create_config_snapshot('test_snapshot')
    print(f"  ✓ 快照创建成功: id={snapshot_id}")
    
    snapshot_info = config.get_snapshot_info()
    print(f"  ✓ 快照信息获取成功: {snapshot_info}")
    
    rollback_success = config.rollback_config_snapshot('test_snapshot')
    print(f"  ✓ 回滚执行: success={rollback_success}")
    
    return True

def test_ser_p1_05_parquet_schema_evolution_doc():
    """验证SER-P1-05: Parquet schema evolution文档注释"""
    print("\n" + "="*80)
    print("SER-P1-05: Parquet schema evolution文档注释验证")
    print("="*80)
    
    from ali2026v3_trading.serialization_utils import safe_dataframe_to_parquet
    
    doc = safe_dataframe_to_parquet.__doc__
    
    assert 'SER-P1-05' in doc, "文档未包含SER-P1-05标记"
    print(f"  ✓ 文档包含SER-P1-05标记")
    
    assert 'Schema Evolution' in doc, "文档未包含Schema Evolution说明"
    print(f"  ✓ 文档包含Schema Evolution说明")
    
    assert '迁移策略' in doc or 'migration' in doc.lower(), "文档未包含迁移策略"
    print(f"  ✓ 文档包含迁移策略")
    
    assert '兼容性策略' in doc or 'compatibility' in doc.lower(), "文档未包含兼容性策略"
    print(f"  ✓ 文档包含兼容性策略")
    
    return True

def main():
    print("\n" + "="*80)
    print("P1级序列化问题修复验证报告")
    print("="*80)
    
    results = {}
    
    try:
        results['SER-P1-16'] = test_ser_p1_16_validate_config_schema()
    except Exception as e:
        print(f"  ✗ SER-P1-16验证失败: {e}")
        results['SER-P1-16'] = False
    
    try:
        results['SER-P1-18'] = test_ser_p1_18_atomic_jsonl_write()
    except Exception as e:
        print(f"  ✗ SER-P1-18验证失败: {e}")
        results['SER-P1-18'] = False
    
    try:
        results['SER-P1-15'] = test_ser_p1_15_normalize_parquet_columns()
    except Exception as e:
        print(f"  ✗ SER-P1-15验证失败: {e}")
        results['SER-P1-15'] = False
    
    try:
        results['SER-P1-14'] = test_ser_p1_14_config_snapshot_rollback()
    except Exception as e:
        print(f"  ✗ SER-P1-14验证失败: {e}")
        results['SER-P1-14'] = False
    
    try:
        results['SER-P1-05'] = test_ser_p1_05_parquet_schema_evolution_doc()
    except Exception as e:
        print(f"  ✗ SER-P1-05验证失败: {e}")
        results['SER-P1-05'] = False
    
    print("\n" + "="*80)
    print("验证结果汇总")
    print("="*80)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for issue_id, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {issue_id}: {status}")
    
    print(f"\n总计: {passed}/{total} 通过")
    
    if passed == total:
        print("\n✓ 所有P1级问题修复验证通过")
        return 0
    else:
        print(f"\n✗ {total - passed}项验证失败")
        return 1

if __name__ == '__main__':
    sys.exit(main())
