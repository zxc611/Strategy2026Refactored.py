# MODULE_ID: M2-483
"""R4-F2: P1-44 监控指标双系统统一 断言测试

验证运行时行为:
1. PathCounter.record_call同步计数到MetricsRegistry
2. MetricsRegistry是统一指标存储入口
3. PathCounter保持自身统计能力
4. 双系统通过桥接不再完全独立
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_path_counter_syncs_to_metrics_registry():
    """P1-44验证: PathCounter.record_call同步计数到MetricsRegistry"""
    from ali2026v3_trading.infra.metrics_registry import get_metrics_registry, MetricsRegistry
    from ali2026v3_trading.infra.performance_monitor import PathCounter

    # 清理PathCounter状态
    PathCounter.reset()
    PathCounter.enable()

    # 获取MetricsRegistry并清理
    mr = get_metrics_registry()

    # 记录调用
    PathCounter.record_call('test_p1_44_path', execution_time=0.1)

    # 验证MetricsRegistry中有对应计数器
    counter_val = mr.get_counter('path_counter.test_p1_44_path')
    assert counter_val >= 1.0, \
        f"MetricsRegistry应有path_counter.test_p1_44_path计数，实际为{counter_val}"


def test_metrics_registry_is_unified_entry():
    """P1-44验证: MetricsRegistry是统一指标存储入口"""
    from ali2026v3_trading.infra.metrics_registry import MetricsRegistry, get_metrics_registry
    mr = get_metrics_registry()
    assert hasattr(mr, 'inc_counter'), "MetricsRegistry应有inc_counter方法"
    assert hasattr(mr, 'set_gauge'), "MetricsRegistry应有set_gauge方法"
    assert hasattr(mr, 'observe_histogram'), "MetricsRegistry应有observe_histogram方法"


def test_path_counter_retains_own_stats():
    """P1-44验证: PathCounter保持自身统计能力"""
    from ali2026v3_trading.infra.performance_monitor import PathCounter
    PathCounter.reset()
    PathCounter.enable()
    PathCounter.record_call('test_own_stats', execution_time=0.05)
    stats = PathCounter.get_stats(output_format='dict')
    assert isinstance(stats, dict), "PathCounter.get_stats应返回dict"


def test_bridge_in_source_code():
    """P1-44验证: PathCounter源码中包含MetricsRegistry桥接"""
    import inspect
    from ali2026v3_trading.infra.performance_monitor import PathCounter
    src = inspect.getsource(PathCounter.record_call)
    assert 'get_metrics_registry' in src, \
        "PathCounter.record_call应桥接到MetricsRegistry"
    assert 'inc_counter' in src, \
        "PathCounter.record_call应调用MetricsRegistry.inc_counter"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
