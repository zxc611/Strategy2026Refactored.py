# MODULE_ID: M2-475
"""R2-F3: P1-08 双调度器边界明确化 断言测试

验证运行时行为:
1. StrategyScheduler拥有界try_delegate_to_scheduler_service方法
2. 委托成功时job元数据标记delegated_to='SchedulerService'
3. SchedulerService不可用时回退到APScheduler
4. _delegated_jobs映射正确记录委托关系
"""
import os
import sys
import pytest

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


def test_strategy_scheduler_has_delegate_method():
    """P1-08验证: StrategyScheduler拥有界try_delegate_to_scheduler_service方法"""
    from ali2026v3_trading.strategy.strategy_scheduler import StrategyScheduler
    ss = StrategyScheduler()
    assert hasattr(ss, '_try_delegate_to_scheduler_service'), \
        "StrategyScheduler应拥有界try_delegate_to_scheduler_service方法"
    assert callable(getattr(ss, '_try_delegate_to_scheduler_service')), \
        "_try_delegate_to_scheduler_service应为可调用方法"


def test_delegate_returns_false_when_scheduler_service_unavailable():
    """P1-08验证: SchedulerService不可用时委托返回False，回退到APScheduler"""
    from ali2026v3_trading.strategy.strategy_scheduler import StrategyScheduler
    ss = StrategyScheduler()

    def _dummy_func():
        pass

    result = ss._try_delegate_to_scheduler_service(_dummy_func, 'interval', 'test_job', seconds=30)
    # SchedulerService不在service_container中，应返回False
    assert result is False, \
        f"SchedulerService不可用时委托应返回False，实际返回{result}"


def test_delegated_jobs_dict_exists():
    """P1-08验证: _delegated_jobs映射存在"""
    from ali2026v3_trading.strategy.strategy_scheduler import StrategyScheduler
    ss = StrategyScheduler()
    assert hasattr(ss, '_delegated_jobs'), \
        "StrategyScheduler应拥有界delegated_jobs属性"
    assert isinstance(ss._delegated_jobs, dict), \
        "_delegated_jobs应为dict类型"


def test_add_job_with_owner_marks_delegated_when_service_available():
    """P1-08验证: 当SchedulerService可用时，add_job_with_owner标记delegated_to"""
    from ali2026v3_trading.strategy.strategy_scheduler import StrategyScheduler

    # Mock _try_delegate_to_scheduler_service to return True
    ss = StrategyScheduler()

    def _mock_delegate(func, trigger, job_id, **trigger_args):
        return True

    _original = ss._try_delegate_to_scheduler_service
    ss._try_delegate_to_scheduler_service = _mock_delegate

    try:
        def _dummy_func():
            pass

        ss.add_job_with_owner(
            _dummy_func, 'interval', 'test_job_1',
            strategy_id='test_strategy', seconds=30
        )

        # 验证元数据标记
        assert 'test_job_1' in ss._delegated_jobs, \
            "test_job_1应在。delegated_jobs中"
        assert ss._delegated_jobs['test_job_1'] == 'SchedulerService', \
            f"委托目标应为'SchedulerService'，实际为'{ss._delegated_jobs['test_job_1']}'"
        assert ss._job_owners['test_job_1']['delegated_to'] == 'SchedulerService', \
            "job_owners元数据应标记delegated_to='SchedulerService'"
    finally:
        ss._try_delegate_to_scheduler_service = _original


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
