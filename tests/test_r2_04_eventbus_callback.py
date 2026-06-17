# MODULE_ID: M2-519
"""P1-05: 事件/回调统一到EventBus断言测试
验证strategy_scheduler.py中关键job回调添加了EventBus事件发布逻辑。
"""
import ast
import os


def _read_file(rel_path: str) -> str:
    base = os.path.join(os.path.dirname(__file__), '..')
    full = os.path.abspath(os.path.join(base, rel_path))
    with open(full, 'r', encoding='utf-8') as f:
        return f.read()


def test_scheduler_has_eventbus_publish_method():
    """StrategyScheduler有_publish_scheduler_event方法"""
    content = _read_file('strategy/strategy_scheduler.py')
    assert '_publish_scheduler_event' in content, \
        "StrategyScheduler缺少_publish_scheduler_event方法"
    assert 'def _publish_scheduler_event' in content, \
        "StrategyScheduler缺少_publish_scheduler_event方法定义"


def test_scheduler_eventbus_uses_lazy_import():
    """EventBus发布使用延迟导入避免循环依赖"""
    content = _read_file('strategy/strategy_scheduler.py')
    # 在_publish_scheduler_event方法内查找延迟导入
    lines = content.split('\n')
    in_method = False
    found_lazy_import = False
    for line in lines:
        if 'def _publish_scheduler_event' in line:
            in_method = True
        elif in_method:
            if 'from ali2026v3_trading.infra.event_bus import EventBus' in line:
                found_lazy_import = True
                break
            if line.strip().startswith('def ') and '_publish_scheduler_event' not in line:
                break
    assert found_lazy_import, "_publish_scheduler_event未使用延迟导入EventBus"


def test_trading_cycle_job_publishes_event():
    """交易循环job发布scheduler.trading_cycle事件"""
    content = _read_file('strategy/strategy_scheduler.py')
    assert '_trading_loop_job_with_event' in content, \
        "缺少_trading_loop_job_with_event包装函数"
    assert "'trading_cycle'" in content, \
        "缺少trading_cycle事件名"


def test_risk_check_job_publishes_event():
    """风控检查job发布scheduler.risk_check事件"""
    content = _read_file('strategy/strategy_scheduler.py')
    assert '_risk_check_job_with_event' in content, \
        "缺少_risk_check_job_with_event包装函数"
    assert "'risk_check'" in content, \
        "缺少risk_check事件名"


def test_pending_order_job_publishes_event():
    """挂单检查job发布scheduler.pending_order_check事件"""
    content = _read_file('strategy/strategy_scheduler.py')
    assert '_pending_order_job_with_event' in content, \
        "缺少_pending_order_job_with_event包装函数"
    assert "'pending_order_check'" in content, \
        "缺少pending_order_check事件名"


def test_scheduler_module_docstring_has_p105_note():
    """模块docstring包含P1-05说明"""
    content = _read_file('strategy/strategy_scheduler.py')
    assert 'P1-05' in content, "strategy_scheduler.py缺少P1-05标注"
    assert 'EventBus' in content, "strategy_scheduler.py缺少EventBus说明"


def test_scheduler_file_parseable():
    """strategy_scheduler.py可正常解析为AST"""
    content = _read_file('strategy/strategy_scheduler.py')
    try:
        ast.parse(content)
    except SyntaxError as e:
        raise AssertionError(f"strategy_scheduler.py语法错误: {e}")


def test_event_publish_in_finally_block():
    """EventBus事件发布在finally块中，确保即使回调异常也能发布"""
    content = _read_file('strategy/strategy_scheduler.py')
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if '_trading_loop_job_with_event' in line and 'def ' in line:
            # 检查后续行有finally和_publish_scheduler_event
            block = '\n'.join(lines[i:i+10])
            assert 'finally' in block, \
                "_trading_loop_job_with_event缺少finally块"
            assert '_publish_scheduler_event' in block, \
                "_trading_loop_job_with_event的finally块中缺少事件发布"
            break
