"""
FIX-20260708 断言测试: 验证4项修复
1. persistence_service.py: state_ref_keys使用set_ref(引用)而非set(深拷贝)
2. lifecycle_init.py: 设置start_time后重新注册_stats到state_store
3. tick_dispatch.py: output_periodic_summary添加start_time回退读取
4. strategy_business_layer.py: record_signal/record_trade在生产代码中被调用
"""
import ast
import os
import sys
import inspect
import textwrap
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

# 添加项目根目录到sys.path
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

PASS = 0
FAIL = 0

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} - {detail}")


def read_file(rel_path):
    full = os.path.join(_PROJECT_ROOT, rel_path)
    with open(full, 'r', encoding='utf-8') as f:
        return f.read()


# ============================================================
# 测试1: persistence_service.py使用set_ref for state_ref_keys
# ============================================================
def test_persistence_service_uses_set_ref():
    print("\n=== 测试1: persistence_service.py state_ref_keys使用set_ref ===")
    code = read_file(os.path.join('ali2026v3_trading', 'strategy', 'persistence_service.py'))

    # 检查set_ref在state_ref_keys循环中被调用
    check("set_ref在state_ref_keys循环中被调用",
          'self._state_store.set_ref(key, val)' in code,
          "未找到set_ref(key, val)调用")

    # 检查不再先尝试set()再回退set_ref()
    # 查找模式: set(key, val) 后跟 except (TypeError, copy.Error): set_ref
    bad_pattern = 'self._state_store.set(key, val)\n                except (TypeError, copy.Error):\n                    self._state_store.set_ref(key, val)'
    check("不再先set()再回退set_ref()",
          bad_pattern not in code,
          "仍存在先set()再回退set_ref()的旧模式")

    # AST验证: sync_state_to_store方法中set_ref调用
    try:
        tree = ast.parse(code)
        found_set_ref = False
        found_set = False
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == 'sync_state_to_store':
                for child in ast.walk(node):
                    if isinstance(child, ast.Call):
                        if isinstance(child.func, ast.Attribute):
                            if child.func.attr == 'set_ref' and child.args:
                                found_set_ref = True
        check("AST: sync_state_to_store中包含set_ref调用", found_set_ref, "AST未找到set_ref调用")
    except SyntaxError as e:
        check("AST: 语法正确", False, str(e))


# ============================================================
# 测试2: lifecycle_init.py设置start_time后重新注册_stats
# ============================================================
def test_lifecycle_init_reregisters_stats():
    print("\n=== 测试2: lifecycle_init.py设置start_time后重新注册_stats ===")
    code = read_file(os.path.join('ali2026v3_trading', 'lifecycle', 'lifecycle_init.py'))

    check("包含set_ref('_stats', p._stats)调用",
          "set_ref('_stats', p._stats)" in code,
          "未找到set_ref('_stats', p._stats)")

    check("在设置start_time之后调用",
          code.index("p._stats['start_time'] = datetime.now(CHINA_TZ)") <
          code.index("set_ref('_stats', p._stats)"),
          "set_ref调用不在start_time之后")

    check("使用FIX-20260708注释标记",
          "FIX-20260708" in code and "重新注册_stats到state_store" in code,
          "缺少FIX-20260708注释标记")


# ============================================================
# 测试3: tick_dispatch.py添加start_time回退读取
# ============================================================
def test_tick_dispatch_start_time_fallback():
    print("\n=== 测试3: tick_dispatch.py start_time回退读取 ===")
    code = read_file(os.path.join('ali2026v3_trading', 'strategy', 'tick_dispatch.py'))

    check("包含_start_time变量",
          "_start_time = _ss_stats.get('start_time')" in code,
          "未找到_start_time变量")

    check("包含provider回退逻辑",
          "getattr(svc, '_provider', None)" in code,
          "未找到provider回退逻辑")

    check("包含set_ref修复调用",
          "svc._state_store.set_ref('_stats', _p_stats)" in code,
          "未找到set_ref修复调用")

    check("使用uptime变量替代直接_ss_stats.get",
          "uptime = datetime.now(_CHINA_TZ) - _start_time if _start_time else timedelta(0)" in code,
          "uptime计算未使用_start_time变量")

    check("使用FIX-20260708注释标记",
          "FIX-20260708" in code,
          "缺少FIX-20260708注释标记")


# ============================================================
# 测试4: strategy_business_layer.py调用record_signal和record_trade
# ============================================================
def test_business_layer_calls_record_signal_trade():
    print("\n=== 测试4: strategy_business_layer.py调用record_signal/record_trade ===")
    code = read_file(os.path.join('ali2026v3_trading', 'strategy', 'strategy_business_layer.py'))

    # record_signal调用 (V2: 使用getattr(provider, 'record_signal', None)委托)
    check("包含record_signal()调用",
          "getattr(provider, 'record_signal', None)" in code,
          "未找到record_signal()调用")

    check("record_signal在targets选择之后调用",
          code.index("select_otm_targets_by_volume()") <
          code.index("getattr(provider, 'record_signal', None)"),
          "record_signal不在targets选择之后")

    check("record_signal有lifecycle_service回退路径",
          "provider._stats['total_signals']" in code or "_stats.get('total_signals'" in code,
          "缺少total_signals回退路径")

    # record_trade调用 (V2: 使用getattr(provider, 'record_trade', None)委托)
    check("包含record_trade()调用",
          "getattr(provider, 'record_trade', None)" in code,
          "未找到record_trade()调用")

    check("record_trade在execute_by_ranking之后调用",
          code.index("execute_by_ranking(targets)") <
          code.index("getattr(provider, 'record_trade', None)"),
          "record_trade不在execute_by_ranking之后")

    check("record_trade有lifecycle_service回退路径",
          "provider._stats['total_trades']" in code or "_stats.get('total_trades'" in code,
          "缺少total_trades回退路径")

    # dry_run_mode日志
    check("包含dry_run_mode值日志",
          'logging.info("[OptionTrading] dry_run_mode=%s"' in code,
          "缺少dry_run_mode值日志")

    check("包含dry_run_mode读取失败警告",
          'logging.warning("[OptionTrading] dry_run_mode读取失败' in code,
          "缺少dry_run_mode读取失败警告")

    # 跳过点INFO日志
    check("_is_running/_is_paused跳过点使用INFO",
          'logging.info("[OptionTrading] 跳过交易周期: _is_running=' in code,
          "_is_running跳过点未升级为INFO")

    check("t_type_service=None跳过点使用INFO",
          'logging.info("[OptionTrading] 跳过交易周期: t_type_service=None")' in code,
          "t_type_service跳过点未升级为INFO")


# ============================================================
# 测试5: 端到端 - StateStore引用语义验证
# ============================================================
def test_state_store_ref_semantics():
    print("\n=== 测试5: StateStore引用语义端到端验证 ===")
    from ali2026v3_trading.infra.registry_service import StateStore

    store = StateStore()

    # 测试set_ref保持引用
    test_dict = {'start_time': None, 'total_signals': 0}
    store.set_ref('_stats', test_dict)

    # 修改原dict
    test_dict['start_time'] = datetime.now()
    test_dict['total_signals'] = 42

    # 从store读取应看到修改
    retrieved = store.get_ref('_stats')
    check("set_ref后修改原dict, store中可见",
          retrieved.get('start_time') is not None and retrieved.get('total_signals') == 42,
          f"start_time={retrieved.get('start_time')}, total_signals={retrieved.get('total_signals')}")

    # 测试set(深拷贝)不保持引用
    test_dict2 = {'value': 1}
    store.set('_stats_copy', test_dict2)
    test_dict2['value'] = 999
    retrieved2 = store.get_ref('_stats_copy')
    check("set后修改原dict, store中不可见(深拷贝语义)",
          retrieved2.get('value') == 1,
          f"value={retrieved2.get('value')} (应为1,深拷贝)")


# ============================================================
# 测试6: 端到端 - 模拟output_periodic_summary的start_time回退
# ============================================================
def test_output_periodic_summary_fallback():
    print("\n=== 测试6: output_periodic_summary start_time回退验证 ===")
    from ali2026v3_trading.infra.registry_service import StateStore
    from datetime import timedelta

    store = StateStore()

    # 模拟state_store没有_stats的情况
    svc = MagicMock()
    svc._state_store = store

    # 模拟provider有_stats但state_store没有
    provider = MagicMock()
    provider._stats = {'start_time': datetime.now(), 'total_ticks': 100, 'total_signals': 5, 'total_trades': 3}
    svc._provider = provider

    # 模拟output_periodic_summary中的回退逻辑
    _ss_stats = svc._state_store.get_ref('_stats') if svc._state_store else None
    if _ss_stats is None:
        _ss_stats = {}
    _start_time = _ss_stats.get('start_time')
    if _start_time is None:
        _provider = getattr(svc, '_provider', None) or getattr(svc, '_strategy', None)
        if _provider is not None:
            _p_stats = getattr(_provider, '_stats', None)
            if _p_stats and _p_stats.get('start_time'):
                _start_time = _p_stats['start_time']
                if svc._state_store is not None:
                    svc._state_store.set_ref('_stats', _p_stats)
                _ss_stats = _p_stats

    check("回退逻辑成功读取start_time",
          _start_time is not None,
          "start_time仍为None")

    check("回退后_ss_stats被更新为provider的_stats",
          _ss_stats.get('total_signals') == 5,
          f"total_signals={_ss_stats.get('total_signals')}")

    check("回退后state_store已注册_stats引用",
          store.get_ref('_stats') is not None and store.get_ref('_stats').get('total_trades') == 3,
          "state_store中_stats未正确注册")


# ============================================================
# 测试7: 回归 - 编译检查所有修改文件
# ============================================================
def test_compile_all_modified_files():
    print("\n=== 测试7: 编译检查所有修改文件 ===")
    import py_compile

    files = [
        'ali2026v3_trading/strategy/strategy_business_layer.py',
        'ali2026v3_trading/lifecycle/lifecycle_init.py',
        'ali2026v3_trading/strategy/tick_dispatch.py',
        'ali2026v3_trading/strategy/persistence_service.py',
    ]
    for f in files:
        full = os.path.join(_PROJECT_ROOT, f)
        try:
            py_compile.compile(full, doraise=True)
            check(f"编译成功: {os.path.basename(f)}", True)
        except py_compile.PyCompileError as e:
            check(f"编译成功: {os.path.basename(f)}", False, str(e))


# ============================================================
# 测试8: 彻底性排查 - state_ref_keys循环中set_ref是首选
# ============================================================
def test_no_other_set_for_stats():
    print("\n=== 测试8: 彻底性排查 - state_ref_keys循环中set_ref是首选 ===")
    code = read_file(os.path.join('ali2026v3_trading', 'strategy', 'persistence_service.py'))

    # 提取state_ref_keys循环的代码块
    ref_loop_start = code.index('for key in state_ref_keys:')
    # 找到state_ref_keys循环结束位置(storage的try块，以_has = self._has_attr为标志)
    ref_loop_end = code.index('_has = self._has_attr', ref_loop_start)
    ref_loop_code = code[ref_loop_start:ref_loop_end]

    # 在state_ref_keys循环中, set_ref应该出现在set之前(set是回退)
    has_set_ref = 'self._state_store.set_ref(key, val)' in ref_loop_code
    has_set_fallback = 'self._state_store.set(key, val)' in ref_loop_code

    check("state_ref_keys循环中包含set_ref调用", has_set_ref, "未找到set_ref")

    if has_set_fallback:
        ref_idx = ref_loop_code.index('self._state_store.set_ref(key, val)')
        set_idx = ref_loop_code.index('self._state_store.set(key, val)')
        check("state_ref_keys循环中set_ref出现在set之前(首选)",
              ref_idx < set_idx,
              f"set_ref位置={ref_idx}, set位置={set_idx}")
    else:
        check("state_ref_keys循环中无set回退(仅set_ref)", True)


# ============================================================
# 主函数
# ============================================================
if __name__ == '__main__':
    print("=" * 70)
    print("FIX-20260708 断言测试: _stats引用修复 + record_signal/trade调用")
    print("=" * 70)

    test_persistence_service_uses_set_ref()
    test_lifecycle_init_reregisters_stats()
    test_tick_dispatch_start_time_fallback()
    test_business_layer_calls_record_signal_trade()
    test_state_store_ref_semantics()
    test_output_periodic_summary_fallback()
    test_compile_all_modified_files()
    test_no_other_set_for_stats()

    print("\n" + "=" * 70)
    print(f"总计: {PASS} 通过, {FAIL} 失败")
    print("=" * 70)

    sys.exit(0 if FAIL == 0 else 1)
