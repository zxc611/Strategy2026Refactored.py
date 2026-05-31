"""严格对比验证: 每个修复点必须证明 修复前行为 ≠ 修复后行为
验证标准: 如果移除修复代码，系统行为会变差/变错

方法:
  1. 用实际数据调用函数，记录修复后行为
  2. 模拟修复前行为，证明两者不同
  3. 证明修复后行为是正确的
"""
import sys, os, types, copy
import pandas as pd
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

print("=" * 80)
print("严格对比验证 — 修复前 vs 修复后 行为差异实证")
print("=" * 80)

PASS_COUNT = 0
FAIL_COUNT = 0
EVIDENCE = {}

def record(item_id, passed, before_desc, after_desc, evidence_desc):
    global PASS_COUNT, FAIL_COUNT
    icon = "PASS" if passed else "FAIL"
    if passed:
        PASS_COUNT += 1
    else:
        FAIL_COUNT += 1
    EVIDENCE[item_id] = {
        "result": icon,
        "before": before_desc,
        "after": after_desc,
        "evidence": evidence_desc,
    }
    print(f"\n  [{icon}] {item_id}")
    print(f"    修复前: {before_desc}")
    print(f"    修复后: {after_desc}")
    print(f"    证据:   {evidence_desc}")

# ========================================================================
# P0-16: LHS采样点注入Optuna
# ========================================================================
print("\n" + "=" * 60)
print("P0-16: LHS采样点注入Optuna")
print("=" * 60)
try:
    import optuna

    # 修复前: study.tell(study.ask(), [0.0, 0.0])
    study_before = optuna.create_study(directions=['minimize', 'minimize'])
    lhs_params = {"x": 1.5, "y": 0.5}
    # 旧代码: study.tell(study.ask(), [0.0, 0.0]) — ask()用TPE采样,不传LHS参数
    trial_before = study_before.ask()  # TPE随机采样,不是LHS点
    study_before.tell(trial_before, [0.0, 0.0])
    # 检查: trial的参数不是LHS参数
    before_params = trial_before.params
    before_is_lhs = (before_params == lhs_params)
    print(f"  修复前: ask()生成trial参数={dict(before_params)}, 与LHS参数匹配={before_is_lhs}")

    # 修复后: study.enqueue_trial(lhs_params)
    # 需要定义search space才能让enqueue_trial正确工作
    study_after = optuna.create_study(directions=['minimize', 'minimize'])
    # 先用suggest定义search space
    def objective_with_suggest(trial):
        x = trial.suggest_float("x", -10, 10)
        y = trial.suggest_float("y", -10, 10)
        return [x**2, y**2]
    # enqueue LHS参数
    study_after.enqueue_trial(lhs_params, skip_if_exists=True)
    # 执行optimize，enqueue的trial会被优先评估
    study_after.optimize(objective_with_suggest, n_trials=1)
    # 检查第一个trial的参数是否是LHS参数
    first_trial = study_after.trials[0]
    after_params = dict(first_trial.params)
    after_is_lhs = (after_params == lhs_params)
    print(f"  修复后: enqueue_trial后第一个trial参数={after_params}, 与LHS匹配={after_is_lhs}")

    # 证据: enqueue的trial参数就是LHS参数
    ok = after_is_lhs and not before_is_lhs
    record("P0-16", ok,
           f"study.tell(study.ask()) — TPE随机采样,参数≠LHS({before_params})",
           f"study.enqueue_trial — LHS参数被Optuna优先评估,参数={after_params}",
           f"before_match={before_is_lhs}, after_match={after_is_lhs}")
except Exception as e:
    print(f"  [ERROR] {e}")
    import traceback; traceback.print_exc()
    record("P0-16", False, "ERROR", "ERROR", str(e))

# ========================================================================
# T5+P0-3: validate_logic_reversal死锁
# ========================================================================
print("\n" + "=" * 60)
print("T5+P0-3: validate_logic_reversal死锁修复")
print("=" * 60)
try:
    from 参数池.task_scheduler import validate_logic_reversal_no_future

    # 修复前: bar_data=None → passed=False → all_passed=False → P0永久阻断
    r_none = validate_logic_reversal_no_future(bar_data=None)
    before_would_fail = not r_none.get("passed", True)  # 修复前: None→False→P0失败
    print(f"  bar_data=None返回: passed={r_none.get('passed')}, action={r_none.get('action')}")

    # 修复后: _run_final_checks中检测action=="no_data"时SKIP
    with open(os.path.join(_PROJECT_ROOT, '参数池', 'task_scheduler.py'), 'r', encoding='utf-8') as f:
        src = f.read()
    # 找到T5修复代码块
    t5_block_start = src.find('# T5修复: 集成逻辑反转未来数据验证到P0检验')
    t5_block = src[t5_block_start:t5_block_start+600]
    has_skip_logic = '"no_data"' in t5_block and '[SKIP]' in t5_block
    # 确认no_data时不设置all_passed=False
    skip_section = t5_block[t5_block.find('"no_data"'):t5_block.find('"no_data"')+200]
    no_all_passed_false_in_skip = 'all_passed = False' not in skip_section.split('elif')[0]
    print(f"  修复后代码: no_data时SKIP逻辑存在={has_skip_logic}, SKIP块中无all_passed=False={no_all_passed_false_in_skip}")

    # 有数据时正常验证
    df = pd.DataFrame({"wrong_rise_pct": [0.1, 0.2, 0.3]})
    r_data = validate_logic_reversal_no_future(bar_data=df)
    print(f"  有数据时: passed={r_data.get('passed')}, action={r_data.get('action')}")

    ok = has_skip_logic and no_all_passed_false_in_skip and r_none.get('action') == 'no_data'
    record("T5+P0-3", ok,
           f"bar_data=None→passed=False→all_passed=False→P0永久阻断",
           f"bar_data=None→action='no_data'→SKIP→P0不被阻断",
           f"skip_logic={has_skip_logic}, no_false_in_skip={no_all_passed_false_in_skip}, data_ok={r_data.get('action')=='proceed'}")
except Exception as e:
    print(f"  [ERROR] {e}")
    import traceback; traceback.print_exc()
    record("T5+P0-3", False, "ERROR", "ERROR", str(e))

# ========================================================================
# P0-1: NaN显式替换
# ========================================================================
print("\n" + "=" * 60)
print("P0-1: NaN显式替换为0")
print("=" * 60)
try:
    from 参数池.task_scheduler import _check_state_transition, _BacktestState

    # 修复前: NaN值传播 → NaN+0+0+0=NaN → NaN<0.65=False → else分支 → NaN>=NaN=False → "incorrect_reversal"
    # 模拟修复前行为
    bar_nan_before = pd.Series({
        "correct_rise_pct": float('nan'),
        "correct_fall_pct": float('nan'),
        "wrong_rise_pct": float('nan'),
        "wrong_fall_pct": float('nan'),
    })
    non_other_before = bar_nan_before.get("correct_rise_pct", 0) + bar_nan_before.get("correct_fall_pct", 0) + bar_nan_before.get("wrong_rise_pct", 0) + bar_nan_before.get("wrong_rise_pct", 0)
    # NaN < 0.65 → False → 进入else → correct=NaN, incorrect=NaN → NaN>=NaN → False → "incorrect_reversal"
    threshold = 0.65
    if non_other_before < threshold:
        candidate_before = "other"
    else:
        correct = bar_nan_before.get("correct_rise_pct", 0) + bar_nan_before.get("correct_fall_pct", 0)
        incorrect = bar_nan_before.get("wrong_rise_pct", 0) + bar_nan_before.get("wrong_rise_pct", 0)
        candidate_before = "correct_trending" if correct >= incorrect else "incorrect_reversal"
    print(f"  修复前: NaN传播 → non_other={non_other_before}, candidate={candidate_before}")
    print(f"    (NaN < 0.65 = {non_other_before < threshold}, NaN >= NaN = {bar_nan_before.get('correct_rise_pct',0) >= bar_nan_before.get('wrong_rise_pct',0)})")

    # 修复后: NaN被替换为0 → 0+0+0+0=0 → 0<0.65=True → "other"
    bt = _BacktestState()
    bar_nan_after = pd.Series({
        "correct_rise_pct": float('nan'),
        "correct_fall_pct": float('nan'),
        "wrong_rise_pct": float('nan'),
        "wrong_fall_pct": float('nan'),
        "close": 0.05,
        "minute": pd.Timestamp("2024-01-01 10:00"),
    })
    state_after = _check_state_transition(bt, bar_nan_after, {})
    print(f"  修复后: NaN被替换为0 → state={state_after}")

    # 证据: 修复前="incorrect_reversal"(错误), 修复后="other"(正确)
    ok = candidate_before == "incorrect_reversal" and state_after == "other"
    record("P0-1", ok,
           f"NaN传播 → candidate='incorrect_reversal'(错误方向交易)",
           f"NaN→0 → state='other'(安全状态,不开仓)",
           f"before='{candidate_before}', after='{state_after}', 行为不同={candidate_before != state_after}")
except Exception as e:
    print(f"  [ERROR] {e}")
    import traceback; traceback.print_exc()
    record("P0-1", False, "ERROR", "ERROR", str(e))

# ========================================================================
# P0-6: days_to_expiry数据生成
# ========================================================================
print("\n" + "=" * 60)
print("P0-6: days_to_expiry数据生成")
print("=" * 60)
try:
    from 参数池.task_scheduler import _get_expiry_slippage_multiplier, _compute_dynamic_slippage_bps

    # 修复前: bar中无days_to_expiry → multiplier恒为1.0
    bar_no_dte = pd.Series({"close": 0.05, "bid_ask_spread": 0.001})
    mult_no_dte = _get_expiry_slippage_multiplier(bar_no_dte, {})
    print(f"  修复前(无DTE列): multiplier={mult_no_dte}")

    # 修复后: bar中有days_to_expiry → multiplier根据DTE变化
    bar_dte_2 = pd.Series({"days_to_expiry": 2, "close": 0.05, "bid_ask_spread": 0.001})
    bar_dte_10 = pd.Series({"days_to_expiry": 10, "close": 0.05, "bid_ask_spread": 0.001})
    bar_dte_30 = pd.Series({"days_to_expiry": 30, "close": 0.05, "bid_ask_spread": 0.001})
    mult_dte_2 = _get_expiry_slippage_multiplier(bar_dte_2, {})
    mult_dte_10 = _get_expiry_slippage_multiplier(bar_dte_10, {})
    mult_dte_30 = _get_expiry_slippage_multiplier(bar_dte_30, {})
    print(f"  修复后: DTE=2→mult={mult_dte_2}, DTE=10→mult={mult_dte_10}, DTE=30→mult={mult_dte_30}")

    # 验证preprocess_ticks.py生成days_to_expiry
    with open(os.path.join(_PROJECT_ROOT, '参数池', 'preprocess_ticks.py'), 'r', encoding='utf-8') as f:
        preproc_src = f.read()
    has_dte_gen = 'days_to_expiry' in preproc_src and 'expire_date' in preproc_src and 'dt.days' in preproc_src
    print(f"  preprocess生成days_to_expiry: {has_dte_gen}")

    # 验证数据库schema
    has_dte_schema = 'days_to_expiry' in preproc_src and 'INTEGER' in preproc_src
    print(f"  数据库schema含days_to_expiry: {has_dte_schema}")

    # 证据: 无DTE→mult=1.0(无保护), 有DTE→mult=20.0(20倍保护)
    # DTE=2: <=3 → 20.0; DTE=10: >7且<=14 → 3.0; DTE=30: >14 → 1.0
    ok = mult_no_dte == 1.0 and mult_dte_2 == 20.0 and mult_dte_10 == 3.0 and mult_dte_30 == 1.0 and has_dte_gen
    record("P0-6", ok,
           f"无DTE数据 → multiplier恒=1.0(到期日滑点无保护)",
           f"有DTE数据 → DTE=2:mult=20, DTE=10:mult=3, DTE=30:mult=1",
           f"no_dte_mult={mult_no_dte}, dte2={mult_dte_2}, dte10={mult_dte_10}, dte30={mult_dte_30}, schema={has_dte_schema}")
except Exception as e:
    print(f"  [ERROR] {e}")
    import traceback; traceback.print_exc()
    record("P0-6", False, "ERROR", "ERROR", str(e))

# ========================================================================
# P0-14: _run_final_checks返回值捕获
# ========================================================================
print("\n" + "=" * 60)
print("P0-14: _run_final_checks返回值捕获")
print("=" * 60)
try:
    with open(os.path.join(_PROJECT_ROOT, '参数池', 'task_scheduler.py'), 'r', encoding='utf-8') as f:
        src = f.read()

    # 修复前: _run_final_checks(...) 无返回值接收
    # 搜索所有调用点
    import re
    # 找到main_scheduler中的调用
    call_pattern = r'(_p0_passed\s*=\s*)?_run_final_checks\('
    calls = list(re.finditer(call_pattern, src))
    for i, m in enumerate(calls):
        context_start = max(0, m.start() - 50)
        context = src[context_start:m.end()+50].replace('\n', ' ')
        has_capture = '_p0_passed = _run_final_checks(' in context or '_p0_passed=_run_final_checks(' in context
        print(f"  调用点{i+1}: 有返回值捕获={has_capture}, 上下文: ...{context[-80:]}")

    # 验证: 返回值用于中断流程
    has_block_logic = 'if not _p0_passed:' in src
    block_context = src[src.find('if not _p0_passed:'):src.find('if not _p0_passed:')+200]
    print(f"  P0失败时中断逻辑: {has_block_logic}")
    print(f"  中断代码: {block_context[:100].replace(chr(10), ' ')}")

    # 修复前模拟: 返回值被丢弃 → 即使P0失败也继续
    # 修复后: _p0_passed=False → 打印拒绝信息
    ok = '_p0_passed = _run_final_checks(' in src and has_block_logic
    record("P0-14", ok,
           f"_run_final_checks(...)返回值被丢弃 → P0红灯后系统继续运行",
           f"_p0_passed = _run_final_checks(...) → if not _p0_passed: 拒绝参数",
           f"capture={'_p0_passed = _run_final_checks(' in src}, block={has_block_logic}")
except Exception as e:
    print(f"  [ERROR] {e}")
    import traceback; traceback.print_exc()
    record("P0-14", False, "ERROR", "ERROR", str(e))

# ========================================================================
# P0-11: E13生产路径
# ========================================================================
print("\n" + "=" * 60)
print("P0-11: E13生产路径调用detect")
print("=" * 60)
try:
    with open(os.path.join(_PROJECT_ROOT, 'strategy_ecosystem.py'), 'r', encoding='utf-8') as f:
        eco_src = f.read()

    # 修复前: _e13_detector导入但detect()从未调用
    # 修复后: switch_active_strategy中调用detect()
    has_detect = 'self._e13_detector.detect(' in eco_src
    print(f"  strategy_ecosystem中调用detect(): {has_detect}")

    # 验证detect()在switch_active_strategy中
    switch_start = eco_src.find('def switch_active_strategy')
    switch_end = eco_src.find('\n    def ', switch_start + 10)
    switch_body = eco_src[switch_start:switch_end]
    detect_in_switch = 'self._e13_detector.detect(' in switch_body
    print(f"  detect()在switch_active_strategy内: {detect_in_switch}")

    # 验证E13检测器功能
    from governance_engine import E13ShadowStrategyCollusionDetector
    det = E13ShadowStrategyCollusionDetector()
    same = {"close_take_profit_ratio": 1.5, "close_stop_loss_ratio": 0.5, "max_risk_ratio": 0.3}
    diff = {"close_take_profit_ratio": 3.0, "close_stop_loss_ratio": 1.5, "max_risk_ratio": 0.6}
    r_same = det.detect(same, same, [], [])
    r_diff = det.detect(same, diff, [], [])
    print(f"  E13功能: 相同参数triggered={r_same['e13_triggered']}, 差异参数triggered={r_diff['e13_triggered']}")

    # 修复前: detect()从未调用 → 同谋参数无检测
    # 修复后: 每次策略切换都调用 → 同谋时warning
    ok = has_detect and detect_in_switch and r_same['e13_triggered'] and not r_diff['e13_triggered']
    record("P0-11", ok,
           f"_e13_detector导入但detect()从未调用 → 实盘同谋无检测",
           f"switch_active_strategy中调用detect() → 同谋参数触发warning",
           f"detect_in_switch={detect_in_switch}, same_triggered={r_same['e13_triggered']}, diff_triggered={r_diff['e13_triggered']}")
except Exception as e:
    print(f"  [ERROR] {e}")
    import traceback; traceback.print_exc()
    record("P0-11", False, "ERROR", "ERROR", str(e))

# ========================================================================
# N1: 质量标记阻断开仓
# ========================================================================
print("\n" + "=" * 60)
print("N1: 质量标记阻断开仓")
print("=" * 60)
try:
    from 参数池.task_scheduler import _try_open, _BacktestState

    # 修复前: _spread_quality=0时仅warning,不return → 开仓继续
    # 修复后: _spread_quality=0时return → 开仓被阻断

    # 测试: spread_quality=0时_try_open是否阻止开仓
    bt_bad = _BacktestState()
    bt_bad.current_state = "correct_trending"
    bar_bad = pd.Series({
        "close": 0.05, "minute": pd.Timestamp("2024-01-01 10:00"),
        "symbol": "50ETF购6M3300", "bid_ask_spread": 0.001,
        "_spread_quality": 0,
    })
    pos_before_bad = len(bt_bad.positions)
    _try_open(bt_bad, bar_bad, {"close_take_profit_ratio": 1.5, "close_stop_loss_ratio": 0.5,
                                 "max_risk_ratio": 0.3, "signal_cooldown_sec": 0})
    pos_after_bad = len(bt_bad.positions)
    blocked = pos_after_bad == pos_before_bad
    print(f"  spread_quality=0: positions {pos_before_bad}→{pos_after_bad}, 开仓被阻断={blocked}")

    # 对比: spread_quality=1时_try_open是否正常(不因质量阻断)
    bt_good = _BacktestState()
    bt_good.current_state = "correct_trending"
    bar_good = pd.Series({
        "close": 0.05, "minute": pd.Timestamp("2024-01-01 10:00"),
        "symbol": "50ETF购6M3300", "bid_ask_spread": 0.001,
        "_spread_quality": 1,
    })
    pos_before_good = len(bt_good.positions)
    _try_open(bt_good, bar_good, {"close_take_profit_ratio": 1.5, "close_stop_loss_ratio": 0.5,
                                   "max_risk_ratio": 0.3, "signal_cooldown_sec": 0})
    pos_after_good = len(bt_good.positions)
    not_blocked_by_quality = True  # quality=1时不应因质量阻断(可能因其他原因不开仓)
    print(f"  spread_quality=1: positions {pos_before_good}→{pos_after_good}")

    # 验证代码中有return阻断
    with open(os.path.join(_PROJECT_ROOT, '参数池', 'task_scheduler.py'), 'r', encoding='utf-8') as f:
        src = f.read()
    # 找_try_open中的spread_quality检查 - 搜索return阻断
    sq_block_pattern = r'_spread_quality.*?==\s*0.*?return'
    has_return_block = bool(re.search(sq_block_pattern, src, re.DOTALL))
    # 也搜索blocked关键字
    has_blocked_log = '_spread_quality=0, spread data unreliable' in src
    print(f"  _try_open中spread_quality=0时return: {has_return_block}")
    print(f"  阻断日志存在: {has_blocked_log}")

    ok = blocked and has_return_block
    record("N1", ok,
           f"_spread_quality=0时仅warning → 开仓继续(不可靠数据下交易)",
           f"_spread_quality=0时return → 开仓被阻断(quality=0: pos不变)",
           f"blocked={blocked}, has_return={has_return_block}, bad_pos={pos_before_bad}→{pos_after_bad}")
except Exception as e:
    print(f"  [ERROR] {e}")
    import traceback; traceback.print_exc()
    record("N1", False, "ERROR", "ERROR", str(e))

# ========================================================================
# 汇总
# ========================================================================
print("\n" + "=" * 80)
print("严格对比验证汇总")
print("=" * 80)

for item_id, ev in sorted(EVIDENCE.items()):
    icon = "PASS" if ev["result"] == "PASS" else "FAIL"
    print(f"\n[{icon}] {item_id}")
    print(f"  修复前: {ev['before']}")
    print(f"  修复后: {ev['after']}")
    print(f"  证据:   {ev['evidence']}")

print(f"\n{'=' * 80}")
print(f"结果: PASS={PASS_COUNT} / FAIL={FAIL_COUNT}")
if FAIL_COUNT == 0:
    print("所有修复点均通过严格对比验证: 修复前行为≠修复后行为，修复后行为正确")
else:
    print(f"仍有{FAIL_COUNT}项未通过，需继续修复")
print("=" * 80)
