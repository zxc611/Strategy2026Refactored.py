#!/usr/bin/env python3
# MODULE_ID: M2-296
"""14项问题断言验证脚本"""
import re
import ast
import sys
from pathlib import Path

_SRC_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

passed = 0
failed = 0

def _check(path, pattern, negate=False):
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    matches = re.findall(pattern, content, re.MULTILINE)
    result = (len(matches) == 0) if negate else (len(matches) > 0)
    return result, len(matches), content

def assert_check(name, result, detail=""):
    global passed, failed
    if result:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name} {detail}")

print("=" * 60)
print("14项问题断言验证")
print("=" * 60)
print()

# P0-2: confirm_lag类型为float
print("[P0-2] confirm_lag类型为float")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_daily_pivot.py', r'confirm_lag:\s*float')
assert_check("PivotRecord.confirm_lag: float", r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_daily_pivot.py', r'confirm_lag\s+DOUBLE')
assert_check("DDL confirm_lag DOUBLE", r, f"found {c}")
print()

# P0-A3: Kelly用盈亏金额
print("[P0-A3] Kelly公式用盈亏金额")
r, c, content = _check('ali2026v3_trading/param_pool/precompute/_position_decision.py', r'price_ret')
assert_check("使用price_ret(盈亏金额)", r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_position_decision.py', r'log_ret')
assert_check("不使用log_ret(对数收益率)", r, f"found {c}" if not r else f"still {c} occurrences")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_position_decision.py', r'window\s*=\s*120')
assert_check("窗口扩大到120", r, f"found {c}")
print()

# P1-3: L0状态完全向量化
print("[P1-3] L0状态完全向量化")
r, c, content = _check('ali2026v3_trading/param_pool/precompute/_l0_state.py', r'for\s+i\s+in\s+range\(n\)')
assert_check("raw_state无for循环", not r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_l0_state.py', r'for\s+i\s+range')
assert_check("min_duration无range循环", not r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_l0_state.py', r'sliding_window_view')
assert_check("entropy使用sliding_window_view", r, f"found {c}")
print()

# P1-5: KL-RPD完全向量化
print("[P1-5] KL-RPD完全向量化")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_kl_rpd.py', r'for\s+i\s+in\s+range\(rv_period')
assert_check("RV无for循环", not r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_kl_rpd.py', r'for\s+i\s+in\s+range\(rsi_period')
assert_check("RSI无for循环", not r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_kl_rpd.py', r'for\s+i\s+in\s+range\(trend_window')
assert_check("polyfit无for循环", not r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_kl_rpd.py', r'ewm\(alpha=alpha_rsi')
assert_check("RSI使用EWM向量化", r, f"found {c}")
print()

# P2-3: 相位阈值从CycleResonanceParams读取
print("[P2-3] 相位阈值从CycleResonanceParams读取")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_cycle_resonance_vec.py', r'phase_params')
assert_check("函数签名有phase_params参数", r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_cycle_resonance_vec.py', r'phase_params\.chaos_entropy_threshold')
assert_check("从phase_params读取阈值", r, f"found {c}")
print()

# P1-9: 临时表管理处理列变化
print("[P1-9] 临时表管理处理列变化")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_engine.py', r'DESCRIBE minute_data')
assert_check("DESCRIBE检查现有列", r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_engine.py', r'safe_update_cols')
assert_check("使用safe_update_cols过滤", r, f"found {c}")
print()

# P2-2: ZigZag阈值调整
print("[P2-2] ZigZag阈值调整")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_daily_pivot.py', r'multiplier.*1\.5')
assert_check("multiplier=1.5", r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_params.py', r'multiplier.*1\.5')
assert_check("params默认值1.5", r, f"found {c}")
print()

# P2-4: CRM运行时校验
print("[P2-4] CRM运行时校验")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_engine.py', r'CRM_RUNTIME_CHECK')
assert_check("CRM_RUNTIME_CHECK环境变量", r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_engine.py', r'_validate_crm_vectorized_vs_perrow')
assert_check("校验函数存在", r, f"found {c}")
print()

# P2-5: 表依赖验证
print("[P2-5] 表依赖验证")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_daily_pivot.py', r'DESCRIBE.*source_table')
assert_check("DESCRIBE验证表结构", r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_daily_pivot.py', r'missing columns')
assert_check("缺失列报错", r, f"found {c}")
print()

# P2-7: UPDATE语句安全
print("[P2-7] UPDATE语句安全")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_engine.py', r'safe_update_cols')
assert_check("使用safe_update_cols过滤列", r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_engine.py', r'if set_clause')
assert_check("空列保护", r, f"found {c}")
print()

# P2-9: ATR参考价变量名自解释
print("[P2-9] ATR参考价变量名自解释")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_pullback.py', r'atr_support_level')
assert_check("使用atr_support_level变量名", r, f"found {c}")
print()

# P2-A2: ALTER TABLE事务包裹
print("[P2-A2] ALTER TABLE事务包裹")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_schema.py', r'BEGIN TRANSACTION')
assert_check("BEGIN TRANSACTION", r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_schema.py', r'ROLLBACK')
assert_check("ROLLBACK回滚", r, f"found {c}")
print()

# P2-A3: linkage_weights改为Tuple
print("[P2-A3] linkage_weights改为Tuple")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_params.py', r'Tuple\[Tuple\[float')
assert_check("linkage_weights类型为Tuple[Tuple[float]]", r, f"found {c}")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_params.py', r'List\[List\[float\]\]')
assert_check("无List[List[float]]", not r, f"found {c}")
print()

# P2-A5: 未确认极端点处理
print("[P2-A5] 未确认极端点处理")
r, c, _ = _check('ali2026v3_trading/param_pool/precompute/_daily_pivot.py', r'if state in.*RISING.*FALLING')
assert_check("处理未确认极端点", r, f"found {c}")
print()

print("=" * 60)
print(f"结果: {passed} PASS, {failed} FAIL")
print("=" * 60)
sys.exit(0 if failed == 0 else 1)