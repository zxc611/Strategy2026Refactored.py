#!/usr/bin/env python3
# MODULE_ID: M2-298
"""5项部分修复问题断言验证"""
import re
import sys
from pathlib import Path

_SRC_ROOT = Path(__file__).resolve().parent.parent.parent

passed = 0
failed = 0

def _check(path, pattern, negate=False):
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    matches = re.findall(pattern, content, re.MULTILINE)
    result = (len(matches) == 0) if negate else (len(matches) > 0)
    return result, len(matches)

def assert_check(name, result, detail=""):
    global passed, failed
    if result:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name} {detail}")

print("5项部分修复问题断言验证")
print("=" * 60)

# P0-A3: Kelly含手续费/滑点
print("[P0-A3] Kelly公式含手续费滑点近似")
r, c = _check('ali2026v3_trading/param_pool/precompute/_position_decision.py', r'transaction_cost_rate')
assert_check("transaction_cost_rate定义", r, f"found {c}")
r, c = _check('ali2026v3_trading/param_pool/precompute/_position_decision.py', r'cost_per_bar')
assert_check("cost_per_bar计算", r, f"found {c}")
r, c = _check('ali2026v3_trading/param_pool/precompute/_position_decision.py', r'gross_pnl - cost_per_bar')
assert_check("净盈亏=毛盈亏-成本", r, f"found {c}")
print()

# P1-3: min_duration向量化
print("[P1-3] min_duration去毛刺向量化")
r, c = _check('ali2026v3_trading/param_pool/precompute/_l0_state.py', r'short_mask')
assert_check("short_mask布尔索引", r, f"found {c}")
r, c = _check('ali2026v3_trading/param_pool/precompute/_l0_state.py', r'np\.where\(short_mask\)')
assert_check("np.where定位短段", r, f"found {c}")
r, c = _check('ali2026v3_trading/param_pool/precompute/_l0_state.py', r'for\s+idx\s+in\s+range\(n\)')
assert_check("无O(n)循环", not r, f"found {c}")
print()

# P2-3: phase_params从CycleResonanceParams读取
print("[P2-3] phase_params从CycleResonanceParams读取")
r, c = _check('ali2026v3_trading/param_pool/precompute/_cycle_resonance_vec.py', r'phase_params.*Optional.*object.*=.*None')
assert_check("_vectorized_phase签名有phase_params", r, f"found {c}")
r, c = _check('ali2026v3_trading/param_pool/precompute/_cycle_resonance_vec.py', r'getattr\(phase_params')
assert_check("_vectorized_phase用getattr读取", r, f"found {c}")
r, c = _check('ali2026v3_trading/param_pool/precompute/_engine.py', r'phase_params=self\._params\.cycle_resonance')
assert_check("engine传入phase_params", r, f"found {c}")
r, c = _check('ali2026v3_trading/param_pool/precompute/_params.py', r'cycle_resonance:\s*CycleResonanceParams')
assert_check("PrecomputeParams有cycle_resonance字段", r, f"found {c}")
print()

# P2-7: UPDATE双重白名单验证
print("[P2-7] UPDATE双重白名单验证")
r, c = _check('ali2026v3_trading/param_pool/precompute/_engine.py', r'schema_col_set\s*=\s*set\(SCHEMA_REQUIRED_COLUMNS_V5')
assert_check("schema_col_set白名单", r, f"found {c}")
r, c = _check('ali2026v3_trading/param_pool/precompute/_engine.py', r'c in existing_cols and c in schema_col_set')
assert_check("双重白名单交叉验证", r, f"found {c}")
print()

# P2-9: ATR参考价docstring说明
print("[P2-9] ATR参考价docstring说明")
r, c = _check('ali2026v3_trading/param_pool/precompute/_pullback.py', r'2 ATR units below')
assert_check("docstring说明ATR支撑位含义", r, f"found {c}")
r, c = _check('ali2026v3_trading/param_pool/precompute/_pullback.py', r'atr_support_level')
assert_check("变量名atr_support_level", r, f"found {c}")
print()

print("=" * 60)
print(f"结果: {passed} PASS, {failed} FAIL")
sys.exit(0 if failed == 0 else 1)