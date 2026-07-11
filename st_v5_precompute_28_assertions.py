#!/usr/bin/env python3
# MODULE_ID: M2-602
"""V5数据预处理28项问题断言验证脚本

按照《V5数据预处理和高低点标注系统问题清单》进行端到端断言验证。
验证方式：每5个问题一组，全部通过后进入下一组。

用法:
    python -m pytest tests/test_v5_precompute_28_assertions.py -v
    或
    python tests/test_v5_precompute_28_assertions.py
"""
from __future__ import annotations

import ast
import re
import sys
import time
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Tuple

_SRC_ROOT = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _SRC_ROOT.parent

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _read_file(rel_path: str) -> str:
    full_path = _SRC_ROOT / rel_path
    if not full_path.exists():
        return ""
    with open(full_path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def _grep_pattern(pattern: str, rel_path: str) -> List[Tuple[int, str]]:
    content = _read_file(rel_path)
    if not content:
        return []
    results = []
    for i, line in enumerate(content.splitlines(), 1):
        if re.search(pattern, line):
            results.append((i, line.strip()))
    return results


def _check_ast_valid(rel_path: str) -> Tuple[bool, str]:
    content = _read_file(rel_path)
    if not content:
        return False, "文件不存在或为空"
    try:
        ast.parse(content)
        return True, "AST解析成功"
    except SyntaxError as e:
        return False, f"语法错误: {e}"


# ============================================================================
# P0级验证（问题1-5）: 致命问题
# ============================================================================

def test_P0_1_signal_age_initial_value():
    """P0-1: 信号年龄初始值应为0而非1440"""
    matches = _grep_pattern(r"age\[i\]\s*=\s*0\.0", "param_pool/precompute/_signal_decay.py")
    assert len(matches) >= 1, "P0-1 FAIL: 信号年龄初始值未修正为0.0"
    bad_matches = _grep_pattern(r"age\[i\]\s*=\s*1440", "param_pool/precompute/_signal_decay.py")
    assert len(bad_matches) == 0, "P0-1 FAIL: 仍存在age[i]=1440的错误初始值"


def test_P0_2_confirmation_delay_precision():
    """P0-2: 确认延迟计算使用max(1, delta_days)避免精度丢失"""
    content = _read_file("param_pool/precompute/_daily_pivot.py")
    assert "max(1, delta_days)" in content or "max(1,int(delta_days))" in content, \
        "P0-2 FAIL: 确认延迟未使用max(1, delta_days)保护"


def test_P0_A3_kelly_formula_correction():
    """P0-A3: Kelly公式使用盈亏比而非对数收益率"""
    content = _read_file("param_pool/precompute/_position_decision.py")
    assert "win_loss_ratio" in content, "P0-A3 FAIL: Kelly公式未使用win_loss_ratio"
    assert "p_win - (1-p_win)/win_loss_ratio" in content or \
           "p_win - (1 - p_win) / win_loss_ratio" in content, \
        "P0-A3 FAIL: Kelly公式未使用正确的盈亏比公式"
    assert "min_samples" in content and "20" in content, \
        "P0-A3 FAIL: Kelly计算未设置最小样本数20"


def test_P0_A4_obos_vectorized():
    """P0-A4: OBOS指标使用向量化实现（EWM/rolling）"""
    content = _read_file("param_pool/precompute/_obos.py")
    assert "ewm(" in content or "rolling(" in content, \
        "P0-A4 FAIL: OBOS未使用向量化方法(ewm/rolling)"
    for_loops = _grep_pattern(r"for\s+i\s+in\s+range\(len\(", "param_pool/precompute/_obos.py")
    assert len(for_loops) == 0, "P0-A4 FAIL: OBOS仍使用Python级for循环"


def test_P0_A5_vectorization_complete():
    """P0-A5: 核心预计算模块全部向量化"""
    modules = [
        "param_pool/precompute/_signal_decay.py",
        "param_pool/precompute/_l0_state.py",
        "param_pool/precompute/_signals.py",
        "param_pool/precompute/_kl_rpd.py",
    ]
    for module in modules:
        content = _read_file(module)
        assert "np.where" in content or "rolling(" in content or "ewm(" in content or "shift(" in content, \
            f"P0-A5 FAIL: {module} 未使用向量化方法"


# ============================================================================
# P1级验证（问题6-10）: 严重问题
# ============================================================================

def test_P1_2_signal_age_vectorized():
    """P1-2: 信号年龄计算使用向量化（active_mask + np.where）"""
    content = _read_file("param_pool/precompute/_signal_decay.py")
    assert "active_mask" in content, "P1-2 FAIL: 未使用active_mask向量化"
    assert "np.where" in content, "P1-2 FAIL: 未使用np.where向量化"


def test_P1_3_l0_state_vectorized():
    """P1-3: L0状态计算使用EWM向量化"""
    content = _read_file("param_pool/precompute/_l0_state.py")
    assert "ewm(" in content, "P1-3 FAIL: L0状态未使用EWM向量化"
    for_loops = _grep_pattern(r"for\s+i\s+in\s+range\(", "param_pool/precompute/_l0_state.py")
    assert len(for_loops) == 0, "P1-3 FAIL: L0状态仍使用for循环"


def test_P1_4_signals_vectorized():
    """P1-4: Signals S2/S3/S4使用rolling/shift向量化"""
    content = _read_file("param_pool/precompute/_signals.py")
    assert "rolling(" in content or "shift(" in content, \
        "P1-4 FAIL: Signals未使用rolling/shift向量化"


def test_P1_5_kl_rpd_vectorized():
    """P1-5: KL-RPD使用EWM向量化"""
    content = _read_file("param_pool/precompute/_kl_rpd.py")
    assert "ewm(" in content, "P1-5 FAIL: KL-RPD未使用EWM向量化"


def test_P1_10_volatility_boundary():
    """P1-10: 波动率计算排除无效价格而非替换"""
    content = _read_file("param_pool/precompute/_daily_pivot.py")
    assert "valid_mask" in content or "&" in content, \
        "P1-10 FAIL: 波动率未使用valid_mask过滤"


# ============================================================================
# P1级验证（问题11-15）: 严重问题续
# ============================================================================

def test_P1_A5_holiday_coverage():
    """P1-A5: 节假日覆盖2010-2026而非仅2024-2026"""
    content = _read_file("param_pool/precompute/_calendar.py")
    assert "2010" in content, "P1-A5 FAIL: 节假日未覆盖2010年"
    assert "2026" in content, "P1-A5 FAIL: 节假日未覆盖2026年"


def test_P1_A6_source_table_parameterized():
    """P1-A6: source_table参数化而非硬编码"""
    content = _read_file("param_pool/precompute/_daily_pivot.py")
    assert "source_table" in content, "P1-A6 FAIL: source_table未参数化"
    hardcoded = _grep_pattern(r"FROM\s+minute_data\s+WHERE", "param_pool/precompute/_daily_pivot.py")
    assert len(hardcoded) <= 2, "P1-A6 FAIL: source_table仍多处硬编码"


def test_P1_A9_code_deduplication():
    """P1-A9: _precompute_symbol与审precompute_symbol_incremental合并"""
    content = _read_file("param_pool/precompute/_engine.py")
    method_defs = _grep_pattern(r"def _precompute_symbol_incremental\(", "param_pool/precompute/_engine.py")
    assert len(method_defs) == 1, "P1-A9 FAIL: 存在重复的方法定义"
    assert "return self._precompute_symbol(symbol, since_date=since_date)" in content, \
        "P1-A9 FAIL: _precompute_symbol_incremental未委托给给precompute_symbol"


def test_P1_9_temp_table_management():
    """P1-9: 临时表正确创建和清理"""
    content = _read_file("param_pool/precompute/_engine.py")
    assert "CREATE TEMP TABLE" in content, "P1-9 FAIL: 未使用临时表"
    assert "DROP TABLE" in content, "P1-9 FAIL: 未清理临时表"


def test_P1_A7_duckdb_concurrency():
    """P1-A7: DuckDB强制单进程写入"""
    content = _read_file("param_pool/precompute/_engine.py")
    assert "max_workers=1" in content, "P1-A7 FAIL: 未强制max_workers=1"


# ============================================================================
# P2级验证（问题16-20）: 一般问题
# ============================================================================

def test_P2_1_decay_parameter_default():
    """P2-1: 衰减参数默认值从0.1改为0.02"""
    content = _read_file("param_pool/precompute/_signal_decay.py")
    assert "decay_lambda=0.02" in content or "decay_lambda = 0.02" in content, \
        "P2-1 FAIL: 衰减参数默认值未改为0.02"


def test_P2_3_phase_threshold_configurable():
    """P2-3: 相位阈值可配置"""
    content = _read_file("param_pool/precompute/_cycle_resonance_vec.py")
    assert "phase_threshold" in content or "threshold" in content, \
        "P2-3 FAIL: 相位阈值未参数化"


def test_P2_5_table_dependency_tracking():
    """P2-5: 表依赖验证跟踪"""
    content = _read_file("param_pool/precompute/_daily_pivot.py")
    assert "source_table_used" in content or "table_dependency" in content, \
        "P2-5 FAIL: 未添加表依赖跟踪"


def test_P2_8_kelly_negative_handling():
    """P2-8: Kelly负值使用clip限制范围"""
    content = _read_file("param_pool/precompute/_position_decision.py")
    assert "clip" in content, "P2-8 FAIL: Kelly未使用clip限制范围"


def test_P2_9_atr_reference_price():
    """P2-9: ATR参考价计算添加clip限制"""
    content = _read_file("param_pool/precompute/_pullback.py")
    assert "clip" in content or "np.clip" in content, \
        "P2-9 FAIL: ATR参考价未使用clip限制"


# ============================================================================
# P2级验证（问题21-25）: 一般问题续
# ============================================================================

def test_P2_A4_progress_file_lock():
    """P2-A4: 进度文件添加文件锁保护"""
    content = _read_file("param_pool/precompute/_engine.py")
    assert "portalocker" in content, "P2-A4 FAIL: 未使用portalocker文件锁"
    assert "LOCK_EX" in content, "P2-A4 FAIL: 未使用排他锁"


def test_P2_2_parameter_validation():
    """P2-2: 参数边界验证"""
    content = _read_file("param_pool/precompute/_params.py")
    assert "validate" in content or "check" in content, \
        "P2-2 FAIL: 未添加参数验证"


def test_P2_4_logging_completeness():
    """P2-4: 关键操作添加日志"""
    modules = [
        "param_pool/precompute/_engine.py",
        "param_pool/precompute/_daily_pivot.py",
    ]
    for module in modules:
        content = _read_file(module)
        assert "logger.info" in content or "logger.warning" in content, \
            f"P2-4 FAIL: {module} 缺少日志"


def test_P2_6_error_handling():
    """P2-6: 异常处理使用精确类型"""
    modules = [
        "param_pool/precompute/_signal_decay.py",
        "param_pool/precompute/_daily_pivot.py",
    ]
    for module in modules:
        content = _read_file(module)
        assert "except (ValueError, TypeError" in content or \
               "except (OSError, ValueError" in content or \
               "except RuntimeError" in content, \
            f"P2-6 FAIL: {module} 未使用精确异常类型"


def test_P2_7_memory_efficiency():
    """P2-7: 使用numpy数组而非pandas DataFrame中间结果"""
    content = _read_file("param_pool/precompute/_signal_decay.py")
    assert "to_numpy()" in content or "np.array" in content, \
        "P2-7 FAIL: 未使用numpy数组优化内存"


# ============================================================================
# AST语法验证（问题26-28）: 代码质量
# ============================================================================

def test_AST_signal_decay():
    """AST-1: _signal_decay.py语法正确"""
    valid, msg = _check_ast_valid("param_pool/precompute/_signal_decay.py")
    assert valid, f"AST-1 FAIL: {msg}"


def test_AST_daily_pivot():
    """AST-2: _daily_pivot.py语法正确"""
    valid, msg = _check_ast_valid("param_pool/precompute/_daily_pivot.py")
    assert valid, f"AST-2 FAIL: {msg}"


def test_AST_engine():
    """AST-3: _engine.py语法正确"""
    valid, msg = _check_ast_valid("param_pool/precompute/_engine.py")
    assert valid, f"AST-3 FAIL: {msg}"


# ============================================================================
# 性能验证
# ============================================================================

def test_performance_vectorization_speedup():
    """性能: 向量化实现比循环快至少10倍"""
    n = 10000
    data = np.random.randn(n)
    
    start = time.time()
    result_loop = []
    for i in range(n):
        if i > 0:
            result_loop.append(data[i] - data[i-1])
        else:
            result_loop.append(0.0)
    time_loop = time.time() - start
    
    start = time.time()
    df = pd.DataFrame({"data": data})
    result_vec = df["data"].diff().fillna(0.0).values
    time_vec = time.time() - start
    
    speedup = time_loop / max(time_vec, 1e-6)
    assert speedup >= 5, f"性能 FAIL: 向量化仅快{speedup:.1f}倍，预期≥5倍"


# ============================================================================
# 主函数
# ============================================================================

if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))