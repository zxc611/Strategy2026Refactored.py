#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""第四轮断言验证: P0-14~P0-20, P1-14, P1-15 修复验证"""
import sys
import os

# 添加项目根目录到 path
_proj_root = os.path.dirname(os.path.abspath(__file__))
if _proj_root not in sys.path:
    sys.path.insert(0, _proj_root)

_results = []

def _check(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    _results.append((status, label, detail))
    print(f"  {status}: {label}" + (f" - {detail}" if detail else ""))

print("=" * 70)
print("第四轮断言验证: P0-14 ~ P0-20, P1-14, P1-15 修复验证")
print("=" * 70)

# ========== P0-14: is_active=1 过滤回退 ==========
print("\n[P0-14] is_active=1 活跃品种定义过滤回退验证")
try:
    _p = os.path.join(_proj_root, "data", "storage_query_instrument.py")
    with open(_p, encoding="utf-8") as _f:
        _src = _f.read()
    _check(
        "future_products 回退查询 is_active=0",
        "WHERE is_active=0" in _src and "FIX-P0-14" in _src,
        "期货品种 is_active=0 回退查询已添加",
    )
    _check(
        "option_products 回退查询 is_active=0",
        "option_products WHERE is_active=0" in _src,
        "期权品种 is_active=0 回退查询已添加",
    )
    _check(
        "非活跃品种回退告警日志",
        "[FIX-P0-14]" in _src and "is_active=0(非活跃)" in _src,
        "回退时输出告警日志",
    )
except Exception as _e:
    _check("P0-14 文件读取", False, str(_e))

# ========== P0-15: drop_empty_instrument_tables 启动保护 ==========
print("\n[P0-15] drop_empty_instrument_tables 启动保护验证")
try:
    _p = os.path.join(_proj_root, "data", "storage_catalog_service.py")
    with open(_p, encoding="utf-8") as _f:
        _src = _f.read()
    _check(
        "启动保护期检查",
        "FIX-P0-15" in _src and "_drop_empty_guard_until" in _src,
        "启动保护期机制已添加",
    )
    _check(
        "保护期内跳过执行",
        "跳过执行以防止误删合约记录" in _src,
        "保护期内跳过删除操作",
    )
except Exception as _e:
    _check("P0-15 文件读取", False, str(_e))

# ========== P0-16: underlying_future_key KeyError 修复 ==========
print("\n[P0-16] underlying_future_key KeyError 修复验证")
try:
    _p = os.path.join(_proj_root, "lifecycle", "product_initializer.py")
    with open(_p, encoding="utf-8") as _f:
        _src = _f.read()
    _check(
        "使用 .get() 替代直接字典访问",
        "future_id_map.get(underlying_future_key)" in _src,
        "直接字典访问改为 .get()",
    )
    _check(
        "None 检查与跳过",
        "FIX-P0-16" in _src and "if underlying_future_id is None:" in _src,
        "None 时跳过该期权并记录日志",
    )
    _check(
        "不再有直接字典访问 future_id_map[underlying_future_key]",
        "future_id_map[underlying_future_key]" not in _src,
        "直接字典访问已移除",
    )
except Exception as _e:
    _check("P0-16 文件读取", False, str(_e))

# ========== P0-17: merge_tick_task_batch info 未找到保留 ==========
print("\n[P0-17] merge_tick_task_batch info 未找到保留验证")
try:
    _p = os.path.join(_proj_root, "data", "ds_data_writer.py")
    with open(_p, encoding="utf-8") as _f:
        _src = _f.read()
    _check(
        "保留延迟解析而非丢弃",
        "FIX-P0-17" in _src and "保留延迟解析(不再丢弃)" in _src,
        "info 未找到时保留 tick_data 延迟解析",
    )
    _check(
        "不再有 ticks dropped 日志",
        "ticks dropped" not in _src.split("FIX-P0-17")[1].split("```")[0] if "FIX-P0-17" in _src else True,
        "不再输出 dropped 日志",
    )
except Exception as _e:
    _check("P0-17 文件读取", False, str(_e))

# ========== P0-18: batch 重试超限 spill 到 WAL ==========
print("\n[P0-18] batch 重试超限 spill 到 WAL 验证")
try:
    _p = os.path.join(_proj_root, "data", "storage_async_writer_mixin.py")
    with open(_p, encoding="utf-8") as _f:
        _src = _f.read()
    _check(
        "spill 到 WAL 文件",
        "FIX-P0-18" in _src and "spill_wal_recovery.jsonl" in _src,
        "失败 batch 写入 spill WAL 文件",
    )
    _check(
        "不再直接丢弃",
        "已spill到WAL" in _src,
        "重试超限时 spill 而非直接丢弃",
    )
    _check(
        "两处 batch 重试都已修复",
        _src.count("FIX-P0-18") >= 2,
        f"修复点数量: {_src.count('FIX-P0-18')}",
    )
except Exception as _e:
    _check("P0-18 文件读取", False, str(_e))

# ========== P0-19: Tier 4 静默过滤添加日志 ==========
print("\n[P0-19] Tier 4 静默过滤添加日志验证")
try:
    _p = os.path.join(_proj_root, "data", "width_cache_query_mixin.py")
    with open(_p, encoding="utf-8") as _f:
        _src = _f.read()
    _check(
        "Tier 4 跳过添加日志",
        "FIX-P0-19" in _src and "_tier4_skip_count" in _src,
        "Tier 4 跳过时输出计数与周期日志",
    )
    _check(
        "周期性告警(前5次+每100次)",
        "_tier4_skip_count <= 5 or _tier4_skip_count % 100 == 0" in _src,
        "前5次+每100次输出日志",
    )
except Exception as _e:
    _check("P0-19 文件读取", False, str(_e))

# ========== P0-20: ctx.reject_name 拼写错误 ==========
print("\n[P0-20] ctx.reject_name 拼写错误修复验证")
try:
    _p = os.path.join(_proj_root, "signal", "signal_service.py")
    with open(_p, encoding="utf-8") as _f:
        _src = _f.read()
    _check(
        "reject_name 改为 reject_reason",
        "ctx.reject_reason = 'decision_score_exception'" in _src,
        "拼写错误已修复",
    )
    _check(
        "FIX-P0-20 标记存在",
        "FIX-P0-20" in _src,
        "修复标记已添加",
    )
    _check(
        "不再有 reject_name 拼写错误(排除注释)",
        all(
            line.strip().startswith('#') or 'reject_name' not in line
            for line in _src.split('\n')
            if 'reject_name' in line
        ),
        "reject_name 仅存在于注释中(实际代码已修复)",
    )
except Exception as _e:
    _check("P0-20 文件读取", False, str(_e))

# ========== P1-14: _is_running is None 保守放行 ==========
print("\n[P1-14] _is_running is None 保守放行验证")
try:
    _p = os.path.join(_proj_root, "strategy", "tick_processing_service.py")
    with open(_p, encoding="utf-8") as _f:
        _src = _f.read()
    _check(
        "None 时视为 True(保守放行)",
        "FIX-P1-14" in _src and "is_running = True" in _src,
        "None 时保守放行而非丢弃",
    )
    _check(
        "不再有 is_running = False(丢弃)",
        "is_running = False" not in _src.split("FIX-P1-14")[0] if "FIX-P1-14" in _src else True,
        "None 时不再设为 False",
    )
except Exception as _e:
    _check("P1-14 文件读取", False, str(_e))

# ========== P1-15: 序列号 <= 改为 < ==========
print("\n[P1-15] 序列号 <= 改为 < 验证")
try:
    _p = os.path.join(_proj_root, "strategy", "tick_dispatch.py")
    with open(_p, encoding="utf-8") as _f:
        _src = _f.read()
    _check(
        "<= 改为 < (允许相等序列号)",
        "FIX-P1-15" in _src and "if _seq_int < _last_seq:" in _src,
        "仅丢弃严格小于的序列号",
    )
    _check(
        "不再有 <= _last_seq 比较",
        "_seq_int <= _last_seq" not in _src,
        "<= 比较已移除",
    )
except Exception as _e:
    _check("P1-15 文件读取", False, str(_e))

# ========== 语法验证 ==========
print("\n[语法验证] py_compile 编译测试")
import py_compile
_files = [
    ("data", "storage_query_instrument.py"),
    ("data", "storage_catalog_service.py"),
    ("data", "ds_data_writer.py"),
    ("data", "storage_async_writer_mixin.py"),
    ("data", "width_cache_query_mixin.py"),
    ("signal", "signal_service.py"),
    ("strategy", "tick_processing_service.py"),
    ("strategy", "tick_dispatch.py"),
    ("lifecycle", "product_initializer.py"),
]
for _sub_dir, _fname in _files:
    _fp = os.path.join(_proj_root, _sub_dir, _fname)
    _mod_name = _fname.replace('.py', '')
    try:
        py_compile.compile(_fp, doraise=True)
        _check(f"SYNTAX: {_mod_name}", True)
    except py_compile.PyCompileError as _e:
        _check(f"SYNTAX: {_mod_name}", False, str(_e))

# ========== 汇总 ==========
print("\n" + "=" * 70)
_pass = sum(1 for s, _, _ in _results if s == "PASS")
_fail = sum(1 for s, _, _ in _results if s == "FAIL")
print(f"汇总: {_pass}/{_pass + _fail} PASS, {_fail} FAIL")
print("=" * 70)

if _fail > 0:
    print("\n失败项:")
    for s, l, d in _results:
        if s == "FAIL":
            print(f"  FAIL: {l} - {d}")
    sys.exit(1)
else:
    print("\n所有断言验证通过!")
    sys.exit(0)
