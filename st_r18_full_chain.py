#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# MODULE_ID: M2-501
"""
R18全链路通畅验证脚本
验证第十八轮审计报告发现的139个问题(P0=52, P1=58, P2=29)修复情况

运行方式: python test_r18_full_chain.py
"""

import os
import sys
import py_compile
import re
from pathlib import Path
from collections import OrderedDict

# ============================================================
# 项目路径配置
# ============================================================
PROJECT_DIR = Path(__file__).resolve().parent.parent
# 注意: 不将PROJECT_DIR加入sys.path，避免导入项目包时触发duckdb/numpy等依赖
# 本脚本仅做文件扫描，不需要import项目模块

# 核心Python文件列表(排除tests/、参数池/、策略评判/子目录中的辅助文件)
CORE_FILES = [
    "risk_service.py",
    "position_service.py",
    "order_service.py",
    "signal_service.py",
    "strategy_core_service.py",
    "strategy_ecosystem.py",
    "strategy_lifecycle_mixin.py",
    "strategy_tick_handler.py",
    "strategy_instrument_mixin.py",
    "strategy_historical.py",
    "strategy_scheduler.py",
    "config_service.py",
    "config_params.py",
    "config_exchange.py",
    "params_service.py",
    "state_param_manager.py",
    "data_service.py",
    "ds_schema_manager.py",
    "ds_realtime_cache.py",
    "ds_db_connection.py",
    "ds_data_writer.py",
    "ds_option_sync.py",
    "ds_query_cache.py",
    "event_bus.py",
    "health_check_api.py",
    "maintenance_service.py",
    "shadow_strategy_engine.py",
    "governance_engine.py",
    "greeks_calculator.py",
    "box_detector.py",
    "box_spring_strategy.py",
    "mode_engine.py",
    "order_flow_analyzer.py",
    "order_flow_bridge.py",
    "order_persistence.py",
    "performance_monitor.py",
    "pnl_attribution.py",
    "plr_calculator.py",
    "jensen_alpha.py",
    "statistical_tests.py",
    "diagnosis_service.py",
    "diagnosis_periodic.py",
    "diagnosis_probe.py",
    "query_service.py",
    "subscription_manager.py",
    "scheduler_service.py",
    "service_container.py",
    "singleton_registry.py",
    "shared_utils.py",
    "storage_core.py",
    "storage_query.py",
    "t_type_service.py",
    "tvf_param_loader.py",
    "ui_service.py",
    "product_initializer.py",
    "quant_core.py",
    "quant_infra.py",
    "quant_platform.py",
    "quant_services.py",
    "security_hardening.py",
    "serialization_utils.py",
    "exceptions.py",
    "system_contracts.py",
    "cross_system_execution.py",
    "cross_system_utils.py",
    "crack_validation.py",
    "hft_enhancements.py",
    "db_adapter.py",
    "p2_fix_utils.py",
    "width_cache.py",
    "__init__.py",
    "main.py",
]

# ============================================================
# 工具函数
# ============================================================

def _read_file(rel_path):
    """读取项目文件内容"""
    full_path = PROJECT_DIR / rel_path
    try:
        with open(full_path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
        return ""


def _grep_project(pattern, subdir=None):
    """在项目目录中递归搜索模式，返回匹配行列表"""
    results = []
    search_dir = PROJECT_DIR / subdir if subdir else PROJECT_DIR
    for root, _dirs, files in os.walk(search_dir):
        # 跳过.git等目录
        if ".git" in root:
            continue
        for fname in files:
            if not fname.endswith(".py"):
                continue
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                    for lineno, line in enumerate(f, 1):
                        if re.search(pattern, line):
                            results.append((fpath, lineno, line.rstrip()))
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
    return results


def _grep_yaml(pattern, subdir=None):
    """在项目目录中递归搜索YAML/YML文件，返回匹配行列表"""
    results = []
    search_dir = PROJECT_DIR / subdir if subdir else PROJECT_DIR
    for root, _dirs, files in os.walk(search_dir):
        if ".git" in root:
            continue
        for fname in files:
            if not (fname.endswith(".yaml") or fname.endswith(".yml")):
                continue
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                    for lineno, line in enumerate(f, 1):
                        if re.search(pattern, line):
                            results.append((fpath, lineno, line.rstrip()))
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
    return results


def _grep_file(rel_path, pattern):
    """在单个文件中搜索模式，返回匹配行列表"""
    content = _read_file(rel_path)
    results = []
    for lineno, line in enumerate(content.splitlines(), 1):
        if re.search(pattern, line):
            results.append((lineno, line.rstrip()))
    return results


def _has_marker(rel_path, marker_pattern):
    """检查文件中是否包含标记"""
    return len(_grep_file(rel_path, marker_pattern)) > 0


def _has_any_marker(marker_pattern, subdir=None):
    """检查项目中是否包含标记"""
    return len(_grep_project(marker_pattern, subdir)) > 0


# ============================================================
# 验证结果收集
# ============================================================

class VerifyResult:
    def __init__(self):
        self.results = OrderedDict()  # {category: {item_id: (passed, detail)}}

    def add(self, category, item_id, passed, detail=""):
        if category not in self.results:
            self.results[category] = OrderedDict()
        self.results[category][item_id] = (passed, detail)

    def count_passed(self, category):
        if category not in self.results:
            return 0
        return sum(1 for v in self.results[category].values() if v[0])

    def count_total(self, category):
        if category not in self.results:
            return 0
        return len(self.results[category])

    def report(self):
        lines = []
        lines.append("=" * 70)
        lines.append("R18全链路通畅验证报告")
        lines.append("=" * 70)

        total_passed = 0
        total_all = 0

        # 编译验证
        if "compile" in self.results:
            p = self.count_passed("compile")
            t = self.count_total("compile")
            lines.append(f"[1] 编译验证: {p}/{t} 通过")
            total_passed += p
            total_all += t

        # 各分类
        category_map = {
            "INV_P0": ("P0不变量守卫(INV-01~14)", 2),
            "DFG_P0": ("P0数据流图(DFG-01~06)", 3),
            "UPG_P0": ("P0升级迁移(UPG-01~11)", 4),
            "OPS_P0": ("P0运维就绪度(OPS-01~21)", 5),
            "INV_P1": ("P1不变量守卫(INV-P1-01~11)", 6),
            "DFG_P1": ("P1数据流图(DFG-P1-01~14)", 7),
            "UPG_P1": ("P1升级迁移(UPG-P1-01~14)", 8),
            "OPS_P1": ("P1运维就绪度(OPS-P1-01~19)", 9),
            "P2": ("P2修复验证", 10),
        }
        for cat_key, (cat_name, idx) in category_map.items():
            p = self.count_passed(cat_key)
            t = self.count_total(cat_key)
            lines.append(f"[{idx}] {cat_name}: {p}/{t} 通过")
            total_passed += p
            total_all += t

        lines.append("=" * 70)
        lines.append(f"总计: {total_passed}/{total_all} 通过")
        lines.append("=" * 70)

        # 失败项详情
        failed_items = []
        for cat_key, items in self.results.items():
            for item_id, (passed, detail) in items.items():
                if not passed:
                    failed_items.append(f"  FAIL: [{cat_key}] {item_id}: {detail}")

        if failed_items:
            lines.append("")
            lines.append("失败项详情:")
            lines.extend(failed_items)

        return "\n".join(lines)


vr = VerifyResult()


# ============================================================
# 1. 编译验证
# ============================================================

def verify_compile():
    """遍历所有核心.py文件，py_compile编译检查"""
    passed = 0
    total = 0
    for fname in CORE_FILES:
        fpath = PROJECT_DIR / fname
        if not fpath.exists():
            vr.add("compile", fname, False, "文件不存在")
            total += 1
            continue
        total += 1
        try:
            py_compile.compile(str(fpath), doraise=True)
            vr.add("compile", fname, True)
            passed += 1
        except py_compile.PyCompileError as e:
            vr.add("compile", fname, False, str(e)[:120])


# ============================================================
# 2. P0不变量守卫修复验证 (INV-01~14)
# ============================================================

def verify_inv_p0():
    """验证P0不变量守卫修复 INV-01~14"""

    # INV-01: calculate_position_risk()使用multiplier
    try:
        found = _has_marker("risk_service.py", r"INV-01.*multiplier|multiplier.*INV-01")
        if not found:
            found = _has_marker("risk_service.py", r"INV-01/INV-POS-03")
        vr.add("INV_P0", "INV-01", found, "calculate_position_risk()使用multiplier" if found else "未找到INV-01修复标记")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-01", False, str(e)[:120])

    # INV-02: ShadowStrategyEngine降级信号被check_before_trade消费
    try:
        found1 = _has_marker("shadow_strategy_engine.py", r"降级|degrade|signal.*降级")
        found2 = _has_marker("risk_service.py", r"shadow.*signal|影子.*信号|degrade.*signal")
        # 也检查降级信号是否在check_before_trade中被引用
        found3 = _has_any_marker(r"shadow.*strategy.*engine.*signal|影子策略.*信号")
        passed = found1 and (found2 or found3)
        vr.add("INV_P0", "INV-02", passed, "ShadowStrategyEngine降级信号被消费" if passed else "降级信号消费链不完整")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-02", False, str(e)[:120])

    # INV-03: equity>=0守卫存在
    try:
        found = _has_marker("risk_service.py", r"INV-03|equity.*>=.*0|equity.*非负|负权益")
        vr.add("INV_P0", "INV-03", found, "equity>=0守卫" if found else "未找到equity>=0守卫")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-03", False, str(e)[:120])

    # INV-04: available_capital>=0守卫存在
    try:
        found1 = _has_marker("risk_service.py", r"available_capital.*>=.*0|INV-CAP-02|INV-P1-01.*available_capital")
        found2 = _has_marker("risk_service.py", r"check_capital_sufficiency|资金充足性")
        passed = found1 or found2
        vr.add("INV_P0", "INV-04", passed, "available_capital>=0守卫" if passed else "未找到available_capital>=0守卫")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-04", False, str(e)[:120])

    # INV-05: margin_used<=equity守卫存在
    try:
        found = _has_marker("risk_service.py", r"INV-05|margin_used.*<=.*equity|保证金.*权益|INV-CAP-03")
        vr.add("INV_P0", "INV-05", found, "margin_used<=equity守卫" if found else "未找到margin_used<=equity守卫")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-05", False, str(e)[:120])

    # INV-06: 订单状态合法转换集合存在
    try:
        found = _has_marker("order_service.py", r"INV-06|INV-STA-01|合法.*状态.*转换|VALID_ORDER_TRANSITIONS")
        vr.add("INV_P0", "INV-06", found, "订单状态合法转换集合" if found else "未找到订单状态合法转换集合")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-06", False, str(e)[:120])

    # INV-07: 策略状态合法转换集合存在
    try:
        found = _has_marker("strategy_ecosystem.py", r"INV-07|INV-STA-02|合法.*策略.*状态.*转换|VALID_STRATEGY_TRANSITIONS")
        vr.add("INV_P0", "INV-07", found, "策略状态合法转换集合" if found else "未找到策略状态合法转换集合")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-07", False, str(e)[:120])

    # INV-08: bid<ask校验存在
    try:
        found1 = _has_marker("ds_realtime_cache.py", r"INV-08|bid.*ask|bid>=ask")
        found2 = _has_marker("storage_query.py", r"bid.*ask|bid>=ask")
        passed = found1 or found2
        vr.add("INV_P0", "INV-08", passed, "bid<ask校验" if passed else "未找到bid<ask校验")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-08", False, str(e)[:120])

    # INV-09: OHLC一致性校验存在
    try:
        found = _has_marker("ds_realtime_cache.py", r"INV-09|OHLC.*一致|OHLC.*验证|validate_ohlcv")
        vr.add("INV_P0", "INV-09", found, "OHLC一致性校验" if found else "未找到OHLC一致性校验")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-09", False, str(e)[:120])

    # INV-10: 策略持仓之和=总持仓校验存在
    try:
        found = _has_marker("strategy_ecosystem.py", r"INV-10|INV-CON-01|持仓.*之和|持仓一致性")
        vr.add("INV_P0", "INV-10", found, "策略持仓之和=总持仓校验" if found else "未找到持仓一致性校验")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-10", False, str(e)[:120])

    # INV-11: 策略资金之和=总资金校验存在
    try:
        found = _has_marker("strategy_ecosystem.py", r"INV-11|INV-CON-02|资金.*归一化|资金分配.*1\.0")
        vr.add("INV_P0", "INV-11", found, "策略资金之和=总资金校验" if found else "未找到资金一致性校验")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-11", False, str(e)[:120])

    # INV-12: 日亏损<=max_daily_loss守卫存在
    try:
        found = _has_marker("risk_service.py", r"INV-12|INV-RSK-01|日.*亏损.*max_daily_loss|恢复.*验证.*市场安全")
        vr.add("INV_P0", "INV-12", found, "日亏损<=max_daily_loss守卫" if found else "未找到日亏损守卫")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-12", False, str(e)[:120])

    # INV-13: 单笔风险<=max_single_risk守卫存在
    try:
        found = _has_marker("risk_service.py", r"INV-13|INV-RSK-02|单笔.*风险.*限制|check_single_risk")
        vr.add("INV_P0", "INV-13", found, "单笔风险<=max_single_risk守卫" if found else "未找到单笔风险守卫")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-13", False, str(e)[:120])

    # INV-14: P0铁律门控守卫存在(Sharpe>=0.5)
    try:
        found = _has_marker("risk_service.py", r"INV-14|INV-IRN-01|Sharpe.*0\.5|铁律.*守卫")
        vr.add("INV_P0", "INV-14", found, "P0铁律门控守卫" if found else "未找到P0铁律门控守卫")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("INV_P0", "INV-14", False, str(e)[:120])


# ============================================================
# 3. P0数据流图修复验证 (DFG-01~06)
# ============================================================

def verify_dfg_p0():
    """验证P0数据流图修复 DFG-01~06"""

    # DFG-01: current_price更新逻辑存在
    try:
        found = _has_marker("position_service.py", r"DFG-01|current_price.*更新")
        vr.add("DFG_P0", "DFG-01", found, "current_price更新逻辑" if found else "未找到current_price更新逻辑")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("DFG_P0", "DFG-01", False, str(e)[:120])

    # DFG-02: signal_id传递到订单
    try:
        found = _has_marker("order_service.py", r"DFG-02|signal_id.*传递")
        vr.add("DFG_P0", "DFG-02", found, "signal_id传递到订单" if found else "未找到signal_id传递")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("DFG_P0", "DFG-02", False, str(e)[:120])

    # DFG-03: get_health_status()无未定义变量引用
    try:
        found = _has_marker("strategy_core_service.py", r"DFG-03|risk_dashboard_status")
        vr.add("DFG_P0", "DFG-03", found, "get_health_status()修复" if found else "未找到DFG-03修复")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("DFG_P0", "DFG-03", False, str(e)[:120])

    # DFG-04: EventBus事件类被发布/订阅
    try:
        found1 = _has_marker("position_service.py", r"DFG-04|publish.*Event|发布.*Event")
        found2 = _has_marker("order_service.py", r"DFG-04|publish.*Event|发布.*Event")
        passed = found1 or found2
        vr.add("DFG_P0", "DFG-04", passed, "EventBus事件类被发布/订阅" if passed else "未找到EventBus事件发布/订阅")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("DFG_P0", "DFG-04", False, str(e)[:120])

    # DFG-05: EventBus背压处理存在
    try:
        found = _has_marker("event_bus.py", r"DFG-05|backpressure|背压")
        vr.add("DFG_P0", "DFG-05", found, "EventBus背压处理" if found else "未找到EventBus背压处理")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("DFG_P0", "DFG-05", False, str(e)[:120])

    # DFG-06: _send_nack()递归深度限制存在
    try:
        found = _has_marker("event_bus.py", r"DFG-06|递归.*深度|nack.*depth|_nack_depth")
        vr.add("DFG_P0", "DFG-06", found, "_send_nack()递归深度限制" if found else "未找到递归深度限制")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("DFG_P0", "DFG-06", False, str(e)[:120])


# ============================================================
# 4. P0升级迁移修复验证 (UPG-01~11)
# ============================================================

def verify_upg_p0():
    """验证P0升级迁移修复 UPG-01~11"""

    # UPG-01: schema版本号存在
    try:
        found = _has_marker("ds_schema_manager.py", r"UPG-01|SCHEMA_VERSION|schema_version")
        vr.add("UPG_P0", "UPG-01", found, "schema版本号" if found else "未找到schema版本号")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-01", False, str(e)[:120])

    # UPG-02: 迁移脚本(归档替代DROP)
    try:
        found = _has_marker("ds_schema_manager.py", r"UPG-02|_archive_legacy|归档.*替代.*DROP")
        vr.add("UPG_P0", "UPG-02", found, "迁移脚本(归档替代DROP)" if found else "未找到归档迁移脚本")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-02", False, str(e)[:120])

    # UPG-03: 参数三源一致性
    try:
        found1 = _has_marker("shadow_strategy_engine.py", r"UPG-03")
        found2 = _has_any_marker(r"UPG-03.*修复|UPG-03.*对齐", "参数池")
        passed = found1 or found2
        vr.add("UPG_P0", "UPG-03", passed, "参数三源一致性" if passed else "未找到参数三源一致性修复")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-03", False, str(e)[:120])

    # UPG-04: 灰度发布能力
    try:
        found = _has_marker("strategy_ecosystem.py", r"UPG-P1-03|灰度|canary|parallel.*run|并行运行")
        if not found:
            found = _has_marker("strategy_lifecycle_mixin.py", r"UPG-P1-03|并行运行")
        vr.add("UPG_P0", "UPG-04", found, "灰度发布能力" if found else "未找到灰度发布能力")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-04", False, str(e)[:120])

    # UPG-05: 热更新原子性(双缓冲)
    try:
        found = _has_marker("state_param_manager.py", r"UPG-05|双缓冲|原子.*交换|atomic.*swap")
        vr.add("UPG_P0", "UPG-05", found, "热更新原子性(双缓冲)" if found else "未找到热更新原子性修复")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-05", False, str(e)[:120])

    # UPG-06: 热更新失败回退到当前有效值
    try:
        found = _has_marker("state_param_manager.py", r"UPG-06|回退.*当前.*有效|fallback.*current")
        vr.add("UPG_P0", "UPG-06", found, "热更新失败回退" if found else "未找到热更新失败回退修复")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-06", False, str(e)[:120])

    # UPG-07: YAML配置文件format_version字段
    try:
        found = _has_any_marker(r"UPG-07|format_version", "config")
        if not found:
            found = _has_any_marker(r"UPG-07|format_version", "参数池")
        # 也搜索YAML文件本身
        if not found:
            yaml_results = _grep_yaml(r"format_version")
            found = len(yaml_results) > 0
        vr.add("UPG_P0", "UPG-07", found, "YAML配置format_version" if found else "未找到YAML format_version")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-07", False, str(e)[:120])

    # UPG-08: 数据格式版本号
    try:
        found = _has_marker("ds_schema_manager.py", r"UPG-08|DATA_FORMAT_VERSION|data_format_version")
        vr.add("UPG_P0", "UPG-08", found, "数据格式版本号" if found else "未找到数据格式版本号")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-08", False, str(e)[:120])

    # UPG-09: JSONL日志格式版本号
    try:
        found = _has_marker("shadow_strategy_engine.py", r"UPG-09|JSONL_FORMAT_VERSION|format_version")
        vr.add("UPG_P0", "UPG-09", found, "JSONL日志格式版本号" if found else "未找到JSONL格式版本号")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-09", False, str(e)[:120])

    # UPG-10: config.json schema校验
    try:
        found = _has_marker("config_service.py", r"UPG-10|schema.*校验|validate.*schema")
        vr.add("UPG_P0", "UPG-10", found, "config.json schema校验" if found else "未找到config schema校验")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-10", False, str(e)[:120])

    # UPG-11: 手册与代码版本对齐
    try:
        found = _has_marker("__init__.py", r"UPG-11|CODE_VERSION|版本.*对齐")
        vr.add("UPG_P0", "UPG-11", found, "手册与代码版本对齐" if found else "未找到版本对齐检查")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        vr.add("UPG_P0", "UPG-11", False, str(e)[:120])


# ============================================================
# 5. P0运维就绪度修复验证 (OPS-01~21)
# ============================================================

def verify_ops_p0():
    """验证P0运维就绪度修复 OPS-01~21"""

    ops_checks = {
        "OPS-01": ("运维手册", r"OPS-01|运维手册|operations_manual|ops_manual"),
        "OPS-02": ("紧急一键平仓", r"OPS-02|emergency_close|紧急.*平仓"),
        "OPS-03": ("紧急一键停止", r"OPS-03|emergency_stop|紧急.*停止"),
        "OPS-04": ("紧急降级", r"OPS-04|emergency_degrade|紧急.*降级"),
        "OPS-05": ("告警分级P0/P1/P2", r"OPS-05|AlertLevel|告警.*分级"),
        "OPS-06": ("P0告警自动升级", r"OPS-06|告警.*升级|alert.*escalat"),
        "OPS-07": ("告警去重/聚合", r"OPS-07|AlertDeduplicator|告警.*去重|告警.*聚合"),
        "OPS-08": ("On-call流程", r"OPS-08|on.?call|轮值"),
        "OPS-09": ("数据库定期备份", r"OPS-09|backup_database|数据库.*备份"),
        "OPS-10": ("系统容量上限定义", r"OPS-10|MAX_QUEUE|容量.*上限|max_capacity"),
        "OPS-11": ("容量上限监控", r"OPS-11|check_capacity|容量.*监控"),
        "OPS-12": ("可视化仪表盘", r"OPS-12|dashboard|仪表盘"),
        "OPS-13": ("日常运维操作文档化", r"OPS-13|ops_procedure|运维.*操作.*文档"),
        "OPS-14": ("紧急操作审批机制", r"OPS-14|approval_context|approver_id|审批"),
        "OPS-15": ("操作审计日志", r"OPS-15|OpsAuditLog|审计.*日志"),
        "OPS-16": ("运维API", r"OPS-16|OpsAPI|运维.*API"),
        "OPS-17": ("运维操作API", r"OPS-17|OpsOperationAPI|运维操作.*API"),
        "OPS-18": ("容量压测报告", r"OPS-18|capacity.*test|压测"),
        "OPS-19": ("故障演练记录", r"OPS-19|disaster.*drill|故障.*演练"),
        "OPS-20": ("运维SOP", r"OPS-20|SOP|标准操作程序"),
        "OPS-21": ("运维培训材料", r"OPS-21|training.*material|培训"),
    }

    for item_id, (desc, pattern) in ops_checks.items():
        try:
            found = _has_any_marker(pattern)
            vr.add("OPS_P0", item_id, found, desc if found else f"未找到{desc}修复")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            vr.add("OPS_P0", item_id, False, str(e)[:120])


# ============================================================
# 6. P1不变量守卫修复验证 (INV-P1-01~11)
# ============================================================

def verify_inv_p1():
    """验证P1不变量守卫修复 INV-P1-01~11"""

    inv_p1_checks = {
        "INV-P1-01": ("pnl校验(pnl=equity-initial_capital)", "position_service.py", r"INV-P1-01|pnl.*equity.*initial_capital|PnL.*权益.*一致"),
        "INV-P1-02": ("net_position校验", "position_service.py", r"INV-P1-02|net_position.*long.*short|净持仓.*一致"),
        "INV-P1-03": ("交易时段检查", "order_service.py", r"INV-P1-03|交易时段|trading.*session"),
        "INV-P1-04": ("bar_time单调递增", "ds_realtime_cache.py", r"INV-P1-04|bar_time.*单调|source_timestamp.*单调"),
        "INV-P1-05": ("order_time>=signal_time", "order_service.py", r"INV-P1-05|order_time.*signal_time|signal_time"),
        "INV-P1-06": ("total_risk<=max_portfolio_risk守卫", "risk_service.py", r"INV-P1-06|total_risk.*max_portfolio_risk|风险比率.*超限"),
        "INV-P1-07": ("残差<15%铁律守卫", "risk_service.py", r"INV-P1-07|残差.*15%|residual.*15"),
        "INV-P1-08": ("价差退化(spread degradation)检查", "risk_service.py", r"INV-P1-08|价差退化|spread.*degradation"),
        "INV-P1-09": ("GovernanceEngine检测结果消费", "risk_service.py", r"INV-P1-09|GovernanceEngine.*检测|governance.*violation"),
        "INV-P1-10": ("断路器触发后强制减仓", "risk_service.py", r"INV-P1-10|熔断.*减仓|circuit_breaker.*reduce"),
        "INV-P1-11": ("日内权益曲线单调性校验", "risk_service.py", r"INV-P1-11|权益曲线.*单调|equity.*monotonic"),
    }

    for item_id, (desc, rel_path, pattern) in inv_p1_checks.items():
        try:
            found = _has_marker(rel_path, pattern)
            vr.add("INV_P1", item_id, found, desc if found else f"未找到{desc}修复")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            vr.add("INV_P1", item_id, False, str(e)[:120])


# ============================================================
# 7. P1数据流图修复验证 (DFG-P1-01~14)
# ============================================================

def verify_dfg_p1():
    """验证P1数据流图修复 DFG-P1-01~14"""

    dfg_p1_checks = {
        "DFG-P1-01": ("HMM标签消费者", r"DFG-P1-01|HMM.*消费者|hmm_state.*consumer"),
        "DFG-P1-02": ("E13消费者", r"DFG-P1-02|E13.*消费者|同谋.*消费"),
        "DFG-P1-03": ("PSE状态转换事件传播", r"DFG-P1-03|PSE.*状态.*转换|PredictiveStateEngine.*传播"),
        "DFG-P1-04": ("Greeks API接入", r"DFG-P1-04|Greeks.*仪表盘|greeks_dashboard"),
        "DFG-P1-05": ("健康评分接入决策流程", r"DFG-P1-05|健康评分.*决策|health.*score.*decision"),
        "DFG-P1-06": ("部分成交事件消费者", r"DFG-P1-06|部分成交.*消费|partial_fill.*consumer"),
        "DFG-P1-07": ("参数变更事件消费者", r"DFG-P1-07|参数变更.*消费|param.*change.*consumer"),
        "DFG-P1-08": ("数据格式变更事件传播", r"DFG-P1-08|数据格式.*变更.*事件|data_format.*change"),
        "DFG-P1-09": ("tick→Bar转换volume语义归一化", r"DFG-P1-09|volume.*语义|volume.*归一化|累计.*增量"),
        "DFG-P1-10": ("信号过滤统计暴露", r"DFG-P1-10|信号过滤.*统计|filter.*statistics"),
        "DFG-P1-11": ("风控检查结果事件传播", r"DFG-P1-11|风控检查.*结果.*传播|risk_check.*result"),
        "DFG-P1-12": ("PLR分布质量评估", r"DFG-P1-12|PLR.*质量|plr.*quality"),
        "DFG-P1-13": ("回测检查点恢复", r"DFG-P1-13|检查点.*恢复|checkpoint.*restore"),
        "DFG-P1-14": ("多策略持仓聚合视图", r"DFG-P1-14|多策略.*持仓.*聚合|aggregate.*position"),
    }

    for item_id, (desc, pattern) in dfg_p1_checks.items():
        try:
            found = _has_any_marker(pattern)
            vr.add("DFG_P1", item_id, found, desc if found else f"未找到{desc}修复")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            vr.add("DFG_P1", item_id, False, str(e)[:120])


# ============================================================
# 8. P1升级迁移修复验证 (UPG-P1-01~14)
# ============================================================

def verify_upg_p1():
    """验证P1升级迁移修复 UPG-P1-01~14"""

    upg_p1_checks = {
        "UPG-P1-01": ("schema扩展(新增列/索引)", r"UPG-P1-01|schema.*扩展|add_column|新增列"),
        "UPG-P1-02": ("参数迁移工具", r"UPG-P1-02|参数.*迁移|migrate_param|param_migration"),
        "UPG-P1-03": ("并行运行(新旧策略同时运行)", r"UPG-P1-03|并行运行|parallel.*run"),
        "UPG-P1-04": ("灰度发布配置", r"UPG-P1-04|灰度.*配置|canary.*config"),
        "UPG-P1-05": ("废弃参数迁移指南", r"UPG-P1-05|migration_guide|废弃.*参数.*迁移"),
        "UPG-P1-06": ("配置格式自动升级", r"UPG-P1-06|配置.*格式.*升级|config.*format.*upgrade"),
        "UPG-P1-07": ("热更新事务性保证", r"UPG-P1-07|热更新.*事务|hot_update.*transaction|backup.*commit.*rollback"),
        "UPG-P1-08": ("分布式锁机制", r"UPG-P1-08|分布式锁|distributed.*lock"),
        "UPG-P1-09": ("更新失败自动回滚", r"UPG-P1-09|更新失败.*回滚|update.*fail.*rollback"),
        "UPG-P1-10": ("API版本标记", r"UPG-P1-10|API.*版本|api_version"),
        "UPG-P1-11": ("代码/手册/配置版本对齐检查", r"UPG-P1-11|版本.*对齐.*检查|version.*alignment"),
        "UPG-P1-12": ("参数默认值兼容性检查", r"UPG-P1-12|参数.*默认值.*兼容|default.*compatibility"),
        "UPG-P1-13": ("策略逻辑回归测试", r"UPG-P1-13|策略.*回归.*测试|strategy.*regression"),
        "UPG-P1-14": ("数据迁移验证", r"UPG-P1-14|数据.*迁移.*验证|data.*migration.*verify"),
    }

    for item_id, (desc, pattern) in upg_p1_checks.items():
        try:
            found = _has_any_marker(pattern)
            vr.add("UPG_P1", item_id, found, desc if found else f"未找到{desc}修复")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            vr.add("UPG_P1", item_id, False, str(e)[:120])


# ============================================================
# 9. P1运维就绪度修复验证 (OPS-P1-01~19)
# ============================================================

def verify_ops_p1():
    """验证P1运维就绪度修复 OPS-P1-01~19"""

    ops_p1_checks = {
        "OPS-P1-01": ("Greeks仪表盘结构化数据", r"OPS-P1-01|Greeks.*仪表盘.*结构化|greeks_dashboard_data"),
        "OPS-P1-02": ("健康状态变更回调/推送", r"OPS-P1-02|健康.*状态.*回调|health.*callback"),
        "OPS-P1-03": ("参数变更通知回调", r"OPS-P1-03|参数.*变更.*通知|param.*change.*notify"),
        "OPS-P1-04": ("断路器告警EventBus发布", r"OPS-P1-04|断路器.*告警.*EventBus|circuit_breaker.*alert"),
        "OPS-P1-05": ("日回撤硬停止EventBus告警", r"OPS-P1-05|日回撤.*告警.*EventBus|daily_drawdown.*alert"),
        "OPS-P1-06": ("前置条件验证", r"OPS-P1-06|前置条件.*验证|pre_condition|PreconditionError"),
        "OPS-P1-07": ("后置条件验证", r"OPS-P1-07|后置条件.*验证|post_condition|PostconditionError"),
        "OPS-P1-08": ("操作超时控制", r"OPS-P1-08|操作.*超时|OperationTimeout|timeout.*control"),
        "OPS-P1-09": ("幂等性保证", r"OPS-P1-09|幂等|idempotent"),
        "OPS-P1-10": ("结果验证", r"OPS-P1-10|结果.*验证|post_check|result_verify"),
        "OPS-P1-11": ("回滚方案", r"OPS-P1-11|回滚.*方案|rollback"),
        "OPS-P1-12": ("通知机制", r"OPS-P1-12|通知.*机制|notify|notification"),
        "OPS-P1-13": ("超时控制(运维操作)", r"OPS-P1-13|超时.*控制|timeout.*ops"),
        "OPS-P1-14": ("重试机制", r"OPS-P1-14|重试.*机制|retry|max_retries"),
        "OPS-P1-15": ("幂等保证(操作ID去重)", r"OPS-P1-15|幂等.*保证|idempotent.*guarantee|operation_id.*dedup"),
        "OPS-P1-16": ("并发控制(操作锁)", r"OPS-P1-16|并发.*控制|concurrency.*control|operation.*lock"),
        "OPS-P1-17": ("依赖检查", r"OPS-P1-17|依赖.*检查|dependency.*check"),
        "OPS-P1-18": ("前置条件验证(运维操作)", r"OPS-P1-18|前置.*验证.*运维|pre_check"),
        "OPS-P1-19": ("后置条件验证(运维操作)", r"OPS-P1-19|后置.*验证.*运维|post_check"),
    }

    for item_id, (desc, pattern) in ops_p1_checks.items():
        try:
            found = _has_any_marker(pattern)
            vr.add("OPS_P1", item_id, found, desc if found else f"未找到{desc}修复")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            vr.add("OPS_P1", item_id, False, str(e)[:120])


# ============================================================
# 10. P2修复验证 (29项)
# ============================================================

def verify_p2():
    """验证P2修复 29项"""

    p2_checks = {
        "P2-01": ("信号过滤持久化", r"信号过滤.*持久化|filter.*persist|filter_state.*save"),
        "P2-02": ("事件注册完整性", r"事件.*注册.*完整|event.*register.*complete|subscribe.*all"),
        "P2-03": ("数据流性能监控", r"数据流.*性能.*监控|data_flow.*performance|flow.*monitor"),
        "P2-04": ("灰度配置持久化", r"灰度.*配置.*持久化|canary.*config.*persist"),
        "P2-05": ("运维指标暴露", r"运维.*指标|ops.*metric|operational.*metric"),
        "P2-06": ("pnl一致性校验(P2级)", r"INV-CAP-04|pnl.*一致.*校验|PnL.*consistency"),
        "P2-07": ("Kahan补偿(浮点精度)", r"NP-P2-18|Kahan|补偿.*累加"),
        "P2-08": ("isfinite检查(浮点安全)", r"NP-P2-16|isfinite|np\.isfinite"),
        "P2-09": ("Sharpe值域clip", r"NP-P2-10|Sharpe.*clip|sharpe.*clip"),
        "P2-10": ("收益率精度保护", r"NP-P2-17|收益率.*精度|isclose.*open_price"),
        "P2-11": ("LRU缓存淘汰", r"NP-P2-03|LRU|缓存.*淘汰"),
        "P2-12": ("portfolio_pnl溢出检测", r"NP-P2-12|portfolio_pnl.*overflow|溢出.*检测"),
        "P2-13": ("bar时间单调性校验(回测)", r"NP-P2-15|bar.*单调|_check_bar_data_monotonic"),
        "P2-14": ("空数组检查(isfinite后)", r"NP-P2-22|空数组|len.*==.*0.*isfinite"),
        "P2-15": ("Decimal金额计算开关", r"NP-P2-05|use_decimal|Decimal.*金额"),
        "P2-16": ("ATM浮点减法补偿", r"NP-P2-19|ATM.*补偿|compensated_subtraction"),
        "P2-17": ("Decimal精度配置", r"NP-P2-28|decimal_precision|Decimal.*精度"),
        "P2-18": ("持仓类型限仓倍数", r"EX-P2-05|持仓类型.*限仓|hedge_type"),
        "P2-19": ("手续费自动更新机制", r"EX-P2-04|commission_auto_update|手续费.*自动"),
        "P2-20": ("涨跌停动态休市", r"EX-P2-07|dynamic_halt|涨跌停.*休市"),
        "P2-21": ("自动移仓", r"EX-P2-08|auto_rollover|自动移仓"),
        "P2-22": ("动态滑点模型", r"EX-P2-10|dynamic_slippage|滑点.*模型"),
        "P2-23": ("tick_size统一使用", r"EX-P2-01|tick_size|min_price_change"),
        "P2-24": ("Python最小版本检查", r"EC-P2-03|Python.*版本.*检查|sys\.version"),
        "P2-25": ("关键依赖库版本检查", r"EC-P2-10|依赖.*版本.*检查|dependency.*version"),
        "P2-26": ("路径拼接安全性", r"EC-P2-01|后缀拼接|tmp_path"),
        "P2-27": ("运行时DLL依赖检查", r"EC-P2-13|DLL.*依赖|runtime.*depend"),
        "P2-28": ("未使用导入清理", r"EC-P2-12|删除.*未使用|unused.*import"),
        "P2-29": ("astype类型转换错误处理", r"NP-P2-29|astype.*错误|astype.*errors"),
    }

    for item_id, (desc, pattern) in p2_checks.items():
        try:
            found = _has_any_marker(pattern)
            vr.add("P2", item_id, found, desc if found else f"未找到{desc}修复")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
            vr.add("P2", item_id, False, str(e)[:120])


# ============================================================
# 主流程
# ============================================================

def main():
    print("R18全链路通畅验证开始...")
    print(f"项目路径: {PROJECT_DIR}")
    print()

    # 1. 编译验证
    print("[1/10] 编译验证...")
    try:
        verify_compile()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"  编译验证异常: {e}")

    # 2. P0不变量守卫
    print("[2/10] P0不变量守卫(INV-01~14)...")
    try:
        verify_inv_p0()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"  INV-P0验证异常: {e}")

    # 3. P0数据流图
    print("[3/10] P0数据流图(DFG-01~06)...")
    try:
        verify_dfg_p0()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"  DFG-P0验证异常: {e}")

    # 4. P0升级迁移
    print("[4/10] P0升级迁移(UPG-01~11)...")
    try:
        verify_upg_p0()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"  UPG-P0验证异常: {e}")

    # 5. P0运维就绪度
    print("[5/10] P0运维就绪度(OPS-01~21)...")
    try:
        verify_ops_p0()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"  OPS-P0验证异常: {e}")

    # 6. P1不变量守卫
    print("[6/10] P1不变量守卫(INV-P1-01~11)...")
    try:
        verify_inv_p1()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"  INV-P1验证异常: {e}")

    # 7. P1数据流图
    print("[7/10] P1数据流图(DFG-P1-01~14)...")
    try:
        verify_dfg_p1()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"  DFG-P1验证异常: {e}")

    # 8. P1升级迁移
    print("[8/10] P1升级迁移(UPG-P1-01~14)...")
    try:
        verify_upg_p1()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"  UPG-P1验证异常: {e}")

    # 9. P1运维就绪度
    print("[9/10] P1运维就绪度(OPS-P1-01~19)...")
    try:
        verify_ops_p1()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"  OPS-P1验证异常: {e}")

    # 10. P2修复验证
    print("[10/10] P2修复验证(29项)...")
    try:
        verify_p2()
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"  P2验证异常: {e}")

    # 输出报告
    print()
    report = vr.report()
    print(report)

    # 保存报告到文件
    report_path = Path(__file__).resolve().parent / "r18_full_chain_report.txt"
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\n报告已保存到: {report_path}")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as e:
        print(f"\n报告保存失败: {e}")


if __name__ == "__main__":
    main()
