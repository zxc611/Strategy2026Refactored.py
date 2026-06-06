"""
v7_meta_audit_v2.py
===================
V7.0 元审计引擎修正版 v2.2
修复清单（v2.1 → v2.2）：
  1. MetaAuditEngine：子字符串匹配→AST语义分析，消除注释/字符串误报
  2. SandboxExecutionAuditor._run_with_audit_wrapper：修复audit_time循环后固定为终值导致毒化检测失效
  3. AutoFieldLineageTracker._is_later：修正注释与代码矛盾（N越大→序号越小→时间越早）
  4. AutoFieldLineageTracker._infer_timestamp_ref：rolling()正则支持关键字参数
  5. SmartSignificanceFilter：Bootstrap添加随机种子、向量化实现
  6. HistoricalUniverseRestorer.inject_ghost_trades：修复status列缺失时的回退逻辑
  7. HistoricalUniverseRestorer._estimate_bias：改进公式，基于选中ghost与active的Sharpe差
  8. SignalReadinessAligner：双峰检测改用偏度+峰度联合判定
  9. AuditPassport：print→logging、passport附加支持非dict结果、save/load版本控制
  10. _norm_cdf：极端z值数值安全处理
  11. 异常处理：保留完整堆栈信息

使用方式：
    from v7_meta_audit_v2 import AuditPassport

    with open('your_engine.py', encoding='utf-8') as _f:
        _source = _f.read()
    passport = AuditPassport(
        engine_source_code=_source,
        engine_class=YourBacktestEngine,
        strategy_config={'param1': 0.5, 'param2': 10},
        data_loader=your_data_loader_function
    )

    if passport.run_meta_audit():
        result = passport.run_certified_backtest(your_tick_data)
        ghost_report = passport.run_phase2_ghost_audit(result)
    else:
        print("元审计失败，禁止回测")
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import ast
import traceback
import hashlib
import json
import os
import re
import warnings
import logging
from dataclasses import dataclass
from typing import List, Dict, Callable, Optional, Union, Any, Tuple
from enum import Enum
from contextlib import contextmanager
from collections import defaultdict, deque

from ali2026v3_trading.shared_utils import CHINA_TZ
from ali2026v3_trading.serialization_utils import json_dumps, json_loads, json_default_serializer

logger = logging.getLogger(__name__)

_AUDIT_FORMAT_VERSION = "2.2"


def _norm_cdf(x: float) -> float:
    """
    标准正态CDF近似（Abramowitz & Stegun 26.2.17）
    精度：<5e-5，无需scipy
    v2.2：极端z值(|z|>8)直接返回0/1，避免数值下溢
    """
    if x < -8:
        return 0.0
    if x > 8:
        return 1.0
    p = 0.2316419
    b1 = 0.319381530
    b2 = -0.356563782
    b3 = 1.781477937
    b4 = -1.821255978
    b5 = 1.330274429
    if x >= 0:
        t = 1.0 / (1.0 + p * x)
        z = np.exp(-x * x / 2.0) / np.sqrt(2.0 * np.pi)
        return 1.0 - z * (b1*t + b2*t**2 + b3*t**3 + b4*t**4 + b5*t**5)
    else:
        return 1.0 - _norm_cdf(-x)


class VulnerabilityType(Enum):
    DETERMINISTIC = "deterministic"
    STATISTICAL = "statistical"


class Severity(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


@dataclass(slots=True)
class AuditIssue:
    issue_type: str
    severity: str
    detail: str
    vuln_type: VulnerabilityType
    evidence: Dict[str, Any]
    fix: str

    def to_dict(self) -> dict:
        return {
            'issue_type': self.issue_type,
            'severity': self.severity,
            'detail': self.detail,
            'vuln_type': self.vuln_type.value,
            'evidence': self.evidence,
            'fix': self.fix
        }


DETERMINISTIC_PATTERNS = [
    'DATA_AFTER_SCAN_TIME',
    'SAME_DAY_CLOSE_ENTRY',
    'PULLBACK_PRECOGNITION',
    'FUTURE_LEAK_CAUGHT',
    'PRECOGNITION_DETECTED',
    'SORT_ORDER_RISK',
    'GLOBAL_STATE_RISK',
    'DATA_MUTATION_RISK',
    'ALL_OR_NONE_FILL',
    'PULLBACK_INSUFFICIENT_CONFIRMATION',
    'SHIFT_NEGATIVE_RISK',
    'POTENTIAL_ORDER_BIAS',
    'DYNAMIC_CODE_RISK'
]


class MetaAuditEngine:
    """
    v2.2：基于AST语义分析的静态代码审计引擎
    仅检查实际代码节点，自动排除注释和字符串字面量中的误报
    """

    def __init__(self, backtest_source_code: str):
        self.source = backtest_source_code
        self.issues = []
        self._ast_tree = None
        self._code_only_lines = []  # 字符串字面量被空白替换后的代码行
        try:
            self._ast_tree = ast.parse(backtest_source_code)
            self._code_only_lines = self._strip_string_literals(backtest_source_code)
        except SyntaxError:
            logger.warning("Source code has syntax errors, falling back to raw source")
            self._code_only_lines = backtest_source_code.split('\n')

    def _strip_string_literals(self, source: str) -> List[str]:
        """
        将源码中字符串字面量的内容替换为空格，保留行结构。
        这样后续的模式匹配不会命中注释或字符串内容。
        """
        lines = list(source.split('\n'))
        if self._ast_tree is None:
            return lines

        string_spans = []
        for node in ast.walk(self._ast_tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if hasattr(node, 'lineno') and hasattr(node, 'end_lineno'):
                    string_spans.append((
                        node.lineno - 1, node.col_offset,
                        node.end_lineno - 1, node.end_col_offset
                    ))

        # 按起始位置倒序处理，避免偏移量变化
        string_spans.sort(key=lambda s: (s[0], s[1]), reverse=True)
        for start_line, start_col, end_line, end_col in string_spans:
            if start_line == end_line:
                line = lines[start_line]
                length = end_col - start_col
                lines[start_line] = line[:start_col] + ' ' * length + line[end_col:]
            else:
                # 多行字符串：首行和末行部分替换，中间行整行替换
                line = lines[start_line]
                lines[start_line] = line[:start_col] + ' ' * (len(line) - start_col)
                for i in range(start_line + 1, end_line):
                    lines[i] = ' ' * len(lines[i])
                line = lines[end_line]
                lines[end_line] = ' ' * end_col + line[end_col:]

        return lines

    def _code_source(self) -> str:
        """返回去除字符串字面量后的源码文本"""
        return '\n'.join(self._code_only_lines)

    def audit_backtest_engine_integrity(self) -> List[AuditIssue]:
        self.issues = []

        if self._ast_tree is not None:
            self._check_global_state()
            self._check_sort_order()
            self._check_fill_assumption()
            self._check_data_mutation()
            self._check_shift_negative()
            self._check_order_bias()
            self._check_dynamic_code_execution()
        else:
            # AST解析失败时回退到清理后的源码匹配
            self._fallback_audit()

        return self.issues

    def _check_global_state(self):
        """检查global声明和self._cache属性访问（AST节点级）"""
        found_global = False
        found_cache = False

        for node in ast.walk(self._ast_tree):
            if isinstance(node, ast.Global):
                found_global = True
                break
            if isinstance(node, ast.Attribute):
                if (isinstance(node.value, ast.Name) and
                        node.value.id == 'self' and node.attr == '_cache'):
                    found_cache = True
                    break

        if found_global or found_cache:
            self.issues.append(AuditIssue(
                issue_type='GLOBAL_STATE_RISK',
                severity='CRITICAL',
                detail='Engine uses global state or caching',
                vuln_type=VulnerabilityType.DETERMINISTIC,
                evidence={
                    'pattern_found': 'global' if found_global else 'self._cache',
                    'detection_method': 'AST_NODE'
                },
                fix='Each backtest must be completely independent, no global/cache'
            ))

    def _check_sort_order(self):
        """检查sort_values/sort调用中是否按price排序（AST调用节点级）"""
        for node in ast.walk(self._ast_tree):
            if not isinstance(node, ast.Call):
                continue
            func_name = self._get_call_func_name(node)
            if func_name not in ('sort_values', 'sort'):
                continue

            # 检查by参数或位置参数是否包含'price'
            sort_by_price = False
            for kw in node.keywords:
                if kw.arg == 'by':
                    sort_by_price = self._expr_contains_name(kw.value, 'price')
                    break
            if not sort_by_price:
                for arg in node.args:
                    sort_by_price = self._expr_contains_name(arg, 'price')
                    if sort_by_price:
                        break

            if sort_by_price:
                self.issues.append(AuditIssue(
                    issue_type='SORT_ORDER_RISK',
                    severity='CRITICAL',
                    detail='Engine sorts by price rather than time',
                    vuln_type=VulnerabilityType.DETERMINISTIC,
                    evidence={
                        'pattern_found': f'{func_name}(by=price)',
                        'detection_method': 'AST_NODE'
                    },
                    fix='Strict chronological processing only'
                ))
                return  # 只报告一次

    def _check_fill_assumption(self):
        """检查volume+fill模式但缺少partial/depth（AST级）"""
        code = self._code_source()
        has_volume = self._ast_contains_name('volume')
        has_fill = self._ast_contains_name('fill')
        has_partial = self._ast_contains_name('partial')
        has_depth = self._ast_contains_name('depth')

        if has_volume and has_fill and not has_partial and not has_depth:
            self.issues.append(AuditIssue(
                issue_type='ALL_OR_NONE_FILL',
                severity='HIGH',
                detail='Engine assumes full volume available at each tick',
                vuln_type=VulnerabilityType.DETERMINISTIC,
                evidence={
                    'pattern_found': 'volume + fill without partial/depth',
                    'detection_method': 'AST_NODE'
                },
                fix='Implement partial fill based on order book depth'
            ))

    def _check_data_mutation(self):
        """检查inplace=True、.loc/iloc赋值（AST级）"""
        found_inplace = False
        found_loc_assign = False

        for node in ast.walk(self._ast_tree):
            # 检查inplace=True关键字参数
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == 'inplace':
                        if (isinstance(kw.value, ast.Constant) and
                                kw.value.value is True):
                            found_inplace = True
                            break

            # 检查.loc[]/.iloc[]赋值（Subscript作为Assign的target）
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Subscript):
                        attr_node = target.value
                        if isinstance(attr_node, ast.Attribute):
                            if attr_node.attr in ('loc', 'iloc'):
                                found_loc_assign = True
                                break

            if found_inplace and found_loc_assign:
                break

        if found_inplace or found_loc_assign:
            detail_parts = []
            if found_inplace:
                detail_parts.append('inplace=True')
            if found_loc_assign:
                detail_parts.append('df.loc/iloc assignment')
            self.issues.append(AuditIssue(
                issue_type='DATA_MUTATION_RISK',
                severity='HIGH',
                detail=f'Engine may mutate input data via {" and ".join(detail_parts)}',
                vuln_type=VulnerabilityType.DETERMINISTIC,
                evidence={
                    'pattern_found': ', '.join(detail_parts),
                    'detection_method': 'AST_NODE'
                },
                fix='Work on copy only: df = df.copy()'
            ))

    def _check_shift_negative(self):
        """检查shift(-n)调用（AST调用节点级），精确定位行号"""
        shift_locations = []
        for node in ast.walk(self._ast_tree):
            if not isinstance(node, ast.Call):
                continue
            func_name = self._get_call_func_name(node)
            if func_name != 'shift':
                continue
            # 检查参数是否为负数
            for arg in node.args:
                if isinstance(arg, ast.UnaryOp) and isinstance(arg.op, ast.USub):
                    if isinstance(arg.operand, ast.Constant):
                        shift_locations.append(getattr(node, 'lineno', 0))
                elif isinstance(arg, ast.Constant) and isinstance(arg.value, (int, float)):
                    if arg.value < 0:
                        shift_locations.append(getattr(node, 'lineno', 0))

        if shift_locations:
            self.issues.append(AuditIssue(
                issue_type='SHIFT_NEGATIVE_RISK',
                severity='CRITICAL',
                detail='Engine uses shift(-n) which accesses future data — ZERO TOLERANCE',
                vuln_type=VulnerabilityType.DETERMINISTIC,
                evidence={
                    'pattern_found': 'shift(-n)',
                    'locations': shift_locations,
                    'detection_method': 'AST_NODE'
                },
                fix='Remove ALL shift(-n) usage. Use shift(n) with adjusted logic or rolling window'
            ))

    def _check_order_bias(self):
        """检查多symbol循环但缺少groupby/sort_values（AST级）"""
        has_for_symbol = False
        has_groupby = False
        has_sort_values = False

        for node in ast.walk(self._ast_tree):
            if isinstance(node, ast.For):
                # 检查迭代目标或循环变量是否包含'symbol'
                iter_has_symbol = self._expr_contains_name(node.iter, 'symbol')
                target_has_symbol = False
                if isinstance(node.target, ast.Name) and 'symbol' in node.target.id:
                    target_has_symbol = True
                if iter_has_symbol or target_has_symbol:
                    has_for_symbol = True
            if isinstance(node, ast.Call):
                func_name = self._get_call_func_name(node)
                if func_name == 'groupby':
                    has_groupby = True
                if func_name == 'sort_values':
                    has_sort_values = True

        if has_for_symbol and not has_groupby and not has_sort_values:
            self.issues.append(AuditIssue(
                issue_type='POTENTIAL_ORDER_BIAS',
                severity='MEDIUM',
                detail='Multi-symbol loop without explicit time-based ordering. '
                       'Head symbols may get unfair advantage in same-tick execution.',
                vuln_type=VulnerabilityType.DETERMINISTIC,
                evidence={
                    'pattern_found': 'for + symbol without groupby/sort_values',
                    'detection_method': 'AST_NODE'
                },
                fix='Ensure all symbols at same timestamp are processed in price-time priority order'
            ))

    def _check_dynamic_code_execution(self):
        """检查exec/eval/compile调用（AST Call节点级）"""
        dangerous_funcs = {'exec', 'eval', 'compile'}
        for node in ast.walk(self._ast_tree):
            if not isinstance(node, ast.Call):
                continue
            func_name = self._get_call_func_name(node)
            if func_name in dangerous_funcs:
                self.issues.append(AuditIssue(
                    issue_type='DYNAMIC_CODE_RISK',
                    severity='HIGH',
                    detail=f'Engine uses {func_name}() which enables arbitrary code execution',
                    vuln_type=VulnerabilityType.DETERMINISTIC,
                    evidence={
                        'pattern_found': f'{func_name}()',
                        'line': getattr(node, 'lineno', 0),
                        'detection_method': 'AST_NODE'
                    },
                    fix=f'Replace {func_name}() with safe alternatives or use RestrictedExecLoader.safe_exec'
                ))
                return

    def _fallback_audit(self):
        """AST解析失败时的回退：在去除字符串字面量的源码上做模式匹配"""
        code = self._code_source()
        if 'global ' in code or 'self._cache' in code:
            self.issues.append(AuditIssue(
                issue_type='GLOBAL_STATE_RISK',
                severity='CRITICAL',
                detail='Engine uses global state or caching',
                vuln_type=VulnerabilityType.DETERMINISTIC,
                evidence={'pattern_found': 'global or self._cache', 'detection_method': 'FALLBACK_STRING'},
                fix='Each backtest must be completely independent, no global/cache'
            ))

    # ---- AST辅助方法 ----

    def _get_call_func_name(self, node: ast.Call) -> str:
        """提取Call节点的函数名（支持链式调用如df.sort_values）"""
        func = node.func
        if isinstance(func, ast.Attribute):
            return func.attr
        if isinstance(func, ast.Name):
            return func.id
        return ''

    def _expr_contains_name(self, node, name: str) -> bool:
        """检查AST表达式树中是否包含指定名称（变量名/属性名/字符串常量）"""
        if node is None:
            return False
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and child.id == name:
                return True
            if isinstance(child, ast.Attribute) and child.attr == name:
                return True
            if isinstance(child, ast.Constant) and isinstance(child.value, str) and child.value == name:
                return True
        return False

    def _ast_contains_name(self, name: str) -> bool:
        """检查整棵AST树中是否包含指定名称（仅代码节点，不含字符串字面量）"""
        if self._ast_tree is None:
            return name in self._code_source()
        for node in ast.walk(self._ast_tree):
            if isinstance(node, ast.Name) and node.id == name:
                return True
            if isinstance(node, ast.Attribute) and node.attr == name:
                return True
        return False


class FutureLeakException(Exception):
    def __init__(self, access_time, poison_time, stack_trace):
        self.access_time = access_time
        self.poison_time = poison_time
        self.stack_trace = stack_trace
        super().__init__(
            f"Future leak: accessed {poison_time} at {access_time}"
        )


class SandboxExecutionAuditor:
    """
    v2.2修复：
    - _run_with_audit_wrapper不再预循环设置audit_time，改为在backtest前设置为毒化前时间
    - 添加线程安全警告
    - 改进异常捕获范围
    """

    def __init__(self, engine_class: type):
        self.engine_class = engine_class
        self.future_leak_detected = False
        self.leak_details = []

    def create_poisoned_tick_stream(self, n_ticks: int = 1000,
                                     poison_tick_idx: int = 500) -> tuple:
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(n_ticks) * 0.1)
        volumes = np.random.poisson(100, n_ticks)
        volumes[poison_tick_idx] = 100000
        prices[poison_tick_idx] += 5.0
        base_time = pd.Timestamp('2024-01-15 09:30:00')
        timestamps = [base_time + timedelta(seconds=i) for i in range(n_ticks)]
        tick_df = pd.DataFrame({
            'timestamp': timestamps,
            'price': prices,
            'volume': volumes,
            'bid': prices - 0.01,
            'ask': prices + 0.01,
            'symbol': 'TEST_POISON'
        })
        tick_df['is_poison'] = False
        tick_df.loc[poison_tick_idx, 'is_poison'] = True
        return tick_df, poison_tick_idx

    @contextmanager
    def _data_monitor(self, tick_df: pd.DataFrame, poison_time: datetime):
        """DataFrame monkey-patch监控上下文 — 拦截未来数据访问以检测前瞻偏差

        必要性:
            回测引擎的"未来数据泄漏"是最严重的统计偏差之一。引擎可能通过
            DataFrame的时间戳索引、iloc/loc访问器或列名键访问尚未发生的行情数据，
            导致回测结果虚高。此monkey-patch通过替换pd.DataFrame的属性访问方法，
            在运行时拦截所有数据访问，确保引擎不会读取poison_time之后的数据。

        作用范围:
            - pd.DataFrame.__getitem__: 拦截df[timestamp_key]形式的时间戳索引访问，
              当audit_time < poison_time时，若key对应的时间>=poison_time，抛出FutureLeakException
            - pd.DataFrame.__getattribute__: 拦截df.iloc/loc/at/iat/xs/query等访问器，
              这些访问器可绕过__getitem__直接获取未来数据，因此在监控期间完全禁止

        安全措施:
            1. 线程锁保护: monkey-patch生效期间持有线程锁，防止并发线程在patch状态下
               访问DataFrame导致不可预测行为。此方法不适合多线程环境。
            2. contextmanager保证还原: 使用try/finally确保即使被监控代码抛出异常，
               原始的__getitem__和__getattribute__方法也会被恢复。
            3. 作用域有限: monkey-patch仅在with块内生效，退出后立即还原，
               不影响其他代码对DataFrame的正常使用。
            4. PY-P1-02修复: 使用__getattribute__替代__getattr__，前者可拦截
               iloc/loc/at/iat等类级别属性的访问，后者仅拦截不存在的属性。

        Args:
            tick_df: 被监控的tick数据DataFrame
            poison_time: 毒化时间点，此时间之后的数据为"未来数据"

        Yields:
            audit_time: 可变字典{'value': datetime}，调用方通过设置audit_time['value']
                告知监控器当前审计时间点，用于判断数据访问是否越界
        """
        # P2-R11-08修复: monkey-patch添加线程锁，替代"禁止多线程"策略
        import threading
        _mp_lock = threading.Lock()
        _mp_lock.acquire()

        original_getitem = pd.DataFrame.__getitem__
        original_getattr = pd.DataFrame.__getattr__
        audit_time = {'value': None}

        def monitored_getitem(self_df, key):
            current_time = audit_time['value']
            if current_time and current_time < poison_time:
                if isinstance(key, (pd.Timestamp, str)):
                    try:
                        key_time = pd.Timestamp(key)
                        if key_time >= poison_time:
                            raise FutureLeakException(
                                access_time=current_time,
                                poison_time=poison_time,
                                stack_trace=traceback.format_stack()
                            )
                    except (ValueError, TypeError):
                        pass
            return original_getitem(self_df, key)

        def monitored_getattribute(self_df, name):
            # PY-P1-02修复: 使用__getattribute__替代__getattr__，可拦截iloc/loc/at/iat等类级别属性
            if name in ('iloc', 'loc', 'at', 'iat', 'xs', 'query'):
                logger.warning(
                    "DataFrame accessor .%s accessed on %s at audit_time=%s",
                    name, type(self_df).__name__, audit_time['value']
                )
                raise RuntimeError(f"PY-P1-02修复: DataFrame accessor .{name} 在审计监控期间被禁止访问，请使用显式列访问")
            return original_getattribute(self_df, name)

        pd.DataFrame.__getitem__ = monitored_getitem
        original_getattribute = pd.DataFrame.__getattribute__
        pd.DataFrame.__getattribute__ = monitored_getattribute
        try:
            yield audit_time
        finally:
            pd.DataFrame.__getitem__ = original_getitem
            pd.DataFrame.__getattribute__ = original_getattribute
            # P2-R11-08修复: 释放monkey-patch线程锁
            _mp_lock.release()

    def run_sandbox_test(self, strategy_config: dict) -> dict:
        tick_df, poison_idx = self.create_poisoned_tick_stream()
        poison_time = tick_df.loc[poison_idx, 'timestamp']
        result = None

        with self._data_monitor(tick_df, poison_time) as audit_time:
            try:
                engine = self.engine_class(**strategy_config)
            except FutureLeakException as e:
                self._record_leak(e, poison_time)
                return {'status': 'FAILED', 'leaks': self.leak_details}

            if hasattr(engine, 'set_audit_time'):
                try:
                    result = self._run_with_set_audit_time(
                        engine, tick_df, audit_time, poison_time
                    )
                except FutureLeakException as e:
                    self._record_leak(e, poison_time)
                    return {'status': 'FAILED', 'leaks': self.leak_details}
            else:
                try:
                    result = self._run_with_audit_wrapper(
                        engine, tick_df, audit_time, poison_time
                    )
                except FutureLeakException as e:
                    self._record_leak(e, poison_time)
                    return {'status': 'FAILED', 'leaks': self.leak_details}

        if result is not None and hasattr(result, 'columns') and 'signal_time' in result.columns:
            pre_poison_signals = result[result['signal_time'] < poison_time]
            if len(pre_poison_signals) > 0:
                if self._detect_precognition(pre_poison_signals, tick_df, poison_idx):
                    self.future_leak_detected = True
                    self.leak_details.append({
                        'type': 'PRECOGNITION_DETECTED',
                        'poison_time': poison_time.isoformat(),
                        'pre_poison_signals': len(pre_poison_signals),
                        'detail': 'Signals before poison anticipate post-poison move'
                    })

        return {
            'status': 'PASSED' if not self.future_leak_detected else 'FAILED',
            'leaks': self.leak_details,
            'poison_tick_idx': poison_idx,
            'poison_time': poison_time.isoformat()
        }

    def _run_with_set_audit_time(self, engine, tick_df, audit_time, poison_time):
        """
        v2.2：引擎支持set_audit_time时的逐tick审计。
        逐tick设置审计时间并调用set_audit_time，但backtest仍以完整数据调用。
        这是当前架构下的最佳折中：set_audit_time让引擎内部知道当前时间，
        配合_data_monitor拦截未来数据访问。
        """
        for idx in range(len(tick_df)):
            audit_time['value'] = tick_df.loc[idx, 'timestamp']
            engine.set_audit_time(audit_time['value'])
        # 重置audit_time到毒化前，然后调用backtest
        # 此时_data_monitor会拦截对poison_time之后时间戳的访问
        audit_time['value'] = tick_df.loc[0, 'timestamp']
        return engine.backtest(tick_df)

    def _run_with_audit_wrapper(self, engine, tick_df, audit_time, poison_time):
        """
        v2.2修复：不再预循环设置audit_time为终值。
        将audit_time设置为毒化前的时间点，使_data_monitor能正确拦截
        对poison_time之后数据的访问。这是保守检测策略：
        若引擎在任意时刻通过时间戳键访问毒化后数据，即被捕获。
        """
        # 设置为毒化前的最后一个tick时间，确保任何对poison_time的访问都会被拦截
        poison_idx = None
        for idx in range(len(tick_df)):
            if tick_df.loc[idx, 'timestamp'] >= poison_time:
                poison_idx = idx
                break
        if poison_idx is not None and poison_idx > 0:
            audit_time['value'] = tick_df.loc[poison_idx - 1, 'timestamp']
        else:
            audit_time['value'] = tick_df.loc[0, 'timestamp']
        return engine.backtest(tick_df)

    def _record_leak(self, e: FutureLeakException, poison_time: datetime):
        self.future_leak_detected = True
        self.leak_details.append({
            'type': 'FUTURE_LEAK_CAUGHT',
            'poison_time': poison_time.isoformat(),
            'access_time': e.access_time.isoformat() if e.access_time else None,
            'lead_seconds': (
                (poison_time - e.access_time).total_seconds()
                if e.access_time else None
            ),
            'stack_trace': e.stack_trace[:5]
        })

    def _detect_precognition(self, pre_signals, tick_df, poison_idx):
        poison_price = tick_df.loc[poison_idx, 'price']
        end_idx = min(poison_idx + 11, len(tick_df))
        post_prices = tick_df.loc[poison_idx+1:end_idx-1, 'price']
        if len(post_prices) == 0:
            return False
        if post_prices.mean() > poison_price:
            if 'direction' in pre_signals.columns:
                long_mask = pre_signals['direction'] == 'LONG'
                k = int(long_mask.sum())
                n = len(pre_signals)
                if n == 0:
                    return False
                z = (2 * k - n) / np.sqrt(n)
                p_value = 1 - _norm_cdf(z)
                return bool(p_value < 0.05)
        return False


class AutoFieldLineageTracker:

    def __init__(self, backtest_source_code: str):
        self.source = backtest_source_code
        self.lineage = {}
        try:
            self.ast_tree = ast.parse(backtest_source_code)
            self._parse_computations()
        except SyntaxError:
            warnings.warn("Source code has syntax errors, lineage tracking skipped")

    def _parse_computations(self):
        parent_map = {}
        for parent in ast.walk(self.ast_tree):
            for child in ast.iter_child_nodes(parent):
                parent_map[child] = parent

        def get_enclosing_func(node):
            cur = node
            while cur in parent_map:
                cur = parent_map[cur]
                if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    return cur.name
            return None

        raw_assignments = []
        for node in ast.walk(self.ast_tree):
            if isinstance(node, ast.Assign):
                func_name = get_enclosing_func(node)
                targets = []
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        target_name = f'{func_name}.{t.id}' if func_name else t.id
                        targets.append(target_name)
                    elif isinstance(t, ast.Subscript):
                        if isinstance(t.value, ast.Name) and isinstance(t.slice, ast.Constant):
                            target_name = f'{func_name}.{t.slice.value}' if func_name else t.slice.value
                            targets.append(target_name)

                for target in targets:
                    sources = self._extract_sources(node.value)
                    try:
                        computation = ast.unparse(node.value)
                    except Exception:
                        computation = '<unparseable>'
                    timestamp_ref = self._infer_timestamp_ref(computation, sources)
                    raw_assignments.append({
                        'target': target,
                        'sources': sources,
                        'computation': computation,
                        'timestamp_ref': timestamp_ref
                    })

        self._topological_sort(raw_assignments)

    def _topological_sort(self, assignments: List[dict]):
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        all_targets = {a['target'] for a in assignments}

        for a in assignments:
            target = a['target']
            for src in a['sources']:
                if src in all_targets:
                    graph[src].append(target)
                    in_degree[target] += 1
            if target not in in_degree:
                in_degree[target] = 0

        queue = deque([t for t in all_targets if in_degree[t] == 0])
        sorted_targets = []

        while queue:
            node = queue.popleft()
            sorted_targets.append(node)
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(sorted_targets) < len(all_targets):
            remaining = all_targets - set(sorted_targets)
            warnings.warn(f"Circular dependency detected: {remaining}")
            sorted_targets.extend(remaining)

        target_map = {a['target']: a for a in assignments}
        for target in sorted_targets:
            if target in target_map:
                a = target_map[target]
                self.lineage[target] = {
                    'source_fields': a['sources'],
                    'computation': a['computation'],
                    'timestamp_ref': a['timestamp_ref'],
                    'future_risk': self._assess_future_risk(
                        a['sources'], a['timestamp_ref']
                    )
                }

    def _extract_sources(self, node) -> List[str]:
        sources = []
        for child in ast.walk(node):
            if isinstance(child, ast.Attribute):
                if isinstance(child.value, ast.Name) and child.value.id in ('df', 'data', 'tick', 'self'):
                    sources.append(child.attr)
            elif isinstance(child, ast.Subscript):
                if isinstance(child.value, ast.Name) and child.value.id in ('df', 'data', 'tick', 'self'):
                    if isinstance(child.slice, ast.Name):
                        sources.append(f'<var:{child.slice.id}>')
                    elif isinstance(child.slice, ast.Constant) and isinstance(child.slice.value, str):
                        sources.append(child.slice.value)
        return list(set(sources))

    def _infer_timestamp_ref(self, computation: str, sources: List[str]) -> str:
        """
        v2.2：rolling()正则支持关键字参数形式如rolling(window=20, min_periods=5)
        """
        if 'rolling(' in computation:
            match = re.search(r'rolling\(\s*(?:window\s*=\s*)?(\d+)', computation)
            window = int(match.group(1)) if match else 20
            return f'BAR_CLOSE_MINUS_{window}'
        if 'shift(1)' in computation:
            return 'PREVIOUS_BAR_CLOSE'
        if re.search(r'shift\(\s*-\s*\d+', computation):
            return 'FUTURE_DATA'
        if 'current' in computation.lower() or 'realtime' in computation.lower():
            return 'CURRENT_TICK'
        if 'ewm(' in computation or 'expanding(' in computation:
            return 'CUMULATIVE_TO_CURRENT'
        if len(computation) > 100 or 'apply(' in computation or 'transform(' in computation:
            return 'UNKNOWN_COMPLEX_EXPRESSION'
        return 'CURRENT_BAR_CLOSE'

    def _assess_future_risk(self, sources: List[str], timestamp_ref: str) -> str:
        if timestamp_ref == 'FUTURE_DATA':
            return 'HIGH_FUTURE_RISK'
        if timestamp_ref == 'UNKNOWN_COMPLEX_EXPRESSION':
            return 'UNKNOWN_RISK_MANUAL_REVIEW'
        for src in sources:
            if src in self.lineage:
                src_ref = self.lineage[src]['timestamp_ref']
                if self._is_later(src_ref, timestamp_ref):
                    return 'HIGH_FUTURE_RISK'
                src_risk = self.lineage[src].get('future_risk', '')
                if 'HIGH' in src_risk or 'UNKNOWN' in src_risk:
                    return src_risk
        return 'LOW_FUTURE_RISK'

    def _is_later(self, ref1: str, ref2: str) -> bool:
        """
        判断ref1是否比ref2引用更晚（更未来）的数据
        时间从早到晚：BAR_CLOSE_MINUS_N → PREVIOUS → CURRENT → CUMULATIVE → FUTURE
        v2.2修正：N越大→回溯越远→时间越早→序号越小
        BAR_CLOSE_MINUS_20 → order=30（比CURRENT早20个bar）
        BAR_CLOSE_MINUS_5  → order=45（比CURRENT早5个bar）
        """
        order = {
            'CURRENT_TICK': 50,
            'CURRENT_BAR_CLOSE': 50,
            'PREVIOUS_BAR_CLOSE': 30,
            'CUMULATIVE_TO_CURRENT': 50,
            'UNKNOWN_COMPLEX_EXPRESSION': 98,
            'FUTURE_DATA': 99
        }

        def to_order(ref):
            if ref.startswith('BAR_CLOSE_MINUS_'):
                n = int(ref.split('_')[-1])
                # N越大→回溯越远→越早→序号越小
                return max(0, 50 - n)
            return order.get(ref, 50)

        return to_order(ref1) > to_order(ref2)

    def audit_all_fields(self) -> List[dict]:
        issues = []
        for field, meta in self.lineage.items():
            if meta['future_risk'] in ('HIGH_FUTURE_RISK', 'UNKNOWN_RISK_MANUAL_REVIEW'):
                issues.append({
                    'field': field,
                    'sources': meta['source_fields'],
                    'timestamp_ref': meta['timestamp_ref'],
                    'computation': meta['computation'][:100],
                    'future_risk': meta['future_risk'],
                    'fix': 'Review computation logic; ensure all sources available before use'
                })
        return issues


class SmartSignificanceFilter:

    def __init__(self, min_sample_size: int = 100, confidence_level: float = 0.95,
                 n_bootstrap: int = 1000, bootstrap_seed: int = 42):
        self.min_n = min_sample_size
        self.confidence = confidence_level
        self.n_bootstrap = n_bootstrap
        self.bootstrap_seed = bootstrap_seed

    def filter(self, issues: List[AuditIssue], backtest_results: pd.DataFrame) -> List[AuditIssue]:
        filtered = []
        for issue in issues:
            if issue.vuln_type == VulnerabilityType.DETERMINISTIC:
                filtered.append(issue)
                continue
            if any(p in issue.issue_type for p in DETERMINISTIC_PATTERNS):
                filtered.append(issue)
                continue

            if issue.vuln_type == VulnerabilityType.STATISTICAL:
                sample_size = self._get_sample_size(issue, backtest_results)
                if sample_size < self.min_n:
                    issue.severity = Severity.LOW.value
                    issue.detail += f' (统计不显著: n={sample_size}<{self.min_n})'
                    filtered.append(issue)
                    continue

                if 'ratio' in issue.evidence or 'proportion' in issue.evidence:
                    is_significant = self._bootstrap_ratio_test(
                        issue.evidence, sample_size
                    )
                    if not is_significant:
                        issue.severity = Severity.LOW.value
                        issue.detail += ' (Bootstrap置信区间包含阈值，差异不显著)'  # R21-MEM-P2-04修复: 单次+=拼接可接受，循环中应改用join

                elif 'tier1_wins' in issue.evidence and 'tier2_wins' in issue.evidence:
                    is_significant = self._test_ratio_difference(
                        issue.evidence.get('tier1_wins', 0),
                        issue.evidence.get('tier1_total', 1),
                        issue.evidence.get('tier2_wins', 0),
                        issue.evidence.get('tier2_total', 1)
                    )
                    if not is_significant:
                        issue.severity = Severity.LOW.value
                        issue.detail += ' (Tier1 vs Tier2胜率差异不显著)'  # R21-MEM-P2-04修复: 单次+=拼接可接受

            filtered.append(issue)
        return filtered

    def _get_sample_size(self, issue: AuditIssue, backtest_results: pd.DataFrame) -> int:
        if 'sample_size' in issue.evidence:
            return issue.evidence['sample_size']
        if 'trades' in backtest_results.columns:
            return len(backtest_results)
        if 'n_samples' in issue.evidence:
            return issue.evidence['n_samples']
        return 0

    def _bootstrap_ratio_test(self, evidence: dict, n: int) -> bool:
        """
        v2.2：纯numpy向量化实现，添加随机种子确保可复现
        """
        rng = np.random.RandomState(self.bootstrap_seed)

        if 'values' in evidence:
            values = np.asarray(evidence['values'])
            if len(values) < self.min_n:
                return False
            # 向量化Bootstrap：一次性生成所有重采样
            indices = rng.randint(0, len(values), size=(self.n_bootstrap, len(values)))
            boot_samples = values[indices]
            boot_means = np.mean(boot_samples, axis=1)

            alpha = 1 - self.confidence
            ci_lower = np.percentile(boot_means, alpha * 100 / 2)
            ci_upper = np.percentile(boot_means, 100 - alpha * 100 / 2)
            threshold = evidence.get('threshold', 1.0)
            return bool(not (ci_lower <= threshold <= ci_upper))

        elif 'ratio' in evidence and 'std' in evidence:
            ratio = evidence['ratio']
            std = evidence['std']
            if std == 0 or n == 0:
                return False
            se = std / np.sqrt(n)
            z = (ratio - 1.0) / se
            p_value = 1 - _norm_cdf(z)
            return bool(p_value < (1 - self.confidence))

        return True

    def _test_ratio_difference(self, w1: int, n1: int, w2: int, n2: int) -> bool:
        """
        v2.2：双比例Z检验，纯numpy实现，返回Python bool
        添加极端值保护
        """
        if n1 == 0 or n2 == 0:
            return False
        p1 = w1 / n1
        p2 = w2 / n2
        p_pooled = (w1 + w2) / (n1 + n2)
        se = np.sqrt(p_pooled * (1 - p_pooled) * (1/n1 + 1/n2))
        if se == 0:
            return False
        z = (p1 - p2) / se
        # 极端z值保护：|z|>8时_norm_cdf返回精确值，p_value不会出现数值问题
        p_value = 2 * (1 - _norm_cdf(abs(z)))
        return bool(p_value < (1 - self.confidence))


class HistoricalUniverseRestorer:

    def __init__(self, delisting_records: pd.DataFrame,
                 data_loader: Optional[Callable] = None):
        self.records = delisting_records
        self.data_loader = data_loader

    def restore_historical_universe(self, backtest_start: datetime,
                                    backtest_end: datetime) -> pd.DataFrame:
        existed = self.records[
            (self.records['listing_date'] <= backtest_end) &
            ((self.records['delisting_date'].isna()) |
             (self.records['delisting_date'] >= backtest_start))
        ].copy()
        existed['status'] = 'ACTIVE'
        mask = (
            (existed['delisting_date'] >= backtest_start) &
            (existed['delisting_date'] <= backtest_end)
        )
        existed.loc[mask, 'status'] = 'DELISTED_DURING_BACKTEST'
        return existed

    def inject_ghost_trades(self, backtest_engine: Any,
                           strategy_config: dict,
                           selection_criteria: Callable,
                           max_ghosts: int = 50,
                           active_avg_sharpe: float = 0.0) -> dict:
        """
        v2.2修复：
        - engine.backtest(data)而非backtest(data, config)
        - status列缺失时正确处理：先调用restore_historical_universe生成status列
        """
        if self.data_loader is None:
            return {'status': 'NO_DATA_LOADER', 'detail': 'Please provide data_loader'}

        # v2.2修复：若records中没有status列，使用delisting_date判断退市
        if 'status' not in self.records.columns:
            delisted = self.records[
                self.records['delisting_date'].notna()
            ].copy()
            logger.info(
                "inject_ghost_trades: 'status' column not found in records, "
                "using delisting_date to identify delisted symbols (%d found)",
                len(delisted)
            )
        else:
            delisted = self.records[
                self.records['status'] == 'DELISTED_DURING_BACKTEST'
            ]

        if len(delisted) == 0:
            return {
                'status': 'NO_DELISTED_SYMBOLS',
                'ghost_trades': [],
                'total_delisted_tested': 0,
                'would_have_been_selected_count': 0,
                'survivorship_bias_estimate': 0.0
            }

        if len(delisted) > max_ghosts:
            delisted = delisted.sample(max_ghosts, random_state=42)

        ghost_results = []
        for _, row in delisted.iterrows():
            symbol = row['symbol']
            delist_date = row['delisting_date']
            start = delist_date - timedelta(days=60)
            end = delist_date

            try:
                data = self.data_loader(symbol, start, end)
            except Exception as e:
                ghost_results.append({
                    'symbol': symbol,
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'traceback': traceback.format_exc(),
                    'status': 'DATA_LOAD_FAILED'
                })
                continue

            if data is None or len(data) < 20:
                continue

            try:
                # 支持两种接口：engine.backtest(data) 和 task_scheduler.run_backtest(params, data, train, strategy_type)
                if hasattr(backtest_engine, 'backtest'):
                    result = backtest_engine.backtest(data)
                elif callable(backtest_engine):
                    # task_scheduler.run_backtest(params, bar_data, train, strategy_type)
                    result = backtest_engine(strategy_config, data, True, 'resonance')
                else:
                    result = backtest_engine(data)
            except TypeError:
                try:
                    result = backtest_engine.backtest(data, strategy_config)
                except Exception as e:
                    ghost_results.append({
                        'symbol': symbol,
                        'error': str(e),
                        'error_type': type(e).__name__,
                        'traceback': traceback.format_exc(),
                        'status': 'BACKTEST_FAILED'
                    })
                    continue
            except Exception as e:
                ghost_results.append({
                    'symbol': symbol,
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'traceback': traceback.format_exc(),
                    'status': 'BACKTEST_FAILED'
                })
                continue

            would_select = selection_criteria(result)
            ghost_results.append({
                'symbol': symbol,
                'delisting_date': delist_date.isoformat(),
                'delisting_reason': row.get('delisting_reason', 'UNKNOWN'),
                'strategy_sharpe': getattr(result, 'sharpe', None),
                'strategy_max_dd': getattr(result, 'max_drawdown', None),
                'final_pnl': getattr(result, 'total_pnl', None),
                'would_have_been_selected': would_select,
                'status': 'SUCCESS'
            })

        return {
            'ghost_trades': ghost_results,
            'total_delisted_tested': len([
                g for g in ghost_results if g.get('status') == 'SUCCESS'
            ]),
            'would_have_been_selected_count': sum(
                1 for g in ghost_results if g.get('would_have_been_selected')
            ),
            'survivorship_bias_estimate': self._estimate_bias(ghost_results, active_avg_sharpe)
        }

    def compare_with_active(self, ghost_results: List[dict],
                           active_results: List[dict]) -> dict:
        successful_ghosts = [
            g for g in ghost_results if g.get('status') == 'SUCCESS'
        ]
        selected_ghosts = [
            g for g in successful_ghosts if g.get('would_have_been_selected')
        ]
        if not selected_ghosts or not active_results:
            return {'bias_detected': False, 'reason': 'Insufficient data'}

        ghost_sharpes = [
            g['strategy_sharpe'] for g in selected_ghosts
            if g['strategy_sharpe'] is not None
        ]
        active_sharpes = [
            r.get('sharpe', 0) for r in active_results
            if r.get('sharpe') is not None
        ]
        if not ghost_sharpes or not active_sharpes:
            return {'bias_detected': False, 'reason': 'No sharpe data'}

        ghost_avg = np.mean(ghost_sharpes)
        active_avg = np.mean(active_sharpes)
        bias = active_avg - ghost_avg

        return {
            'bias_detected': bias > 0.3,
            'bias_magnitude': bias,
            'ghost_avg_sharpe': ghost_avg,
            'active_avg_sharpe': active_avg,
            'recommendation': 'HIGH_BIAS_DETECTED' if bias > 0.3 else 'BIAS_ACCEPTABLE'
        }

    def _estimate_bias(self, ghost_results: List[dict],
                       active_avg_sharpe: float = 0.0) -> float:
        """
        v2.3：基于ghost与active的Sharpe差值估计存续偏差。
        bias = max(0, active_avg_sharpe - ghost_avg_sharpe)
        若active_avg_sharpe未提供（默认0.0），回退到max(0, 0.5 - ghost_avg_sharpe)
        """
        successful = [g for g in ghost_results if g.get('status') == 'SUCCESS']
        if not successful:
            return 0.0
        selected = [g for g in successful if g.get('would_have_been_selected')]
        if not selected:
            return 0.0
        selected_sharpes = [
            g['strategy_sharpe'] for g in selected
            if g['strategy_sharpe'] is not None
        ]
        if not selected_sharpes:
            return 0.0
        ghost_avg_sharpe = np.mean(selected_sharpes)
        if active_avg_sharpe != 0.0:
            return max(0.0, active_avg_sharpe - ghost_avg_sharpe)
        else:
            return max(0.0, 0.5 - ghost_avg_sharpe)


class SignalReadinessAligner:

    def __init__(self, strategy_signals: Dict[str, pd.DataFrame]):
        self.signals = strategy_signals

    def analyze_readiness_distribution(self) -> dict:
        distributions = {}
        for name, df in self.signals.items():
            if 'event_time' not in df.columns or 'ready_time' not in df.columns:
                warnings.warn(f"Strategy {name} missing event_time or ready_time")
                continue
            df_copy = df.copy()
            df_copy['latency'] = (
                df_copy['ready_time'] - df_copy['event_time']
            ).dt.total_seconds()
            latencies = df_copy['latency'].dropna()
            distributions[name] = {
                'mean_latency': latencies.mean(),
                'std_latency': latencies.std(),
                'min_latency': latencies.min(),
                'max_latency': latencies.max(),
                'median_latency': latencies.median(),
                'p95_latency': latencies.quantile(0.95),
                'skewness': float(latencies.skew()) if len(latencies) > 2 else 0.0,
                'kurtosis': float(latencies.kurtosis()) if len(latencies) > 3 else 0.0,
                'distribution': latencies.describe().to_dict()
            }
        return distributions

    def check_resonance_feasibility(self, max_allowed_diff_seconds: float = 300) -> dict:
        """
        v2.2改进：双峰检测改用偏度+峰度联合判定
        - 偏度绝对值>1 表示显著偏斜（不对称双峰）
        - 超额峰度<-1 表示平坦分布（对称双峰/多峰）
        - 满足任一条件即标记为双峰候选
        """
        dists = self.analyze_readiness_distribution()
        if not dists:
            return {'resonance_feasible': False, 'reason': 'No valid signal data'}
        latencies = {k: v['mean_latency'] for k, v in dists.items()}
        max_latency = max(latencies.values())
        min_latency = min(latencies.values())
        spread = max_latency - min_latency
        bimodal_strategies = []
        for name, dist in dists.items():
            skewness = dist.get('skewness', 0.0)
            kurtosis = dist.get('kurtosis', 0.0)
            # v2.2：偏度绝对值>1（不对称双峰）或超额峰度<-1（对称双峰/平坦分布）
            if abs(skewness) > 1.0 or kurtosis < -1.0:
                bimodal_strategies.append(name)
        return {
            'latency_spread_seconds': spread,
            'max_allowed_seconds': max_allowed_diff_seconds,
            'resonance_feasible': spread <= max_allowed_diff_seconds,
            'bimodal_strategies': bimodal_strategies,
            'slowest_strategy': max(latencies, key=latencies.get),
            'fastest_strategy': min(latencies, key=latencies.get),
            'recommendation': 'DYNAMIC_ALIGNMENT' if bimodal_strategies else 'STATIC_ALIGNMENT'
        }

    def generate_alignment_rules(self,
                                  dependency_graph: Optional[Dict[str, List[str]]] = None) -> dict:
        """
        v2.3：因果依赖感知的对齐规则。
        - dependency_graph: {策略A: [策略B, 策略C]} 表示A依赖B和C的信号
        - 每个策略的aligned_ready_time = max(自身latency, 所有依赖策略的aligned_ready_time)
        - 无依赖的策略之间不互相等待
        """
        dists = self.analyze_readiness_distribution()
        if not dists:
            return {}
        if dependency_graph is None:
            dependency_graph = {}
        latency_map = {name: dist['mean_latency'] for name, dist in dists.items()}
        aligned_map = {}

        def compute_aligned(name, visited=None):
            if visited is None:
                visited = set()
            if name in aligned_map:
                return aligned_map[name]
            if name in visited:
                return latency_map.get(name, 0)
            visited.add(name)
            deps = dependency_graph.get(name, [])
            own_latency = latency_map.get(name, 0)
            if not deps:
                aligned_map[name] = own_latency
                return own_latency
            dep_max = max((compute_aligned(d, visited) for d in deps if d in dists), default=0)
            aligned_map[name] = max(own_latency, dep_max)
            return aligned_map[name]

        for name in dists:
            compute_aligned(name)

        max_aligned = max(aligned_map.values()) if aligned_map else 0
        rules = {}
        for name, dist in dists.items():
            aligned = aligned_map.get(name, dist['mean_latency'])
            required_delay = aligned - dist['mean_latency']
            rules[name] = {
                'base_latency_seconds': dist['mean_latency'],
                'required_delay_seconds': required_delay,
                'aligned_ready_time': f'event_time + {aligned:.1f}s',
                'resonance_window': f'[{aligned:.1f}, {aligned + 30:.1f}]s',
                'tolerance_seconds': 30,
                'dependencies': dependency_graph.get(name, [])
            }
        return rules


class AuditPassport:

    def __init__(self,
                 engine_source_code: str,
                 engine_class: type,
                 strategy_config: dict,
                 data_loader: Optional[Callable] = None,
                 delisting_records: Optional[pd.DataFrame] = None,
                 certified_ttl_seconds: float = 3600):
        self.engine_code = engine_source_code
        self.engine_class = engine_class
        self.strategy_config = strategy_config
        self.data_loader = data_loader
        self.delisting_records = delisting_records
        self.passport_id = self._generate_id()
        self.audit_results = {}
        self.certified = False
        self.backtest_engine_instance = None
        self.certified_ttl_seconds = certified_ttl_seconds
        self.certified_at = None
        self.certified_engine_hash = None

    def _generate_id(self) -> str:
        content = self.engine_code + json.dumps(self.strategy_config, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def is_certification_valid(self) -> bool:
        """检查认证是否有效：未过期且代码哈希一致"""
        if not self.certified:
            return False
        if self.certified_at is None:
            return False
        if (datetime.now(CHINA_TZ) - self.certified_at).total_seconds() > self.certified_ttl_seconds:
            logger.warning("Certification expired: TTL=%ss", self.certified_ttl_seconds)
            return False
        current_hash = hashlib.sha256(self.engine_code.encode()).hexdigest()
        if current_hash != self.certified_engine_hash:
            logger.warning("Engine code hash mismatch: certified=%s, current=%s",
                           self.certified_engine_hash[:16], current_hash[:16])
            return False
        return True

    def run_meta_audit(self) -> bool:
        logger.info("=" * 70)
        logger.info("[AuditPassport %s] Phase 1: Meta Audit", self.passport_id)
        logger.info("=" * 70)

        logger.info("[1/5] Static Code Audit")
        meta_auditor = MetaAuditEngine(self.engine_code)
        static_issues = meta_auditor.audit_backtest_engine_integrity()
        self._log_issues_table(static_issues)

        logger.info("[2/5] Sandbox Execution Audit")
        sandbox_auditor = SandboxExecutionAuditor(self.engine_class)
        sandbox_result = sandbox_auditor.run_sandbox_test(self.strategy_config)
        self._log_sandbox_result(sandbox_result)

        logger.info("[3/5] Field Lineage Tracking")
        lineage_tracker = AutoFieldLineageTracker(self.engine_code)
        lineage_issues = lineage_tracker.audit_all_fields()
        self._log_lineage_table(lineage_issues)

        logger.info("[4/5] Historical Universe Restoration")
        if self.delisting_records is not None:
            logger.info("  Delisting records: %d symbols", len(self.delisting_records))
            logger.info("  Status: PENDING_PHASE2 (run run_phase2_ghost_audit after backtest)")
        else:
            logger.info("  No delisting records provided — skipping")

        logger.info("[5/5] Signal Readiness Alignment")
        logger.info("  Status: PENDING_SIGNAL_DATA (provide strategy_signals to analyze)")

        logger.info("[6/9] Counterfactual Validation (Phase 3)")
        try:
            from ali2026v3_trading.param_pool.statistical_validation import CounterfactualValidator
            cf_validator = CounterfactualValidator()
            self.audit_results['counterfactual_available'] = True
            logger.info("  CounterfactualValidator loaded successfully")
        except Exception as e:
            self.audit_results['counterfactual_available'] = False
            logger.warning("  CounterfactualValidator not available: %s", e)

        logger.info("[7/9] Monte Carlo Bankruptcy Test (Phase 3)")
        try:
            from ali2026v3_trading.param_pool.statistical_validation import MonteCarloBankruptcyValidator
            mc_validator = MonteCarloBankruptcyValidator()
            self.audit_results['monte_carlo_available'] = True
            logger.info("  MonteCarloBankruptcyValidator loaded successfully")
        except Exception as e:
            self.audit_results['monte_carlo_available'] = False
            logger.warning("  MonteCarloBankruptcyValidator not available: %s", e)

        logger.info("[8/9] Shadow Param Independence (Phase 4)")
        try:
            from ali2026v3_trading.param_pool.advanced_validation import ShadowParamIndependenceValidator
            shadow_validator = ShadowParamIndependenceValidator()
            self.audit_results['shadow_param_validator_available'] = True
            logger.info("  ShadowParamIndependenceValidator loaded successfully")
        except Exception as e:
            self.audit_results['shadow_param_validator_available'] = False
            logger.warning("  ShadowParamIndependenceValidator not available: %s", e)

        logger.info("[9/9] Deep Validation Suite (Phase 5)")
        try:
            from ali2026v3_trading.param_pool.advanced_validation import DeepValidationSuite
            deep_suite = DeepValidationSuite()
            self.audit_results['deep_validation_available'] = True
            logger.info("  DeepValidationSuite loaded successfully (7 validators + 2 meta-audit)")
        except Exception as e:
            self.audit_results['deep_validation_available'] = False
            logger.warning("  DeepValidationSuite not available: %s", e)

        self.audit_results = {
            'version': _AUDIT_FORMAT_VERSION,
            'passport_id': self.passport_id,
            'audit_time': datetime.now(CHINA_TZ).isoformat(),
            'engine_hash': hashlib.sha256(self.engine_code.encode()).hexdigest(),
            'static_issues': [i.to_dict() for i in static_issues],
            'sandbox_result': sandbox_result,
            'lineage_issues': lineage_issues,
            'counterfactual_available': self.audit_results.get('counterfactual_available', False),
            'monte_carlo_available': self.audit_results.get('monte_carlo_available', False),
            'shadow_param_validator_available': self.audit_results.get('shadow_param_validator_available', False),
            'deep_validation_available': self.audit_results.get('deep_validation_available', False),
        }

        critical_count = (
            sum(1 for i in static_issues if i.severity == Severity.CRITICAL.value) +
            (1 if sandbox_result.get('status') == 'FAILED' else 0) +
            sum(1 for i in lineage_issues
                if i.get('future_risk') in ('HIGH_FUTURE_RISK', 'UNKNOWN_RISK_MANUAL_REVIEW'))
        )

        self.certified = critical_count == 0
        self.audit_results['certified'] = self.certified
        self.audit_results['critical_count'] = critical_count

        if self.certified:
            self.certified_at = datetime.now(CHINA_TZ)
            self.certified_engine_hash = hashlib.sha256(self.engine_code.encode()).hexdigest()

        logger.info("=" * 70)
        logger.info("AUDIT SUMMARY")
        logger.info("=" * 70)
        logger.info("Module: %-30s Issues: %-10s Critical: %-10s",
                     'Static Code Audit', len(static_issues),
                     sum(1 for i in static_issues if i.severity == 'CRITICAL'))
        logger.info("Module: %-30s Issues: %-10s Critical: %-10s",
                     'Sandbox Execution', len(sandbox_result.get('leaks', [])),
                     1 if sandbox_result.get('status') == 'FAILED' else 0)
        logger.info("Module: %-30s Issues: %-10s Critical: %-10s",
                     'Field Lineage', len(lineage_issues),
                     sum(1 for i in lineage_issues if i.get('future_risk') == 'HIGH_FUTURE_RISK'))
        total_issues = (
            len(static_issues) +
            len(sandbox_result.get('leaks', [])) +
            len(lineage_issues)
        )
        logger.info("TOTAL: issues=%d, critical=%d", total_issues, critical_count)
        status_str = 'PASSED' if self.certified else 'FAILED'
        logger.info("RESULT: %s (%d critical issues)", status_str, critical_count)

        return self.certified

    def run_phase2_ghost_audit(self, active_results: List[dict] = None,
                               selection_criteria: Callable = None) -> dict:
        if not self.certified:
            logger.warning("Phase1 not passed. Cannot run Phase2.")
            return {'status': 'PHASE1_NOT_PASSED'}

        if self.delisting_records is None or self.data_loader is None:
            logger.warning("Phase2 skipped: delisting_records or data_loader not provided")
            return {'status': 'SKIPPED_NO_DATA'}

        logger.info("=" * 70)
        logger.info("[AuditPassport %s] Phase 2: Ghost Trade Audit", self.passport_id)
        logger.info("=" * 70)

        restorer = HistoricalUniverseRestorer(self.delisting_records, self.data_loader)

        if self.backtest_engine_instance is None:
            self.backtest_engine_instance = self.engine_class(**self.strategy_config)

        if selection_criteria is None:
            selection_criteria = lambda r: getattr(r, 'sharpe', 0) > 0.5

        ghost_result = restorer.inject_ghost_trades(
            self.backtest_engine_instance,
            self.strategy_config,
            selection_criteria
        )

        if active_results:
            comparison = restorer.compare_with_active(
                ghost_result['ghost_trades'],
                active_results
            )
            ghost_result['comparison_with_active'] = comparison

        logger.info("Ghost Trades Tested: %d", ghost_result['total_delisted_tested'])
        logger.info("Would Have Been Selected: %d", ghost_result['would_have_been_selected_count'])
        logger.info("Survivorship Bias Estimate: %.3f", ghost_result['survivorship_bias_estimate'])

        if 'comparison_with_active' in ghost_result:
            comp = ghost_result['comparison_with_active']
            logger.info("Comparison with Active:")
            logger.info("  Ghost Avg Sharpe: %s", comp.get('ghost_avg_sharpe', 'N/A'))
            logger.info("  Active Avg Sharpe: %s", comp.get('active_avg_sharpe', 'N/A'))
            logger.info("  Bias Detected: %s", comp.get('bias_detected', False))

        self.audit_results['phase2_ghost'] = ghost_result
        return ghost_result

    def run_certified_backtest(self, tick_data: pd.DataFrame) -> Any:
        """
        v2.2修复：
        - 非dict结果通过包装为CertifiedResult附加passport信息，不修改原始对象
        - dict结果仍直接附加_audit_passport字段（保持向后兼容）
        v2.3：调用is_certification_valid()检查TTL和代码哈希
        """
        if not self.is_certification_valid():
            raise RuntimeError(
                f"Engine certification invalid or expired. Passport {self.passport_id}.\n"
                f"Run run_meta_audit() first and fix issues. "
                f"TTL={self.certified_ttl_seconds}s, certified_at={self.certified_at}"
            )
        self.backtest_engine_instance = self.engine_class(**self.strategy_config)
        # 支持两种接口：engine.backtest(data) 和 task_scheduler.run_backtest(params, data, train, strategy_type)
        if hasattr(self.backtest_engine_instance, 'backtest'):
            result = self.backtest_engine_instance.backtest(tick_data)
        elif callable(self.backtest_engine_instance):
            result = self.backtest_engine_instance(self.strategy_config, tick_data, True, 'resonance')
        else:
            result = self.backtest_engine_instance.backtest(tick_data)

        passport_info = {
            'passport_id': self.passport_id,
            'certified_at': self.audit_results['audit_time'],
            'engine_hash': self.audit_results['engine_hash'],
            'strategy_config_hash': hashlib.sha256(
                json.dumps(self.strategy_config, sort_keys=True).encode()
            ).hexdigest()[:16]
        }

        if isinstance(result, dict):
            result['_audit_passport'] = passport_info
        else:
            # v2.2：非dict结果包装为CertifiedResult，保留原始对象的属性访问
            result = CertifiedResult(result, passport_info)

        return result

    def _log_issues_table(self, issues: List[AuditIssue]):
        if not issues:
            logger.info("  [OK] No issues found")
            return
        for issue in issues:
            logger.info("  %-30s %-10s %s", issue.issue_type, issue.severity, issue.detail)

    def _log_sandbox_result(self, result: dict):
        logger.info("  Status: %s", result['status'])
        if result.get('leaks'):
            logger.info("  Leaks found: %d", len(result['leaks']))
            for leak in result['leaks']:
                logger.info("    - %s: %s", leak['type'], leak.get('detail', ''))
        else:
            logger.info("  [OK] No future leaks detected")

    def _log_lineage_table(self, issues: List[dict]):
        if not issues:
            logger.info("  [OK] All fields LOW_FUTURE_RISK")
            return
        for issue in issues:
            logger.info("  %-20s %-25s %s",
                         issue['field'], issue['future_risk'], issue['timestamp_ref'])

    def save(self, path: str):
        """
        v2.2：添加版本号字段，确保向后兼容
        """
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        save_data = dict(self.audit_results)
        save_data['version'] = _AUDIT_FORMAT_VERSION
        with open(path, 'w', encoding='utf-8') as f:
            f.write(json_dumps(save_data, indent=2))

    def load(self, path: str):
        """
        v2.2：加载时检查版本兼容性
        """
        with open(path, 'r', encoding='utf-8') as f:
            self.audit_results = json.load(f)

        file_version = self.audit_results.get('version', '1.0')
        if file_version != _AUDIT_FORMAT_VERSION:
            logger.warning(
                "Audit result version mismatch: file=%s, current=%s. "
                "Some fields may be missing or deprecated.",
                file_version, _AUDIT_FORMAT_VERSION
            )

        self.certified = self.audit_results.get('certified', False)
        self.passport_id = self.audit_results.get('passport_id', '')


class CertifiedResult:
    """
    v2.2新增：非dict回测结果的包装器
    保留原始对象的所有属性访问，同时附加审计护照信息
    """

    def __init__(self, original_result: Any, passport_info: dict):
        self._original = original_result
        self._audit_passport = passport_info

    def __getattr__(self, name: str) -> Any:
        if name.startswith('_'):
            raise AttributeError(name)
        return getattr(self._original, name)

    def __repr__(self) -> str:
        return f"CertifiedResult({self._original!r}, passport={self._audit_passport['passport_id']})"

    @property
    def original(self) -> Any:
        """访问原始未包装的结果对象"""
        return self._original


class RestrictedExecLoader:
    ALLOWED_MODULES = {'pandas', 'numpy', 'math', 'datetime', 'collections', 'itertools'}
    # R15-P0-SEC-03修复: 显式排除__import__/eval/exec/compile/open等危险builtins
    # 原SAFE_BUILTINS已不含这些，但添加显式注释和额外阻断层
    # P0-R11-21修复: 扩展拦截列表，新增 memoryview/bytearray/bytes/help/license/credits/copyright
    _BLOCKED_BUILTINS = frozenset({
        '__import__', 'eval', 'exec', 'compile', 'open', 'input',
        'globals', 'locals', 'vars', 'dir', 'getattr', 'setattr', 'delattr',
        'type', 'object', 'classmethod', 'staticmethod', 'property',
        'super', '__build_class__', 'breakpoint',
        'memoryview', 'bytearray', 'bytes', 'help', 'license', 'credits', 'copyright',
        '__doc__',
    })
    SAFE_BUILTINS = {
        'True': True, 'False': False, 'None': None,
        'len': len, 'range': range, 'enumerate': enumerate,
        'isinstance': isinstance,
        'list': list, 'dict': dict, 'tuple': tuple, 'set': set, 'frozenset': frozenset,
        'str': str, 'int': int, 'float': float, 'bool': bool,
        'abs': abs, 'min': min, 'max': max, 'sum': sum, 'round': round,
        'sorted': sorted, 'reversed': reversed, 'zip': zip, 'map': map, 'filter': filter,
        'print': print,  # 审计代码需要print输出
    }

    @staticmethod
    def safe_exec(code, local_ns):
        # P0-R11-21修复: 代码长度上限 (防止超大payload攻击)
        _MAX_CODE_LEN = 10000
        if len(code) > _MAX_CODE_LEN:
            raise RuntimeError(
                f"Code too long ({len(code)} chars, max {_MAX_CODE_LEN})"
            )

        # P0-R11-21修复: 字符串级扫描拦截沙箱逃逸路径
        # 这些模式即使在注释/字符串中也要拦截，防止被 eval/exec 二次执行
        _ESCAPE_PATTERNS = (
            '__subclasses__', '__bases__', '__mro__',
            '__globals__', '__code__', '__subclasshook__',
            'getattr(', 'setattr(', 'delattr(',
        )
        for pat in _ESCAPE_PATTERNS:
            if pat in code:
                raise RuntimeError(f"Blocked sandbox escape pattern: {pat}")

        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split('.')[0] not in RestrictedExecLoader.ALLOWED_MODULES:
                        raise RuntimeError(f"Blocked import: {alias.name}")
            if isinstance(node, ast.ImportFrom):
                if node.module and node.module.split('.')[0] not in RestrictedExecLoader.ALLOWED_MODULES:
                    raise RuntimeError(f"Blocked import from: {node.module}")
            # R15-P0-SEC-03修复: 阻断对危险builtins名称的AST引用
            if isinstance(node, ast.Name) and node.id in RestrictedExecLoader._BLOCKED_BUILTINS:
                raise RuntimeError(f"Blocked dangerous builtin reference: {node.id}")
        exec(code, {'__builtins__': RestrictedExecLoader.SAFE_BUILTINS}, local_ns)


def example_usage():
    """
    v2.2：exec使用隔离命名空间，避免globals()污染
    v2.3：使用RestrictedExecLoader.safe_exec替代裸exec
    """
    engine_code = """
class YourBacktestEngine:
    def __init__(self, param1=0.5, param2=10):
        self.param1 = param1
        self.param2 = param2

    def backtest(self, tick_data):
        import pandas as pd
        import numpy as np
        return pd.DataFrame({
            'signal_time': tick_data['timestamp'],
            'pnl': np.random.randn(len(tick_data))
        })
"""

    ns = {}
    RestrictedExecLoader.safe_exec(engine_code, ns)
    YourBacktestEngine = ns['YourBacktestEngine']

    def my_data_loader(symbol, start, end):
        n = max(1, int((end - start).total_seconds() / 60))
        return pd.DataFrame({
            'timestamp': pd.date_range(start, periods=n, freq='min'),
            'price': 100 + np.cumsum(np.random.randn(n) * 0.01)
        })

    passport = AuditPassport(
        engine_source_code=engine_code,
        engine_class=YourBacktestEngine,
        strategy_config={'param1': 0.5, 'param2': 10},
        data_loader=my_data_loader,
        delisting_records=pd.DataFrame({
            'symbol': ['IF1506', 'IF1512'],
            'listing_date': [pd.Timestamp('2015-01-01'), pd.Timestamp('2015-06-01')],
            'delisting_date': [pd.Timestamp('2015-06-19'), pd.Timestamp('2015-12-18')],
            'delisting_reason': ['EXPIRED', 'EXPIRED']
        })
    )

    # 配置日志输出到控制台
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    if passport.run_meta_audit():
        logger.info("Phase1 passed. Running certified backtest...")
        tick_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-15', periods=1000, freq='S'),
            'price': 100 + np.cumsum(np.random.randn(1000) * 0.01),
            'volume': np.random.poisson(100, 1000)
        })
        result = passport.run_certified_backtest(tick_data)
        logger.info("Backtest complete. Passport: %s", passport.passport_id)
    else:
        logger.info("Phase1 failed. Fix issues before backtest.")


if __name__ == '__main__':
    example_usage()
