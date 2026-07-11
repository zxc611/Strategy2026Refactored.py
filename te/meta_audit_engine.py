# MODULE_ID: M1-169
# [M1-135] 元审计引擎
from __future__ import annotations
from ali2026v3_trading.infra.exceptions import FutureLeakException
from ali2026v3_trading.governance.greeks_calculator import _norm_cdf

import ast
import logging
from ali2026v3_trading.infra._helpers import get_logger  # R9-5

import numpy as np

from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any

logger = get_logger(__name__)  # R9-5



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

    参数专用审计：职责边界=回测引擎静态代码审计(未来泄露/排序偏差/数据变异等)。与config/config_logging.AuditLogger(日志审计)、risk/safety_meta_audit(风控审计)分工。'
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
        将源码中字符串字面量的内容替换为空格，保留行结构。'
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

# ── Meta Audit Helpers (merged 2026-06-12) ──

import traceback
import warnings
import logging
from ali2026v3_trading.infra._helpers import get_logger  # R9-5

import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from typing import List, Dict, Callable, Optional, Any


logger = get_logger(__name__)  # R9-5


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
        # 极端z值保护：|z|>8时常norm_cdf返回精确值，p_value不会出现数值问题
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
            except (OSError, ValueError, TypeError, KeyError, RuntimeError) as e:
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
                except (ValueError, KeyError, TypeError, AttributeError, RuntimeError) as e:
                    ghost_results.append({
                        'symbol': symbol,
                        'error': str(e),
                        'error_type': type(e).__name__,
                        'traceback': traceback.format_exc(),
                        'status': 'BACKTEST_FAILED'
                    })
                    continue
            except (ValueError, KeyError, TypeError, AttributeError, RuntimeError, OSError) as e:
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
        v2.3：基于ghost与active的Sharpe差值估计存续偏差。'
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
        - 偏度绝对值>1 表示显著偏斜（不对称双峰）'
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
            # v2.2：偏度绝对值>1（不对称双峰）或超额峰度<-1（对称双峰/平坦分布）'
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
        v2.3：因果依赖感知的对齐规则。'
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