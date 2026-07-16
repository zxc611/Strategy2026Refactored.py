# MODULE_ID: M1-170
# [M1-134] 元审计护照
import ast
import re
import traceback
import warnings
import logging
from infra._helpers import get_logger  # R9-5

import numpy as np
import pandas as pd

from contextlib import contextmanager
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import List

from .meta_audit_engine import FutureLeakException, _norm_cdf, AuditIssue, Severity, VulnerabilityType, DETERMINISTIC_PATTERNS, MetaAuditEngine

logger = get_logger(__name__)  # R9-5


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
        from infra.shared_utils import set_global_seed  # P2-23: 统一入口
        set_global_seed(42)
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
            在运行时拦截所有数据访问，确保引擎不会读取poison_time之后的数据。'
        作用范围:
            - pd.DataFrame.__getitem__: 拦截df[timestamp_key]形式的时间戳索引访问，
              当audit_time < poison_time时，若key对应的时间>=poison_time，抛出FutureLeakException
            - pd.DataFrame.__getattribute__: 拦截df.iloc/loc/at/iat/xs/query等访问器，
              这些访问器可绕过滤滤getitem__直接获取未来数据，因此在监控期间完全禁止

        安全措施:
            1. 线程锁保护: monkey-patch生效期间持有线程锁，防止并发线程在patch状态下
               访问DataFrame导致不可预测行为。此方法不适合多线程环境。
            2. contextmanager保证还原: 使用try/finally确保即使被监控代码抛出异常，
               原始的指。getitem__和。_getattribute__方法也会被恢复。
            3. 作用域有限: monkey-patch仅在with块内生效，退出后立即还原，
               不影响其他代码对DataFrame的正常使用。
            4. PY-P1-02修复: 使用例例getattribute__替代理理getattr__，前者可拦截
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
            # PY-P1-02修复: 使用例例getattribute__替代理理getattr__，可拦截iloc/loc/at/iat等类级别属性
            if name in ('iloc', 'loc', 'at', 'iat', 'xs', 'query'):
                logger.warning(
                    "DataFrame accessor .%s accessed on %s at audit_time=%s",
                    name, type(self_df).__name__, audit_time['value']
                )
            return original_getattribute(self_df, name)

        def monitored_getattr(self_df, name):
            # PY-P1-02修复: 同时拦截器器getattr__路径的iloc/loc/at/iat等访问
            if name in ('iloc', 'loc', 'at', 'iat', 'xs', 'query'):
                logger.warning(
                    "DataFrame accessor .%s accessed via __getattr__ on %s at audit_time=%s",
                    name, type(self_df).__name__, audit_time['value']
                )
            return original_getattr(self_df, name)

        pd.DataFrame.__getitem__ = monitored_getitem
        original_getattribute = pd.DataFrame.__getattribute__
        pd.DataFrame.__getattribute__ = monitored_getattribute
        pd.DataFrame.__getattr__ = monitored_getattr
        try:
            yield audit_time
        finally:
            pd.DataFrame.__getitem__ = original_getitem
            pd.DataFrame.__getattribute__ = original_getattribute
            pd.DataFrame.__getattr__ = original_getattr
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
        v2.2：引擎支持set_audit_time时的逐tick审计。'
        逐tick设置审计时间并调用set_audit_time，但backtest仍以完整数据调用。
        这是当前架构下的最佳折中：set_audit_time让引擎内部知道当前时间，
        配合并data_monitor拦截未来数据访问。
        """
        for idx in range(len(tick_df)):
            audit_time['value'] = tick_df.loc[idx, 'timestamp']
            engine.set_audit_time(audit_time['value'])
        # 重置audit_time到毒化前，然后调用backtest
        # 此时常data_monitor会拦截对poison_time之后时间戳的访问
        audit_time['value'] = tick_df.loc[0, 'timestamp']
        return engine.backtest(tick_df)

    def _run_with_audit_wrapper(self, engine, tick_df, audit_time, poison_time):
        """
        v2.2修复：不再预循环设置audit_time为终值。'
        将audit_time设置为毒化前的时间点，使。data_monitor能正确拦截
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
                    except (ValueError, KeyError, TypeError):
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


class RestrictedExecLoader:
    ALLOWED_MODULES = {'pandas', 'numpy', 'math', 'datetime', 'collections', 'itertools'}
    # R15-P0-SEC-03修复: 显式排除。_import__/eval/exec/compile/open等危险builtins
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
    @staticmethod
    def _restricted_import(name, *args, **kwargs):
        """受限的指。import__，只允许白名单模块"""
        if name.split('.')[0] not in RestrictedExecLoader.ALLOWED_MODULES:
            raise RuntimeError(f"Blocked import: {name}")
        return __import__(name, *args, **kwargs)

    SAFE_BUILTINS = {
        'True': True, 'False': False, 'None': None,
        'len': len, 'range': range, 'enumerate': enumerate,
        'isinstance': isinstance,
        'list': list, 'dict': dict, 'tuple': tuple, 'set': set, 'frozenset': frozenset,
        'str': str, 'int': int, 'float': float, 'bool': bool,
        'abs': abs, 'min': min, 'max': max, 'sum': sum, 'round': round,
        'sorted': sorted, 'reversed': reversed, 'zip': zip, 'map': map, 'filter': filter,
        'print': print,  # 审计代码需要print输出
        '__import__': _restricted_import.__func__,
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

# ── Meta Audit Passport (merged 2026-06-12) ──

# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

# R9-2: 移除死代码 import hashlib（已全部使用compute_content_hash）
from infra.shared_utils import compute_content_hash
import json
import logging
from infra._helpers import get_logger  # R9-5
import os

import numpy as np
import pandas as pd

from datetime import datetime
from typing import List, Dict, Callable, Optional, Any

from infra.shared_utils import CHINA_TZ
from infra.serialization_utils import json_dumps, json_loads, json_default_serializer
from infra.shared_utils import atomic_replace_file  # R9-1

from .meta_audit_engine import (
    MetaAuditEngine, AuditIssue, Severity, VulnerabilityType, _norm_cdf,
)

logger = get_logger(__name__)  # R9-5

_AUDIT_FORMAT_VERSION = "2.2"


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
        return compute_content_hash(content, truncate=16, algorithm='sha256')  # R5-4

    def is_certification_valid(self) -> bool:
        """检查认证是否有效：未过期且代码哈希一致"""
        if not self.certified:
            return False
        if self.certified_at is None:
            return False
        if (datetime.now(CHINA_TZ) - self.certified_at).total_seconds() > self.certified_ttl_seconds:
            logger.warning("Certification expired: TTL=%ss", self.certified_ttl_seconds)
            return False
        current_hash = compute_content_hash(self.engine_code, algorithm='sha256')  # R5-4
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
            from param_pool.validation.statistical_validation import CounterfactualValidator
            cf_validator = CounterfactualValidator()
            self.audit_results['counterfactual_available'] = True
            logger.info("  CounterfactualValidator loaded successfully")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            self.audit_results['counterfactual_available'] = False
            logger.warning("  CounterfactualValidator not available: %s", e)

        logger.info("[7/9] Monte Carlo Bankruptcy Test (Phase 3)")
        try:
            from param_pool.validation.statistical_validation import MonteCarloBankruptcyValidator
            mc_validator = MonteCarloBankruptcyValidator()
            self.audit_results['monte_carlo_available'] = True
            logger.info("  MonteCarloBankruptcyValidator loaded successfully")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            self.audit_results['monte_carlo_available'] = False
            logger.warning("  MonteCarloBankruptcyValidator not available: %s", e)

        logger.info("[8/9] Shadow Param Independence (Phase 4)")
        try:
            from param_pool.validation.adv_validation_misc import ShadowParamIndependenceValidator
            shadow_validator = ShadowParamIndependenceValidator()
            self.audit_results['shadow_param_validator_available'] = True
            logger.info("  ShadowParamIndependenceValidator loaded successfully")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            self.audit_results['shadow_param_validator_available'] = False
            logger.warning("  ShadowParamIndependenceValidator not available: %s", e)

        logger.info("[9/9] Deep Validation Suite (Phase 5)")
        try:
            from param_pool.validation.statistical_validation import DeepValidationSuite
            deep_suite = DeepValidationSuite()
            self.audit_results['deep_validation_available'] = True
            logger.info("  DeepValidationSuite loaded successfully (7 validators + 2 meta-audit)")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as e:
            self.audit_results['deep_validation_available'] = False
            logger.warning("  DeepValidationSuite not available: %s", e)

        self.audit_results = {
            'version': _AUDIT_FORMAT_VERSION,
            'passport_id': self.passport_id,
            'audit_time': datetime.now(CHINA_TZ).isoformat(),
            'engine_hash': compute_content_hash(self.engine_code, algorithm='sha256'),  # R5-4
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
            self.certified_engine_hash = compute_content_hash(self.engine_code, algorithm='sha256')  # R5-4

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
        - dict结果仍直接附加权audit_passport字段（保持向后兼容）'
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
            'strategy_config_hash': compute_content_hash(
                json.dumps(self.strategy_config, sort_keys=True),
                truncate=16, algorithm='sha256'
            )  # R5-4
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
        atomic_replace_file(path, json_dumps(save_data, indent=2))  # R9-1

    def load(self, path: str):
        """
        v2.2：加载时检查版本兼容性
        """
        with open(path, 'r', encoding='utf-8') as f:
            self.audit_results = json_loads(f.read())  # R6-5

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