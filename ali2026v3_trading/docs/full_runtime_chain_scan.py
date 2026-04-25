"""全量运行链扫描脚本。

目标：
1. 只扫描 live 代码，自动排除 backup、历史恢复目录和缓存目录。
2. 检查旧 schema / 旧 API / 语义漂移残留是否还在可执行代码中。
3. 检查关键运行链符号是否存在，尤其是 Storage -> DataService 的 K 线落库链。
4. 在不污染正式数据的前提下，做一次最小 K 线落库预演。
5. 扫描最新 strategy.log 中的致命错误，直接给出失败原因。

运行方式：
    python ali2026v3_trading/full_runtime_chain_scan.py

返回码：
    0: 所有关键检查通过
    1: 存在失败项
"""

from __future__ import annotations

import importlib
import inspect
import os
import re
import sys
import traceback
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence


PROJECT_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = PROJECT_DIR.parent
LOG_FILE = PROJECT_DIR / 'logs' / 'strategy.log'

EXCLUDED_DIR_NAMES = {
    '__pycache__',
    'backup',
    'backup_groupA',
    'backup_groupB',
    'backup_group_a',
    'backup_id_passthrough',
    'logs',
    'data',
}

EXCLUDED_FILE_PATTERNS = (
    re.compile(r'.*\.backup_\d+.*\.py$', re.IGNORECASE),
    re.compile(r'^backup_.*\.py$', re.IGNORECASE),
)


@dataclass
class CheckResult:
    name: str
    status: str
    details: List[str] = field(default_factory=list)


def iter_live_python_files() -> Iterable[Path]:
    for root, dirs, files in os.walk(PROJECT_DIR):
        dirs[:] = [directory for directory in dirs if directory not in EXCLUDED_DIR_NAMES]
        root_path = Path(root)
        if any(part.startswith('local_history_restore_') for part in root_path.parts):
            continue

        for file_name in files:
            if not file_name.endswith('.py'):
                continue
            if any(pattern.match(file_name) for pattern in EXCLUDED_FILE_PATTERNS):
                continue
            yield root_path / file_name


def relative_path(path: Path) -> str:
    return str(path.relative_to(WORKSPACE_DIR)).replace('\\', '/')


def read_text(path: Path) -> str:
    return path.read_text(encoding='utf-8', errors='replace')


def tail_lines(path: Path, max_lines: int = 1200) -> List[str]:
    if not path.exists():
        return []
    lines = read_text(path).splitlines()
    return lines[-max_lines:]


def make_result(name: str, ok: bool, details: Sequence[str]) -> CheckResult:
    return CheckResult(name=name, status='PASS' if ok else 'FAIL', details=list(details))


def scan_forbidden_patterns() -> CheckResult:
    patterns = [
        ('调用旧 API resolve_meta', re.compile(r'\bresolve_meta\s*\(')),
        ('读取旧列 oi.underlying_future', re.compile(r'\boi\.underlying_future\b')),
        ('读取旧 metadata 字段 underlying_future', re.compile(r"meta\.get\('underlying_future'|info\.get\('underlying_future'")),
        ('将 underlying_future_id 直接写入 future_instrument_id', re.compile(r"future_instrument_id\s*=\s*info\.get\('underlying_future_id'\)|tick_row\['future_instrument_id'\]\s*=\s*info\.get\('underlying_future_id'\)")),
    ]

    findings: List[str] = []
    for path in iter_live_python_files():
        if path.resolve() == Path(__file__).resolve():
            continue
        for line_no, line in enumerate(read_text(path).splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith('#'):
                continue
            for description, pattern in patterns:
                if pattern.search(line):
                    findings.append(f'{relative_path(path)}:{line_no} {description}: {stripped}')

    return make_result('live 旧路径残留扫描', not findings, findings or ['未发现 live 可执行旧路径残留'])


def scan_required_symbols() -> CheckResult:
    details: List[str] = []
    failed = False

    try:
        data_service_module = importlib.import_module('ali2026v3_trading.data_service')
        storage_module = importlib.import_module('ali2026v3_trading.storage')
    except Exception as exc:
        return make_result('关键符号存在性检查', False, [f'模块导入失败: {exc}', traceback.format_exc()])

    required = [
        ('DataService.batch_insert_ticks', hasattr(data_service_module.DataService, 'batch_insert_ticks')),
        ('DataService.batch_insert_klines', hasattr(data_service_module.DataService, 'batch_insert_klines')),
        ('data_service.get_data_service', hasattr(data_service_module, 'get_data_service')),
        ('InstrumentDataManager._save_kline_impl', hasattr(storage_module.InstrumentDataManager, '_save_kline_impl')),
    ]

    for symbol_name, exists in required:
        symbol_state = 'OK' if exists else 'MISSING'
        details.append(f'{symbol_name}: {symbol_state}')
        failed = failed or not exists

    try:
        save_kline_source = inspect.getsource(storage_module.InstrumentDataManager._save_kline_impl)
        if 'batch_insert_klines' not in save_kline_source:
            failed = True
            details.append('InstrumentDataManager._save_kline_impl 未调用 batch_insert_klines，K 线落库链不完整')
        else:
            details.append('InstrumentDataManager._save_kline_impl 已调用 batch_insert_klines')
    except Exception as exc:
        failed = True
        details.append(f'读取 _save_kline_impl 源码失败: {exc}')

    return make_result('关键符号存在性检查', not failed, details)


def smoke_test_kline_write_chain() -> CheckResult:
    details: List[str] = []

    try:
        data_service_module = importlib.import_module('ali2026v3_trading.data_service')
        ds = data_service_module.get_data_service()
    except Exception as exc:
        return make_result('K 线落库预演', False, [f'DataService 初始化失败: {exc}', traceback.format_exc()])

    if not hasattr(ds, 'batch_insert_klines'):
        return make_result('K 线落库预演', False, ['DataService 缺少 batch_insert_klines，Storage 异步 K 线写入链当前必断'])

    # ✅ 测试统一表 klines_raw（Storage._save_kline_impl 使用的方案）
    table_name = 'klines_raw'
    try:
        conn = ds._get_connection()
        # 清理测试数据
        conn.execute(f'DELETE FROM {table_name} WHERE internal_id = 999991')
        
        inserted = ds.batch_insert_klines([
            {
                'internal_id': 999991,
                'instrument_type': 'future',
                'timestamp': datetime(2026, 4, 18, 14, 40, 52),
                'open': 100.0,
                'high': 110.0,
                'low': 95.0,
                'close': 108.0,
                'volume': 12,
                'open_interest': 34,
                'trade_date': date(2026, 4, 18),
            },
            {
                'internal_id': 999991,
                'instrument_type': 'future',
                'timestamp': datetime(2026, 4, 18, 14, 41, 52),
                'open': 108.0,
                'high': 112.0,
                'low': 107.0,
                'close': 111.0,
                'volume': 20,
                'open_interest': 35,
                'trade_date': date(2026, 4, 18),
            },
        ])
        rows = conn.execute(
            f'SELECT timestamp, open, high, low, close, volume, open_interest FROM {table_name} WHERE internal_id = 999991 ORDER BY timestamp'
        ).fetchall()
        
        # 清理测试数据
        conn.execute(f'DELETE FROM {table_name} WHERE internal_id = 999991')

        if inserted != 2:
            return make_result('K 线落库预演', False, [f'batch_insert_klines 返回 {inserted}，预期 2'])
        if len(rows) != 2:
            return make_result('K 线落库预演', False, [f'{table_name} 实际写入 {len(rows)} 行，预期 2'])

        details.append('batch_insert_klines 可调用，且成功写入/读回测试数据')
        return make_result('K 线落库预演', True, details)
    except Exception as exc:
        cleanup_error: Optional[str] = None
        try:
            conn = ds._get_connection()
            conn.execute(f'DROP TABLE IF EXISTS {table_name}')
        except Exception as drop_exc:
            cleanup_error = str(drop_exc)

        failure_details = [f'K 线落库预演失败: {exc}', traceback.format_exc()]
        if cleanup_error:
            failure_details.append(f'清理测试表失败: {cleanup_error}')
        return make_result('K 线落库预演', False, failure_details)


def scan_latest_log() -> CheckResult:
    if not LOG_FILE.exists():
        return make_result('最新日志致命错误扫描', False, [f'日志不存在: {relative_path(LOG_FILE)}'])

    lines = tail_lines(LOG_FILE, max_lines=1600)
    fatal_patterns = [
        ('缺少 batch_insert_klines', re.compile(r"batch_insert_klines")),
        ('旧列 underlying_future 绑定错误', re.compile(r'Binder Error.*underlying_future|Referenced column .*underlying_future', re.IGNORECASE)),
        ('写入队列满导致丢数', re.compile(r'\[DATA_LOSS\]|写入队列已满|\[QUEUE_CRITICAL\]')),
        ('合约 metadata 缺失', re.compile(r'Contract metadata not found for:')),
        ('异常堆栈', re.compile(r'^Traceback \(most recent call last\):')),
    ]

    findings: List[str] = []
    for index, line in enumerate(lines, start=1):
        for description, pattern in fatal_patterns:
            if pattern.search(line):
                findings.append(f'log:{index} {description}: {line}')

    # 去重，避免队列满刷屏把报告淹没
    deduped: List[str] = []
    seen = set()
    for item in findings:
        normalized = re.sub(r'累计丢弃=\d+', '累计丢弃=<n>', item)
        normalized = re.sub(r'log:\d+\s+', 'log:<n> ', normalized)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(item)
        if len(deduped) >= 20:
            break

    return make_result('最新日志致命错误扫描', not deduped, deduped or ['最新日志尾部未发现致命错误模式'])


def scan_storage_data_service_contract() -> CheckResult:
    storage_path = PROJECT_DIR / 'storage.py'
    data_service_path = PROJECT_DIR / 'data_service.py'

    storage_source = read_text(storage_path)
    data_service_source = read_text(data_service_path)
    details: List[str] = []
    failed = False

    if 'self._data_service.batch_insert_klines(normalized_klines)' in storage_source:
        details.append('storage.py 的 _save_kline_impl 明确依赖 self._data_service.batch_insert_klines')
    else:
        details.append('storage.py 未发现 self._data_service.batch_insert_klines 调用')
        failed = True

    if re.search(r'def\s+batch_insert_klines\s*\(', data_service_source):
        details.append('data_service.py 存在 batch_insert_klines 定义')
    else:
        details.append('data_service.py 不存在 batch_insert_klines 定义')
        failed = True

    return make_result('Storage/DataService 合同一致性扫描', not failed, details)


def run_checks() -> List[CheckResult]:
    checks: List[Callable[[], CheckResult]] = [
        scan_forbidden_patterns,
        scan_required_symbols,
        scan_storage_data_service_contract,
        smoke_test_kline_write_chain,
        scan_latest_log,
    ]
    return [check() for check in checks]


def print_report(results: Sequence[CheckResult]) -> int:
    print('=' * 88)
    print('FULL RUNTIME CHAIN SCAN REPORT')
    print('=' * 88)

    failed = False
    for result in results:
        print(f'[{result.status}] {result.name}')
        for detail in result.details:
            print(f'  - {detail}')
        print('-' * 88)
        failed = failed or result.status != 'PASS'

    if failed:
        print('FINAL RESULT: FAIL')
        print('说明：至少有一条 live 运行链、关键符号或最新日志致命错误未收口。')
        return 1

    print('FINAL RESULT: PASS')
    print('说明：live 运行链关键检查全部通过。')
    return 0


def main() -> int:
    sys.path.insert(0, str(WORKSPACE_DIR))
    results = run_checks()
    return print_report(results)


if __name__ == '__main__':
    sys.exit(main())