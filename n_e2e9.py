import sys, os, inspect
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

P=0; F=0
def ck(n,c,d=''):
    global P,F
    if c: P+=1; print(f'  PASS: {n}')
    else: F+=1; print(f'  FAIL: {n} {d}')

print('=== E2E-9: 快照定时导出 ===')
from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
ck('threshold=100', MarketSnapshotCollector._auto_export_threshold==100)

col=MarketSnapshotCollector.__new__(MarketSnapshotCollector)
col._auto_export_counter=0; col._auto_export_threshold=100; col._snapshots=[]
col._maybe_auto_export()
ck('计数器递增到1', col._auto_export_counter==1)
for _ in range(98): col._maybe_auto_export()
ck('计数器递增到99', col._auto_export_counter==99)
with patch.object(col,'export_to_duckdb'): col._maybe_auto_export()
ck('计数器达阈值重置0', col._auto_export_counter==0)
scap=inspect.getsource(MarketSnapshotCollector.capture)
ck('capture调用_maybe_auto_export', '_maybe_auto_export' in scap)
print(f'E2E-9: {P} PASS, {F} FAIL')