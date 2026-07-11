import sys, os, inspect
from datetime import datetime
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

P=0; F=0
def ck(n,c,d=''):
    global P,F
    if c: P+=1; print(f'  PASS: {n}')
    else: F+=1; print(f'  FAIL: {n} {d}')

print('=== E2E-7: 快照注入时机 ===')
from ali2026v3_trading.position.position_service import PositionService, PositionRecord
ps = PositionService(risk_service=None)
ck('有set_snapshot_collector', hasattr(ps, 'set_snapshot_collector'))
ck('初始None', ps._snapshot_collector is None)
mc=MagicMock(); ps.set_snapshot_collector(mc)
ck('注入成功', ps._snapshot_collector is mc)

from ali2026v3_trading.position import position_command_service
sa=inspect.getsource(position_command_service.PositionCommandService._add_position)
ck('开仓含ORDER_OPENED', 'ORDER_OPENED' in sa)
ck('开仓用_snapshot_collector', '_snapshot_collector' in sa)
sc=inspect.getsource(position_command_service.PositionCommandService._trigger_close_position)
ck('平仓含ORDER_CLOSED', 'ORDER_CLOSED' in sc)
ck('平仓用_snapshot_collector', '_snapshot_collector' in sc)

from ali2026v3_trading.lifecycle import lifecycle_callbacks
sl=inspect.getsource(lifecycle_callbacks)
ck('lifecycle注入snapshot', 'set_snapshot_collector' in sl)
print(f'E2E-7: {P} PASS, {F} FAIL')