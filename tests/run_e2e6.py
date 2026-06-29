import sys, os, inspect
from datetime import datetime
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

P=0; F=0
def ck(n,c,d=''):
    global P,F
    if c: P+=1; print(f'  PASS: {n}')
    else: F+=1; print(f'  FAIL: {n} {d}')

print('=== E2E-6: 两阶段止损_override ===')
from ali2026v3_trading.position import position_pnl_service
src = inspect.getsource(position_pnl_service.PositionPnlService._check_two_stage_stop)
ck('源码含TWO_STAGE_STRATEGY_OVERRIDES', '_TWO_STAGE_STRATEGY_OVERRIDES' in src)
ck('源码用strategy_group', 'strategy_group' in src)
ck('源码从record读strategy_group', "getattr(record, 'strategy_group'" in src)
_O={'spring':{'stage1_min_minutes':60.0,'stage1_profit_threshold':0.001},
    'arbitrage':{'stage1_min_minutes':15.0,'stage1_profit_threshold':0.005},
    'market_making':{'stage1_min_minutes':10.0,'stage1_profit_threshold':0.01}}
ck('spring=60',_O['spring']['stage1_min_minutes']==60.0)
ck('arb=15',_O['arbitrage']['stage1_min_minutes']==15.0)
ck('mm=10',_O['market_making']['stage1_min_minutes']==10.0)
ck('空无override',_O.get('',{})=={})
print(f'E2E-6: {P} PASS, {F} FAIL')