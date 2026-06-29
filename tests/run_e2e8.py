import sys, os, inspect
from datetime import datetime
from unittest.mock import MagicMock
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

P=0; F=0
def ck(n,c,d=''):
    global P,F
    if c: P+=1; print(f'  PASS: {n}')
    else: F+=1; print(f'  FAIL: {n} {d}')

print('=== E2E-8: EOD豁免逻辑 ===')
from ali2026v3_trading.position import position_pnl_service
from ali2026v3_trading.position.position_service import PositionRecord
se=inspect.getsource(position_pnl_service.PositionPnlService._check_eod_close)
ck('EOD含spring豁免', 'spring' in se)
ck('EOD含arbitrage豁免', 'arbitrage' in se)
ck('EOD区分夜盘', 'EOD_Night_Close' in se)
ck('EOD用strategy_group', 'strategy_group' in se)

ps=MagicMock()
ps.positions={}; ps._trigger_close_position=MagicMock()
ps.global_lock=MagicMock()
ps.global_lock.__enter__=MagicMock(return_value=None)
ps.global_lock.__exit__=MagicMock(return_value=False)
now=datetime.now()
for i,sg in enumerate(['spring','arbitrage','high_freq']):
    r=PositionRecord(position_id=f'p{i}',instrument_id=f'IF{i}',exchange='CFFEX',
        volume=1,direction='long',open_price=4000.0,open_time=now,open_date=now.date(),
        position_type='SPECULATIVE',open_reason='CORRECT_RESONANCE')
    r.strategy_group=sg
    ps.positions[f'IF{i}']={f'p{i}':r}

pnl=position_pnl_service.PositionPnlService(ps)
pnl._check_eod_close(now=datetime(2026,6,22,2,35))
cg=[getattr(c[0][0],'strategy_group','') for c in ps._trigger_close_position.call_args_list]
ck('high_freq夜盘EOD平仓','high_freq' in cg)
ck('spring夜盘EOD豁免','spring' not in cg)
ck('arbitrage夜盘EOD豁免','arbitrage' not in cg)

ps2=MagicMock()
ps2.positions={}; ps2._trigger_close_position=MagicMock()
ps2.global_lock=MagicMock()
ps2.global_lock.__enter__=MagicMock(return_value=None)
ps2.global_lock.__exit__=MagicMock(return_value=False)
r2=PositionRecord(position_id='p0',instrument_id='IF0',exchange='CFFEX',
    volume=1,direction='long',open_price=4000.0,open_time=now,open_date=now.date(),
    position_type='SPECULATIVE',open_reason='CORRECT_RESONANCE')
r2.strategy_group='spring'
ps2.positions['IF0']={'p0':r2}
pnl2=position_pnl_service.PositionPnlService(ps2)
pnl2._check_eod_close(now=datetime(2026,6,22,14,56))
dcg=[getattr(c[0][0],'strategy_group','') for c in ps2._trigger_close_position.call_args_list]
ck('spring日盘EOD不豁免','spring' in dcg)
print(f'E2E-8: {P} PASS, {F} FAIL')