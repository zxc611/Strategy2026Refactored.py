import sys, os, inspect
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

P = 0; F = 0
def ck(n, c, d=''):
    global P, F
    if c: P += 1; print(f'  PASS: {n}')
    else: F += 1; print(f'  FAIL: {n} {d}')

print('=== E2E-6: 两阶段止损_override ===')
from ali2026v3_trading.position import position_pnl_service
src = inspect.getsource(position_pnl_service.PositionPnlService._check_two_stage_stop)
ck('源码含_TWO_STAGE_STRATEGY_OVERRIDES', '_TWO_STAGE_STRATEGY_OVERRIDES' in src)
ck('源码用strategy_group', 'strategy_group' in src)
ck('源码从record读strategy_group', "getattr(record, 'strategy_group'" in src)

_O = {'spring': {'stage1_min_minutes': 60.0, 'stage1_profit_threshold': 0.001},
      'arbitrage': {'stage1_min_minutes': 15.0, 'stage1_profit_threshold': 0.005},
      'market_making': {'stage1_min_minutes': 10.0, 'stage1_profit_threshold': 0.01}}
ck('spring stage1_min=60', _O['spring']['stage1_min_minutes'] == 60.0)
ck('arb stage1_min=15', _O['arbitrage']['stage1_min_minutes'] == 15.0)
ck('mm stage1_min=10', _O['market_making']['stage1_min_minutes'] == 10.0)
ck('空group无override', _O.get('', {}) == {})
print(f'\nE2E-6: {P} PASS, {F} FAIL')

P2 = 0; F2 = 0
def ck2(n, c, d=''):
    global P2, F2
    if c: P2 += 1; print(f'  PASS: {n}')
    else: F2 += 1; print(f'  FAIL: {n} {d}')

print('\n=== E2E-7: 快照注入时机 ===')
from ali2026v3_trading.position.position_service import PositionService, PositionRecord
ps = PositionService(risk_service=None)
ck2('有set_snapshot_collector', hasattr(ps, 'set_snapshot_collector'))
ck2('_snapshot_collector初始None', ps._snapshot_collector is None)
mc = MagicMock(); ps.set_snapshot_collector(mc)
ck2('注入成功', ps._snapshot_collector is mc)

from ali2026v3_trading.position import position_command_service
sa = inspect.getsource(position_command_service.PositionCommandService._add_position)
ck2('开仓含ORDER_OPENED', 'ORDER_OPENED' in sa)
ck2('开仓用_snapshot_collector', '_snapshot_collector' in sa)
sc = inspect.getsource(position_command_service.PositionCommandService._trigger_close_position)
ck2('平仓含ORDER_CLOSED', 'ORDER_CLOSED' in sc)
ck2('平仓用_snapshot_collector', '_snapshot_collector' in sc)

from ali2026v3_trading.lifecycle import lifecycle_callbacks
sl = inspect.getsource(lifecycle_callbacks)
ck2('lifecycle注入snapshot', 'set_snapshot_collector' in sl)
print(f'\nE2E-7: {P2} PASS, {F2} FAIL')

P3 = 0; F3 = 0
def ck3(n, c, d=''):
    global P3, F3
    if c: P3 += 1; print(f'  PASS: {n}')
    else: F3 += 1; print(f'  FAIL: {n} {d}')

print('\n=== E2E-8: EOD豁免逻辑 ===')
se = inspect.getsource(position_pnl_service.PositionPnlService._check_eod_close)
ck3('EOD含spring豁免', 'spring' in se)
ck3('EOD含arbitrage豁免', 'arbitrage' in se)
ck3('EOD区分夜盘', 'EOD_Night_Close' in se)
ck3('EOD用strategy_group', 'strategy_group' in se)

ps2 = MagicMock()
ps2.positions = {}
ps2._trigger_close_position = MagicMock()
ps2.global_lock = MagicMock()
ps2.global_lock.__enter__ = MagicMock(return_value=None)
ps2.global_lock.__exit__ = MagicMock(return_value=False)
now = datetime.now()
for i, sg in enumerate(['spring', 'arbitrage', 'high_freq']):
    r = PositionRecord(position_id=f'p{i}', instrument_id=f'IF{i}', exchange='CFFEX',
        volume=1, direction='long', open_price=4000.0, open_time=now, open_date=now.date(),
        position_type='SPECULATIVE', open_reason='CORRECT_RESONANCE')
    r.strategy_group = sg
    ps2.positions[f'IF{i}'] = {f'p{i}': r}

pnl = position_pnl_service.PositionPnlService(ps2)
pnl._check_eod_close(now=datetime(2026, 6, 22, 2, 35))
cg = [getattr(c[0][0], 'strategy_group', '') for c in ps2._trigger_close_position.call_args_list]
ck3('high_freq夜盘EOD平仓', 'high_freq' in cg)
ck3('spring夜盘EOD豁免', 'spring' not in cg)
ck3('arbitrage夜盘EOD豁免', 'arbitrage' not in cg)

ps3 = MagicMock()
ps3.positions = {}; ps3._trigger_close_position = MagicMock()
ps3.global_lock = MagicMock()
ps3.global_lock.__enter__ = MagicMock(return_value=None)
ps3.global_lock.__exit__ = MagicMock(return_value=False)
r2 = PositionRecord(position_id='p0', instrument_id='IF0', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0, open_time=now, open_date=now.date(),
    position_type='SPECULATIVE', open_reason='CORRECT_RESONANCE')
r2.strategy_group = 'spring'
ps3.positions['IF0'] = {'p0': r2}
pnl3 = position_pnl_service.PositionPnlService(ps3)
pnl3._check_eod_close(now=datetime(2026, 6, 22, 14, 56))
dcg = [getattr(c[0][0], 'strategy_group', '') for c in ps3._trigger_close_position.call_args_list]
ck3('spring日盘EOD不平仓豁免', 'spring' in dcg)
print(f'\nE2E-8: {P3} PASS, {F3} FAIL')

P4 = 0; F4 = 0
def ck4(n, c, d=''):
    global P4, F4
    if c: P4 += 1; print(f'  PASS: {n}')
    else: F4 += 1; print(f'  FAIL: {n} {d}')

print('\n=== E2E-9: 快照定时导出 ===')
from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
ck4('threshold=100', MarketSnapshotCollector._auto_export_threshold == 100)

col = MarketSnapshotCollector.__new__(MarketSnapshotCollector)
col._auto_export_counter = 0; col._auto_export_threshold = 100; col._snapshots = []
col._maybe_auto_export()
ck4('计数器递增到1', col._auto_export_counter == 1)
for _ in range(98): col._maybe_auto_export()
ck4('计数器递增到99', col._auto_export_counter == 99)
with patch.object(col, 'export_to_duckdb'): col._maybe_auto_export()
ck4('计数器达阈值重置0', col._auto_export_counter == 0)
scap = inspect.getsource(MarketSnapshotCollector.capture)
ck4('capture调用_maybe_auto_export', '_maybe_auto_export' in scap)
print(f'\nE2E-9: {P4} PASS, {F4} FAIL')

P5 = 0; F5 = 0
def ck5(n, c, d=''):
    global P5, F5
    if c: P5 += 1; print(f'  PASS: {n}')
    else: F5 += 1; print(f'  FAIL: {n} {d}')

print('\n=== E2E-10: close_reason持久化 ===')
now2 = datetime.now()
r3 = PositionRecord(position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0, open_time=now2, open_date=now2.date(),
    position_type='SPECULATIVE')
ck5('有close_reason字段', hasattr(r3, 'close_reason'))
ck5('close_reason初始空', r3.close_reason == '')

ps4 = PositionService(risk_service=None)
r4 = PositionRecord(position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0, open_time=now2, open_date=now2.date(),
    position_type='SPECULATIVE', open_reason='CORRECT_RESONANCE')
ps4.positions['IF2607'] = {'p1': r4}
mos = MagicMock(); mos.send_order.return_value = 'co123'
with patch.object(ps4._command_svc, '_get_order_service', return_value=mos):
    with patch('ali2026v3_trading.position.position_command_service.PositionCommandService._get_signal_id', return_value='sc'):
        ps4._trigger_close_position(r4, "StopLoss@3900.00", 3900.0)
ck5('close_reason被设置', r4.close_reason == "StopLoss@3900.00", f'actual={r4.close_reason}')
ck5('_closing被设置', r4._closing)

r5 = PositionRecord(position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0, open_time=now2, open_date=now2.date(),
    position_type='SPECULATIVE')
r5.close_reason = 'StopLoss@3900.00'
d = r5.to_dict()
ck5('close_reason在to_dict', 'close_reason' in d and d['close_reason'] == 'StopLoss@3900.00')
r6 = PositionRecord.from_dict(d)
ck5('close_reason从from_dict恢复', r6.close_reason == 'StopLoss@3900.00')

r7 = PositionRecord(position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0, open_time=now2, open_date=now2.date(),
    position_type='SPECULATIVE')
r7.strategy_group = 'spring'
d7 = r7.to_dict()
ck5('strategy_group在to_dict', 'strategy_group' in d7 and d7['strategy_group'] == 'spring')
r8 = PositionRecord.from_dict(d7)
ck5('strategy_group从from_dict恢复', r8.strategy_group == 'spring')

r9 = PositionRecord(position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0, open_time=now2, open_date=now2.date(),
    position_type='SPECULATIVE')
r9.strategy_group = 'arbitrage'; r9.close_reason = 'EOD_Night_Close'
d9 = r9.to_dict(); r10 = PositionRecord.from_dict(d9)
ck5('往返: strategy_group', r10.strategy_group == 'arbitrage')
ck5('往返: close_reason', r10.close_reason == 'EOD_Night_Close')

from ali2026v3_trading.position.position_greeks import _REASON_STRATEGY_MAP
req = ['CORRECT_RESONANCE', 'CORRECT_DIVERGENCE', 'INCORRECT_REVERSAL',
       'INCORRECT_DIVERGENCE', 'OTHER_SCALP', 'BOX_SPRING', 'ARBITRAGE', 'MARKET_MAKING', 'MANUAL']
mis = [k for k in req if k not in _REASON_STRATEGY_MAP]
ck5('_REASON_STRATEGY_MAP完整', len(mis) == 0, f'missing={mis}')
print(f'\nE2E-10: {P5} PASS, {F5} FAIL')

print('\n' + '=' * 60)
tp = P+P2+P3+P4+P5; tf = F+F2+F3+F4+F5
print(f'总计: {tp}/{tp+tf} PASS, {tf}/{tp+tf} FAIL')
if tf == 0: print('ALL TESTS PASSED!')
else: print(f'{tf} TESTS FAILED!')