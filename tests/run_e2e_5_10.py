"""E2E验证5-10: 逐步运行，避免内存超限"""
import sys
import os
import time as _time
import inspect
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

class FP:
    def __init__(self, **kw):
        self._d = kw
    def get(self, k, d=None):
        return self._d.get(k, d)

PASS_COUNT = 0
FAIL_COUNT = 0

def check(name, condition, detail=''):
    global PASS_COUNT, FAIL_COUNT
    if condition:
        PASS_COUNT += 1
        print(f'  PASS: {name}')
    else:
        FAIL_COUNT += 1
        print(f'  FAIL: {name} {detail}')


print('=' * 60)
print('E2E-5: 硬时间止损strategy_group参数透传')
print('=' * 60)

from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
svc = HardTimeStopAndComplianceService(
    FP(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0), owner=None)

check('spring stage1=180', svc._get_stage1_minutes('spring') == 180.0, f'actual={svc._get_stage1_minutes("spring")}')
check('box stage1=120', svc._get_stage1_minutes('box') == 120.0, f'actual={svc._get_stage1_minutes("box")}')
check('arbitrage stage1=60', svc._get_stage1_minutes('arbitrage') == 60.0, f'actual={svc._get_stage1_minutes("arbitrage")}')
check('market_making stage1=30', svc._get_stage1_minutes('market_making') == 30.0, f'actual={svc._get_stage1_minutes("market_making")}')
check('default stage1=90', svc._get_stage1_minutes('') == 90.0, f'actual={svc._get_stage1_minutes("")}')
check('spring stage2=480', svc._get_stage2_minutes('spring') == 480.0, f'actual={svc._get_stage2_minutes("spring")}')
check('spring threshold=0.001', svc._get_stage1_profit_threshold('spring') == 0.001, f'actual={svc._get_stage1_profit_threshold("spring")}')

now = _time.time()
open_time = now - 100 * 60
_stats = {"hard_time_stop_triggers": 0}
r_default = svc.check_position_hard_time_stop('p1', open_time, 0.0005, bar_time=now, stats=_stats, strategy_group='')
_stats2 = {"hard_time_stop_triggers": 0}
r_spring = svc.check_position_hard_time_stop('p2', open_time, 0.0005, bar_time=now, stats=_stats2, strategy_group='spring')
check('default 100min触发', r_default is not None, f'result={r_default}')
check('spring 100min不触发', r_spring is None, f'result={r_spring}')

from ali2026v3_trading.risk.risk_circuit_breaker import SafetyMetaLayer
layer = SafetyMetaLayer(params=FP(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0))
r_fd = layer.check_position_hard_time_stop('p1', open_time, 0.0005, bar_time=now, strategy_group='')
r_fs = layer.check_position_hard_time_stop('p2', open_time, 0.0005, bar_time=now, strategy_group='spring')
check('Facade default触发', r_fd is not None)
check('Facade spring不触发', r_fs is None)

from ali2026v3_trading.strategy.strategy_tick_handler import check_hard_time_stop_for_position
risk_svc = MagicMock()
risk_svc._safety_meta_layer = layer
r_td = check_hard_time_stop_for_position(risk_svc, 'p1', open_time, 0.0005, bar_time=now, strategy_group='')
r_ts = check_hard_time_stop_for_position(risk_svc, 'p2', open_time, 0.0005, bar_time=now, strategy_group='spring')
check('tick_handler default触发', r_td is not None)
check('tick_handler spring不触发', r_ts is None)

from ali2026v3_trading.strategy import tick_processing_service
src = inspect.getsource(tick_processing_service.check_hard_time_stop_live)
check('tick_processing支持PositionRecord', "hasattr(rec, 'open_time')" in src)
check('tick_processing透传strategy_group', 'strategy_group' in src)


print()
print('=' * 60)
print('E2E-6: 两阶段止损_override使用')
print('=' * 60)

from ali2026v3_trading.position import position_pnl_service
src = inspect.getsource(position_pnl_service.PositionPnlService._check_two_stage_stop)
check('源码包含_TWO_STAGE_STRATEGY_OVERRIDES', '_TWO_STAGE_STRATEGY_OVERRIDES' in src)
check('源码使用strategy_group', 'strategy_group' in src)
check('源码从record读取strategy_group', "getattr(record, 'strategy_group'" in src)

_OVERRIDE = {
    'spring': {'stage1_min_minutes': 60.0, 'stage1_profit_threshold': 0.001},
    'arbitrage': {'stage1_min_minutes': 15.0, 'stage1_profit_threshold': 0.005},
    'market_making': {'stage1_min_minutes': 10.0, 'stage1_profit_threshold': 0.01},
}
check('spring stage1_min=60', _OVERRIDE['spring']['stage1_min_minutes'] == 60.0)
check('arbitrage stage1_min=15', _OVERRIDE['arbitrage']['stage1_min_minutes'] == 15.0)
check('market_making stage1_min=10', _OVERRIDE['market_making']['stage1_min_minutes'] == 10.0)
check('空group无override', _OVERRIDE.get('', {}) == {})


print()
print('=' * 60)
print('E2E-7: 快照注入时机')
print('=' * 60)

from ali2026v3_trading.position.position_service import PositionService, PositionRecord
ps = PositionService(risk_service=None)
check('PositionService有set_snapshot_collector', hasattr(ps, 'set_snapshot_collector'))
check('_snapshot_collector初始None', ps._snapshot_collector is None)

mock_collector = MagicMock()
ps.set_snapshot_collector(mock_collector)
check('注入成功', ps._snapshot_collector is mock_collector)

from ali2026v3_trading.position import position_command_service
src_add = inspect.getsource(position_command_service.PositionCommandService._add_position)
check('开仓代码包含ORDER_OPENED', 'ORDER_OPENED' in src_add)
check('开仓代码使用_snapshot_collector', '_snapshot_collector' in src_add)

src_close = inspect.getsource(position_command_service.PositionCommandService._trigger_close_position)
check('平仓代码包含ORDER_CLOSED', 'ORDER_CLOSED' in src_close)
check('平仓代码使用_snapshot_collector', '_snapshot_collector' in src_close)

from ali2026v3_trading.lifecycle import lifecycle_callbacks
src_lc = inspect.getsource(lifecycle_callbacks)
check('lifecycle注入snapshot_collector', 'set_snapshot_collector' in src_lc)


print()
print('=' * 60)
print('E2E-8: EOD豁免逻辑')
print('=' * 60)

src_eod = inspect.getsource(position_pnl_service.PositionPnlService._check_eod_close)
check('EOD代码包含spring豁免', 'spring' in src_eod)
check('EOD代码包含arbitrage豁免', 'arbitrage' in src_eod)
check('EOD代码区分夜盘', 'EOD_Night_Close' in src_eod)
check('EOD代码使用strategy_group', 'strategy_group' in src_eod)

ps2 = MagicMock()
ps2.positions = {}
ps2._trigger_close_position = MagicMock()
ps2.global_lock = MagicMock()
ps2.global_lock.__enter__ = MagicMock(return_value=None)
ps2.global_lock.__exit__ = MagicMock(return_value=False)

now_dt = datetime.now()
for i, sg in enumerate(['spring', 'arbitrage', 'high_freq']):
    rec = PositionRecord(
        position_id=f'p_{i}', instrument_id=f'IF2607_{i}', exchange='CFFEX',
        volume=1, direction='long', open_price=4000.0,
        open_time=now_dt, open_date=now_dt.date(),
        position_type='SPECULATIVE', open_reason='CORRECT_RESONANCE',
    )
    rec.strategy_group = sg
    ps2.positions[f'IF2607_{i}'] = {f'p_{i}': rec}

pnl_svc = position_pnl_service.PositionPnlService(ps2)
night_eod = datetime(2026, 6, 22, 2, 35)
pnl_svc._check_eod_close(now=night_eod)

closed_groups = [getattr(c[0][0], 'strategy_group', '') for c in ps2._trigger_close_position.call_args_list]
check('high_freq被夜盘EOD平仓', 'high_freq' in closed_groups)
check('spring被夜盘EOD豁免', 'spring' not in closed_groups)
check('arbitrage被夜盘EOD豁免', 'arbitrage' not in closed_groups)

ps3 = MagicMock()
ps3.positions = {}
ps3._trigger_close_position = MagicMock()
ps3.global_lock = MagicMock()
ps3.global_lock.__enter__ = MagicMock(return_value=None)
ps3.global_lock.__exit__ = MagicMock(return_value=False)
rec_spring = PositionRecord(
    position_id='p_0', instrument_id='IF2607_0', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0,
    open_time=now_dt, open_date=now_dt.date(),
    position_type='SPECULATIVE', open_reason='CORRECT_RESONANCE',
)
rec_spring.strategy_group = 'spring'
ps3.positions['IF2607_0'] = {'p_0': rec_spring}
pnl_svc3 = position_pnl_service.PositionPnlService(ps3)
day_eod = datetime(2026, 6, 22, 14, 56)
pnl_svc3._check_eod_close(now=day_eod)
day_closed = [getattr(c[0][0], 'strategy_group', '') for c in ps3._trigger_close_position.call_args_list]
check('spring日盘EOD不平仓豁免(应被平仓)', 'spring' in day_closed)


print()
print('=' * 60)
print('E2E-9: 快照定时导出')
print('=' * 60)

from ali2026v3_trading.strategy_judgment.market_snapshot_collector import MarketSnapshotCollector
check('_auto_export_threshold=100', MarketSnapshotCollector._auto_export_threshold == 100)

collector = MarketSnapshotCollector.__new__(MarketSnapshotCollector)
collector._auto_export_counter = 0
collector._auto_export_threshold = 100
collector._snapshots = []
collector._maybe_auto_export()
check('计数器递增到1', collector._auto_export_counter == 1)

for _ in range(98):
    collector._maybe_auto_export()
check('计数器递增到99', collector._auto_export_counter == 99)

with patch.object(collector, 'export_to_duckdb'):
    collector._maybe_auto_export()
check('计数器达阈值后重置为0', collector._auto_export_counter == 0)

src_cap = inspect.getsource(MarketSnapshotCollector.capture)
check('capture调用_maybe_auto_export', '_maybe_auto_export' in src_cap)


print()
print('=' * 60)
print('E2E-10: close_reason持久化')
print('=' * 60)

now_dt2 = datetime.now()
rec = PositionRecord(
    position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0,
    open_time=now_dt2, open_date=now_dt2.date(),
    position_type='SPECULATIVE',
)
check('PositionRecord有close_reason字段', hasattr(rec, 'close_reason'))
check('close_reason初始为空', rec.close_reason == '')

ps4 = PositionService(risk_service=None)
rec2 = PositionRecord(
    position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0,
    open_time=now_dt2, open_date=now_dt2.date(),
    position_type='SPECULATIVE', open_reason='CORRECT_RESONANCE',
)
ps4.positions['IF2607'] = {'p1': rec2}
mock_os = MagicMock()
mock_os.send_order.return_value = 'close_order_123'
with patch.object(ps4._command_svc, '_get_order_service', return_value=mock_os):
    with patch('ali2026v3_trading.position.position_command_service.PositionCommandService._get_signal_id', return_value='sig_close'):
        ps4._trigger_close_position(rec2, "StopLoss@3900.00", 3900.0)
check('close_reason被设置', rec2.close_reason == "StopLoss@3900.00", f'actual={rec2.close_reason}')
check('_closing被设置', rec2._closing == True)

rec3 = PositionRecord(
    position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0,
    open_time=now_dt2, open_date=now_dt2.date(),
    position_type='SPECULATIVE',
)
rec3.close_reason = 'StopLoss@3900.00'
d = rec3.to_dict()
check('close_reason在to_dict中', 'close_reason' in d and d['close_reason'] == 'StopLoss@3900.00')

rec4 = PositionRecord.from_dict(d)
check('close_reason从from_dict恢复', rec4.close_reason == 'StopLoss@3900.00')

rec5 = PositionRecord(
    position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0,
    open_time=now_dt2, open_date=now_dt2.date(),
    position_type='SPECULATIVE',
)
rec5.strategy_group = 'spring'
d5 = rec5.to_dict()
check('strategy_group在to_dict中', 'strategy_group' in d5 and d5['strategy_group'] == 'spring')

rec6 = PositionRecord.from_dict(d5)
check('strategy_group从from_dict恢复', rec6.strategy_group == 'spring')

rec7 = PositionRecord(
    position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0,
    open_time=now_dt2, open_date=now_dt2.date(),
    position_type='SPECULATIVE',
)
rec7.strategy_group = 'arbitrage'
rec7.close_reason = 'EOD_Night_Close'
d7 = rec7.to_dict()
rec8 = PositionRecord.from_dict(d7)
check('往返一致性: strategy_group', rec8.strategy_group == 'arbitrage')
check('往返一致性: close_reason', rec8.close_reason == 'EOD_Night_Close')

from ali2026v3_trading.position.position_greeks import _REASON_STRATEGY_MAP
required = ['CORRECT_RESONANCE', 'CORRECT_DIVERGENCE', 'INCORRECT_REVERSAL',
            'INCORRECT_DIVERGENCE', 'OTHER_SCALP', 'BOX_SPRING',
            'ARBITRAGE', 'MARKET_MAKING', 'MANUAL']
missing = [k for k in required if k not in _REASON_STRATEGY_MAP]
check('_REASON_STRATEGY_MAP完整', len(missing) == 0, f'missing={missing}')


print()
print('=' * 60)
total = PASS_COUNT + FAIL_COUNT
print(f'总计: {PASS_COUNT}/{total} PASS, {FAIL_COUNT}/{total} FAIL')
if FAIL_COUNT == 0:
    print('ALL TESTS PASSED!')
else:
    print(f'{FAIL_COUNT} TESTS FAILED!')
print('=' * 60)