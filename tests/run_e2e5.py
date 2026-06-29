import sys, os, time as _time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

class FP:
    def __init__(self, **kw): self._d = kw
    def get(self, k, d=None): return self._d.get(k, d)

P = 0; F = 0
def ck(n, c, d=''):
    global P, F
    if c: P += 1; print(f'  PASS: {n}')
    else: F += 1; print(f'  FAIL: {n} {d}')

print('=== E2E-5: 硬时间止损strategy_group透传 ===')
from ali2026v3_trading.risk.safety_meta_position import HardTimeStopAndComplianceService
svc = HardTimeStopAndComplianceService(FP(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0), owner=None)
ck('spring stage1=180', svc._get_stage1_minutes('spring') == 180.0)
ck('box stage1=120', svc._get_stage1_minutes('box') == 120.0)
ck('arbitrage stage1=60', svc._get_stage1_minutes('arbitrage') == 60.0)
ck('mm stage1=30', svc._get_stage1_minutes('market_making') == 30.0)
ck('default stage1=90', svc._get_stage1_minutes('') == 90.0)
ck('spring stage2=480', svc._get_stage2_minutes('spring') == 480.0)
ck('spring threshold=0.001', svc._get_stage1_profit_threshold('spring') == 0.001)

now = _time.time(); ot = now - 6000
_st = {"hard_time_stop_triggers": 0}
rd = svc.check_position_hard_time_stop('p1', ot, 0.0005, bar_time=now, stats=_st, strategy_group='')
_st2 = {"hard_time_stop_triggers": 0}
rs = svc.check_position_hard_time_stop('p2', ot, 0.0005, bar_time=now, stats=_st2, strategy_group='spring')
ck('default 100min触发', rd is not None, f'result={rd}')
ck('spring 100min不触发', rs is None, f'result={rs}')

from ali2026v3_trading.risk.risk_circuit_breaker import SafetyMetaLayer
ly = SafetyMetaLayer(params=FP(stage1_minutes=90.0, stage2_minutes=240.0, stage1_profit_threshold=0.002, bar_period=1.0))
ck('Facade default触发', ly.check_position_hard_time_stop('p1', ot, 0.0005, bar_time=now, strategy_group='') is not None)
ck('Facade spring不触发', ly.check_position_hard_time_stop('p2', ot, 0.0005, bar_time=now, strategy_group='spring') is None)

from ali2026v3_trading.strategy.strategy_tick_handler import check_hard_time_stop_for_position
from unittest.mock import MagicMock
rs2 = MagicMock(); rs2._safety_meta_layer = ly
ck('tick_handler default触发', check_hard_time_stop_for_position(rs2, 'p1', ot, 0.0005, bar_time=now, strategy_group='') is not None)
ck('tick_handler spring不触发', check_hard_time_stop_for_position(rs2, 'p2', ot, 0.0005, bar_time=now, strategy_group='spring') is None)

import inspect
from ali2026v3_trading.strategy import tick_processing_service
src = inspect.getsource(tick_processing_service.check_hard_time_stop_live)
ck('tick_proc支持PositionRecord', "hasattr(rec, 'open_time')" in src)
ck('tick_proc透传strategy_group', 'strategy_group' in src)

print(f'\nE2E-5: {P} PASS, {F} FAIL')