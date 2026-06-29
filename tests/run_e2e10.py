import sys, os
from datetime import datetime
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

P=0; F=0
def ck(n,c,d=''):
    global P,F
    if c: P+=1; print(f'  PASS: {n}')
    else: F+=1; print(f'  FAIL: {n} {d}')

print('=== E2E-10: close_reason持久化 ===')
from ali2026v3_trading.position.position_service import PositionRecord
now=datetime.now()

# 字段存在性
r=PositionRecord(position_id='p1',instrument_id='IF2607',exchange='CFFEX',
    volume=1,direction='long',open_price=4000.0,open_time=now,open_date=now.date(),
    position_type='SPECULATIVE')
ck('有close_reason', hasattr(r,'close_reason'))
ck('close_reason初始空', r.close_reason=='')
ck('有strategy_group', hasattr(r,'strategy_group'))

# close_reason to_dict/from_dict
r.close_reason='StopLoss@3900.00'
d=r.to_dict()
ck('close_reason在to_dict', 'close_reason' in d and d['close_reason']=='StopLoss@3900.00')
r2=PositionRecord.from_dict(d)
ck('close_reason从from_dict恢复', r2.close_reason=='StopLoss@3900.00')

# strategy_group to_dict/from_dict
r3=PositionRecord(position_id='p1',instrument_id='IF2607',exchange='CFFEX',
    volume=1,direction='long',open_price=4000.0,open_time=now,open_date=now.date(),
    position_type='SPECULATIVE')
r3.strategy_group='spring'
d3=r3.to_dict()
ck('strategy_group在to_dict', 'strategy_group' in d3 and d3['strategy_group']=='spring')
r4=PositionRecord.from_dict(d3)
ck('strategy_group从from_dict恢复', r4.strategy_group=='spring')

# 往返一致性
r5=PositionRecord(position_id='p1',instrument_id='IF2607',exchange='CFFEX',
    volume=1,direction='long',open_price=4000.0,open_time=now,open_date=now.date(),
    position_type='SPECULATIVE')
r5.strategy_group='arbitrage'; r5.close_reason='EOD_Night_Close'
d5=r5.to_dict(); r6=PositionRecord.from_dict(d5)
ck('往返:sg', r6.strategy_group=='arbitrage')
ck('往返:cr', r6.close_reason=='EOD_Night_Close')

# _REASON_STRATEGY_MAP
from ali2026v3_trading.position.position_greeks import _REASON_STRATEGY_MAP
req=['CORRECT_RESONANCE','CORRECT_DIVERGENCE','INCORRECT_REVERSAL',
     'INCORRECT_DIVERGENCE','OTHER_SCALP','BOX_SPRING','ARBITRAGE','MARKET_MAKING','MANUAL']
mis=[k for k in req if k not in _REASON_STRATEGY_MAP]
ck('REASON_STRATEGY_MAP完整', len(mis)==0, f'missing={mis}')

# close_reason在_trigger_close_position中设置（源码验证）
import inspect
from ali2026v3_trading.position import position_command_service
src=inspect.getsource(position_command_service.PositionCommandService._trigger_close_position)
ck('平仓源码设置close_reason', 'close_reason' in src and 'reason' in src)

print(f'E2E-10: {P} PASS, {F} FAIL')
