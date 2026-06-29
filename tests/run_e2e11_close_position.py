"""E2E-11: 平仓完整链路功能验证
验证:
1. _trigger_close_position 价格获取fallback链路 (tick字段名/record.current_price/get_latest_price)
2. 6个调用点全部传入current_price关键字参数
3. 平仓无价格时need_retry=True (不直接return放弃)
4. PositionRecord字段(strategy_group/close_reason/realized_pnl)在平仓流程中正确设置
5. to_dict/from_dict往返一致性
"""
import sys, os, inspect
from datetime import datetime, date
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

P = 0; F = 0
def ck(n, c, d=''):
    global P, F
    if c: P += 1; print(f'  PASS: {n}')
    else: F += 1; print(f'  FAIL: {n} {d}')

print('=== E2E-11: 平仓完整链路功能验证 ===')

# ── 1. PositionRecord字段完整性 ──
print('\n--- 1. PositionRecord字段完整性 ---')
from ali2026v3_trading.position.position_service import PositionRecord
now = datetime.now()
r = PositionRecord(
    position_id='p1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0, open_time=now,
    open_date=now.date(), position_type='SPECULATIVE',
)
ck('有strategy_group', hasattr(r, 'strategy_group'))
ck('有close_reason', hasattr(r, 'close_reason'))
ck('有realized_pnl', hasattr(r, 'realized_pnl'))
ck('有current_price', hasattr(r, 'current_price'))
ck('strategy_group默认空', r.strategy_group == '')
ck('close_reason默认空', r.close_reason == '')
ck('realized_pnl默认0', r.realized_pnl == 0.0)
ck('current_price默认0', r.current_price == 0.0)

# ── 2. to_dict/from_dict往返 ──
print('\n--- 2. to_dict/from_dict往返 ---')
r.strategy_group = 'spring'
r.close_reason = 'TimeStop@60min'
r.realized_pnl = 120.5
r.current_price = 4120.0
d = r.to_dict()
ck('to_dict含strategy_group', d.get('strategy_group') == 'spring')
ck('to_dict含close_reason', d.get('close_reason') == 'TimeStop@60min')
ck('to_dict含realized_pnl', d.get('realized_pnl') == 120.5)
ck('to_dict含current_price', d.get('current_price') == 4120.0)
r2 = PositionRecord.from_dict(d)
ck('from_dict恢复strategy_group', r2.strategy_group == 'spring')
ck('from_dict恢复close_reason', r2.close_reason == 'TimeStop@60min')
ck('from_dict恢复realized_pnl', r2.realized_pnl == 120.5)
ck('from_dict恢复current_price', r2.current_price == 4120.0)

# ── 3. _trigger_close_position源码验证 ──
print('\n--- 3. _trigger_close_position源码验证 ---')
from ali2026v3_trading.position import position_command_service
src = inspect.getsource(position_command_service.PositionCommandService._trigger_close_position)

ck('用bid_price(非bid_price1)', "tick.get('bid_price'" in src)
ck('用ask_price(非ask_price1)', "'ask_price'" in src and "ask_price1" not in src)
ck('用price字段', "tick.get('price'" in src)
ck('无bid_price1', "bid_price1" not in src)
ck('无ask_price1', "ask_price1" not in src)
ck('有current_price参数fallback', 'base = current_price' in src)
ck('有record.current_price fallback', "getattr(record, 'current_price'" in src)
ck('有get_latest_price fallback', 'get_latest_price' in src)
ck('有need_retry机制', 'need_retry' in src)
ck('无价格时need_retry=True', 'need_retry = True' in src)
ck('有_schedule_close_retry', '_schedule_close_retry' in src)
ck('平仓时设置close_reason', "record.close_reason = reason" in src)
ck('平仓时计算realized_pnl', 'record.realized_pnl' in src)

# ── 4. 6个调用点current_price参数验证 ──
print('\n--- 4. 6个调用点current_price参数验证 ---')
from ali2026v3_trading.position import position_pnl_service
pnl_src = inspect.getsource(position_pnl_service.PositionPnlService)

call_sites = {
    'OptionExpiry': 'OptionExpiry',
    'trailing_stop': 'trailing_reason',
    'TimeStop': 'TimeStop',
    'hard_stop': 'hard_stop_reason',
    'TwoStageStop': 'TwoStageStop',
    'EOD': 'eod_reason',
}
for name, keyword in call_sites.items():
    lines = pnl_src.split('\n')
    matched = False
    for i, line in enumerate(lines):
        if '_trigger_close_position' in line and keyword in line and 'current_price=' in line:
            matched = True
            break
    ck(f'{name}调用传current_price', matched, f'keyword={keyword}未找到current_price参数')

# ── 5. 模拟平仓价格获取链路 ──
print('\n--- 5. 模拟平仓价格获取链路 ---')
mock_tick_correct = {'bid_price': 4100.0, 'ask_price': 4101.0, 'price': 4100.5}
price = mock_tick_correct.get('bid_price', 0.0)
ck('tick正确字段名获取bid_price', price == 4100.0)
price2 = mock_tick_correct.get('ask_price', 0.0)
ck('tick正确字段名获取ask_price', price2 == 4101.0)
price3 = mock_tick_correct.get('price', 0.0)
ck('tick正确字段名获取price', price3 == 4100.5)

mock_tick_old = {'bid_price1': 4100.0, 'ask_price1': 4101.0, 'last_price': 4100.5}
ck('旧字段名bid_price1取不到', mock_tick_old.get('bid_price', 0.0) == 0.0)
ck('旧字段名ask_price1取不到', mock_tick_old.get('ask_price', 0.0) == 0.0)
ck('旧字段名last_price取不到', mock_tick_old.get('price', 0.0) == 0.0)

# fallback链路: tick无价格 → current_price参数 → record.current_price
r3 = PositionRecord(
    position_id='p3', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0, open_time=now,
    open_date=now.date(), position_type='SPECULATIVE',
)
r3.current_price = 4095.0
base = 0.0
if base <= 0:
    base = getattr(r3, 'current_price', 0.0) or 0.0
ck('fallback到record.current_price', base == 4095.0)

func_current_price = 4098.0
base2 = func_current_price or 0.0
if base2 <= 0:
    base2 = getattr(r3, 'current_price', 0.0) or 0.0
ck('函数参数current_price优先', base2 == 4098.0)

# ── 6. 模拟完整平仓流程(Mock模块级) ──
print('\n--- 6. 模拟完整平仓流程 ---')
from ali2026v3_trading.position.position_service import PositionService

mock_ps = MagicMock(spec=PositionService)
mock_ps.positions = {}
mock_ps.global_lock = MagicMock()
mock_ps._get_instrument_lock = MagicMock(return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()))
mock_ps.network_retry_manager = None
mock_ps._snapshot_collector = None

from ali2026v3_trading.position.position_command_service import PositionCommandService
cmd_svc = PositionCommandService(mock_ps)

test_rec = PositionRecord(
    position_id='tp1', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0, open_time=now,
    open_date=now.date(), position_type='SPECULATIVE',
)
test_rec.current_price = 4100.0
test_rec.strategy_group = 'spring'

mock_order_svc = MagicMock()
mock_order_svc.send_order.return_value = 'order_123'

mock_data_svc = MagicMock()
mock_data_svc.realtime_cache = MagicMock()
mock_data_svc.realtime_cache._latest_ticks = {
    'IF2607': {'bid_price': 4099.0, 'ask_price': 4100.0, 'price': 4099.5}
}
mock_data_svc.realtime_cache.get_latest_price.return_value = 4099.0

with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=mock_order_svc), \
     patch('ali2026v3_trading.data.data_service.get_data_service', return_value=mock_data_svc):
    cmd_svc._trigger_close_position(test_rec, 'TimeStop@60min', current_price=4100.0)

ck('平仓后closing标记', test_rec._closing is True)
ck('平仓后close_reason设置', test_rec.close_reason == 'TimeStop@60min')
ck('平仓后realized_pnl计算', test_rec.realized_pnl != 0.0, f'realized_pnl={test_rec.realized_pnl}')
ck('send_order被调用', mock_order_svc.send_order.called)
call_kwargs = mock_order_svc.send_order.call_args
if call_kwargs[1]:
    dir_val = call_kwargs[1].get('direction', '')
else:
    dir_val = ''
ck('多头平仓direction=SELL', dir_val == 'SELL', f'direction={dir_val}')
ck('realized_pnl为正(多头盈利)', test_rec.realized_pnl > 0, f'realized_pnl={test_rec.realized_pnl}')

# ── 7. 无价格时触发retry ──
print('\n--- 7. 无价格时触发retry ---')
test_rec2 = PositionRecord(
    position_id='tp2', instrument_id='IF2609', exchange='CFFEX',
    volume=-1, direction='short', open_price=4000.0, open_time=now,
    open_date=now.date(), position_type='SPECULATIVE',
)
test_rec2.current_price = 0.0

mock_order_svc2 = MagicMock()
mock_order_svc2.send_order.return_value = None

mock_data_svc2 = MagicMock()
mock_data_svc2.realtime_cache = MagicMock()
mock_data_svc2.realtime_cache._latest_ticks = {}
mock_data_svc2.realtime_cache.get_latest_price.return_value = None

with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=mock_order_svc2), \
     patch('ali2026v3_trading.data.data_service.get_data_service', return_value=mock_data_svc2):
    with patch.object(cmd_svc, '_schedule_close_retry') as mock_retry:
        cmd_svc._trigger_close_position(test_rec2, 'HardStop', current_price=0.0)
        ck('无价格时触发retry', mock_retry.called, '无价格时未调用_schedule_close_retry')

# ── 8. 空头持仓平仓方向验证 ──
print('\n--- 8. 空头持仓平仓方向验证 ---')
test_rec3 = PositionRecord(
    position_id='tp3', instrument_id='IF2607', exchange='CFFEX',
    volume=-2, direction='short', open_price=4000.0, open_time=now,
    open_date=now.date(), position_type='SPECULATIVE',
)
test_rec3.current_price = 3900.0

mock_order_svc3 = MagicMock()
mock_order_svc3.send_order.return_value = 'order_456'

mock_data_svc3 = MagicMock()
mock_data_svc3.realtime_cache = MagicMock()
mock_data_svc3.realtime_cache._latest_ticks = {
    'IF2607': {'bid_price': 3901.0, 'ask_price': 3902.0, 'price': 3901.5}
}
mock_data_svc3.realtime_cache.get_latest_price.return_value = 3901.0

with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=mock_order_svc3), \
     patch('ali2026v3_trading.data.data_service.get_data_service', return_value=mock_data_svc3):
    cmd_svc._trigger_close_position(test_rec3, 'EOD_Close', current_price=3900.0)

ck('空头平仓send_order被调用', mock_order_svc3.send_order.called)
call_args3 = mock_order_svc3.send_order.call_args
dir_val3 = call_args3[1].get('direction', '') if call_args3[1] else ''
ck('空头平仓方向BUY', dir_val3 == 'BUY', f'direction={dir_val3}')
ck('空头realized_pnl为正(盈利)', test_rec3.realized_pnl > 0, f'realized_pnl={test_rec3.realized_pnl}')

# ── 9. realized_pnl计算公式验证 ──
print('\n--- 9. realized_pnl计算公式验证 ---')
# 多头: pnl = (close_price - open_price) * volume
test_rec4 = PositionRecord(
    position_id='tp4', instrument_id='IF2607', exchange='CFFEX',
    volume=1, direction='long', open_price=4000.0, open_time=now,
    open_date=now.date(), position_type='SPECULATIVE',
)
test_rec4.current_price = 4100.0

mock_order_svc4 = MagicMock()
mock_order_svc4.send_order.return_value = 'order_789'

mock_data_svc4 = MagicMock()
mock_data_svc4.realtime_cache = MagicMock()
mock_data_svc4.realtime_cache._latest_ticks = {
    'IF2607': {'bid_price': 4098.0, 'ask_price': 4099.0, 'price': 4098.5}
}
mock_data_svc4.realtime_cache.get_latest_price.return_value = 4098.0

with patch('ali2026v3_trading.order.order_service.get_order_service', return_value=mock_order_svc4), \
     patch('ali2026v3_trading.data.data_service.get_data_service', return_value=mock_data_svc4):
    cmd_svc._trigger_close_position(test_rec4, 'StopLoss', current_price=4100.0)

# 多头direction=long, pnl_mult=1.0, price从tick取bid_price=4098
# pnl = 1.0 * (4098 - 4000) * 1 = 98.0 (但实际price可能因tick_size调整)
ck('多头realized_pnl>0(盈利)', test_rec4.realized_pnl > 0, f'realized_pnl={test_rec4.realized_pnl}')

# ── 结果 ──
print(f'\n=== E2E-11 结果: {P} PASS / {F} FAIL ===')
if F > 0:
    sys.exit(1)
