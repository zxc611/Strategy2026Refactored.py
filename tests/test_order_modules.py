# MODULE_ID: M2-428
"""
订单模块单元测试
覆盖: OrderStateManager, OrderWALStateService, OrderFlowBridge
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, call

import pytest


# ============================================================
# OrderStateManager 测试
# ============================================================

class TestOrderStateManager:
    """订单状态管理器测试"""

    def test_add_and_get_pending(self):
        """添加订单后能通过 get_pending 取回"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        order_info = {'instrument_id': 'al2605', 'direction': 'BUY', 'volume': 10}
        mgr.add_pending('ORD001', order_info)
        result = mgr.get_pending('ORD001')
        assert result is not None
        assert result['instrument_id'] == 'al2605'
        assert result['direction'] == 'BUY'
        assert result['volume'] == 10

    def test_remove_pending(self):
        """添加后删除，返回被删除的订单且 get_pending 返回 None"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        order_info = {'instrument_id': 'al2605', 'direction': 'BUY'}
        mgr.add_pending('ORD001', order_info)
        removed = mgr.remove_pending('ORD001')
        assert removed is not None
        assert removed['instrument_id'] == 'al2605'
        assert mgr.get_pending('ORD001') is None

    def test_remove_nonexistent(self):
        """删除不存在的订单返回 None"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        result = mgr.remove_pending('NONEXIST')
        assert result is None

    def test_get_nonexistent(self):
        """查询不存在的订单返回 None"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        result = mgr.get_pending('NONEXIST')
        assert result is None

    def test_pending_count(self):
        """添加3个订单后 pending_count 为3"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        for i in range(3):
            mgr.add_pending(f'ORD{i:03d}', {'idx': i})
        assert mgr.pending_count == 3

    def test_pending_count_after_remove(self):
        """添加3个订单后删除1个，pending_count 为2"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        for i in range(3):
            mgr.add_pending(f'ORD{i:03d}', {'idx': i})
        mgr.remove_pending('ORD001')
        assert mgr.pending_count == 2

    def test_scan_timeouts_no_timeout(self):
        """刚添加的订单不会超时"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        mgr.add_pending('ORD001', {'instrument_id': 'al2605'})
        # 刚添加，默认30秒超时内不会超时
        result = mgr.scan_timeouts()
        assert result == []

    def test_scan_timeouts_with_timeout(self):
        """手动设置时间戳为过去，scan_timeouts 返回超时订单ID"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        mgr.add_pending('ORD001', {'instrument_id': 'al2605'})
        # 手动将时间戳设为60秒前
        mgr._order_timestamps['ORD001'] = time.time() - 60
        result = mgr.scan_timeouts()
        assert 'ORD001' in result

    def test_scan_timeouts_custom_timeout(self):
        """使用自定义超时参数检测超时"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        mgr.add_pending('ORD001', {'instrument_id': 'al2605'})
        # 将时间戳设为5秒前
        mgr._order_timestamps['ORD001'] = time.time() - 5
        # 默认30秒超时不会超时
        assert mgr.scan_timeouts() == []
        # 使用3秒自定义超时，应超时
        result = mgr.scan_timeouts(timeout_sec=3.0)
        assert 'ORD001' in result

    def test_check_pending_orders_same_as_scan_timeouts(self):
        """check_pending_orders 委托给 scan_timeouts"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        mgr.add_pending('ORD001', {'instrument_id': 'al2605'})
        mgr._order_timestamps['ORD001'] = time.time() - 60
        # 两个方法应返回相同结果
        assert mgr.check_pending_orders() == mgr.scan_timeouts()

    def test_max_pending_default(self):
        """默认 max_pending 为 1000"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        assert mgr._max_pending == 1000

    def test_concurrent_access(self):
        """多线程并发添加/删除不会导致数据损坏"""
        from ali2026v3_trading.order.order_state_manager import OrderStateManager
        mgr = OrderStateManager()
        errors = []

        def add_orders(start, count):
            try:
                for i in range(start, start + count):
                    mgr.add_pending(f'THRD_{i}', {'idx': i})
            except Exception as e:
                errors.append(e)

        def remove_orders(start, count):
            try:
                for i in range(start, start + count):
                    mgr.remove_pending(f'THRD_{i}')
            except Exception as e:
                errors.append(e)

        # 先添加一批
        threads_add = [threading.Thread(target=add_orders, args=(i * 100, 100))
                       for i in range(5)]
        for t in threads_add:
            t.start()
        for t in threads_add:
            t.join()

        assert mgr.pending_count == 500
        assert errors == []

        # 并发删除
        threads_rm = [threading.Thread(target=remove_orders, args=(i * 100, 100))
                      for i in range(5)]
        for t in threads_rm:
            t.start()
        for t in threads_rm:
            t.join()

        assert mgr.pending_count == 0
        assert errors == []


# ============================================================
# OrderWALStateService 测试
# ============================================================

def _make_wal_provider(tmp_path):
    """构造WAL测试用的 mock provider"""
    provider = MagicMock()
    provider._wal_dir = str(tmp_path / "wal")
    provider._idempotent_state_file = str(tmp_path / "idempotent.jsonl")
    provider._order_state_file = str(tmp_path / "order_state.jsonl")
    provider._lock = threading.Lock()
    provider._idempotent_lock = threading.Lock()
    provider._order_state_lock = threading.Lock()
    provider._orders_by_id = {}
    provider._order_idempotent_set = set()
    return provider


class TestOrderWALStateService:
    """WAL状态服务测试"""

    def test_wal_path_uses_sanitize_filename(self, tmp_path):
        """_wal_path 调用 sanitize_filename 处理 order_id"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.sanitize_filename',
                   return_value='safe_id') as mock_sanitize, \
             patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider
            result = svc._wal_path('order/with\\slashes')
            mock_sanitize.assert_called_once_with('order/with\\slashes')
            assert result == os.path.join(provider._wal_dir, 'safe_id.wal')

    def test_wal_write_and_read(self, tmp_path):
        """写入WAL条目后能正确读回"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider

            order = {'instrument_id': 'al2605', 'direction': 'BUY', 'volume': 10, 'price': 20000.0}
            svc._ensure_wal_dir()
            svc._wal_write('ORD001', 'PENDING', order)
            result = svc._wal_read('ORD001')
            assert result is not None
            assert result['order_id'] == 'ORD001'
            assert result['state'] == 'PENDING'
            assert result['instrument_id'] == 'al2605'
            assert result['direction'] == 'BUY'
            assert result['volume'] == 10
            assert result['price'] == 20000.0

    def test_wal_read_nonexistent(self, tmp_path):
        """读取不存在的WAL文件返回 None"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider
            svc._ensure_wal_dir()
            result = svc._wal_read('NONEXIST')
            assert result is None

    def test_wal_delete(self, tmp_path):
        """写入后删除，文件不存在"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider

            order = {'instrument_id': 'al2605', 'direction': 'BUY', 'volume': 10, 'price': 20000.0}
            svc._ensure_wal_dir()
            svc._wal_write('ORD001', 'PENDING', order)
            # 确认文件存在
            assert os.path.exists(svc._wal_path('ORD001'))
            # 删除
            svc._wal_delete('ORD001')
            assert not os.path.exists(svc._wal_path('ORD001'))

    def test_wal_delete_nonexistent(self, tmp_path):
        """删除不存在的文件不抛异常"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider
            svc._ensure_wal_dir()
            # 不应抛异常
            svc._wal_delete('NONEXIST')

    def test_ensure_wal_dir(self, tmp_path):
        """验证WAL目录被创建"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider
            assert not os.path.exists(provider._wal_dir)
            svc._ensure_wal_dir()
            assert os.path.isdir(provider._wal_dir)

    def test_persist_idempotent_key(self, tmp_path):
        """写入幂等键后文件包含该键"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider

            svc._persist_idempotent_key('al2605_BUY_OPEN_10_20000.0')
            # 读取文件验证
            with open(provider._idempotent_state_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            record = json.loads(content)
            assert record['key'] == 'al2605_BUY_OPEN_10_20000.0'
            assert 'ts' in record

    def test_recover_idempotent_state(self, tmp_path):
        """从文件恢复幂等键到 provider 的集合中"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider

            # 手动写入多条幂等键记录
            with open(provider._idempotent_state_file, 'w', encoding='utf-8') as f:
                f.write(json.dumps({'key': 'KEY_A', 'ts': time.time()}) + '\n')
                f.write(json.dumps({'key': 'KEY_B', 'ts': time.time()}) + '\n')
                f.write(json.dumps({'key': 'KEY_C', 'ts': time.time()}) + '\n')

            svc._recover_idempotent_state()
            assert 'KEY_A' in provider._order_idempotent_set
            assert 'KEY_B' in provider._order_idempotent_set
            assert 'KEY_C' in provider._order_idempotent_set

    def test_append_order_state(self, tmp_path):
        """追加订单状态记录后文件包含该记录"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None), \
             patch('ali2026v3_trading.order.order_wal_state_service.is_disk_full_error',
                   return_value=False):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider

            order = {'instrument_id': 'al2605', 'direction': 'BUY', 'volume': 10, 'price': 20000.0}
            svc._append_order_state('ORD001', 'SUBMITTED', order)

            with open(provider._order_state_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            record = json.loads(content)
            assert record['order_id'] == 'ORD001'
            assert record['state'] == 'SUBMITTED'
            assert record['instrument_id'] == 'al2605'

    def test_recover_order_state(self, tmp_path):
        """从JSONL文件恢复订单状态到 provider 的 orders_by_id"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider

            # 手动写入订单状态记录
            with open(provider._order_state_file, 'w', encoding='utf-8') as f:
                f.write(json.dumps({
                    'order_id': 'ORD001', 'state': 'SUBMITTED',
                    'instrument_id': 'al2605', 'direction': 'BUY',
                    'volume': 10, 'price': 20000.0, 'ts': time.time(),
                }) + '\n')
                f.write(json.dumps({
                    'order_id': 'ORD002', 'state': 'FILLED',
                    'instrument_id': 'cu2606', 'direction': 'SELL',
                    'volume': 5, 'price': 80000.0, 'ts': time.time(),
                }) + '\n')

            svc._recover_order_state()
            assert 'ORD001' in provider._orders_by_id
            assert provider._orders_by_id['ORD001']['status'] == 'SUBMITTED'
            assert provider._orders_by_id['ORD001']['instrument_id'] == 'al2605'
            assert 'ORD002' in provider._orders_by_id
            assert provider._orders_by_id['ORD002']['status'] == 'FILLED'

    def test_execute_with_compensation_v2_all_success(self, tmp_path):
        """所有步骤成功时返回所有 order_id"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider

            # 模拟 send_order 返回带 order_id 的结果
            result1 = MagicMock()
            result1.order_id = 'ORD001'
            result2 = MagicMock()
            result2.order_id = 'ORD002'
            provider.send_order.side_effect = [result1, result2]

            steps = [
                {'instrument_id': 'al2605', 'direction': 'BUY', 'volume': 10},
                {'instrument_id': 'cu2606', 'direction': 'SELL', 'volume': 5},
            ]
            result_ids = []
            executed = svc._execute_with_compensation_v2(steps, result_ids)
            assert executed == ['ORD001', 'ORD002']
            assert result_ids == ['ORD001', 'ORD002']

    def test_execute_with_compensation_v2_partial_failure(self, tmp_path):
        """步骤2失败时对步骤1执行补偿"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider

            # 步骤1成功，步骤2失败（无order_id）
            result1 = MagicMock()
            result1.order_id = 'ORD001'
            result2 = MagicMock()
            result2.order_id = ''  # 失败
            provider.send_order.side_effect = [result1, result2]
            provider._orders_by_id = {'ORD001': {'status': 'SUBMITTED', 'instrument_id': 'al2605'}}

            compensate_fn = MagicMock()
            steps = [
                {'instrument_id': 'al2605', 'direction': 'BUY', 'volume': 10},
                {'instrument_id': 'cu2606', 'direction': 'SELL', 'volume': 5},
            ]
            result_ids = []
            # patch _append_order_state 避免文件操作问题
            with patch.object(svc, '_append_order_state'):
                executed = svc._execute_with_compensation_v2(steps, result_ids, compensate_fn=compensate_fn)
            # 补偿函数应对ORD001调用
            compensate_fn.assert_called_once_with('ORD001')

    def test_remove_order_and_idempotent_key(self, tmp_path):
        """移除订单和对应的幂等键"""
        provider = _make_wal_provider(tmp_path)
        with patch('ali2026v3_trading.order.order_wal_state_service.OrderWALStateService.__init__',
                   return_value=None):
            from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
            svc = OrderWALStateService.__new__(OrderWALStateService)
            svc._provider = provider

            # 构造幂等键与订单
            idempotent_key = 'al2605_BUY_OPEN_10_20000.0'
            provider._order_idempotent_set = {idempotent_key}
            provider._orders_by_id = {
                'ORD001': {
                    'instrument_id': 'al2605', 'direction': 'BUY',
                    'action': 'OPEN', 'volume': 10, 'price': 20000.0,
                }
            }

            svc.remove_order_and_idempotent_key(provider, 'ORD001',
                                                 provider._orders_by_id.get('ORD001', {}))
            assert idempotent_key not in provider._order_idempotent_set
            assert 'ORD001' not in provider._orders_by_id

    def test_constants(self):
        """验证常量值"""
        from ali2026v3_trading.order.order_wal_state_service import OrderWALStateService
        assert OrderWALStateService._ORDER_STATE_MAX_BYTES == 50 * 1024 * 1024
        assert OrderWALStateService._ORDER_STATE_BACKUP_COUNT == 3


# ============================================================
# OrderFlowBridge 测试
# ============================================================

# 需要mock的order_flow_analyzer依赖
_MOCK_PATCHES = [
    'ali2026v3_trading.order.order_flow_bridge_service.MicrostructureAnalyzer',
    'ali2026v3_trading.order.order_flow_bridge_service.MicrostructureConfig',
    'ali2026v3_trading.order.order_flow_bridge_service.VolumeWeightedOrderFlow',
    'ali2026v3_trading.order.order_flow_bridge_service.LiquidityConsumptionTracker',
]


class TestOrderFlowBridge:
    """订单流桥接服务测试"""

    @patch(_MOCK_PATCHES[3], MagicMock())
    @patch(_MOCK_PATCHES[2], MagicMock())
    @patch(_MOCK_PATCHES[1], MagicMock())
    @patch(_MOCK_PATCHES[0], MagicMock())
    def test_init_creates_bridge(self):
        """OrderFlowBridge 可正常创建"""
        from ali2026v3_trading.order.order_flow_bridge import OrderFlowBridge
        bridge = OrderFlowBridge()
        assert bridge is not None
        assert hasattr(bridge, '_analyzer')
        assert hasattr(bridge, '_stats')

    @patch(_MOCK_PATCHES[3], MagicMock())
    @patch(_MOCK_PATCHES[2], MagicMock())
    @patch(_MOCK_PATCHES[1], MagicMock())
    @patch(_MOCK_PATCHES[0], MagicMock())
    def test_extract_product(self):
        """_extract_product 从 instrument_id 提取品种代码"""
        from ali2026v3_trading.order.order_flow_bridge import OrderFlowBridge
        bridge = OrderFlowBridge()
        # extract_product_code 在 _extract_product 方法内通过 from...import 导入
        # 需要patch ali2026v3_trading.infra.shared_utils.extract_product_code
        with patch('ali2026v3_trading.infra.shared_utils.extract_product_code',
                   return_value='al'):
            result = bridge._extract_product('al2605')
            assert result == 'al'
            # 第二次应走缓存
            result2 = bridge._extract_product('al2605')
            assert result2 == 'al'

    @patch(_MOCK_PATCHES[3], MagicMock())
    @patch(_MOCK_PATCHES[2], MagicMock())
    @patch(_MOCK_PATCHES[1], MagicMock())
    @patch(_MOCK_PATCHES[0], MagicMock())
    def test_get_stats_returns_dict(self):
        """get_stats 返回包含预期键的字典"""
        from ali2026v3_trading.order.order_flow_bridge import OrderFlowBridge
        bridge = OrderFlowBridge()
        stats = bridge.get_stats()
        assert isinstance(stats, dict)
        assert 'ticks_fed' in stats
        assert 'depth_updates' in stats
        assert 'direction_buy' in stats
        assert 'direction_sell' in stats
        assert 'cache_hits' in stats
        assert 'cache_misses' in stats
        assert 'products_tracked' in stats
        assert 'product_cache_size' in stats
        assert 'flow_cache_size' in stats
        assert 'depth_quality' in stats

    @patch(_MOCK_PATCHES[3], MagicMock())
    @patch(_MOCK_PATCHES[2], MagicMock())
    @patch(_MOCK_PATCHES[1], MagicMock())
    @patch(_MOCK_PATCHES[0], MagicMock())
    def test_get_products_returns_list(self):
        """get_products 返回列表"""
        from ali2026v3_trading.order.order_flow_bridge import OrderFlowBridge
        bridge = OrderFlowBridge()
        bridge._analyzer.get_products.return_value = ['al', 'cu']
        result = bridge.get_products()
        assert isinstance(result, list)
        assert 'al' in result
        assert 'cu' in result

    @patch(_MOCK_PATCHES[3], MagicMock())
    @patch(_MOCK_PATCHES[2], MagicMock())
    @patch(_MOCK_PATCHES[1], MagicMock())
    @patch(_MOCK_PATCHES[0], MagicMock())
    def test_on_tick_feed_updates_stats(self):
        """喂入tick后 ticks_fed 计数增加"""
        from ali2026v3_trading.order.order_flow_bridge import OrderFlowBridge
        bridge = OrderFlowBridge()
        # 设置前置状态：需要prev_volume和prev_timestamp以触发delta_vol > 0
        bridge._prev_volume['al2605'] = 0
        bridge._prev_timestamp['al2605'] = time.time() - 1

        with patch.object(bridge, '_extract_product', return_value='al'):
            bridge.on_tick_feed(
                instrument_id='al2605',
                price=20000.0,
                volume=100,
                bid_price=19999.0,
                ask_price=20001.0,
            )
        # 验证 ticks_fed 增加
        assert bridge._stats['ticks_fed'] >= 1

    @patch(_MOCK_PATCHES[3], MagicMock())
    @patch(_MOCK_PATCHES[2], MagicMock())
    @patch(_MOCK_PATCHES[1], MagicMock())
    @patch(_MOCK_PATCHES[0], MagicMock())
    def test_default_depth_volume(self):
        """DEFAULT_DEPTH_VOLUME 常量为 50"""
        from ali2026v3_trading.order.order_flow_bridge import OrderFlowBridge
        assert OrderFlowBridge.DEFAULT_DEPTH_VOLUME == 50
