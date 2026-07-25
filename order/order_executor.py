# [M1-46] 订单执行器 - 组合编排模式 (V2.0 重构)
# MODULE_ID: M1-46
# _INTERNAL: 本模块为子系统内部实现，外部请通过 __init__.py 的公共API访问

"""
OrderExecutor — 订单执行器 (组合编排层)

V2.0 重构: Mixin 组合模式 → 组合 + 策略模式
- 编排层 (OrderExecutor) 持有 _backend: OrderExecutionBackend
- 后端 (Live/DryRun/Backtest) 可独立替换
- 业务方法 (send_order_split / execute_by_ranking) 保留在此层
- _post_send_persist 共享编排方法保留在此层

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第三节 3.5
迁移映射: 第四部分 项 1, 2, 8, 9, 10, 11
"""

from __future__ import annotations

import logging
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from infra.shared_utils import CHINA_TZ, TradeDirection, VALID_TRADE_DIRECTIONS
from order.order_base import OrderResult
from order.order_executor_validation import _OrderExecutorBase, OrderContext
from order.execution_backend_base import OrderExecutionBackend


def _place_hft_take_profit(
    svc: Any,
    instrument_id: str,
    open_direction: str,
    open_price: float,
    volume: float,
    tick_size: float,
) -> float:
    """HFT开仓成功后挂一个最小变动单位的止盈单

    核心逻辑:
    - 买入开仓 → 止盈卖价 = 开仓价 + 1个tick_size
    - 卖出开仓 → 止盈买价 = 开仓价 - 1个tick_size
    - 止盈方向与开仓方向相反, action=CLOSE
    - tick_size从品种ID自带属性中读取(一个生命周期仅定一次)

    Args:
        svc: OrderService实例
        instrument_id: 合约ID
        open_direction: 开仓方向 'BUY'/'SELL'
        open_price: 开仓成交价
        volume: 开仓手数
        tick_size: 最小变动价位(从instrument_meta.tick_size读取)
    Returns:
        止盈单价格, 0表示未下单
    """
    if tick_size <= 0:
        return 0.0

    # 确定止盈方向和价格
    if open_direction == TradeDirection.BUY:
        # 买入开仓 → 卖出止盈(赚一个tick)
        tp_direction = TradeDirection.SELL
        tp_price = open_price + tick_size
    elif open_direction == TradeDirection.SELL:
        # 卖出开仓 → 买入止盈(赚一个tick)
        tp_direction = TradeDirection.BUY
        tp_price = max(tick_size, open_price - tick_size)
    else:
        return 0.0

    # 价格校正
    tp_price = svc._correct_price(tp_price, instrument_id)

    # 发送止盈单(CLOSE动作), 最多重试3次
    # FIX-HFT-TP-RETRY-20260724: 止盈单失败需重试,连续失败触发告警
    last_error = None
    for attempt in range(1, 4):
        tp_result = svc.send_order(
            instrument_id=instrument_id,
            volume=volume,
            price=tp_price,
            direction=tp_direction,
            action='CLOSE',
            signal_id='HFT_TP',
            open_reason='hft_take_profit_1tick',
        )

        if tp_result and tp_result.success:
            return tp_price

        last_error = getattr(tp_result, 'error_message', None) or getattr(tp_result, 'error_code', None) or 'unknown'
        if attempt < 3:
            logging.debug(
                "[HFT-TP] 止盈单第%d次尝试失败, %.1fms后重试: %s err=%s",
                attempt, 10.0 * attempt, instrument_id, last_error
            )
            time.sleep(0.01 * attempt)

    # 三次均失败,触发告警(不影响已开仓订单)
    logging.warning(
        "[HFT-TP-ALERT] 止盈单连续3次失败: %s %s @%.2f volume=%s err=%s",
        instrument_id, tp_direction, tp_price, volume, last_error
    )
    return 0.0


class OrderExecutor(_OrderExecutorBase):
    """订单执行器 — 组合模式: 编排 + 后端委托

    继承 _OrderExecutorBase 保留 _pre_send_checks (共享前置校验)
    新增 _backend 字段持有执行后端 (策略模式注入)

    业务方法 (send_order_split / execute_by_ranking / _post_send_persist) 保留在此层，
    迁移自 _OrderExecutorPlatformMethods (P4 阶段)
    """

    def __init__(self, order_service: Any, backend: OrderExecutionBackend = None):
        super().__init__(order_service)
        # 后端路由: 优先级 _backtest_mode > _dry_run_mode > live
        self._backend: OrderExecutionBackend = backend or self._resolve_default_backend(order_service)
        # 双向绑定: backend 持有 orchestrator + svc
        self._backend.bind_orchestrator(self)
        self._backend.bind_service(order_service)

    def _resolve_default_backend(self, svc: Any) -> OrderExecutionBackend:
        """后端路由 — _backtest_mode > _dry_run_mode > live

        决策点 4 (用户已确认): OrderService 持有 _executor 单例
        决策点 5 (用户已确认): _backtest_mode 由 enter/exit_backtest_mode 管理
        """
        # 优先级 1: 回测模式 (新增属性，由 enter_backtest_mode 设置)
        if getattr(svc, '_backtest_mode', False):
            from order.backends.backtest_backend import BacktestExecutionBackend
            _ctx = getattr(svc, '_backtest_context', None)
            if _ctx is not None:
                return BacktestExecutionBackend(context=_ctx)
            # 回测模式但未注入 context，降级为 dry_run
            logging.warning("[OrderExecutor] _backtest_mode=True but _backtest_context=None, fallback to DryRun")

        # 优先级 2: dry_run 模式
        _dry_run = getattr(svc, '_dry_run_mode', False)
        if not isinstance(_dry_run, bool):
            _dry_run = bool(_dry_run)
        if _dry_run:
            from order.backends.dry_run_backend import DryRunExecutionBackend
            return DryRunExecutionBackend()

        # 默认: 生产
        from order.backends.live_backend import LiveExecutionBackend
        return LiveExecutionBackend()

    def execute(self, ctx: OrderContext) -> OrderResult:
        """编排入口 — 三阶段流水线

        阶段1: _pre_send_checks (前置校验，_OrderExecutorBase)
        阶段2: _backend.execute (委托给后端)
        阶段3: _post_send_persist (由后端内部调用)

        迁移自 _OrderExecutorBase.execute (validation.py:147-186)
        改为委托后端，不再 if-else 分支
        """
        # FIX-20260707-DRY-RUN: dry_run 模式下跳过平台下单，注入虚拟成交回调
        _dry_run = self._backend.supports_dry_run()

        ctx = self._pre_send_checks(ctx, dry_run=_dry_run)

        if ctx.rejected:
            # FIX-R31-CYCLIC-LEAK: 订单被拒绝时必须调用exit
            if ctx._cyclic_guard:
                ctx._cyclic_guard.exit("order_send_order")
            return OrderResult.fail(ctx.reject_code, ctx.reject_message)

        # 委托给后端 (不再 if-else 分支)
        _result = self._backend.execute(ctx)

        # FIX-PP2 (RETURN-TYPE): 后端返回类型归一化
        # 根因: live_backend.py L85/105/132/213/267/294 在错误路径return ctx(OrderContext)
        #       而非OrderResult→调用方order_id.success抛AttributeError(6次ERROR)
        # 修复: 编排层统一归一化返回类型，确保调用方始终收到OrderResult
        if isinstance(_result, OrderContext):
            if ctx._cyclic_guard:
                ctx._cyclic_guard.exit("order_send_order")
            if getattr(_result, 'rejected', False):
                return OrderResult.fail(_result.reject_code, _result.reject_message)
            return OrderResult.ok(_result.order_id)

        return _result

    # ==================================================================
    # 共享编排方法 — 所有 Backend 通过 _orchestrator._post_send_persist 调用
    # 迁移自 _OrderExecutorPlatformMethods._post_send_persist (L453-504)
    # ==================================================================

    def _post_send_persist(self, ctx: OrderContext) -> OrderResult:
        """阶段3: 后置持久化 — EventBus通知 + 延迟打点 + 返回结果

        迁移自 _OrderExecutorPlatformMethods._post_send_persist (L453-504)
        所有 Backend 通过 _orchestrator._post_send_persist 调用
        """
        svc = self._svc

        try:
            from infra.event_bus import get_global_event_bus
            _bus = get_global_event_bus()
            if _bus is not None:
                _bus.publish('order.submitted', {
                    'order_id': ctx.order_id, 'instrument_id': ctx.instrument_id,
                    'direction': ctx.direction, 'action': ctx.action,
                    'volume': ctx.volume, 'price': ctx.price,
                }, async_mode=True)
        except Exception as _r3_err:
            # 实时回调路径硬约束: except Exception (扩展自原窄异常元组)
            logging.debug("[R3-L2] suppressed exception", exc_info=True)

        _delay_ms = (time.perf_counter() - ctx._order_submit_start_ts) * 1000.0

        if _delay_ms > 10.0:
            _sig02_log_key = f'_sig02_delay_logged_{ctx.instrument_id}'
            _sig02_last_ts = getattr(svc, _sig02_log_key, 0)
            if (time.time() - _sig02_last_ts) > 300:
                logging.warning('[SIG-02] 订单提交延迟: %.1fms instrument=%s', _delay_ms, ctx.instrument_id)
                setattr(svc, _sig02_log_key, time.time())

        if ctx._cyclic_guard:
            ctx._cyclic_guard.exit("order_send_order")

        return OrderResult.ok(ctx.order_id)

    # ==================================================================
    # 业务方法 — 迁移自 _OrderExecutorPlatformMethods (L508-860)
    # ==================================================================

    def send_order_split(self, instrument_id: str, volume: float, price: float,
                         direction: str = 'BUY', action: str = 'OPEN', exchange: str = '',
                         signal_strength: float = 1.0, bids: Optional[List] = None,
                         asks: Optional[List] = None, open_reason: str = '',
                         signal_id: str = '') -> List[str]:
        """拆单编排 — 迁移自 _OrderExecutorPlatformMethods.send_order_split (L508-633)"""
        from order.order_compliance import check_self_trade_across_splits
        svc = self._svc

        if direction not in VALID_TRADE_DIRECTIONS:
            logging.error("[OrderService] R24-P0-IV-05: send_order_split direction必须是BUY/SELL, 实际=%s, 订单被拒: %s", direction, instrument_id)
            return []

        if not signal_id:
            logging.warning("[OrderService] R24-P1-TR-08: send_order_split signal_id为空, 订单无法追溯到信号 %s %s", instrument_id, direction)

        split_threshold = getattr(svc, '_split_volume_threshold', 5)

        if volume > split_threshold and (bids or asks):
            split_orders = self._plan_volume_split(volume, price, direction, bids, asks, signal_strength)

            if split_orders:
                violations = check_self_trade_across_splits(split_orders)
                if violations:
                    logging.warning("[OrderService] SOS-FAKE-01修复: 拆单自成交检测发现%d个违规，降级为单笔下单", len(violations))
                else:
                    executed_ids = []
                    for i, sub in enumerate(split_orders):
                        result = svc.send_order(
                            instrument_id=instrument_id,
                            volume=sub['volume'],
                            price=sub['price'],
                            direction=direction,
                            action=action,
                            exchange=exchange,
                            signal_id=f"{signal_id}_split{i}" if signal_id else '',
                            open_reason=open_reason,
                        )
                        if result and result.order_id:
                            executed_ids.append(result.order_id)
                        else:
                            logging.warning("[OrderService] SOS-FAKE-01修复: 拆单第%d笔失败 已执行%d", i + 1, len(executed_ids))
                            break
                    if executed_ids:
                        logging.info("[OrderService] SOS-FAKE-01修复: 拆单执行完成 %d/%d instrument=%s", len(executed_ids), len(split_orders), instrument_id)
                        return executed_ids
                    logging.warning("[OrderService] SOS-FAKE-01修复: 拆单全部失败，降级为单笔下单")

        order_id = svc.send_order(
            instrument_id=instrument_id,
            volume=volume,
            price=price,
            direction=direction,
            action=action,
            exchange=exchange,
            signal_id=signal_id,
            open_reason=open_reason,
        )

        if order_id:
            return [order_id.order_id]
        return []

    def _plan_volume_split(self, volume: float, price: float, direction: str,
                           bids: Optional[List], asks: Optional[List],
                           signal_strength: float = 1.0) -> List[Dict[str, Any]]:
        """拆单规划 — 迁移自 _OrderExecutorPlatformMethods._plan_volume_split (L637-713)"""
        from order.order_split_models import SmartOrderSplitter, OrderSplitStrategy

        try:
            splitter = SmartOrderSplitter(split_threshold=1, strategy=OrderSplitStrategy.AGGRESSIVE)
            result = splitter.plan_order_split(
                instrument_id='', volume=volume, direction=direction,
                signal_strength=signal_strength, bids=bids, asks=asks,
                strategy=OrderSplitStrategy.AGGRESSIVE,
            )

            if not result or not result.child_orders:
                return []

            splits = []
            for child in result.child_orders:
                if isinstance(child, dict):
                    splits.append({
                        'price': child.get('price', price),
                        'volume': int(child.get('volume', 0)),
                        'direction': direction,
                    })
                else:
                    splits.append({
                        'price': getattr(child, 'price', price),
                        'volume': int(getattr(child, 'volume', 0)),
                        'direction': direction,
                    })

            return splits if len(splits) > 1 else []
        except Exception as e:
            # 实时回调路径硬约束: except Exception (扩展自原窄异常元组)
            logging.error("[OrderService] SOS-FAKE-01修复: 拆单规划异常: %s", e)
            return []

    def execute_by_ranking(self, targets: List[Dict[str, Any]],
                          direction: str = 'BUY', action: str = 'OPEN') -> List[str]:
        """批量排序下单 — 迁移自 _OrderExecutorPlatformMethods.execute_by_ranking (L717-860)

        保留全部 FIX-OPEN-UNIQUE-07 / FIX-P1-2 / FIX-DD R10-4-7 逻辑
        """
        svc = self._svc

        if not targets:
            return []

        results = []

        # FIX-OPEN-UNIQUE-07: 同合约防重复开仓检查
        _seen_open_instruments = set()
        # FIX-P1-2: 跨周期"已尝试开仓"状态持久化
        if not hasattr(svc, '_cross_cycle_open_attempted'):
            svc._cross_cycle_open_attempted = {}
        _now_ts = time.time()
        _cross_cycle_ttl = 300  # 5分钟内不重复尝试同一合约开仓

        for target in targets:
            instrument_id = target.get('instrument_id', '')
            volume = target.get('lots', 1)
            price = target.get('price', 0)
            target_direction = target.get('direction', direction)
            target_action = target.get('action', action)
            _is_simulated = bool(target.get('simulated', False))

            if not instrument_id or price <= 0:
                continue

            # FIX-FAKE-INSTR-FILTER-20260723: 过滤假合约信号（半拉子工程双重保险）
            # 根因: S5/S6 fallback信号使用假合约ID(S5_ARB_FALLBACK/S5_ARB/S6_MM/S6_MM_FALLBACK)
            #   → execute_by_ranking不过滤capital_allocation/instrument_id → send_order创建持仓
            #   → resolve_product_exchange解析失败82次 → 持仓无法平仓（半拉子工程）
            # 修复: 在下游下单入口过滤假合约ID，与strategy_business_layer源头过滤形成双重保险
            #   正常S5/S6信号instrument_id为真实合约ID（如rb2609/CU609等），不受影响
            #   S6_MM_FALLBACK: market_making_monitor.py L1052 在instrument_id为空时设置的fallback
            if instrument_id in ('S5_ARB_FALLBACK', 'S5_ARB', 'S6_MM', 'S6_MM_FALLBACK'):
                _fake_filter_count = getattr(svc, '_fake_inst_filter_count', 0) + 1
                svc._fake_inst_filter_count = _fake_filter_count
                if _fake_filter_count <= 5 or _fake_filter_count % 100 == 0:
                    logging.info("[FIX-FAKE-INSTR-FILTER-20260723] 过滤假合约信号: inst=%s reason=%s simulated=%s (累计%d次)",
                                 instrument_id, target.get('open_reason', ''), _is_simulated, _fake_filter_count)
                continue

            # FIX-S5-FAKE-CONTRACT-20260723: 过滤模拟信号/零资金分配信号，防止进入PositionService
            # 根因: S5/S6(以及S1等)模拟信号使用capital_allocation=0.0 + simulated=True，
            #   execute_by_ranking原不检查这两个字段 → 真实合约模拟信号调用send_order创建持仓 → 进入PositionService
            #   → 造成虚假持仓/无法平仓
            # 修复: 在下游下单入口对simulated=True或capital_allocation=0.0的信号执行continue跳过，
            #   与strategy_business_layer源头过滤形成双重保险。真实交易信号capital_allocation>0且simulated=False，不受影响
            _capital_allocation_raw = target.get('capital_allocation', 1.0)
            _capital_allocation = float(_capital_allocation_raw) if _capital_allocation_raw is not None else 1.0
            if _is_simulated or _capital_allocation <= 0.0:
                _sim_filter_count = getattr(svc, '_simulated_filter_count', 0) + 1
                svc._simulated_filter_count = _sim_filter_count
                if _sim_filter_count <= 5 or _sim_filter_count % 100 == 0:
                    logging.info("[FIX-SIMULATED-FILTER-20260723] 过滤模拟信号: inst=%s reason=%s simulated=%s capital=%.2f (累计%d次)",
                                 instrument_id, target.get('open_reason', ''), _is_simulated, _capital_allocation, _sim_filter_count)
                continue

            # FIX-OPEN-UNIQUE-07: OPEN动作时检查同合约同方向是否已有未成交订单
            if target_action == 'OPEN':
                _dedup_key = (instrument_id, target_direction)
                if _dedup_key in _seen_open_instruments:
                    logging.debug("[OPEN-UNIQUE-07] execute_by_ranking同合约同方向重复开仓target跳过: inst=%s dir=%s",
                                    instrument_id, target_direction)
                    continue
                # FIX-P1-2: 跨周期去重 — 5分钟内已尝试开仓的合约不再重复
                # FIX-20260716-SIM-DEDUP: 模拟信号使用10秒TTL
                _cross_key = f"{instrument_id}_{target_direction}"
                _last_attempt_ts = svc._cross_cycle_open_attempted.get(_cross_key, 0)
                _effective_ttl = 10 if _is_simulated else _cross_cycle_ttl
                if _last_attempt_ts and (_now_ts - _last_attempt_ts) < _effective_ttl:
                    continue
                # 检查order_service中是否已有同合约同方向未成交OPEN订单
                try:
                    _has_pending_open = False
                    for _o in svc._orders_by_id.values():
                        if (_o.get('instrument_id') == instrument_id
                            and _o.get('action') == 'OPEN'
                            and _o.get('direction') == target_direction
                            and _o.get('status') in ('PENDING', 'NEW', 'ACCEPTED', 'PARTIAL_FILLED')):
                            _has_pending_open = True
                            break
                    if _has_pending_open:
                        logging.debug("[OPEN-UNIQUE-07] inst=%s dir=%s 已有未成交OPEN订单，跳过本次开仓",
                                        instrument_id, target_direction)
                        continue
                except Exception as _chk_err:
                    # 实时回调路径硬约束: except Exception
                    logging.debug("[OPEN-UNIQUE-07] 检查未成交OPEN订单异常: %s", _chk_err)
                # FIX-OPEN-UNIQUE-07-ASSERT: 断言去重key未重复添加
                assert _dedup_key not in _seen_open_instruments, \
                    f"去重key重复添加: {_dedup_key} (不应到达此处)"
                _seen_open_instruments.add(_dedup_key)
                # FIX-P1-2: 标记跨周期开仓尝试时间
                # FIX-DD R10-4-7: TTL后置到send_order成功后更新，避免失败后无法重试
                if len(svc._cross_cycle_open_attempted) > 200:
                    _expire_cutoff = _now_ts - 600  # 10分钟过期
                    svc._cross_cycle_open_attempted = {
                        k: v for k, v in svc._cross_cycle_open_attempted.items() if v > _expire_cutoff
                    }

            tick_size = svc._get_tick_size(instrument_id)

            if target_direction == TradeDirection.BUY:
                price = svc._correct_price(price + tick_size, instrument_id)
            elif target_direction == TradeDirection.SELL:
                price = svc._correct_price(max(0.01, price - tick_size), instrument_id)

            order_id = svc.send_order(
                instrument_id=instrument_id,
                volume=volume,
                price=price,
                direction=target_direction,
                action=target_action,
                signal_id=target.get('signal_id', ''),
                open_reason=target.get('open_reason', ''),
                decision_score=target.get('decision_score', 0.0),
                position_scale=target.get('position_scale', 1.0),
                decision_action=target.get('decision_action', ''),
                dimension_scores=target.get('dimension_scores'),
                dimension_weights=target.get('dimension_weights'),
            )

            if order_id and order_id.success:
                results.append(order_id.order_id)
                # FIX-DD R10-4-7: TTL后置到send_order成功后更新
                # FIX-EXEC-CLOSE-20260724: 仅在OPEN动作时更新_cross_cycle_open_attempted,
                #   CLOSE动作无_cross_key定义, 原代码会触发NameError/AttributeError并影响平仓成功率统计。
                if target_action == 'OPEN':
                    svc._cross_cycle_open_attempted[_cross_key] = _now_ts
                # HFT-TAKE-PROFIT-20260724: 高频交易开仓成功后自动挂一个最小变动单位的止盈单
                # 从品种ID中直接读取tick_size(一个生命周期仅需定一次的属性)
                if target_action == 'OPEN' and getattr(svc, '_hft_enabled', False):
                    try:
                        _tp_result = _place_hft_take_profit(
                            svc, instrument_id, target_direction, price, volume, tick_size
                        )
                        if _tp_result:
                            logging.info(
                                "[HFT-TP] 开仓止盈单已挂: %s %s @%.2f+%.4f=%.2f",
                                instrument_id, target_direction, price, tick_size, _tp_result
                            )
                    except Exception as _tp_err:
                        logging.debug("[HFT-TP] 止盈单异常(不影响开仓): %s", _tp_err)
            else:
                # FIX-R31-ERR-LOG: 修复运算符优先级问题
                if order_id is not None:
                    _err = order_id.error_code or order_id.error_message or 'unknown'
                else:
                    _err = 'unknown'
                logging.debug("[OrderService] execute_by_ranking 下单失败: %s, error=%s", instrument_id, _err)

        return results


__all__ = ['OrderExecutor', 'OrderContext']
