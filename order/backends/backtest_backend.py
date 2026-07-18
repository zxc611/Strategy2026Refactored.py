# [M1-46-BT] 回测保真度执行后端
# MODULE_ID: M1-46-BT
# _INTERNAL: 本模块为子系统内部实现

"""
BacktestExecutionBackend — 回测保真度模拟执行后端

5 阶段保真度调用序列 — 严格对齐 try_open_execute (backtest_state.py:542-643):
1. 成交量计算 (BacktestFidelity.compute_fill_quantity)
2. 限价单队列模拟 (BacktestFidelity.simulate_limit_order_queue)
3. 滑点 + 市价冲击 + 手续费 (BacktestFidelity.compute_dynamic_slippage_bps / compute_market_impact_v2 / compute_commission)
4. 持仓/平仓更新 (BacktestContext.add_position / reduce_position)
5. 订单记录 + 后置编排 (复用 _orchestrator._post_send_persist)

依赖倒置 (完整版,经 P2-1 修复):
- 状态访问: 通过 BacktestContext 协议 (param_pool/backtest/backtest_context_adapter.py 实现)
- 保真度计算: 通过 BacktestFidelity 协议 (param_pool/backtest/backtest_fidelity_adapter.py 实现)
- BacktestExecutionBackend 不再直接 import param_pool.* 任何模块

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第三节 3.4
      docs/audit/OrderExecutor_V2_交付报告批判式吸收与余下问题调查报告_20260718.md (P2-1 完整依赖倒置)

[FIX-EXEC-V2-P2-1] 修复历史:
- 原 BacktestExecutionBackend 直接 import param_pool.backtest.* 6 处 (4 模块)
- 现通过 BacktestFidelity 协议完整抽象 11 个保真度函数
- 实现 order/ 完全不依赖 param_pool/ 的目标
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from order.execution_backend_base import OrderExecutionBackend
from order.order_base import OrderResult
from order.backtest_context_protocol import BacktestContext
from order.backtest_fidelity_protocol import BacktestFidelity


class BacktestExecutionBackend(OrderExecutionBackend):
    """回测后端 — 通过协议接口复用保真度模拟

    与 param_pool/backtest/run_backtest 路径 A 共享同一套保真度函数,
    消除"双重回测路径不一致"问题 (根因 4)。

    特点:
    - supports_dry_run() 返回 True (跳过断路器/胖手指等实盘校验)
    - 5 阶段调用序列对齐 try_open_execute
    - 通过 BacktestContext 协议操作 _BacktestState (依赖倒置 - 状态)
    - 通过 BacktestFidelity 协议调用保真度函数 (依赖倒置 - 计算, P2-1 完整版)
    - 线程安全: 回测为单线程串行，无需加锁
    """

    def __init__(self, orchestrator: Any = None,
                 context: Optional[BacktestContext] = None,
                 fidelity: Optional[BacktestFidelity] = None):
        super().__init__(orchestrator=orchestrator)
        self._ctx: Optional[BacktestContext] = context
        # [FIX-EXEC-V2-P2-1] 保真度协议注入 (默认 None, 由 OrderService.enter_backtest_mode 注入)
        self._fid: Optional[BacktestFidelity] = fidelity

    def supports_dry_run(self) -> bool:
        return True  # 回测也跳过断路器/胖手指等实盘校验

    def execute(self, ctx: Any) -> OrderResult:
        """5 阶段保真度模拟 — 严格对齐 try_open_execute (backtest_state.py:542-643)"""
        if self._ctx is None:
            logging.error("[BacktestBackend] BacktestContext 未注入，拒绝执行")
            return OrderResult.fail('no_context', 'BacktestContext 未注入')

        # [FIX-EXEC-V2-P2-1] 保真度协议必须注入
        if self._fid is None:
            logging.error("[BacktestBackend] BacktestFidelity 未注入，拒绝执行")
            return OrderResult.fail('no_fidelity', 'BacktestFidelity 未注入')

        # [FIX-20260708-V4-SIG02] 补设 start_ts，确保 _post_send_persist 延迟计算正确
        if getattr(ctx, '_order_submit_start_ts', 0.0) == 0.0:
            ctx._order_submit_start_ts = time.perf_counter()

        bar = self._ctx.get_current_bar()
        params = self._ctx.get_params()

        if bar is None:
            logging.warning("[BacktestBackend] bar 为 None，跳过")
            return OrderResult.fail('no_bar', '当前 bar 为 None')

        _fid = self._fid  # 局部引用,避免重复属性查找

        # ===== 阶段 1: 成交量计算 (迁移自 backtest_state.py:561) =====
        _instrument_type = _fid.infer_instrument_type(ctx.instrument_id)
        _part_rate = self._resolve_participation_rate(params, _instrument_type)
        _adjusted_params = dict(params)
        _adjusted_params["max_participation_rate"] = _part_rate
        _req_volume = int(ctx.volume) if ctx.volume else 0
        actual_lots = _fid.compute_fill_quantity(_req_volume, bar, _adjusted_params)

        if actual_lots <= 0:
            # institutional 模式: 加入 pending_orders
            if params.get("execution_model", "standard") == "institutional" and _req_volume > 0:
                self._append_pending_order(ctx, params)
            return OrderResult.fail('no_fill', '回测成交量不足')

        # [FIX-V2-DELAY-3] institutional 模式: 即使成交量充足,
        # open_execution_delay_bars > 0 时仍应延迟开仓,对齐 legacy try_open_execute
        # (legacy 路径在成交前检查 open_delay,满足条件时创建 pending_order 并返回)
        _open_delay_bars = int(params.get("open_execution_delay_bars", 0))
        if params.get("execution_model", "standard") == "institutional" and _open_delay_bars > 0:
            self._append_pending_order(ctx, params)
            return OrderResult.fail('delayed_open', f'institutional 延迟开仓 {_open_delay_bars} bars')

        # ===== 阶段 2: 限价单队列模拟 (迁移自 backtest_state.py:564-573) =====
        _order_type = params.get("default_order_type", "taker")
        _enable_queue_sim = params.get("enable_queue_simulation", True)
        if _order_type == "maker" and _enable_queue_sim:
            _queue_result = _fid.simulate_limit_order_queue(
                order_price=ctx.price, current_price=ctx.price, bar=bar,
                order_lots=actual_lots, enable_queue=True
            )
            if not _queue_result["filled"]:
                logging.debug("[BT-P1-02] 限价单排队未成交: %s", ctx.instrument_id)
                return OrderResult.fail('queue_timeout', '限价单排队未成交')
            if _queue_result["fill_lots"] < actual_lots:
                actual_lots = _queue_result["fill_lots"]

        fill_ratio = actual_lots / _req_volume if _req_volume > 0 else 1.0

        # ===== 阶段 3: 滑点 + 市价冲击 + 手续费 (迁移自 backtest_state.py:591-621) =====
        exec_price = float(ctx.price)
        direction = 1 if (ctx.direction or 'BUY').upper() == 'BUY' else -1
        # [FIX-DOUBLE-DEDUCT] 提前判定 _is_open,用于阶段 3b-3e 守卫
        # 平仓路径跳过阶段 3b/3c/3d/3e,因为 _close_position_internal 会全权计算
        # slippage (步骤 4) / commission (步骤 5) / net_pnl (步骤 6) / 权益更新 (步骤 7)
        # 否则会导致 commission/slippage 双重扣减 (P0-3 真正根因, 评判一/二未发现)
        _is_open = (ctx.action or '').upper() in ('OPEN', '')

        # 3a. 执行延迟滑点 (来自 infra.shared_utils,非 param_pool,可保留直接 import)
        _exec_delay_ms = params.get("execution_delay_ms", 50)
        if _exec_delay_ms > 0:
            from infra.shared_utils import compute_execution_delay_slippage_bps
            _bar_high = bar.get("high", exec_price) if bar is not None else exec_price
            _bar_low = bar.get("low", exec_price) if bar is not None else exec_price
            _bar_dur = float(params.get("bar_duration_sec", 60.0))
            try:
                _delay_bps = compute_execution_delay_slippage_bps(
                    price=exec_price, bar_high=_bar_high,
                    bar_low=_bar_low,
                    bar_duration_sec=_bar_dur,
                    exec_delay_ms=_exec_delay_ms, z_score=1.0
                )
                _delay_adj = exec_price * _delay_bps / 10000.0
                exec_price += direction * _delay_adj
            except Exception as _delay_err:
                # 实时回调路径硬约束: except Exception
                logging.debug("[BacktestBackend] 延迟滑点计算异常(降级为0): %s", _delay_err)

        # [FIX-DOUBLE-DEDUCT] 阶段 3b/3c/3d/3e 仅在开仓时执行
        # 平仓路径下,_close_position_internal 会全权完成 slippage/commission/pnl/权益更新,
        # 此处若再扣减会导致 commission/slippage 双重扣减 (回测 equity 偏低)。
        # 参考实现: backtest_runner_validation.py:252-284 (平仓只扣 1 次,通过 _safe_equity_add(net_pnl))
        if _is_open:
            # 3b. 动态滑点 (按子订单拆分) — 通过 _fid 协议调用,不再直接 import param_pool
            _max_sub_lots = int(params.get("backtest_max_sub_order_lots", 5))
            _sub_orders = _fid.backtest_order_split(actual_lots, max_sub_order_lots=_max_sub_lots)
            _total_slippage_cost = 0.0
            _multiplier = _fid.get_contract_multiplier(ctx.instrument_id)
            bid_ask = bar.get("bid_ask_spread", 0.0) if bar is not None else 0.0
            spread_q = bar.get("_spread_quality", 0) if bar is not None else 0
            for _sub_lots in _sub_orders:
                _sub_slippage = _fid.compute_dynamic_slippage_bps(
                    exec_price, bid_ask, spread_quality=spread_q, bar=bar, params=params
                )
                _total_slippage_cost += _sub_lots * exec_price * _multiplier * _sub_slippage / 10000.0

            # 3c. 市价冲击 — 通过 _fid 协议调用
            _market_impact_cost = _fid.compute_market_impact_v2(actual_lots, bar, exec_price, params)

            # 3d. 手续费 — 通过 _fid 协议调用
            try:
                _exchange_id = _fid.infer_exchange_id(ctx.instrument_id)
                commission = _fid.compute_commission(
                    ctx.instrument_id, actual_lots, is_open=_is_open,
                    exchange_id=_exchange_id,
                    exchange=_fid.infer_exchange_from_id(_exchange_id),
                )
            except Exception as _comm_err:
                # 实时回调路径硬约束: except Exception
                logging.debug("[BacktestBackend] 手续费计算异常(降级为0): %s", _comm_err)
                commission = 0.0

            # 3e. 扣减权益 (开仓时一次性扣减 slippage + impact + commission)
            self._ctx.update_equity(commission + _total_slippage_cost + _market_impact_cost)

        # ===== 阶段 4: 持仓/平仓更新 =====
        bar_time = bar.get("minute") if bar is not None else None
        signed_volume = direction * actual_lots

        if _is_open:
            # 开仓: 计算 TP/SL 并 add_position — TP/SL 通过 _fid 协议调用
            tp_ratio, sl_ratio = _fid.resolve_tp_sl(params, ctx.open_reason or '')
            if signed_volume > 0:
                sp_price = exec_price * tp_ratio
                sl_price = exec_price * (1 - sl_ratio)
            else:
                sp_price = exec_price / tp_ratio
                sl_price = exec_price * (1 + sl_ratio)

            # [FIX-V2-P2-2] 传递 open_state/open_strength,对齐 legacy try_open_execute
            _bar_strength = bar.get("strength", 0.0) if bar is not None else 0.0
            _current_state = self._ctx.get_current_state()
            self._ctx.add_position(
                instrument_id=ctx.instrument_id, volume=signed_volume,
                open_price=exec_price, open_time=bar_time,
                stop_profit_price=sp_price, stop_loss_price=sl_price,
                open_reason=ctx.open_reason or 'backtest', lots=actual_lots,
                instrument_type=_instrument_type, fill_ratio=fill_ratio,
                open_state=_current_state,
                open_strength=_bar_strength,
            )
            _strategy_type = params.get("strategy_type", 'main')
            self._ctx.capture_snapshot("OPEN_SNAPSHOT", f"{ctx.instrument_id} {direction}",
                                       _strategy_type)
        else:
            # 平仓: reduce_position (委托 _close_position_internal 完成 11 步平仓序列)
            # [FIX-EXEC-V2-P0-3] 平仓路径完整修复:
            # - 原实现仅 reduce_position + capture_snapshot,未计算 pnl/未调用 record_trade/未更新权益
            # - 现通过 reduce_position → _close_position_internal 自动完成全部平仓逻辑
            #   (pnl 计算 / slippage / commission / equity 更新 / total_trades+1 /
            #    pnl_pct 计算 / hold_minutes 计算 / record_trade / recent_pnls / 持仓移除)
            # [FIX-V2-P3-1] 调用方通过 ctx.open_reason 传递平仓原因 (StopProfit/StopLoss/
            # backtest_end_force_close 等),避免 _close_position_internal 全部使用默认 'backtest_close'。
            _close_reason = ctx.open_reason or 'backtest_close'
            self._ctx.reduce_position(
                instrument_id=ctx.instrument_id, volume=actual_lots,
                is_buy=(direction > 0), price=exec_price,
                close_reason=_close_reason,
            )
            _strategy_type = params.get("strategy_type", 'main')
            self._ctx.capture_snapshot("CLOSE_SNAPSHOT", f"{ctx.instrument_id} close",
                                       _strategy_type)

        # ===== 阶段 5: 订单记录 + 后置编排 =====
        _bar_idx = getattr(bar, 'name', None)
        _bar_idx_str = str(int(_bar_idx)) if _bar_idx is not None else '0'
        ctx.order_id = f"BT_{ctx.instrument_id}_{_bar_idx_str}_{int(time.time()*1000)%100000}"
        ctx.order = {
            'order_id': ctx.order_id, 'instrument_id': ctx.instrument_id,
            'volume': signed_volume, 'price': exec_price,
            'direction': ctx.direction, 'action': ctx.action,
            'status': 'FILLED', 'traded_volume': actual_lots,
            'fill_ratio': fill_ratio, 'is_backtest': True,
        }

        if self._svc is not None:
            try:
                with self._svc._lock:
                    self._svc._orders_by_id[ctx.order_id] = ctx.order
                    self._svc._stats['total_orders'] += 1
            except Exception as _track_err:
                # 实时回调路径硬约束: except Exception
                logging.debug("[BacktestBackend] 订单追踪失败(非致命): %s", _track_err)

        # 后置编排 (复用 _orchestrator)
        try:
            return self._orchestrator._post_send_persist(ctx)
        except Exception as _post_err:
            # 实时回调路径硬约束: except Exception
            logging.warning("[BacktestBackend] _post_send_persist 异常(非致命): %s", _post_err)
            return OrderResult.ok(ctx.order_id)

    def _resolve_participation_rate(self, params, instrument_type):
        """迁移自 backtest_state.py:553-557 — 解析参与率"""
        rate = params.get("max_participation_rate", 1.0)
        if instrument_type in ('option_buyer', 'option_seller'):
            rate = min(rate, params.get("option_max_participation_rate", 0.10))
        elif instrument_type == 'future':
            rate = min(rate, params.get("future_max_participation_rate", 0.15))
        return rate

    def _append_pending_order(self, ctx, params):
        """institutional 模式 — 追加延迟订单

        [FIX-EXEC-V2-P2-1] 依赖倒置: 不直接 import param_pool._param_grids._PendingOrder,
        通过 BacktestContext.create_pending_order 工厂方法创建,
        保持 order/ 不依赖 param_pool/。

        [FIX-V2-DELAY-1] 使用真实 bar_idx 替代硬编码 0:
        - legacy try_open_execute 使用 bt.bar_idx 作为 signal_bar_idx/created_at_bar
        - 原 V2 实现硬编码 0,导致 order_age = idx - 0 错误,延迟订单在第一个 bar 即被判定超期
        - 现通过 BacktestContext.get_current_bar_idx() 获取当前索引
        """
        direction = 1 if (ctx.direction or 'BUY').upper() == 'BUY' else -1
        _bar_idx = self._ctx.get_current_bar_idx()
        # [FIX-V2-DELAY-3] 使用 _fid.resolve_tp_sl 对齐 legacy try_open_execute,
        # 避免 params.get("tp_ratio"/"sl_ratio") 默认值与策略配置不一致
        _tp_ratio, _sl_ratio = self._fid.resolve_tp_sl(params, ctx.open_reason or '')
        order = self._ctx.create_pending_order(
            symbol=ctx.instrument_id, order_type="open",
            volume=direction * int(ctx.volume), lots=int(ctx.volume),
            reason=ctx.open_reason or 'backtest', signal_bar_idx=_bar_idx,
            signal_price=ctx.price,
            tp_ratio=_tp_ratio,
            sl_ratio=_sl_ratio,
            params_snapshot=dict(params), created_at_bar=_bar_idx,
            fee_type=params.get("default_order_type", "taker"),
        )
        self._ctx.append_pending_order(order)


__all__ = ['BacktestExecutionBackend']
