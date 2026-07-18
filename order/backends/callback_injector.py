# [M1-46-DRY-INJ] dry_run 虚拟回调注入器策略
# MODULE_ID: M1-46-DRY-INJ
# _INTERNAL: 本模块为子系统内部实现

"""
CallbackInjector 策略 — 统一 dry_run 两套虚拟回调路径

代码中存在两套独立的 dry_run 虚拟回调机制:
- 路径 A: PositionServiceCallbackInjector — 通过 _position_service._add_position/_reduce_position 驱动持仓
  源码: order_executor_validation.py:256-371 (_inject_dry_run_callback)
  特点: 2 级 provider_ref fallback

- 路径 B: BusinessLayerCallbackInjector — 通过 _business_layer.on_order/on_trade 驱动策略回调
  源码: order_executor_platform.py:912-992 (_simulate_dry_run_callbacks)
  特点: 3 级 business_layer fallback，模拟 '全部成交' 状态

统一方式:
- 默认使用路径 A (在 execute() 编排层触发，语义更清晰，except Exception 更安全)
- 路径 B 作为可选注入器，用于需要 on_order/on_trade 完整生命周期的场景
- NullCallbackInjector 用于单元测试

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第三节 3.3
迁移映射: 第四部分 项 4, 7
"""

from __future__ import annotations

import logging
import threading
import time as _time
from abc import ABC, abstractmethod
from typing import Any


class CallbackInjector(ABC):
    """虚拟回调注入器 — 策略模式抽象，统一路径 A/B"""

    @abstractmethod
    def inject(self, ctx: Any, svc: Any, delay: float) -> None:
        """异步注入虚拟成交回调

        Args:
            ctx: OrderContext (已填充虚拟订单数据)
            svc: OrderService 实例
            delay: 回调延迟秒数 (模拟网络往返)
        """
        ...


class PositionServiceCallbackInjector(CallbackInjector):
    """路径 A: 通过 _position_service._add_position/_reduce_position 驱动持仓

    迁移自 _inject_dry_run_callback (order_executor_validation.py:256-371)
    特点:
    - 2 级 provider_ref fallback (direct → platform_fn.__self__ → get_cached_params)
    - except Exception (符合实时回调路径硬约束)
    - 在 execute() 编排层触发，语义清晰
    """

    def inject(self, ctx: Any, svc: Any, delay: float = 0.05) -> None:
        """异步注入 — 迁移自 _inject_dry_run_callback 的 _virtual_callback 内部逻辑"""
        def _virtual_callback():
            _time.sleep(delay)  # 模拟网络延迟
            try:
                # [FIX-20260708-PROVIDER-FALLBACK] 多级回退获取_provider_ref
                _provider_ref = getattr(svc, '_provider_ref', None)
                _fb_source = 'direct'
                if _provider_ref is None:
                    # 回退1: 通过_platform_insert_order.__self__回溯策略对象
                    # FIX-PROVIDER-FALLBACK-1: __self__是OrderService自身而非策略对象，
                    # 获取到后_position_service仍为None，需跳过此无效回退
                    _platform_fn = getattr(svc, '_platform_insert_order', None)
                    if _platform_fn is not None:
                        _host = getattr(_platform_fn, '__self__', None)
                        if _host is not None and _host is not svc:
                            _provider_ref = _host
                            _fb_source = 'platform_fn_self'
                if _provider_ref is None:
                    # 回退2: 通过get_cached_params查找strategy对象
                    try:
                        from config.config_service import get_cached_params as _gcp_v
                        _strategy_obj = _gcp_v().get('strategy', None)
                        if _strategy_obj is not None:
                            _core = getattr(_strategy_obj, 'strategy_core', None)
                            _provider_ref = _core if _core else _strategy_obj
                            _fb_source = 'cached_params'
                    except Exception:
                        pass
                if _provider_ref is not None:
                    _fb_ok = getattr(svc, '_dry_run_provider_fallback_ok_count', 0) + 1
                    svc._dry_run_provider_fallback_ok_count = _fb_ok
                    if _fb_ok <= 3 or _fb_ok % 100 == 1:
                        logging.info("[DRY-RUN] provider回退成功: source=%s order_id=%s",
                                     _fb_source, getattr(ctx, 'order_id', '?'))
                if _provider_ref is None:
                    _fb_count = getattr(svc, '_dry_run_provider_fallback_fail_count', 0) + 1
                    svc._dry_run_provider_fallback_fail_count = _fb_count
                    if _fb_count <= 5 or _fb_count % 100 == 1:
                        logging.warning("[DRY-RUN] 所有回退均无法获取provider，虚拟回调注入跳过(累计%d次) order_id=%s",
                                        _fb_count, getattr(ctx, 'order_id', '?'))
                    return
                _ps = getattr(_provider_ref, '_position_service', None)
                if _ps is None and hasattr(_provider_ref, '_ensure_position_service'):
                    _provider_ref._ensure_position_service()
                    _ps = getattr(_provider_ref, '_position_service', None)
                if _ps is None:
                    logging.warning("[DRY-RUN] _position_service为None（ensure后仍为空），虚拟回调注入跳过 order_id=%s",
                                    getattr(ctx, 'order_id', '?'))
                    return

                _action = (ctx.action or '').upper()
                _instrument_id = ctx.instrument_id
                _direction = (ctx.direction or 'BUY').upper()
                _volume = int(ctx.volume or 1)
                _price = float(ctx.price or 0.0)
                _order_id = ctx.order_id
                _signal_id = getattr(ctx, 'signal_id', '') or ''
                _open_reason = getattr(ctx, 'open_reason', '') or 'dry_run'
                # [FIX-20260708-V4] 修复_add_position/_reduce_position参数不匹配
                # direction通过volume正负传递: BUY=正值, SELL=负值
                _exchange = getattr(ctx, 'exchange', '') or ''
                if not _exchange:
                    try:
                        from config.params_service import get_params_service
                        _ps_svc = get_params_service()
                        if _ps_svc is not None:
                            _meta = _ps_svc.get_instrument_meta(_instrument_id) if hasattr(_ps_svc, 'get_instrument_meta') else None
                            if _meta:
                                _exchange = _meta.get('exchange', '') or ''
                    except Exception:
                        pass
                _signed_volume = _volume if _direction == 'BUY' else -_volume

                if _action in ('OPEN', ''):
                    # 开仓回调 → _add_position → OPEN快照
                    _ps._add_position(
                        exchange=_exchange,
                        instrument_id=_instrument_id,
                        volume=_signed_volume,
                        price=_price,
                        order_id=_order_id,
                        signal_id=_signal_id,
                        open_reason=f"dry_run:{_open_reason}",
                    )
                    logging.info("[DRY-RUN] 虚拟开仓回调注入成功: %s %s %d@%.2f",
                                _instrument_id, _direction, _volume, _price)
                elif _action in ('CLOSE',):
                    # 平仓回调 → _reduce_position → CLOSE快照
                    _ps._reduce_position(
                        exchange=_exchange,
                        instrument_id=_instrument_id,
                        volume=_volume,
                        is_buy=(_direction == 'BUY'),
                        price=_price,
                    )
                    logging.info("[DRY-RUN] 虚拟平仓回调注入成功: %s %s %d@%.2f",
                                _instrument_id, _direction, _volume, _price)
            except Exception as e:
                # 实时回调路径硬约束: except Exception (不使用窄异常元组)
                logging.error("[DRY-RUN] 虚拟回调注入失败: %s", e, exc_info=True)

        _t = threading.Thread(target=_virtual_callback, daemon=True,
                               name=f"DryRunCallback_{ctx.order_id}")
        _t.start()


class BusinessLayerCallbackInjector(CallbackInjector):
    """路径 B: 通过 _business_layer.on_order/on_trade 驱动策略回调

    迁移自 _simulate_dry_run_callbacks (order_executor_platform.py:912-992)
    特点:
    - 3 级 business_layer fallback (provider → platform_fn.__self__.strategy_core → get_cached_params)
    - 模拟 '全部成交' 状态，触发 _on_order_filled_trigger_position_update
    - except Exception (扩展自原窄异常元组，符合实时回调路径硬约束)
    """

    def inject(self, ctx: Any, svc: Any, delay: float = 0.05) -> None:
        """异步注入 — 迁移自 _simulate_dry_run_callbacks 的 _fire 内部逻辑"""
        _platform_id = ctx.order.get('platform_order_id', f"DRY_{ctx.order_id}")
        _internal_id = ctx.order_id

        def _fire() -> None:
            try:
                _time.sleep(delay)
                _virtual_order = {
                    'order_id': _platform_id,
                    'instrument_id': ctx.instrument_id,
                    'direction': ctx.direction,
                    'offset': ctx.action,
                    'volume': ctx.volume,
                    'traded_volume': ctx.volume,
                    'price': ctx.price,
                    'status': '全部成交',
                    'memo': _internal_id,
                }
                _virtual_trade = {
                    'trade_id': f"DRY_TRADE_{_internal_id}",
                    'order_id': _platform_id,
                    'instrument_id': ctx.instrument_id,
                    'direction': ctx.direction,
                    'offset': ctx.action,
                    'volume': ctx.volume,
                    'traded_volume': ctx.volume,
                    'price': ctx.price,
                    'memo': _internal_id,
                }
                _provider = getattr(svc, '_provider_ref', None)
                _bl = getattr(_provider, '_business_layer', None) if _provider else None
                if _bl is None:
                    _platform_fn = getattr(svc, '_platform_insert_order', None)
                    _host = getattr(_platform_fn, '__self__', None) if _platform_fn else None
                    _core = getattr(_host, 'strategy_core', None) if _host else None
                    _bl = getattr(_core, '_business_layer', None) if _core else None
                    if _bl is None and _host is not None:
                        _bl = getattr(_host, '_business_layer', None)
                if _bl is None:
                    try:
                        from config.config_service import get_cached_params as _gcp_bl
                        _strategy_obj = _gcp_bl().get('strategy', None)
                        if _strategy_obj is not None:
                            _core3 = getattr(_strategy_obj, 'strategy_core', None)
                            _bl = getattr(_core3, '_business_layer', None) if _core3 else None
                    except Exception:
                        pass
                if _bl is None:
                    logging.warning("[DRY-RUN] 业务层不可用，虚拟回调注入跳过: order_id=%s", _internal_id)
                    return
                if hasattr(_bl, 'on_order'):
                    _bl.on_order(_virtual_order)
                if hasattr(_bl, 'on_trade'):
                    _bl.on_trade(_virtual_trade)
                logging.info("[DRY-RUN] 虚拟回调已注入: order_id=%s trade_id=%s status=%s",
                             _internal_id, _virtual_trade['trade_id'], _virtual_order['status'])
            except Exception as _cb_e:
                # 实时回调路径硬约束: except Exception (原窄异常元组已扩展)
                logging.warning("[DRY-RUN] 虚拟回调注入失败(非阻断): %s", _cb_e)

        threading.Thread(target=_fire, name='DryRunCallback', daemon=True).start()


class NullCallbackInjector(CallbackInjector):
    """空操作注入器 — 用于单元测试，不触发任何回调"""

    def inject(self, ctx: Any, svc: Any, delay: float) -> None:
        logging.debug("[DRY-RUN] NullCallbackInjector 跳过回调注入: order_id=%s", getattr(ctx, 'order_id', '?'))


__all__ = [
    'CallbackInjector',
    'PositionServiceCallbackInjector',
    'BusinessLayerCallbackInjector',
    'NullCallbackInjector',
]
