# [M1-46-DRY] dry_run 虚拟成交执行后端
# MODULE_ID: M1-46-DRY
# _INTERNAL: 本模块为子系统内部实现

"""
DryRunExecutionBackend — dry_run 虚拟成交后端

迁移自:
- _execute_dry_run (order_executor_validation.py:189-254) -> execute()
- _execute_platform_insert 中的 dry_run 4 级检查 (platform.py:166-214) -> _is_dry_run_active()
- _inject_dry_run_callback (路径 A) -> PositionServiceCallbackInjector
- _simulate_dry_run_callbacks (路径 B) -> BusinessLayerCallbackInjector

统一编排:
- 默认使用 PositionServiceCallbackInjector (路径 A，在 execute() 编排层触发)
- 可选注入 BusinessLayerCallbackInjector (路径 B) 或 NullCallbackInjector (测试)

依据: docs/audit/OrderExecutor_Mixin_重构最终落地方案_20260718.md 第三节 3.3
迁移映射: 第四部分 项 3, 6
"""

from __future__ import annotations

import logging
import time
from datetime import datetime as _dt
from typing import Any

from infra.shared_utils import CHINA_TZ
from order.execution_backend_base import OrderExecutionBackend
from order.order_base import OrderResult
from order.backends.callback_injector import (
    CallbackInjector,
    PositionServiceCallbackInjector,
)


class DryRunExecutionBackend(OrderExecutionBackend):
    """dry_run 后端 — 统一编排层拦截 + 可配置回调注入器

    特点:
    - supports_dry_run() 返回 True (跳过断路器/胖手指等实盘校验)
    - 整合原 _execute_dry_run 和 _execute_platform_insert 内的 dry_run 分支
    - 默认使用 PositionServiceCallbackInjector (路径 A)
    - 4 级 dry_run_active 检查保留 (FIX-20260708-DRY-RUN-V2)
    - provider_ref 2 级 fallback 保留 (FIX-PROVIDER-FALLBACK)
    """

    # 默认使用路径 A (在 execute() 编排层触发，语义清晰，except Exception 更安全)
    DEFAULT_INJECTOR = PositionServiceCallbackInjector

    def __init__(self, orchestrator: Any = None,
                 injector: CallbackInjector = None):
        super().__init__(orchestrator=orchestrator)
        self._injector: CallbackInjector = injector or self.DEFAULT_INJECTOR()

    def supports_dry_run(self) -> bool:
        return True

    def execute(self, ctx: Any) -> OrderResult:
        """统一拦截: 整合 validation.py:189-254 + platform.py:166-214 的 dry_run 逻辑"""
        svc = self._svc

        # [FIX-20260708-V4-SIG02] dry_run路径_pre_send_checks提前返回
        # 未设置ctx._order_submit_start_ts，导致_post_send_persist中
        # _delay_ms 计算异常。此处补设start_ts
        if getattr(ctx, '_order_submit_start_ts', 0.0) == 0.0:
            ctx._order_submit_start_ts = time.perf_counter()

        # 1. 4 级 dry_run_active 检查 (迁移自 platform.py:166-189)
        if not self._is_dry_run_active(svc):
            # 非 dry_run 状态不应到达此处，由编排层路由
            logging.warning("[DRY-RUN] DryRunBackend called in non-dry_run mode, fallback to FILLED")
            # 不阻断，继续构造虚拟订单

        # 2. 构造虚拟订单ID和成交结果 (迁移自 validation.py:212-230)
        _ts_ms = int(time.time() * 1000)
        ctx.order_id = f"DRY_{ctx.instrument_id}_{_ts_ms}"
        ctx.order = {
            'order_id': ctx.order_id,
            'instrument_id': ctx.instrument_id,
            'exchange': ctx.exchange or '',
            'volume': ctx.volume,
            'price': ctx.price,
            'direction': ctx.direction,
            'action': ctx.action,
            'status': 'FILLED',
            'traded_volume': ctx.volume,  # 虚拟全额成交
            'signal_id': ctx.signal_id,
            'open_reason': ctx.open_reason,
            'is_dry_run': True,  # 标记为dry_run订单
            'created_at': _dt.now(),
            'updated_at': _dt.now(),
        }

        # 3. 记录到订单追踪 (迁移自 validation.py:233-236)
        with svc._lock:
            svc._orders_by_id[ctx.order_id] = ctx.order
            svc._stats['total_orders'] += 1

        logging.info("[DRY-RUN] 模拟下单成功: %s %s %s %d@%.2f action=%s",
                     ctx.order_id, ctx.instrument_id, ctx.direction,
                     int(ctx.volume), ctx.price, ctx.action)

        # 4. 平台 ID 映射 (迁移自 platform.py:206-209)
        with svc._lock:
            svc._platform_id_to_order_id[f"DRY_{ctx.order_id}"] = ctx.order_id

        # 5. WAL 持久化 (迁移自 platform.py:208-209 + validation.py:246)
        svc._wal_write(ctx.order_id, 'DRY_RUN_ACCEPTED', ctx.order)
        svc._append_order_state(ctx.order_id, 'DRY_RUN_ACCEPTED', ctx.order)
        # [FIX-DRY-RUN-FILLED] 同步追加 FILLED 状态 — 与内存 status=FILLED 一致
        # 重构前路径A (validation.py:246) 直接写入 FILLED，此处保持一致
        # 避免异步回调注入失败(如无provider)时 FILLED 状态在磁盘丢失
        svc._append_order_state(ctx.order_id, 'FILLED', ctx.order)
        # [FIX-WAL-CLEANUP] DRY订单终态后立即删除.wal文件
        # 根因: DRY模式下_wal_write写入.wal但从不删除(与LiveBackend CONFIRMED后_wal_delete不对称)
        # 导致.wal文件无限累积(实测158K个)，_recover_orphaned_orders扫描阻塞on_init 24分钟
        # 修复: 与live_backend.py:236对称，终态后立即删除.wal文件
        # 安全性: order_state.jsonl已保留完整订单状态，.wal仅是提交瞬态冗余
        svc._wal_delete(ctx.order_id)

        # 6. 异步注入虚拟回调 (统一入口，可配置注入器)
        _delay = getattr(svc, '_dry_run_callback_delay_sec', 0.05)
        try:
            self._injector.inject(ctx, svc, delay=_delay)
        except Exception as _cb_e:
            # 实时回调路径硬约束: except Exception
            logging.warning("[DRY-RUN] 虚拟回调注入失败(非阻断): %s", _cb_e)

        # 7. 后置编排 (复用 _orchestrator，迁移自 validation.py:250)
        try:
            return self._orchestrator._post_send_persist(ctx)
        except Exception as _post_err:
            # 实时回调路径硬约束: except Exception
            logging.warning("[DRY-RUN] _post_send_persist异常(非致命): %s", _post_err)
            return OrderResult.ok(ctx.order_id)

    def _is_dry_run_active(self, svc: Any) -> bool:
        """4 级 dry_run 检查 — 迁移自 platform.py:166-189

        多重回退检查（任一为True即拦截），避免单点失败导致拦截失效
        """
        _dry_run = False
        # 检查1: svc._dry_run_mode（init_order_service_attrs设置）
        if getattr(svc, '_dry_run_mode', False):
            _dry_run = True
        # 检查2: _provider_ref._dry_run_active（lifecycle_callbacks已设置）
        if not _dry_run:
            _provider = getattr(svc, '_provider_ref', None)
            if _provider is not None and getattr(_provider, '_dry_run_active', False):
                _dry_run = True
        # 检查3: strategy对象._dry_run_active（通过_platform_insert_order回溯）
        if not _dry_run:
            _platform_fn = getattr(svc, '_platform_insert_order', None)
            if _platform_fn is not None:
                _host = getattr(_platform_fn, '__self__', None)
                if _host is not None and getattr(_host, '_dry_run_active', False):
                    _dry_run = True
        # 检查4: get_cached_params兜底
        if not _dry_run:
            try:
                from config.config_params import get_cached_params as _gcp_dr
                if _gcp_dr().get('dry_run_mode', False):
                    _dry_run = True
            except Exception:
                # 实时回调路径硬约束: except Exception
                pass
        # 诊断日志：首次到达时输出dry_run检查结果（5秒内仅1次）
        if not hasattr(svc, '_dry_run_check_logged') or (time.time() - svc._dry_run_check_logged) > 5.0:
            logging.info("[DRY-RUN-CHECK] dry_run=%s method=%s svc_has_attr=%s provider_has=%s",
                         _dry_run,
                         'svc._dry_run_mode' if getattr(svc, '_dry_run_mode', None) is not None
                         else 'provider._dry_run_active' if getattr(getattr(svc, '_provider_ref', None), '_dry_run_active', None) is not None
                         else 'get_cached_params',
                         hasattr(svc, '_dry_run_mode'),
                         hasattr(getattr(svc, '_provider_ref', None), '_dry_run_active') if getattr(svc, '_provider_ref', None) else False)
            svc._dry_run_check_logged = time.time()
        return _dry_run

    def invoke_cancel(self, platform_id) -> None:
        """[FIX-DRY-CANCEL-20260720] dry_run 虚拟撤单 — 不调用平台API

        根因: DryRunExecutionBackend未实现invoke_cancel，OrderService委托时
        抛NotImplementedError(184次ERROR)。
        修复: dry_run模式下撤单仅标记CANCELLED状态，不调用平台撤单API。
        与LiveExecutionBackend.invoke_cancel对称，但无平台调用。

        Args:
            platform_id: 平台订单ID (dry_run模式下可能为int/str/None，均虚拟处理)
        """
        svc = self._svc
        _pid_str = str(platform_id) if platform_id else ''

        # 查找对应的internal order_id
        _order_id = None
        with svc._lock:
            if _pid_str and _pid_str in svc._platform_id_to_order_id:
                _order_id = svc._platform_id_to_order_id[_pid_str]
            elif _pid_str:
                # fallback: 遍历查找
                for _pid, _oid in svc._platform_id_to_order_id.items():
                    if str(_pid) == _pid_str:
                        _order_id = _oid
                        break

        if _order_id and _order_id in svc._orders_by_id:
            _order = svc._orders_by_id[_order_id]
            _order['status'] = 'CANCELLED'
            _order['updated_at'] = _dt.now()
            try:
                svc._wal_write(_order_id, 'CANCELLED', _order)
                svc._append_order_state(_order_id, 'CANCELLED', _order)
                # [FIX-WAL-CLEANUP] DRY撤单终态后立即删除.wal文件(与下单路径对称)
                svc._wal_delete(_order_id)
            except Exception as _cancel_persist_err:
                # 实时回调路径硬约束: except Exception
                logging.warning("[DRY-RUN] 虚拟撤单持久化失败(非阻断): %s err=%s",
                                _order_id, _cancel_persist_err)
            logging.info("[DRY-RUN] 虚拟撤单成功: order=%s platform_id=%s",
                         _order_id, _pid_str)
        else:
            logging.debug("[DRY-RUN] 虚拟撤单: 未找到订单 platform_id=%s (可能已平仓)",
                          _pid_str)


__all__ = ['DryRunExecutionBackend']
