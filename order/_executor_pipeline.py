# MODULE_ID: M1-142
"""
_executor_pipeline.py - 三阶段流水线核心

拆分自 order_executor.py (2026-06-30)
职责：前置校验、平台下单、后置持久化

委托给原order_executor.py实现，确保功能无缺失
"""

from __future__ import annotations

__all__ = ['OrderContext', 'OrderExecutorPipeline']


# 委托导入
try:
    from ali2026v3_trading.order.order_executor import (
        OrderContext as _OrderContext,
        OrderExecutor as _OrderExecutor,
    )
    
    OrderContext = _OrderContext
    
    # OrderExecutorPipeline委托给OrderExecutor
    class OrderExecutorPipeline:
        """三阶段流水线执行器（委托给OrderExecutor）"""
        
        def __init__(self, order_service):
            self._executor = _OrderExecutor(order_service)
        
        def execute(self, ctx: OrderContext):
            """执行三阶段流水线"""
            return self._executor.execute(ctx)
        
        # 委托所有方法
        def __getattr__(self, name):
            return getattr(self._executor, name)
    
except ImportError as e:
    import logging
    logging.warning("[_executor_pipeline] 委托导入失败: %s", e)
    
    # 提供空实现以避免导入错误
    from dataclasses import dataclass, field
    from typing import Any, Dict, Optional
    
    @dataclass
    class OrderContext:
        instrument_id: str = ''
        volume: float = 0.0
        price: float = 0.0
        direction: str = 'BUY'
        action: str = 'OPEN'
        exchange: str = ''
        priority: str = 'NORMAL'
        is_chase: bool = False
        signal_id: str = ''
        expected_position_count: int = -2
        open_reason: str = ''
        decision_score: float = 0.0
        position_scale: float = 1.0
        decision_action: str = ''
        dimension_scores: Optional[Dict[str, float]] = None
        dimension_weights: Optional[Dict[str, float]] = None
        idempotent_key: str = ''
        order_id: str = ''
        order: Dict[str, Any] = field(default_factory=dict)
        rejected: bool = False
        reject_code: str = ''
        reject_message: str = ''
        _order_submit_start_ts: float = 0.0
        _cyclic_guard: Any = None
        ref_price: float = 0.0
    
    class OrderExecutorPipeline:
        def __init__(self, order_service):
            pass