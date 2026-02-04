"""持仓/订单模型定义。"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class PositionLimitConfig:
    """仓位限额配置"""
    limit_amount: float = 0.0
    effective_until: Optional[datetime] = None
    created_at: Optional[datetime] = None
    account_id: str = ""

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class PositionType:
    """仓位类型枚举"""
    LONG = "long"
    SHORT = "short"
    INTRADAY = "intraday"    # 日内仓（当天开仓）
    OVERNIGHT = "overnight"  # 隔夜仓（非当天开仓）


class OrderStatus:
    """订单状态枚举"""
    PENDING = "pending"      # 等待中
    PARTIAL = "partial"      # 部分成交
    COMPLETED = "completed"  # 完全成交
    CANCELLED = "cancelled"  # 已撤销
    CHASING = "chasing"      # 追单中


@dataclass
class PositionRecord:
    """持仓记录数据类"""
    position_id: str
    instrument_id: str
    exchange: str
    open_price: float
    volume: int
    direction: str
    open_time: datetime
    open_date: datetime.date
    position_type: str
    stop_profit_price: float
    days_held: int = 0
    chase_count: int = 0
    stop_profit_order_id: Optional[str] = None


@dataclass
class ChaseOrderTask:
    """追单任务数据类"""
    order_ref: str
    exchange: str
    instrument_id: str
    direction: str
    offset: str
    volume: int
    target_price: float
    max_retries: int
    remark: str = ""
    retry_count: int = 0
    status: str = "pending"
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
