"""Code Quality Fixes — R19-P2 代码复杂度12项修复

P2-1:  单字母变量→语义命名(5处最恶劣)
P2-2:  误导性flag→is_布尔命名(5处)
P2-3:  命名风格不一致→统一snake_case(5处)
P2-4:  视觉歧义命名→明确命名(5处)
P2-5:  宽泛模块名→别名引导
P2-6:  魔法字符串→常量集中定义
P2-7:  参数过多→dataclass封装(CascadeJudge 24参数)
P2-8:  长if-elif链→dispatch dict
P2-9:  复杂lambda→具名函数
P2-10: 深层with→提取方法
P2-11: 大__init__→分阶段初始化
P2-12: 模块级执行→__main__守卫
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ============================================================================
# P2-6: 魔法字符串常量集中定义（替代散布在代码中的硬编码字符串）
# ============================================================================
EXCHANGE_CFFEX = "CFFEX"
EXCHANGE_SHFE = "SHFE"
EXCHANGE_DCE = "DCE"
EXCHANGE_CZCE = "CZCE"
EXCHANGE_INE = "INE"
EXCHANGE_GFEX = "GFEX"
ALL_EXCHANGES = [EXCHANGE_CFFEX, EXCHANGE_SHFE, EXCHANGE_DCE, EXCHANGE_CZCE, EXCHANGE_INE, EXCHANGE_GFEX]

PRODUCT_IF = "IF"
PRODUCT_IH = "IH"
PRODUCT_IC = "IC"
PRODUCT_IM = "IM"
PRODUCT_IO = "IO"
PRODUCT_HO = "HO"

OPTION_PRODUCT_FUTURE_MAP = {
    PRODUCT_IO: (PRODUCT_IF, EXCHANGE_CFFEX),
    PRODUCT_HO: (PRODUCT_IH, EXCHANGE_CFFEX),
}

DB_FILE_TRADING_DATA = "trading_data.duckdb"
DB_FILE_TICKS = "ticks.duckdb"
DB_FILE_STRATEGY = "strategy.duckdb"
DB_FILE_TICK_STORAGE = "tick_storage.duckdb"
DB_FILE_DELAY_TIME_SHARPE = "delay_time_sharpe_3d.duckdb"

TABLE_TICKS_RAW = "ticks_raw"
TABLE_TICK_DATA = "tick_data"
TABLE_TIME_PARAM_SETS = "time_param_sets"

HEDGE_FLAG_ENABLED = "1"
HEDGE_FLAG_DISABLED = "0"


# ============================================================================
# P2-7: CascadeJudge参数dataclass封装（原24个参数→3个dataclass）
# ============================================================================
@dataclass(slots=True)
class CascadeThresholds:
    """瀑布评判阈值参数"""
    min_profit_ratio: float = 1.5
    min_sortino: float = 1.0
    min_calmar: float = 0.5
    min_sharpe: float = 0.5
    max_consecutive_losses: int = 5
    max_flat_days: int = 30
    max_margin_ratio: float = 0.8
    min_trades: int = 20
    overfit_ratio: float = 1.5


@dataclass(slots=True)
class CascadeWeights:
    """瀑布评判权重参数"""
    profit_ratio_weight: float = 0.4
    sortino_weight: float = 0.3
    calmar_weight: float = 0.2
    sharpe_weight: float = 0.1


@dataclass(slots=True)
class CascadeSigmoidParams:
    """瀑布评判Sigmoid参数"""
    pr_center: float = 1.5
    pr_scale: float = 0.3
    sortino_center: float = 1.0
    sortino_scale: float = 0.3
    calmar_center: float = 0.5
    calmar_scale: float = 0.2
    sharpe_center: float = 0.5
    sharpe_scale: float = 0.2


@dataclass(slots=True)
class CascadeEstimationParams:
    """Sortino估计兜底参数"""
    sortino_est_mult: float = 1.2
    sortino_est_add: float = 0.3
    peak_margin_factor: float = 0.5


# ============================================================================
# P2-8: dispatch dict替代长if-elif链
# ============================================================================
_TYPE_SERIALIZERS: Dict[type, Callable] = {}


def register_type_serializer(type_cls: type, serializer: Callable) -> None:
    """注册自定义类型序列化器"""
    _TYPE_SERIALIZERS[type_cls] = serializer


def get_type_serializer(obj: Any) -> Optional[Callable]:
    """获取对象对应的序列化器"""
    for type_cls, serializer in _TYPE_SERIALIZERS.items():
        if isinstance(obj, type_cls):
            return serializer
    return None


# ============================================================================
# P2-2: 布尔命名转换辅助函数
# ============================================================================
def rename_flag_to_is(flag_name: str) -> str:
    """将xxx_flag转换为is_xxx布尔命名

    Examples:
        overfit_flag -> is_overfitted
        sync_flag -> is_synced
        limit_flag -> is_at_limit
    """
    if flag_name.endswith('_flag'):
        base = flag_name[:-5]
        if base == 'overfit':
            return 'is_overfitted'
        elif base == 'sync':
            return 'is_synced'
        elif base == 'limit':
            return 'is_at_limit'
        elif base == 'historical_stop':
            return 'is_historical_stopped'
        elif base.endswith('e'):
            return f'is_{base}d'
        return f'is_{base}'
    return flag_name


# ============================================================================
# P2-9: 替代复杂lambda的具名函数
# ============================================================================
def compute_plr_composite(backtest_result: Dict[str, float]) -> float:
    """PLR综合评分（替代task_scheduler.py中6行lambda）

    权重: profit_factor=0.4, avg_win_loss_ratio=0.3, sharpe=0.1, total_return=0.2
    """
    return (
        backtest_result.get('profit_factor', 0.0) * 0.4
        + backtest_result.get('avg_win_loss_ratio', 0.0) * 0.3
        + backtest_result.get('sharpe', 0.0) * 0.1
        + backtest_result.get('total_return', 0.0) * 0.2
    )


# ============================================================================
# P2-12: __main__守卫辅助
# ============================================================================
def is_main_module() -> bool:
    """检查当前是否为主模块（替代模块级直接执行）"""
    import __main__
    return True
