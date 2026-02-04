"""交易时段/开盘判断。"""
from __future__ import annotations

from datetime import datetime, timedelta, time as dtime
from typing import List, Optional, Tuple

# 实盘日盘时段定义
# 中金所（股指/国债）
CFFEX_DAY_SESSIONS = [
    (dtime(9, 30, 0), dtime(11, 30, 0)),
    (dtime(13, 0, 0), dtime(15, 0, 0)),
]
# 商品交易所（上期所/大商所/郑商所/广期所）
COMMODITY_DAY_SESSIONS = [
    (dtime(9, 0, 0), dtime(10, 15, 0)),
    (dtime(10, 30, 0), dtime(11, 30, 0)),
    (dtime(13, 30, 0), dtime(15, 0, 0)),
]

# 夜盘（大商所/郑商所/广期所）
NIGHT_START_DCE_CZCE_GFEX = dtime(21, 0, 0)
NIGHT_END_DCE_CZCE_GFEX = dtime(23, 0, 0)

# 夜盘（上期所，跨午夜至次日2:00）
NIGHT_START_SHFE = dtime(21, 0, 0)
NIGHT_END_SHFE = dtime(2, 0, 0)


def get_trading_sessions(
    ref: Optional[datetime] = None,
    exchange: Optional[str] = None,
) -> List[Tuple[datetime, datetime]]:
    """按实盘定义返回日/夜盘时段，可按交易所选择（含上期所跨午夜夜盘）"""
    base_now = ref or datetime.now()
    today = base_now.date()
    yesterday = today - timedelta(days=1)
    exch = (exchange or "").upper()

    sessions: List[Tuple[datetime, datetime]] = []

    # 日盘（按交易所选择，未指定则并集）
    if exch == "CFFEX":
        day_defs = CFFEX_DAY_SESSIONS
    elif exch in ("SHFE", "DCE", "CZCE", "GFEX"):
        day_defs = COMMODITY_DAY_SESSIONS
    else:
        day_defs = CFFEX_DAY_SESSIONS + COMMODITY_DAY_SESSIONS
    for start_t, end_t in day_defs:
        sessions.append((datetime.combine(today, start_t), datetime.combine(today, end_t)))

    # 夜盘（商品交易所）
    include_dce_czce_gfex = exch in ("DCE", "CZCE", "GFEX", "")
    include_shfe = exch in ("SHFE", "")

    if include_dce_czce_gfex:
        sessions.append(
            (
                datetime.combine(today, NIGHT_START_DCE_CZCE_GFEX),
                datetime.combine(today, NIGHT_END_DCE_CZCE_GFEX),
            )
        )
        sessions.append(
            (
                datetime.combine(yesterday, NIGHT_START_DCE_CZCE_GFEX),
                datetime.combine(yesterday, NIGHT_END_DCE_CZCE_GFEX),
            )
        )

    if include_shfe:
        # 当日夜盘 21:00 至次日 02:00，需跨午夜
        sessions.append(
            (datetime.combine(today, NIGHT_START_SHFE), datetime.combine(today + timedelta(days=1), NIGHT_END_SHFE))
        )
        # 覆盖当日凌晨 00:00-02:00 的前一日夜盘尾段
        sessions.append((datetime.combine(yesterday, NIGHT_START_SHFE), datetime.combine(today, NIGHT_END_SHFE)))

    return sessions


class MarketCalendarMixin:
    """交易时段/开盘判断混入。"""

    def _get_trading_sessions(
        self,
        ref: Optional[datetime] = None,
        exchange: Optional[str] = None,
    ) -> List[Tuple[datetime, datetime]]:
        return get_trading_sessions(ref=ref, exchange=exchange)

    def is_market_open(self) -> bool:
        """判断当前是否为开盘时间（含日盘和各交易所夜盘）；测试模式下始终返回True"""
        if bool(getattr(self.params, "test_mode", False)):
            return True
        now_dt = datetime.now()
        default_exch = getattr(self.params, "exchange", None)
        for start, end in self._get_trading_sessions(now_dt, default_exch):
            if start <= now_dt <= end:
                return True
        return False
