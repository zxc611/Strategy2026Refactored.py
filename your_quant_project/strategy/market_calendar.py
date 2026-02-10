"""交易时段/开盘判断。"""
from __future__ import annotations

from datetime import date, datetime, timedelta, time as dtime
import re
from typing import Iterable, List, Optional, Set, Tuple

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


def _coerce_holiday_date(value: object) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        digits = str(int(value))
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        digits = re.sub(r"\D", "", raw)
    else:
        return None
    if len(digits) != 8:
        return None
    try:
        year = int(digits[0:4])
        month = int(digits[4:6])
        day = int(digits[6:8])
        return date(year, month, day)
    except Exception:
        return None


def _parse_holiday_input(raw: object) -> Set[date]:
    if raw is None:
        return set()
    items: Iterable[object]
    if isinstance(raw, (list, tuple, set)):
        items = raw
    elif isinstance(raw, str):
        parts = [part for part in re.split(r"[\s,;]+", raw) if part]
        items = parts
    else:
        items = [raw]
    results: Set[date] = set()
    for item in items:
        parsed = _coerce_holiday_date(item)
        if parsed:
            results.add(parsed)
    return results


def _get_holiday_dates_from_obj(obj: Optional[object]) -> Set[date]:
    holidays: Set[date] = set()
    if obj is None:
        return holidays
    try:
        params = getattr(obj, "params", None)
        if params is not None:
            holidays |= _parse_holiday_input(getattr(params, "holiday_dates", None))
    except Exception:
        pass
    try:
        if hasattr(obj, "_load_param_table"):
            table = obj._load_param_table()
            if isinstance(table, dict):
                raw = table.get("holiday_dates")
                if raw is None and isinstance(table.get("params"), dict):
                    raw = table["params"].get("holiday_dates")
                if raw is not None:
                    holidays |= _parse_holiday_input(raw)
    except Exception:
        pass
    return holidays


def is_trading_day(target_date: date, holiday_dates: Optional[Set[date]] = None) -> bool:
    if target_date.weekday() >= 5:
        return False
    if holiday_dates and target_date in holiday_dates:
        return False
    return True


def get_trading_sessions(
    ref: Optional[datetime] = None,
    exchange: Optional[str] = None,
    holiday_dates: Optional[Set[date]] = None,
) -> List[Tuple[datetime, datetime]]:
    """按实盘定义返回日/夜盘时段，可按交易所选择（含上期所跨午夜夜盘）"""
    base_now = ref or datetime.now()
    today = base_now.date()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)
    holidays = holiday_dates or set()
    today_is_trading = is_trading_day(today, holidays)
    yesterday_is_trading = is_trading_day(yesterday, holidays)
    tomorrow_is_trading = is_trading_day(tomorrow, holidays)
    exch = (exchange or "").upper()

    sessions: List[Tuple[datetime, datetime]] = []

    # 日盘（按交易所选择，未指定则并集）
    if exch == "CFFEX":
        day_defs = CFFEX_DAY_SESSIONS
    elif exch in ("SHFE", "DCE", "CZCE", "GFEX", "INE"):
        day_defs = COMMODITY_DAY_SESSIONS
    else:
        day_defs = CFFEX_DAY_SESSIONS + COMMODITY_DAY_SESSIONS
    if today_is_trading:
        for start_t, end_t in day_defs:
            sessions.append((datetime.combine(today, start_t), datetime.combine(today, end_t)))

    # 夜盘（商品交易所）
    include_dce_czce_gfex = exch in ("DCE", "CZCE", "GFEX", "")
    include_shfe = exch in ("SHFE", "INE", "")

    if include_dce_czce_gfex and today_is_trading:
        sessions.append(
            (
                datetime.combine(today, NIGHT_START_DCE_CZCE_GFEX),
                datetime.combine(today, NIGHT_END_DCE_CZCE_GFEX),
            )
        )
    if include_dce_czce_gfex and yesterday_is_trading:
        sessions.append(
            (
                datetime.combine(yesterday, NIGHT_START_DCE_CZCE_GFEX),
                datetime.combine(yesterday, NIGHT_END_DCE_CZCE_GFEX),
            )
        )

    if include_shfe and today_is_trading:
        # 当日夜盘 21:00 至次日 02:00，需跨午夜
        sessions.append(
            (datetime.combine(today, NIGHT_START_SHFE), datetime.combine(today + timedelta(days=1), NIGHT_END_SHFE))
        )
    if include_shfe and yesterday_is_trading:
        # 覆盖当日凌晨 00:00-02:00 的前一日夜盘尾段
        sessions.append((datetime.combine(yesterday, NIGHT_START_SHFE), datetime.combine(today, NIGHT_END_SHFE)))

    return sessions


def is_market_open_safe(obj: object) -> bool:
    """尽量安全判断是否开盘；遵从“最长开盘时间”原则（任一配置的交易所开盘即视为开盘）。"""
    try:
        fn = getattr(obj, "is_market_open", None)
        if callable(fn):
            return bool(fn())
    except Exception:
        pass

    try:
        params = getattr(obj, "params", None)
        # [Fix] 不再只依赖单一的 params.exchange (默认CFFEX导致无夜盘)
        # 而是检查所有配置的exchanges，取并集
        exchanges_raw = getattr(params, "exchanges", "") or "CFFEX,SHFE,DCE,CZCE,INE,GFEX"
        if isinstance(exchanges_raw, (list, tuple, set)):
            exchanges = [str(e).strip().upper() for e in exchanges_raw if str(e).strip()]
        else:
            exchanges = [e.strip().upper() for e in str(exchanges_raw).split(",") if e.strip()]
        if not exchanges: 
            exchanges = ["CFFEX"]

        now_dt = datetime.now()
        holidays = _get_holiday_dates_from_obj(obj)
        # 遍历所有交易所，只要有一个处于交易时段，即视为开盘
        for exch in exchanges:
            for start, end in get_trading_sessions(now_dt, exch, holiday_dates=holidays):
                if start <= now_dt <= end:
                    return True
    except Exception as e:
        print(f"[Warn] is_market_open_safe exception: {e}")

    return False


def minutes_to_next_open(
    ref: Optional[datetime] = None,
    exchange: Optional[str] = None,
    holiday_dates: Optional[Set[date]] = None,
) -> Optional[float]:
    now_dt = ref or datetime.now()
    holidays = holiday_dates or set()
    candidates: List[datetime] = []
    for day_offset in range(0, 8):
        base_ref = now_dt + timedelta(days=day_offset)
        if not is_trading_day(base_ref.date(), holidays):
            continue
        for start, _ in get_trading_sessions(base_ref, exchange, holiday_dates=holidays):
            if start >= now_dt:
                candidates.append(start)
    if not candidates:
        return None
    next_start = min(candidates)
    return (next_start - now_dt).total_seconds() / 60.0


def is_close_debug_allowed(obj: object, minutes_to_open: int = 30) -> Tuple[bool, Optional[str]]:
    try:
        params = getattr(obj, "params", None)
        now_dt = datetime.now()
        holidays = _get_holiday_dates_from_obj(obj)
        if not is_trading_day(now_dt.date(), holidays):
            return True, None
        # If any exchange is open, disable close-debug.
        for start, end in get_trading_sessions(now_dt, exchange=None, holiday_dates=holidays):
            if start <= now_dt <= end:
                return False, "market_open"
        # If any exchange opens soon, also disable.
        mins = minutes_to_next_open(now_dt, exchange=None, holiday_dates=holidays)
        if mins is not None and mins <= float(minutes_to_open):
            return False, "near_open"
    except Exception:
        pass
    return True, None


class MarketCalendarMixin:
    """交易时段/开盘判断混入。"""

    def _get_holiday_dates(self) -> Set[date]:
        return _get_holiday_dates_from_obj(self)

    def _is_trading_day(self, target_date: date) -> bool:
        return is_trading_day(target_date, self._get_holiday_dates())

    def _get_trading_sessions(
        self,
        ref: Optional[datetime] = None,
        exchange: Optional[str] = None,
    ) -> List[Tuple[datetime, datetime]]:
        return get_trading_sessions(ref=ref, exchange=exchange, holiday_dates=self._get_holiday_dates())

    def is_market_open(self) -> bool:
        """判断当前是否为开盘时间（含日盘和各交易所夜盘）。"""
        now_dt = datetime.now()
        exchanges_raw = getattr(self.params, "exchanges", None)
        if isinstance(exchanges_raw, (list, tuple, set)):
            exchanges = [str(e).strip().upper() for e in exchanges_raw if str(e).strip()]
        else:
            exchanges = [e.strip().upper() for e in str(exchanges_raw or "").split(",") if e.strip()]
        if not exchanges:
            default_exch = getattr(self.params, "exchange", None)
            exchanges = [default_exch] if default_exch else [None]
        for exch in exchanges:
            for start, end in self._get_trading_sessions(now_dt, exch):
                if start <= now_dt <= end:
                    return True
        return False
