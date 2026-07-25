"""
market_time_service.py - 市场时间服务与时钟同步检测

从 scheduler_service.py 提取，解决 P1-08 双调度器问题。
职责：市场开收盘判断、交易日历、时钟同步检测

作者：CodeArts 代码智能体
版本：v1.0
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Dict, Optional
from datetime import datetime
from datetime import time as dt_time


# ============================================================================
# DR-P1-12修复: 时钟同步检测
# ============================================================================
class TimeSyncChecker:
    """DR-P1-12: 使用简单时间源比对检测时钟偏差

    偏差 > 5秒  → WARNING
    偏差 > 30秒 → CRITICAL → 触发暂停交易
    """

    WARNING_THRESHOLD_SEC = 5.0
    CRITICAL_THRESHOLD_SEC = 30.0
    CHECK_INTERVAL_SEC = 300.0  # 每5分钟检查一次

    def __init__(self):
        self._last_check_time: float = 0.0
        self._last_deviation: float = 0.0
        self._status: str = 'HEALTHY'  # HEALTHY / WARNING / CRITICAL
        self._lock = threading.Lock()

    def check_time_sync(self) -> Dict[str, Any]:
        """DR-P1-12: 检测系统时钟与外部时间源的偏差

        Returns:
            dict: 包含 deviation_sec, status, message
        """
        now = time.time()
        if now - self._last_check_time < self.CHECK_INTERVAL_SEC:
            return {
                'deviation_sec': self._last_deviation,
                'status': self._status,
                'message': f'上次检查: {self._status} (偏差={self._last_deviation:.1f}s)',
            }

        deviation = self._measure_time_deviation()
        self._last_check_time = now
        self._last_deviation = deviation

        with self._lock:
            if abs(deviation) >= self.CRITICAL_THRESHOLD_SEC:
                self._status = 'CRITICAL'
                msg = (f"[DR-P1-12] CRITICAL: 时钟偏差{deviation:.1f}秒超过{self.CRITICAL_THRESHOLD_SEC}秒阈值,"
                       f"建议暂停交易!")
                logging.critical(msg)
            elif abs(deviation) >= self.WARNING_THRESHOLD_SEC:
                self._status = 'WARNING'
                msg = f"[DR-P1-12] WARNING: 时钟偏差{deviation:.1f}秒超过{self.WARNING_THRESHOLD_SEC}秒阈值"
                logging.warning(msg)
            else:
                self._status = 'HEALTHY'
                msg = f"[DR-P1-12] 时钟同步正常,偏差={deviation:.3f}秒"
                logging.info(msg)

            return {
                'deviation_sec': deviation,
                'status': self._status,
                'message': msg,
            }

    def _measure_time_deviation(self) -> float:
        """测量本地时钟与外部时间源的偏差"""
        try:
            # 方法1: 使用HTTP Date头获取外部时间
            # R21-CC-P1-06修复: urllib网络I/O已有timeout=5.0，防止后台线程阻塞
            import urllib.request
            req = urllib.request.Request('http://www.baidu.com', method='HEAD')
            req_start = time.time()
            response = urllib.request.urlopen(req, timeout=5.0)
            req_end = time.time()
            server_date = response.headers.get('Date', '')
            if server_date:
                from email.utils import parsedate_to_datetime
                server_time = parsedate_to_datetime(server_date).timestamp()
                rtt = req_end - req_start
                estimated_server_now = server_time + rtt / 2.0
                local_now = req_end
                return local_now - estimated_server_now
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logging.debug("[DR-P1-12] HTTP时间同步检测失败: %s", e)

        try:
            # 方法2: 使用time.time()自身作为后备
            before = time.time()
            time.sleep(0.01)  # R23-P2-09标记: P2级调度等待
            after = time.time()
            drift = (after - before) - 0.01
            if abs(drift) > 0.1:
                return drift
        except (OSError, IOError, ValueError) as _err:
            logging.debug("[market_time_service] I/O操作降级: %s", _err)

        return 0.0

    def get_status(self) -> str:
        with self._lock:
            return self._status

    def is_critical(self) -> bool:
        return self.get_status() == 'CRITICAL'

    def is_warning(self) -> bool:
        return self.get_status() == 'WARNING'


# 全局单例
_time_sync_checker: Optional[TimeSyncChecker] = None
_time_sync_lock = threading.Lock()


def get_time_sync_checker() -> TimeSyncChecker:
    global _time_sync_checker
    with _time_sync_lock:
        if _time_sync_checker is None:
            _time_sync_checker = TimeSyncChecker()
        return _time_sync_checker


# ============================================================================
# P1 功能恢复：交易日历相关（从 01_constants.py 恢复）
# ============================================================================

# ✅ 删除is_trading_day模块级函数，统一使用MarketTimeService.is_trading_day方法


_market_time_service_instance: Optional['MarketTimeService'] = None
_market_time_service_lock = threading.Lock()

def get_market_time_service() -> 'MarketTimeService':
    global _market_time_service_instance
    if _market_time_service_instance is None:
        with _market_time_service_lock:
            if _market_time_service_instance is None:
                _market_time_service_instance = MarketTimeService()
    return _market_time_service_instance

def is_market_open(exchange: Optional[str] = None) -> bool:
    """市场是否开盘（委托给MarketTimeService）"""
    return get_market_time_service().is_market_open(exchange)

def get_market_status(exchange: str) -> str:
    """获取交易所市场状态: 'OPEN'/'PRE_MARKET'/'CLOSED'（委托给MarketTimeService）"""
    return get_market_time_service().get_market_status(exchange)

# ✅ 删除is_trading_day模块级函数，统一使用MarketTimeService

class MarketTimeService:
    def __init__(self):
        self._sessions = {
            'SHFE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'DCE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'CZCE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'INE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'GFEX': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'CFFEX': [(9, 30, 11, 30), (13, 0, 15, 0)],  # FIX-20260709-CFFEX: 中金所股指期货/期权收盘15:00(非15:15)
        }
        self._night_sessions = {
            # [R22-TIME-P1-11] 夜盘扩展至凌晨02:30，覆盖完整交易时段
            # P2-8修复: 商品期货(SHFE/DCE/CZCE/INE/GFEX)夜盘至次日02:30
            'SHFE': [(21, 0, 23, 0), (0, 0, 2, 30)],
            'DCE': [(21, 0, 23, 0), (0, 0, 2, 30)],
            'CZCE': [(21, 0, 23, 30), (0, 0, 2, 30)],
            'INE': [(21, 0, 23, 0), (0, 0, 2, 30)],
            'GFEX': [(21, 0, 23, 0), (0, 0, 2, 30)],
            # FIX-20260709-CFFEX: 中金所(股指期货/期权/国债期货)无夜盘，删除错误配置
            # 原配置'CFFEX': [(21, 0, 23, 0)]导致21:00-23:00被误判为开盘
        }
        # [R22-TIME-P1-14] 默认节假日数据，防止非交易日判断失效
        self.holidays: set = set()  # 仍为空集合，但提供add_default_holidays方法

    def add_holiday(self, d: datetime.date) -> None:
        """添加节假日"""
        self.holidays.add(d)

    def add_default_holidays(self) -> None:
        """[R22-TIME-P1-14] 添加2026年默认中国法定节假日"""
        import datetime as _dt
        _default_2026 = [
            _dt.date(2026,1,1), _dt.date(2026,1,2), _dt.date(2026,1,3),  # 元旦
            _dt.date(2026,2,17), _dt.date(2026,2,18), _dt.date(2026,2,19),  # 春节
            _dt.date(2026,2,20), _dt.date(2026,2,21), _dt.date(2026,2,22),
            _dt.date(2026,4,4), _dt.date(2026,4,5), _dt.date(2026,4,6),  # 清明
            _dt.date(2026,5,1), _dt.date(2026,5,2), _dt.date(2026,5,3),  # 劳动节
            _dt.date(2026,6,19), _dt.date(2026,6,20), _dt.date(2026,6,21),  # 端午
            _dt.date(2026,10,1), _dt.date(2026,10,2), _dt.date(2026,10,3),  # 国庆
            _dt.date(2026,10,4), _dt.date(2026,10,5), _dt.date(2026,10,6),
            _dt.date(2026,10,7), _dt.date(2026,10,8),
        ]
        self.holidays.update(_default_2026)

    def is_trading_day(self, target_date: datetime.date, holiday_dates: Optional[set] = None) -> bool:
        """
        判断是否为交易日

        Args:
            target_date: 目标日期
            holiday_dates: 额外的节假日集合（可选）

        Returns:
            bool: 是否为交易日
        """
        if target_date.weekday() >= 5:  # 周末
            return False

        # 检查内部节假日
        if target_date in self.holidays:
            return False

        # 检查外部传入的节假日
        if holiday_dates and target_date in holiday_dates:
            return False

        return True

    def get_market_status(self, exchange: str) -> str:
        """返回交易所当前状态: 'OPEN'(交易中), 'PRE_MARKET'(尚未开盘), 'CLOSED'(已收盘)

        FIX-20260709: 区分PRE_MARKET和CLOSED，避免9:29:57被误判为"已收盘"。
        PRE_MARKET状态下策略应短暂等待而非延后重试。
        """
        from infra.shared_utils import CHINA_TZ
        now = datetime.now(CHINA_TZ)
        now_time = now.time()
        # 收集该交易所所有时段的起止时间
        all_sessions = list(self._sessions.get(exchange, []))
        all_sessions.extend(self._night_sessions.get(exch, []) for exch in [exchange] if exchange in self._night_sessions)
        # 展平夜盘
        night_sessions = self._night_sessions.get(exchange, [])
        all_sessions = list(self._sessions.get(exchange, [])) + list(night_sessions)
        # 判断当前是否在某个交易时段内
        for start_h, start_m, end_h, end_m in all_sessions:
            start_time = dt_time(start_h, start_m)
            end_time = dt_time(end_h, end_m)
            if start_time <= end_time:
                if start_time <= now_time <= end_time:
                    return 'OPEN'
            else:
                # 跨午夜时段(如21:00-次日02:30)
                if now_time >= start_time or now_time <= end_time:
                    return 'OPEN'
        # 不在任何交易时段内，判断是PRE_MARKET还是CLOSED
        # PRE_MARKET: 当前时间 < 今天第一个交易时段的开始时间
        if all_sessions:
            first_start = min(dt_time(s[0], s[1]) for s in all_sessions
                              if dt_time(s[0], s[1]) <= dt_time(s[2], s[3]))  # 排除跨午夜
            # 夜盘21:00开始的也应纳入比较
            night_starts = [dt_time(s[0], s[1]) for s in night_sessions
                           if dt_time(s[0], s[1]) > dt_time(s[2], s[3])]
            # 日盘开盘前的时段（如9:00或9:30之前）算PRE_MARKET
            day_starts = [dt_time(s[0], s[1]) for s in self._sessions.get(exchange, [])]
            earliest_day = min(day_starts) if day_starts else first_start
            if now_time < earliest_day:
                return 'PRE_MARKET'
        return 'CLOSED'

    def is_market_open(self, exchange: Optional[str] = None) -> bool:
        from infra.shared_utils import CHINA_TZ
        now = datetime.now(CHINA_TZ)
        now_time = now.time()
        exchanges = [exchange] if exchange else list(self._sessions.keys())
        result = False
        for exch in exchanges:
            sessions = self._sessions.get(exch, [])
            for start_h, start_m, end_h, end_m in sessions:
                start_time = dt_time(start_h, start_m)
                end_time = dt_time(end_h, end_m)
                if start_time <= now_time <= end_time:
                    result = True
                    break
            if result:
                break
            night_sessions = self._night_sessions.get(exch, [])
            for start_h, start_m, end_h, end_m in night_sessions:
                start_time = dt_time(start_h, start_m)
                end_time = dt_time(end_h, end_m)
                if start_time <= end_time:
                    if start_time <= now_time <= end_time:
                        result = True
                        break
                else:
                    if now_time >= start_time or now_time <= end_time:
                        result = True
                        break
            if result:
                break
        self._maybe_report_market_status(exchange, result)
        return result

    _last_market_status_time = 0.0
    _MARKET_STATUS_INTERVAL_SEC = 3600.0

    def _maybe_report_market_status(self, exchange: Optional[str], is_open: bool) -> None:
        now = time.time()
        if now - self._last_market_status_time < self._MARKET_STATUS_INTERVAL_SEC:
            return
        self.__class__._last_market_status_time = now
        exch_name = exchange or 'ALL'
        status_str = 'OPEN' if is_open else 'CLOSED'
        logging.info("[Observability] 市场状态(每小时): exchange=%s status=%s", exch_name, status_str)


# ============================================================================
# FIX-MARKET-CLOSE-20260720: 市场开收盘状态缓存
# ============================================================================
# 用户要求: "只在固定时间（开盘/收市）收市门控判断"
# 设计原则:
#   1. onTick 只读缓存，零开销（避免每tick调用 is_market_open()）
#   2. 常规30秒刷新一次
#   3. 固定时间点（开盘/收盘）±5分钟窗口内使用5秒刷新
#   4. 初始化失败时默认返回 True（开盘），避免漏处理tick
# ============================================================================

class MarketOpenCache:
    """市场开收盘状态缓存 — onTick调用零开销

    FIX-MARKET-CLOSE-20260720
    根因: strategy_2026.py onTick入口没有市场时间门控，
          15:00收市后PQS更新/信号生成/诊断日志仍持续运行4分钟以上。
          现有门控(tick_dispatch.py L758)每tick调用 is_market_open() 性能差。
    修复:
      1. 本类提供缓存式 is_open() 接口，onTick 只读缓存
      2. 后台定时刷新（常规30秒，固定时间点附近5秒）
      3. 固定时间点覆盖所有交易所开盘/收盘:
         09:00(SHFE/DCE/CZCE/INE/GFEX开盘)
         09:30(CFFEX开盘)
         10:15(日盘休盘)
         10:30(日盘复盘)
         11:30(上午收盘)
         13:00(CFFEX下午开盘)
         13:30(SHFE等下午开盘)
         15:00(全部收盘)
         21:00(夜盘开盘)
         23:00(SHFE等夜盘休盘)
         23:30(CZCE夜盘休盘)
    """

    _instance: Optional['MarketOpenCache'] = None
    _lock = threading.Lock()

    # 类级常量（避免实例化时重建）
    _REFRESH_INTERVAL_NORMAL = 30.0       # 常规30秒刷新
    _REFRESH_INTERVAL_CRITICAL = 5.0      # 固定时间点附近5秒刷新
    _CRITICAL_WINDOW_MINUTES = 5          # ±5分钟窗口

    # 全部交易所开盘/收盘时间点 (hour, minute)
    _CRITICAL_WINDOWS = [
        (9, 0), (9, 30), (10, 15), (10, 30), (11, 30),
        (13, 0), (13, 30), (15, 0),
        (21, 0), (23, 0), (23, 30),
    ]

    def __init__(self):
        # 默认 True（开盘），避免启动时漏处理tick
        # 若启动时实际已收盘，第一次刷新后会立即纠正
        self._cache: bool = True
        self._last_refresh: float = 0.0
        self._refresh_count: int = 0
        self._last_status_change: float = 0.0
        self._prev_cache: bool = True

    def is_open(self) -> bool:
        """onTick调用 — 只读缓存，零开销

        若距上次刷新超过间隔则触发刷新。
        刷新失败时保留原值（不抛异常）。
        """
        try:
            now = time.time()
            if now - self._last_refresh >= self._get_refresh_interval():
                self._refresh()
        except Exception:
            # 任何异常都保留当前缓存值，避免 onTick 崩溃
            pass
        return self._cache

    def is_closed(self) -> bool:
        """便捷方法 — 市场是否已收盘"""
        return not self.is_open()

    def force_refresh(self) -> bool:
        """强制刷新缓存（用于关键时间点触发）"""
        self._refresh()
        return self._cache

    def get_status(self) -> Dict[str, Any]:
        """获取缓存详细状态（诊断用）"""
        return {
            'is_open': self._cache,
            'last_refresh': self._last_refresh,
            'refresh_count': self._refresh_count,
            'last_status_change': self._last_status_change,
            'current_interval': self._get_refresh_interval(),
        }

    def _get_refresh_interval(self) -> float:
        """根据当前时间判断刷新间隔

        在固定时间点±5分钟窗口内使用5秒刷新，否则使用30秒刷新。
        """
        try:
            from infra.shared_utils import CHINA_TZ
            now = datetime.now(CHINA_TZ)
            now_minutes = now.hour * 60 + now.minute
            for ch, cm in self._CRITICAL_WINDOWS:
                target_minutes = ch * 60 + cm
                # 处理跨午夜（如23:30 vs 00:10）
                diff = abs(now_minutes - target_minutes)
                diff = min(diff, 24 * 60 - diff)
                if diff <= self._CRITICAL_WINDOW_MINUTES:
                    return self._REFRESH_INTERVAL_CRITICAL
        except Exception:
            pass
        return self._REFRESH_INTERVAL_NORMAL

    def _refresh(self) -> None:
        """刷新缓存 — 调用 MarketTimeService.is_market_open()

        失败时保留原值，不抛异常。
        """
        try:
            new_value = get_market_time_service().is_market_open()
            if new_value != self._cache:
                self._prev_cache = self._cache
                self._cache = new_value
                self._last_status_change = time.time()
                # 状态变化时输出日志（开盘→收盘 / 收盘→开盘）
                status_str = 'OPEN' if new_value else 'CLOSED'
                logging.info(
                    "[FIX-MARKET-CLOSE-20260720] 市场状态切换: %s->%s (refresh_count=%d)",
                    'OPEN' if self._prev_cache else 'CLOSED',
                    status_str,
                    self._refresh_count + 1,
                )
            self._last_refresh = time.time()
            self._refresh_count += 1
        except Exception as _refresh_err:
            # 刷新失败保留原值，记录debug日志
            logging.debug("[MarketOpenCache] 刷新失败(保留原值=%s): %s", self._cache, _refresh_err)


# MarketOpenCache 单例
_market_open_cache_instance: Optional[MarketOpenCache] = None
_market_open_cache_lock = threading.Lock()


def get_market_open_cache() -> MarketOpenCache:
    """获取 MarketOpenCache 单例

    FIX-MARKET-CLOSE-20260720: 提供 onTick 调用的零开销市场状态判断。
    """
    global _market_open_cache_instance
    if _market_open_cache_instance is None:
        with _market_open_cache_lock:
            if _market_open_cache_instance is None:
                _market_open_cache_instance = MarketOpenCache()
    return _market_open_cache_instance


# ============================================================================
# 模块导出
# ============================================================================

__all__ = [
    'MarketTimeService',
    'MarketOpenCache',
    'is_market_open',
    'get_market_status',
    'get_market_time_service',
    'get_market_open_cache',
    'TimeSyncChecker',
    'get_time_sync_checker',
]
