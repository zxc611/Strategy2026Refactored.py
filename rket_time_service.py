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

# ✅ 删除is_trading_day模块级函数，统一使用MarketTimeService

class MarketTimeService:
    def __init__(self):
        self._sessions = {
            'SHFE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'DCE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'CZCE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'INE': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'GFEX': [(9, 0, 10, 15), (10, 30, 11, 30), (13, 30, 15, 0)],
            'CFFEX': [(9, 30, 11, 30), (13, 0, 15, 15)],  # P2-8修复: 中金所日盘时段
        }
        self._night_sessions = {
            # [R22-TIME-P1-11] 夜盘扩展至凌晨02:30，覆盖完整交易时段
            # P2-8修复: 商品期货(SHFE/DCE/CZCE/INE/GFEX)夜盘至次日02:30
            'SHFE': [(21, 0, 23, 0), (0, 0, 2, 30)],
            'DCE': [(21, 0, 23, 0), (0, 0, 2, 30)],
            'CZCE': [(21, 0, 23, 30), (0, 0, 2, 30)],
            'INE': [(21, 0, 23, 0), (0, 0, 2, 30)],
            'GFEX': [(21, 0, 23, 0), (0, 0, 2, 30)],
            # P2-8修复: 金融期货(CFFEX)夜盘至23:00，无次日凌晨段
            'CFFEX': [(21, 0, 23, 0)],
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

    def is_market_open(self, exchange: Optional[str] = None) -> bool:
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
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
# 模块导出
# ============================================================================

__all__ = [
    'MarketTimeService',
    'is_market_open',
    'get_market_time_service',
    'TimeSyncChecker',
    'get_time_sync_checker',
]
