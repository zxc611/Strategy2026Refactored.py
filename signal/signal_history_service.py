"""
信号历史服务 — 从signal_service.py拆分
职责: 信号历史记录、统计、查询、日报生成
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from collections import deque


class SignalHistoryService:
    def __init__(self, max_history: int = 10000):
        self._history: deque = deque(maxlen=max_history)
        self._signal_counts: Dict[str, int] = {}
        self._rejected_counts: Dict[str, int] = {}
        self._daily_report_generated: Dict[str, bool] = {}
        self._log_dir: str = ''

    def set_log_dir(self, log_dir: str) -> None:
        self._log_dir = log_dir

    def record_signal(self, signal: Dict[str, Any]) -> None:
        signal['_record_time'] = time.time()
        self._history.append(signal)
        reason = signal.get('reason', 'unknown')
        self._signal_counts[reason] = self._signal_counts.get(reason, 0) + 1

    def record_rejection(self, reason: str) -> None:
        self._rejected_counts[reason] = self._rejected_counts.get(reason, 0) + 1

    def get_recent(self, n: int = 100) -> List[Dict[str, Any]]:
        return list(self._history)[-n:]

    def get_statistics(self) -> Dict[str, Any]:
        return {
            'total_signals': len(self._history),
            'signal_counts': dict(self._signal_counts),
            'rejected_counts': dict(self._rejected_counts),
        }

    def clear(self) -> None:
        self._history.clear()
        self._signal_counts.clear()
        self._rejected_counts.clear()

    def generate_daily_signal_report(self, date: Optional[str] = None,
                                     market_close_hour: int = 15,
                                     market_close_minute_start: int = 15,
                                     market_close_minute_end: int = 20) -> Optional[str]:
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        if date is None:
            date = datetime.now(CHINA_TZ).strftime("%Y-%m-%d")
        if self._daily_report_generated.get(date):
            return None
        date_signals = [
            s for s in list(self._history)
            if isinstance(s.get('generated_at'), datetime) and s['generated_at'].strftime("%Y-%m-%d") == date
        ]
        if not date_signals:
            return None
        buy_signals = [s for s in date_signals if s.get('signal_type') in ('BUY',)]
        sell_signals = [s for s in date_signals if s.get('signal_type') in ('SELL',)]
        close_long = [s for s in date_signals if s.get('signal_type') == 'CLOSE_LONG']
        close_short = [s for s in date_signals if s.get('signal_type') == 'CLOSE_SHORT']
        lines = [
            "=" * 80,
            f"当日信号明细报告 - {date}",
            "=" * 80,
            f"总信号数: {len(date_signals)}",
            f"  买入信号: {len(buy_signals)}",
            f"  卖出信号: {len(sell_signals)}",
            f"  平多信号: {len(close_long)}",
            f"  平空信号: {len(close_short)}",
            "-" * 80,
            "信号明细:",
        ]
        for i, s in enumerate(date_signals, 1):
            ts = s['generated_at'].strftime('%H:%M:%S') if isinstance(s.get('generated_at'), datetime) else str(s.get('generated_at', ''))
            lines.extend([
                f"【信号 {i}】",
                f"  时间: {ts}",
                f"  合约: {s.get('instrument_id', '')}",
                f"  类型: {s.get('signal_type', '')}",
                f"  价格: {s.get('price', 0)}",
                f"  数量: {s.get('volume', 0)}",
                f"  原因: {s.get('reason', '')}",
                f"  信号ID: {s.get('signal_id', '')}",
            ])
        lines.extend([
            "=" * 80,
            f"报告生成时间: {datetime.now(CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80,
        ])
        report = "\n".join(lines)
        try:
            os.makedirs(self._log_dir, exist_ok=True)
            report_file = os.path.join(self._log_dir, f"signal_daily_report_{date}.txt")
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            json_file = os.path.join(self._log_dir, f"signal_daily_report_{date}.json")
            serializable_signals = []
            for s in date_signals:
                entry = dict(s)
                if isinstance(entry.get('generated_at'), datetime):
                    entry['generated_at'] = entry['generated_at'].isoformat()
                serializable_signals.append(entry)
            report_data = {
                'date': date,
                'total': len(date_signals),
                'buy': len(buy_signals),
                'sell': len(sell_signals),
                'close_long': len(close_long),
                'close_short': len(close_short),
                'signals': serializable_signals,
            }
            with open(json_file, 'w', encoding='utf-8') as f:
                try:
                    from ali2026v3_trading.serialization_utils import json_dumps
                    f.write(json_dumps(report_data, indent=2))
                except ImportError:
                    def _fallback_default(obj):
                        import math as _math
                        import datetime as _dt
                        import decimal as _decimal
                        if isinstance(obj, float):
                            if _math.isnan(obj):
                                return {"__special__": "NaN"}
                            if _math.isinf(obj):
                                return {"__special__": "Infinity" if obj > 0 else "-Infinity"}
                        if isinstance(obj, (_dt.datetime, _dt.date)):
                            return obj.isoformat()
                        if isinstance(obj, _decimal.Decimal):
                            return {"__decimal__": str(obj)}
                        return str(obj)
                    json.dump(report_data, f, indent=2, default=_fallback_default, ensure_ascii=False)
            self._daily_report_generated[date] = True
            logging.info("[SignalService] 日信号报告已生成: %s", report_file)
        except Exception as e:
            logging.warning("[SignalService] 生成日信号报告失败: %s", e)
        return report

    def check_market_close_and_report(self,
                                       market_close_hour: int = 15,
                                       market_close_minute_start: int = 15,
                                       market_close_minute_end: int = 20) -> Optional[str]:
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        now = datetime.now(CHINA_TZ)
        current_time = now.time()
        from datetime import time as dt_time
        close_time = dt_time(market_close_hour, market_close_minute_start)
        check_end = dt_time(market_close_hour, market_close_minute_end)
        if close_time <= current_time <= check_end:
            today = now.strftime("%Y-%m-%d")
            return self.generate_daily_signal_report(today,
                                                      market_close_hour=market_close_hour,
                                                      market_close_minute_start=market_close_minute_start,
                                                      market_close_minute_end=market_close_minute_end)
        return None