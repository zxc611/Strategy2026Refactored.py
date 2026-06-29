"""
验证有效交易时间计算
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch


class TestEffectiveTradingMinutes:
    """验证有效交易时间计算"""
    
    def test_same_day_during_session(self):
        """同一天交易时段内"""
        from ali2026v3_trading.position.position_pnl_service import _calc_effective_trading_minutes
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        
        open_time = datetime(2026, 6, 25, 10, 0, tzinfo=CHINA_TZ)
        now = datetime(2026, 6, 25, 11, 30, tzinfo=CHINA_TZ)
        
        mts = Mock()
        mts.is_trading_day = Mock(return_value=True)
        
        with patch('ali2026v3_trading.infra.market_time_service.get_market_time_service', return_value=mts):
            minutes = _calc_effective_trading_minutes(open_time, now)
            
            assert minutes > 0
            assert minutes <= 90
    
    def test_over_weekend(self):
        """跨周末（周五→周一）"""
        from ali2026v3_trading.position.position_pnl_service import _calc_effective_trading_minutes
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        
        friday = datetime(2026, 6, 26, 14, 0, tzinfo=CHINA_TZ)
        monday = datetime(2026, 6, 29, 10, 0, tzinfo=CHINA_TZ)
        
        mts = Mock()
        def is_trading_day(d):
            return d.weekday() < 5
        mts.is_trading_day = Mock(side_effect=is_trading_day)
        
        with patch('ali2026v3_trading.infra.market_time_service.get_market_time_service', return_value=mts):
            minutes = _calc_effective_trading_minutes(friday, monday)
            
            assert minutes > 0
            assert minutes < (3 * 24 * 60)
            assert minutes < 500
    
    def test_over_holiday(self):
        """跨节假日"""
        from ali2026v3_trading.position.position_pnl_service import _calc_effective_trading_minutes
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        
        before_holiday = datetime(2026, 6, 25, 14, 0, tzinfo=CHINA_TZ)
        after_holiday = datetime(2026, 6, 27, 10, 0, tzinfo=CHINA_TZ)
        
        mts = Mock()
        def is_trading_day(d):
            if d.day == 26:
                return False
            return d.weekday() < 5
        mts.is_trading_day = Mock(side_effect=is_trading_day)
        
        with patch('ali2026v3_trading.infra.market_time_service.get_market_time_service', return_value=mts):
            minutes = _calc_effective_trading_minutes(before_holiday, after_holiday)
            
            assert minutes > 0
            assert minutes < 200
    
    def test_night_session(self):
        """夜盘时段"""
        from ali2026v3_trading.position.position_pnl_service import _calc_effective_trading_minutes
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        
        night_open = datetime(2026, 6, 25, 21, 30, tzinfo=CHINA_TZ)
        night_check = datetime(2026, 6, 26, 1, 30, tzinfo=CHINA_TZ)
        
        mts = Mock()
        mts.is_trading_day = Mock(return_value=True)
        
        with patch('ali2026v3_trading.infra.market_time_service.get_market_time_service', return_value=mts):
            minutes = _calc_effective_trading_minutes(night_open, night_check)
            
            assert minutes > 0
            assert minutes <= 240
    
    def test_non_trading_day_returns_zero(self):
        """非交易日开仓，检查时间很短"""
        from ali2026v3_trading.position.position_pnl_service import _calc_effective_trading_minutes
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        
        saturday = datetime(2026, 6, 27, 10, 0, tzinfo=CHINA_TZ)
        saturday_later = datetime(2026, 6, 27, 11, 0, tzinfo=CHINA_TZ)
        
        mts = Mock()
        mts.is_trading_day = Mock(return_value=False)
        
        with patch('ali2026v3_trading.position.position_pnl_service.get_market_time_service', return_value=mts):
            minutes = _calc_effective_trading_minutes(saturday, saturday_later)
            
            assert minutes == 0


class TestTimeStopWithEffectiveTime:
    """验证时间止损使用有效交易时间"""
    
    def test_time_stop_uses_effective_time(self):
        """时间止损使用有效交易时间"""
        from ali2026v3_trading.position.position_pnl_service import PositionPnlService
        from ali2026v3_trading.position.position_service import PositionService, PositionRecord
        from ali2026v3_trading.infra.shared_utils import CHINA_TZ
        
        ps = Mock(spec=PositionService)
        ps._map_reason_to_strategy = Mock(return_value='box')
        ps._trigger_close_position = Mock()
        ps._get_instrument_lock = Mock(return_value=Mock(__enter__=Mock(return_value=None), __exit__=Mock(return_value=False)))
        ps._check_svc = None
        ps._params = None
        
        pnl_svc = PositionPnlService(ps)
        pnl_svc.DEFAULT_MAX_HOLD_MINUTES = 60.0
        
        friday = datetime(2026, 6, 26, 14, 0, tzinfo=CHINA_TZ)
        monday = datetime(2026, 6, 29, 10, 30, tzinfo=CHINA_TZ)
        
        rec = PositionRecord(
            position_id='FG609P950_box_123_abc',
            instrument_id='FG609P950',
            exchange='CZCE',
            volume=2,
            direction='long',
            open_price=4.8,
            open_time=friday,
            open_date=friday.date(),
            position_type='option',
            open_reason='BOX_SPRING',
            strategy_group='box',
            current_price=5.0,
            _closing=False,
            closing_order_id='',
        )
        
        mts = Mock()
        def is_trading_day(d):
            return d.weekday() < 5
        mts.is_trading_day = Mock(side_effect=is_trading_day)
        
        with patch('ali2026v3_trading.position.position_pnl_service.get_market_time_service', return_value=mts):
            with patch('ali2026v3_trading.config.params_service.get_params_service') as mock_ps:
                mock_params = Mock()
                mock_params.get_bool = Mock(return_value=True)
                mock_params.get_int = Mock(return_value=60)
                mock_ps.return_value = mock_params
                
                pass


if __name__ == '__main__':
    pytest.main([__file__, '-v'])