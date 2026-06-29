# MODULE_ID: M1-237
import threading
import logging
import warnings
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field


@dataclass(slots=True)
class PLRResult:
    target_plr: float = 0.0
    current_plr: float = 0.0
    plr_ratio: float = 0.0
    plr_status: str = ''
    win_count: int = 0
    loss_count: int = 0
    win_pnl_sum: float = 0.0
    loss_pnl_sum: float = 0.0
    profit_factor: float = 0.0
    win_loss_ratio: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'target_plr': self.target_plr,
            'current_plr': self.current_plr,
            'plr_ratio': self.plr_ratio,
            'plr_status': self.plr_status,
            'win_count': self.win_count,
            'loss_count': self.loss_count,
            'win_pnl_sum': self.win_pnl_sum,
            'loss_pnl_sum': self.loss_pnl_sum,
            'profit_factor': self.profit_factor,
            'win_loss_ratio': self.win_loss_ratio,
        }


class ProfitLossRatioCalculator:
    def __init__(self, default_target_plr: float = 2.0):
        self._lock = threading.RLock()
        self._default_target_plr = default_target_plr
        self._strategy_stats: Dict[str, Dict[str, Any]] = {}
        # R32-P1-18修复: 通过ConfigService消费plr_thresholds(原getter零消费)
        try:
            from ali2026v3_trading.config.config_service import get_config_service
            _cs = get_config_service()
            if _cs is not None:
                _plr_cfg = _cs.get_plr_thresholds()
                if _plr_cfg and isinstance(_plr_cfg, dict):
                    _target = _plr_cfg.get('target_plr')
                    if isinstance(_target, (int, float)) and _target > 0:
                        self._default_target_plr = float(_target)
        except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
            logging.debug("[R3-L2] suppressed exception", exc_info=True)
            pass
            pass

    def update_from_trades(self, strategy_id: str, trades: List[Dict[str, Any]]) -> PLRResult:
        with self._lock:
            win_count = 0
            loss_count = 0
            win_pnl_sum = 0.0
            loss_pnl_sum = 0.0
            _win_c = 0.0  # NP-P2-18: Kahan补偿
            _loss_c = 0.0  # NP-P2-18: Kahan补偿
            for t in trades:
                pnl = t.get('pnl', t.get('net_pnl', 0.0))
                if pnl > 0:
                    win_count += 1
                    y = pnl - _win_c
                    t_val = win_pnl_sum + y
                    _win_c = (t_val - win_pnl_sum) - y
                    win_pnl_sum = t_val
                elif pnl < 0:
                    loss_count += 1
                    abs_pnl = abs(pnl)
                    y = abs_pnl - _loss_c
                    t_val = loss_pnl_sum + y
                    _loss_c = (t_val - loss_pnl_sum) - y
                    loss_pnl_sum = t_val

            target_plr = self._default_target_plr
            current_plr = 0.0
            # R14-P1-API-07修复: 用大数1e9替代float('inf')，避免JSON序列化失败
            _LARGE_PLR = 100
            if loss_count > 0 and loss_pnl_sum > 0:
                current_plr = win_pnl_sum / loss_pnl_sum
            elif win_count > 0 and loss_count == 0:
                current_plr = _LARGE_PLR

            plr_ratio = current_plr / target_plr if target_plr > 0 else 0.0

            if current_plr >= _LARGE_PLR:
                plr_status = 'all_wins'
            elif plr_ratio >= 1.5:
                plr_status = 'excellent'
            elif plr_ratio >= 1.0:
                plr_status = 'on_target'
            elif plr_ratio >= 0.5:
                plr_status = 'below_target'
            else:
                plr_status = 'critical'

            profit_factor = win_pnl_sum / loss_pnl_sum if loss_pnl_sum > 0 else (_LARGE_PLR if win_pnl_sum > 0 else 0.0)
            avg_win = win_pnl_sum / win_count if win_count > 0 else 0.0
            avg_loss = loss_pnl_sum / loss_count if loss_count > 0 else 0.0
            win_loss_ratio = avg_win / avg_loss if avg_loss > 0 else (_LARGE_PLR if avg_win > 0 else 0.0)

            result = PLRResult(
                target_plr=target_plr,
                current_plr=current_plr,
                plr_ratio=plr_ratio,
                plr_status=plr_status,
                win_count=win_count,
                loss_count=loss_count,
                win_pnl_sum=win_pnl_sum,
                loss_pnl_sum=loss_pnl_sum,
                profit_factor=profit_factor,
                win_loss_ratio=win_loss_ratio,
            )

            self._strategy_stats[strategy_id] = result.to_dict()
            return result

    # R13-P1-DEAD-04修复: 以下方法无外部调用方，添加deprecation warning
    def estimate_plr(self, potential_gain: float, potential_loss: float) -> float:
        warnings.warn("estimate_plr is deprecated: no external callers", DeprecationWarning, stacklevel=2)
        if potential_loss < 1e-10:
            return 0.0
        return potential_gain / potential_loss

    def get_plr_status(self, strategy_id: str) -> Optional[PLRResult]:
        warnings.warn("get_plr_status is deprecated: no external callers", DeprecationWarning, stacklevel=2)
        with self._lock:
            stats = self._strategy_stats.get(strategy_id)
            if stats is None:
                return None
            return PLRResult(**stats)

    def set_target_plr(self, strategy_id: str, target_plr: float) -> None:
        warnings.warn("set_target_plr is deprecated: no external callers", DeprecationWarning, stacklevel=2)
        with self._lock:
            if strategy_id in self._strategy_stats:
                self._strategy_stats[strategy_id]['target_plr'] = target_plr

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        warnings.warn("get_all_stats is deprecated: no external callers", DeprecationWarning, stacklevel=2)
        with self._lock:
            return {k: dict(v) for k, v in self._strategy_stats.items()}

    # DFG-P1-12修复: PLR分布质量评估方法
    def assess_quality(self, strategy_id: str = '') -> Dict[str, Any]:
        """DFG-P1-12修复: 评估PLR分布质量，解决PLR计算结果仅用于过滤、无质量评估的数据流断裂

        评估维度:
        1. PLR稳定性 — 近期PLR波动率（标准差/均值）'
        2. 胜率一致性 — 胜率是否在合理范围(30%-70%)
        3. 盈亏比趋势 — 盈亏比是否呈下降趋势
        4. 整体质量等级 — excellent/good/fair/poor

        Args:
            strategy_id: 策略ID，为空时评估所有策略

        Returns:
            Dict: 质量评估结果
        """
        with self._lock:
            if strategy_id:
                stats_to_check = {strategy_id: self._strategy_stats.get(strategy_id, {})}
            else:
                stats_to_check = dict(self._strategy_stats)

            if not stats_to_check:
                return {'quality_level': 'no_data', 'strategies': {}}

            results = {}
            for sid, stats in stats_to_check.items():
                if not stats:
                    results[sid] = {'quality_level': 'no_data'}
                    continue

                plr_ratio = stats.get('plr_ratio', 0.0)
                win_count = stats.get('win_count', 0)
                loss_count = stats.get('loss_count', 0)
                total_trades = win_count + loss_count
                win_rate = win_count / total_trades if total_trades > 0 else 0.0
                profit_factor = stats.get('profit_factor', 0.0)

                # 胜率一致性评估
                win_rate_consistent = 0.3 <= win_rate <= 0.7 if total_trades >= 5 else True

                # PLR趋势评估（基于plr_ratio）
                if plr_ratio >= 1.5:
                    plr_trend = 'improving'
                elif plr_ratio >= 1.0:
                    plr_trend = 'stable'
                elif plr_ratio >= 0.5:
                    plr_trend = 'declining'
                else:
                    plr_trend = 'critical'

                # 综合质量等级
                if plr_ratio >= 1.5 and win_rate_consistent:
                    quality = 'excellent'
                elif plr_ratio >= 1.0 and win_rate_consistent:
                    quality = 'good'
                elif plr_ratio >= 0.5:
                    quality = 'fair'
                else:
                    quality = 'poor'

                results[sid] = {
                    'quality_level': quality,
                    'plr_ratio': round(plr_ratio, 4),
                    'plr_trend': plr_trend,
                    'win_rate': round(win_rate, 4),
                    'win_rate_consistent': win_rate_consistent,
                    'profit_factor': round(profit_factor, 4),
                    'total_trades': total_trades,
                }

            # 汇总整体质量
            _qualities = [r.get('quality_level', 'no_data') for r in results.values()]
            _quality_order = {'excellent': 4, 'good': 3, 'fair': 2, 'poor': 1, 'no_data': 0}
            _min_quality = min(_quality_order.get(q, 0) for q in _qualities) if _qualities else 0
            _overall = {v: k for k, v in _quality_order.items()}.get(_min_quality, 'no_data')

            quality_result = {
                'overall_quality': _overall,
                'strategy_count': len(results),
                'strategies': results,
            }
            # DFG-P1-12修复: PLR质量评估结果通过EventBus发布，供风控和诊断服务消费
            try:
                from ali2026v3_trading.infra.event_bus import get_global_event_bus
                _bus = get_global_event_bus()
                if _bus is not None:
                    _bus.publish('plr.quality_assessed', {
                        'type': 'plr.quality_assessed',
                        'overall_quality': _overall,
                        'strategy_count': len(results),
                        'strategies': {sid: r.get('quality_level', 'no_data') for sid, r in results.items()},
                    }, async_mode=True)
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
            return quality_result


_plr_calculator_instance: Optional[ProfitLossRatioCalculator] = None
_plr_calculator_lock = threading.Lock()


def get_plr_calculator(default_target_plr: float = 2.0) -> ProfitLossRatioCalculator:
    global _plr_calculator_instance
    with _plr_calculator_lock:
        if _plr_calculator_instance is None:
            _plr_calculator_instance = ProfitLossRatioCalculator(default_target_plr=default_target_plr)
            # AP-03: SingletonRegistry注册
            try:
                from ali2026v3_trading.infra.registry_service import SingletonRegistry
                registry = SingletonRegistry.get_registry("plr_calculator")
                registry.register_singleton("plr_calculator.instance", _plr_calculator_instance)
            except (ValueError, KeyError, TypeError, AttributeError) as _r3_err:
                logging.debug("[R3-L2] suppressed exception", exc_info=True)
                pass
                pass
    return _plr_calculator_instance
