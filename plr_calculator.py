import threading
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field


@dataclass
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

    def update_from_trades(self, strategy_id: str, trades: List[Dict[str, Any]]) -> PLRResult:
        with self._lock:
            win_count = 0
            loss_count = 0
            win_pnl_sum = 0.0
            loss_pnl_sum = 0.0
            for t in trades:
                pnl = t.get('pnl', t.get('net_pnl', 0.0))
                if pnl > 0:
                    win_count += 1
                    win_pnl_sum += pnl
                elif pnl < 0:
                    loss_count += 1
                    loss_pnl_sum += abs(pnl)

            target_plr = self._default_target_plr
            current_plr = 0.0
            if loss_count > 0 and loss_pnl_sum > 0:
                current_plr = win_pnl_sum / loss_pnl_sum
            elif win_count > 0 and loss_count == 0:
                current_plr = float('inf')

            plr_ratio = current_plr / target_plr if target_plr > 0 else 0.0

            if current_plr == float('inf'):
                plr_status = 'all_wins'
            elif plr_ratio >= 1.5:
                plr_status = 'excellent'
            elif plr_ratio >= 1.0:
                plr_status = 'on_target'
            elif plr_ratio >= 0.5:
                plr_status = 'below_target'
            else:
                plr_status = 'critical'

            profit_factor = win_pnl_sum / loss_pnl_sum if loss_pnl_sum > 0 else (float('inf') if win_pnl_sum > 0 else 0.0)
            avg_win = win_pnl_sum / win_count if win_count > 0 else 0.0
            avg_loss = loss_pnl_sum / loss_count if loss_count > 0 else 0.0
            win_loss_ratio = avg_win / avg_loss if avg_loss > 0 else (float('inf') if avg_win > 0 else 0.0)

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

    def estimate_plr(self, potential_gain: float, potential_loss: float) -> float:
        if potential_loss < 1e-10:
            return 0.0
        return potential_gain / potential_loss

    def get_plr_status(self, strategy_id: str) -> Optional[PLRResult]:
        with self._lock:
            stats = self._strategy_stats.get(strategy_id)
            if stats is None:
                return None
            return PLRResult(**stats)

    def set_target_plr(self, strategy_id: str, target_plr: float) -> None:
        with self._lock:
            if strategy_id in self._strategy_stats:
                self._strategy_stats[strategy_id]['target_plr'] = target_plr

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in self._strategy_stats.items()}


_plr_calculator_instance: Optional[ProfitLossRatioCalculator] = None
_plr_calculator_lock = threading.Lock()


def get_plr_calculator(default_target_plr: float = 2.0) -> ProfitLossRatioCalculator:
    global _plr_calculator_instance
    with _plr_calculator_lock:
        if _plr_calculator_instance is None:
            _plr_calculator_instance = ProfitLossRatioCalculator(default_target_plr=default_target_plr)
    return _plr_calculator_instance
