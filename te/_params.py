# MODULE_ID: M1-149
"""V5 pre-computation parameter centralised configuration."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Tuple


@dataclass
class KL_RPD_Params:
    ema_period: int = 20
    atr_period: int = 14
    atr_mult: float = 2.0
    rv_period: int = 20
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9


@dataclass
class SignalParams:
    s1_weights: Tuple[float, float, float] = (0.4, 0.3, 0.3)
    s2_bb_period: int = 20
    s3_lookback: int = 60
    s4_short_period: int = 5
    s6_max_drawdown_pct: float = 3.0


@dataclass
class DecayParams:
    decay_lambda: float = 0.02
    age_threshold: float = 0.1
    max_initial_age_minutes: float = 1440.0
    linkage_weights: Tuple[Tuple[float, ...], ...] = (
            (1.0, 0.3, 0.1, 0.5),
            (0.3, 1.0, 0.4, 0.2),
            (0.1, 0.4, 1.0, 0.1),
            (0.5, 0.2, 0.1, 1.0),
        )


@dataclass
class L0StateParams:
    smoothing_alpha: float = 0.15
    min_duration: int = 3
    entropy_window: int = 20


@dataclass
class HMMParams:
    n_states: int = 3
    window: int = 60


@dataclass
class TrendParams:
    periods: Tuple[int, int, int] = (20, 60, 240)


@dataclass
class OBOSParams:
    rsi_period: int = 14
    stoch_period: int = 14
    cci_period: int = 20
    williams_period: int = 14


@dataclass
class PositionParams:
    tvf_weights: Tuple[float, ...] = (0.25, 0.15, 0.15, 0.20, 0.15, 0.10)
    kelly_window: int = 60
    kelly_max: float = 0.25


@dataclass
class PullbackParams:
    ma_period: int = 20
    atr_period: int = 14


@dataclass
class DailyPivotParams:
    multiplier: float = 1.5
    min_bars: int = 20
    min_reversal_floor: float = 0.001
    compute_version: str = "adaptive_zigzag_v1.0"


@dataclass
class CycleResonanceParams:
    chaos_entropy_threshold: float = 0.7
    phase_transition_threshold: float = 0.3
    release_strength_threshold: float = 0.5
    release_bias_threshold: float = 0.3
    exhaust_strength_threshold: float = 0.2
    exhaust_highvol_threshold: float = 0.4
    secondary_chaos_entropy: float = 0.4
    strength_trend_release_threshold: float = 0.05


@dataclass
class PrecomputeParams:
    kl_rpd: KL_RPD_Params = field(default_factory=KL_RPD_Params)
    signal: SignalParams = field(default_factory=SignalParams)
    decay: DecayParams = field(default_factory=DecayParams)
    l0_state: L0StateParams = field(default_factory=L0StateParams)
    hmm: HMMParams = field(default_factory=HMMParams)
    trend: TrendParams = field(default_factory=TrendParams)
    obos: OBOSParams = field(default_factory=OBOSParams)
    position: PositionParams = field(default_factory=PositionParams)
    pullback: PullbackParams = field(default_factory=PullbackParams)
    daily_pivot: DailyPivotParams = field(default_factory=DailyPivotParams)
    cycle_resonance: CycleResonanceParams = field(default_factory=CycleResonanceParams)
    max_workers: int = 4
    batch_size: int = 50000
    db_path: str = "preprocessed.duckdb"
    symbols: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> PrecomputeParams:
        params = cls()
        if "kl_rpd" in d:
            params.kl_rpd = KL_RPD_Params(**d["kl_rpd"])
        if "signal" in d:
            params.signal = SignalParams(**d["signal"])
        if "decay" in d:
            params.decay = DecayParams(**d["decay"])
        if "l0_state" in d:
            params.l0_state = L0StateParams(**d["l0_state"])
        if "hmm" in d:
            params.hmm = HMMParams(**d["hmm"])
        if "trend" in d:
            params.trend = TrendParams(**d["trend"])
        if "obos" in d:
            params.obos = OBOSParams(**d["obos"])
        if "position" in d:
            params.position = PositionParams(**d["position"])
        if "pullback" in d:
            params.pullback = PullbackParams(**d["pullback"])
        if "daily_pivot" in d:
            params.daily_pivot = DailyPivotParams(**d["daily_pivot"])
        if "cycle_resonance" in d:
            params.cycle_resonance = CycleResonanceParams(**d["cycle_resonance"])
        for k in ["max_workers", "batch_size", "db_path", "symbols"]:
            if k in d:
                setattr(params, k, d[k])
        return params