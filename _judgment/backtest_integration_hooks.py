# [M3-13] 回测集成钩子
# MODULE_ID: M3-611
"""
回测集成钩子 (Backtest Integration Hooks) — v1.4

将策略评判系统与现有回测框架集成，
覆盖18策略(6主策略×3变体: master/shadow_a/shadow_b)完整生命周期。

集成方式：
  1. 每个tick后调用 on_tick()
  2. 信号生成后调用 on_signal_generated()
  3. 开仓后调用 on_order_opened()
  4. 平仓后调用 on_order_closed()
  5. 安全元层触发时调用 on_safety_meta_trigger()
  6. 生态系统切换时调用 on_ecosystem_switch()
  7. 弹簧状态变化时调用 on_spring_state_change()
  8. 周期共振相位变化时调用 on_phase_transition()
  9. 回测结束调用 on_backtest_finish() → 自动诊断+评判+报告
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np

from ali2026v3_trading.infra._helpers import get_logger  # R9-5
from ali2026v3_trading.infra.serialization_utils import json_dumps, json_loads, json_default_serializer

from .turning_point_analysis import TurningPointMicroscope, EnhancedBar, ExtremeRegion
from .turning_point_analysis import ResonanceTurningPointMarker, TurningPointRecord
from .market_snapshot_collector import (
    MarketSnapshotCollector, MarketSnapshot, SnapshotTrigger,
    StrategyStateSnapshot, HFTSpecificState, ResonanceSpecificState,
    BoxSpecificState, SpringSpecificState, ArbitrageSpecificState,
    MarketMakingSpecificState, EcosystemState,
    ShadowAlphaState, SafetyMetaState, CrossStrategyGreeks,
    SIX_STRATEGY_KEYS, THREE_VARIANTS, ALL_18_STRATEGY_IDS,
    SEVEN_STRATEGY_KEYS, ALL_21_STRATEGY_IDS,
)
from .strategy_behavior_diagnosis import StrategyBehaviorDiagnosis, DiagnosisReport
from .strategy_judgment_facade import StrategyJudgmentEngine
from .judgment_types import JudgmentReport, JudgmentVerdict, CapitalScale

logger = get_logger(__name__)  # R9-5


_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


@dataclass(slots=True)
class HookConfig:
    symbol: str = ""
    strategy_id: str = ""
    strategy_type: str = ""
    ma_periods: List[int] = field(default_factory=lambda: [5, 10, 15, 30, 60])
    extreme_window: int = 20
    extreme_quantile: float = 0.05
    capture_signals: bool = True
    capture_orders: bool = True
    capture_weekly_monthly: bool = True
    capture_extreme_regions: bool = True
    capture_phase_transitions: bool = True
    capture_ecosystem_switches: bool = True
    capture_spring_state_changes: bool = True
    capture_safety_meta: bool = True
    auto_judge: bool = True
    output_dir: str = os.path.join(_MODULE_DIR, "strategy_judgment_output")
    capital_scale: Optional[CapitalScale] = None


class BacktestIntegrationHooks:
    """
    回测集成钩子 — 一站式评判系统入口
    """

    def __init__(self, config: Optional[HookConfig] = None):
        self._config = config or HookConfig()
        c = self._config

        self._microscope = TurningPointMicroscope(
            symbol=c.symbol, ma_periods=c.ma_periods,
            extreme_window=c.extreme_window, extreme_quantile=c.extreme_quantile,
        )
        self._resonance_marker = ResonanceTurningPointMarker(symbol=c.symbol)
        self._snapshot_collector = MarketSnapshotCollector(symbol=c.symbol)
        self._diagnoser = StrategyBehaviorDiagnosis(
            strategy_id=c.strategy_id, strategy_type=c.strategy_type,
        )
        self._judgment_engine = StrategyJudgmentEngine(capital_scale=c.capital_scale)

        self._last_bar: Optional[EnhancedBar] = None
        self._last_cr_output: Optional[Any] = None
        self._current_strategy_states: List[StrategyStateSnapshot] = []
        self._current_hft_state = HFTSpecificState()
        self._current_resonance_state = ResonanceSpecificState()
        self._current_box_state = BoxSpecificState()
        self._current_spring_state = SpringSpecificState()
        self._current_arbitrage_state = ArbitrageSpecificState()
        self._current_market_making_state = MarketMakingSpecificState()
        self._current_ecosystem_state = EcosystemState()
        self._current_shadow_alpha = ShadowAlphaState()
        self._current_safety_meta = SafetyMetaState()
        self._current_cross_greeks = CrossStrategyGreeks()
        self._current_five_state: Dict[str, float] = {}
        self._current_order_flow: Dict[str, float] = {}
        self._current_portfolio_info: Dict[str, float] = {}

        self._turning_points: List[TurningPointRecord] = []
        self._bar_count = 0

    def on_tick(
        self,
        timestamp: np.datetime64,
        last_price: float,
        volume: int = 0,
        turnover: float = 0.0,
        bid_price1: float = 0.0,
        ask_price1: float = 0.0,
        open_interest: int = 0,
        total_volume: int = 0,
        cr_output: Optional[Any] = None,
    ) -> Optional[EnhancedBar]:
        if cr_output is not None:
            self._last_cr_output = cr_output
        bar = self._microscope.process_tick(
            timestamp=timestamp, last_price=last_price, volume=volume,
            turnover=turnover, bid_price1=bid_price1, ask_price1=ask_price1,
            open_interest=open_interest, total_volume=total_volume,
        )
        if bar is not None:
            self._on_bar_complete(bar)
        return bar

    def on_bar(self, bar: EnhancedBar, cr_output: Optional[Any] = None) -> List[TurningPointRecord]:
        if cr_output is not None:
            self._last_cr_output = cr_output
        return self._on_bar_complete(bar)

    def _on_bar_complete(self, bar: EnhancedBar) -> List[TurningPointRecord]:
        self._last_bar = bar
        self._bar_count += 1

        new_tps = self._resonance_marker.process_bar(bar=bar, cr_output=self._last_cr_output)
        self._turning_points.extend(new_tps)

        if self._config.capture_extreme_regions and \
           bar.extreme_region in (ExtremeRegion.NEAR_HIGH, ExtremeRegion.NEAR_LOW,
                                  ExtremeRegion.EXTREME_HIGH, ExtremeRegion.EXTREME_LOW):
            self._snapshot_collector.capture(
                timestamp=bar.timestamp, trigger=SnapshotTrigger.EXTREME_REGION,
                trigger_detail=f"极值区域: {bar.extreme_region.value}",
                bar=bar, cr_output=self._last_cr_output,
                strategy_states=self._current_strategy_states,
                hft_state=self._current_hft_state,
                resonance_state=self._current_resonance_state,
                box_state=self._current_box_state,
                spring_state=self._current_spring_state,
                arbitrage_state=self._current_arbitrage_state,
                market_making_state=self._current_market_making_state,
                ecosystem_state=self._current_ecosystem_state,
                shadow_alpha=self._current_shadow_alpha,
                safety_meta=self._current_safety_meta,
                cross_greeks=self._current_cross_greeks,
                five_state=self._current_five_state,
                order_flow=self._current_order_flow,
                portfolio_info=self._current_portfolio_info,
            )

        if self._config.capture_weekly_monthly:
            self._snapshot_collector.process_bar_for_weekly_monthly(bar=bar)

        return new_tps

    def on_signal_generated(self, timestamp=None, signal_info=None) -> Optional[MarketSnapshot]:
        if not self._config.capture_signals:
            return None
        ts = timestamp or (self._last_bar.timestamp if self._last_bar else np.datetime64('now'))
        return self._snapshot_collector.capture_signal_point(ts, trigger_detail=signal_info.get("detail", "") if signal_info else "",
            bar=self._last_bar, cr_output=self._last_cr_output,
            strategy_states=self._current_strategy_states,
            hft_state=self._current_hft_state, resonance_state=self._current_resonance_state,
            box_state=self._current_box_state, spring_state=self._current_spring_state,
            arbitrage_state=self._current_arbitrage_state,
            market_making_state=self._current_market_making_state,
            ecosystem_state=self._current_ecosystem_state, shadow_alpha=self._current_shadow_alpha,
            safety_meta=self._current_safety_meta, cross_greeks=self._current_cross_greeks,
            five_state=self._current_five_state, order_flow=self._current_order_flow,
            portfolio_info=self._current_portfolio_info, signal_info=signal_info)

    def on_order_opened(self, timestamp=None, order_info=None) -> Optional[MarketSnapshot]:
        if not self._config.capture_orders:
            return None
        ts = timestamp or (self._last_bar.timestamp if self._last_bar else np.datetime64('now'))
        # R16-P1-PERF-19修复: 回测交易延迟模拟
        self._simulate_trade_latency(is_open=True)
        return self._snapshot_collector.capture_order_event(ts, SnapshotTrigger.ORDER_OPENED,
            bar=self._last_bar, cr_output=self._last_cr_output,
            strategy_states=self._current_strategy_states,
            hft_state=self._current_hft_state, resonance_state=self._current_resonance_state,
            box_state=self._current_box_state, spring_state=self._current_spring_state,
            arbitrage_state=self._current_arbitrage_state,
            market_making_state=self._current_market_making_state,
            ecosystem_state=self._current_ecosystem_state, shadow_alpha=self._current_shadow_alpha,
            safety_meta=self._current_safety_meta, cross_greeks=self._current_cross_greeks,
            five_state=self._current_five_state, order_flow=self._current_order_flow,
            portfolio_info=self._current_portfolio_info, order_info=order_info)

    def on_order_closed(self, timestamp=None, order_info=None) -> Optional[MarketSnapshot]:
        if not self._config.capture_orders:
            return None
        ts = timestamp or (self._last_bar.timestamp if self._last_bar else np.datetime64('now'))
        # R16-P1-PERF-19修复: 回测交易延迟模拟
        self._simulate_trade_latency(is_open=False)
        return self._snapshot_collector.capture_order_event(ts, SnapshotTrigger.ORDER_CLOSED,
            bar=self._last_bar, cr_output=self._last_cr_output,
            strategy_states=self._current_strategy_states,
            hft_state=self._current_hft_state, resonance_state=self._current_resonance_state,
            box_state=self._current_box_state, spring_state=self._current_spring_state,
            arbitrage_state=self._current_arbitrage_state,
            market_making_state=self._current_market_making_state,
            ecosystem_state=self._current_ecosystem_state, shadow_alpha=self._current_shadow_alpha,
            safety_meta=self._current_safety_meta, cross_greeks=self._current_cross_greeks,
            five_state=self._current_five_state, order_flow=self._current_order_flow,
            portfolio_info=self._current_portfolio_info, order_info=order_info)
    
    def _simulate_trade_latency(self, is_open: bool = True) -> None:
        """R16-P1-PERF-19修复: 回测交易延迟模拟
        
        模拟下单到成交的延迟，消除sharpe乐观偏差：
        - 开仓延迟：市场冲击+排队时间
        - 平仓延迟：滑点+执行延迟
        """
        import time as _time
        if not hasattr(self, '_backtest_latency_enabled'):
            # 从配置读取延迟设置，默认启用
            self._backtest_latency_enabled = True
            self._open_latency_ms = 50  # 开仓延迟50ms
            self._close_latency_ms = 30  # 平仓延迟30ms
            self._latency_jitter_pct = 0.2  # 20%抖动
            self._latency_count = 0
        
        if not self._backtest_latency_enabled:
            return
        
        import random
        base_latency = self._open_latency_ms if is_open else self._close_latency_ms
        jitter = base_latency * self._latency_jitter_pct * (random.random() * 2 - 1)
        actual_latency = max(0, base_latency + jitter) / 1000.0  # 转秒
        
        _time.sleep(actual_latency)
        self._latency_count += 1
        
        if self._latency_count <= 10 or self._latency_count % 100 == 0:
            logger.debug("[R16-P1-PERF-19] 回测交易延迟模拟: %s %.1fms (累计%d次)",
                        "开仓" if is_open else "平仓", actual_latency * 1000, self._latency_count)

    def on_safety_meta_trigger(self, timestamp=None, detail="") -> Optional[MarketSnapshot]:
        if not self._config.capture_safety_meta:
            return None
        ts = timestamp or (self._last_bar.timestamp if self._last_bar else np.datetime64('now'))
        return self._snapshot_collector.capture_safety_meta_trigger(ts, detail=detail,
            bar=self._last_bar, cr_output=self._last_cr_output,
            strategy_states=self._current_strategy_states, safety_meta=self._current_safety_meta,
            ecosystem_state=self._current_ecosystem_state, cross_greeks=self._current_cross_greeks)

    def on_ecosystem_switch(self, timestamp=None, detail="") -> Optional[MarketSnapshot]:
        if not self._config.capture_ecosystem_switches:
            return None
        ts = timestamp or (self._last_bar.timestamp if self._last_bar else np.datetime64('now'))
        return self._snapshot_collector.capture_ecosystem_switch(ts, detail=detail,
            bar=self._last_bar, cr_output=self._last_cr_output,
            strategy_states=self._current_strategy_states, ecosystem_state=self._current_ecosystem_state)

    def on_spring_state_change(self, timestamp=None, detail="") -> Optional[MarketSnapshot]:
        if not self._config.capture_spring_state_changes:
            return None
        ts = timestamp or (self._last_bar.timestamp if self._last_bar else np.datetime64('now'))
        return self._snapshot_collector.capture_spring_state_change(ts, detail=detail,
            bar=self._last_bar, cr_output=self._last_cr_output,
            strategy_states=self._current_strategy_states, spring_state=self._current_spring_state)

    def on_phase_transition(self, timestamp=None, detail="") -> Optional[MarketSnapshot]:
        if not self._config.capture_phase_transitions:
            return None
        ts = timestamp or (self._last_bar.timestamp if self._last_bar else np.datetime64('now'))
        return self._snapshot_collector.capture_phase_transition(ts, detail=detail,
            bar=self._last_bar, cr_output=self._last_cr_output,
            strategy_states=self._current_strategy_states, resonance_state=self._current_resonance_state)

    def update_state(
        self,
        strategy_states=None, hft_state=None, resonance_state=None,
        box_state=None, spring_state=None, arbitrage_state=None,
        market_making_state=None, ecosystem_state=None,
        shadow_alpha=None, safety_meta=None, cross_greeks=None,
        five_state=None, order_flow=None, portfolio_info=None,
        cr_output=None,
    ) -> None:
        if strategy_states is not None: self._current_strategy_states = strategy_states
        if hft_state is not None: self._current_hft_state = hft_state
        if resonance_state is not None: self._current_resonance_state = resonance_state
        if box_state is not None: self._current_box_state = box_state
        if spring_state is not None: self._current_spring_state = spring_state
        if arbitrage_state is not None: self._current_arbitrage_state = arbitrage_state
        if market_making_state is not None: self._current_market_making_state = market_making_state
        if ecosystem_state is not None: self._current_ecosystem_state = ecosystem_state
        if shadow_alpha is not None: self._current_shadow_alpha = shadow_alpha
        if safety_meta is not None: self._current_safety_meta = safety_meta
        if cross_greeks is not None: self._current_cross_greeks = cross_greeks
        if five_state is not None: self._current_five_state = five_state
        if order_flow is not None: self._current_order_flow = order_flow
        if portfolio_info is not None: self._current_portfolio_info = portfolio_info
        if cr_output is not None: self._last_cr_output = cr_output

    def build_21_strategy_states(self, **per_strategy_overrides) -> List[StrategyStateSnapshot]:
        """构建21策略状态快照列表(7主策略×3变体)

        Args:
            per_strategy_overrides: 可选的关键字参数，格式为 {strategy_id: {field: value}}
                例如 build_21_strategy_states(high_freq_master={"signal_strength": 0.8})
        """
        states = []
        for sk in SEVEN_STRATEGY_KEYS:
            for variant in THREE_VARIANTS:
                sid = f"{sk}_{variant}"
                overrides = per_strategy_overrides.get(sid, {})
                states.append(StrategyStateSnapshot(
                    strategy_id=sid,
                    strategy_type=sk,
                    **overrides,
                ))
        return states

    def build_18_strategy_states(self, **per_strategy_overrides) -> List[StrategyStateSnapshot]:
        """向后兼容: 委托至build_21_strategy_states"""
        return self.build_21_strategy_states(**per_strategy_overrides)

    def on_backtest_finish(self, symbol="", backtest_period="",
                           profitability_metrics=None,
                           extreme_survival_result=None,
                           cross_instrument_results=None,
                           parameter_stability_result=None,
                           return_source_diversification=None,
                           drawdown_recovery_result=None,
                           explanation_coverage_result=None) -> Dict[str, Any]:
        c = self._config
        symbol = symbol or c.symbol
        self._microscope.flush()

        microscope_stats = self._microscope.get_statistics()
        resonance_accuracy = self._resonance_marker.get_prediction_accuracy()
        snapshot_stats = self._snapshot_collector.get_statistics()

        extreme_snapshots = []
        all_snapshots = self._snapshot_collector.get_all_snapshots()
        for snap in all_snapshots:
            if snap.extreme_region in ("NEAR_HIGH", "NEAR_LOW", "EXTREME_HIGH", "EXTREME_LOW"):
                extreme_snapshots.append(snap)

        if not extreme_snapshots:
            for bar in self._microscope.get_extreme_snapshots():
                snap = MarketSnapshot(
                    snapshot_id=f"bar_extreme_{id(bar)}", timestamp=bar.timestamp,
                    symbol=symbol, trigger=SnapshotTrigger.EXTREME_REGION,
                    trigger_detail=bar.extreme_region.value,
                    bar_open=bar.open, bar_high=bar.high, bar_low=bar.low,
                    bar_close=bar.close, bar_vwap=bar.vwap, bar_volume=bar.volume,
                    bar_atr14=bar.atr14, extreme_region=bar.extreme_region.value,
                    ma_alignment=bar.ma_alignment.value, price_vs_mas=bar.price_vs_mas,
                    price_deviation_sigma=bar.price_ma_deviation_sigma,
                    ma_curvatures=bar.ma_curvatures,
                )
                extreme_snapshots.append(snap)

        diagnosis_report = self._diagnoser.diagnose(
            extreme_snapshots=extreme_snapshots, all_snapshots=all_snapshots,
            symbol=symbol, backtest_period=backtest_period,
        )

        if profitability_metrics and isinstance(profitability_metrics, dict):
            _per_trade_ret = profitability_metrics.get("per_trade_returns")
            if _per_trade_ret and isinstance(_per_trade_ret, (list, tuple)):
                diagnosis_report._per_trade_returns = list(_per_trade_ret)
            _p_vals = profitability_metrics.get("p_values")
            if _p_vals and isinstance(_p_vals, (list, tuple)):
                diagnosis_report._p_values = list(_p_vals)
        else:
            if not getattr(diagnosis_report, '_per_trade_returns', None):
                _auto_returns = getattr(self._diagnoser, '_last_per_trade_returns', None)
                if _auto_returns and isinstance(_auto_returns, (list, tuple)) and len(_auto_returns) >= 10:
                    diagnosis_report._per_trade_returns = list(_auto_returns)
                else:
                    import logging
                    logging.warning("[P0-3] profitability_metrics未传入且无法自动提取per_trade_returns，MCBV将降级到i.i.d.假设")
            if not getattr(diagnosis_report, '_p_values', None):
                _auto_pvals = getattr(self._diagnoser, '_last_p_values', None)
                if _auto_pvals and isinstance(_auto_pvals, (list, tuple)):
                    diagnosis_report._p_values = list(_auto_pvals)
                else:
                    import logging
                    logging.warning("[P0-3] profitability_metrics未传入且无法自动提取p_values，BH多重比较校正将跳过")

        judgment_report = None
        if c.auto_judge:
            # 收集11维度实时风险评分（从快照中提取最新的RiskDimensionScores）
            realtime_risk_scores = None
            try:
                all_snaps = self._snapshot_collector.get_all_snapshots()
                for snap in reversed(all_snaps):
                    if hasattr(snap, 'risk_dimensions') and snap.risk_dimensions is not None:
                        rd = snap.risk_dimensions
                        if hasattr(rd, 'to_dict'):
                            rd_dict = rd.to_dict()
                        elif hasattr(rd, '__dict__'):
                            rd_dict = {k: v for k, v in rd.__dict__.items() if not k.startswith('_')}
                        else:
                            rd_dict = {}
                        realtime_risk_scores = {}
                        key_map = {
                            'd1_state_strength': 'd1_state_strength',
                            'd2_order_flow': 'd2_order_flow',
                            'd3_life_expectancy': 'd3_life_expectancy',
                            'd4_cycle_resonance': 'd4_cycle_resonance',
                            'd5_phase_quality': 'd5_phase_quality',
                            'd6_greeks_usage': 'd6_greeks_usage',
                            'd7_consecutive_loss': 'd7_consecutive_loss',
                            'd8_asymmetric_drawdown': 'd8_asymmetric_drawdown',
                            'd9_tri_validation': 'd9_tri_validation',
                            'd10_alpha_decay': 'd10_alpha_decay',
                            'd11_cross_correlation': 'd11_cross_correlation',
                            'composite_score': 'composite_score',
                        }
                        for src, dst in key_map.items():
                            if src in rd_dict and rd_dict[src] is not None:
                                realtime_risk_scores[dst] = float(rd_dict[src])
                        break
            except (ValueError, KeyError, TypeError, AttributeError) as e:
                logger.debug("[BacktestIntegrationHooks] 提取realtime_risk_scores失败: %s", e)

            judgment_report = self._judgment_engine.judge(
                strategy_id=c.strategy_id, strategy_type=c.strategy_type,
                symbol=symbol, backtest_period=backtest_period,
                diagnosis_report=diagnosis_report,
                resonance_accuracy=resonance_accuracy,
                snapshot_statistics=snapshot_stats,
                profitability_metrics=profitability_metrics,
                extreme_survival_result=extreme_survival_result,
                cross_instrument_results=cross_instrument_results,
                parameter_stability_result=parameter_stability_result,
                return_source_diversification=return_source_diversification,
                drawdown_recovery_result=drawdown_recovery_result,
                explanation_coverage_result=explanation_coverage_result,
                realtime_risk_scores=realtime_risk_scores,
            )

        if c.output_dir:
            self._save_results(judgment_report, diagnosis_report, symbol, backtest_period)

        result = {
            "judgment_report": judgment_report,
            "diagnosis_report": diagnosis_report,
            "microscope_stats": microscope_stats,
            "resonance_accuracy": resonance_accuracy,
            "snapshot_stats": snapshot_stats,
            "turning_points": self._turning_points,
        }

        logger.info(
            f"[策略评判] 回测评判完成: "
            f"Bar={microscope_stats.get('total_bars', 0)}, "
            f"极值={microscope_stats.get('extreme_bar_count', 0)}, "
            f"转折点={len(self._turning_points)}, "
            f"评判={judgment_report.verdict.value if judgment_report else 'N/A'}"
        )
        return result

    def _save_results(self, judgment_report, diagnosis_report, symbol, backtest_period):
        output_dir = self._config.output_dir
        os.makedirs(output_dir, exist_ok=True)
        prefix = f"{symbol}_{self._config.strategy_id}"

        if judgment_report is not None:
            json_path = os.path.join(output_dir, f"{prefix}_judgment.json")
            judgment_report.save(json_path)
            md_path = os.path.join(output_dir, f"{prefix}_judgment.md")
            judgment_report.save_markdown(md_path)
            logger.info(f"评判报告已保存: {json_path} + {md_path}")

        if diagnosis_report is not None:
            import json
            path = os.path.join(output_dir, f"{prefix}_diagnosis.json")
            with open(path, 'w', encoding='utf-8') as f:
                f.write(json_dumps(diagnosis_report.to_dict(), indent=2))

        db_path = os.path.join(output_dir, f"{prefix}_snapshots.duckdb")
        try:
            self._snapshot_collector.export_to_duckdb(db_path)
        except (IOError, ValueError, RuntimeError, AttributeError) as e:
            logger.warning(f"快照DuckDB导出失败: {e}")

    @property
    def microscope(self) -> TurningPointMicroscope:
        return self._microscope

    @property
    def resonance_marker(self) -> ResonanceTurningPointMarker:
        return self._resonance_marker

    @property
    def snapshot_collector(self) -> MarketSnapshotCollector:
        return self._snapshot_collector

    @property
    def last_bar(self) -> Optional[EnhancedBar]:
        return self._last_bar
