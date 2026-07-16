# [M3-03] 盈利性评判服务
# MODULE_ID: M3-617
"""Profitability Judgment Service - extracted from strategy_judgment_engine.py"""
from __future__ import annotations

import json, logging, math, os
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from .judgment_types import (
    _JudgmentDimension, _safe_float, _safe_clip_score,
    CapitalScale, SCORING_COEFFICIENTS, CAPITAL_SCALE_CONFIGS,
)
from infra._helpers import get_logger  # R9-5

logger = get_logger(__name__)  # R9-5


class ProfitabilityJudger:
    """Profitability judgment service."""

    def __init__(
        self,
        scoring_coefficients: Optional[Dict[str, Any]] = None,
        capital_scale: Optional[CapitalScale] = None,
        shadow_metrics: Optional[Dict[str, Any]] = None,
    ):
        self.SCORING_COEFFICIENTS = scoring_coefficients or SCORING_COEFFICIENTS
        self._capital_scale = capital_scale
        self._shadow_metrics: Dict[str, Any] = shadow_metrics or {}

    def diminishing_return_score(value: float, breakpoints: Optional[List[Tuple[float, float]]] = None) -> float:
        if breakpoints is None:
            breakpoints = [(1.0, 0.50), (2.0, 0.80), (3.0, 1.00)]
        if value <= 0:
            return 0.0
        if value >= breakpoints[-1][0]:
            return breakpoints[-1][1]
        for i in range(len(breakpoints) - 1):
            x0, y0 = breakpoints[i]
            x1, y1 = breakpoints[i + 1]
            if x0 <= value < x1:
                t = (value - x0) / (x1 - x0)
                return y0 + t * (y1 - y0)
        return 0.0

    def judge_profitability(self, metrics: Optional[Dict[str, float]], threshold: float, weight: float) -> _JudgmentDimension:
        threshold = threshold
        is_blocker = False
        if metrics is None:
            return _JudgmentDimension(
                name="盈利能力", score=0.0, weight=weight,
                passed=False, threshold=threshold, is_blocker=is_blocker,
                detail="未提供盈利指标，无法评判",
            )

        if self._capital_scale is not None:
            return self.judge_profitability_by_scale(metrics, threshold, is_blocker, weight)

        sharpe = metrics.get("sharpe", 0.0)
        calmar = metrics.get("calmar", 0.0)
        win_rate = metrics.get("win_rate", 0.0)
        pl_ratio = metrics.get("profit_loss_ratio", 0.0)

        sharpe_score = self.diminishing_return_score(sharpe)
        calmar_score = self.diminishing_return_score(calmar)
        win_score = min(1.0, max(0.0, win_rate / self.SCORING_COEFFICIENTS["win_rate_full_score_at"]))
        pl_score = self.diminishing_return_score(pl_ratio, [(1.0, 0.50), (2.0, 0.80), (3.0, 1.00)])

        score = self.SCORING_COEFFICIENTS["profit_sharpe_w"] * sharpe_score + self.SCORING_COEFFICIENTS["profit_calmar_w"] * calmar_score + self.SCORING_COEFFICIENTS["profit_win_w"] * win_score + self.SCORING_COEFFICIENTS["profit_pl_w"] * pl_score

        # P2-R9-01修复: 消费。shadow_metrics的alpha_decay_rate作为评判维度输入
        _alpha_decay_adj = 0.0
        try:
            if self._shadow_metrics:
                _all_decay_rates = []
                for _group_key, _records in self._shadow_metrics.items():
                    for _rec in _records:
                        _adr = _rec.get('alpha_decay_rate', None)
                        if _adr is not None:
                            _all_decay_rates.append(float(_adr))
                if _all_decay_rates:
                    _avg_decay = sum(_all_decay_rates) / len(_all_decay_rates)
                    _alpha_decay_adj = max(0.0, min(0.15, _avg_decay * 0.1))
                    score = score * (1.0 - _alpha_decay_adj)
        except (ValueError, KeyError, TypeError, RuntimeError) as _e:
            logging.debug("[R3-L2] alpha衰减调整跳过: %s", _e)

        passed = score >= threshold
        detail = (f"Sharpe={sharpe:.2f}(score={sharpe_score:.2f}), "
                  f"Calmar={calmar:.2f}(score={calmar_score:.2f}), "
                  f"WinRate={win_rate:.2f}, P/L={pl_ratio:.2f}")
        if _alpha_decay_adj > 0:
            detail += f", AlphaDecayAdj=-{_alpha_decay_adj:.3f}"
        return _JudgmentDimension(
            name="盈利能力", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )

    def load_shadow_jsonl(self) -> None:
        """读取影子策略JSONL日志文件，供评判引擎消费"""
        import json as _json
        import glob as _glob
        try:
            _log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs', 'shadow')
            _files = _glob.glob(os.path.join(_log_dir, 'shadow_*.jsonl'))
            for _f in _files:
                try:
                    with open(_f, 'r', encoding='utf-8') as _fh:
                        for _line in _fh:
                            _line = _line.strip()
                            if not _line:
                                continue
                            _rec = _json.loads(_line)
                            _key = f"{_rec.get('shadow_type', '')}_{_rec.get('strategy_group', '')}"
                            self._shadow_metrics.setdefault(_key, []).append(_rec)
                except (IOError, ValueError, KeyError) as _e:
                    logging.debug("[P2-R9-01] 读取影子JSONL失败: %s, %s", _f, _e)
            if self._shadow_metrics:
                logging.info("[P2-R9-01] 读取到%d个影子策略组数据", len(self._shadow_metrics))
        except (IOError, ValueError, KeyError, AttributeError) as e:
            logging.debug("[P2-R9-01] 影子JSONL加载失败: %s", e)

    def resolve_scale_config(self, capital_scale: CapitalScale) -> Dict[str, Any]:
        try:
            from governance.mode_engine import ModeEngine, CAPITAL_MODE_CONFIGS
            _scale_map = {
                CapitalScale.SMALL: 'small',
                CapitalScale.MEDIUM: 'medium',
                CapitalScale.LARGE: 'large',
            }
            scale_str = _scale_map.get(capital_scale, 'medium')
            mc = CAPITAL_MODE_CONFIGS.get(scale_str)
            if mc is not None:
                return {
                    "profitability_mode": mc.profitability_mode,
                    "profitability_weights": {k: v for k, v in mc.profitability_weights},
                    "win_loss_ratio_full_score_at": mc.win_loss_ratio_full_score_at,
                    "profit_factor_full_score_at": mc.profit_factor_full_score_at,
                    "recovery_efficiency_full_score_at": mc.recovery_efficiency_full_score_at,
                    "sharpe_full_score_at": mc.sharpe_full_score_at,
                    "calmar_full_score_at": mc.calmar_full_score_at,
                    "max_consecutive_losses_full": mc.max_consecutive_losses_full,
                    "max_consecutive_losses_zero": mc.max_consecutive_losses_zero,
                    "drawdown_recovery_max_hours": mc.drawdown_recovery_max_hours,
                    "extreme_max_recovery_hours": mc.extreme_max_recovery_hours,
                    "overall_pass_threshold": mc.overall_pass_threshold,
                    "overall_conditional_threshold": mc.overall_conditional_threshold,
                }
        except (ImportError, ValueError, KeyError, AttributeError) as _resolve_err:
            # R15-P1-DEAD-04修复: 静默吞没异常改为日志记录
            logging.info("[StrategyJudgmentEngine] _resolve_scale_config fallback: %s", _resolve_err)  # R26-P2-DF-05修复: debug→info
            pass

    def judge_profitability_by_scale(
        self, metrics: Dict[str, float], threshold: float, is_blocker: bool,
        weight: float = 0.09,
    ) -> _JudgmentDimension:
        scale_cfg = self.resolve_scale_config(self._capital_scale)
        pw = scale_cfg["profitability_weights"]
        mode = scale_cfg["profitability_mode"]

        win_loss_ratio = _safe_float(metrics.get("win_loss_ratio", 0.0))
        profit_factor = _safe_float(metrics.get("profit_factor", 0.0))
        recovery_efficiency = _safe_float(metrics.get("recovery_efficiency", 0.0))
        max_consecutive_losses = int(_safe_float(metrics.get("max_consecutive_losses", 999), default=999))
        sharpe = max(-10.0, min(10.0, _safe_float(metrics.get("sharpe", 0.0))))
        calmar = max(-10.0, min(10.0, _safe_float(metrics.get("calmar", 0.0))))

        wl_full = scale_cfg.get("win_loss_ratio_full_score_at", 2.0)
        pf_full = scale_cfg.get("profit_factor_full_score_at", 1.5)
        re_full = scale_cfg.get("recovery_efficiency_full_score_at", 2.0)
        cl_full = scale_cfg.get("max_consecutive_losses_full", 3)
        cl_zero = scale_cfg.get("max_consecutive_losses_zero", 10)

        wl_score = min(1.0, max(0.0, win_loss_ratio / wl_full)) if wl_full > 0 else 0.0
        pf_score = min(1.0, max(0.0, profit_factor / pf_full)) if pf_full > 0 else 0.0
        re_score = min(1.0, max(0.0, recovery_efficiency / re_full)) if re_full > 0 else 0.0
        cl_score = max(0.0, 1.0 - max(0, max_consecutive_losses - cl_full) / max(1, cl_zero - cl_full))

        score_parts = {}
        score_parts["win_loss_ratio"] = wl_score
        score_parts["profit_factor"] = pf_score
        score_parts["recovery_efficiency"] = re_score
        score_parts["consecutive_loss_tolerance"] = cl_score

        if mode in ("balanced", "sharpe_dominant"):
            sh_full = scale_cfg.get("sharpe_full_score_at", 2.0)
            ca_full = scale_cfg.get("calmar_full_score_at", 2.0)
            sharpe_score = self.diminishing_return_score(sharpe, [(1.0, 0.50), (sh_full, 1.00)])
            calmar_score = self.diminishing_return_score(calmar, [(1.0, 0.50), (ca_full, 1.00)])
            score_parts["sharpe"] = sharpe_score
            score_parts["calmar"] = calmar_score

        score = sum(pw.get(k, 0.0) * v for k, v in score_parts.items())
        score = float(np.clip(score, 0.0, 1.0))
        passed = score >= threshold

        detail_parts = [f"模式={mode}"]
        if "sharpe" in score_parts:
            detail_parts.append(f"Sharpe={sharpe:.2f}(s={score_parts['sharpe']:.2f})")
        if "calmar" in score_parts:
            detail_parts.append(f"Calmar={calmar:.2f}(s={score_parts['calmar']:.2f})")
        detail_parts.append(f"盈亏比={win_loss_ratio:.2f}(s={wl_score:.2f})")
        detail_parts.append(f"盈利因子={profit_factor:.2f}(s={pf_score:.2f})")
        detail_parts.append(f"恢复效率={recovery_efficiency:.2f}(s={re_score:.2f})")
        detail_parts.append(f"连亏={max_consecutive_losses}(s={cl_score:.2f})")
        detail = ", ".join(detail_parts)

        return _JudgmentDimension(
            name="盈利能力", score=score, weight=weight,
            passed=passed, threshold=threshold, is_blocker=is_blocker, detail=detail,
        )
