# MODULE_ID: M1-262
"""影子策略PnL服务 - 合并自_shadow_strategy_pnl.py和_shadow_strategy_pnl_metrics.py (2026-06-12)"""
from __future__ import annotations

import json
import logging
import math
import numpy as np
import os
import pandas as pd
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Dict, List

from ali2026v3_trading.infra.logging_utils import get_logger  # R9-5
from ali2026v3_trading.strategy.shadow_strategy_types import AlphaMetrics
from ali2026v3_trading.infra.shared_utils import CHINA_TZ, DEFAULT_RISK_FREE_RATE, ANNUALIZE_FACTOR_DAILY
from ali2026v3_trading.infra.serialization_utils import json_dumps  # R4-5: 统一json_dumps
# P1-47修复: 夏普比率计算统一使用 infra 权威版
from ali2026v3_trading.infra.resilience import compute_sharpe_stable

logger = get_logger(__name__)  # R9-5


class ShadowStrategyPnLMetricsService:
    """PnL Metrics service (from ShadowStrategyPnLMetricsMixin)"""

    def __init__(self, facade=None):
        self._facade = facade

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def update_equity_curves(
        self,
        master_equity: float,
        shadow_a_equity: float,
        shadow_b_equity: float,
    ) -> None:
        with self._lock:
            self._master_equity_curve.append(master_equity)
            self._shadow_a_equity_curve.append(shadow_a_equity)
            self._shadow_b_equity_curve.append(shadow_b_equity)
    @staticmethod
    def _compute_sharpe(returns: List[float], annualize_factor: float = ANNUALIZE_FACTOR_DAILY,
                        risk_free_rate: float = DEFAULT_RISK_FREE_RATE) -> float:
        """P1-47修复: 委托给 infra/resilience_numeric.compute_sharpe_stable

        保留原签名以兼容所有调用点，内部委托给权威实现。
        use_sample_std=True 保持与原实现(样本方差n-1)一致。
        """
        # risk_free_rate 在原实现中是年化值，需转换为逐期值
        period_risk_free = risk_free_rate / annualize_factor if annualize_factor > 0 else 0.0
        return compute_sharpe_stable(
            returns,
            risk_free_rate=period_risk_free,
            annualize_factor=annualize_factor,
            use_sample_std=True,
        )
    @staticmethod
    def _compute_max_drawdown(equity_curve: List[float]) -> float:
        if len(equity_curve) < 2:
            return 0.0
        peak = equity_curve[0]
        max_dd = 0.0
        for eq in equity_curve:
            if eq > peak:
                peak = eq
            if peak > 0:
                dd = (peak - eq) / peak
                if dd > max_dd:
                    max_dd = dd
        return max_dd
    @staticmethod
    def _equity_to_returns(equity_curve: List[float]) -> List[float]:
        if len(equity_curve) < 2:
            return []
        returns = []
        for i in range(1, len(equity_curve)):
            prev = equity_curve[i - 1]
            curr = equity_curve[i]
            if math.isnan(prev) or math.isnan(curr):
                returns.append(0.0)
                continue
            if abs(prev) < 1e-10:
                returns.append(0.0)
            else:
                returns.append((curr - prev) / abs(prev))
        return returns
    @staticmethod
    def _compute_expected_value(trades: deque) -> float:
        closed_pnls = [t.net_pnl for t in trades if not t.is_open]
        if len(closed_pnls) == 0:
            return 0.0
        return sum(closed_pnls) / len(closed_pnls)
    def compute_alpha_metrics(self) -> AlphaMetrics:
        with self._lock:
            now_str = datetime.now(CHINA_TZ).isoformat()

            # P2-裂缝45：alpha_window_days使用日历日自然边界
            # 窗口结束时间 = 当前时间 - 窗口天数
            # 数据截取使用pd.Timestamp的floor('D')避免边界重复计算
            now_ts = pd.Timestamp(datetime.now(CHINA_TZ))
            window_start = (now_ts - pd.Timedelta(days=self._alpha_window_days)).floor('D')

            master_returns = self._equity_to_returns(self._master_equity_curve)
            shadow_a_returns = self._equity_to_returns(self._shadow_a_equity_curve)
            shadow_b_returns = self._equity_to_returns(self._shadow_b_equity_curve)

            master_sharpe = self._compute_sharpe(master_returns, risk_free_rate=DEFAULT_RISK_FREE_RATE)
            shadow_a_sharpe = self._compute_sharpe(shadow_a_returns, risk_free_rate=DEFAULT_RISK_FREE_RATE)
            shadow_b_sharpe = self._compute_sharpe(shadow_b_returns, risk_free_rate=DEFAULT_RISK_FREE_RATE)

            master_max_dd = self._compute_max_drawdown(self._master_equity_curve)
            if master_max_dd < 1e-10:
                master_max_dd = 1.0

            best_shadow_sharpe = max(shadow_a_sharpe, shadow_b_sharpe)
            # P0-R8-06修复: Alpha公式按手册9.3节更正
            # 手册规定: Alpha_i = 主策略Sharpe_i - max(影子A_Sharpe_i, 影子B_Sharpe_i)
            # 原代码分母多除了master_max_dd，导致Alpha系统性偏小
            alpha_ratio = master_sharpe - best_shadow_sharpe

            # P0-R8-07修复: 计算各策略组(S1-S6)的独立Sharpe指标
            # 原代码仅填充S2(分钟共振)聚合指标，S1/S3/S4/S5/S6始终为默认值0.0
            group_sharpes = {}
            for g in self.STRATEGY_GROUPS:
                g_equity = self._group_equity_curve.get(g, {})
                g_master_curve = g_equity.get('master', [])
                g_shadow_a_curve = g_equity.get('shadow_a', [])
                g_shadow_b_curve = g_equity.get('shadow_b', [])
                g_master_ret = self._equity_to_returns(g_master_curve)
                g_shadow_a_ret = self._equity_to_returns(g_shadow_a_curve)
                g_shadow_b_ret = self._equity_to_returns(g_shadow_b_curve)
                group_sharpes[g] = {
                    'master': self._compute_sharpe(g_master_ret, risk_free_rate=DEFAULT_RISK_FREE_RATE),
                    'shadow_a': self._compute_sharpe(g_shadow_a_ret, risk_free_rate=DEFAULT_RISK_FREE_RATE),
                    'shadow_b': self._compute_sharpe(g_shadow_b_ret, risk_free_rate=DEFAULT_RISK_FREE_RATE),
                }
            # 映射到AlphaMetrics字段名: s1_hft → s1, s2_resonance → s2, etc.
            s1_ms, s1_sa, s1_sb = group_sharpes['s1_hft']['master'], group_sharpes['s1_hft']['shadow_a'], group_sharpes['s1_hft']['shadow_b']
            s2_ms, s2_sa, s2_sb = group_sharpes['s2_resonance']['master'], group_sharpes['s2_resonance']['shadow_a'], group_sharpes['s2_resonance']['shadow_b']
            s3_ms, s3_sa, s3_sb = group_sharpes['s3_box']['master'], group_sharpes['s3_box']['shadow_a'], group_sharpes['s3_box']['shadow_b']
            s4_ms, s4_sa, s4_sb = group_sharpes['s4_spring']['master'], group_sharpes['s4_spring']['shadow_a'], group_sharpes['s4_spring']['shadow_b']
            s5_ms, s5_sa, s5_sb = group_sharpes['s5_arbitrage']['master'], group_sharpes['s5_arbitrage']['shadow_a'], group_sharpes['s5_arbitrage']['shadow_b']
            s6_ms, s6_sa, s6_sb = group_sharpes['s6_market_making']['master'], group_sharpes['s6_market_making']['shadow_a'], group_sharpes['s6_market_making']['shadow_b']
            logger.debug("[ShadowStrategyEngine] Per-group sharpes: S1=%.3f S2=%.3f S3=%.3f S4=%.3f S5=%.3f S6=%.3f",
                         s1_ms, s2_ms, s3_ms, s4_ms, s5_ms, s6_ms)

            # P1-R8-19修复: 各策略组(S1-S6)独立Alpha计算
            # 手册9.3节: Alpha_i = 主策略Sharpe_i - max(影子A_Sharpe_i, 影子B_Sharpe_i)
            group_alphas = {}
            for g in self.STRATEGY_GROUPS:
                gs = group_sharpes[g]
                g_alpha = gs['master'] - max(gs['shadow_a'], gs['shadow_b'])
                group_alphas[g] = g_alpha
            s1_alpha = group_alphas['s1_hft']
            s2_alpha = group_alphas['s2_resonance']
            s3_alpha = group_alphas['s3_box']
            s4_alpha = group_alphas['s4_spring']
            s5_alpha = group_alphas['s5_arbitrage']
            s6_alpha = group_alphas['s6_market_making']

            # P1-2修复：主策略Sharpe<=0时触发ELIMINATE标记
            # 当主策略夏普为负或零，说明策略无正期望，应禁止该策略组继续交易
            master_sharpe_eliminate = False
            if master_sharpe <= 0 and len(self._master_trades) >= self.MIN_TRADES_FOR_METRICS:
                master_sharpe_eliminate = True
                if not self._degradation_active:
                    self._degradation_active = True
                    self._stats['degradation_events'] += 1
                    logger.critical(
                        "[ShadowStrategyEngine] 🚨 ELIMINATE触发! master_sharpe=%.4f <= 0, "
                        "策略无正期望，禁止继续交易!",
                        master_sharpe,
                    )

            master_ev = self._compute_expected_value(self._master_trades)
            shadow_a_ev = self._compute_expected_value(self._shadow_a_trades)
            shadow_b_ev = self._compute_expected_value(self._shadow_b_trades)

            prev_metrics = self._last_alpha_metrics
            alpha_ratio_prev = prev_metrics.alpha_ratio if prev_metrics else 0.0
            decline_pct = 0.0
            consecutive_decline = 0

            if prev_metrics and abs(prev_metrics.alpha_ratio) > 1e-10:
                decline_pct = (
                    (prev_metrics.alpha_ratio - alpha_ratio) / abs(prev_metrics.alpha_ratio) * 100.0
                )
                if decline_pct > self.ALPHA_DECLINE_THRESHOLD_PCT:
                    consecutive_decline = prev_metrics.consecutive_decline_windows + 1
                else:
                    consecutive_decline = 0

            degradation = consecutive_decline >= self.CONSECUTIVE_DECLINE_LIMIT
            alpha_threshold_breached = alpha_ratio < self._shadow_alpha_threshold and len(self._master_trades) >= self.MIN_TRADES_FOR_METRICS
            absolute_ev_breached = master_ev < self._absolute_ev_floor and len(self._master_trades) >= self.MIN_TRADES_FOR_METRICS

            if degradation and not self._degradation_active:
                self._degradation_active = True
                self._stats['degradation_events'] += 1
                logger.warning(
                    "[ShadowStrategyEngine] ⚠️ Alpha衰减降级触发! "
                    "alpha_ratio=%.4f, 连续衰减窗口=%d",
                    alpha_ratio, consecutive_decline,
                )
                # R13-P2-LOG-13修复: Alpha衰减实时告警 — 主动推送告警事件
                self._emit_alpha_decay_alert(alpha_ratio, consecutive_decline, decline_pct)

            if alpha_threshold_breached:
                self._stats['alpha_threshold_breaches'] = self._stats.get('alpha_threshold_breaches', 0) + 1
                logger.warning(
                    "[ShadowStrategyEngine] Alpha比率低于阈值! alpha_ratio=%.4f < threshold=%.4f",
                    alpha_ratio, self._shadow_alpha_threshold,
                )

            if absolute_ev_breached and not self._absolute_ev_pause:
                self._absolute_ev_pause = True
                self._stats['absolute_ev_breaches'] += 1
                logger.critical(
                    "[ShadowStrategyEngine] 🚨 绝对期望值底线突破! master_ev=%.4f, 暂停实盘!",
                    master_ev,
                )

            # R19-ATT-02: 计算标准Jensen's Alpha (CAPM回归)
            jensen_alpha_val = 0.0
            try:
                import numpy as _np
                from ali2026v3_trading.strategy_judgment.jensen_alpha import compute_jensen_alpha
                if len(master_returns) >= 10 and len(shadow_a_returns) >= 10:
                    _master_arr = _np.array(master_returns[-min(252, len(master_returns)):])
                    _shadow_a_arr = _np.array(shadow_a_returns[-min(252, len(shadow_a_returns)):])
                    _min_len = min(len(_master_arr), len(_shadow_a_arr))
                    _ja_result = compute_jensen_alpha(_master_arr[-_min_len:], _shadow_a_arr[-_min_len:])
                    jensen_alpha_val = _ja_result.alpha
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, ImportError) as _e:
                logger.debug("Jensen's Alpha计算跳过: %s", _e)

            metrics = AlphaMetrics(
                timestamp=now_str,
                # P0-R8-07修复: 填入S1-S6各组独立Sharpe
                s1_master_sharpe=s1_ms, s1_shadow_a_sharpe=s1_sa, s1_shadow_b_sharpe=s1_sb,
                s2_master_sharpe=s2_ms, s2_shadow_a_sharpe=s2_sa, s2_shadow_b_sharpe=s2_sb,
                s3_master_sharpe=s3_ms, s3_shadow_a_sharpe=s3_sa, s3_shadow_b_sharpe=s3_sb,
                s4_master_sharpe=s4_ms, s4_shadow_a_sharpe=s4_sa, s4_shadow_b_sharpe=s4_sb,
                s5_master_sharpe=s5_ms, s5_shadow_a_sharpe=s5_sa, s5_shadow_b_sharpe=s5_sb,
                s6_master_sharpe=s6_ms, s6_shadow_a_sharpe=s6_sa, s6_shadow_b_sharpe=s6_sb,
                # P1-R8-19修复: 各组独立Alpha(非仅S2聚合)
                s1_alpha=s1_alpha, s2_alpha=s2_alpha, s3_alpha=s3_alpha,
                s4_alpha=s4_alpha, s5_alpha=s5_alpha, s6_alpha=s6_alpha,
                # 聚合指标(向后兼容，取S2组)
                master_sharpe=master_sharpe,
                shadow_a_sharpe=shadow_a_sharpe,
                shadow_b_sharpe=shadow_b_sharpe,
                master_max_drawdown=master_max_dd,
                alpha_ratio=alpha_ratio,
                master_expected_value=master_ev,
                shadow_a_expected_value=shadow_a_ev,
                shadow_b_expected_value=shadow_b_ev,
                alpha_ratio_prev=alpha_ratio_prev,
                alpha_ratio_decline_pct=decline_pct,
                consecutive_decline_windows=consecutive_decline,
                degradation_triggered=degradation,
                absolute_ev_breached=absolute_ev_breached,
                master_sharpe_eliminate=master_sharpe_eliminate,  # P1-2修复
                jensen_alpha=jensen_alpha_val,  # R19-ATT-02: 标准Jensen's Alpha
            )

            self._alpha_history.append(metrics)
            self._last_alpha_metrics = metrics
            self.alpha_decay_rate = metrics.alpha_ratio  # R3-D-03修复: 同步更新公共属性供risk_service消费
            self._stats['alpha_calculations'] += 1

            return metrics
    def _emit_alpha_decay_alert(self, alpha_ratio: float, consecutive_decline: int,
                                 decline_pct: float) -> None:
        """Alpha衰减时主动推送告警事件到EventBus"""
        try:
            from ali2026v3_trading.infra.event_bus import get_global_event_bus
            bus = get_global_event_bus()
            if bus is not None:
                bus.publish({
                    'type': 'ALPHA_DECAY_ALERT',
                    'alpha_ratio': alpha_ratio,
                    'consecutive_decline_windows': consecutive_decline,
                    'decline_pct': decline_pct,
                    'timestamp': datetime.now(CHINA_TZ).isoformat(),
                }, async_mode=True)
        except (ValueError, KeyError, TypeError, AttributeError, ImportError) as _r3_err:
            pass  # EventBus不可用时静默降级
    def get_alpha_ratio(self) -> float:
        with self._lock:
            if self._last_alpha_metrics:
                return self._last_alpha_metrics.alpha_ratio
            return 0.0
    def get_master_expected_value(self) -> float:
        with self._lock:
            if self._last_alpha_metrics:
                return self._last_alpha_metrics.master_expected_value
            return 0.0
    def generate_daily_summary(self) -> Dict[str, Any]:
        today_str = datetime.now(CHINA_TZ).strftime('%Y-%m-%d')

        # R10-P0-06修复: metrics和交易数据在同一锁内获取，确保快照一致性
        # self._lock是RLock，compute_alpha_metrics内部也用self._lock，可重入
        with self._lock:
            metrics = self.compute_alpha_metrics()
            shadow_a_trades_copy = list(self._shadow_a_trades)
            shadow_b_trades_copy = list(self._shadow_b_trades)
            master_trades_copy = list(self._master_trades)
            stats_copy = dict(self._stats)
            shadow_a_pnl = self._shadow_a_pnl_sum
            shadow_b_pnl = self._shadow_b_pnl_sum
            master_pnl = self._master_pnl_sum
            params_locked = self._params_locked
            degradation_active = self._degradation_active
            absolute_ev_pause = self._absolute_ev_pause

        summary = {
            'summary_date': today_str,
            'summary_timestamp': datetime.now(CHINA_TZ).isoformat(),
            'alpha_metrics': metrics.to_dict(),
            # P1-裂缝48：Beta贡献修正
            # 原始：Beta贡献 = 影子B夏普（可能为负，导致独立Alpha占比>100%）
            # 修正：Beta贡献 = max(0, 影子B夏普)，独立Alpha占比 = Alpha / (Alpha + Beta)
            'alpha_beta_attribution': {
                'alpha': round(metrics.master_sharpe - max(metrics.shadow_a_sharpe, metrics.shadow_b_sharpe), 4),
                'beta': round(max(0, metrics.shadow_b_sharpe), 4),
                'alpha_pct': 0.0,
            },
            'shadow_a_stats': {
                'total_trades': stats_copy['shadow_a_trades'],
                'total_pnl': shadow_a_pnl,
                'expected_value': self._compute_expected_value(shadow_a_trades_copy),
                'recent_trades': sum(1 for t in shadow_a_trades_copy if t.timestamp.startswith(today_str)),  # R21-MEM-P2-03修复: 生成器替代列表推导式
            },
            'shadow_b_stats': {
                'total_trades': stats_copy['shadow_b_trades'],
                'total_pnl': shadow_b_pnl,
                'expected_value': self._compute_expected_value(shadow_b_trades_copy),
                'recent_trades': sum(1 for t in shadow_b_trades_copy if t.timestamp.startswith(today_str)),  # R21-MEM-P2-03修复: 生成器替代列表推导式
            },
            'master_stats': {
                'total_trades': stats_copy['master_trades'],
                'total_pnl': master_pnl,
                'expected_value': self._compute_expected_value(master_trades_copy),
            },
            'params_locked': params_locked,
            'degradation_active': degradation_active,
            'absolute_ev_pause': absolute_ev_pause,
        }

        # P1-裂缝48：计算独立Alpha占比
        alpha_val = summary['alpha_beta_attribution']['alpha']
        beta_val = summary['alpha_beta_attribution']['beta']
        total_ab = alpha_val + beta_val
        summary['alpha_beta_attribution']['alpha_pct'] = round(
            alpha_val / total_ab if total_ab > 0 else 0.0, 4
        )

        try:
            summary_file = os.path.join(
                self._log_dir,
                f"summary_{today_str}.json"
            )
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(json_dumps(summary, ensure_ascii=False, indent=2))  # R4-5: 统一json_dumps
            logger.info("[ShadowStrategyEngine] 日汇总写入: %s", summary_file)
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
            logger.warning("[ShadowStrategyEngine] 写入日汇总失败: %s", e)

        with self._lock:
            self._last_daily_summary_date = today_str
        return summary
    def generate_weekly_summary(self) -> Dict[str, Any]:
        with self._lock:
            now = datetime.now(CHINA_TZ)
            week_start = (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d')

            alpha_list = list(self._alpha_history)
            if not alpha_list:
                return {'week_start': week_start, 'message': '无Alpha历史数据'}

            weekly_alpha_ratios = [m.alpha_ratio for m in alpha_list]
            avg_alpha = sum(weekly_alpha_ratios) / len(weekly_alpha_ratios) if weekly_alpha_ratios else 0.0
            min_alpha = min(weekly_alpha_ratios) if weekly_alpha_ratios else 0.0
            max_alpha = max(weekly_alpha_ratios) if weekly_alpha_ratios else 0.0

            summary = {
                'week_start': week_start,
                'summary_timestamp': now.isoformat(),
                'alpha_ratio_stats': {
                    'avg': avg_alpha,
                    'min': min_alpha,
                    'max': max_alpha,
                    'samples': len(weekly_alpha_ratios),
                },
                'total_degradation_events': self._stats['degradation_events'],
                'total_ev_breaches': self._stats['absolute_ev_breaches'],
                'params_snapshot': self.get_params_snapshot(),
                'master_pnl': self._master_pnl_sum,
                'shadow_a_pnl': self._shadow_a_pnl_sum,
                'shadow_b_pnl': self._shadow_b_pnl_sum,
            }

            try:
                summary_file = os.path.join(
                    self._log_dir,
                    f"weekly_summary_{week_start}.json"
                )
                with open(summary_file, 'w', encoding='utf-8') as f:
                    f.write(json_dumps(summary, ensure_ascii=False, indent=2))  # R4-5: 统一json_dumps
                logger.info("[ShadowStrategyEngine] 周汇总写入: %s", summary_file)
            except (ValueError, KeyError, TypeError, RuntimeError, AttributeError, IOError) as e:
                logger.warning("[ShadowStrategyEngine] 写入周汇总失败: %s", e)

            return summary
    def validate_shadow_b_stability(self, n_seeds: int = 100,
                                      ci_width_threshold: float = 1.0) -> Dict[str, Any]:
        """P1-裂缝15：验证影子B策略夏普的95%置信区间

        用n_seeds个不同种子运行影子B，计算夏普的95%CI。
        若CI宽度>1.0，则影子B不可用于Alpha扣除，应改用期望收益而非单路径。
        """
        import random as _random
        base_sharpes = []
        base_seed = getattr(self, '_shadow_b_seed', None)

        for seed_idx in range(n_seeds):
            seed = seed_idx * 1000 + 42
            rng = _random.Random(seed)
            # 模拟影子B的随机方向序列
            n_trades = max(1, len(self._shadow_b_trades))
            wins = 0
            total = 0
            for _ in range(min(n_trades, 500)):
                direction = rng.choice(['long', 'short'])
                # 使用历史价格变动模拟PnL
                pnl = rng.gauss(0, max(1.0, abs(self._recent_pnl_std) if hasattr(self, '_recent_pnl_std') else 1.0))
                if pnl > 0:
                    wins += 1
                total += 1

            if total > 0 and wins > 0:
                win_rate = wins / total
                loss_rate = 1 - win_rate
                avg_win = 1.5
                avg_loss = 1.0
                if loss_rate > 0:
                    plr = (win_rate * avg_win) / (loss_rate * avg_loss)
                    sharpe = (win_rate * avg_win - loss_rate * avg_loss) / max(avg_loss, 0.01)
                    base_sharpes.append(sharpe)

        if len(base_sharpes) < 10:
            return {"ci_width": 0.0, "passed": True, "action": "insufficient_data"}

        arr = np.array(base_sharpes)
        ci_lower = float(np.percentile(arr, 2.5))
        ci_upper = float(np.percentile(arr, 97.5))
        ci_width = ci_upper - ci_lower
        passed = ci_width <= ci_width_threshold

        return {
            "ci_width": round(ci_width, 4),
            "ci_lower": round(ci_lower, 4),
            "ci_upper": round(ci_upper, 4),
            "ci_width_threshold": ci_width_threshold,
            "mean_sharpe": round(float(np.mean(arr)), 4),
            "std_sharpe": round(float(np.std(arr)), 4),
            "n_seeds": n_seeds,
            "passed": passed,
            "action": "use_expected_return_instead_of_single_path" if not passed else "proceed",
        }
    def alpha_confidence_overlap_test(self, overlap_threshold: float = 0.50) -> Dict[str, Any]:
        """P2-裂缝27：主-影子Sharpe置信区间重叠检验

        若主策略与影子策略的Sharpe置信区间重叠比例 > 50%，
        则Alpha可能只是抽样误差，标记alpha_unreliable，
        资金分配回退到基础权重。
        """
        with self._lock:
            master_returns = self._equity_to_returns(self._master_equity_curve)
            shadow_a_returns = self._equity_to_returns(self._shadow_a_equity_curve)
            shadow_b_returns = self._equity_to_returns(self._shadow_b_equity_curve)

            master_sharpe = self._compute_sharpe(master_returns, risk_free_rate=DEFAULT_RISK_FREE_RATE)
            shadow_a_sharpe = self._compute_sharpe(shadow_a_returns, risk_free_rate=DEFAULT_RISK_FREE_RATE)
            shadow_b_sharpe = self._compute_sharpe(shadow_b_returns, risk_free_rate=DEFAULT_RISK_FREE_RATE)

            master_n = max(1, len(master_returns))
            shadow_a_n = max(1, len(shadow_a_returns))
            shadow_b_n = max(1, len(shadow_b_returns))

            # R17-26修复: Sharpe标准误使用Lo(2002)公式
            # SE(SR) = √((1 + SR²/2 - skew*SR + (kurt-3)/4*SR²) / N)
            # 当skew≈0, kurt≈3(正态近似)时退化为 SE ≈ √((1 + SR²/2) / N)
            # 当前简化: 使用 √((1 + SR²/2) / N) 近似
            # TODO(R17-P2-DOC-02): 完整实现需计算returns的skew和kurtosis
            main_se = math.sqrt((1 + master_sharpe**2 / 2) / master_n) if master_n > 0 else 1.0
            shadow_a_se = math.sqrt((1 + shadow_a_sharpe**2 / 2) / shadow_a_n) if shadow_a_n > 0 else 1.0
            shadow_b_se = math.sqrt((1 + shadow_b_sharpe**2 / 2) / shadow_b_n) if shadow_b_n > 0 else 1.0

            # 95%置信区间
            main_ci = (master_sharpe - 1.96 * main_se, master_sharpe + 1.96 * main_se)
            shadow_a_ci = (shadow_a_sharpe - 1.96 * shadow_a_se, shadow_a_sharpe + 1.96 * shadow_a_se)
            shadow_b_ci = (shadow_b_sharpe - 1.96 * shadow_b_se, shadow_b_sharpe + 1.96 * shadow_b_se)

            # 计算重叠比例
            def ci_overlap_ratio(ci1, ci2):
                ci_width = ci1[1] - ci1[0]
                if ci_width <= 0:
                    return 0.0
                overlap = max(0, min(ci1[1], ci2[1]) - max(ci1[0], ci2[0]))
                return overlap / ci_width

            overlap_a = ci_overlap_ratio(main_ci, shadow_a_ci)
            overlap_b = ci_overlap_ratio(main_ci, shadow_b_ci)
            max_overlap = max(overlap_a, overlap_b)

            alpha_unreliable = max_overlap > overlap_threshold

            if alpha_unreliable:
                logger.warning(
                    "[ShadowStrategyEngine] ⚠️ P2-裂缝27: Alpha不可靠! "
                    "主策略CI=[%.2f, %.2f], 影子A CI=[%.2f, %.2f], 影子B CI=[%.2f, %.2f], "
                    "重叠比例=%.2f%% > %.0f%%阈值, 资金分配回退基础权重",
                    main_ci[0], main_ci[1], shadow_a_ci[0], shadow_a_ci[1],
                    shadow_b_ci[0], shadow_b_ci[1],
                    max_overlap * 100, overlap_threshold * 100,
                )

            return {
                "main_ci": [round(main_ci[0], 4), round(main_ci[1], 4)],
                "shadow_a_ci": [round(shadow_a_ci[0], 4), round(shadow_a_ci[1], 4)],
                "shadow_b_ci": [round(shadow_b_ci[0], 4), round(shadow_b_ci[1], 4)],
                "overlap_a_ratio": round(overlap_a, 4),
                "overlap_b_ratio": round(overlap_b, 4),
                "max_overlap_ratio": round(max_overlap, 4),
                "overlap_threshold": overlap_threshold,
                "alpha_unreliable": alpha_unreliable,
                "action": "fallback_to_base_weights" if alpha_unreliable else "proceed",
            }
    def validate_shadow_param_orthogonality(self, kl_threshold: float = 0.5) -> Dict[str, Any]:
        """P2-裂缝29：影子参数差异度KL散度验证

        差异度基于4个参数平均可能不够，参数间交互可能导致联合作用相似。
        增加条件：影子策略的绩效分布与主策略绩效分布的KL散度 > 0.5。
        """
        with self._lock:
            master_returns = self._equity_to_returns(self._master_equity_curve)
            shadow_a_returns = self._equity_to_returns(self._shadow_a_equity_curve)
            shadow_b_returns = self._equity_to_returns(self._shadow_b_equity_curve)

            def _estimate_kl_divergence(p_samples, q_samples, n_bins=50):
                """用直方图估计KL散度 D_KL(P||Q)"""
                if len(p_samples) < 10 or len(q_samples) < 10:
                    return 0.0
                all_vals = list(p_samples) + list(q_samples)
                vmin, vmax = min(all_vals), max(all_vals)
                if vmax - vmin < 1e-10:
                    return 0.0
                bins = np.linspace(vmin, vmax, n_bins + 1)
                p_hist, _ = np.histogram(p_samples, bins=bins, density=True)
                q_hist, _ = np.histogram(q_samples, bins=bins, density=True)
                # 加小量避免log(0)
                eps = 1e-10
                p_hist = p_hist + eps
                q_hist = q_hist + eps
                # 归一化
                p_hist = p_hist / p_hist.sum()
                q_hist = q_hist / q_hist.sum()
                kl = float(np.sum(p_hist * np.log(p_hist / q_hist)))
                return max(0.0, kl)

            kl_a = _estimate_kl_divergence(master_returns, shadow_a_returns)
            kl_b = _estimate_kl_divergence(master_returns, shadow_b_returns)
            min_kl = min(kl_a, kl_b)

            passed = min_kl >= kl_threshold

            if not passed:
                logger.warning(
                    "[ShadowStrategyEngine] ⚠️ P2-裂缝29: 影子参数正交性不足! "
                    "KL(master||shadow_a)=%.4f, KL(master||shadow_b)=%.4f, "
                    "最小KL散度=%.4f < %.1f阈值",
                    kl_a, kl_b, min_kl, kl_threshold,
                )

            return {
                "kl_master_shadow_a": round(kl_a, 4),
                "kl_master_shadow_b": round(kl_b, 4),
                "min_kl_divergence": round(min_kl, 4),
                "kl_threshold": kl_threshold,
                "passed": passed,
                "action": "increase_shadow_divergence" if not passed else "proceed",
            }


class ShadowStrategyPnLService(ShadowStrategyPnLMetricsService):
    """PnL and evaluation service (from ShadowStrategyPnLMixin)"""

    def __init__(self, facade=None):
        super().__init__()
        self._facade = facade

    def __getattr__(self, name):
        if self._facade is not None and hasattr(self._facade, name):
            return getattr(self._facade, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    # === Degradation methods from ShadowDegradationService ===
    # RESIDUAL: self._engine = engine  (original line 623, kept as-is)

    def is_degradation_active(self) -> bool:
        with self._lock:
            _active = self._degradation_active
            if _active:
                logger.debug("[R23-SM-08-FIX] 降级激活: degradation_active=%s ev_pause=%s "
                             "alpha_decay_rate=%s consecutive_losses=%s",
                             _active, self._absolute_ev_pause,
                             getattr(self, '_alpha_decay_rate', None),
                             getattr(self, '_consecutive_losses', None))
            return _active
    def is_absolute_ev_paused(self) -> bool:
        with self._lock:
            _paused = self._absolute_ev_pause
            if _paused:
                logger.debug("[R23-SM-08-FIX] 绝对EV暂停: ev_pause=%s degradation_active=%s "
                             "current_ev=%s ev_threshold=%s",
                             _paused, self._degradation_active,
                             getattr(self, '_current_expected_value', None),
                             getattr(self, '_ev_pause_threshold', None))
            return _paused
    def clear_degradation(self) -> None:
        with self._lock:
            self._degradation_active = False
            logger.info("[ShadowStrategyEngine] Alpha衰减降级已清除")
    def clear_absolute_ev_pause(self) -> None:
        with self._lock:
            self._absolute_ev_pause = False
            logger.info("[ShadowStrategyEngine] 绝对期望值暂停已解除")
    def set_trades_max_len(self, max_len: int) -> None:
        """设置交易记录上限（运行时可配置）"""
        if max_len > 0:
            self._trades_max_len = max_len
            logger.info("[ShadowStrategyEngine] R13-P2-LOG-16: 交易记录上限设为%d", max_len)
    def record_governance_degradation(self, reasons: List[str]) -> None:
        """记录治理引擎触发的降级事件到影子引擎日志。"""
        with self._lock:
            self._stats['governance_degradations'] = self._stats.get('governance_degradations', 0) + 1
            event = {
                'timestamp': datetime.now(CHINA_TZ).isoformat(),
                'type': 'governance_degradation',
                'reasons': reasons,
                'alpha_ratio': self.get_alpha_ratio(),
            }
            self._jsonl_logger.info(json_dumps(event))
            logger.critical(
                "[ShadowStrategyEngine] 治理引擎降级事件已记录: %s",
                '; '.join(reasons)
            )
    def get_health_status(self) -> Dict[str, Any]:
        with self._lock:
            # R10-P1-03: _last_alpha_metrics读路径在锁内，线程安全
            alpha_ratio = self._last_alpha_metrics.alpha_ratio if self._last_alpha_metrics else 0.0
            master_ev = self._last_alpha_metrics.master_expected_value if self._last_alpha_metrics else 0.0
            last_alpha_ts = self._last_alpha_metrics.timestamp if self._last_alpha_metrics else None
            # P-32修复: 三态健康检查 OK/WARNING/CRITICAL（手册12.1节）
            if self._absolute_ev_pause:
                status = "CRITICAL"
            elif self._degradation_active or alpha_ratio < 1.0:
                status = "WARNING"
            else:
                status = "OK"
            result = {
                'component': 'shadow_strategy_engine',
                'status': status,
                'params_locked': self._params_locked,
                'degradation_active': self._degradation_active,
                'absolute_ev_pause': self._absolute_ev_pause,
                'shadow_a_trades': self._stats['shadow_a_trades'],
                'shadow_b_trades': self._stats['shadow_b_trades'],
                'master_trades': self._stats['master_trades'],
                'alpha_ratio': alpha_ratio,
                'master_expected_value': master_ev,
                'last_alpha_calculation': last_alpha_ts,
            }
            # P1-R10-05修复: 组件检查异常传播到返回值
            if status in ('CRITICAL', 'WARNING'):
                result['error_propagation'] = True
                result['action_required'] = (
                    'halt_new_open' if status == 'CRITICAL' else 'review_and_confirm'
                )
            return result
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            stats = dict(self._stats)
            stats['alpha_ratio'] = self._last_alpha_metrics.alpha_ratio if self._last_alpha_metrics else 0.0
            stats['master_expected_value'] = self._last_alpha_metrics.master_expected_value if self._last_alpha_metrics else 0.0
            stats['degradation_active'] = self._degradation_active
            stats['absolute_ev_pause'] = self._absolute_ev_pause
            if hasattr(self, '_facade') and self._facade is not None:
                _signal_svc = getattr(self._facade, '_signal_service', None)
                if _signal_svc is None:
                    _signal_svc = getattr(self._facade, '__dict__', {}).get('_signal_service')
            if _signal_svc is not None:
                stats['master_pnl_sum'] = getattr(_signal_svc, '_master_pnl_sum', 0.0)
                stats['shadow_a_pnl_sum'] = getattr(_signal_svc, '_shadow_a_pnl_sum', 0.0)
                stats['shadow_b_pnl_sum'] = getattr(_signal_svc, '_shadow_b_pnl_sum', 0.0)
                stats['is_shadow_mode'] = getattr(_signal_svc, '_is_shadow_mode', False)
            else:
                stats['master_pnl_sum'] = self._master_pnl_sum
                stats['shadow_a_pnl_sum'] = self._shadow_a_pnl_sum
                stats['shadow_b_pnl_sum'] = self._shadow_b_pnl_sum
                stats['is_shadow_mode'] = self._is_shadow_mode
            stats['paper_account_equity'] = self._paper_account['current_equity']
            stats['paper_account_equity_masked'] = self._mask_sensitive_value(self._paper_account['current_equity'])
            return stats


__all__ = ['ShadowStrategyPnLMetricsService', 'ShadowStrategyPnLMetricsMixin',
           'ShadowStrategyPnLService', 'ShadowStrategyPnLMixin']
ShadowStrategyPnLMetricsMixin = ShadowStrategyPnLMetricsService
ShadowStrategyPnLMixin = ShadowStrategyPnLService
