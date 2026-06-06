"""
sensitivity_analysis.py - 参数敏感性分析工具

对关键参数逐一进行 ±N% 扰动测试，报告策略排名/夏普/回撤变化。
用于识别哪些参数最敏感（微调即大幅影响策略），哪些较鲁棒。

用法：
    from sensitivity_analysis import SensitivityAnalyzer

    analyzer = SensitivityAnalyzer(
        db_path="data/processed/minute_bars.duckdb",
        base_params={...},
        train_period=("2024-01-01", "2025-06-30"),
        test_period=("2025-07-01", "2026-04-30"),
    )
    report = analyzer.run(perturb_pct=0.05, top_k=10)
    analyzer.print_report(report)

架构约束：
- 复用 task_scheduler.run_backtest() 的策略逻辑，确保敏感性分析与回测一致性
- 单参数扰动（one-at-a-time, OAT），不组合扰动（组合维度爆炸）
- 扰动后自动 clamp 到 attribute_matrix 定义的 range
- 结果写入 DuckDB 表 sensitivity_results 供后续查询
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    from ali2026v3_trading.data_access import get_data_access
    from ali2026v3_trading.db_adapter import connect, get_duckdb_module
    duckdb = get_duckdb_module()
except ImportError:
    duckdb = None
import numpy as np
import pandas as pd

try:
    import task_scheduler as _ts
    _run_backtest = _ts.run_backtest
    _load_data_for_period = _ts._load_data_for_period
except ImportError:
    from ali2026v3_trading.param_pool import task_scheduler as _ts
    _run_backtest = _ts.run_backtest
    _load_data_for_period = _ts._load_data_for_period

logger = logging.getLogger(__name__)

_yaml_cache: Optional[Dict[str, Any]] = None
_yaml_mtime: float = 0.0
_yaml_path: str = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'parameter_attribute_matrix.yaml',
)


def _load_attribute_matrix() -> Dict[str, Any]:
    global _yaml_cache, _yaml_mtime
    try:
        _current_mtime = os.path.getmtime(_yaml_path)
    except OSError:
        if _yaml_cache is not None:
            return _yaml_cache
        return {}
    if _yaml_cache is not None and abs(_current_mtime - _yaml_mtime) < 1.0:
        return _yaml_cache
    try:
        import yaml
        with open(_yaml_path, 'r', encoding='utf-8') as f:
            _yaml_cache = yaml.safe_load(f) or {}
        _yaml_mtime = _current_mtime
        return _yaml_cache
    except Exception as e:
        logger.warning("Failed to load attribute matrix: %s", e)
        return _yaml_cache or {}


def _clamp_value(value: float, attr: Optional[Dict[str, Any]]) -> float:
    if not attr:
        return value
    value_range = attr.get('range')
    if value_range and isinstance(value_range, list) and len(value_range) == 2:
        lo, hi = value_range
        return max(lo, min(hi, value))
    return value


class SensitivityResult:
    __slots__ = (
        'param_key', 'base_value', 'perturb_pct',
        'base_sharpe', 'low_sharpe', 'high_sharpe',
        'base_return', 'low_return', 'high_return',
        'base_max_dd', 'low_max_dd', 'high_max_dd',
        'sharpe_delta_low', 'sharpe_delta_high', 'sharpe_sensitivity',
        'base_profit_factor', 'low_profit_factor', 'high_profit_factor',
        'base_win_loss_ratio', 'low_win_loss_ratio', 'high_win_loss_ratio',
        'plr_sensitivity',
    )

    def __init__(self, param_key: str, base_value: float, perturb_pct: float):
        self.param_key = param_key
        self.base_value = base_value
        self.perturb_pct = perturb_pct
        self.base_sharpe = 0.0
        self.low_sharpe = 0.0
        self.high_sharpe = 0.0
        self.base_return = 0.0
        self.low_return = 0.0
        self.high_return = 0.0
        self.base_max_dd = 0.0
        self.low_max_dd = 0.0
        self.high_max_dd = 0.0
        self.sharpe_delta_low = 0.0
        self.sharpe_delta_high = 0.0
        self.sharpe_sensitivity = 0.0
        self.base_profit_factor = 0.0
        self.low_profit_factor = 0.0
        self.high_profit_factor = 0.0
        self.base_win_loss_ratio = 0.0
        self.low_win_loss_ratio = 0.0
        self.high_win_loss_ratio = 0.0
        self.plr_sensitivity = 0.0

    def compute_sensitivity(self) -> None:
        self.sharpe_delta_low = self.low_sharpe - self.base_sharpe
        self.sharpe_delta_high = self.high_sharpe - self.base_sharpe
        self.sharpe_sensitivity = abs(self.sharpe_delta_high - self.sharpe_delta_low) / 2.0
        pf_delta_low = self.low_profit_factor - self.base_profit_factor
        pf_delta_high = self.high_profit_factor - self.base_profit_factor
        wlr_delta_low = self.low_win_loss_ratio - self.base_win_loss_ratio
        wlr_delta_high = self.high_win_loss_ratio - self.base_win_loss_ratio
        self.plr_sensitivity = (
            abs(pf_delta_high - pf_delta_low) / 2.0 * 0.6
            + abs(wlr_delta_high - wlr_delta_low) / 2.0 * 0.4
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'param_key': self.param_key,
            'base_value': self.base_value,
            'perturb_pct': self.perturb_pct,
            'base_sharpe': self.base_sharpe,
            'low_sharpe': self.low_sharpe,
            'high_sharpe': self.high_sharpe,
            'sharpe_delta_low': self.sharpe_delta_low,
            'sharpe_delta_high': self.sharpe_delta_high,
            'sharpe_sensitivity': self.sharpe_sensitivity,
            'base_return': self.base_return,
            'low_return': self.low_return,
            'high_return': self.high_return,
            'base_max_dd': self.base_max_dd,
            'low_max_dd': self.low_max_dd,
            'high_max_dd': self.high_max_dd,
            'base_profit_factor': self.base_profit_factor,
            'low_profit_factor': self.low_profit_factor,
            'high_profit_factor': self.high_profit_factor,
            'base_win_loss_ratio': self.base_win_loss_ratio,
            'low_win_loss_ratio': self.low_win_loss_ratio,
            'high_win_loss_ratio': self.high_win_loss_ratio,
            'plr_sensitivity': self.plr_sensitivity,
        }


class SensitivityAnalyzer:
    def __init__(
        self,
        db_path: str,
        base_params: Dict[str, float],
        train_period: Tuple[str, str],
        test_period: Tuple[str, str],
        symbols: Optional[List[str]] = None,
    ):
        self.db_path = db_path
        self.base_params = dict(base_params)
        self.train_period = train_period
        self.test_period = test_period
        self.symbols = symbols
        self._attr_matrix = _load_attribute_matrix()

        logger.info("Loading train data: %s ~ %s", *train_period)
        self._train_data = _load_data_for_period(
            db_path, train_period[0], train_period[1], symbols,
        )
        logger.info("Loading test data: %s ~ %s", *test_period)
        self._test_data = _load_data_for_period(
            db_path, test_period[0], test_period[1], symbols,
        )
        logger.info("Train bars: %d, Test bars: %d", len(self._train_data), len(self._test_data))

    def _run_single(self, params: Dict[str, float], data: pd.DataFrame) -> Dict[str, Any]:
        return _run_backtest(params, data, train=True)

    def run(
        self,
        perturb_pct: float = 0.05,
        target_params: Optional[List[str]] = None,
        top_k: int = 10,
        use_test: bool = True,
    ) -> List[SensitivityResult]:
        if target_params is None:
            target_params = list(self.base_params.keys())

        data = self._test_data if use_test else self._train_data
        results: List[SensitivityResult] = []

        base_result = self._run_single(self.base_params, data)
        base_sharpe = base_result.get('sharpe', 0.0)
        base_return = base_result.get('total_return', 0.0)
        base_max_dd = base_result.get('max_drawdown', 0.0)
        base_profit_factor = base_result.get('profit_factor', 0.0)
        base_win_loss_ratio = base_result.get('win_loss_ratio', 0.0)
        logger.info("Base: sharpe=%.4f, return=%.4f, max_dd=%.4f, pf=%.4f, wlr=%.4f",
                     base_sharpe, base_return, base_max_dd, base_profit_factor, base_win_loss_ratio)

        for key in target_params:
            base_val = self.base_params.get(key)
            if base_val is None or not isinstance(base_val, (int, float)):
                continue
            if base_val == 0.0:
                logger.info("Skip %s: base_value=0 (cannot perturb by pct)", key)
                continue

            attr = self._attr_matrix.get(key) if isinstance(self._attr_matrix.get(key), dict) else None
            sr = SensitivityResult(key, float(base_val), perturb_pct)
            sr.base_sharpe = base_sharpe
            sr.base_return = base_return
            sr.base_max_dd = base_max_dd
            sr.base_profit_factor = base_profit_factor
            sr.base_win_loss_ratio = base_win_loss_ratio

            low_val = _clamp_value(base_val * (1.0 - perturb_pct), attr)
            high_val = _clamp_value(base_val * (1.0 + perturb_pct), attr)

            if abs(low_val - base_val) < 1e-12 and abs(high_val - base_val) < 1e-12:
                logger.info("Skip %s: perturbed values clamped to base (range too narrow)", key)
                continue

            params_low = dict(self.base_params)
            params_low[key] = low_val
            res_low = self._run_single(params_low, data)

            params_high = dict(self.base_params)
            params_high[key] = high_val
            res_high = self._run_single(params_high, data)

            sr.low_sharpe = res_low.get('sharpe', 0.0)
            sr.high_sharpe = res_high.get('sharpe', 0.0)
            sr.low_return = res_low.get('total_return', 0.0)
            sr.high_return = res_high.get('total_return', 0.0)
            sr.low_max_dd = res_low.get('max_drawdown', 0.0)
            sr.high_max_dd = res_high.get('max_drawdown', 0.0)
            sr.low_profit_factor = res_low.get('profit_factor', 0.0)
            sr.high_profit_factor = res_high.get('profit_factor', 0.0)
            sr.low_win_loss_ratio = res_low.get('win_loss_ratio', 0.0)
            sr.high_win_loss_ratio = res_high.get('win_loss_ratio', 0.0)
            sr.compute_sensitivity()

            logger.info(
                "  %s: sensitivity=%.4f | low_s=%.4f base_s=%.4f high_s=%.4f",
                key, sr.sharpe_sensitivity, sr.low_sharpe, sr.base_sharpe, sr.high_sharpe,
            )
            results.append(sr)

        results.sort(key=lambda r: r.sharpe_sensitivity, reverse=True)
        return results[:top_k] if top_k > 0 else results

    def save_to_duckdb(
        self,
        results: List[SensitivityResult],
        db_path: Optional[str] = None,
        table_name: str = 'sensitivity_results',
    ) -> None:
        if db_path is None:
            db_path = self.db_path
        con = connect(db_path)
        try:
            safe_table_name = table_name.replace("'", "''").replace(";", "")
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {safe_table_name} (
                    param_key VARCHAR,
                    base_value DOUBLE,
                    perturb_pct DOUBLE,
                    base_sharpe DOUBLE,
                    low_sharpe DOUBLE,
                    high_sharpe DOUBLE,
                    sharpe_delta_low DOUBLE,
                    sharpe_delta_high DOUBLE,
                    sharpe_sensitivity DOUBLE,
                    base_return DOUBLE,
                    low_return DOUBLE,
                    high_return DOUBLE,
                    base_max_dd DOUBLE,
                    low_max_dd DOUBLE,
                    high_max_dd DOUBLE,
                    analyzed_at VARCHAR
                )
            """)
            now_str = time.strftime('%Y-%m-%d %H:%M:%S')
            for sr in results:
                d = sr.to_dict()
                con.execute(
                    f"INSERT INTO {safe_table_name} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",  # 预留灵敏度分析查询表(P2-10)
                    [
                        d['param_key'], d['base_value'], d['perturb_pct'],
                        d['base_sharpe'], d['low_sharpe'], d['high_sharpe'],
                        d['sharpe_delta_low'], d['sharpe_delta_high'], d['sharpe_sensitivity'],
                        d['base_return'], d['low_return'], d['high_return'],
                        d['base_max_dd'], d['low_max_dd'], d['high_max_dd'],
                        now_str,
                    ],
                )
            logger.info("Saved %d sensitivity results to %s.%s", len(results), db_path, table_name)
        finally:
            con.close()

    def query_sensitivity_results(self, table_name: str, param_name: Optional[str] = None) -> List[Dict[str, Any]]:  # P2-10修复: 提供sensitivity查询消费方法
        """查询灵敏度分析结果"""
        try:
            safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
            conn = connect(self.db_path)
            try:
                if param_name:
                    rows = conn.execute(f"SELECT * FROM {safe_table_name} WHERE param_key = ?", [param_name]).fetchall()
                else:
                    rows = conn.execute(f"SELECT * FROM {safe_table_name} LIMIT 1000").fetchall()
                columns = [desc[0] for desc in conn.execute(f"SELECT * FROM {safe_table_name} LIMIT 0").description]
                return [dict(zip(columns, row)) for row in rows]
            finally:
                conn.close()
        except Exception as e:
            logging.debug("[SensitivityAnalyzer] P2-10: query_sensitivity_results查询失败: %s", e)
            return []

    @staticmethod
    def print_report(results: List[SensitivityResult]) -> None:
        if not results:
            logger.info("No sensitivity results to display.")
            return

        logger.info("\n" + "=" * 100)
        logger.info("参数敏感性分析报告 (OAT: One-At-a-Time ±{:.1%} 扰动)".format(results[0].perturb_pct))
        logger.info("=" * 100)
        logger.info(
            f"{'参数':<30} {'基准值':>10} {'夏普敏感度':>12} "
            f"{'Δ夏普(-%)':>12} {'Δ夏普(+%)':>12} "
            f"{'夏普(-)':>10} {'夏普(0)':>10} {'夏普(+)':>10}"
        )
        logger.info("-" * 100)

        for sr in results:
            logger.info(
                f"{sr.param_key:<30} {sr.base_value:>10.4f} {sr.sharpe_sensitivity:>12.4f} "
                f"{sr.sharpe_delta_low:>12.4f} {sr.sharpe_delta_high:>12.4f} "
                f"{sr.low_sharpe:>10.4f} {sr.base_sharpe:>10.4f} {sr.high_sharpe:>10.4f}"
            )

        high_sensitivity = [sr for sr in results if sr.sharpe_sensitivity > 0.5]
        if high_sensitivity:
            logger.warning("\n高敏感参数 (夏普敏感度 > 0.5):")
            for sr in high_sensitivity:
                logger.warning(f"  - {sr.param_key}: 敏感度={sr.sharpe_sensitivity:.4f}")

        logger.info("=" * 100)


def validate_hmm_perturbation_sensitivity(iv_series: pd.Series,
                                            strategy_func=None,
                                            n_perturb: int = 10,
                                            noise_pct: float = 0.02,
                                            cv_threshold: float = 0.3,
                                            random_seed: int = 42) -> Dict[str, Any]:
    """P1-裂缝12：HMM扰动敏感性测试

    对IV序列添加1-2%噪声，重算HMM状态和策略绩效。
    若夏普变异系数 > 0.3，需增加状态平滑或降低HMM依赖。
    """
    if len(iv_series) < 100:
        return {"cv": 0.0, "passed": True, "action": "insufficient_data"}

    try:
        from ali2026v3_trading.quant_core import AdaptiveHMM
    except ImportError:
        return {"cv": 0.0, "passed": True, "action": "hmm_unavailable"}

    iv_values = iv_series.dropna().values
    rng = np.random.RandomState(random_seed)

    def _get_hmm_states(iv_arr):
        hmm = AdaptiveHMM(n_states=3, update_interval=len(iv_arr) + 1)
        states = []
        for val in iv_arr:
            result = hmm.update(val)
            states.append(result.get('state', 1))
        return states

    # 基准状态
    base_states = _get_hmm_states(iv_values)

    # 扰动测试
    state_match_rates = []
    for _ in range(n_perturb):
        noise = rng.uniform(1.0 - noise_pct, 1.0 + noise_pct, len(iv_values))
        perturbed_iv = iv_values * noise
        perturbed_states = _get_hmm_states(perturbed_iv)
        match_rate = sum(1 for a, b in zip(base_states, perturbed_states) if a == b) / len(base_states)
        state_match_rates.append(match_rate)

    avg_match_rate = np.mean(state_match_rates)
    std_match_rate = np.std(state_match_rates)

    # P1-裂缝12修复v2：同时计算状态匹配率CV和策略绩效CV
    state_cv = float(std_match_rate / avg_match_rate) if avg_match_rate > 0 else 0.0

    # 如果提供了strategy_func，计算策略绩效（夏普）的变异系数
    sharpe_cv = None
    if strategy_func is not None:
        try:
            sharpes = []
            for noise_level in [noise_pct * (i + 1) / n_perturb for i in range(n_perturb)]:
                noisy_iv = iv_values * (1 + rng.uniform(-noise_level, noise_level, size=len(iv_values)))
                # 运行策略获取绩效
                perf = strategy_func(pd.Series(noisy_iv))
                sharpe_val = perf.get("sharpe", 0.0) if isinstance(perf, dict) else 0.0
                sharpes.append(sharpe_val)
            if len(sharpes) > 1 and np.mean(sharpes) != 0:
                sharpe_cv = float(np.std(sharpes) / abs(np.mean(sharpes)))
        except Exception as e:
            logger.debug("[P1-裂缝12v2] strategy_func执行失败: %s", e)

    # 使用夏普CV（如果有）或状态CV作为判断依据
    effective_cv = sharpe_cv if sharpe_cv is not None else state_cv
    passed = effective_cv <= cv_threshold

    return {
        "state_match_cv": round(state_cv, 4),
        "sharpe_cv": round(sharpe_cv, 4) if sharpe_cv is not None else None,
        "effective_cv": round(effective_cv, 4),
        "threshold": cv_threshold,
        "passed": passed,
        "action": "increase_state_smoothing" if not passed else "proceed",
        "n_perturbations": n_perturb,
        "avg_state_match_rate": round(float(avg_match_rate), 4),
        "noise_pct": noise_pct,
    }


_sensitivity_analyzer_lock = threading.Lock()


def get_sensitivity_analyzer(
    db_path: str = ":memory:",
    base_params: Optional[Dict[str, float]] = None,
    train_period: Tuple[str, str] = ("2024-01-01", "2025-06-30"),
    test_period: Tuple[str, str] = ("2025-07-01", "2026-04-30"),
) -> SensitivityAnalyzer:
    from ali2026v3_trading.singleton_registry import SingletonRegistry
    with _sensitivity_analyzer_lock:
        _registry = SingletonRegistry.get_registry('sensitivity_analyzer')
        _inst = _registry.get('instance')
        if _inst is None:
            import threading as _thr_sa
            _inst = SensitivityAnalyzer(
                db_path=db_path,
                base_params=base_params or {},
                train_period=train_period,
                test_period=test_period,
            )
            _registry.set('instance', _inst)
            logger.info("[P2-R8-08] SensitivityAnalyzer单例已创建 db_path=%s", db_path)
        return _inst


def reset_sensitivity_analyzer() -> None:
    from ali2026v3_trading.singleton_registry import SingletonRegistry
    with _sensitivity_analyzer_lock:
        _registry = SingletonRegistry.get_registry('sensitivity_analyzer')
        _registry.remove('instance')
        logger.info("[P2-R8-08] SensitivityAnalyzer单例已重置")
