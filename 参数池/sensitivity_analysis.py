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
import time
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd

import task_scheduler as _ts

_run_backtest = _ts.run_backtest
_load_data_for_period = _ts._load_data_for_period

logger = logging.getLogger(__name__)


def _load_attribute_matrix() -> Dict[str, Any]:
    yaml_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'parameter_attribute_matrix.yaml',
    )
    try:
        import yaml
        with open(yaml_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning("Failed to load attribute matrix: %s", e)
        return {}


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

    def compute_sensitivity(self) -> None:
        self.sharpe_delta_low = self.low_sharpe - self.base_sharpe
        self.sharpe_delta_high = self.high_sharpe - self.base_sharpe
        self.sharpe_sensitivity = abs(self.sharpe_delta_high - self.sharpe_delta_low) / 2.0

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
        logger.info("Base: sharpe=%.4f, return=%.4f, max_dd=%.4f", base_sharpe, base_return, base_max_dd)

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
        con = duckdb.connect(db_path)
        try:
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
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
                    f"INSERT INTO {table_name} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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

    @staticmethod
    def print_report(results: List[SensitivityResult]) -> None:
        if not results:
            print("No sensitivity results to display.")
            return

        print("\n" + "=" * 100)
        print("参数敏感性分析报告 (OAT: One-At-a-Time ±{:.1%} 扰动)".format(results[0].perturb_pct))
        print("=" * 100)
        print(
            f"{'参数':<30} {'基准值':>10} {'夏普敏感度':>12} "
            f"{'Δ夏普(-%)':>12} {'Δ夏普(+%)':>12} "
            f"{'夏普(-)':>10} {'夏普(0)':>10} {'夏普(+)':>10}"
        )
        print("-" * 100)

        for sr in results:
            print(
                f"{sr.param_key:<30} {sr.base_value:>10.4f} {sr.sharpe_sensitivity:>12.4f} "
                f"{sr.sharpe_delta_low:>12.4f} {sr.sharpe_delta_high:>12.4f} "
                f"{sr.low_sharpe:>10.4f} {sr.base_sharpe:>10.4f} {sr.high_sharpe:>10.4f}"
            )

        high_sensitivity = [sr for sr in results if sr.sharpe_sensitivity > 0.5]
        if high_sensitivity:
            print("\n⚠️  高敏感参数 (夏普敏感度 > 0.5):")
            for sr in high_sensitivity:
                print(f"  - {sr.param_key}: 敏感度={sr.sharpe_sensitivity:.4f}")

        print("=" * 100)
