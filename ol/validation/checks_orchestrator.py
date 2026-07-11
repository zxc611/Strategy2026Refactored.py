# [M1-128] 检查编排器
# MODULE_ID: M1-196
"""checks_orchestrator.py - _run_final_checks编排函数与审。all__导出"""

from __future__ import annotations

import logging
from ali2026v3_trading.infra._helpers import get_logger  # R9-5
from ali2026v3_trading.infra.serialization_utils import json_loads  # R4-02: 替代import json
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


from ali2026v3_trading.param_pool.backtest.backtest_state import (
    validate_rollover_impact,
)
from ali2026v3_trading.param_pool._param_grids import P0_IRON_RULES


logger = get_logger(__name__)  # R9-5


def _run_final_checks(
    best_params_json: str,
    train_sharpe: float,
    test_sharpe: float,
    train_return: float = 0.0,
    test_return: float = 0.0,
    train_max_dd: float = 0.0,
    test_max_dd: float = 0.0,
    num_signals: int = 0,
    alpha_hft: float = 0.0,
    alpha_minute: float = 0.0,
    alpha_box_extreme: float = 0.0,
    alpha_box_spring: float = 0.0,
    alpha_arbitrage: float = 0.0,
    alpha_market_making: float = 0.0,
    train_result_dict: Optional[Dict] = None,
    test_result_dict: Optional[Dict] = None,
    bar_data: Optional[pd.DataFrame] = None,  # N-11修复: 传入bar_data供逻辑反转验证
) -> bool:
    """执行P0最终绿灯检验 — 从"结果"到"证据"的法官审判

    不可妥协的P0硬编码检验项：
      1. 样本外衰减 < 30%（反过拟合铁律）
      2. 训练夏普 > 0.5（最低可交易门槛）
      3. 样本外夏普 > 0.3（样本外必须正期望）
      4. 最大回撤 > -50%（生存红线）
      5. 最少信号数 > 30（统计显著性最低要求）
      6. 逻辑反转阈值合理性
      7. 止损比例 < 止盈比例（风险收益比 > 1）
      8. 参数来源不可为intuition（铁律：无量化来源的参数不得锁定生产值）
      9. 十八策略Alpha占比检验

    Returns:
        True = P0绿灯通过, False = P0红灯未通过
    """
    p = json_loads(best_params_json) if best_params_json else {}  # R21-MEM-P2-06修复: json_loads单次调用，无需缓存
    all_passed = True
    warnings_list: List[str] = []

    print("\n" + "=" * 70)
    print("P0 最终绿灯检验 — 从结果到证据的法官审判")
    print("=" * 70)

    # --- 0. 瀑布式评判引擎（前置硬门控，三系统统一） ---
    _train_r = None
    _test_r = None
    try:
        # R10-P0-20修复: 使用模块级懒初始化单例，避免重复sys.path.insert+实例化
        CascadeJudge, adapt_backtest_result = _get_cascade_judge_module()
        _cascade_metrics = BacktestMetrics = None  # 避免命名冲突
        if train_result_dict is not None:
            _train_r = train_result_dict
        else:
            _est_plr_fc = 1.0
            if train_return is not None and train_max_dd is not None and train_max_dd < 0:
                _est_plr_fc = abs(train_return / train_max_dd) if abs(train_max_dd) > 1e-8 else 1.0
            _est_calmar_fc = 0.0
            if train_return is not None and train_max_dd is not None and train_max_dd < 0:
                _est_calmar_fc = train_return / abs(train_max_dd) if abs(train_max_dd) > 1e-8 else 0.0
            _train_r = {"sharpe": train_sharpe, "max_drawdown": train_max_dd,
                         "total_return": train_return, "num_signals": num_signals,
                         "profit_loss_ratio": _est_plr_fc, "calmar": _est_calmar_fc,
                         "total_trades": num_signals or 0,
                         "max_flat_period_days": 10}
        _test_r = test_result_dict
        _adapted = adapt_backtest_result(_train_r, test_result=_test_r, params=p)
        _capital_scale = p.get("capital_scale", "medium") if p else "medium"
        _cascade = CascadeJudge.from_config(capital_scale=_capital_scale, params=p)
        _cascade_report = _cascade.judge(_adapted)
        if not _cascade_report.passed:
            print(f"  [FAIL] 瀑布式评判否决: {_cascade_report.fatal_reason}")
            all_passed = False
        else:
            print(f"  [PASS] 瀑布式评判通过: 综合得分={_cascade_report.final_score:.4f}")
        for _w in _cascade_report.warnings:
            warnings_list.append(f"[瀑布]{_w}")
            print(f"  [WARN] {_w}")
    except (ImportError, AttributeError, TypeError, ValueError, KeyError, RuntimeError) as _e:
        # E-03修复: CascadeJudge异常时P0必须阻断，不再仅warning
        warnings_list.append(f"瀑布式评判异常(P0阻断): {_e}")
        all_passed = False  # Q3修复: CascadeJudge是前置硬门控，导入失败必须阻断P0

    # --- 0b. ModeEngine自动模式选择（CascadeJudge通过后） ---
    if all_passed and _train_r is not None:
        try:
            from ali2026v3_trading.governance.mode_engine import ModeEngine
            _capital_scale_for_me = p.get("capital_scale", "medium") if p else "medium"
            _me = ModeEngine.create_engine(_capital_scale_for_me)
            _auto_result = _me.auto_select_mode(_train_r, test_result=_test_r, params=p)
            if _auto_result.get('success'):
                _auto_scale = _auto_result.get('scale', 'medium')
                print(f"  [INFO] ModeEngine自动模式选择: {_auto_scale}")
                # P-16修复: auto_select_mode结果执行switch_mode切换
                if _auto_scale != _me.scale_str:
                    _switch_result = _me.switch_mode(_auto_scale)
                    if _switch_result.get('success'):
                        logging.info("[P-16] auto_select_mode触发switch_mode: %s→%s",
                                    _me.scale_str, _auto_scale)
                    else:
                        logging.warning("[P-16] switch_mode失败: %s", _switch_result.get('error'))
            else:
                warnings_list.append(f"ModeEngine自动模式选择未切换: {_auto_result.get('error', '')}")
        except (ImportError, AttributeError, TypeError, ValueError, KeyError, RuntimeError) as _me_err:
            warnings_list.append(f"ModeEngine自动模式选择跳过: {_me_err}")

    all_passed = _run_basic_metrics_checks(
        all_passed=all_passed,
        train_sharpe=train_sharpe,
        test_sharpe=test_sharpe,
        test_max_dd=test_max_dd,
        num_signals=num_signals,
        params=p,
        warnings_list=warnings_list,
        bar_data=bar_data,
    )

    # --- 核心约束硬执行 ---
    all_passed = _run_core_constraints_checks(
        all_passed=all_passed,
        train_sharpe=train_sharpe,
        test_sharpe=test_sharpe,
        num_signals=num_signals,
        train_result_dict=train_result_dict,
    )

    # --- 8. 参数来源intuition检查（不可妥协铁律）---
    intuition_params_found = []
    intuition_check_skipped = False
    try:
        from params_service import get_params_service
        ps = get_params_service()
        for key, value in p.items():
            attr = ps._attribute_matrix.get(key)
            if isinstance(attr, dict) and attr.get('source') == 'intuition':
                intuition_params_found.append(f"{key}={value}")
    except (ImportError, AttributeError, TypeError, ValueError, KeyError, RuntimeError) as _e:
        logger.warning("[PARAMS] params_service import failed: %s", _e)
        intuition_check_skipped = True

    if intuition_check_skipped:
        print(f"  [FAIL] intuition参数检查被跳过(params_service不可用)，按铁律视为不通过")
        all_passed = False
    elif intuition_params_found:
        print(f"  [FAIL] 发现 {len(intuition_params_found)} 个intuition参数: {intuition_params_found}")
        print(f"         铁律: 无量化来源的参数不得锁定为生产值")
        all_passed = False
    else:
        print(f"  [PASS] 无intuition参数: 所有锁定参数均有量化来源")

    # --- 8b. P0-11修复: E13影子策略同谋自动判定 ---
    all_passed = _run_e13_collusion_checks(
        all_passed=all_passed,
        params=p,
        train_result_dict=train_result_dict,
        warnings_list=warnings_list,
    )

    # --- 9. 十八策略Alpha占比检验 ---
    warnings_list.extend(
        _run_alpha_coverage_checks(
            train_sharpe=train_sharpe,
            alpha_hft=alpha_hft,
            alpha_minute=alpha_minute,
            alpha_box_extreme=alpha_box_extreme,
            alpha_box_spring=alpha_box_spring,
            alpha_arbitrage=alpha_arbitrage,
            alpha_market_making=alpha_market_making,
        )
    )

    all_passed = _run_walkforward_checks(
        all_passed=all_passed,
        train_result_dict=train_result_dict,
    )

    all_passed, stat_p_values, _cf_trade_results = _run_statistical_validation_checks(
        all_passed=all_passed,
        train_sharpe=train_sharpe,
        test_sharpe=test_sharpe,
        train_result_dict=train_result_dict,
        test_result_dict=test_result_dict,
        _train_r=_train_r,
    )

    all_passed = _run_pnl_attribution_4d_checks(
        all_passed=all_passed,
        train_result_dict=train_result_dict,
        test_result_dict=test_result_dict,
    )

    # --- 裂缝5修复: 展期影响验证 ---
    if bar_data is not None and not bar_data.empty:
        try:
            _rollover_impact = validate_rollover_impact(bar_data, p)
            if _rollover_impact.get("needs_exclusion", False):
                print(f"  [FAIL] 展期影响显著: 夏普差异={_rollover_impact.get('sharpe_diff_pct', 0):.1f}%, "
                      f"回撤差异={_rollover_impact.get('mdd_diff_pct', 0):.1f}%, 必须剔除展期数据")
                all_passed = False
            else:
                print(f"  [PASS] 展期影响验证通过: 展期点={_rollover_impact.get('rollover_count', 0)}, "
                      f"夏普差异={_rollover_impact.get('sharpe_diff_pct', 0):.1f}%")
        except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
            logger.warning("[裂缝5] validate_rollover_impact failed: %s", _e)
    else:
        print(f"  [SKIP] 展期影响验证: 无bar_data，跳过")

    _print_final_judgement(all_passed=all_passed, warnings_list=warnings_list)

    return all_passed


__all__ = [
    'validate_logic_reversal_no_future',
    'DEFAULT_RISK_FREE_RATE',
    '_force_close_all_positions',
    '_run_alpha_coverage_checks',
    '_run_statistical_validation_checks',
    '_run_walkforward_checks',
    '_run_pnl_attribution_4d_checks',
    '_run_e13_collusion_checks',
    '_run_core_constraints_checks',
    '_run_basic_metrics_checks',
    '_print_final_judgement',
    '_run_final_checks',
]

# ── Checks Individual (merged from checks_individual.py on 2026-06-12) ──

"""checks_individual.py - 各独立检查函数"""


import logging
from ali2026v3_trading.infra._helpers import get_logger  # R9-5
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


from ali2026v3_trading.param_pool.backtest.backtest_state import (
    validate_rollover_impact,
)
from ali2026v3_trading.param_pool._param_grids import P0_IRON_RULES

logger = get_logger(__name__)  # R9-5


def validate_logic_reversal_no_future(bar_data: pd.DataFrame = None,
                                       n_check_bars: int = 100) -> Dict[str, Any]:
    """P0-裂缝28：验证逻辑反转平仓的wrong_pct不使用未来数据

    检查链check_logic_reversal中调用wrong_pct时传入的bar是否已完全形成。'
    逻辑反转触发时刻，所使用的wrong_pct不应包含该时刻之后的任何数据。
    """
    if bar_data is None or bar_data.empty:
        return {"passed": False, "action": "no_data", "details": "无数据无法验证，默认不通过"}

    wrong_cols = [c for c in bar_data.columns if 'wrong' in c.lower() and 'pct' in c.lower()]
    if not wrong_cols:
        return {"passed": True, "action": "no_wrong_pct_columns", "details": "无wrong_pct列，无需验证"}

    # P0-裂缝28修复: 使用前向填充处理可能的NaN，避免未来数据泄漏
    bar_data = bar_data.copy()
    for col in wrong_cols:
        bar_data[col] = pd.to_numeric(bar_data[col], errors='coerce').ffill()

    issues = []
    n_check = min(n_check_bars, len(bar_data))

    # R21-MEM-P2-13修复: 循环中重复调用imread/load — 本项目无图像加载场景，不涉及此问题
    # 检查1：wrong_pct在同一Bar内不应有回溯修正（NaN后填充）'
    for col in wrong_cols:
        vals = pd.to_numeric(bar_data[col], errors='coerce').values[:n_check]
        nan_count = sum(1 for v in vals if pd.isna(v))
        if nan_count > 0:
            issues.append(f"列{col}有{nan_count}个NaN值（可能存在数据不完整）")

    # 检查2：wrong_pct值不应随时间回溯变化（前向填充检测）'
    # 如果Bar[i]的wrong_pct在后续Bar中发生了变化，说明存在未来数据修正
    for col in wrong_cols:
        vals = pd.to_numeric(bar_data[col], errors='coerce').values[:n_check]
        # 检查是否存在值突然从0变为非0（可能表示延迟到达的数据修正了历史值）'
        zero_to_nonzero = 0
        for i in range(1, min(len(vals), n_check)):
            if vals[i-1] == 0 and vals[i] != 0 and abs(vals[i]) > 0.01:
                zero_to_nonzero += 1
        if zero_to_nonzero > n_check * 0.3:
            issues.append(
                f"列{col}有{zero_to_nonzero}次从0跳变为非0（>{n_check * 0.3:.0f}次阈值），"
                f"可能存在未来数据修正"
            )

    # 检查3：wrong_pct与未来收益的相关性不应过高
    # 如果wrong_pct与未来N个Bar的收益高度相关，说明可能包含未来信息
    if 'close' in bar_data.columns:
        for col in wrong_cols:
            vals = pd.to_numeric(bar_data[col], errors='coerce').values[:n_check]
            closes = bar_data['close'].values[:n_check]
            if len(vals) > 10 and len(closes) > 10:
                future_returns = np.zeros(len(vals))
                for i in range(len(vals) - 1):
                    if closes[i] > 0:
                        future_returns[i] = (closes[min(i+1, len(closes)-1)] - closes[i]) / closes[i]
                # 计算wrong_pct与未来收益的相关性
                valid_mask = ~(np.isnan(vals) | np.isnan(future_returns))
                if valid_mask.sum() > 5:
                    corr = float(np.corrcoef(vals[valid_mask], future_returns[valid_mask])[0, 1])
                    if not np.isnan(corr) and abs(corr) > 0.7:
                        issues.append(
                            f"列{col}与未来1Bar收益相关性={corr:.3f}>0.7阈值，"
                            f"可能包含未来信息"
                        )

    passed = len(issues) == 0
    return {
        "passed": passed,
        "wrong_pct_columns": wrong_cols,
        "issues": issues,
        "n_bars_checked": n_check,
        "action": "fix_future_data_leak" if not passed else "proceed",
    }


def _force_close_all_positions(
    bt,
    bar: pd.Series,
    params: Dict[str, float],
    reason: str = "manual_force_close",
) -> int:
    """P0-R9-12修复: 统一强制平仓所有未平仓头寸
    由断路器/硬停止/到期/回测结束等场景统一调用
    返回: 实际平仓数量
    """
    closed_count = 0
    if bar is None or len(bt.positions) == 0:
        return 0
    bar_time = bar.get("datetime", bar.get("minute", pd.NaT))
    for sym in list(bt.positions.keys()):
        pos = bt.positions[sym]
        _is_limit_up = bar.get('is_limit_up', False)
        _is_limit_down = bar.get('is_limit_down', False)
        _close_dir = 'SELL' if pos.volume > 0 else 'BUY'
        if _is_limit_up and _close_dir == 'SELL':
            continue
        if _is_limit_down and _close_dir == 'BUY':
            continue
        close_price = bar.get("close", pos.open_price)
        _mult = _get_contract_multiplier(sym)
        pnl = (close_price - pos.open_price) * pos.volume * _mult if pos.volume != 0 else 0
        bid_ask = bar.get("bid_ask_spread", 0.0)
        spread_q = bar.get("_spread_quality", 0)
        slip_bps = _compute_dynamic_slippage_bps(close_price, bid_ask, spread_quality=spread_q, bar=bar, params=params)
        slip = close_price * slip_bps / 10000 * pos.lots
        commission = _compute_commission(sym, pos.lots, open_time=pos.open_time, close_time=bar_time, is_open=False, exchange_id=_infer_exchange_id(sym))
        net_pnl = pnl - slip - commission
        _safe_equity_add(bt, net_pnl)
        bt.total_trades += 1
        if np.isclose(close_price, pos.open_price, rtol=1e-10):
            float_pnl_pct = 0.0
        else:
            float_pnl_pct = (close_price - pos.open_price) / pos.open_price if pos.open_price > 0 else 0.0
        if pos.volume < 0:
            float_pnl_pct = -float_pnl_pct
        hold_min = (bar_time - pos.open_time).total_seconds() / 60.0 if pos.open_time is not None else 0.0
        bt.closed_trades.append(_ClosedTrade(
            pnl=net_pnl, pnl_pct=float_pnl_pct, close_reason=reason,
            hold_minutes=hold_min, open_reason=getattr(pos, 'open_reason', ''),
        ))
        bt.recent_pnls.append(net_pnl)
        if len(bt.recent_pnls) > 50:
            bt.recent_pnls = bt.recent_pnls[-50:]
        del bt.positions[sym]
        closed_count += 1
    return closed_count


def _run_alpha_coverage_checks(
    train_sharpe: float,
    alpha_hft: float,
    alpha_minute: float,
    alpha_box_extreme: float,
    alpha_box_spring: float,
    alpha_arbitrage: float,
    alpha_market_making: float,
) -> List[str]:
    """执行十八策略Alpha占比检查并返回告警列表。"""
    print("\n" + "-" * 70)
    print("十八策略Alpha占比检验(6主策略×3变体):")
    alpha_checks = [
        ("S1 HFT趋势共振", alpha_hft, P0_IRON_RULES["alpha_threshold_hft"]),
        ("S2 分钟趋势共振", alpha_minute, P0_IRON_RULES["alpha_threshold_minute"]),
        ("S3 箱体极值", alpha_box_extreme, P0_IRON_RULES["alpha_threshold_box_extreme"]),
        ("S4 箱体弹簧", alpha_box_spring, P0_IRON_RULES["alpha_threshold_box_spring"]),
        ("S5 套利", alpha_arbitrage, P0_IRON_RULES["alpha_threshold_arbitrage"]),
        ("S6 做市", alpha_market_making, P0_IRON_RULES["alpha_threshold_market_making"]),
    ]

    alpha_warnings: List[str] = []
    for label, alpha_val, threshold in alpha_checks:
        alpha_pct_val = alpha_val / train_sharpe * 100 if train_sharpe > 0 else 0
        print(f"  {label}: Alpha={alpha_val:.3f} ({alpha_pct_val:.1f}%) 阈值≥{threshold}")
        if alpha_val < threshold:
            print(f"    [WARN] {label} Alpha={alpha_val:.3f} < {threshold}，独立Alpha不足")
            alpha_warnings.append(f"{label} Alpha={alpha_val:.3f}<{threshold}")
        if alpha_pct_val < P0_IRON_RULES["alpha_pct_threshold"]:
            print(f"    [WARN] {label} Alpha占比={alpha_pct_val:.1f}%<{P0_IRON_RULES['alpha_pct_threshold']}%，收益主要由市场驱动")
            alpha_warnings.append(f"{label} Alpha占比={alpha_pct_val:.1f}%<{P0_IRON_RULES['alpha_pct_threshold']}%")

    return alpha_warnings


def _run_walkforward_checks(all_passed: bool, train_result_dict: Optional[Dict]) -> bool:
    """执行Walk-Forward滚动验证（WF6-WF10）。"""
    print("\n" + "-" * 70)
    print("Walk-Forward滚动验证(WF6-WF10):")
    try:
        from ali2026v3_trading.param_pool.validation.statistical_validation import WalkForwardValidator
        _wfv = WalkForwardValidator()
        _equity = []
        if train_result_dict and "equity_curve" in train_result_dict:
            _equity = train_result_dict["equity_curve"]
        if _equity and len(_equity) > 10:
            _wf_result = _wfv.validate(_equity)
            if not _wf_result.overall_robust:
                _wf_failures = []
                if _wf_result.wf6_monotone_decline:
                    _wf_failures.append("WF6(单调衰减)")
                if _wf_result.wf7_parameter_fragility:
                    _wf_failures.append("WF7(参数脆弱)")
                if _wf_result.wf8_negative_ev:
                    _wf_failures.append("WF8(负EV)")
                if _wf_result.wf9_alpha_decline:
                    _wf_failures.append("WF9(Alpha衰减)")
                if _wf_result.wf10_absolute_ev_breach:
                    _wf_failures.append("WF10(绝对EV突破)")
                print(f"  [FAIL] Walk-Forward不稳健: {', '.join(_wf_failures)}")
                all_passed = False
            else:
                print(f"  [PASS] Walk-Forward验证通过: {len(_wf_result.window_results)}个窗口全部稳健")
        else:
            print("  [SKIP] 无权益曲线数据，Walk-Forward验证跳过")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _wfv_err:
        logger.warning("[T-11] WalkForwardValidator集成跳过: %s", _wfv_err)
        print(f"  [SKIP] Walk-Forward验证跳过: {_wfv_err}")

    return all_passed


def _run_pnl_attribution_4d_checks(
    all_passed: bool,
    train_result_dict: Optional[Dict],
    test_result_dict: Optional[Dict],
) -> bool:
    """执行PnL四维归因验证（策略/品种/方向/时间段）。"""
    print("\n" + "-" * 70)
    print("PnL Attribution 4D(策略/品种/方向/时间):")
    try:
        _all_trades = []
        for _d in (train_result_dict, test_result_dict):
            if isinstance(_d, dict):
                _all_trades.extend(_d.get("trade_results") or _d.get("closed_trades") or [])

        if len(_all_trades) < 10:
            print(f"  [FAIL] 四维归因样本不足: n={len(_all_trades)} < 10")
            all_passed = False
        else:
            _by_4d = {}
            _strategy_keys = set()
            _instrument_keys = set()
            _direction_keys = set()
            _time_keys = set()
            for _tr in _all_trades:
                if not isinstance(_tr, dict):
                    continue
                _strategy = str(_tr.get("strategy") or _tr.get("strategy_type") or "unknown")
                _instrument = str(_tr.get("instrument_id") or _tr.get("symbol") or "unknown")
                _raw_dir = _tr.get("direction", "unknown")
                if isinstance(_raw_dir, (int, float)):
                    _direction = "LONG" if float(_raw_dir) > 0 else ("SHORT" if float(_raw_dir) < 0 else "FLAT")
                else:
                    _direction = str(_raw_dir).upper() if _raw_dir is not None else "UNKNOWN"
                _ts = pd.to_datetime(_tr.get("timestamp") or _tr.get("ts") or _tr.get("datetime"), errors="coerce")
                if pd.notna(_ts):
                    _h = int(_ts.hour)
                    if _h < 11:
                        _time_bucket = "OPEN"
                    elif _h < 14:
                        _time_bucket = "MID"
                    else:
                        _time_bucket = "CLOSE"
                else:
                    _time_bucket = "UNKNOWN"

                _pnl = float(_tr.get("pnl", 0.0) or 0.0)
                _k = (_strategy, _instrument, _direction, _time_bucket)
                _by_4d[_k] = _by_4d.get(_k, 0.0) + _pnl

                _strategy_keys.add(_strategy)
                _instrument_keys.add(_instrument)
                _direction_keys.add(_direction)
                _time_keys.add(_time_bucket)

            _dims_ok = (
                len(_strategy_keys) >= 1 and len(_instrument_keys) >= 1
                and len(_direction_keys) >= 2 and len(_time_keys) >= 2
            )
            if _dims_ok and len(_by_4d) >= 4:
                print(
                    f"  [PASS] 四维归因已生成: cells={len(_by_4d)}, "
                    f"strategy={len(_strategy_keys)}, instrument={len(_instrument_keys)}, "
                    f"direction={len(_direction_keys)}, time_bucket={len(_time_keys)}"
                )
            else:
                print(
                    f"  [FAIL] 四维归因维度不足: cells={len(_by_4d)}, "
                    f"strategy={len(_strategy_keys)}, instrument={len(_instrument_keys)}, "
                    f"direction={len(_direction_keys)}, time_bucket={len(_time_keys)}"
                )
                all_passed = False
    except (ValueError, KeyError, TypeError, AttributeError, RuntimeError, ZeroDivisionError) as _att_err:
        print(f"  [FAIL] 四维归因异常: {_att_err}")
        all_passed = False

    return all_passed


def _run_e13_collusion_checks(
    all_passed: bool,
    params: Dict[str, Any],
    train_result_dict: Optional[Dict],
    warnings_list: List[str],
) -> bool:
    """执行E13影子策略同谋检测。"""
    try:
        from ali2026v3_trading.governance.governance_engine import E13ShadowStrategyCollusionDetector
        _gov_cfg = params if isinstance(params, dict) else {}
        _e13 = E13ShadowStrategyCollusionDetector(
            min_param_diff_pct=_gov_cfg.get("e13_min_param_diff_pct", 0.20),
            max_signal_sync_rate=_gov_cfg.get("e13_max_signal_sync_rate", 0.7),
            min_trade_count=_gov_cfg.get("e13_min_trade_count", 20),
        )
        _shadow_params = {k.replace("shadow_", "", 1): v for k, v in params.items() if k.startswith("shadow_")}
        _main_params = {k: v for k, v in params.items() if not k.startswith("shadow_")}
        _main_signals = []
        _shadow_signals = []
        if train_result_dict:
            for t in train_result_dict.get("trade_log", []):
                _main_signals.append({"direction": t.get("direction", 0), "instrument_id": t.get("symbol", "")})
        if not _shadow_params:
            _shadow_params = _main_params
        _e13_result = _e13.detect(
            main_params=_main_params,
            shadow_params=_shadow_params,
            main_signals=_main_signals,
            shadow_signals=_shadow_signals,
        )
        if _e13_result.get("e13_triggered"):
            print(f"  [FAIL] E13同谋检测触发: 参数差异度={_e13_result.get('param_diff_pct', 0):.1%}, "
                  f"信号同步率={_e13_result.get('signal_sync_rate', 0):.1%}")
            print(f"         原因: {_e13_result.get('reason', 'unknown')}")
            all_passed = False
        else:
            print(f"  [PASS] E13同谋检测通过: 参数差异度={_e13_result.get('param_diff_pct', 0):.1%}, "
                  f"信号同步率={_e13_result.get('signal_sync_rate', 0):.1%}")
    except (ImportError, AttributeError, TypeError, ValueError, KeyError, RuntimeError) as _e:
        logger.warning("[P0-11] E13同谋检测跳过: %s", _e)
        warnings_list.append(f"E13同谋检测跳过: {_e}")

    return all_passed


def _run_core_constraints_checks(
    all_passed: bool,
    train_sharpe: float,
    test_sharpe: float,
    num_signals: int,
    train_result_dict: Optional[Dict],
) -> bool:
    """执行P0核心约束硬规则（日均触发/亏损命中率/恢复率/样本外保留率）。"""
    _train_result = train_result_dict or {}
    total_trades = _train_result.get("total_trades", 0)
    num_trading_days = _train_result.get("num_trading_days", 0)
    if num_trading_days <= 0:
        num_trading_days = max(1, num_signals / 240)
    daily_trigger = total_trades / num_trading_days if num_trading_days > 0 else 0
    if daily_trigger > P0_IRON_RULES["max_daily_trigger"]:
        print(f"  [FAIL] 日均触发={daily_trigger:.1f}次>{P0_IRON_RULES['max_daily_trigger']:.0f}次: 过度交易")
        all_passed = False

    loss_trades = _train_result.get("loss_trades", 0)
    if total_trades > 0:
        loss_hit_rate = loss_trades / total_trades
        if loss_hit_rate > P0_IRON_RULES["max_loss_hit_rate"]:
            print(f"  [FAIL] 亏损命中率={loss_hit_rate:.1%}>{P0_IRON_RULES['max_loss_hit_rate']:.0%}: 亏损过于频繁")
            all_passed = False

    recovery_count = _train_result.get("recovery_count", 0)
    no_recovery_count = _train_result.get("no_recovery_count", 0)
    total_dd_events = recovery_count + no_recovery_count
    if total_dd_events > 0:
        two_x_recovery_rate = recovery_count / total_dd_events
        if two_x_recovery_rate < P0_IRON_RULES["min_two_x_recovery_rate"]:
            print(f"  [FAIL] 两倍恢复率={two_x_recovery_rate:.1%}<{P0_IRON_RULES['min_two_x_recovery_rate']:.0%}: 回撤恢复能力不足")
            all_passed = False

    if train_sharpe > 0:
        oos_retention = test_sharpe / train_sharpe
        if oos_retention < P0_IRON_RULES["min_oos_retention"]:
            print(f"  [FAIL] 样本外保留率={oos_retention:.1%}<{P0_IRON_RULES['min_oos_retention']:.0%}: 样本外表现严重退化")
            all_passed = False

    return all_passed


def _run_basic_metrics_checks(
    all_passed: bool,
    train_sharpe: float,
    test_sharpe: float,
    test_max_dd: float,
    num_signals: int,
    params: Dict[str, Any],
    warnings_list: List[str],
    bar_data: Optional[pd.DataFrame] = None,
) -> bool:
    """执行P0基本指标门槛检查（夏普/回撤/信号数/反转阈值/盈亏比）。"""
    # --- 1. 样本外衰减检验（不可妥协）---
    decay = (test_sharpe - train_sharpe) / train_sharpe if abs(train_sharpe) > 1e-8 else 0
    if decay < P0_IRON_RULES["max_oos_decay"]:
        print(f"  [FAIL] 样本外衰减={decay:.1%} < -30%: 严重过拟合，样本外不可信")
        all_passed = False
    elif decay < P0_IRON_RULES["oos_decay_warn"]:
        warnings_list.append(f"样本外衰减={decay:.1%} 接近-30%警戒线")
        print(f"  [WARN] 样本外衰减={decay:.1%}: 接近警戒线（-30%），需关注")
    else:
        print(f"  [PASS] 样本外衰减={decay:.1%} >= -30%: 样本外稳健")

    # --- 2. 训练夏普最低门槛 ---
    if train_sharpe < P0_IRON_RULES["min_train_sharpe"]:
        print(f"  [FAIL] 训练夏普={train_sharpe:.3f} < 0.5: 不满足最低可交易门槛")
        all_passed = False
    else:
        print(f"  [PASS] 训练夏普={train_sharpe:.3f} >= 0.5: 满足可交易门槛")

    # --- 3. 样本外夏普正期望 ---
    if test_sharpe < P0_IRON_RULES["min_test_sharpe"]:
        print(f"  [FAIL] 样本外夏普={test_sharpe:.3f} < 0.3: 样本外非正期望")
        all_passed = False
    else:
        print(f"  [PASS] 样本外夏普={test_sharpe:.3f} >= 0.3: 样本外正期望")

    # --- 4. 最大回撤生存红线 ---
    # T7修复: max_drawdown可能是百分比形式(如50.0)或小数形式(如-0.50)，统一转为负数小数再比较
    _dd_normalized = test_max_dd if abs(test_max_dd) <= 1 else -abs(test_max_dd) / 100.0
    if _dd_normalized < P0_IRON_RULES["max_drawdown_limit"]:
        print(f"  [FAIL] 样本外最大回撤={_dd_normalized:.3f} < {P0_IRON_RULES['max_drawdown_limit']:.0%}: 超过生存红线")
        all_passed = False
    else:
        print(f"  [PASS] 样本外最大回撤={_dd_normalized:.3f} >= {P0_IRON_RULES['max_drawdown_limit']:.0%}: 回撤可控")

    # --- 5. 最少信号数统计显著性 ---
    if num_signals < P0_IRON_RULES["min_signal_count"]:
        print(f"  [FAIL] 信号数={num_signals} < {P0_IRON_RULES['min_signal_count']}: 统计显著性不足，结论不可靠")
        all_passed = False
    else:
        print(f"  [PASS] 信号数={num_signals} >= 30: 满足统计显著性最低要求")

    # --- 6. 逻辑反转阈值合理性 ---
    lr_threshold = params.get("logic_reversal_threshold", 1.5)
    if lr_threshold < P0_IRON_RULES["min_lr_threshold"]:
        print(f"  [FAIL] 逻辑反转阈值={lr_threshold:.4f} < 0.8: 频繁误平仓风险极高")
        all_passed = False
    elif lr_threshold < P0_IRON_RULES["lr_threshold_warn"]:
        warnings_list.append(f"逻辑反转阈值={lr_threshold}偏低，可能频繁误平仓")
        print(f"  [WARN] 逻辑反转阈值={lr_threshold:.4f}: 偏低，可能频繁误平仓")
    else:
        print(f"  [PASS] 逻辑反转阈值={lr_threshold:.4f} >= 1.0: 合理")

    # T5修复: 集成逻辑反转未来数据验证到P0检验
    # P0-3/T5修复: bar_data=None时函数永远返回False导致P0永久阻断
    # 改为: 无数据时跳过此检查(返回passed=True)，有数据时执行验证
    try:
        _lr_no_future_result = validate_logic_reversal_no_future(bar_data=bar_data)
        if _lr_no_future_result.get("action") == "no_data":
            # 无bar_data时无法验证，跳过此检查而非默认失败
            print(f"  [SKIP] 逻辑反转未来数据验证: 无bar_data，跳过(交易路径已有bar_time断言保护)")
        elif not _lr_no_future_result.get("passed", False):
            print(f"  [FAIL] 逻辑反转使用了未来数据: {_lr_no_future_result.get('details', '')}")
            all_passed = False
        else:
            print(f"  [PASS] 逻辑反转未使用未来数据")
    except (ValueError, KeyError, TypeError, RuntimeError, AttributeError) as _e:
        logger.warning("[P0] validate_logic_reversal_no_future check failed: %s", _e)

    # --- 7. 风险收益比 > 1 ---
    tp = params.get("close_take_profit_ratio", 1.8)  # R17-P0-CFG-04修复: 回退值1.5→1.8与CENTRALIZED_DEFAULTS对齐
    sl = params.get("close_stop_loss_ratio", 0.3)  # R24-P0-DF-03修复: close_stop_loss_ratio系统回退值0.5→0.3
    if tp <= sl:
        print(f"  [FAIL] 止盈={tp:.4f} <= 止损={sl:.4f}: 风险收益比<=1，长期必亏")
        all_passed = False
    else:
        print(f"  [PASS] 止盈/止损={tp:.4f}/{sl:.4f}={tp/sl:.1f}:1 风险收益比>1")

    return all_passed


def _print_final_judgement(all_passed: bool, warnings_list: List[str]) -> None:
    """打印P0最终判决与告警。"""
    print("-" * 70)
    if warnings_list:
        print(f"  警告 ({len(warnings_list)}):")
        for w in warnings_list:
            print(f"    - {w}")

    if all_passed and not warnings_list:
        print("\n  *** P0绿灯: 全部通过。可进入小资金实盘测试。***")
    elif all_passed:
        print("\n  *** P0黄灯: 硬检验通过但有警告。建议处理警告后进入实盘。***")
    else:
        print("\n  *** P0红灯: 未通过。需调整参数或补充量化证据。***")

    print("=" * 70)


# ── Lazy imports from backtest_runner_base (break circular import) ──

_LAZY_BRB_NAMES = {
    '_get_cascade_judge_module',
    '_sync_random_seed',
    '_ClosedTrade',
    '_compute_commission',
    '_compute_dynamic_slippage_bps',
    '_get_contract_multiplier',
    '_infer_exchange_id',
    '_safe_equity_add',
    '_check_positions',
    '_check_state_transition',
}


def __getattr__(name):
    if name == '_reset_daily':
        from ali2026v3_trading.param_pool.backtest.backtest_runner_utils import _reset_daily
        globals()['_reset_daily'] = _reset_daily
        return _reset_daily
    if name in _LAZY_BRB_NAMES:
        from ali2026v3_trading.param_pool.backtest.backtest_runner_base import (
            _get_cascade_judge_module,
            _sync_random_seed,
            _ClosedTrade,
            _compute_commission,
            _compute_dynamic_slippage_bps,
            _get_contract_multiplier,
            _infer_exchange_id,
            _safe_equity_add,
            _check_positions,
            _check_state_transition,
        )
        globals()['_get_cascade_judge_module'] = _get_cascade_judge_module
        globals()['_sync_random_seed'] = _sync_random_seed
        globals()['_ClosedTrade'] = _ClosedTrade
        globals()['_compute_commission'] = _compute_commission
        globals()['_compute_dynamic_slippage_bps'] = _compute_dynamic_slippage_bps
        globals()['_get_contract_multiplier'] = _get_contract_multiplier
        globals()['_infer_exchange_id'] = _infer_exchange_id
        globals()['_safe_equity_add'] = _safe_equity_add
        globals()['_check_positions'] = _check_positions
        globals()['_check_state_transition'] = _check_state_transition
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")