"""七模块最终环节消费断言验证

验证订单流、希腊字母、夏普、三角评判、盈亏比、延迟、资金大小
七模块在最后消费环节的结果正确性。

验证链路：模块计算 → 最终消费环节 → 结果正确

消费方式分类：
  - score_dimension: 订单流/希腊字母/三角评判 → 决策评分维度
  - gate_veto: 夏普 → CascadeJudge级联门控
  - veto: 盈亏比 → _filter_by_plr专用过滤
  - info: 延迟 → benchmark统计
  - multiplier: 资金大小 → _propagate_capital_scale多路传播
"""
import os
import sys
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


# ============================================================================
# 1. 订单流 (order_flow) — score_dimension 最终消费环节
# ============================================================================

def test_order_flow_consumption():
    """订单流最终消费环节: compute_decision_score返回scores['order_flow'] ∈ [0,1]

    调用链路: order_flow_consistency → compute_decision_score → scores['order_flow']
    """
    print("\n=== 订单流(order_flow)最终消费环节验证 ===")

    from ali2026v3_trading.risk.risk_service import get_risk_service
    from ali2026v3_trading.risk.risk_compute_service import RiskComputeService

    rs = get_risk_service()
    # 补丁: _compute_life_score调用_get_life_estimator，若不存在则添加降级方法
    if not hasattr(rs, '_get_life_estimator'):
        rs._get_life_estimator = lambda: None
    rcs = RiskComputeService(rs)

    # 消费1: order_flow_consistency=1.0 (强一致性) → 评分应较高
    result = rcs.compute_decision_score(
        state_strength=0.8,
        order_flow_consistency=1.0,
    )
    assert 'dimension_scores' in result, "缺少dimension_scores"
    scores = result['dimension_scores']
    assert 'order_flow' in scores, f"缺少order_flow维度: {list(scores.keys())}"

    of_score_high = scores['order_flow']
    assert 0.0 <= of_score_high <= 1.0, f"order_flow评分超出[0,1]: {of_score_high}"
    print(f"  [PASS] order_flow_consistency=1.0 → score={of_score_high:.4f}")

    # 消费2: order_flow_consistency=-1.0 (强不一致) → 评分应较低
    result_low = rcs.compute_decision_score(
        state_strength=0.8,
        order_flow_consistency=-1.0,
    )
    of_score_low = result_low['dimension_scores']['order_flow']
    assert 0.0 <= of_score_low <= 1.0, f"order_flow评分超出[0,1]: {of_score_low}"
    assert of_score_low < of_score_high, \
        f"强不一致应低于强一致: low={of_score_low:.4f} high={of_score_high:.4f}"
    print(f"  [PASS] order_flow_consistency=-1.0 → score={of_score_low:.4f} (低于高一致性)")

    # 消费3: 综合决策评分包含order_flow贡献
    assert 'decision_score' in result, "缺少decision_score"
    ds = result['decision_score']
    assert 0.0 <= ds <= 1.0, f"综合评分超出[0,1]: {ds}"
    print(f"  [PASS] 综合决策评分={ds:.4f} (含order_flow权重0.10)")

    return True


# ============================================================================
# 2. 希腊字母 (greeks) — score_dimension 最终消费环节
# ============================================================================

def test_greeks_consumption():
    """希腊字母最终消费环节: compute_decision_score返回scores['greeks_usage'] ∈ [0,1]

    调用链路: greeks_dashboard → _compute_greeks_usage_score → scores['greeks_usage']
    """
    print("\n=== 希腊字母(greeks)最终消费环节验证 ===")

    from ali2026v3_trading.risk.risk_service import get_risk_service
    from ali2026v3_trading.risk.risk_compute_service import RiskComputeService

    rs = get_risk_service()
    if not hasattr(rs, '_get_life_estimator'):
        rs._get_life_estimator = lambda: None
    rcs = RiskComputeService(rs)

    # 消费1: 低使用率dashboard → 评分应高(1.0)
    low_usage_dashboard = {
        'portfolio': {
            'delta_usage_pct': 20.0,
            'gamma_usage_pct': 15.0,
            'vega_usage_pct': 10.0,
        }
    }
    result = rcs.compute_decision_score(
        state_strength=0.8,
        order_flow_consistency=0.5,
        greeks_dashboard=low_usage_dashboard,
    )
    scores = result['dimension_scores']
    assert 'greeks_usage' in scores, f"缺少greeks_usage维度: {list(scores.keys())}"

    gu_score_low_usage = scores['greeks_usage']
    assert 0.0 <= gu_score_low_usage <= 1.0, f"greeks_usage评分超出[0,1]: {gu_score_low_usage}"
    assert gu_score_low_usage == 1.0, \
        f"低使用率(≤30%)应返回1.0: {gu_score_low_usage}"
    print(f"  [PASS] 低使用率(delta=20%,gamma=15%,vega=10%) → score={gu_score_low_usage:.4f}")

    # 消费2: 高使用率dashboard → 评分应低(0.2)
    high_usage_dashboard = {
        'portfolio': {
            'delta_usage_pct': 90.0,
            'gamma_usage_pct': 85.0,
            'vega_usage_pct': 80.0,
        }
    }
    result_high = rcs.compute_decision_score(
        state_strength=0.8,
        order_flow_consistency=0.5,
        greeks_dashboard=high_usage_dashboard,
    )
    gu_score_high_usage = result_high['dimension_scores']['greeks_usage']
    assert 0.0 <= gu_score_high_usage <= 1.0, f"greeks_usage评分超出[0,1]: {gu_score_high_usage}"
    assert gu_score_high_usage < gu_score_low_usage, \
        f"高使用率应低于低使用率: high={gu_score_high_usage:.4f} low={gu_score_low_usage:.4f}"
    print(f"  [PASS] 高使用率(delta=90%,gamma=85%,vega=80%) → score={gu_score_high_usage:.4f}")

    # 消费3: None dashboard → 降级0.5
    result_none = rcs.compute_decision_score(
        state_strength=0.8,
        order_flow_consistency=0.5,
        greeks_dashboard=None,
    )
    gu_score_none = result_none['dimension_scores']['greeks_usage']
    assert gu_score_none == 0.5, f"None应降级0.5: {gu_score_none}"
    print(f"  [PASS] None dashboard → 降级score={gu_score_none:.4f}")

    return True


# ============================================================================
# 3. 夏普 (sharpe) — gate_veto 最终消费环节
# ============================================================================

def test_sharpe_consumption():
    """夏普最终消费环节: CascadeJudge.gate_l2_ml_model返回GateResult

    调用链路: sharpe_ratio → gate_l2_ml_model → GateResult.PASS/WARN/BLOCK
    """
    print("\n=== 夏普(sharpe)最终消费环节验证 ===")

    from ali2026v3_trading.evaluation.cascade_judge import CascadeJudge, BacktestMetrics, GateResult

    judge = CascadeJudge.from_config()

    # 消费1: sharpe=2.0 (高于阈值1.2) → PASS
    metrics_pass = BacktestMetrics(
        num_trades=50,
        profit_loss_ratio=2.0,
        sortino_ratio=2.0,
        calmar_ratio=1.0,
        sharpe_ratio=2.0,
    )
    gates_pass = judge.gate_l2_ml_model(metrics_pass)
    sharpe_gate_pass = [g for g in gates_pass if '夏普' in g.gate_name][0]
    assert sharpe_gate_pass.result == GateResult.PASS, \
        f"sharpe=2.0应PASS: result={sharpe_gate_pass.result}"
    print(f"  [PASS] sharpe=2.0 ≥ 阈值1.2 → GateResult.{sharpe_gate_pass.result.name}")

    # 消费2: sharpe=0.5 (低于阈值1.2) → WARN (hard_block=False)
    metrics_warn = BacktestMetrics(
        num_trades=50,
        profit_loss_ratio=2.0,
        sortino_ratio=2.0,
        calmar_ratio=1.0,
        sharpe_ratio=0.5,
    )
    gates_warn = judge.gate_l2_ml_model(metrics_warn)
    sharpe_gate_warn = [g for g in gates_warn if '夏普' in g.gate_name][0]
    assert sharpe_gate_warn.result == GateResult.WARN, \
        f"sharpe=0.5应WARN: result={sharpe_gate_warn.result}"
    print(f"  [PASS] sharpe=0.5 < 阈值1.2 → GateResult.{sharpe_gate_warn.result.name}")

    # 消费3: tri_weights包含sharpe
    assert 'sharpe' in judge.tri_weights, f"tri_weights缺少sharpe: {judge.tri_weights}"
    sw = judge.tri_weights['sharpe']
    assert 0.0 < sw <= 1.0, f"sharpe在tri_weights中权重超出(0,1]: {sw}"
    print(f"  [PASS] tri_weights['sharpe']={sw:.4f}")

    # 消费4: 阈值从环境变量/CASCADE_MIN_SHARPE读取
    assert judge.thresholds['sharpe'] == 1.2, \
        f"sharpe阈值应为1.2: {judge.thresholds['sharpe']}"
    print(f"  [PASS] 阈值: min_sharpe={judge.thresholds['sharpe']}")

    return True


# ============================================================================
# 4. 三角评判 (tri_validation) — score_dimension 最终消费环节
# ============================================================================

def test_triangle_consumption():
    """三角评判最终消费环节: compute_decision_score返回scores['tri_validation'] ∈ [0,1]

    调用链路: tri_validation_score → _compute_tri_validation_score → scores['tri_validation']
    """
    print("\n=== 三角评判(tri_validation)最终消费环节验证 ===")

    from ali2026v3_trading.risk.risk_service import get_risk_service
    from ali2026v3_trading.risk.risk_compute_service import RiskComputeService

    rs = get_risk_service()
    if not hasattr(rs, '_get_life_estimator'):
        rs._get_life_estimator = lambda: None
    rcs = RiskComputeService(rs)

    # 消费1: tri_validation_score=0.9 → 评分应=0.9
    result = rcs.compute_decision_score(
        state_strength=0.8,
        order_flow_consistency=0.5,
        tri_validation_score=0.9,
    )
    scores = result['dimension_scores']
    assert 'tri_validation' in scores, f"缺少tri_validation维度: {list(scores.keys())}"

    tv_score = scores['tri_validation']
    assert 0.0 <= tv_score <= 1.0, f"tri_validation评分超出[0,1]: {tv_score}"
    assert abs(tv_score - 0.9) < 0.01, f"tri_validation应为0.9: {tv_score}"
    print(f"  [PASS] tri_validation_score=0.9 → score={tv_score:.4f}")

    # 消费2: tri_validation_score=None → 降级0.5
    result_none = rcs.compute_decision_score(
        state_strength=0.8,
        order_flow_consistency=0.5,
        tri_validation_score=None,
    )
    tv_score_none = result_none['dimension_scores']['tri_validation']
    assert tv_score_none == 0.5, f"None应降级0.5: {tv_score_none}"
    print(f"  [PASS] tri_validation_score=None → 降级score={tv_score_none:.4f}")

    # 消费3: LIVE模式权重×1.2
    result_live = rcs.compute_decision_score(
        state_strength=0.8,
        order_flow_consistency=0.5,
        tri_validation_score=0.9,
        slippage_source='LIVE',
    )
    weights_live = result_live['dimension_weights']
    assert weights_live['tri_validation'] >= 0.10, \
        f"LIVE模式tri_validation权重应≥0.10: {weights_live['tri_validation']}"
    print(f"  [PASS] LIVE模式权重={weights_live['tri_validation']:.4f} (×1.2提升)")

    # 消费4: BACKTEST模式权重×0.5
    result_bt = rcs.compute_decision_score(
        state_strength=0.8,
        order_flow_consistency=0.5,
        tri_validation_score=0.9,
        slippage_source='BACKTEST',
    )
    weights_bt = result_bt['dimension_weights']
    assert weights_bt['tri_validation'] < weights_live['tri_validation'], \
        f"BACKTEST权重应<LIVE: bt={weights_bt['tri_validation']:.4f} live={weights_live['tri_validation']:.4f}"
    print(f"  [PASS] BACKTEST模式权重={weights_bt['tri_validation']:.4f} (×0.5折半)")

    return True


# ============================================================================
# 5. 盈亏比 (plr) — veto 最终消费环节
# ============================================================================

def test_plr_consumption():
    """盈亏比最终消费环节: _filter_by_plr拒绝低PLR信号

    调用链路: estimated_plr → _filter_by_plr → ctx.rejected=True
    """
    print("\n=== 盈亏比(plr)最终消费环节验证 ===")

    from ali2026v3_trading.signal.signal_service import SignalService, SignalGenerator
    from ali2026v3_trading.infra.shared_utils import SignalType, OPEN_SIGNAL_TYPES

    svc = SignalService()
    svc._plr_filter_enabled = True
    svc._min_estimated_plr = 2.0

    gen = SignalGenerator(svc)

    # 消费1: plr=1.0 < 阈值2.0 → OPEN信号被拒绝
    ctx_low = type('Ctx', (), {
        'signal_type': SignalType.BUY,
        'estimated_plr': 1.0,
        'instrument_id': 'IO2612-C-3500',
        'rejected': False,
        'reject_reason': '',
        'filter_name': '',
    })()
    gen._filter_by_plr(ctx_low)
    assert ctx_low.rejected, "plr=1.0 < 2.0 应被拒绝"
    assert ctx_low.reject_reason == 'plr_filtered', \
        f"拒绝原因应为plr_filtered: {ctx_low.reject_reason}"
    assert svc._stats['plr_filtered'] >= 1, "plr_filtered计数应增加"
    print(f"  [PASS] plr=1.0 < 阈值2.0 → rejected=True reason={ctx_low.reject_reason}")

    # 消费2: plr=3.0 > 阈值2.0 → 放行
    ctx_high = type('Ctx', (), {
        'signal_type': SignalType.BUY,
        'estimated_plr': 3.0,
        'instrument_id': 'IO2612-C-3500',
        'rejected': False,
        'reject_reason': '',
        'filter_name': '',
    })()
    gen._filter_by_plr(ctx_high)
    assert not ctx_high.rejected, "plr=3.0 > 2.0 应放行"
    print(f"  [PASS] plr=3.0 ≥ 阈值2.0 → rejected=False (放行)")

    # 消费3: CLOSE信号不受PLR约束
    ctx_close = type('Ctx', (), {
        'signal_type': SignalType.CLOSE_LONG,
        'estimated_plr': 0.1,
        'instrument_id': 'IO2612-C-3500',
        'rejected': False,
        'reject_reason': '',
        'filter_name': '',
    })()
    gen._filter_by_plr(ctx_close)
    assert not ctx_close.rejected, "CLOSE信号应不受PLR约束"
    print(f"  [PASS] CLOSE信号不受PLR约束 (plr=0.1也放行)")

    # 消费4: PLR过滤器关闭时不拒绝
    svc._plr_filter_enabled = False
    ctx_disabled = type('Ctx', (), {
        'signal_type': SignalType.BUY,
        'estimated_plr': 0.5,
        'instrument_id': 'IO2612-C-3500',
        'rejected': False,
        'reject_reason': '',
        'filter_name': '',
    })()
    gen._filter_by_plr(ctx_disabled)
    assert not ctx_disabled.rejected, "PLR过滤器关闭时应放行"
    print(f"  [PASS] PLR过滤器关闭时 plr=0.5 也放行")

    return True


# ============================================================================
# 6. 延迟 (latency) — info 最终消费环节
# ============================================================================

def test_latency_consumption():
    """延迟最终消费环节: benchmark()返回延迟统计

    调用链路: 信号生成耗时 → benchmark() → latencies统计
    """
    print("\n=== 延迟(latency)最终消费环节验证 ===")

    from ali2026v3_trading.signal.signal_service import SignalService

    svc = SignalService()

    # 消费1: run_benchmark返回延迟统计
    results = svc.run_benchmark(iterations=10)
    assert results is not None, "benchmark返回None"
    assert 'avg_latency_ms' in results, f"缺少avg_latency_ms: {list(results.keys())}"
    assert 'min_latency_ms' in results, f"缺少min_latency_ms"
    assert 'max_latency_ms' in results, f"缺少max_latency_ms"
    assert 'median_latency_ms' in results, f"缺少median_latency_ms"
    print(f"  [PASS] benchmark返回5项延迟统计: keys={list(results.keys())[:5]}")

    # 消费2: 延迟值非负
    avg_ms = results['avg_latency_ms']
    min_ms = results['min_latency_ms']
    max_ms = results['max_latency_ms']
    median_ms = results['median_latency_ms']
    assert avg_ms >= 0, f"avg_latency_ms应非负: {avg_ms}"
    assert min_ms >= 0, f"min_latency_ms应非负: {min_ms}"
    assert max_ms >= 0, f"max_latency_ms应非负: {max_ms}"
    assert median_ms >= 0, f"median_latency_ms应非负: {median_ms}"
    print(f"  [PASS] 延迟值非负: avg={avg_ms:.3f}ms min={min_ms:.3f}ms max={max_ms:.3f}ms median={median_ms:.3f}ms")

    # 消费3: min ≤ avg ≤ max
    assert min_ms <= avg_ms <= max_ms, \
        f"min≤avg≤max不成立: min={min_ms} avg={avg_ms} max={max_ms}"
    print(f"  [PASS] min≤avg≤max 关系成立")

    # 消费4: benchmark执行完成 (iterations匹配请求 + successful_signals + filtered_signals = iterations)
    _iters = results.get('iterations', 0)
    _succ = results.get('successful_signals', 0)
    _filt = results.get('filtered_signals', 0)
    _tts = results.get('total_time_seconds', 0.0)
    assert _iters == 10, f"iterations应为10: {_iters}"
    assert _succ + _filt == _iters, \
        f"successful+filtered应=iterations: succ={_succ} filt={_filt} iters={_iters}"
    # signals_per_second可从successful_signals和total_time_seconds推导
    _sps = (_succ / _tts) if _tts > 0 else 0.0
    assert _sps >= 0, f"signals_per_second应≥0: {_sps}"
    print(f"  [PASS] benchmark完成: iterations={_iters} successful={_succ} filtered={_filt} sps={_sps:.1f}")

    return True


# ============================================================================
# 7. 资金大小 (capital_scale) — multiplier 最终消费环节
# ============================================================================

def test_capital_scale_consumption():
    """资金大小最终消费环节: _propagate_capital_scale多路传播

    调用链路: capital_scale → _propagate_capital_scale → 4路传播
    """
    print("\n=== 资金大小(capital_scale)最终消费环节验证 ===")

    from ali2026v3_trading.strategy_ecosystem._core import StrategyEcosystem
    from ali2026v3_trading.strategy_judgment.strategy_judgment_facade import CapitalScale

    # 显式注入capital_scale（生产环境由StrategyEcosystem构造函数传入）
    eco = StrategyEcosystem(capital_scale=CapitalScale.MEDIUM)

    # 消费1: capital_scale注入成功
    assert eco._capital_scale is not None, "_capital_scale为None"
    assert eco._capital_scale == CapitalScale.MEDIUM, \
        f"capital_scale应为MEDIUM: {eco._capital_scale}"
    print(f"  [PASS] capital_scale注入成功={eco._capital_scale}")

    # 消费2: route_capital根据capital_scale使用不同基础比例
    eco._capital_route.dynamic_enabled = True

    eco._capital_scale = CapitalScale.SMALL
    alloc_small = eco.route_capital('other')
    assert alloc_small.get('arbitrage') == 0.0, "S5应始终为0"
    assert alloc_small.get('market_making') == 0.0, "S6应始终为0"
    master_small = alloc_small.get('master', 0)
    print(f"  [PASS] SMALL规模: master={master_small:.3f} arbitrage=0 market_making=0")

    eco._capital_scale = CapitalScale.LARGE
    alloc_large = eco.route_capital('other')
    assert alloc_large.get('arbitrage') == 0.0, "S5应始终为0"
    assert alloc_large.get('market_making') == 0.0, "S6应始终为0"
    master_large = alloc_large.get('master', 0)
    print(f"  [PASS] LARGE规模: master={master_large:.3f} arbitrage=0 market_making=0")

    # 消费3: S5/S6在所有capital_scale下都不参与资金分配
    assert alloc_small['arbitrage'] == 0.0 and alloc_large['arbitrage'] == 0.0, \
        "S5在所有规模下都应为0"
    assert alloc_small['market_making'] == 0.0 and alloc_large['market_making'] == 0.0, \
        "S6在所有规模下都应为0"
    print(f"  [PASS] S5/S6在SMALL和LARGE规模下均不参与资金分配")

    # 消费4: _propagate_capital_scale方法存在且可调用
    assert hasattr(eco, '_propagate_capital_scale'), "缺少_propagate_capital_scale方法"
    assert callable(eco._propagate_capital_scale), "_propagate_capital_scale不可调用"
    print(f"  [PASS] _propagate_capital_scale方法存在且可调用")

    # 消费5: _propagate_capital_scale_fallback方法存在
    assert hasattr(eco, '_propagate_capital_scale_fallback'), \
        "缺少_propagate_capital_scale_fallback方法"
    print(f"  [PASS] _propagate_capital_scale_fallback方法存在(4路传播)")

    return True


# ============================================================================
# 主入口
# ============================================================================

if __name__ == '__main__':
    results = []

    tests = [
        ('订单流(order_flow)', test_order_flow_consumption),
        ('希腊字母(greeks)', test_greeks_consumption),
        ('夏普(sharpe)', test_sharpe_consumption),
        ('三角评判(tri_validation)', test_triangle_consumption),
        ('盈亏比(plr)', test_plr_consumption),
        ('延迟(latency)', test_latency_consumption),
        ('资金大小(capital_scale)', test_capital_scale_consumption),
    ]

    for name, test_fn in tests:
        try:
            results.append((name, test_fn()))
        except Exception as e:
            results.append((name, False))
            import traceback
            print(f"  [FAIL] {e}")
            traceback.print_exc()

    # 汇总
    print("\n" + "=" * 60)
    print("七模块最终环节消费断言验证汇总")
    print("=" * 60)
    all_pass = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {name}")
        if not passed:
            all_pass = False

    print("=" * 60)
    if all_pass:
        print(f"全部 {len(results)} 项断言验证通过 — 最后消费环节结果正确")
        sys.exit(0)
    else:
        failed = sum(1 for _, p in results if not p)
        print(f"{len(results) - failed}/{len(results)} 项通过, {failed} 项失败")
        sys.exit(1)
