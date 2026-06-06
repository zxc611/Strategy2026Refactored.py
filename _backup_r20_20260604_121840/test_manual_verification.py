"""
手动验证脚本 v2 - 第二轮深度隐患排查

验证范围:
  第一轮(29项): Spring集成、Gamma检查、fail-safe、互斥规则
  第二轮新增:
    - route_capital 动态禁用时含spring
    - switch_active_strategy 更新spring state
    - get_stats 包含spring pnl/ev
    - _stats 包含spring_trades计数
    - _is_same_delta_direction 支持spring策略
    - PositionService._risk_service 属性存在性
    - CrossStrategyRiskGuard Gamma REDUCE级别
    - record_spring_trade 统计完整性
    - switch_active_strategy 返回含spring分配
"""
import sys
import os
import traceback
from datetime import datetime, date, timezone  # ENV-P2修复

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

PASS_COUNT = 0
FAIL_COUNT = 0
ISSUES = []


def check(condition, test_name, detail=''):
    global PASS_COUNT, FAIL_COUNT
    if condition:
        PASS_COUNT += 1
        print(f"  [PASS] {test_name}")
    else:
        FAIL_COUNT += 1
        msg = f"  [FAIL] {test_name}"
        if detail:
            msg += f" -- {detail}"
        print(msg)
        ISSUES.append((test_name, detail))


def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


# ============================================================================
# 第一轮复查 (29项)
# ============================================================================
def test_round1_spring_integration():
    section("R1. Spring策略集成完整性")

    from ali2026v3_trading.strategy_ecosystem import StrategyEcosystem, StrategySlot

    eco = StrategyEcosystem()

    spring_slot = eco._get_slot('spring')
    check(spring_slot is not None, "_get_slot('spring') 返回非None")
    check(isinstance(spring_slot, StrategySlot) if spring_slot else False,
          "spring slot 是 StrategySlot 实例")

    spring_trades = eco._get_trades('spring')
    check(spring_trades is not None, "_get_trades('spring') 返回非None")

    ev_result = eco.check_absolute_ev_bottomline()
    check('spring' in ev_result.get('strategies', {}),
          "check_absolute_ev_bottomline 包含 spring")

    alloc = eco.get_capital_allocations()
    check('spring' in alloc, "get_capital_allocations 包含 spring")

    slots = eco.get_strategy_slots()
    check('spring' in slots, "get_strategy_slots 包含 spring")

    health = eco.get_health_status()
    check('spring' in health.get('strategies', {}),
          "get_health_status.strategies 包含 spring")

    eco.record_spring_trade('BUY_CALL')
    check(eco._spring.position_count > 0, "record_spring_trade 增加 position_count")

    eco.record_strategy_pnl('spring', 100.0, 5.0)
    check(eco._spring.total_pnl != 0.0, "record_strategy_pnl('spring', ...) 生效")


def test_round1_gamma_check():
    section("R1. CrossStrategyRiskGuard Gamma检查")

    from ali2026v3_trading.position_service import CrossStrategyRiskGuard, GreeksExposure

    guard = CrossStrategyRiskGuard(delta_limit=3.0, vega_limit=1.5, gamma_limit=0.5)

    normal_exposure = GreeksExposure(gross_delta=1.0, gross_vega=0.5, gross_gamma=0.1)
    level, _, _ = guard.check(normal_exposure)
    check(level == 'PASS', "正常敞口返回 PASS")

    high_gamma = GreeksExposure(gross_delta=1.0, gross_vega=0.5, gross_gamma=1.0)
    level, reason, _ = guard.check(high_gamma)
    check(level in ('WARN', 'REDUCE', 'BLOCK', 'CIRCUIT_BREAK'),
          "Gamma超限触发风控响应", f"实际: {level}")

    extreme_gamma = GreeksExposure(gross_delta=1.0, gross_vega=0.5, gross_gamma=2.0)
    level, _, _ = guard.check(extreme_gamma)
    check(level in ('BLOCK', 'CIRCUIT_BREAK'), "Gamma严重超限触发阻断")


def test_round1_fail_safe():
    section("R1. Fail-safe一致性")

    from ali2026v3_trading.position_service import PositionService

    svc = PositionService(risk_service=None)
    result = svc.check_position_limit('test_account', 1000.0)
    check(result is False, "check_position_limit RiskService不可用时返回False")

    from ali2026v3_trading.order_service import OrderService
    osvc = OrderService()
    risk_level = osvc._check_cross_strategy_risk()
    check(risk_level in ('PASS', 'BLOCK', 'CIRCUIT_BREAK'),
          "OrderService._check_cross_strategy_risk 返回有效级别")

    from ali2026v3_trading.strategy_ecosystem import StrategyEcosystem
    eco = StrategyEcosystem()
    risk_level = eco._check_cross_strategy_risk('IF2606', 'BUY')
    check(risk_level in ('PASS', 'BLOCK', 'CIRCUIT_BREAK'),
          "StrategyEcosystem._check_cross_strategy_risk 返回有效级别")


def test_round1_mutual_exclusion():
    section("R1. 互斥规则覆盖Spring策略")

    from ali2026v3_trading.strategy_ecosystem import StrategyEcosystem

    eco = StrategyEcosystem()

    eco._spring.frozen = True
    allowed, reason = eco.check_mutual_exclusion('spring', 'BUY')
    check(allowed is False, "Spring策略冻结时返回False")
    eco._spring.frozen = False

    eco._master.position_count = 1
    eco._master.state = 'active'
    eco._master.last_direction = 'BUY'
    allowed, reason = eco.check_mutual_exclusion('spring', 'BUY')
    check(allowed is False, "Spring与master同向开仓被互斥规则阻断",
          f"allowed={allowed}, reason={reason}")
    eco._master.position_count = 0
    eco._master.last_direction = ''


def test_round1_greeks_aggregation():
    section("R1. Greeks聚合Spring映射")

    from ali2026v3_trading.position_service import (
        aggregate_greeks_exposure, PositionRecord, _REASON_STRATEGY_MAP,
    )

    check('BOX_SPRING' in _REASON_STRATEGY_MAP, "_REASON_STRATEGY_MAP 包含 BOX_SPRING")
    check(_REASON_STRATEGY_MAP.get('BOX_SPRING') == 'spring', "BOX_SPRING 映射到 spring")

    spring_record = PositionRecord(
        position_id='test_spring_1', instrument_id='IF2606-C-3900',
        exchange='CFFEX', volume=1, direction='long', open_price=50.0,
        open_time=datetime.now(timezone.utc), open_date=date.today(),
        position_type='long', open_reason='BOX_SPRING',
    )
    positions = {'IF2606-C-3900': {'test_spring_1': spring_record}}
    exposure = aggregate_greeks_exposure(positions)
    check('spring' in exposure.by_strategy, "Spring持仓出现在by_strategy中")


def test_round1_risk_guard_tiers():
    section("R1. CrossStrategyRiskGuard分层响应")

    from ali2026v3_trading.position_service import CrossStrategyRiskGuard, GreeksExposure

    guard = CrossStrategyRiskGuard(delta_limit=3.0, vega_limit=1.5, gamma_limit=0.5)

    level, _, _ = guard.check(GreeksExposure(gross_delta=3.5, net_delta=3.5))
    check(level == 'WARN', "注意线 WARN")

    level, _, _ = guard.check(GreeksExposure(gross_delta=4.0, net_delta=4.0))
    check(level == 'REDUCE', "预警线 REDUCE")

    level, _, _ = guard.check(GreeksExposure(gross_delta=5.0, net_delta=5.0))
    check(level == 'BLOCK', "硬阻断线 BLOCK")

    level, _, _ = guard.check(GreeksExposure(gross_delta=7.0, net_delta=7.0))
    check(level == 'CIRCUIT_BREAK', "熔断线 CIRCUIT_BREAK")

    guard_dd = CrossStrategyRiskGuard(delta_limit=3.0)
    guard_dd.set_daily_drawdown(5.0)
    level, _, _ = guard_dd.check(GreeksExposure(gross_delta=1.0))
    check(level == 'CIRCUIT_BREAK', "日回撤5%触发CIRCUIT_BREAK")


def test_round1_order_service():
    section("R1. OrderService跨策略风控集成")

    from ali2026v3_trading.order_service import OrderService, _OPEN_REASON_CODES

    osvc = OrderService()
    check(hasattr(osvc, '_check_cross_strategy_risk'),
          "OrderService 有 _check_cross_strategy_risk 方法")
    check('BOX_SPRING' in _OPEN_REASON_CODES, "_OPEN_REASON_CODES 包含 BOX_SPRING")


# ============================================================================
# 第二轮深度排查 (新增检查项)
# ============================================================================
def test_r2_route_capital_static():
    section("R2. route_capital 动态禁用时含spring")

    from ali2026v3_trading.strategy_ecosystem import StrategyEcosystem, CapitalRoute

    cr = CapitalRoute(dynamic_enabled=False)
    eco = StrategyEcosystem(capital_route=cr)
    alloc = eco.route_capital('correct_trending')
    check('spring' in alloc, "route_capital(动态禁用) 包含 spring",
          f"实际keys: {list(alloc.keys())}")


def test_r2_switch_active_strategy_spring_state():
    section("R2. switch_active_strategy 更新spring state")

    from ali2026v3_trading.strategy_ecosystem import StrategyEcosystem

    eco = StrategyEcosystem()

    eco.switch_active_strategy('correct_trending')
    check(eco._spring.state == 'active',
          "spring策略在correct_trending状态下为active",
          f"实际state: {eco._spring.state}")

    eco.switch_active_strategy('incorrect_reversal')
    check(eco._spring.state == 'active',
          "spring策略在incorrect_reversal状态下仍为active",
          f"实际state: {eco._spring.state}")

    eco.switch_active_strategy('other')
    check(eco._spring.state == 'active',
          "spring策略在other状态下仍为active",
          f"实际state: {eco._spring.state}")


def test_r2_get_stats_spring():
    section("R2. get_stats 包含spring pnl/ev")

    from ali2026v3_trading.strategy_ecosystem import StrategyEcosystem

    eco = StrategyEcosystem()
    stats = eco.get_stats()
    check('spring_pnl' in stats, "get_stats 包含 spring_pnl",
          f"实际keys: {[k for k in stats.keys() if 'spring' in k or 'pnl' in k or 'ev' in k]}")
    check('spring_ev' in stats, "get_stats 包含 spring_ev",
          f"实际keys: {[k for k in stats.keys() if 'spring' in k or 'ev' in k]}")


def test_r2_stats_spring_trades():
    section("R2. _stats 包含spring_trades计数")

    from ali2026v3_trading.strategy_ecosystem import StrategyEcosystem

    eco = StrategyEcosystem()
    check('spring_trades' in eco._stats, "_stats 包含 spring_trades 键",
          f"实际keys: {list(eco._stats.keys())}")


def test_r2_risk_service_attribute():
    section("R2. PositionService._risk_service 属性存在性")

    from ali2026v3_trading.position_service import PositionService

    svc_with = PositionService(risk_service='mock')
    check(hasattr(svc_with, '_risk_service'), "有risk_service时 _risk_service 属性存在")
    check(svc_with._risk_service == 'mock', "_risk_service 值正确",
          f"实际值: {svc_with._risk_service}")

    svc_without = PositionService(risk_service=None)
    check(hasattr(svc_without, '_risk_service'), "无risk_service时 _risk_service 属性存在")
    check(svc_without._risk_service is None, "_risk_service 为None",
          f"实际值: {svc_without._risk_service}")


def test_r2_gamma_reduce_tier():
    section("R2. CrossStrategyRiskGuard Gamma REDUCE级别")

    from ali2026v3_trading.position_service import CrossStrategyRiskGuard, GreeksExposure

    guard = CrossStrategyRiskGuard(delta_limit=3.0, vega_limit=1.5, gamma_limit=0.5)

    gamma_reduce = GreeksExposure(gross_delta=1.0, gross_vega=0.5, gross_gamma=0.7)
    level, reason, _ = guard.check(gamma_reduce)
    check(level in ('WARN', 'REDUCE'), "Gamma超限但未超1.5x触发WARN或REDUCE",
          f"实际: level={level}, reason={reason}")


def test_r2_record_spring_trade_stats():
    section("R2. record_spring_trade 统计完整性")

    from ali2026v3_trading.strategy_ecosystem import StrategyEcosystem

    eco = StrategyEcosystem()
    initial_total = eco._stats['total_trades']
    eco.record_spring_trade('BUY_CALL')
    check(eco._stats['total_trades'] == initial_total + 1,
          "record_spring_trade 增加 total_trades",
          f"初始={initial_total}, 当前={eco._stats['total_trades']}")

    if 'spring_trades' in eco._stats:
        check(eco._stats['spring_trades'] > 0,
              "record_spring_trade 增加 spring_trades",
              f"实际spring_trades: {eco._stats.get('spring_trades', 'N/A')}")
    else:
        check(False, "spring_trades 键存在于 _stats", "键不存在")


def test_r2_switch_return_spring_alloc():
    section("R2. switch_active_strategy 返回含spring分配")

    from ali2026v3_trading.strategy_ecosystem import StrategyEcosystem

    eco = StrategyEcosystem()
    result = eco.switch_active_strategy('incorrect_reversal')
    alloc = result.get('capital_allocations', {})
    check('spring' in alloc, "switch返回的capital_allocations包含spring",
          f"实际keys: {list(alloc.keys())}")


def test_r2_is_same_delta_direction_spring():
    section("R2. _is_same_delta_direction 支持spring策略")

    from ali2026v3_trading.strategy_ecosystem import StrategyEcosystem

    eco = StrategyEcosystem()
    eco._master.last_direction = 'BUY'

    result = eco._is_same_delta_direction('spring', 'BUY', 'master')
    check(result is True, "spring BUY 与 master BUY 同向",
          f"实际: {result}")

    result = eco._is_same_delta_direction('spring', 'SELL', 'master')
    check(result is False, "spring SELL 与 master BUY 反向",
          f"实际: {result}")

    eco._master.last_direction = ''


def test_r2_position_service_no_comment_code():
    section("R2. PositionService 无注释行代码陷阱")

    from ali2026v3_trading.position_service import PositionService
    import inspect

    source = inspect.getsource(PositionService.__init__)
    lines = source.split('\n')
    dangerous_lines = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('#') and '=' in stripped and 'self.' in stripped:
            if not stripped.startswith('# ✅') and not stripped.startswith('# P'):
                dangerous_lines.append((i+1, stripped))

    check(len(dangerous_lines) == 0, "__init__中无注释行代码陷阱",
          f"可疑行: {dangerous_lines[:5]}")


# ============================================================================
# 运行所有测试
# ============================================================================
if __name__ == '__main__':
    tests = [
        test_round1_spring_integration,
        test_round1_gamma_check,
        test_round1_fail_safe,
        test_round1_mutual_exclusion,
        test_round1_greeks_aggregation,
        test_round1_risk_guard_tiers,
        test_round1_order_service,
        test_r2_route_capital_static,
        test_r2_switch_active_strategy_spring_state,
        test_r2_get_stats_spring,
        test_r2_stats_spring_trades,
        test_r2_risk_service_attribute,
        test_r2_gamma_reduce_tier,
        test_r2_record_spring_trade_stats,
        test_r2_switch_return_spring_alloc,
        test_r2_is_same_delta_direction_spring,
        test_r2_position_service_no_comment_code,
    ]

    for test_fn in tests:
        try:
            test_fn()
        except Exception as e:
            FAIL_COUNT += 1
            tb = traceback.format_exc()
            ISSUES.append((test_fn.__name__, str(e)))
            print(f"  [ERROR] {test_fn.__name__} 异常: {e}")
            print(f"  {tb[:500]}")

    print(f"\n{'='*70}")
    print(f"  验证结果汇总 (第二轮)")
    print(f"{'='*70}")
    print(f"  通过: {PASS_COUNT}")
    print(f"  失败: {FAIL_COUNT}")
    print(f"  总计: {PASS_COUNT + FAIL_COUNT}")

    if ISSUES:
        print(f"\n  发现的隐患清单:")
        print(f"  {'─'*60}")
        for i, (name, detail) in enumerate(ISSUES, 1):
            print(f"  {i}. {name}")
            if detail:
                print(f"     详情: {detail}")

    sys.exit(0 if FAIL_COUNT == 0 else 1)
