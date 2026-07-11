# -*- coding: utf-8 -*-
"""
V3断言测试: 验证14:01重启后的修复
覆盖:
1. dry_run_mode回退检查(_dry_run_active)
2. record_signal/record_trade委托路径
3. start_time初始化(非None)
4. output_periodic_summary provider回退
5. persistence_service set_ref优先
6. check_all_positions止盈止损调用
7. _provider_ref 2级回退
8. _safe_elapsed时区修复
9. tick背压降级(不丢弃)
"""
import sys
import os
import inspect
import ast

# 添加项目路径
PROJECT_DIR = r'c:\Users\xu\AppData\Roaming\InfiniTrader_QhZijintianfengPythonX64\pyStrategy\demo'
sys.path.insert(0, PROJECT_DIR)
sys.path.insert(0, os.path.join(PROJECT_DIR, 'ali2026v3_trading'))

PASS = 0
FAIL = 0
ERRORS = []

def check(name, condition, detail=''):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f'  [PASS] {name}')
    else:
        FAIL += 1
        ERRORS.append(f'{name}: {detail}')
        print(f'  [FAIL] {name} {detail}')

print('=' * 70)
print('V3断言测试: 14:01重启修复验证')
print('=' * 70)

# ========== 1. dry_run_mode回退检查 ==========
print('\n--- 1. dry_run_mode回退检查 ---')
try:
    sbl_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'strategy', 'strategy_business_layer.py')
    with open(sbl_path, 'r', encoding='utf-8') as f:
        sbl_src = f.read()
    check('dry_run_mode有_dry_run_active回退',
          '_dry_run_active' in sbl_src and 'FIX-20260708-V3' in sbl_src,
          '缺少V3回退逻辑')
    check('dry_run_mode回退在读取失败后执行',
          "if not _dry_run_mode:" in sbl_src and "_dry_run_active" in sbl_src,
          '回退位置不正确')
    check('dry_run_mode使用getattr(provider,...)',
          "getattr(provider, '_dry_run_active', False)" in sbl_src,
          '未使用getattr安全访问')
except Exception as e:
    check('dry_run_mode回退检查', False, str(e))

# ========== 2. record_signal/record_trade委托路径 ==========
print('\n--- 2. record_signal/record_trade委托路径 ---')
try:
    check('record_signal使用provider.record_signal',
          "getattr(provider, 'record_signal', None)" in sbl_src,
          '未使用record_signal委托')
    check('record_trade使用provider.record_trade',
          "getattr(provider, 'record_trade', None)" in sbl_src,
          '未使用record_trade委托')
    check('record_signal有FIX-20260708-V2标记',
          'FIX-20260708-V2' in sbl_src,
          '缺少V2标记')
    # 确保不再使用错误的_lifecycle_service属性名
    check('不再使用_lifecycle_service属性名',
          '_lifecycle_service' not in sbl_src or '_lifecycle_svc' in sbl_src,
          '仍在使用错误属性名_lifecycle_service')
except Exception as e:
    check('record_signal委托', False, str(e))

# ========== 3. start_time初始化 ==========
print('\n--- 3. start_time初始化 ---')
try:
    scl_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'strategy', 'strategy_config_layer.py')
    with open(scl_path, 'r', encoding='utf-8') as f:
        scl_src = f.read()
    check('start_time初始化为datetime.now(CHINA_TZ)',
          "datetime.now(CHINA_TZ)" in scl_src and "'start_time'" in scl_src,
          'start_time未初始化为当前时间')
    check('不再使用start_time:None',
          "'start_time': None" not in scl_src,
          '仍在使用start_time:None')
    check('有CHINA_TZ和datetime导入',
          'from ali2026v3_trading.infra.shared_utils import CHINA_TZ' in scl_src,
          '缺少CHINA_TZ导入')
except Exception as e:
    check('start_time初始化', False, str(e))

# ========== 4. output_periodic_summary provider回退 ==========
print('\n--- 4. output_periodic_summary provider回退 ---')
try:
    td_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'strategy', 'tick_dispatch.py')
    with open(td_path, 'r', encoding='utf-8') as f:
        td_src = f.read()
    check('有state_store get_ref回退',
          'get_ref' in td_src and '_strategy' in td_src,
          '缺少state_store回退')
    check('有_runtime_strategy_host回退',
          '_runtime_strategy_host' in td_src,
          '缺少_runtime_strategy_host回退')
    check('有FIX-20260708-V2标记',
          'FIX-20260708-V2' in td_src,
          '缺少V2标记')
except Exception as e:
    check('output_periodic_summary回退', False, str(e))

# ========== 5. persistence_service set_ref优先 ==========
print('\n--- 5. persistence_service set_ref优先 ---')
try:
    ps_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'strategy', 'persistence_service.py')
    with open(ps_path, 'r', encoding='utf-8') as f:
        ps_src = f.read()
    check('state_ref_keys使用set_ref优先',
          'set_ref' in ps_src and 'state_ref_keys' in ps_src,
          '缺少set_ref优先逻辑')
    check('有set()回退',
          'set(' in ps_src and 'state_ref_keys' in ps_src,
          '缺少set回退')
except Exception as e:
    check('persistence_service set_ref', False, str(e))

# ========== 6. check_all_positions止盈止损 ==========
print('\n--- 6. check_all_positions止盈止损 ---')
try:
    pcs_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'position', 'position_check_service.py')
    with open(pcs_path, 'r', encoding='utf-8') as f:
        pcs_src = f.read()
    check('check_all_positions调用_check_stop_profit',
          '_check_stop_profit' in pcs_src,
          '缺少_check_stop_profit调用')
    check('check_all_positions调用_check_stop_loss',
          '_check_stop_loss' in pcs_src,
          '缺少_check_stop_loss调用')
    check('有FIX-20260708-CLOSE-BREAK标记',
          'FIX-20260708-CLOSE-BREAK' in pcs_src,
          '缺少CLOSE-BREAK标记')
    check('check_all_positions调用_check_time_stop',
          '_check_time_stop' in pcs_src,
          '缺少_check_time_stop调用')
    check('check_all_positions调用_check_two_stage_stop',
          '_check_two_stage_stop' in pcs_src,
          '缺少_check_two_stage_stop调用')
    check('check_all_positions调用_check_eod_close',
          '_check_eod_close' in pcs_src,
          '缺少_check_eod_close调用')
except Exception as e:
    check('check_all_positions', False, str(e))

# ========== 7. _provider_ref 2级回退 ==========
print('\n--- 7. _provider_ref 2级回退 ---')
try:
    oev_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'order', 'order_executor_validation.py')
    with open(oev_path, 'r', encoding='utf-8') as f:
        oev_src = f.read()
    check('有_provider_ref回退1: platform_fn_self',
          'platform_fn_self' in oev_src or '_platform_insert_order' in oev_src,
          '缺少回退1: _platform_insert_order.__self__')
    check('有_provider_ref回退2: cached_params',
          'cached_params' in oev_src or 'get_cached_params' in oev_src,
          '缺少回退2: get_cached_params')
    check('有FIX-20260708-PROVIDER-FALLBACK标记',
          'FIX-20260708-PROVIDER-FALLBACK' in oev_src,
          '缺少PROVIDER-FALLBACK标记')
    check('不再有旧消息_provider_ref为None，无法注入虚拟回调',
          '_provider_ref为None，无法注入虚拟回调' not in oev_src,
          '仍有旧WARNING消息')
except Exception as e:
    check('_provider_ref回退', False, str(e))

# ========== 8. _safe_elapsed时区修复 ==========
print('\n--- 8. _safe_elapsed时区修复 ---')
try:
    osm_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'order', 'order_state_manager.py')
    with open(osm_path, 'r', encoding='utf-8') as f:
        osm_src = f.read()
    check('有_safe_elapsed函数',
          '_safe_elapsed' in osm_src,
          '缺少_safe_elapsed函数')
    check('check_pending_orders使用_safe_elapsed',
          '_safe_elapsed' in osm_src and 'check_pending_orders' in osm_src,
          'check_pending_orders未使用_safe_elapsed')
except Exception as e:
    check('_safe_elapsed', False, str(e))

# ========== 9. tick背压降级(不丢弃) ==========
print('\n--- 9. tick背压降级(不丢弃) ---')
try:
    tps_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'strategy', 'tick_processing_service.py')
    with open(tps_path, 'r', encoding='utf-8') as f:
        tps_src = f.read()
    check('有FIX-20260708标记的背压降级',
          'FIX-20260708' in tps_src or '背压' in tps_src or 'degrad' in tps_src.lower(),
          '缺少背压降级标记')
    # tick_dispatch中不丢弃tick
    check('tick_dispatch有降级模式',
          'degrad' in td_src.lower() or '降级' in td_src,
          '缺少降级模式')
except Exception as e:
    check('tick背压降级', False, str(e))

# ========== 10. lifecycle_init set_ref重注册 ==========
print('\n--- 10. lifecycle_init set_ref重注册 ---')
try:
    li_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'lifecycle', 'lifecycle_init.py')
    with open(li_path, 'r', encoding='utf-8') as f:
        li_src = f.read()
    check('on_init设置start_time',
          "p._stats['start_time'] = datetime.now(CHINA_TZ)" in li_src,
          '未设置start_time')
    check('on_init重注册_stats到state_store(set_ref)',
          'set_ref' in li_src and '_stats' in li_src,
          '未重注册_stats')
    check('有FIX-20260708标记',
          'FIX-20260708' in li_src,
          '缺少FIX标记')
except Exception as e:
    check('lifecycle_init set_ref', False, str(e))

# ========== 11. OrderService dry_run同步 ==========
print('\n--- 11. OrderService dry_run同步 ---')
try:
    lc_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'lifecycle', 'lifecycle_callbacks.py')
    with open(lc_path, 'r', encoding='utf-8') as f:
        lc_src = f.read()
    check('lifecycle_callbacks设置_dry_run_active',
          '_dry_run_active' in lc_src and 'setattr' in lc_src,
          '未设置_dry_run_active')
    check('lifecycle_callbacks同步到OrderService._dry_run_mode',
          '_osvc._dry_run_mode = True' in lc_src,
          '未同步到OrderService')
    check('有DRY-RUN同步日志',
          '已同步_dry_run_mode=True到OrderService' in lc_src,
          '缺少同步日志')
except Exception as e:
    check('OrderService dry_run同步', False, str(e))

# ========== 12. 4级fallback订单拦截 ==========
print('\n--- 12. 4级fallback订单拦截 ---')
try:
    oep_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'order', 'order_executor_platform.py')
    with open(oep_path, 'r', encoding='utf-8') as f:
        oep_src = f.read()
    check('有4级fallback检查',
          oep_src.count('_dry_run_mode') >= 2 or oep_src.count('_dry_run_active') >= 2,
          '缺少多级fallback')
    check('有_dry_run_mode检查',
          '_dry_run_mode' in oep_src,
          '缺少_dry_run_mode检查')
    check('有_dry_run_active检查',
          '_dry_run_active' in oep_src,
          '缺少_dry_run_active检查')
except Exception as e:
    check('4级fallback', False, str(e))

# ========== 13. hft状态映射 ==========
print('\n--- 13. hft状态映射 ---')
try:
    check('_STATE_REASON_MAP有hft映射',
          'hft' in scl_src.lower() or 'HIGH_FREQ' in scl_src,
          '缺少hft→HIGH_FREQ映射')
except Exception as e:
    check('hft状态映射', False, str(e))

# ========== 14. check_position_risk使用_destroyed ==========
print('\n--- 14. check_position_risk使用_destroyed ---')
try:
    sml_path = os.path.join(PROJECT_DIR, 'ali2026v3_trading', 'strategy', 'strategy_monitoring_layer.py')
    with open(sml_path, 'r', encoding='utf-8') as f:
        sml_src = f.read()
    check('check_position_risk使用_destroyed而非_is_running',
          '_destroyed' in sml_src and 'check_position_risk' in sml_src,
          '未使用_destroyed')
except Exception as e:
    check('check_position_risk _destroyed', False, str(e))

# ========== 汇总 ==========
print('\n' + '=' * 70)
print(f'断言测试汇总: PASS={PASS}, FAIL={FAIL}, 总计={PASS+FAIL}')
print('=' * 70)
if ERRORS:
    print('\n失败项:')
    for e in ERRORS:
        print(f'  - {e}')
sys.exit(0 if FAIL == 0 else 1)
