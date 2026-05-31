ali2026v3_trading 目录完整探索报告
一、目录层级结构

code
 
ali2026v3_trading/
├── (根目录) — 生产代码主体 (91个.py文件)
├── config/          — 配置文件目录 (4个文件)
├── evaluation/      — 评判系统目录 (8个.py文件)
├── tests/           — 测试系统目录 (33个.py文件)
├── 策略评判/         — 策略评判专用模块 (10个.py文件)
├── 参数池/           — 参数优化池(中文目录) (22个.py文件)
├── param_pool/       — 参数优化池(英文目录,与参数池内容相同) (22个.py文件)
├── quant_data/       — 量化数据快照
│   └── snapshots/
├── logs/             — 日志目录
│   ├── ecosystem/
│   ├── readable/
│   ├── shadow/
│   └── state_snapshots/
├── data/             — 数据目录 (空)
├── docs/             — 文档目录 (空)
├── scripts/          — 脚本目录 (空)
├── specs/            — 规格目录 (空)
├── _verify_logs/     — 验证日志
├── _wal_orders/      — WAL订单日志
└── .bandit           — Bandit安全扫描配置
二、三系统文件分布
系统	目录	.py文件数	说明
生产系统	根目录 + config/	91 + 0 = 91	核心交易、策略、风控、数据管理等
测试系统	tests/	33	pytest测试、端到端验证、验收测试
评判系统	evaluation/ + 策略评判/	8 + 10 = 18	策略评判引擎、瀑布评判、行为诊断等
参数池系统	参数池/ + param_pool/	22 + 22 = 44 (存在重复)	参数扫描、优化、量化分析
三、完整文件列表与功能描述
3.1 根目录 — 生产系统核心 (91个.py文件)
入口/门面层：

文件	功能
main.py	策略兼容入口，纯导出层，延迟导入t_type_bootstrap
ProductionQuantSystem.py	量化核心系统V3门面类+模块级单例+主程序入口
__init__.py	包初始化
量化核心层 (quant_*)：

文件	功能
quant_infra.py	基础设施原语(NumpyRingBuffer+日志限流)
quant_platform.py	平台层(交易所时间+Tick聚合+原子状态+健康监控)
quant_core.py	6个核心量化模块(趋势+PCA+HMM+波动率+协整+生存分析)
quant_services.py	服务层(持久化+热配置+Numba JIT+单例管理)
策略层 (strategy_*)：

文件	功能
strategy_core_service.py	策略核心服务Facade=Lifecycle+Instrument+Historical+Tick
strategy_lifecycle_mixin.py	策略生命周期状态定义与管理
strategy_instrument_mixin.py	合约加载、规范化、推导纯逻辑方法簇
strategy_historical.py	历史K线加载Mixin
strategy_tick_handler.py	Tick数据处理Mixin(on_tick回调+统计汇总)
strategy_ecosystem.py	策略生态系统：三大策略协同+资金路由+互斥规则
strategy_scheduler.py	策略定时任务管理(APScheduler)
策略实现：

文件	功能
box_detector.py	箱体检测与极值判断(箱底/箱顶识别)
box_spring_strategy.py	箱体波动率脉冲策略(弹簧策略)，交易波动率脉冲
shadow_strategy_engine.py	影子策略监控引擎(Alpha衰减监测+反向逻辑+随机动作)
服务层 (CQRS)：

文件	功能
signal_service.py	信号服务(CQRS Command)：信号生成+冷却管理+信号历史
order_service.py	订单服务(CQRS Command)：订单执行+认证+限流+撤单追单
order_flow_analyzer.py	微观结构分析器+成交量加权订单流
order_flow_bridge.py	订单流桥接+微观结构套利检测
order_persistence.py	订单持久化(WAL日志+JSON快照)
position_service.py	持仓服务(CQRS Command)：开平仓+持仓查询+风控管理
risk_service.py	风控服务：限额管理+信号限频+策略状态检查
params_service.py	参数服务：参数管理核心功能
tvf_param_loader.py	TVF参数加载器：从YAML加载六维仓位调整因子参数
state_param_manager.py	状态参数管理器(状态转换捕捉+事件)
scheduler_service.py	调度服务：超时+重试+优先级+取消
query_service.py	数据查询服务(DuckDB)：合约/K线/Tick/期权链查询
maintenance_service.py	存储维护服务：数据自愈(启动检查+孤儿清理+空表检测)
ui_service.py	UI服务模块：策略UI混入类+独立控制面板
config_exchange.py	交易所与合约配置模块：品种映射+合约解析
product_initializer.py	品种初始化模块：期货/期权配置解析+DB建表
t_type_service.py	T型图服务模块(TTypeService+单例工厂)
ds_option_sync.py	期权同步状态计算Mixin
service_container.py	服务容器：完整依赖管理+层级规则
治理/监控层：

文件	功能
governance_engine.py	治理引擎：参数漂移检测+策略违规追踪
mode_engine.py	模式切换引擎：大/小资金模式切换+凯利公式+时间衰减过滤
health_check_api.py	健康检查API(系统状态快照+模式切换)
performance_monitor.py	性能监控：函数调用统计+执行时间+错误率跟踪
diagnosis_periodic.py	周期性诊断：14合约诊断+宽限期管理+资源快照
subscription_manager.py	订阅管理器v2：重试队列+退避+异步落盘+告警回调
量化分析/归因：

文件	功能
greeks_calculator.py	希腊字母计算器：BS Greeks+IV+二叉树+蒙特卡洛
plr_calculator.py	盈亏比(PLR)计算器
pnl_attribution.py	四维PnL归因(策略×品种×方向×时段)
attribution_extensions.py	策略归因13项扩展修复
jensen_alpha.py	Jensen's Alpha标准计算
statistical_tests.py	统计检验增强(Sharpe t-test+DSR+回撤置信区间)
statistical_validity_extensions.py	统计有效性7项修复(regime change+幸存者偏差+AIC/BIC等)
基础设施/工具层：

文件	功能
shared_utils.py	共享工具函数：标准化+类型转换+RingBuffer+ShardRouter
resilience_utils.py	容错/退避/看门狗/断路器/浮点精度辅助
serialization_utils.py	统一序列化工具(JSON/YAML+NaN防护+datetime可逆)
concurrent_utils.py	线程池拒绝策略枚举
singleton_registry.py	单例命名空间隔离注册表
db_adapter.py	DuckDB访问适配器(集中抽象层，减少直接依赖泄露)
storage_core.py	存储核心Mixin(DB路径+核心写入)
storage_query.py	存储查询Mixin(READ+HELPER+INSTRUMENT+SCHEMA)
width_cache.py	期权宽度缓存模块(从t_type_service拆分)
system_contracts.py	跨系统契约(生产/回测/评判对齐接口)
cross_system_execution.py	统一执行原语(生产/回测/评判三系统共享)
exceptions.py	全系统自定义异常类层次
module_load_status.py	模块加载状态管理(降级状态记录)
crack_validation.py	V7实盘前边缘裂缝验证(裂缝14-30)
causal_chain_utils.py	数据因果链追踪/隔离/清理/回滚(CC-01~12)
hft_enhancements.py	HFT增强引擎薄门面(七大竞争优势统一入口)
ops_documentation.py	运维文档补充(容错策略+告警级别+冷启动)
chaos_engineering.py	混沌工程/故障注入框架
security_hardening.py	安全加固框架(错误脱敏+堆栈截断+pickle安全+环境差异化)
code_quality_fixes.py	代码质量修复
审计/验证/修复脚本（临时性质，不属于生产链路）：

文件	功能
_audit_p0_full.py	P0级全量审计脚本
_audit_p1p2_full.py	P1/P2级全量审计脚本
_audit_r16_recheck.py	R16轮重审计
_check_syntax.py	语法检查脚本
_fix_p2_issues_batch2.py	P2问题批量修复(第二批)
_func_verify_r24.py	R24轮功能验证
_verify_compile.py	编译验证
_verify_p1_fixes_minimal.py	P1修复最小验证
_verify_p1_serialization_fixes.py	P1序列化修复验证
_verify_p2_fixes_batch2.py	P2修复第二批验证
adversarial_test.py	对抗性测试
audit_edge_cases_v71.py	V7.1边缘案例审计
audit_verification.py	审计验证脚本
fix_p1_p2_issues.py	P1/P2问题修复脚本
fix_importerror_batch.py	ImportError批量修复
final_verify.py	最终验证脚本
full_chain_verify_v9.py	V9全链路验证
goldilocks_test.py	金发姑娘测试(参数适度性)
strict_verify_v9.py	V9严格验证
validate_fixes.py	修复验证
verify_crack_fixes.py	裂缝修复验证
verify_d_issues_compile.py	D类问题编译验证
verify_n_issues_compile.py	N类问题编译验证
P1_P2_问题修复脚本.py	P1/P2问题修复脚本(中文命名)
全链路实证验证脚本.py	全链路实证验证
全系统审计扫描脚本.py	全系统审计扫描
p2_fix_utils.py	P2修复工具函数
r5_verify.py	R5轮验证
r5_verify_clean.py	R5轮验证(清洁版)
r7_count_lines.py	R7轮行数统计
r7_verify_compile.py	R7轮编译验证
r10_fix_indent.py	R10轮缩进修复
r10_fix_r04.py	R10轮R04修复
r10_fix_r04b.py	R10轮R04b修复
r10_fix_r13.py	R10轮R13修复
r10_fix_r13b.py	R10轮R13b修复
r10_fix_r13c.py	R10轮R13c修复
r10_verify.py	R10轮验证
r13_backup_and_count.py	R13轮备份与计数
3.2 evaluation/ — 评判系统 (8个.py文件)
文件	功能
__init__.py	包初始化
cascade_judge.py	三层瀑布式评判引擎(盈亏比->三角验证->小资金约束)
parameter_drift_detector.py	参数漂移检测器(委托governance_engine)
violation_tracker.py	策略违规追踪器(委托governance_engine)
marquee_threshold.py	看板阈值配置(三阶段阈值)
activity_weighted_scorer.py	活跃度加权评分器
state_density_decay.py	状态密度衰减追踪器
chicory_eviction.py	菊苣驱逐策略(衰减+违规加权驱逐)
3.3 策略评判/ — 策略评判专用模块 (10个.py文件)
文件	功能
__init__.py	包初始化
strategy_judgment_engine.py	策略评判引擎v1.4(盈利能力评分+边际递减+权重归一化)
strategy_behavior_diagnosis.py	策略行为诊断v1.1(4子项拆解+6策略预设逻辑映射)
turning_point_microscope.py	拐点显微镜(Tick->增强Bar+在线高低点+多周期均线)
resonance_turning_point_marker.py	周期共振拐点标记(预期转折点+实际转折点)
parameter_pool_adapter.py	参数池适配器(回测结果->评判引擎输入转换)
backtest_integration_hooks.py	回测集成钩子(18策略完整生命周期)
market_snapshot_collector.py	市场快照采集器(关键时刻完整市场状态捕获)
config.py	策略评判配置
usage_example.py	使用示例
3.4 tests/ — 测试系统 (33个.py文件)
文件	功能
__init__.py	包初始化
conftest.py	pytest fixtures配置
test_e2e_pipeline.py	端到端管道测试
test_acceptance.py	验收测试
test_service_core.py	服务核心测试
test_risk_service_core.py	风控服务核心测试
test_strategy_core_service.py	策略核心服务测试
test_strategy_ecosystem.py	策略生态系统测试
test_shadow_strategy_engine.py	影子策略引擎测试
test_subscription_manager_integration.py	订阅管理器集成测试
test_box_order.py	箱体订单测试
test_resonance_order.py	共振订单测试
test_shard_acceptance.py	分片验收测试
test_l1_l2_pipeline.py	L1/L2参数管道测试
test_param_pool_alignment.py	参数池对齐测试
test_option_sort_alignment.py	期权排序对齐测试
test_crack_validation.py	裂缝验证测试
test_governance_greeks.py	治理Greeks测试
test_v7_meta_audit_v2.py	V7元审计v2测试
test_v71_task_scheduler.py	V7.1任务调度器测试
test_kline_fix.py	K线修复测试
test_kline_length_backtest.py	K线长度回测测试
test_arrow_optimization.py	Arrow优化测试
test_duckdb_fix.py	DuckDB修复测试
test_safety_meta_layer_patches.py	安全元层补丁测试
test_infrastructure.py	基础设施测试
test_r15_p0_fixes.py	R15 P0修复测试
test_r18_full_chain.py	R18全链路测试
test_order_service_resilience.py	订单服务容错测试
test_hft_order.py	HFT订单测试
test_strategy_2026.py	2026策略测试
test_spring_order.py	弹簧策略订单测试
test_manual_verification.py	手动验证测试
3.5 参数池/ — 参数优化池 (22个.py文件)
文件	功能
__init__.py	包初始化
task_scheduler.py	量化任务调度系统(参数网格扫描+多进程回测+V7两轮优化)
cycle_resonance_module.py	周期共振模块(四变量输出:方向偏置+共振强度+相位+能量)
l2_optimizer.py	L2优化器(多进程网格搜索)
delay_time_sharpe_3d.py	延迟-时间参数-夏普三维映射表
optuna_multiobjective_search.py	Optuna多目标贝叶斯优化(26维混合空间+P0绿灯)
enhanced_phase_scan.py	增强版三阶段全参数扫描+物理约束裁剪+耦合验证
preprocess_ticks.py	数据预处理(Tick->分钟Bar+衍生指标)
sensitivity_analysis.py	参数敏感性分析(逐一±N%扰动)
statistical_validation.py	统计验证
advanced_validation.py	高级验证
test_design_suite.py	测试设计套件
run_verification.py	运行验证
L1参数量化/__init__.py	L1参数量化子包初始化
L1参数量化/test_l1_l2_pipeline.py	L1/L2管道测试
L1参数量化/v7_meta_audit_v2.py	V7元审计v2
L1参数量化/bayesian_shrinkage_life_estimator.py	贝叶斯收缩生命估计器
L1参数量化/soft_constrained_optimizer.py	软约束优化器
L1参数量化/external_validation_pipeline.py	外部验证管道
L1参数量化/performance_tier_manager.py	性能分层管理器
L1参数量化/triple_truth_anchor.py	三重真值锚定
L1参数量化/duckdb_tick_storage.py	DuckDB Tick存储
注意: param_pool/ 目录与 参数池/ 目录内容完全相同（22个.py文件一一对应），属于中英文目录名重复。

3.6 配置文件
文件	格式	功能
config/params_default.json	JSON	默认参数配置
config/cascade_config.yaml	YAML	瀑布评判配置
config/judgment_scoring_config.yaml	YAML	评判评分配置
config/cascade_threshold_grid.yaml	YAML	瀑布阈值网格
参数池/tvf_params.yaml	YAML	TVF六维参数配置
参数池/plr_thresholds.yaml	YAML	PLR阈值配置
参数池/parameter_attribute_matrix.yaml	YAML	参数属性矩阵
参数池/state_param_sets.yaml	YAML	状态参数集
param_pool/tvf_params.yaml	YAML	(同参数池，重复)
param_pool/plr_thresholds.yaml	YAML	(同参数池，重复)
param_pool/parameter_attribute_matrix.yaml	YAML	(同参数池，重复)
param_pool/state_param_sets.yaml	YAML	(同参数池，重复)
3.7 文档文件 (100+个.md文件)
核心文档（根目录）：

文件	功能
修改必看（模块功能分布）.md	模块功能分布说明
FUNCTION_DOC_TOP20.md	Top20函数文档
HFT_Optimization_Report_v8.md	HFT优化报告v8
HFT_P0_BugFix_Report.md	HFT P0 Bug修复报告
核查/审计报告（50+份，按R12-R20轮次编号）：

R12: Pandas陷阱/数值精度/环境兼容性审计报告
R13: 日志可观测性/业务逻辑漏洞/API契约/死路径审计报告
R14: 依赖管理/文档一致性/测试覆盖/配置管理审计报告
R15: 安全/容错弹性/性能延迟/数据质量审计报告
R16: 参数一致性/信号链路/跨系统一致性/策略生命周期审计报告
R17: 微观结构/合规/灾难恢复/金融模型审计报告
R18: 数据流图/运维就绪度/升级迁移/不变量守卫审计报告
R19: 设计反模式/策略归因/统计有效性/代码复杂度审计报告
R20: 回测保真度审计报告
四、文件统计汇总
类别	数量
根目录.py文件	91
evaluation/ .py文件	8
tests/ .py文件	33
策略评判/ .py文件	10
参数池/ .py文件	22
param_pool/ .py文件	22 (与参数池重复)
全部.py文件(去重)	约176 (含param_pool/参数池重复约22个)
.md文件	100+
.yaml文件	11
.json文件	13
.txt文件	10
五、三系统关键发现
生产系统: 核心链路为 main.py -> ProductionQuantSystem -> strategy_core_service -> strategy_ecosystem -> signal_service/risk_service/order_service，架构清晰。

测试系统: 33个测试文件，覆盖策略核心、风控、订单、参数对齐、全链路等维度，使用pytest框架。

评判系统: 分两层部署：

evaluation/ — 通用评判组件（瀑布引擎、参数漂移检测、违规追踪、驱逐策略）
策略评判/ — 策略专用评判（评判引擎、行为诊断、拐点显微镜、共振拐点标记、回测集成钩子）
参数池重复: 参数池/ 和 param_pool/ 两个目录内容完全相同（22个.py文件一一对应），属于中英文命名重复，需确认哪个为权威源。
