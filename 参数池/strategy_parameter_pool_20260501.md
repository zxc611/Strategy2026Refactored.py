# 策略参数池复核与优化方案

日期：2026-05-01

## 1. 复核结论

本次复核针对上一版参数池方案，按当前实际代码链路重新校正。结论如下：

1. 上一版方案的大方向是对的，但混入了两类不同口径的参数：
   - 代码默认参数表中已定义、可直接批量覆盖的运行参数。
   - 仅存在于函数调用层或测试脚本层的实验变量。

2. `top_n` 不是统一默认参数表键，而是 [ali2026v3_trading/t_type_service.py](ali2026v3_trading/t_type_service.py) 中 `select_trading_targets`、`select_from_sort_bucket` 一类函数参数；因此它不能与 `option_width_min_threshold` 同样视为“配置表键”，应归入“测试注入变量”。

3. 当前主执行链已经显式实现“成交量加权择券”，核心在 [ali2026v3_trading/t_type_service.py](ali2026v3_trading/t_type_service.py)：
   - 同步虚值优先
   - 成交量优先
   - 虚值距离优先
   - Tick 新鲜度优先

4. 当前主执行链没有独立的“等权排序模式”。如果需要验证“等权排序”，应在测试层引入 `ranking_mode` 之类的实验变量做 A/B，对比当前生产排序与等权基线，而不是假设代码里已经有该模式。

5. 希腊字母模块 [ali2026v3_trading/greeks_calculator.py](ali2026v3_trading/greeks_calculator.py) 已经完整实现，但当前代码搜索结果显示，它更多是独立能力模块，尚未明确成为主交易路径里的强依赖步骤。因此希腊字母参数池应归为“条件启用池”，而不应与订阅、五态、风控参数混成一个基础测试池。

6. 专业测试不应把“工程稳定性参数”和“Alpha 选择参数”放进同一笛卡尔积。正确做法是拆成四层：
   - 链路稳定性池
   - 状态与择券池
   - 执行与风控池
   - 条件能力池（Greeks / 等权基线 / 压力测试）

## 2. 优化原则

1. 只把当前代码中真实被读取的参数纳入主参数池。
2. 只把统一默认参数表存在的键纳入“基础池”。
3. 函数级变量和实验模式单独列为“测试注入变量池”。
4. 优先使用分层测试和两两组合，不做全量笛卡尔积。
5. 先测链路正确性，再测择券有效性，最后测执行鲁棒性。

## 3. 参数池分层

### 3.1 A 层：基础运行参数池

这部分参数在 [ali2026v3_trading/config_service.py](ali2026v3_trading/config_service.py) 的 `DEFAULT_PARAM_TABLE` 中已有默认值，可直接进入批量配置表。

### 3.2 B 层：扩展运行参数池

这部分参数被业务代码读取，但不一定在默认参数表里显式声明，例如：

- `max_signals_per_window`
- `max_risk_ratio`
- `total_position_limit`
- `close_stop_loss_ratio`
- `max_hold_minutes`

它们可以进入测试配置，但建议在批量运行器里显式补齐默认值，避免回测与实盘行为不一致。

### 3.3 C 层：测试注入变量池

这部分不是统一参数表键，而是测试脚本或实验逻辑注入：

- `ranking_mode`
- `target_top_n`
- `month_selection_mode`
- `product_selection_mode`
- `candidate_cap_per_product`

### 3.4 D 层：条件能力池

只有在对应能力链路被显式启用时才纳入测试：

- Greeks 更新策略参数
- 事件总线压力参数
- Storage 队列与异步落盘压力参数

## 4. 基线配置

| 分类 | 参数 | 基线值 | 来源模块 | 说明 |
| --- | --- | --- | --- | --- |
| 历史数据 | auto_load_history | True | config_service | 启动时自动加载历史 K 线 |
| 历史数据 | history_minutes | 1440 | config_service | 基准用 1 天历史 |
| 历史数据 | kline_style | M1 | config_service | 基准使用 1 分钟线 |
| 订阅 | subscription_batch_size | 10 | config_service | 平台订阅批大小 |
| 订阅 | subscription_interval | 1 | config_service | 订阅批次间隔 |
| 订阅 | rate_limit_global_per_min | 60 | config_service / order_service | 全局下单限速基线 |
| 五态宽度 | option_width_month_count | 2 | config_service | 当月 + 次月 |
| 五态宽度 | option_width_min_threshold | 4.0 | config_service / t_type_service | 默认宽度阈值 |
| 风控 | signal_cooldown_sec | 0.0 | config_service / risk_service | 默认不加冷却 |
| 风控 | position_limit_max_ratio | 0.2 | config_service | 持仓占比基线 |
| 平仓 | close_max_hold_days | 3 | config_service | 隔夜持有上限 |
| 平仓 | close_take_profit_ratio | 1.5 | config_service / position_service | 止盈比例 |
| 时效 | signal_max_age_sec | 180 | config_service | 信号有效期 |

## 5. 详细参数池配置表

### 5.1 链路稳定性池

目标：验证初始化、预注册、订阅、历史 K 线加载、异步落盘链路是否稳定。

| 参数 | 层级 | 当前默认 | 推荐池值 | 建议档位数 | 作用链路 | 主要观察指标 |
| --- | --- | --- | --- | --- | --- | --- |
| auto_load_history | A | True | True, False | 2 | on_init -> historical | 初始化耗时、历史加载成功率 |
| load_history_options | A | True | True, False | 2 | historical options | 历史期权加载量、启动阻塞度 |
| history_minutes | A | 1440 | 240, 720, 1440, 2880 | 4 | historical | 首次可交易时间、K 线覆盖率 |
| kline_style | A | M1 | M1, M5, M15 | 3 | historical / kline | 信号颗粒度、K 线生成成功率 |
| history_load_batch_size | A | 200 | 50, 100, 200, 400 | 4 | historical batch | 批处理吞吐、超时率 |
| history_load_max_batch_size | A | 50 | 20, 50, 100 | 3 | historical chunk | 平台请求压力、单批失败率 |
| history_load_batch_delay_sec | A | 0.2 | 0.0, 0.1, 0.2, 0.5 | 4 | historical throttle | 平台限流触发率 |
| history_load_request_delay_sec | A | 0.1 | 0.0, 0.05, 0.1, 0.2 | 4 | historical request | 历史接口稳定性 |
| history_load_max_workers | A | 4 | 1, 2, 4, 8 | 4 | historical parallel | CPU 峰值、失败率、加载时延 |
| subscription_batch_size | A | 10 | 5, 10, 20 | 3 | subscription_manager | 订阅成功率、握手失败率 |
| subscription_interval | A | 1 | 0.5, 1, 2 | 3 | subscription_manager | 订阅节奏、平台响应稳定性 |
| subscription_fetch_count | A | 5 | 0, 5, 10 | 3 | subscribe fetch | 首次行情补齐能力 |

推荐组合方式：

1. 固定其余参数，仅对历史加载参数做单因子扫描。
2. 对 `subscription_batch_size` 和 `subscription_interval` 做 3 x 3 组合。
3. 对 `history_load_max_workers` 与 `history_load_batch_delay_sec` 做 4 x 4 组合。

### 5.2 状态与择券池

目标：验证五态稳定性、宽度筛选、同步虚值判定、成交量排序效果。

| 参数 | 层级 | 当前默认 | 推荐池值 | 建议档位数 | 作用链路 | 主要观察指标 |
| --- | --- | --- | --- | --- | --- | --- |
| option_width_month_count | A | 2 | 2, 3, 4 | 3 | width_strength | 宽度稳定性、候选数量 |
| option_width_min_threshold | A | 4.0 | 2.0, 4.0, 6.0, 8.0 | 4 | width filter | 候选通过率、信号密度 |
| option_sync_tolerance | A | 0.5 | 0.25, 0.5, 1.0 | 3 | sync 判定 | 五态分布、同步虚值计数 |
| option_sync_allow_flat | A | True | True, False | 2 | state continuity | 平价 Tick 稳定性 |
| ignore_otm_filter | A | False | False, True | 2 | candidate filter | OTM 约束对收益的贡献 |
| allow_minimal_signal | A | False | False, True | 2 | signal gate | 信号稀疏时的触发能力 |
| min_option_width | A | 1 | 1, 2, 3 | 3 | minimal gate | 低强度候选质量 |
| target_top_n | C | 函数默认 5 | 1, 3, 5 | 3 | select_trading_targets | 候选集中度、成交笔数 |
| candidate_cap_per_product | C | 无 | 1, 2, 3 | 3 | harness injection | 产品分散度 |

说明：

1. `target_top_n` 不是默认参数表键，必须由测试层注入。
2. 当前生产择券核心在 `select_otm_targets_by_volume()`，因此测试主线应围绕 `option_width_min_threshold`、`option_sync_tolerance` 和 `target_top_n`。

### 5.3 排序模式 A/B 池

目标：将当前生产排序与等权基线明确分开，识别排序贡献是否真实存在。

| 变量 | 层级 | 当前生产模式 | 推荐池值 | 建议档位数 | 实现位置 | 主要观察指标 |
| --- | --- | --- | --- | --- | --- | --- |
| ranking_mode | C | volume_weighted_sync_first | volume_weighted_sync_first, equal_weight_round_robin, equal_weight_random, width_only | 4 | harness injection | 胜率、PnL、集中度 |
| target_top_n | C | 5 | 1, 3, 5 | 3 | harness injection | 信号密度、成交笔数 |
| lots_mode | C | ranked_lots | ranked_lots, equal_lots | 2 | harness injection | 仓位集中度 |
| product_rank_weight | C | 2/1 | 2/1, 1/1, 3/1 | 3 | harness injection | 头部产品偏置 |

排序模式定义建议：

1. `volume_weighted_sync_first`：当前生产逻辑。
2. `equal_weight_round_robin`：同产品同月份同方向候选轮转分配，不看成交量。
3. `equal_weight_random`：同分组内随机等权采样，多次重复看稳定性。
4. `width_only`：只按宽度强度，不看成交量。

### 5.4 风控与执行池

目标：验证冷却、限频、仓位控制、止盈止损、超时追单对收益曲线和回撤的影响。

| 参数 | 层级 | 当前默认 | 推荐池值 | 建议档位数 | 作用链路 | 主要观察指标 |
| --- | --- | --- | --- | --- | --- | --- |
| signal_cooldown_sec | A | 0.0 | 0, 5, 15, 60 | 4 | risk_service | 信号重复率、过度交易 |
| max_signals_per_window | B | 10 | 3, 5, 10, 20 | 4 | risk_service | 窗口信号阻断率 |
| rate_limit_window_sec | A/B | 120 | 60, 120, 300 | 3 | risk_service | 窗口拥塞度 |
| rate_limit_global_per_min | A | 60 | 30, 60, 120 | 3 | order_service | 下单拒绝率 |
| position_limit_max_ratio | A | 0.2 | 0.1, 0.2, 0.3 | 3 | config / position gate | 暴露率、回撤 |
| max_risk_ratio | B | 0.8 | 0.5, 0.8, 1.0 | 3 | risk_service | 风控阻断率 |
| total_position_limit | B | 1000000 | 300000, 1000000, 3000000 | 3 | risk_service | 资金利用率 |
| close_take_profit_ratio | A | 1.5 | 1.2, 1.5, 2.0 | 3 | position_service | 止盈效率 |
| close_stop_loss_ratio | B | 0.5 | 0.3, 0.5, 0.8 | 3 | position_service | 止损敏感性 |
| close_max_hold_days | A | 3 | 1, 3, 5 | 3 | close policy | 隔夜收益/风险平衡 |
| close_overnight_loss_threshold | A | -0.5 | -1.0, -0.5, -0.2 | 3 | overnight close | 隔夜止损行为 |
| close_overnight_profit_threshold | A | 4.0 | 2.0, 4.0, 6.0 | 3 | overnight close | 利润保护能力 |
| close_max_chase_attempts | A | 5 | 1, 3, 5 | 3 | close chase | 成交回补率 |
| close_chase_interval_seconds | A | 2 | 1, 2, 5 | 3 | close chase | 追单成本 |
| max_hold_minutes | B | 120 | 30, 60, 120, 240 | 4 | time stop | 日内持仓时长分布 |
| signal_max_age_sec | A | 180 | 60, 180, 300 | 3 | stale signal filter | 过期信号成交率 |

### 5.5 工程压力池

目标：验证异步写盘、队列水位、事件总线背压和订阅重试在高流量场景下的稳定性。

| 参数 | 层级 | 当前默认 | 推荐池值 | 建议档位数 | 作用链路 | 主要观察指标 |
| --- | --- | --- | --- | --- | --- | --- |
| storage.batch_size | D | 5000 | 1000, 5000, 10000 | 3 | storage write | 落盘延迟、批写失败率 |
| storage.async_queue_size | D | 500000 | 100000, 500000, 1000000 | 3 | storage queue | 队列填充率峰值 |
| storage.retry_delay | D | 0.1 | 0.05, 0.1, 0.5 | 3 | storage retry | 锁冲突恢复速度 |
| subscription.max_retries | D | 3 | 1, 3, 5 | 3 | subscription_manager | 订阅恢复率 |
| subscription.handshake_timeout | D | 10.0 | 5, 10, 20 | 3 | handshake | 握手超时率 |
| subscription.retry_interval | D | 5.0 | 2, 5, 10 | 3 | retry worker | 重试密度 |
| event_bus.rate_limit | D | 100 | 50, 100, 200 | 3 | event_bus | 事件丢弃率 |
| event_bus.backpressure_limit | D | 5000 | 1000, 5000, 10000 | 3 | event_bus | 背压触发率 |

说明：

1. 压力池不与 Alpha 池联跑。
2. 压力池应固定择券逻辑，只评估工程鲁棒性。

### 5.6 Greeks 条件能力池

目标：只有在测试脚本明确将 Tick 接入 GreeksCalculator 时才启用，用于评估 Greeks 刷新频率与计算成本。

| 参数 | 层级 | 当前默认 | 推荐池值 | 建议档位数 | 作用链路 | 主要观察指标 |
| --- | --- | --- | --- | --- | --- | --- |
| greeks.time_interval_sec | D | 1.0 | 0.5, 1.0, 2.0 | 3 | Greeks update | Greeks 刷新率、CPU |
| greeks.price_change_pct | D | 0.5 | 0.2, 0.5, 1.0 | 3 | Greeks trigger | 价格触发灵敏度 |
| greeks.iv_change_pct | D | 5.0 | 2.0, 5.0, 10.0 | 3 | Greeks trigger | IV 更新频率 |
| greeks.min_tick_interval | D | 5 | 1, 5, 10 | 3 | Greeks trigger | 高频行情下计算压力 |
| greeks.risk_free_rate | D | 0.02 | 0.01, 0.02, 0.03 | 3 | Greeks calc | IV / Delta 偏移 |

注意：当前主策略代码搜索中未发现 GreeksCalculator 被主链显式调用，因此该池必须单独启用，不能默认与主交易池合并统计。

## 6. 推荐测试编排

### 6.1 第一阶段：冒烟池

目标：证明链路打通。

建议组合数：24 组。

固定：

- `ranking_mode = volume_weighted_sync_first`
- `target_top_n = 3`
- `option_width_min_threshold = 4.0`

变动：

- `auto_load_history`
- `history_minutes`
- `subscription_batch_size`
- `subscription_interval`

### 6.2 第二阶段：策略有效性池

目标：验证五态与排序是否贡献 Alpha。

建议组合数：48 组。

核心变量：

- `option_width_month_count`
- `option_width_min_threshold`
- `option_sync_tolerance`
- `ranking_mode`
- `target_top_n`
- `signal_cooldown_sec`

### 6.3 第三阶段：执行与风控池

目标：验证回撤控制和成交效率。

建议组合数：36 组。

核心变量：

- `position_limit_max_ratio`
- `max_risk_ratio`
- `close_take_profit_ratio`
- `close_stop_loss_ratio`
- `close_max_hold_days`
- `close_max_chase_attempts`

### 6.4 第四阶段：压力池

目标：验证高负载不丢链路。

建议组合数：18 组。

核心变量：

- `storage.batch_size`
- `storage.async_queue_size`
- `event_bus.rate_limit`
- `event_bus.backpressure_limit`
- `subscription.max_retries`

## 7. 最终推荐核心池

如果只保留一套最有价值的“核心参数池”，建议如下：

| 分组 | 参数 | 推荐池值 |
| --- | --- | --- |
| 五态宽度 | option_width_month_count | 2, 3, 4 |
| 五态宽度 | option_width_min_threshold | 2.0, 4.0, 6.0 |
| 五态宽度 | option_sync_tolerance | 0.25, 0.5, 1.0 |
| 排序实验 | ranking_mode | volume_weighted_sync_first, equal_weight_round_robin, width_only |
| 排序实验 | target_top_n | 1, 3, 5 |
| 风控 | signal_cooldown_sec | 0, 5, 15 |
| 风控 | max_signals_per_window | 5, 10, 20 |
| 风控 | position_limit_max_ratio | 0.1, 0.2, 0.3 |
| 风控 | max_risk_ratio | 0.5, 0.8, 1.0 |
| 平仓 | close_take_profit_ratio | 1.2, 1.5, 2.0 |
| 平仓 | close_stop_loss_ratio | 0.3, 0.5, 0.8 |
| 平仓 | close_max_hold_days | 1, 3, 5 |
| 历史数据 | history_minutes | 240, 1440 |
| 历史数据 | history_load_max_workers | 2, 4, 8 |

这套核心池不建议全量笛卡尔积，推荐使用：

1. 单因子扫描。
2. 两两组合。
3. 对 `ranking_mode` 做固定其它参数的 A/B。

## 8. 输出建议

最终批量回测结果建议统一输出以下指标：

1. 初始化耗时。
2. 预注册成功率。
3. 订阅成功率。
4. 首 Tick 到达时间。
5. 首 K 线到达时间。
6. 五态分布稳定度。
7. 候选数量与候选切换频率。
8. 风控阻断率。
9. 订单成功率与追单成功率。
10. 收益、回撤、Sharpe、换手率、持仓集中度。

## 9. 参数池设计依据

这份参数池不是凭经验拍脑袋罗列，而是基于四类依据收敛出来的。

### 9.1 代码可达性依据

只有满足以下条件的参数，才进入正式参数池：

1. 在当前代码中存在明确读取点。
2. 能映射到确定的行为链路。
3. 变更后能在回测或仿真结果中产生可观测差异。

例如：

- `option_width_min_threshold` 会直接影响 [ali2026v3_trading/t_type_service.py](ali2026v3_trading/t_type_service.py) 的宽度筛选结果。
- `signal_cooldown_sec` 会直接影响 [ali2026v3_trading/risk_service.py](ali2026v3_trading/risk_service.py) 的信号放行频率。
- `close_take_profit_ratio` 会直接影响 [ali2026v3_trading/position_service.py](ali2026v3_trading/position_service.py) 的止盈触发逻辑。

这保证了参数池中的每个变量都具备“代码真实生效”属性，而不是研究报告式的概念参数。

### 9.2 策略机理依据

参数池围绕当前策略的四个真实收益形成环节设计：

1. 候选生成：五态识别、同步虚值、宽度阈值。
2. 候选排序：当前生产逻辑偏向同步优先和成交量优先。
3. 交易放行：冷却、限频、风险比例、持仓上限。
4. 收益兑现：止盈、止损、追单、持有时长、隔夜处理。

也就是说，参数池不是按模块名平均分配，而是按 PnL 形成链路拆分。真正对收益和回撤有一阶影响的环节，才值得进入高优先级池。

### 9.3 工程约束依据

当前系统不是纯回测脚本，而是带订阅、事件总线、异步存储和平台限流约束的实盘式结构。因此参数池必须区分：

1. Alpha 参数。
2. 执行参数。
3. 稳定性参数。

如果把三者混在一起做全量组合，会出现两个问题：

1. 统计上无法判断收益提升来自择券还是来自更松的执行约束。
2. 工程上组合数爆炸，且很多组合根本不具备可比性。

### 9.4 统计识别依据

参数池的目标不是“把所有值都试一遍”，而是识别以下三类问题：

1. 哪些参数对结果高度敏感。
2. 哪些参数只在与别的参数联动时才有效。
3. 哪些参数对结果基本无贡献，应从生产可配置项中降级。

因此参数设计遵循“先粗后细”的识别思路：

1. 先用少量档位识别方向性。
2. 再在有效区间内做局部加密。
3. 最后只对关键参数做联动实验。

## 10. 当前排列组合够不够

结论是：

1. 对于第一轮专业复核，当前组合已经够用。
2. 对于最终生产定型，当前组合还不够。

原因不在于参数个数太少，而在于实验设计还停留在“分层穷举 + 局部组合”的阶段，尚未进入正式 DOE 优化阶段。

### 10.1 为什么说当前组合够用

当前方案已经覆盖了三类最关键问题：

1. 链路是否打通。
2. 五态和排序是否真的有 Alpha 贡献。
3. 风控与平仓是否只是在重塑收益曲线。

对策略研究早期来说，只要能把这三件事分开识别，参数池就是合格的。

### 10.2 为什么说当前组合还不够

当前方案还存在三个限制：

1. 大部分参数仍按离散档位人工设定，缺少基于历史分布和业务分位点的取值设计。
2. 主要依赖单因子和两两组合，对三阶以上交互作用识别不足。
3. 还没有引入样本切片一致性检验，无法判断某个优选参数是否只是时间段偶然最优。

例如：

- `option_width_min_threshold`
- `target_top_n`
- `signal_cooldown_sec`

这三个参数很可能存在明显交互。如果只做单因子扫描，很容易高估某个参数的独立贡献。

### 10.3 如何判断“组合数是否足够”

不是看组合总数，而是看是否满足以下判定标准：

1. 主效应是否已经稳定。
2. 关键二阶交互是否已经识别。
3. 样本外结果是否保持排序一致。
4. 最优区间是否而非单点最优已经出现。

如果满足下面四项，就可以认为当前阶段组合数够了：

1. 前 20% 优胜组合在不同交易日切片中重复出现。
2. 关键参数的收益曲线不是锯齿随机，而是呈区间稳定。
3. 更换排序基线后，优胜组合仍相对占优。
4. 增加更多组合后，排名前列结果不再明显漂移。

如果这四项做不到，就说明组合仍然不够，应该继续补实验，而不是急着固化参数。

## 11. 更专业的参数池设计方案

如果按更专业的量化研究标准，我建议把当前方案升级成“五层 DOE 参数池”。

### 11.1 第一层：业务分布定锚池

先不急着跑组合，而是先根据历史数据分布确定参数候选区间。

做法：

1. 用近 1 到 3 个月样本统计 `option_width`、候选数量、信号间隔、持仓时长分布。
2. 用分位数而不是拍脑袋给档位，例如使用 P20 / P50 / P80。
3. 把明显不在历史有效区间内的参数值直接剔除。

这样做的好处是参数池从一开始就更贴近真实市场状态。

### 11.2 第二层：筛选型正交池

对于主研究参数，不建议一开始做全量笛卡尔积，而应先做筛选设计。

推荐方法：

1. 采用 Plackett-Burman 或近似正交表做主效应筛选。
2. 先从 8 到 12 个关键参数里筛出真正敏感的 3 到 5 个。
3. 不敏感参数固定在基线值，不再进入后续高成本联动测试。

适合进入这一层的参数：

1. `option_width_min_threshold`
2. `option_sync_tolerance`
3. `target_top_n`
4. `signal_cooldown_sec`
5. `position_limit_max_ratio`
6. `close_take_profit_ratio`
7. `close_stop_loss_ratio`
8. `max_hold_minutes`

### 11.3 第三层：响应面精修池

对筛出来的关键参数，再做局部连续区间优化。

推荐方法：

1. 中心复合设计。
2. Box-Behnken 设计。
3. 贝叶斯优化作为补充，而不是替代。

这一步的目标不是找“单次最高收益点”，而是找：

1. 稳定收益区间。
2. 回撤可接受区间。
3. 对样本扰动不敏感的鲁棒区间。

### 11.4 第四层：样本切片稳健池

专业参数池不能只在整段样本上看一次结果，必须切片。

至少要做四类切片：

1. 时间切片：按周、按月、按主力切换前后。
2. 波动切片：高波动日、低波动日。
3. 流动性切片：高成交时段、低成交时段。
4. 合约切片：不同品种、不同月份、不同虚值深度。

最终保留的参数组必须在大多数切片下都不失真。

### 11.5 第五层：生产约束回归池

当研究层已经找到候选优值后，再把工程约束加回来做回归测试。

这里固定 Alpha 参数，只测试：

1. 订阅节奏变化是否改变结果。
2. 存储延迟和队列水位是否污染信号链路。
3. 事件总线背压是否造成择券偏差。

这一层的目标是验证“研究最优参数”在真实系统里仍成立，而不是在理想环境中才成立。

### 11.6 更专业方案的最终输出物

完整的专业参数池设计，不应只输出“最优参数表”，还应输出以下内容：

1. 参数字典：每个参数的业务含义、代码读取点、默认值、试验区间、剔除依据。
2. 试验矩阵：每轮 DOE 的组合表。
3. 敏感度报告：主效应、交互效应、边际收益曲线。
4. 稳健性报告：不同切片下的表现一致性。
5. 生产建议表：生产固定值、可调值、应废弃值。

如果做到这一步，参数池就不只是“测试参数列表”，而是完整的策略参数治理方案。

## 12. 正式 DOE 试验矩阵表

下面给出一版可以直接执行的正式 DOE 试验矩阵。设计目标有三个：

1. 控制总组合数，避免无意义笛卡尔积。
2. 第一轮保留主效应识别能力。
3. 第二轮开始只保留关键交互项和优胜区间。

### 12.1 DOE 设计原则

本矩阵采用“5 轮递进”设计：

1. R0：链路与基线确认。
2. R1：Alpha 筛选矩阵。
3. R2：关键交互矩阵。
4. R3：排序 A/B 确认矩阵。
5. R4：执行与风控矩阵。

总建议组合数：44 组。

这 44 组不是平均分配，而是按信息增益分配。真正决定收益方向的参数会被重复观察，工程类参数只做必要覆盖。

### 12.2 R0 基线与链路确认矩阵

目标：确认初始化、历史加载、订阅节奏三段链路无硬故障，并建立统一基线。

建议执行数：8 组。

| RunID | auto_load_history | history_minutes | subscription_batch_size | subscription_interval | history_load_max_workers | 目的 |
| --- | --- | --- | --- | --- | --- | --- |
| R0-01 | True | 1440 | 10 | 1.0 | 4 | 统一基线 |
| R0-02 | False | 1440 | 10 | 1.0 | 4 | 验证无历史加载链路 |
| R0-03 | True | 240 | 10 | 1.0 | 4 | 验证轻量历史窗口 |
| R0-04 | True | 2880 | 10 | 1.0 | 4 | 验证重历史窗口 |
| R0-05 | True | 1440 | 5 | 0.5 | 4 | 验证高频小批订阅 |
| R0-06 | True | 1440 | 20 | 2.0 | 4 | 验证低频大批订阅 |
| R0-07 | True | 1440 | 10 | 1.0 | 2 | 验证低并发历史加载 |
| R0-08 | True | 1440 | 10 | 1.0 | 8 | 验证高并发历史加载 |

进入下一轮前的通过标准：

1. 预注册成功率 >= 99%。
2. 订阅成功率 >= 99%。
3. 首 Tick / 首 K 线到达时间无结构性异常。
4. 无批量超时、无明显背压积压。

### 12.3 R1 Alpha 筛选矩阵

目标：在受控组合数下识别五态、排序、冷却三类主效应。

设计方法：6 因子两水平筛选矩阵，外加 2 组中心确认。

低水平定义：

- `option_width_min_threshold = 2.0`
- `option_sync_tolerance = 0.25`
- `ranking_mode = volume_weighted_sync_first`
- `target_top_n = 1`
- `signal_cooldown_sec = 0`
- `max_signals_per_window = 5`

高水平定义：

- `option_width_min_threshold = 6.0`
- `option_sync_tolerance = 1.0`
- `ranking_mode = width_only`
- `target_top_n = 5`
- `signal_cooldown_sec = 15`
- `max_signals_per_window = 20`

中心点定义：

- `option_width_min_threshold = 4.0`
- `option_sync_tolerance = 0.5`
- `ranking_mode = volume_weighted_sync_first`
- `target_top_n = 3`
- `signal_cooldown_sec = 5`
- `max_signals_per_window = 10`

建议执行数：12 组。

| RunID | width_threshold | sync_tolerance | ranking_mode | top_n | cooldown | max_signals | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| R1-01 | 2.0 | 0.25 | volume_weighted_sync_first | 1 | 0 | 5 | 全低位 |
| R1-02 | 2.0 | 0.25 | width_only | 5 | 15 | 20 | 排序与放行高位 |
| R1-03 | 2.0 | 1.0 | volume_weighted_sync_first | 5 | 15 | 5 | 宽松同步判定 |
| R1-04 | 2.0 | 1.0 | width_only | 1 | 0 | 20 | 宽松同步 + 宽度排序 |
| R1-05 | 6.0 | 0.25 | volume_weighted_sync_first | 5 | 0 | 20 | 严格宽度阈值 |
| R1-06 | 6.0 | 0.25 | width_only | 1 | 15 | 5 | 严格宽度 + 冷却 |
| R1-07 | 6.0 | 1.0 | volume_weighted_sync_first | 1 | 15 | 20 | 严格宽度 + 高容忍 |
| R1-08 | 6.0 | 1.0 | width_only | 5 | 0 | 5 | 高容忍 + 宽度排序 |
| R1-09 | 2.0 | 0.5 | volume_weighted_sync_first | 3 | 5 | 10 | 中心偏低 |
| R1-10 | 6.0 | 0.5 | volume_weighted_sync_first | 3 | 5 | 10 | 宽度主效应确认 |
| R1-11 | 4.0 | 0.25 | volume_weighted_sync_first | 3 | 5 | 10 | 同步容忍主效应确认 |
| R1-12 | 4.0 | 0.5 | volume_weighted_sync_first | 3 | 5 | 10 | 中心基线重复 |

R1 输出：

1. 主效应排序表。
2. 前 20% 优胜组合。
3. 需要进入 R2 的关键交互候选。

### 12.4 R2 关键交互矩阵

目标：确认最可能决定 Alpha 的三项交互是否真实存在。

本轮只保留三项关键因子：

1. `option_width_min_threshold`
2. `target_top_n`
3. `signal_cooldown_sec`

固定：

1. `ranking_mode = volume_weighted_sync_first`
2. `option_sync_tolerance = 0.5`
3. `max_signals_per_window = 10`

建议执行数：9 组，采用 3 水平 L9 矩阵。

| RunID | width_threshold | top_n | cooldown | 主要回答问题 |
| --- | --- | --- | --- | --- |
| R2-01 | 2.0 | 1 | 0 | 极度集中、无冷却 |
| R2-02 | 2.0 | 3 | 5 | 宽松阈值下的平衡态 |
| R2-03 | 2.0 | 5 | 15 | 宽松阈值下的分散高冷却 |
| R2-04 | 4.0 | 1 | 5 | 基线阈值下的集中交易 |
| R2-05 | 4.0 | 3 | 15 | 基线阈值下的保守放行 |
| R2-06 | 4.0 | 5 | 0 | 基线阈值下的高扩张 |
| R2-07 | 6.0 | 1 | 15 | 严格筛选 + 强冷却 |
| R2-08 | 6.0 | 3 | 0 | 严格筛选 + 快速响应 |
| R2-09 | 6.0 | 5 | 5 | 严格筛选 + 分散候选 |

R2 的判断重点不是绝对收益，而是看三件事：

1. 严格阈值是否需要更低 TopN 才能有效。
2. 宽松阈值是否必须依赖冷却来抑制噪声。
3. TopN 的提升是否只是换手变高，而非收益质量变好。

### 12.5 R3 排序 A/B 确认矩阵

目标：确认当前生产排序是否真实优于等权基线，而不是样本偶然占优。

本轮只测排序，不再混入过多风控变量。

固定：

1. `signal_cooldown_sec = 5`
2. `max_signals_per_window = 10`
3. `option_sync_tolerance = 0.5`

建议执行数：6 组。

| RunID | ranking_mode | width_threshold | top_n | lots_mode | 主要回答问题 |
| --- | --- | --- | --- | --- | --- |
| R3-01 | volume_weighted_sync_first | 4.0 | 3 | ranked_lots | 生产基线 |
| R3-02 | equal_weight_round_robin | 4.0 | 3 | equal_lots | 等权轮转基线 |
| R3-03 | width_only | 4.0 | 3 | ranked_lots | 仅宽度排序 |
| R3-04 | volume_weighted_sync_first | 2.0 | 5 | ranked_lots | 宽松筛选环境 |
| R3-05 | equal_weight_round_robin | 2.0 | 5 | equal_lots | 宽松环境下等权 |
| R3-06 | volume_weighted_sync_first | 6.0 | 1 | ranked_lots | 严格筛选环境 |

若 R3 结果显示生产排序没有持续领先，则后续不应再把成交量优先当作默认真理，而应回退到并行保留模式。

### 12.6 R4 执行与风控矩阵

目标：在 Alpha 参数固定后，单独识别收益曲线塑形参数。

固定 Alpha 参数：

1. `ranking_mode = R3 优胜模式`
2. `option_width_min_threshold = R2 优胜区间中位值`
3. `target_top_n = R2 优胜区间中位值`

建议执行数：9 组，采用 4 因子 3 水平近似正交矩阵。

| RunID | position_limit_ratio | take_profit | stop_loss | max_hold_minutes | 主要回答问题 |
| --- | --- | --- | --- | --- | --- |
| R4-01 | 0.1 | 1.2 | 0.3 | 30 | 极保守 |
| R4-02 | 0.1 | 1.5 | 0.5 | 120 | 低暴露基线 |
| R4-03 | 0.1 | 2.0 | 0.8 | 240 | 低暴露长持有 |
| R4-04 | 0.2 | 1.2 | 0.5 | 240 | 基线暴露 + 快止盈 |
| R4-05 | 0.2 | 1.5 | 0.8 | 30 | 基线暴露 + 快平仓 |
| R4-06 | 0.2 | 2.0 | 0.3 | 120 | 基线暴露 + 大盈亏比 |
| R4-07 | 0.3 | 1.2 | 0.8 | 120 | 高暴露防亏 |
| R4-08 | 0.3 | 1.5 | 0.3 | 240 | 高暴露趋势持有 |
| R4-09 | 0.3 | 2.0 | 0.5 | 30 | 高暴露快周转 |

### 12.7 DOE 总体执行顺序

推荐实际顺序如下：

1. R0 全跑。
2. R1 全跑。
3. R2 只在 R1 前 20% 优胜区间基础上跑。
4. R3 只在 R2 最优区间附近跑。
5. R4 固定 Alpha 后再跑。

这样总组合数仍控制在 44 组，但主效应、排序 A/B 和关键交互都能保留。

## 13. Walk-forward 优胜区间筛选流程

DOE 找到的不是最终参数，而只是“候选优胜区间”。真正要筛掉伪优解，必须进入 walk-forward。

### 13.1 Walk-forward 总体框架

推荐使用固定长度滚动窗口：

1. 训练窗：20 个交易日。
2. 验证窗：5 个交易日。
3. 前移步长：5 个交易日。
4. 最少滚动次数：6 个窗口。

这意味着任何候选参数组都必须至少经过 6 次独立的样本外考验。

### 13.2 候选优胜区间生成规则

只有满足下面条件的参数区间，才进入 walk-forward：

1. 在 DOE 中进入前 20% 表现分组。
2. 不只是单点最优，而是相邻档位也表现稳定。
3. 排序模式切换后不完全失真。
4. 没有造成显著工程退化，例如拒单率或背压显著上升。

### 13.3 Walk-forward 执行表

| WFID | 训练窗 | 验证窗 | 操作 |
| --- | --- | --- | --- |
| WF-01 | T1-T20 | T21-T25 | 从 DOE 前 20% 中选前 5 组入围 |
| WF-02 | T6-T25 | T26-T30 | 观察入围组是否重复出现 |
| WF-03 | T11-T30 | T31-T35 | 检查排名稳定性 |
| WF-04 | T16-T35 | T36-T40 | 检查收益/回撤同步性 |
| WF-05 | T21-T40 | T41-T45 | 检查 regime 变化适应性 |
| WF-06 | T26-T45 | T46-T50 | 形成最终上线候选 |

说明：

1. `T1-T20` 等表示连续交易日窗，不要求必须对应自然月。
2. 每个验证窗只允许使用该训练窗内筛出来的参数组，禁止回看未来数据。

### 13.4 伪优解淘汰规则

满足以下任一条件，即判定为伪优解并淘汰：

1. 只在单一窗口排名靠前，6 窗中入选次数 < 2。
2. 训练窗显著领先，但验证窗收益由正转负且回撤显著扩大。
3. 验证窗 Sharpe 排名明显跌出前 50%。
4. 参数最优点漂移过大，相邻窗口无法收敛到同一区间。
5. 收益改善主要依赖成交次数暴增，而非单位风险收益改善。

### 13.5 参数区间升级规则

只有满足以下四项的参数区间，才能进入“生产候选区间”：

1. 6 个窗口中至少 4 个窗口验证收益为正。
2. 验证窗最大回撤不劣于基线超过 20%。
3. 6 窗中位数 Sharpe 高于基线。
4. 参数漂移不超过一个离散档位。

如果某个参数组合满足收益高，但参数漂移过大，则只能保留为“研究候选”，不能直接上线。

### 13.6 Walk-forward 最终输出

walk-forward 阶段最终应输出三类结果：

1. 生产候选区间：可进入灰度或仿真长期观察。
2. 研究保留区间：有潜力但稳定性不足。
3. 剔除区间：只在单窗口有效的伪优解。

## 14. 专业版参数治理表

### 14.1 剔除标准编码

为了让治理表可执行，统一使用以下剔除编码：

1. `E1`：代码无稳定读取点或只能偶发触发。
2. `E2`：DOE 主效应不稳定，方向反复。
3. `E3`：walk-forward 6 窗入选次数 < 2。
4. `E4`：收益改善小，但回撤、拒单率或拥塞明显恶化。
5. `E5`：最优点高度尖峰化，相邻档位无法形成稳定区间。
6. `E6`：只能在测试注入层存在，不宜直接进入生产配置。

### 14.2 治理表

| 参数 | 分类 | 参数定义 | 代码读取点 | 默认/基线 | 试验区间 | 剔除标准 | 上线建议 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| history_minutes | A | 启动历史 K 线回看长度 | strategy_historical.py: `_get_param_value(..., 'history_minutes')` | 1440 | 240, 720, 1440, 2880 | E2, E4 | 保留为生产可调，建议上线前收敛到 2 档 |
| history_load_max_workers | A | 历史加载并发度 | strategy_historical.py: `history_load_max_workers` | 4 | 1, 2, 4, 8 | E4 | 保留为工程可调，不进 Alpha 优化结论 |
| subscription_batch_size | A | 单批订阅大小 | config_service.py / subscription_manager.py | 10 | 5, 10, 20 | E4 | 固定在平台稳定区间，默认不与 Alpha 联调 |
| subscription_interval | A | 订阅批间隔 | config_service.py / subscription_manager.py | 1 | 0.5, 1, 2 | E4 | 固定为工程参数，只做稳定性巡检 |
| option_width_month_count | A | 纳入宽度计算的月份数量 | config_service.py / t_type_service.py 宽度链路 | 2 | 2, 3, 4 | E2, E3 | 保留为研究可调，生产建议不超过 3 |
| option_width_min_threshold | A | 宽度筛选阈值 | t_type_service.py: 宽度筛选与 `select_otm_targets_by_volume` | 4.0 | 2.0, 4.0, 6.0, 8.0 | E2, E3, E5 | 核心 Alpha 参数，必须进入 DOE 与 WFO |
| option_sync_tolerance | A | 同步虚值判定容忍度 | t_type_service.py: 同步判定链路 | 0.5 | 0.25, 0.5, 1.0 | E2, E3 | 核心 Alpha 参数，建议保留 3 档研究 |
| ranking_mode | C | 候选排序模式 | harness 注入，映射到 t_type_service.py 选择逻辑 | 生产为 volume_weighted_sync_first | volume_weighted_sync_first, equal_weight_round_robin, width_only | E3, E6 | 作为研究开关保留，生产只允许白名单模式 |
| target_top_n | C | 候选截断数量 | harness 注入，对应 `select_trading_targets` 参数层 | 5 | 1, 3, 5 | E2, E3, E6 | 保留为测试注入变量，不建议直接放入统一参数表 |
| signal_cooldown_sec | A | 同标的信号冷却时间 | risk_service.py: `_get_signal_cooldown` | 0.0 | 0, 5, 15, 60 | E2, E3 | 核心交易节奏参数，建议上线只保留 2 到 3 档 |
| max_signals_per_window | B | 窗口内信号数量上限 | risk_service.py: `_get_max_signals_per_window` | 10 | 3, 5, 10, 20 | E2, E4 | 保留为风控可调，必须与冷却参数联测 |
| rate_limit_window_sec | B | 风控计数窗口长度 | risk_service.py: `_get_rate_limit_window` | 60 或 120 | 60, 120, 300 | E4 | 归入执行风控域，不纳入主 Alpha 结论 |
| position_limit_max_ratio | A | 单策略持仓暴露比例上限 | config_service.py / 风控持仓门控 | 0.2 | 0.1, 0.2, 0.3 | E3, E4 | 生产可调，但应与回撤一起定型 |
| max_risk_ratio | B | 风险比率上限 | risk_service.py: `_get_max_risk_ratio` | 0.8 | 0.5, 0.8, 1.0 | E4 | 保留为风控硬约束，不追求收益最优 |
| total_position_limit | B | 总名义持仓上限 | risk_service.py: `_get_total_position_limit` | 1000000 | 300000, 1000000, 3000000 | E4 | 只做容量管理，不参与 Alpha 归因 |
| rate_limit_global_per_min | A | 每分钟全局下单上限 | order_service.py: `_get_rate_limit` | 60 | 30, 60, 120 | E4 | 固定在平台与成交效率平衡点 |
| close_take_profit_ratio | A | 止盈阈值 | position_service.py: `ps.get_float('close_take_profit_ratio', 1.5)` | 1.5 | 1.2, 1.5, 2.0 | E2, E3 | 核心收益兑现参数，需进入 R4 与 WFO |
| close_stop_loss_ratio | B | 止损阈值 | position_service.py: `ps.get_float('close_stop_loss_ratio', 0.5)` | 0.5 | 0.3, 0.5, 0.8 | E2, E4 | 保留为风险参数，优先控制尾部损失 |
| close_max_hold_days | A | 隔夜最长持有天数 | config_service.py / 平仓策略链路 | 3 | 1, 3, 5 | E2, E3 | 生产可调，但必须配合隔夜阈值联测 |
| max_hold_minutes | B | 日内时间止损上限 | position_service.py: `ps.get_int('max_hold_minutes', 120)` | 120 | 30, 60, 120, 240 | E2, E3 | 核心持有时长参数，建议进入 R4 |
| close_max_chase_attempts | A | 平仓追单最大次数 | config_service.py / close chase 链路 | 5 | 1, 3, 5 | E4 | 工程执行参数，单独定型，不纳入 Alpha 归因 |

### 14.3 上线治理规则

建议把最终参数治理分成四类：

1. `生产固定值`：例如工程稳定性已经确定，后续只巡检不调优。
2. `生产可调值`：允许在白名单区间内微调。
3. `研究保留值`：保留在实验池，但默认不上线。
4. `测试注入值`：只允许出现在回测或仿真脚本，不进入统一配置表。

对于当前策略，最推荐的治理边界是：

1. `ranking_mode`、`target_top_n` 留在测试注入层。
2. `option_width_min_threshold`、`option_sync_tolerance`、`signal_cooldown_sec` 进入研究主池。
3. `subscription_*`、`history_load_*`、`close_max_chase_attempts` 归入工程治理池。
4. `close_take_profit_ratio`、`close_stop_loss_ratio`、`max_hold_minutes` 归入执行风控池。

## 15. 结论

本次优化后的参数池与上一版相比，主要修正了三点：

1. 不再把函数级参数误写成统一配置参数。
2. 不再把 Greeks 和工程压力参数混入主策略 Alpha 池。
3. 明确把“等权排序”定义为测试基线，而不是现有生产逻辑。

按照这份表执行，可以更准确地区分：

- 是五态判定在贡献收益。
- 还是排序方式在贡献收益。
- 还是风控/执行仅仅在改变收益波动形态。