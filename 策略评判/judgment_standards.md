# 策略评判标准 — 顶级量化基金视角

## 一、核心哲学

**从"结果验证"推进到"过程验证"**

| 维度 | 结果验证（传统） | 过程验证（本系统） |
|------|----------------|-------------------|
| 回答的问题 | 策略赚不赚钱？ | 策略为什么赚钱/亏钱？是否在正确的时间做正确的事？ |
| 评判依据 | 夏普比率、最大回撤、胜率 | 高低点处信号演化、仓位调整、Greeks暴露 |
| 黑箱容忍度 | 高（回测漂亮即可） | **零容忍**（无法解释行为模式的策略不具备上线资格） |

> 顶级基金的PM在Review策略时，不会先看夏普比率，而是先问：
> "策略在上个月的5个局部顶底处，信号是怎么演化的？"

---

## 二、二维验证矩阵

```
                    局部高低点（日常）          全局极端（危机）
                   ┌─────────────────┬─────────────────┐
策略核心逻辑验证   │   ★ 本系统     │    次要关注     │
（信号/仓位/Greeks）│  微观行为诊断   │  风控系统接管   │
                   ├─────────────────┼─────────────────┤
策略生存能力验证   │    一般关注      │   ★ 压力测试    │
（回撤/流动性）    │  正常风控即可    │  尾部风险预算   │
                   └─────────────────┴─────────────────┘
```

- **局部高低点**：检验**策略核心逻辑**的显微镜
- **全局极端**：检验**风控系统**的炼狱
- **顶级策略必须同时通过两个维度的检验**

---

## 三、为什么"明显高低点（非极端行情）"是独特诊断窗口

| 维度 | 全局极端行情（如2020/3） | 局部明显高低点（日常波段顶底） |
|------|------------------------|------------------------------|
| 样本频率 | 极少（数年一遇） | 频繁（每周/每日多次） |
| 市场机制 | 流动性危机、相关性崩溃 | 正常博弈、多空平衡转换 |
| 策略行为 | 风控系统主导 | **策略核心逻辑主导** |
| 诊断价值 | 验证"会不会死" | 验证**"逻辑是否正确执行"** |

**核心洞察**：在局部高低点，策略处于**"决策压力最大"**的状态——趋势是否延续？反转是否成立？仓位该加仓还是减仓？此时策略监控数据的暴露模式，直接反映了其**核心逻辑的健康度**。

---

## 四、十一维度评判标准 (v1.3)

### 4.1 行为一致性 (权重22%)

**定义**：策略在局部高低点处行为与预设逻辑的一致程度

**评判规则**：
- 动量策略（high_freq/resonance）：
  - ✅ 健康：高点处信号衰减、仓位平滑收缩
  - ❌ 危险：高点处信号跳变、仓位剧烈震荡
- 均值回归策略（box/spring）：
  - ✅ 健康：高点处发出反向信号
  - ❌ 危险：高点处同向信号放大

**阈值**：≥0.70（阻塞项）

### 4.2 过程可解释性 (权重8%)

**定义**：策略行为能否被其逻辑模型解释

**评判规则**（v1.3升级为主动Explanation Coverage Ratio）：
- **主动ECR模式**（推荐）：提供`explanation_coverage_result`
  - 每个交易决策可追溯到规则触发链
  - Explanation Coverage Ratio = 可解释决策数 / 总决策数
  - 链深度质量：avg_chain_length / 3.0（深度≥3说明有完整推理链）
  - 规则多样性：unique_rules_triggered / 10.0（触发≥10条规则说明非硬编码）
  - score = 0.50×ECR + 0.25×链深度 + 0.25×规则多样性
- **被动模式**（fallback）：健康维度占总维度比例

**阈值**：≥0.65

### 4.3 统计显著性 (权重8%)

**定义**：诊断样本量是否足够，结论是否可复现

**评判规则**：
- 极值点样本量 ≥ 30 → 高置信度
- 极值点样本量 < 10 → 统计效力不足，结论无效
- 解决方案：降低时间维度（分钟级）、跨品种验证

**阈值**：≥0.70

### 4.4 风险预算遵守 (权重13%)

**定义**：策略是否遵守周期共振模块的风险曲面调节

**评判规则**：
- 仓位变化平滑，Greeks暴露可控 → 遵守
- 仓位剧烈震荡或Delta反常扩大 → 违反

**阈值**：≥0.70（阻塞项）

### 4.5 极端生存能力 (权重10%)

**定义**：策略在全局极端行情中是否存活，含回撤恢复时间约束

**评判规则**（v1.3升级集成recovery time）：
- 最大回撤 < 20% 且系统恢复 且 恢复时间 ≤ max_recovery_hours(默认24h) → 存活
- 最大回撤 > 30% → 不存活
- 恢复时间超限 → 恢复不达标，passed=False
- score = 0.70×生存分 + 0.30×恢复惩罚

**阈值**：≥0.60（阻塞项）

### 4.6 跨品种一致性 (权重5%)

**定义**：同一策略在不同品种上的行为一致性

**评判规则**：
- 行为一致性得分跨品种CV < 0.3 → 一致
- CV > 0.5 → 策略逻辑对品种过拟合

**阈值**：≥0.60

### 4.7 预测能力校准 (权重5%)

**定义**：周期共振模块拐点预测的命中率

**评判规则**：
- 命中率 > 0.4 且方向准确率 > 0.4 → 校准良好
- 命中率 < 0.2 → 共振模块对策略无预测价值

**阈值**：≥0.50

### 4.8 参数稳定性 (权重5%)

**定义**：策略在参数邻域内表现的连续性

**评判规则**：
- 参数高原度(plateau_score)高 → 稳定
- 收益分布偏度/峰度接近正态 → 稳定
- 参数敏感度低 → 稳定

**阈值**：≥0.50

### 4.9 盈利能力 (权重12%)

**定义**：策略的绝对盈利能力

**评判规则**：
- Sharpe/Calmar/WinRate/Profit-Loss Ratio综合评分

**阈值**：≥0.50

### 4.10 收益来源分散度 (权重7%) — v1.3新增

**定义**：策略收益是否来自单一信号源/单一市场状态/单一时间集中

**评判规则**：
- **信号源分散度**：基于Herfindahl-Hirschman Index (HHI)
  - HHI = Σ(signal_weight²)，1-HHI为分散度
  - 单一信号源贡献>70% → HHI>0.49 → 分散度<0.51 → 危险
- **市场状态分散度**：趋势/震荡/极端行情贡献的HHI
  - 仅在趋势中盈利 → 分散度低 → 策略脆弱
- **时间分散度**：1 - time_concentration
  - 集中度=1说明收益集中在单一时间窗口 → 危险
- score = 0.35×信号源分散 + 0.35×市场状态分散 + 0.30×时间分散

**阈值**：≥0.50

### 4.11 回撤恢复时间 (权重5%) — v1.3新增

**定义**：策略从回撤中恢复的速度约束

**评判规则**：
- **最大恢复时间**：max_recovery_hours ≤ 引擎硬性阈值(默认24h)
- **均值恢复时间**：mean_recovery_hours ≤ 12h(0.5×max)
- **恢复率**：成功恢复次数 / (成功+未恢复)次数
- score = 0.40×最大恢复分 + 0.30×均值恢复分 + 0.30×恢复率
- 恢复时间超限 → passed=False

**阈值**：≥0.50

---

## 五、评判结论

| 结论 | 含义 | 总分条件 | 附加条件 |
|------|------|---------|---------|
| **PASS** | 可上线策略 | ≥0.75 | 所有维度均通过阈值，无阻塞项 |
| **CONDITIONAL_PASS** | 有条件通过 | ≥0.60 | ≤2个维度未通过，无阻塞项 |
| **FAIL** | 不可上线（回测幻觉） | <0.60 | 或存在阻塞项未通过 |
| **INSUFFICIENT_EVIDENCE** | 证据不足 | — | 样本量<30或≥3项数据源缺失 |

---

## 六、方法论红线

### 6.1 禁止未来信息泄露 (Look-ahead Bias)

- ❌ 不能用"今天的收盘价"来定义"今天的高点"
- ✅ 只能用**截至当前Bar的实时数据**来判断极值区域
- ❌ 当前Bar的收盘价参与包含当前Bar的均线计算
- ✅ 当前Bar只参与下一Bar的均线

### 6.2 诊断工具 ≠ 优化目标

- ❌ 错误："因为策略在高低点处表现不好，所以调整参数让它在高低点处表现更好"
- ✅ 正确："策略在高低点处的行为与预设逻辑不一致，说明逻辑有漏洞，需要修复逻辑而非调整参数"

### 6.3 避免事后合理化 (Post-hoc Rationalization)

- 评判规则在回测前预定义，非事后拟合
- 不同策略类型有不同的预期行为（预设逻辑）
- 结论必须可审计、可复现

### 6.4 统计显著性底线

- 基于不足10个极值样本的诊断结论**统计效力很弱**
- 必须标注"低置信度"而非直接判定
- 解决方案：分钟级数据、跨品种验证、蒙特卡洛模拟

---

## 七、Tick→Bar增强标准

### 普通做法（有偏）

- 简单收盘价聚合
- 高低点取tick极值
- 均线基于收盘价计算

### 顶级做法（无偏/低偏）— 本系统实现

- **VWAP作为Bar代表价**，非简单收盘价
- **高低点记录时间戳和成交量分布**（高点是大单瞬间打出还是缓慢堆积？）
- **均线计算严格时间对齐**，无未来信息泄露
- **ATR标准化偏离度**（Price - MA) / ATR，而非简单差值

增强Bar字段：
```
EnhancedBar:
  OHLCV + vwap + high_time_offset_ms + low_time_offset_ms
  + volume_at_high + volume_at_low + bid_ask_spread_avg
  + extreme_region + price_vs_mas + ma_alignment
  + price_ma_deviation_sigma + ma_curvatures + atr14
```

---

## 八、市场快照标准

快照在以下时刻捕获：
1. **所有信号发生点**（开单策略触发信号时）
2. **所有开仓、持仓检查、平仓时刻**
3. **周线/月线级别高低点前后5天**
4. **极值区域进入时**

快照内容（策略池监测的所有信息）：
- 增强Bar数据
- 周期共振四变量 + 风险曲面调节
- 各策略信号强度/仓位/PnL/Greeks
- 五态分布
- 订单流指标
- HMM状态和后验概率
- 组合级PnL/回撤

存储：DuckDB列式存储，支持高效查询和聚合

---

## 九、与参数池对齐状态 (v1.4)

### 9.1 已正确对齐

| 对齐项 | 说明 |
|--------|------|
| CycleResonanceOutput四变量 | `directional_bias`/`resonance_strength`/`phase`/`state_entropy`与策略评判鸭子类型访问完全一致 |
| Phase枚举值 | 蓄力/释放/衰竭/混沌完全一致 |
| preprocess_ticks输出 | `minute`/`open`/`high`/`low`/`close`/`volume`/`vwap`等列名与`build_from_preprocessed_df()`期望匹配 |
| 六大策略类型 | `high_freq`/`resonance`/`box`/`spring`/`arbitrage`/`market_making`在WEIGHT_OVERRIDES/THRESHOLD_OVERRIDES/EXPECTED_LOGIC/SnapshotState/task_scheduler/CRModule中全覆盖 |
| ShadowAlphaState | `master_sharpe`/`alpha_ratio`/`degradation_active`/`absolute_ev_breached`与AlphaMetrics对齐 |
| EcosystemState | `active_strategy`/`capital`/`ev`与StrategyEcosystem对齐 |

### 9.2 已修复的对齐问题

| 问题 | 修复 |
|------|------|
| ShadowAlphaState.alpha_decline_pct命名不一致 | 重命名为`alpha_ratio_decline_pct`，与AlphaMetrics对齐 |
| BoxSpecificState字段名不对齐 | `box_profile_upper/lower`→`box_top/bottom`，`box_bounce_count`→`touch_count` |
| 死策略类型arbitrage/market_making | ~~删除~~ **已恢复**：arbitrage对应`MicrostructureArbitrageDetector`套利信号链，market_making对应`MarketMakerDefenseEngine`做市防御，均属已有策略模块 |
| ArbitrageSpecificState/MarketMakingSpecificState | 新增数据类：`deviation_bps`/`confidence`/`direction`/`is_opportunity`/`ttl_remaining_seconds`等字段，与`MicrostructureArbitrageDetector`输出对齐；`current_inventory`/`spread_bps`/`fill_count`/`ioc_count`/`rebalance_needed`等字段，与`MarketMakerDefenseEngine`状态对齐 |
| capture()签名缺arbitrage/market_making | **已补齐**：capture()/capture_signal_point()/capture_order_event()均支持arbitrage_state/market_making_state参数传递 |
| backtest_integration_hooks缺S5/S6状态 | **已补齐**：构造函数初始化+update_state()签名+所有capture调用点均传递arbitrage_state/market_making_state |
| 回撤开仓(pullback)三落地对齐 | **已落地(5策略通用+v2专业化)**：shared_utils.py通用PendingPullbackSignal+PullbackManager(v2: ref_mode/ATR动态等待/分方向回撤/Theta衰减/最小绝对回撤)；box_spring_strategy.py改用通用PullbackManager+命名统一`pullback_retrace_pct`；task_scheduler.py PULLBACK_DEFAULTS(10参数)/PULLBACK_GRID(7扫描维度)统一参数字典，5策略PARAM_DEFAULTS补`**PULLBACK_DEFAULTS`+PARAM_GRID补`**PULLBACK_GRID`(HFT不适用)；strategy_behavior_diagnosis.py 5策略reason均补pullback延迟描述；THRESHOLD_OVERRIDES补5策略behavior_consistency=0.60 |
| 未知strategy_type回退high_freq | 改为警告+宽松通用标准，而非静默使用高频逻辑 |
| ecosystem命名体系不同 | 新增`ECOSYSTEM_TO_JUDGMENT_TYPE_MAP`双向映射 |

### 9.3 参数池集成方式

参数池(task_scheduler.py)的回测函数**不直接导入**策略评判模块（避免循环依赖和169KB文件修改风险）。
集成通过`parameter_pool_adapter.py`适配器实现：

```python
from ali2026v3_trading.策略评判 import judge_backtest_result, judge_sweep_results

# 单个回测结果评判
result = run_backtest(params, bar_data)
report = judge_backtest_result("resonance", "rb888", "2025-01~06", result)

# 批量扫描结果评判
df = run_kline_length_sweep()
reports = judge_sweep_results("resonance", "rb888", "2025-01~06", df.to_dict('records'))
```

### 9.4 鸭子类型 vs 类型引用

策略评判对CycleResonanceOutput采用`Optional[Any]`+`hasattr()`鸭子类型访问，同时通过`TYPE_CHECKING`条件导入提供IDE提示：
- **运行时**：鸭子类型，零耦合，无循环导入风险
- **静态检查时**：`TYPE_CHECKING`导入`CycleResonanceOutput`，IDE/mypy可提示字段名

---

## 10. 三层瀑布式评判引擎（CascadeJudge）

### 10.1 定位与架构

三层瀑布式评判引擎(`evaluation/cascade_judge.py`)是**独立于十一维度评判**的硬门控体系，定位为"从结果到证据的法官审判"。

**与十一维度评判的关系**：
- 十一维度评判：**综合评分**体系，输出0~1分数，用于策略排序和横向比较
- CascadeJudge：**瀑布式否决**体系，输出PASS/BLOCK/WARN，用于生产准入硬门控
- 两者并行不替代：CascadeJudge先否决不合格策略，十一维度再对通过策略排序

**三系统落地位置**：
| 系统 | 集成位置 | 构造方式 |
|------|---------|---------|
| 策略评判系统 | `enhanced_phase_scan.py` p0_gate_check行330 / `optuna_multiobjective_search.py` p0_gate_check行246 | `CascadeJudge.from_config()` |
| 策略回测系统 | `task_scheduler.py` _run_final_checks行4053 | `CascadeJudge.from_config()` |
| 策略生产系统 | `strategy_ecosystem.py` check_absolute_ev_bottomline行736 | `CascadeJudge.from_config()` |

### 10.2 四关卡瀑布逻辑

```
第零关: 数据质量前置检查 → 第一关: 盈亏比验证(发动机) → 第二关: 三角验证(安全壳) → 第三关: 小资金约束(驾驶座)
```

| 关卡 | 名称 | 指标 | 阈值 | 行为 | 配置键 |
|------|------|------|------|------|--------|
| 0 | 数据质量检查 | `num_trades` | ≥30 | BLOCK | `data_quality.min_trades` |
| 1 | 盈亏比验证 | `profit_loss_ratio` | ≥1.8 | BLOCK | `gate1_profit_ratio.min` |
| 2a | 索提诺验证 | `sortino_ratio` | ≥1.5 | BLOCK | `gate2_sortino.min` |
| 2b | 卡玛验证 | `calmar_ratio` | ≥0.8 | BLOCK | `gate2_calmar.min` |
| 2c | 夏普验证 | `sharpe_ratio` | ≥1.2 | WARN | `gate2_sharpe.min` |
| 3a | 连续亏损约束 | `max_consecutive_losses` | ≤5 | BLOCK | `gate3_consecutive_losses.max` |
| 3b | 横盘天数约束 | `max_flat_period_days` | ≤20 | BLOCK | `gate3_flat_days.max` |
| 3c | 保证金约束 | `peak_margin_used` | ≤50% | BLOCK | `gate3_margin_ratio.max` |

任何关卡BLOCK即终止瀑布，后续关卡不再执行。

### 10.3 核心接口

```python
from evaluation.cascade_judge import CascadeJudge, adapt_backtest_result, BacktestMetrics

# 方式1: 默认参数构造
judge = CascadeJudge()

# 方式2: 从cascade_config.yaml加载（推荐，三系统统一）
judge = CascadeJudge.from_config()

# 适配回测结果
metrics = adapt_backtest_result(
    train_result,               # task_scheduler.run_backtest返回的dict
    test_result=None,           # 测试集回测结果(可选，启用过拟合检测)
    params=None,                # 策略参数dict(可选，提取max_risk_ratio等)
    sortino_est_mult=1.2,       # 索提诺兜底乘性系数
    sortino_est_add=0.3,        # 索提诺兜底加性系数
    peak_margin_factor=0.85,    # 保证金利用率因子
    overfit_ratio=1.5,          # 过拟合比例阈值
)

# 执行评判
report = judge.judge(metrics)
report.passed        # bool: 是否全部通过
report.final_score   # float: Sigmoid归一化综合得分[0,1]
report.gates         # List[GateReport]: 各关卡结果(含details中间值)
report.warnings      # List[str]: 警告列表(过拟合/夏普偏低)
report.fatal_reason  # Optional[str]: 否决原因
```

### 10.4 BacktestMetrics字段

| 字段 | 类型 | 说明 | 来源 |
|------|------|------|------|
| `profit_loss_ratio` | float | 盈亏比 | 直接取train_result |
| `sortino_ratio` | float | 索提诺比率 | 三级兜底: 直接值→returns序列计算→保守估计 |
| `calmar_ratio` | float | 卡玛比率 | 直接取train_result |
| `sharpe_ratio` | float | 夏普比率 | 直接取train_result |
| `max_consecutive_losses` | int | 最大连续亏损次数 | 直接取train_result |
| `max_flat_period_days` | int | 最大横盘天数 | nav_curve直接计算/无NAV时安全降级999 |
| `peak_margin_used` | float | 保证金峰值占比 | 直接取/兜底max_risk_ratio×0.85 |
| `num_trades` | int | 交易次数 | 取train_result["total_trades"] |
| `test_profit_loss_ratio` | float | 测试集盈亏比 | 取test_result["profit_loss_ratio"] |
| `overfit_flag` | bool | 过拟合标记 | 训练/测试盈亏比绝对比值>1.5时True |

### 10.5 索提诺推导三级兜底

1. **优先**: 回测引擎直接返回`sortino_ratio` → 直接使用
2. **次选**: 从`returns`序列计算下行标准差(分母用下行样本数，更保守)
3. **兜底**: `min(sharpe×1.2, sharpe+0.3)` — 期权卖方策略经验上界

### 10.6 横盘天数计算

```python
_compute_max_flat_from_nav(nav_curve, tolerance=0.001)
```
- 从净值曲线逐点判断是否创新高(超过peak×(1+tolerance))
- 返回连续未创新高的最大天数
- `tolerance=0.001`(0.1%): 日频适用; 分钟频可放宽至0.005
- 无nav_curve时安全降级为999(必然触发阻断或需显式放行)

### 10.7 综合评分

```
final = profit_ratio_weight × sigmoid(pr) + (1 - profit_ratio_weight) × tri_score
tri_score = tri_weights[sortino] × sigmoid(sortino) + tri_weights[calmar] × sigmoid(calmar) + tri_weights[sharpe] × sigmoid(sharpe)
```
- `tri_weights`内部归一化，和恒等于1.0
- Sigmoid参数从`cascade_config.yaml`读取，分两套用途：
  - `*_sort_center/scale`: 排序评分(score_metric/sigmoid_score)使用，中心点取经验最优值(如pr=2.0)
  - `*_tri_center/scale`: 三角验证门控(_calculate_score)使用，中心点=对应门控阈值(如pr=1.8)

### 10.8 与P0绿灯检验的关系

`task_scheduler.py`的`_run_final_checks()`中存在**双评判体系**：

| 维度 | P0绿灯检验(8项) | CascadeJudge(7关卡) |
|------|-----------------|---------------------|
| 目的 | 参数质量全面检验 | 生产准入硬门控 |
| 逻辑 | 全部并列检查 | 瀑布式短路否决 |
| 夏普阈值 | 训练≥0.5(最低可交易) | 夏普≥1.2(仅WARN) |
| 索提诺 | 无 | ≥1.5(BLOCK) |
| 优先级 | CascadeJudge先执行(第0项)，P0绿灯后执行 | 否决时直接跳过后续P0检验 |

**执行顺序**: CascadeJudge(第0项) → 样本外衰减 → 训练夏普 → ... → 参数来源

### 10.9 配置文件

所有评判参数统一在`config/cascade_config.yaml`中定义，修改即同步更新三个系统。

**配置键命名规则**: `{指标}_{用途}_center/scale`
- 用途后缀: `sort`=排序评分, `tri`=三角验证门控
- 示例: `sharpe_sort_center`=1.5(排序), `sharpe_tri_center`=1.2(门控)

**估算系数节(estimation)**:
| 键 | 默认值 | 量化来源 |
|----|--------|---------|
| `sortino_est_mult` | 1.2 | 期权卖方策略夏普/索提诺经验比值上界 |
| `sortino_est_add` | 0.3 | 索提诺兜底加性修正 |
| `peak_margin_factor` | 0.85 | 回测统计: peak_margin/MaxRiskRatio中位数(N=1200) |
| `overfit_ratio` | 1.5 | 统计显著性: 2σ区间外 |
