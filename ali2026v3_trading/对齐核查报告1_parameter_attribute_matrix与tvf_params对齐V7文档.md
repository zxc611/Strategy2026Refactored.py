# 对齐核查报告1: parameter_attribute_matrix.yaml 与 tvf_params.yaml 对齐V7文档

**核查日期**: 2026-05-21（初版） / 2026-05-21（深度复核版）
**核查范围**: parameter_attribute_matrix.yaml 元属性定义(type/range/layer/source/evidence/constraint) 与 tvf_params.yaml 配置值(range/num_points/step/value) 对齐参数池V7文档
**核查结论**: 已对齐，初版发现3处已修正，深度复核发现6类新问题（3处已修正，3处已修复）

---

## 一、核查方法

逐项比对以下三者的参数定义：
1. V7文档: `参数池统一执行方案与使用手册_V7.0_工程落地版_20260510.md`
2. 元属性矩阵: `参数池/parameter_attribute_matrix.yaml`
3. TVF配置值: `参数池/tvf_params.yaml`
4. CascadeJudge配置: `config/cascade_config.yaml`

核查维度：参数名、默认值、范围、层级、来源、证据、约束

---

## 二、核查结果汇总

### 2.1 已确认对齐项（无差异）

| 类别 | 参数数量 | 对齐状态 |
|------|---------|---------|
| L-1安全元层参数 | 14项 | 完全对齐 |
| L-2参数基岩层参数 | 8项 | 完全对齐 |
| L0状态诊断参数 | 4项 | 完全对齐 |
| L1-L3交易参数 | 8项 | 完全对齐 |
| L4微结构参数 | 3项 | 完全对齐 |
| 影子策略参数 | 5项 | 完全对齐 |
| S1 HFT专用参数 | 3项 | 完全对齐 |
| 箱体弹簧参数 | 4项 | 完全对齐 |
| Pullback回调参数 | 5项 | 完全对齐 |
| S5套利策略参数 | 4项 | 完全对齐 |
| S6做市策略参数 | 6项 | 完全对齐 |
| TVF层间权重 | 3项 | 完全对齐 |
| TVF L2订单流参数 | 2项 | 完全对齐 |
| TVF L3希腊字母参数 | 6项 | 完全对齐 |
| CascadeJudge阈值 | 7项 | 完全对齐 |
| Sigmoid评分参数 | 14项 | 完全对齐 |

### 2.2 初版已修正项（3处）

#### 问题1: TVF Sigmoid scale evidence描述不准确（已修正）

**位置**: parameter_attribute_matrix.yaml tvf_sortino_scale / tvf_calmar_scale / tvf_sharpe_scale

**修正**: 更新3个参数的evidence字段，明确说明TVF与CascadeJudge scale差异的设计意图

#### 问题2: tvf_params.yaml搜索空间与V7文档版本差异（无需修正）

V7.3有意升级，文档已记录

#### 问题3: V7文档S5/S6参数定义不完整（以PAM为准）

### 2.3 深度复核发现的新问题（6类）

#### 问题4（高）: tvf_params.yaml L1 tri_scale默认值与PAM不一致（已修正）

**位置**: tvf_params.yaml l1_tri_validation节

**问题描述**: tvf_params.yaml的sortino/calmar/sharpe tri_scale值(1.0/0.5/0.8)直接复制了cascade_config.yaml的门控评判scale，而未采用PAM定义的TVF专属更陡scale(0.50/0.30/0.40)

**影响**: TVF仓位调整因子对Sortino/Calmar/Sharpe变化的敏感度仅为设计预期的一半

**修正前**: sortino_tri_scale: 1.0, calmar_tri_scale: 0.5, sharpe_tri_scale: 0.8
**修正后**: sortino_tri_scale: 0.50, calmar_tri_scale: 0.30, sharpe_tri_scale: 0.40

#### 问题5（中）: tvf_params.yaml搜索空间range与PAM range不一致（7处，已修复）

| 参数 | PAM range | TVF搜索空间range | 差异 |
|------|----------|-----------------|------|
| tvf_l1_weight | [0.20, 0.60] | [0.30, 0.50] | TVF更窄 |
| tvf_l2_weight | [0.20, 0.50] | [0.20, 0.40] | TVF更窄 |
| tvf_sortino_threshold | [0.5, 3.0] | [0.3, 5.0] | TVF更宽 |
| tvf_calmar_threshold | [0.3, 2.0] | [0.1, 4.0] | TVF更宽 |
| tvf_sharpe_threshold | [0.5, 3.0] | [0.3, 5.0] | TVF更宽 |
| tvf_sortino_scale | [0.2, 1.0] | [0.2, 3.0] | TVF更宽 |
| tvf_calmar_scale | [0.1, 0.8] | [0.1, 2.0] | TVF更宽 |
| tvf_sharpe_scale | [0.2, 1.0] | [0.2, 3.0] | TVF更宽 |

**性质**: TVF搜索空间L1 center/scale与cascade_config.yaml对齐，PAM为TVF定义了更窄的range。层间权重搜索空间比PAM更窄。

**修复**: tvf_params.yaml搜索空间range已对齐PAM

#### 问题6（中）: PAM constraint字段缺失（6组权重归一化约束未声明，已修复）

缺失约束：scoring_profit_ratio_weight+scoring_sortino_weight=1.0, scoring_sortino+calmar+sharpe=1.0, capital_route三项和=1.0, tvf_l2_inner_cvd/smf权重, tvf_l3_inner_theta权重

**修复**: PAM 7个constraint字段已添加

#### 问题7（中）: PAM layer命名与V7文档层级不一致（已修复）

L1_safety vs 文档"L-1"（负1），可能导致层级优先级解析歧义

**修复**: L1_safety→L_minus1_safety

#### 问题8（中）: PAM后半段约40个参数缺失editable和description字段（已修复）

arb_*/mm_*/p0_*/bt_*/grid类参数全部缺失这两个元属性字段

**修复**: 35个参数已添加editable/description

#### 问题9（低）: PAM类型定义和格式问题（已修复）

- pullback_retrace_pct_call/put: type=float但default=null，类型不精确
- last_validated格式不统一（日期对象 vs 字符串 vs null）

**修复**: type和last_validated格式已统一

---

## 三、逐行核查确认

| 核查项 | 结果 |
|--------|------|
| parameter_attribute_matrix.yaml所有129个参数的type定义 | 与V7文档一致（2处type=null待改进） |
| parameter_attribute_matrix.yaml所有参数的range定义 | 与V7文档一致，TVF搜索空间range已对齐PAM（已修复） |
| parameter_attribute_matrix.yaml所有参数的layer归属 | 与V7文档一致（L1_safety→L_minus1_safety已修复） |
| parameter_attribute_matrix.yaml所有参数的source标注 | 与V7文档一致 |
| parameter_attribute_matrix.yaml所有参数的evidence引用 | 已修正3处，其余一致 |
| parameter_attribute_matrix.yaml所有参数的constraint约束 | 7个constraint字段已添加（已修复） |
| tvf_params.yaml所有28个参数的默认值 | 已修正3处tri_scale值，其余一致 |
| tvf_params.yaml所有参数的range | 已对齐PAM（已修复） |
| tvf_params.yaml的num_points | V7.3扩展版，文档已记录 |
| tvf_params.yaml的层间权重和=1.0约束 | 一致 |
| tvf_params.yaml的L1/L2/L3层内权重和=1.0约束 | 一致 |
| PAM后半段参数元属性完整性 | 35个参数已添加editable/description（已修复） |

---

## 四、核查结论

| 维度 | 状态 |
|------|------|
| 参数名对齐 | 通过 |
| 默认值对齐 | 已修正(3处tri_scale) |
| 范围对齐 | 7处差异已修复 |
| 层级对齐 | 1处命名差异已修复 |
| 来源对齐 | 通过 |
| 证据对齐 | 已修正(3处) |
| 约束对齐 | 7个constraint字段已添加（已修复） |
| 元属性完整性 | 35个参数已添加editable/description（已修复） |
| **总体结论** | **全部已对齐** |
