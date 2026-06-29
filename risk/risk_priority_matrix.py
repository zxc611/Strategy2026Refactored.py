# MODULE_ID: M1-217

# [M1-34] RPN______________

"""

Phase0增强: RPN风险优先数矩阵阵定量风险评估

RPN = 概率(1-5) × 影响(1-5) × 可检测量1-5)

CRITICAL(_6) > HIGH(_4) > MEDIUM(_5) > LOW(<15)

"""

from __future__ import annotations



import logging

from typing import Any, Dict, List, Optional

from dataclasses import dataclass





@dataclass

class RiskItem:

    name: str

    probability: int

    impact: int

    detectability: int

    description: str = ''

    mitigation: str = ''

    trigger_phase: str = ''



    @property

    def rpn(self) -> int:

        return self.probability * self.impact * self.detectability



    @property

    def severity(self) -> str:

        _rpn = self.rpn

        if _rpn >= 36:

            return 'CRITICAL'

        if _rpn >= 24:

            return 'HIGH'

        if _rpn >= 15:

            return 'MEDIUM'

        return 'LOW'





class RiskPriorityMatrix:

    """RPN风险优先数矩阵



    10项风险按RPN降序排序，CRITICAL级风险必须先缓解才能进入下一Phase

    """



    def __init__(self):

        self._risks: List[RiskItem] = []

        self._init_default_risks()



    def _init_default_risks(self) -> None:

        self._risks = [

            RiskItem('拆分引入新BUG', 3, 4, 3, '每Sprint后运行全量回撤单元测试+契约测试', 'Phase 1-4'),

            RiskItem('循环依赖再现', 3, 4, 3, '每Sprint后运行import_graph DAG验证;CI/CD阻断', 'Phase 2-3'),

            RiskItem('测试覆盖不足', 4, 3, 3, '拆分前先补测量覆盖率≥80%再拆;先写测试再重构', 'Phase 1-4'),

            RiskItem('人员技能不足', 3, 3, 4, '提前进行技能矩阵评估关键Sprint配备备份人员', 'Phase 1-4'),

            RiskItem('_LifecycleMixin MRO断裂', 2, 5, 3, '拆分前确认所有调用方已迁移到类方法委托A/B灰度验证', 'Phase 2-Sprint 6'),

            RiskItem('WAL/JSONL格式兼容灾', 3, 3, 3, '拆分前归档现有WAL;新格式向下兼容旧格式读取;格式版本号', 'Phase 2-Sprint 4'),

            RiskItem('双写不一凁', 3, 4, 2, '6个月双写过渡期定期对账脚本+差异告警', 'Phase 2'),

            RiskItem('Phase并行冲突', 2, 3, 4, 'Phase 3和Phase 4操作不同模块,可部分并行但需协调', 'Phase 3-4'),

            RiskItem('可观测性缺陷', 3, 5, 1, 'Phase 0先部署可观测性基础设施(Sprint 0独立前置)', '全程'),

            RiskItem('Facade性能退避', 2, 3, 2, '委托方法用例slots+描述符缓存优化性能基线对比', 'Phase 2'),

        ]



    def add_risk(self, risk: RiskItem) -> None:

        self._risks.append(risk)



    def get_sorted_risks(self) -> List[RiskItem]:

        return sorted(self._risks, key=lambda r: r.rpn, reverse=True)



    def get_critical_risks(self) -> List[RiskItem]:

        return [r for r in self._risks if r.severity == 'CRITICAL']



    def get_risk_by_name(self, name: str) -> Optional[RiskItem]:

        for r in self._risks:

            if r.name == name:

                return r

        return None



    def to_report(self) -> str:

        lines = ['# RPN风险优先数矩阵\n']

        lines.append('| # | 风险 | 概率 | 影响 | 可检测量| RPN | 严重复| 触发Phase |')

        lines.append('|---|------|:--:|:--:|:----:|:---:|:------:|----------|')

        for i, r in enumerate(self.get_sorted_risks(), 1):

            lines.append(f'| {i} | {r.name} | {r.probability} | {r.impact} | {r.detectability} | **{r.rpn}** | **{r.severity}** | {r.trigger_phase} |')

        return '\n'.join(lines)