# [M1-78] 状态机
# MODULE_ID: M1-114

"""state_machine.py _状态机抽象基类



P1-56修复: 提取 LifecycleStateMachine _InitStateMachine 的公共抽象，

消除多业务对象状态机重复定义

"""

from __future__ import annotations



import logging

from abc import ABC, abstractmethod

from typing import Dict, Set





class BaseStateMachine(ABC):

    """状态机抽象基类



    提供统一的状态转换接口，子类需实现:

    - can_transition(target_state) -> bool: 判断是否可转换到目标状态

    - transition_to(target_state) -> None: 执行状态转换

    - state 属性 获取当前状态



    子类应定义义LEGAL_TRANSITIONS: Dict[str, Set[str]] 类属性，

    描述合法的状态转换图。若子类使用不同的转换逻辑（如顺序阶段），

    可不使用 _LEGAL_TRANSITIONS 而自行实现can_transition_

    """



    _LEGAL_TRANSITIONS: Dict[str, Set[str]] = {}



    @property

    @abstractmethod

    def state(self) -> str:

        """当前状态"""

        ...



    @abstractmethod

    def can_transition(self, target_state: str) -> bool:

        """判断是否可以从当前状态转换到目标状态"""

        ...



    @abstractmethod

    def transition_to(self, target_state: str) -> None:

        """执行状态转换，非法转换时记录警告并跳过"""

        ...

