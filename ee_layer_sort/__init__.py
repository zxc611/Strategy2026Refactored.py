# MODULE_ID: M1-056-TL
"""
three_layer_sort — 期权五态排序并列信号源实现

依据文档：docs/audit/三层期权五态排序方案_最终落地方案_20260624.md

三个并列独立的排序信号源：
  信号源A：单品种月份排序（IntraProductSorter）
  信号源B：联动品种簇排序（InterProductClusterSorter）
  信号源C：全域品种排序（GlobalSorter）

核心原则：排序只做排序，风控归模块层。三个信号源并列独立，可单独或组合使用。

合并说明 (2026-06-30): 信号源B/C合入signal_source_a.py，本文件改为统一re-export
"""
from data.three_layer_sort.signal_source_a import (
    IntraProductSorter,
    AsymmetricDecay,
    GreeksHardFilter,
    InterProductClusterSorter,
    GlobalSorter,
)

__all__ = [
    'IntraProductSorter',
    'AsymmetricDecay',
    'GreeksHardFilter',
    'InterProductClusterSorter',
    'GlobalSorter',
]
