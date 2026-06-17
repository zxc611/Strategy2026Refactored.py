# 影子策略模块公共API污染问题 —— 根因分析与解决方案报告

**文档编号**: PA-03-SUPP-01  
**关联架构原则**: PA-03（影子策略隔离）、ARCH-01（Facade模式）  
**阶段**: P2-S3 → P2-S4（全量目录重组 → 全量回归测试）  
**严重等级**: Sev-2（架构污染，非功能性阻断）  
**报告日期**: 2026-06-07

---

## 一、问题陈述

### 1.1 现象

`ali2026v3_trading/shadow_strategy_engine.py`（以及同架构模式的平铺模块）作为 re-export 模块，在模块顶层 `import` 了大量实现细节符号（`logger`, `copy`, `json`, 内部辅助函数等），这些符号被当作模块级变量暴露。当 `strategy/__init__.py` 执行 `from .shadow_strategy_engine import *` 重导出时，所有实现细节被污染到子系统公共命名空间。

### 1.2 问题定性

**这不是目录重组引入的新缺陷，而是平铺架构（256文件平铺）下"实现细节 = 公共API"的历史债务在重导出时被显性化。**

在平铺架构中，`shadow_strategy_engine.py` 既是内部实现文件又是事实上的公共API入口。目录重组和 `__init__.py` 重导出只是把原本藏在暗处的API污染摆上了台面。

---

## 二、根因分析

### 2.1 平铺架构的反模式

```python
# 原文件（平铺架构下的反模式示例）
# ali2026v3_trading/shadow_strategy_engine.py

import json, copy, logging          # ← 实现细节，无下划线前缀
from .some_deep_module import _internal_helper  # ← 内部辅助函数

logger = logging.getLogger(__name__)  # ← 模块级变量

# === 公共API（仅3个） ===
class ShadowStrategyEngine: ...
def emit_shadow_signal(): ...
ShadowConfig = ...

# === 以下4个被污染暴露 ===
# logger           ← 被外部误用: from shadow_strategy_engine import logger
# copy             ← 被外部误用: from shadow_strategy_engine import copy
# json             ← 被外部误用
# _internal_helper ← 被外部误用
```

### 2.2 污染传播路径

```
shadow_strategy_engine.py（污染源）
    ↓ import * 或显式 import
strategy/__init__.py（放大器）
    ↓ from ali2026v3_trading.strategy import *
外部消费者（受害者）
    → 获得了 logger, copy, json, _internal_helper
    → 这些符号与标准库同名，造成命名冲突和认知混淆
```

### 2.3 技术债务链

```
平铺架构（256文件）
  → 模块既是实现又是API（无边界）
    → re-export 暴露所有模块级符号
      → 目录重组后 _init__.py 放大污染
        → 外部消费者收到不可控的符号集
```

---

## 三、解决方案

### 3.1 总策略：三层物理隔离

```
strategy/                              ← 子系统目录
├── __init__.py                        ← 只重导出公共API（≤7个符号）
├── shadow_strategy_facade.py          ← 公共门面（原 shadow_strategy_engine 的API层）
├── _shadow_engine_core.py             ← 内部实现（原文件的核心逻辑）
├── _shadow_internals.py               ← 纯内部辅助（logger初始化、json序列化工具等）
└── tests/
```

### 3.2 各层职责与约束

#### 第一层：`_shadow_internals.py` —— 内部工具集

- **定位**: strategy 子系统的内部工具包
- **允许内容**: 纯函数工具、logger初始化、常量、序列化辅助函数
- **暴露面**: 仅对 strategy/ 下所有 `_*.py` 文件和 `__init__.py` 可见
- **禁止**: 跨子系统引用、公共模块（无下划线前缀）引用

```python
# strategy/_shadow_internals.py
import json
import logging

_logger = logging.getLogger("shadow_strategy")

def _serialize_shadow_payload(tick_data: dict) -> str:
    """影子策略信号序列化（仅子系统内使用）"""
    return json.dumps(tick_data, ensure_ascii=False, default=str)

def _deserialize_shadow_config(raw: bytes) -> dict:
    """影子配置反序列化（仅子系统内使用）"""
    return json.loads(raw)

SHADOW_DEFAULT_TIMEOUT = 5.0
SHADOW_MAX_RETRIES = 3

# 显式声明：即使被 import * 也不暴露到外部
__all__: list[str] = []
```

#### 第二层：`_shadow_engine_core.py` —— 内部核心实现

- **定位**: 影子策略引擎的核心实现
- **可见性**: 包内可见（`strategy/` 内），不跨子系统暴露
- **依赖**: 只能引用 `_shadow_internals` 和标准库

```python
# strategy/_shadow_engine_core.py
import logging
from dataclasses import dataclass, field
from ._shadow_internals import (
    _serialize_shadow_payload,
    _deserialize_shadow_config,
    SHADOW_DEFAULT_TIMEOUT,
)

_logger = logging.getLogger("shadow_strategy.core")

@dataclass
class _ShadowEngineState:
    """影子引擎内部状态机（包外不可见）"""
    active: bool = False
    buffer: list = field(default_factory=list)
    last_signal_time: float = 0.0

class ShadowEngineCore:
    """影子策略引擎核心（包内可见，不跨子系统暴露）"""

    def __init__(self, config: dict):
        self._state = _ShadowEngineState()
        self._config = _deserialize_shadow_config(config)
        _logger.info("ShadowEngineCore initialized")

    def process(self, tick_snapshot: dict) -> dict:
        _logger.debug("Processing tick in shadow engine")
        raw = _serialize_shadow_payload(tick_snapshot)
        # ... 核心逻辑 ...
        return {"raw": raw, "state": self._state.active}

# 不声明 __all__，防止被 import * 带走
```

#### 第三层：`shadow_strategy_facade.py` —— 公共门面（唯一对外入口）

- **行数限制**: ≤200行
- **暴露内容**: ≤3个核心符号
- **依赖**: 仅引用 `_shadow_engine_core`，不引用 `_shadow_internals`
- **设计模式**: Facade（ARCH-01）+ 工厂函数（DIP）

```python
# strategy/shadow_strategy_facade.py
"""影子策略子系统——唯一公共入口（ARCH-01 Facade模式）

外部消费者只能通过此模块访问影子策略功能。
任何 _shadow_ 前缀的内部模块都不应被外部直接import。
"""

from dataclasses import dataclass
from ._shadow_engine_core import ShadowEngineCore

__all__ = [
    "ShadowStrategyFacade",
    "ShadowSignal",
    "create_shadow_engine",
]


@dataclass(frozen=True)
class ShadowSignal:
    """影子策略标准化信号（不可变数据类）"""
    strategy_id: str
    direction: str        # "LONG" | "SHORT" | "NEUTRAL"
    confidence: float     # 0.0 ~ 1.0
    timestamp: float
    metadata: dict | None = None


class ShadowStrategyFacade:
    """影子策略子系统唯一公共入口"""

    def __init__(self, core: ShadowEngineCore):
        self._core = core  # 组合，非继承

    def evaluate(self, market_snapshot: dict) -> ShadowSignal:
        """评估市场快照，返回标准化影子信号"""
        result = self._core.process(market_snapshot)
        return ShadowSignal(
            strategy_id=result.get("id", "shadow"),
            direction=result.get("direction", "NEUTRAL"),
            confidence=result.get("confidence", 0.0),
            timestamp=result.get("timestamp", 0.0),
        )


def create_shadow_engine(config: dict) -> ShadowStrategyFacade:
    """工厂函数：隐藏 ShadowEngineCore 的构造细节"""
    core = ShadowEngineCore(config)
    return ShadowStrategyFacade(core)
```

#### 第四层：`strategy/__init__.py` —— 精确白名单重导出

```python
# strategy/__init__.py
"""strategy 子系统公共API"""

from .shadow_strategy_facade import (
    ShadowStrategyFacade,
    ShadowSignal,
    create_shadow_engine,
)

__all__ = [
    "ShadowStrategyFacade",
    "ShadowSignal",
    "create_shadow_engine",
]

# 严格约束：__all__ 长度 ≤ 7
# 禁止: import *、重导出任何 _ 前缀模块
# 禁止: 重导出 logger / json / copy 等实现细节
```

---

## 四、强制执行机制

### 4.1 CI lint规则

```python
# tools/verify_internal_imports.py —— 纳入P2-S3验证脚本集
"""AST级架构边界验证脚本

规则：
  - strategy/_shadow_*.py 和 strategy/__init__.py 可以引用 _shadow_internals
  - 其他所有文件引用 _shadow_internals → 违反架构边界
  - 公共模块（无 _ 前缀）引用 _shadow_engine_core → 违反架构边界
"""

import ast
import sys
from pathlib import Path

# ============================================================
# 配置：内部模块 → 合法引用者白名单
# ============================================================
INTERNAL_MODULE_RULES = {
    "_shadow_internals": {
        "allowed_importers": [
            "strategy/_shadow_",       # strategy/ 下所有 _shadow_ 前缀文件
            "strategy/__init__.py",     # 重导出控制文件
        ],
        "message": "跨子系统或公共模块禁止引用 _shadow_internals",
    },
    "_shadow_engine_core": {
        "allowed_importers": [
            "strategy/shadow_strategy_facade.py",  # 仅Facade可引用Core
            "strategy/__init__.py",
        ],
        "message": "仅 ShadowStrategyFacade 可引用 _shadow_engine_core",
    },
}

INTERNAL_PATTERNS = list(INTERNAL_MODULE_RULES.keys())


def _matches_any(pattern: str, filepath: str) -> bool:
    """检查filepath是否匹配任一白名单模式"""
    return pattern in filepath


def _is_allowed_importer(filepath: str, internal_module: str) -> bool:
    """检查给定文件是否有权引用指定的内部模块"""
    rules = INTERNAL_MODULE_RULES.get(internal_module, {})
    allowed = rules.get("allowed_importers", [])
    return any(_matches_any(pattern, filepath) for pattern in allowed)


def scan_file(filepath: Path) -> list[dict]:
    """AST解析，精确区分import语句与注释/字符串"""
    violations = []
    try:
        tree = ast.parse(filepath.read_text(encoding="utf-8"))
    except SyntaxError:
        return []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                for pattern in INTERNAL_PATTERNS:
                    if pattern in alias.name:
                        if not _is_allowed_importer(str(filepath), pattern):
                            violations.append({
                                "file": str(filepath),
                                "line": node.lineno,
                                "import": alias.name,
                                "rule": INTERNAL_MODULE_RULES[pattern]["message"],
                            })

        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for pattern in INTERNAL_PATTERNS:
                if pattern in module:
                    if not _is_allowed_importer(str(filepath), pattern):
                        imported = ", ".join(a.name for a in node.names)
                        violations.append({
                            "file": str(filepath),
                            "line": node.lineno,
                            "import": f"from {module} import {imported}",
                            "rule": INTERNAL_MODULE_RULES[pattern]["message"],
                        })

    return violations


def main(root: str = "ali2026v3_trading") -> int:
    root_path = Path(root)
    if not root_path.exists():
        print(f"ERROR: Directory not found: {root}")
        return 1

    all_violations = []
    for pyfile in root_path.rglob("*.py"):
        all_violations.extend(scan_file(pyfile))

    if all_violations:
        print(f"\nARCHITECTURE VIOLATION: {len(all_violations)} internal module leak(s) detected\n")
        for v in all_violations:
            print(f"  {v['file']}:{v['line']}")
            print(f"    → {v['import']}")
            print(f"    → {v['rule']}\n")
        return 1

    print("PASS: No internal module leakage detected")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```
---

## 五、G2b-G4 交叉验证

### 5.1 问题

`StrategyCoreService`（主策略核心Facade）的业务层包含"影子策略推送"功能。若它直接实例化 `ShadowEngineCore`（或旧的 `ShadowStrategyEngine`）而非通过 `ShadowStrategyFacade` 接口注入，则 PA-03（影子策略隔离）存在泄漏路径。

### 5.2 目标状态

```python
# StrategyCoreService（Mixin消灭后的组合架构）
class StrategyCoreService:
    def __init__(
        self,
        kline_service: KlineDataService,
        tick_service: TickProcessingService,
        shadow_facade: ShadowStrategyFacade,  # ← 接口注入，非具体类
        config_service: ConfigService,
    ):
        self._shadow = shadow_facade  # 仅通过Facade交互，不触及内部状态

    def _push_to_shadow(self, signal: dict) -> None:
        """影子策略推送——仅通过Facade接口"""
        result = self._shadow.evaluate(signal)
        # result 是 ShadowSignal 数据类，不暴露内部实现
```

### 5.3 交叉验证项（纳入G4清单）

```
G2b-G4 交叉验证:
[ ] StrategyCoreService.__init__ 中 shadow 参数的类型注解是 ShadowStrategyFacade
[ ] StrategyCoreService 的所有方法中不存在 from strategy._shadow_* import ...
[ ] 影子推送路径中不存在直接访问 _engine._buffer / _engine._cache / _engine._state
[ ] AST扫描确认 StrategyCoreService 所在文件无 _shadow_ 字符串出现在 import 语句中
```

---

## 六、执行计划

### 6.1 步骤与工时（修正后）

| 步骤 | 操作 | 工时 | 通过标准 |
|------|------|------|----------|
| 1 | AST扫描 `shadow_strategy_engine.py` 的所有符号，列出 `__all__` 候选清单 | 0.5h | 输出符号清单（公共API vs 实现细节） |
| 2 | AST扫描全系统消费者：`ast.parse` 分析所有 `import shadow_strategy_engine` 的外部引用，精确区分import语句与注释/字符串 | 1h | 确认所有消费者及其import的精确符号列表，标记可迁移/需保留 |
| 3 | 创建 `_shadow_internals.py`，迁移 logger/copy/json/内部辅助函数 | 2h | 原文件编译通过，消费者import路径不变 |
| 4 | 原文件重命名为 `shadow_strategy_facade.py`，清理为纯门面（≤200行） | 2h | 仅暴露 Facade类 + 数据类 + 工厂函数 |
| 5 | `strategy/__init__.py` 精确白名单重导出（`__all__` ≤7） | 0.5h | IDE跳转直达Facade，无冗余符号建议 |
| 6 | 消费者import路径验证（复用 `verify_internal_imports.py` AST扫描） | 1h | `verify_internal_imports.py` 零违规 |
| 7 | lint规则编写与CI集成 | 2h | pre-commit / CI pipeline 中生效 |
| **总计** | | **10h** | G4验证清单全部通过 |

### 6.2 对其他子系统的推广

若G4中发现其他子系统存在同类问题（`order/`, `risk/`, `config/` 等），统一应用此模式：

```
子系统目录规范:
├── __init__.py          ← __all__ 白名单，≤7个公共符号
├── xxx_facade.py        ← 跨子系统公共API
├── _xxx_core.py          ← 子系统内部核心实现
├── _xxx_internals.py     ← 工具函数、logger、序列化等纯内部细节
└── _xxx_utils.py         ← 仅限本子系统内复用的辅助函数
```

---

## 七、验证清单（G4合入版）

```
G4（strategy/ + governance/子系统）验证清单：

  [ ] strategy/__init__.py 的 __all__ 长度 ≤7
  [ ] ShadowStrategyFacade 为影子策略子系统唯一公共入口（ARCH-01）
  [ ] strategy/ 下划线前缀模块（_*.py）不被跨子系统 import
  [ ] 无消费者直接引用 json / copy / logger 等实现细节符号
  [ ] verify_internal_imports.py AST扫描通过
  [ ] CI lint规则生效：
       允许: strategy/_shadow_*.py, strategy/__init__.py
       禁止: 其他所有文件引用 _shadow_internals
  [ ] G2b-G4 交叉验证: StrategyCoreService 不直接引用任何 _shadow_ 前缀模块
  [ ] ShadowStrategyFacade 文件行数 ≤200
  [ ] 工厂函数 create_shadow_engine() 为唯一构造路径
```

---

## 八、结论

> **这不是目录重组引起的问题，而是平铺架构下"实现细节 = 公共API"的历史债务在重导出时被显性化。重构的红利正在于此——它逼迫我们正视并清理这些隐藏的架构污染。**

解决方案的核心不是让 `__init__.py` 用 `del` 或复杂逻辑去"掩盖"污染，而是通过**三层物理隔离**（`_internals` → `_core` → `facade`）配合**AST级lint规则强制执行**，将公共API的暴露面从"所有模块级符号"压缩到"≤7个精确声明符号"。

此模式已在整体架构中形成韵律（`ServiceFactory` 硬隔离、`StrategyCoreService` 显式注入、`ShadowStrategyFacade` 双向密封），在P2-S3执行，P2-S4回归验证。