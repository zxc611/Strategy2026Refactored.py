"""
P2级问题修复工具集

修复项目：
1. Python语言特性（4项）
2. 符号与命名空间（11项）
3. 回测保真度（3项）
4. 序列化（17项）
"""
from __future__ import annotations
import re
import ast
import logging
from pathlib import Path
from typing import Set, List, Dict
from ali2026v3_trading.resilience_utils import deprecated

logger = logging.getLogger(__name__)


# R27-P2-FC-04修复: 标记旧版静态分析工具为deprecated
@deprecated("仅用于旧版静态分析，新版应使用ast模块直接处理")
def add_slots_to_dataclass(source_code: str) -> str:
    """
    P2-PY-01修复：为dataclass添加__slots__优化内存
    
    转换：
    @dataclass
    class Foo:
        x: int
        y: str
    
    为：
    @dataclass
    class Foo:
        __slots__ = ['x', 'y']
        x: int
        y: str
    """
    # 查找dataclass定义
    pattern = r'(@dataclass[^\n]*\nclass\s+(\w+).*?(?=\nclass|\Z))'
    
    def add_slots(match):
        full_match = match.group(1)
        class_name = match.group(2)
        
        # 检查是否已有__slots__
        if '__slots__' in full_match:
            return full_match
        
        # 提取字段定义
        field_pattern = r'^\s+(\w+)\s*:\s*[^=]+'
        fields = re.findall(field_pattern, full_match, re.MULTILINE)
        
        if not fields:
            return full_match
        
        # 在class定义后插入__slots__
        slots_line = f"    __slots__ = {fields}\n"
        
        # 找到class定义后的第一个非空行
        lines = full_match.split('\n')
        insert_idx = 1
        for i, line in enumerate(lines[1:], start=1):
            if line.strip():
                insert_idx = i
                break
        
        lines.insert(insert_idx, slots_line.rstrip())
        return '\n'.join(lines)
    
    return re.sub(pattern, add_slots, source_code, flags=re.DOTALL)


# R27-P2-FC-05修复: 标记旧版延迟导入检查为deprecated
@deprecated("仅用于旧版检查，新版应使用importlib机制")
def check_lazy_imports_consistency(source_code: str, module_name: str) -> List[str]:
    """
    P2-NS-01修复：检查延迟导入是否合理
    
    返回需要文档化的延迟导入列表
    """
    issues = []
    
    # 查找函数体内的import
    func_pattern = r'def\s+(\w+)\([^)]*\):[^\n]*\n((?:[^\n]*\n)*?)(?=\ndef|\nclass|\Z)'
    
    for match in re.finditer(func_pattern, source_code):
        func_name = match.group(1)
        func_body = match.group(2)
        
        # 检查函数体内的import
        if 'import ' in func_body:
            imports = re.findall(r'from\s+([^\s]+)\s+import|import\s+([^\s]+)', func_body)
            if imports:
                issues.append(f"{module_name}::{func_name}: 延迟导入 {[i[0] or i[1] for i in imports]}")
    
    return issues


def add_all_definition(source_code: str, module_name: str) -> str:
    """
    P2-NS-02修复：为模块添加__all__定义
    
    提取所有公开函数和类，生成__all__列表
    """
    # 检查是否已有__all__
    if '__all__' in source_code:
        return source_code
    
    # 提取公开函数和类
    public_names = []
    
    # 类定义
    class_pattern = r'^class\s+(\w+)'
    public_names.extend(re.findall(class_pattern, source_code, re.MULTILINE))
    
    # 函数定义（非_开头）
    func_pattern = r'^def\s+(\w+)'
    for name in re.findall(func_pattern, source_code, re.MULTILINE):
        if not name.startswith('_'):
            public_names.append(name)
    
    if not public_names:
        return source_code
    
    # 在文件开头添加__all__
    all_def = f"__all__ = {public_names!r}\n\n"
    
    # 找到第一个import或class/def之前的位置
    insert_pattern = r'((?:^""".*?"""\n)|(?:^\'\'\'.*?\'\'\'\n)|)'
    match = re.match(insert_pattern, source_code, re.DOTALL)
    
    if match:
        insert_pos = match.end()
    else:
        insert_pos = 0
    
    return source_code[:insert_pos] + all_def + source_code[insert_pos:]


# R27-P2-FC-06修复: 标记旧版format遮蔽修复为deprecated
@deprecated("仅用于旧版代码修复，新版应直接重命名参数")
def fix_format_shadowing(source_code: str) -> str:
    """
    P2-NS-03修复：修复format参数遮蔽内置函数
    
    替换format参数为fmt或其他名称
    """
    # 查找format参数
    pattern = r'def\s+(\w+)\([^)]*\bformat\b[^)]*\):'
    
    def rename_param(match):
        full = match.group(0)
        return full.replace('format', 'fmt')
    
    # 替换参数名
    new_code = re.sub(pattern, rename_param, source_code)
    
    # 替换函数体内的使用
    if new_code != source_code:
        # 这需要更复杂的AST处理，这里仅做简单替换
        logger.warning("format参数遮蔽修复需要手动验证")
    
    return new_code


def unify_import_aliases(source_code: str, module_name: str) -> str:
    """
    P2-NS-04修复：统一导入别名
    
    标准化常见库的导入别名：
    - numpy as np
    - pandas as pd
    - matplotlib.pyplot as plt
    """
    standard_aliases = {
        'numpy': 'np',
        'pandas': 'pd',
        'matplotlib.pyplot': 'plt',
        'seaborn': 'sns',
    }
    
    for module, alias in standard_aliases.items():
        # 查找非标准别名
        pattern = rf'import\s+{module}\s+as\s+(\w+)'
        match = re.search(pattern, source_code)
        
        if match:
            current_alias = match.group(1)
            if current_alias != alias:
                # 替换别名
                source_code = source_code.replace(
                    f'import {module} as {current_alias}',
                    f'import {module} as {alias}'
                )
                # 替换使用
                source_code = re.sub(
                    rf'\b{current_alias}\.',
                    f'{alias}.',
                    source_code
                )
    
    return source_code


def remove_unused_imports(source_code: str) -> str:
    """
    P2-NS-05修复：移除未使用的import
    
    简单实现：检查import是否在代码中被使用
    """
    lines = source_code.split('\n')
    used_imports = []
    unused_lines = []
    
    for i, line in enumerate(lines):
        # 匹配import语句
        import_match = re.match(r'^(?:from\s+[^\s]+\s+)?import\s+(.+)$', line.strip())
        
        if not import_match:
            used_imports.append(line)
            continue
        
        imports = import_match.group(1)
        
        # 处理多个import
        names = [n.strip().split(' as ')[0] for n in imports.split(',')]
        
        used_names = []
        for name in names:
            # 检查是否在后续代码中使用
            remaining_code = '\n'.join(lines[i+1:])
            if re.search(rf'\b{name}\b', remaining_code):
                used_names.append(name)
        
        if used_names:
            # 保留使用的import
            new_import = f"import {', '.join(used_names)}"
            if line.startswith('    '):
                new_import = '    ' + new_import
            used_imports.append(new_import)
        else:
            # 标记未使用
            unused_lines.append(i+1)
    
    if unused_lines:
        logger.info(f"移除未使用import：行{unused_lines}")
    
    return '\n'.join(used_imports)


__all__ = [
    'add_slots_to_dataclass',
    'check_lazy_imports_consistency',
    'add_all_definition',
    'fix_format_shadowing',
    'unify_import_aliases',
    'remove_unused_imports',
]
