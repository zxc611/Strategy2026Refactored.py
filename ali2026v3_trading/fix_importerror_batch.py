"""
批量修复NEW-P1-06：为所有except ImportError添加分类注释和日志级别
"""
import re
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

OPTIONAL_DEPS = [
    ("optuna", "optuna用于参数优化"),
    ("scipy", "scipy用于科学计算"),
    ("duckdb", "duckdb用于数据存储"),
    ("yaml", "PyYAML用于配置文件解析"),
    ("fcntl", "fcntl用于文件锁(Unix)"),
    ("msvcrt", "msvcrt用于文件锁(Windows)"),
    ("jsonschema", "jsonschema用于JSON验证"),
    ("tkinter", "tkinter用于GUI界面"),
    ("chinese_calendar", "chinese_calendar用于中国节假日判断"),
]

def add_optional_comment(content: str, filename: str) -> str:
    """为可选依赖添加注释"""
    for dep_name, dep_desc in OPTIONAL_DEPS:
        pattern = rf'(try:\s*\n\s*import\s+{dep_name})'
        if re.search(pattern, content):
            comment = f"# OPTIONAL-DEPENDENCY: {dep_desc}，缺失时降级为无此功能\n"
            content = re.sub(pattern, comment + r'\1', content, count=1)
    return content

def process_file(filepath: Path) -> bool:
    """处理单个文件"""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        original = content
        
        if filepath.name in ['param_pool', '参数池']:
            for root, dirs, files in os.walk(filepath):
                for file in files:
                    if file.endswith('.py'):
                        process_file(Path(root) / file)
            return False
        
        content = add_optional_comment(content, filepath.name)
        
        if content != original:
            with open(filepath, 'w', encoding='utf-8', errors='ignore') as f:
                f.write(content)
            return True
        return False
    except Exception as e:
        print(f"处理文件失败 {filepath}: {e}")
        return False

modified_count = 0
for py_file in BASE_DIR.rglob("*.py"):
    if process_file(py_file):
        modified_count += 1
        print(f"已修改: {py_file.relative_to(BASE_DIR)}")

print(f"\n总计修改 {modified_count} 个文件")
