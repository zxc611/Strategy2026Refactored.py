import sys
import os
import shutil
from typing import Dict, Any

class AutoFixer:
    """热重载自动修复工具"""
    
    def __init__(self):
        self.fix_strategies = {
            'IMPORT_ERROR': self._fix_import_error,
            'SYNTAX_ERROR': self._fix_syntax_error,
            'RUNTIME_ERROR': self._fix_runtime_error,
            'MEMORY_ERROR': self._fix_memory_error
        }
    
    def auto_fix(self, module_path: str, error_type: str, error_info: Dict) -> bool:
        """根据错误类型自动修复"""
        if error_type not in self.fix_strategies:
            print(f"⚠️  没有针对 {error_type} 的自动修复策略")
            return False
        
        print(f"🔧 尝试自动修复: {error_type}")
        
        try:
            fix_func = self.fix_strategies[error_type]
            return fix_func(module_path, error_info)
        except Exception as e:
            print(f"❌ 自动修复失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _fix_import_error(self, module_path: str, error_info: Dict) -> bool:
        """修复导入错误"""
        import re
        
        error_msg = error_info.get('message', '')
        missing_module = None
        
        # 提取缺失的模块名
        match = re.search(r"'([^']+)'", error_msg)
        if match:
            missing_module = match.group(1)
        
        if not missing_module:
            # 尝试另一种常见格式 "No module named 'xxx'"
            match = re.search(r"No module named '([^']+)'", error_msg)
            if match:
                missing_module = match.group(1)
        
        if not missing_module:
            return False
        
        print(f"  检测到缺失模块: {missing_module}")
        
        # 忽略本地模块尝试 pip install
        if missing_module.startswith("your_quant_project"):
             print(f"  跳过本地模块安装: {missing_module}")
             return self._fix_import_path(module_path, missing_module)

        # 尝试安装缺失模块
        try:
            import subprocess
            import sys
            
            print(f"  尝试安装: {missing_module}")
            
            # 使用pip安装
            subprocess.check_call([
                sys.executable, '-m', 'pip', 'install', missing_module
            ])
            
            print(f"  ✅ 成功安装: {missing_module}")
            return True
            
        except subprocess.CalledProcessError:
            print(f"  ❌ 安装失败: {missing_module}")
            
            # 尝试修复导入路径
            return self._fix_import_path(module_path, missing_module)
            
    def _fix_import_path(self, module_path: str, missing_module: str) -> bool:
        """尝试修复导入路径"""
        print(f"  尝试修复导入路径以找到: {missing_module}")
        try:
            # 策略：将上级目录加入Path
            if not module_path:
                 return False
                 
            current_dir = os.path.dirname(os.path.abspath(module_path))
            parent_dir = os.path.dirname(current_dir)
            grandparent_dir = os.path.dirname(parent_dir)
            
            added = False
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
                added = True
                print(f"  已添加路径: {parent_dir}")
                
            if grandparent_dir not in sys.path:
                sys.path.insert(0, grandparent_dir)
                added = True
                print(f"  已添加路径: {grandparent_dir}")
                
            return added
        except Exception as e:
            print(f"  导入路径修复失败: {e}")
        return False
    
    def _fix_syntax_error(self, module_path: str, error_info: Dict) -> bool:
        """修复语法错误"""
        try:
            if not module_path or not os.path.exists(module_path):
                print(f"  文件不存在: {module_path}")
                return False

            # 读取文件
            with open(module_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 常见的语法错误修复
            fixes = [
                # 修复缩进 (简单的4空格替换)
                (r'^( {4})* {1,3}[^ ]', lambda m: m.group().replace(' ', '    ')),
                # 修复缺少的冒号
                (r'^(def|class|if|elif|else|for|while|try|except|finally|with)\s+.*[^:]$',
                 lambda m: m.group() + ':'),
                # 修复字符串引号不匹配 (简单尝试)
                (r"'[^']*\"[^']*'", lambda m: m.group().replace('"', '\\"')),
                (r'"[^"]*\'[^"]*"', lambda m: m.group().replace("'", "\\'"))
            ]
            
            fixed = False
            import re
            for pattern, replacement in fixes:
                new_content, count = re.subn(pattern, replacement, content, flags=re.MULTILINE)
                if count > 0:
                    content = new_content
                    fixed = True
                    print(f"  应用了 {count} 处自动语法修复")
            
            if fixed:
                # 备份原文件
                import shutil
                shutil.copy2(module_path, f"{module_path}.bak_autofix")
                
                # 写入修复后的内容
                with open(module_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"  ✅ 语法错误修复完成，已备份为 .bak_autofix")
                return True
            else:
                print(f"  ⚠️  无法自动修复语法错误 (规则不匹配)")
                return False
                
        except Exception as e:
            print(f"  ❌ 修复过程出错: {e}")
            return False

    def _fix_runtime_error(self, module_path: str, error_info: Dict) -> bool:
        """修复运行时错误"""
        print(f"  尝试分析 Runtime Error: {error_info.get('message')}")
        return False

    def _fix_memory_error(self, module_path: str, error_info: Dict) -> bool:
        """修复内存错误"""
        print("  尝试执行 GC 回收...")
        try:
            import gc
            gc.collect()
            print("  GC 回收完成")
            return True
        except:
            return False
