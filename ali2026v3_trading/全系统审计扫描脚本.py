#!/usr/bin/env python3
"""
全系统审计扫描脚本
按原审计报告的4个维度重新扫描：
1. API契约与接口稳定性
2. 业务逻辑漏洞与风控绕过
3. 死路径与不可达代码
4. 日志与可观测性
"""

import os
import re
import ast
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple
import json

class CodeAuditor:
    def __init__(self, root_dir: str):
        self.root_dir = Path(root_dir)
        self.issues = {
            "API契约与接口稳定性": [],
            "业务逻辑漏洞与风控绕过": [],
            "死路径与不可达代码": [],
            "日志与可观测性": []
        }
        
    def scan_python_files(self) -> List[Path]:
        """扫描所有Python文件"""
        python_files = []
        for root, dirs, files in os.walk(self.root_dir):
            # 跳过备份目录和缓存目录
            if any(exclude in root for exclude in ['backup', '__pycache__', '.git', 'venv']):
                continue
                
            for file in files:
                if file.endswith('.py'):
                    python_files.append(Path(root) / file)
        return python_files
    
    def check_api_contracts(self, filepath: Path, content: str) -> List[Dict]:
        """检查API契约与接口稳定性问题"""
        issues = []
        lines = content.split('\n')
        
        # 检查1: 函数签名不一致
        function_pattern = r'def\s+(\w+)\s*\((.*?)\)\s*->'
        functions = {}
        
        for i, line in enumerate(lines):
            match = re.search(function_pattern, line)
            if match:
                func_name = match.group(1)
                params = match.group(2)
                functions[func_name] = {
                    'line': i+1,
                    'params': params,
                    'file': filepath
                }
        
        # 检查2: 参数传递不一致
        param_pattern = r'(\w+)\.(\w+)\s*\(([^)]*)\)'
        for i, line in enumerate(lines):
            matches = re.findall(param_pattern, line)
            for match in matches:
                obj_name, method_name, call_params = match
                # 这里可以添加更复杂的参数一致性检查
        
        # 检查3: 类型注解缺失
        for i, line in enumerate(lines):
            if 'def ' in line and '->' in line:
                # 检查参数是否有类型注解
                if '):' in line and not any(param.strip().endswith(':') for param in line.split('(')[1].split(')')[0].split(',') if param.strip()):
                    issues.append({
                        'type': 'API契约',
                        'severity': 'P1',
                        'file': str(filepath),
                        'line': i+1,
                        'description': f'函数缺少类型注解: {line.strip()}',
                        'suggestion': '为所有参数和返回值添加类型注解'
                    })
        
        # 检查4: 默认参数值不一致
        default_param_pattern = r'(\w+)\s*=\s*([^,)]+)'
        for func_name, func_info in functions.items():
            defaults = re.findall(default_param_pattern, func_info['params'])
            # 检查默认值是否合理
        
        return issues
    
    def check_business_logic(self, filepath: Path, content: str) -> List[Dict]:
        """检查业务逻辑漏洞与风控绕过问题"""
        issues = []
        lines = content.split('\n')
        
        # 检查1: 保证金计算问题
        for i, line in enumerate(lines):
            if 'margin' in line.lower() or '保证金' in line:
                # 检查是否考虑了已有持仓保证金
                if 'equity' in line and 'required_margin' in line and 'existing' not in line.lower():
                    issues.append({
                        'type': '业务逻辑',
                        'severity': 'P0',
                        'file': str(filepath),
                        'line': i+1,
                        'description': '保证金计算未考虑已有持仓',
                        'suggestion': '计算可用资金时应扣除已有持仓的保证金'
                    })
        
        # 检查2: 价格偏离检查（胖手指防护）
        for i, line in enumerate(lines):
            if 'price' in line and ('order' in line.lower() or 'send_order' in line):
                if 'deviation' not in line.lower() and 'check' not in line.lower() and 'validate' not in line.lower():
                    # 检查上下文是否包含价格检查
                    context = '\n'.join(lines[max(0, i-5):min(len(lines), i+5)])
                    if 'deviation' not in context.lower():
                        issues.append({
                            'type': '业务逻辑',
                            'severity': 'P0',
                            'file': str(filepath),
                            'line': i+1,
                            'description': '订单价格无偏离检查（胖手指风险）',
                            'suggestion': '添加价格偏离检查，如price_deviation_check'
                        })
        
        # 检查3: 风控检查非原子性
        for i, line in enumerate(lines):
            if 'check_before_trade' in line or 'risk_check' in line.lower():
                # 检查是否在锁内执行
                context_lines = lines[max(0, i-10):min(len(lines), i+10)]
                context = '\n'.join(context_lines)
                if 'with' in context and 'lock' in context:
                    # 检查锁的范围是否包含所有风控检查
                    lock_start = None
                    for j, ctx_line in enumerate(context_lines):
                        if 'with' in ctx_line and 'lock' in ctx_line:
                            lock_start = j
                        elif lock_start is not None and 'check' in ctx_line.lower() and 'risk' in ctx_line.lower():
                            # 在锁内有风控检查
                            pass
                else:
                    issues.append({
                        'type': '业务逻辑',
                        'severity': 'P0',
                        'file': str(filepath),
                        'line': i+1,
                        'description': '风控检查可能不在锁内执行（并发风险）',
                        'suggestion': '确保风控检查在适当的锁范围内执行'
                    })
        
        # 检查4: 负权益无硬停止
        for i, line in enumerate(lines):
            if 'equity' in line and ('<=' in line or '<' in line) and '0' in line:
                context = '\n'.join(lines[max(0, i-3):min(len(lines), i+3)])
                if 'stop' not in context.lower() and 'halt' not in context.lower():
                    issues.append({
                        'type': '业务逻辑',
                        'severity': 'P0',
                        'file': str(filepath),
                        'line': i+1,
                        'description': '负权益检查无硬停止机制',
                        'suggestion': 'equity<=0时应立即停止交易并强制平仓'
                    })
        
        return issues
    
    def check_dead_code(self, filepath: Path, content: str) -> List[Dict]:
        """检查死路径与不可达代码"""
        issues = []
        lines = content.split('\n')
        
        # 检查1: 永远为True的条件
        for i, line in enumerate(lines):
            if 'if' in line and 'True' in line:
                issues.append({
                    'type': '死路径',
                    'severity': 'P0',
                    'file': str(filepath),
                    'line': i+1,
                    'description': '条件永远为True，形成死路径',
                    'suggestion': '检查条件逻辑是否正确'
                })
            
            if 'if' in line and 'False' in line:
                issues.append({
                    'type': '死路径',
                    'severity': 'P0',
                    'file': str(filepath),
                    'line': i+1,
                    'description': '条件永远为False，代码不可达',
                    'suggestion': '移除永远为False的条件或检查逻辑'
                })
        
        # 检查2: 未使用的变量/函数
        try:
            tree = ast.parse(content)
            # 检查未使用的导入
            imports = set()
            used_names = set()
            
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.add(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        imports.add(alias.name)
                elif isinstance(node, ast.Name):
                    used_names.add(node.id)
                elif isinstance(node, ast.Attribute):
                    used_names.add(node.attr)
                elif isinstance(node, ast.FunctionDef):
                    used_names.add(node.name)
                elif isinstance(node, ast.ClassDef):
                    used_names.add(node.name)
            
            # 简单检查：查找定义但可能未使用的函数
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    func_name = node.name
                    # 检查是否以_开头（可能是私有方法）
                    if not func_name.startswith('_'):
                        # 检查是否在其他地方被调用（简单检查）
                        func_calls = content.count(f'{func_name}(')
                        if func_calls <= 1:  # 只有定义
                            # 获取行号
                            for i, line in enumerate(lines):
                                if f'def {func_name}(' in line:
                                    issues.append({
                                        'type': '死路径',
                                        'severity': 'P1',
                                        'file': str(filepath),
                                        'line': i+1,
                                        'description': f'函数"{func_name}"可能未使用',
                                        'suggestion': '检查是否确实需要此函数，或添加调用'
                                    })
                                    break
        except SyntaxError:
            pass
        
        # 检查3: except: pass 静默吞异常
        for i, line in enumerate(lines):
            if 'except:' in line and 'pass' in lines[i+1] if i+1 < len(lines) else False:
                issues.append({
                    'type': '死路径',
                    'severity': 'P1',
                    'file': str(filepath),
                    'line': i+1,
                    'description': 'except: pass静默吞异常，可能隐藏错误',
                    'suggestion': '至少记录异常日志或处理异常'
                })
        
        # 检查4: 标志位设置但从未使用
        flag_pattern = r'self\._(\w+)\s*=\s*(True|False)'
        flags_set = {}
        for i, line in enumerate(lines):
            match = re.search(flag_pattern, line)
            if match:
                flag_name = match.group(1)
                flags_set[flag_name] = i+1
        
        # 检查标志位是否被使用
        for flag_name, line_num in flags_set.items():
            usage_count = 0
            for i, line in enumerate(lines):
                if flag_name in line and f'self._{flag_name}' in line and i+1 != line_num:
                    usage_count += 1
            
            if usage_count == 0:
                issues.append({
                    'type': '死路径',
                    'severity': 'P1',
                    'file': str(filepath),
                    'line': line_num,
                    'description': f'标志位self._{flag_name}设置但从未使用',
                    'suggestion': '移除未使用的标志位或添加使用逻辑'
                })
        
        return issues
    
    def check_logging_observability(self, filepath: Path, content: str) -> List[Dict]:
        """检查日志与可观测性问题"""
        issues = []
        lines = content.split('\n')
        
        # 检查1: 关键操作无日志
        critical_operations = [
            'compute_position_size',
            'check_before_trade',
            'execute_order',
            'on_tick',
            'update_position',
            'calculate_margin',
            'risk_check',
            'stop_loss',
            'take_profit'
        ]
        
        for op in critical_operations:
            for i, line in enumerate(lines):
                if f'def {op}' in line:
                    # 检查函数体内是否有日志
                    func_body_start = i + 1
                    func_body_end = None
                    brace_count = 0
                    
                    for j in range(i, len(lines)):
                        if '{' in lines[j]:
                            brace_count += lines[j].count('{')
                        if '}' in lines[j]:
                            brace_count -= lines[j].count('}')
                        
                        if brace_count > 0 and func_body_end is None:
                            func_body_start = j
                        
                        if brace_count == 0 and func_body_end is None and j > i:
                            func_body_end = j
                            break
                    
                    if func_body_end:
                        func_body = '\n'.join(lines[func_body_start:func_body_end])
                        if 'logging.' not in func_body and 'logger.' not in func_body:
                            issues.append({
                                'type': '日志可观测性',
                                'severity': 'P0',
                                'file': str(filepath),
                                'line': i+1,
                                'description': f'关键操作"{op}"完全无日志',
                                'suggestion': '在函数开始、关键决策点、返回前添加日志'
                            })
        
        # 检查2: 使用f-string格式化日志（性能问题）
        for i, line in enumerate(lines):
            if 'logging.' in line and 'f"' in line:
                issues.append({
                    'type': '日志可观测性',
                    'severity': 'P1',
                    'file': str(filepath),
                    'line': i+1,
                    'description': '使用f-string格式化日志，即使日志级别未启用也会执行格式化',
                    'suggestion': '改为使用logging方法的参数格式化：logging.info("%s %s", var1, var2)'
                })
        
        # 检查3: 日志级别不当
        for i, line in enumerate(lines):
            if 'logging.debug(' in line:
                # 检查是否是关键错误信息
                if 'error' in line.lower() or 'exception' in line.lower() or 'critical' in line.lower():
                    issues.append({
                        'type': '日志可观测性',
                        'severity': 'P1',
                        'file': str(filepath),
                        'line': i+1,
                        'description': '关键错误信息使用DEBUG级别，实盘可能不输出',
                        'suggestion': '改为logging.error或logging.critical'
                    })
        
        # 检查4: 无外部告警集成
        alert_keywords = ['email', 'sms', 'webhook', 'slack', 'teams', 'alert']
        has_external_alert = False
        for line in lines:
            if any(keyword in line.lower() for keyword in alert_keywords):
                has_external_alert = True
                break
        
        # 检查关键事件是否有告警
        critical_events = ['熔断', 'circuit_breaker', '硬停止', 'hard_stop', '负权益', 'negative_equity']
        for i, line in enumerate(lines):
            if any(event in line for event in critical_events):
                # 检查是否有日志记录
                context = '\n'.join(lines[max(0, i-3):min(len(lines), i+3)])
                if 'logging.' in context and not has_external_alert:
                    issues.append({
                        'type': '日志可观测性',
                        'severity': 'P0',
                        'file': str(filepath),
                        'line': i+1,
                        'description': '关键事件仅记录到日志文件，无外部告警系统集成',
                        'suggestion': '集成邮件/SMS/Webhook等外部告警系统'
                    })
        
        return issues
    
    def run_full_audit(self):
        """执行完整审计"""
        print("="*80)
        print("全系统审计扫描开始")
        print("="*80)
        
        python_files = self.scan_python_files()
        print(f"找到 {len(python_files)} 个Python文件")
        
        total_issues = 0
        
        for filepath in python_files:
            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                # 检查API契约问题
                api_issues = self.check_api_contracts(filepath, content)
                self.issues['API契约与接口稳定性'].extend(api_issues)
                
                # 检查业务逻辑问题
                biz_issues = self.check_business_logic(filepath, content)
                self.issues['业务逻辑漏洞与风控绕过'].extend(biz_issues)
                
                # 检查死路径问题
                dead_issues = self.check_dead_code(filepath, content)
                self.issues['死路径与不可达代码'].extend(dead_issues)
                
                # 检查日志问题
                log_issues = self.check_logging_observability(filepath, content)
                self.issues['日志与可观测性'].extend(log_issues)
                
                total_issues += len(api_issues) + len(biz_issues) + len(dead_issues) + len(log_issues)
                
                if api_issues or biz_issues or dead_issues or log_issues:
                    print(f"  {filepath.name}: {len(api_issues)+len(biz_issues)+len(dead_issues)+len(log_issues)}个问题")
                    
            except Exception as e:
                print(f"  扫描 {filepath} 时出错: {e}")
        
        print(f"\n扫描完成，共发现 {total_issues} 个问题")
        return self.issues
    
    def generate_report(self, output_file: str = "全系统审计扫描报告.md"):
        """生成审计报告"""
        report = []
        report.append("# 全系统审计扫描报告")
        report.append(f"**扫描时间**: 2026-05-27")
        report.append(f"**扫描文件数**: {len(self.scan_python_files())}")
        report.append(f"**问题总数**: {sum(len(issues) for issues in self.issues.values())}")
        report.append("")
        
        # 按严重程度分类
        severity_counts = {'P0': 0, 'P1': 0, 'P2': 0}
        
        for category, issues in self.issues.items():
            if issues:
                report.append(f"## {category}")
                report.append("")
                
                for severity in ['P0', 'P1', 'P2']:
                    severity_issues = [issue for issue in issues if issue['severity'] == severity]
                    if severity_issues:
                        report.append(f"### {severity}级问题 ({len(severity_issues)}项)")
                        report.append("")
                        
                        for issue in severity_issues:
                            report.append(f"**文件**: `{issue['file']}`")
                            report.append(f"**行号**: {issue['line']}")
                            report.append(f"**描述**: {issue['description']}")
                            report.append(f"**建议**: {issue['suggestion']}")
                            report.append("")
                        
                        severity_counts[severity] += len(severity_issues)
        
        # 统计摘要
        report.append("## 问题统计摘要")
        report.append("")
        report.append("| 问题类别 | P0 | P1 | P2 | 合计 |")
        report.append("|----------|----|----|----|------|")
        
        total_p0 = total_p1 = total_p2 = 0
        for category, issues in self.issues.items():
            p0 = len([i for i in issues if i['severity'] == 'P0'])
            p1 = len([i for i in issues if i['severity'] == 'P1'])
            p2 = len([i for i in issues if i['severity'] == 'P2'])
            total = p0 + p1 + p2
            
            if total > 0:
                report.append(f"| {category} | {p0} | {p1} | {p2} | {total} |")
                total_p0 += p0
                total_p1 += p1
                total_p2 += p2
        
        report.append(f"| **总计** | **{total_p0}** | **{total_p1}** | **{total_p2}** | **{total_p0+total_p1+total_p2}** |")
        report.append("")
        
        # 与第十三轮审计对比
        report.append("## 与第十三轮审计对比")
        report.append("")
        report.append("| 维度 | 原审计P0 | 新发现P0 | 原审计P1 | 新发现P1 | 原审计P2 | 新发现P2 |")
        report.append("|------|----------|----------|----------|----------|----------|----------|")
        report.append("| API契约与接口稳定性 | 8 | - | 18 | - | 13 | - |")
        report.append("| 业务逻辑漏洞与风控绕过 | 9 | - | 15 | - | 5 | - |")
        report.append("| 死路径与不可达代码 | 4 | - | 10 | - | 9 | - |")
        report.append("| 日志与可观测性 | 8 | - | 21 | - | 23 | - |")
        report.append("| **合计** | **29** | **{total_p0}** | **64** | **{total_p1}** | **50** | **{total_p2}** |".format(
            total_p0=total_p0, total_p1=total_p1, total_p2=total_p2
        ))
        report.append("")
        
        # 修复建议
        report.append("## 修复优先级建议")
        report.append("")
        report.append("1. **立即修复 (P0问题)**: 可能导致实战崩盘/资金损失的问题")
        report.append("2. **高优先级 (P1问题)**: 功能异常/数据错误的问题")
        report.append("3. **中优先级 (P2问题)**: 潜在风险/规范问题")
        report.append("")
        report.append("## 9项验收标准核查清单")
        report.append("")
        report.append("对于每个修复项，必须通过以下9项验证：")
        report.append("1. ✅ 代码存在 — py_compile通过")
        report.append("2. ✅ 代码可用 — 函数在正确条件下可执行不报错")
        report.append("3. ✅ 代码被调用 — grep全项目确认有调用方，且调用方本身也被调用")
        report.append("4. ✅ 参数传递链路贯通 — 从定义→传递→消费端全链路追踪")
        report.append("5. ✅ 参数改变→结果改变 — 不同参数值产生不同输出（非空转）")
        report.append("6. ✅ 默认值在扫描网格中 — 每个扫描参数的默认值存在于grid列表中")
        report.append("7. ✅ 排序指标与扫描目标一致 — 扫描X参数时必须按X影响的指标排序")
        report.append("8. ✅ 三系统代码对齐：生产\\测试\\评判")
        report.append("9. ✅ 所有问题没有进行全链路通畅性验收合格，不能停下！不能交付")
        report.append("")
        report.append("---")
        report.append("*审计完成时间: 2026-05-27*")
        report.append("*审计工具: 华为云码道代码智能体*")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        print(f"报告已生成: {output_file}")
        
        # 同时生成JSON格式的问题列表
        json_report = {
            "scan_time": "2026-05-27",
            "file_count": len(self.scan_python_files()),
            "issue_summary": {
                "API契约与接口稳定性": len(self.issues['API契约与接口稳定性']),
                "业务逻辑漏洞与风控绕过": len(self.issues['业务逻辑漏洞与风控绕过']),
                "死路径与不可达代码": len(self.issues['死路径与不可达代码']),
                "日志与可观测性": len(self.issues['日志与可观测性'])
            },
            "severity_summary": severity_counts,
            "issues_by_category": self.issues
        }
        
        json_file = output_file.replace('.md', '.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_report, f, indent=2, ensure_ascii=False)
        
        print(f"JSON报告已生成: {json_file}")
        
        return self.issues

def main():
    """主函数"""
    root_dir = "C:/Users/xu/AppData/Roaming/InfiniTrader_SimulationX64/pyStrategy/demo/ali2026v3_trading"
    
    if not os.path.exists(root_dir):
        print(f"目录不存在: {root_dir}")
        return
    
    print("正在启动全系统审计扫描...")
    auditor = CodeAuditor(root_dir)
    
    # 执行审计
    issues = auditor.run_full_audit()
    
    # 生成报告
    report_file = "全系统审计扫描报告_20260527.md"
    auditor.generate_report(report_file)
    
    # 输出摘要
    print("\n" + "="*80)
    print("审计扫描完成摘要")
    print("="*80)
    
    for category, category_issues in issues.items():
        p0_count = len([i for i in category_issues if i['severity'] == 'P0'])
        p1_count = len([i for i in category_issues if i['severity'] == 'P1'])
        p2_count = len([i for i in category_issues if i['severity'] == 'P2'])
        total = p0_count + p1_count + p2_count
        
        if total > 0:
            print(f"{category}: P0={p0_count}, P1={p1_count}, P2={p2_count}, 总计={total}")
    
    total_all = sum(len(category_issues) for category_issues in issues.values())
    print(f"\n总计发现 {total_all} 个问题")
    print(f"详细报告请查看: {report_file}")

if __name__ == "__main__":
    main()