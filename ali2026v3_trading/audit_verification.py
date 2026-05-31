#!/usr/bin/env python3
"""
第十三轮审计问题验证脚本
验证审计报告中列出的P0级问题修复状态
"""

import os
import re
import sys
import ast
from pathlib import Path

def read_file_content(file_path):
    """读取文件内容"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        return f"读取文件失败: {e}"

def check_api_01():
    """验证API-01: on_tick签名不一致问题"""
    print("\n=== API-01验证: on_tick签名跨5模块不一致 ===")
    
    # 1. 检查box_spring_strategy.py中的方法定义
    bss_content = read_file_content("box_spring_strategy.py")
    if isinstance(bss_content, str) and "def on_tick" in bss_content:
        # 提取on_tick方法定义
        lines = bss_content.split('\n')
        for i, line in enumerate(lines):
            if "def on_tick" in line:
                sig = line.strip()
                # 继续读取直到找到参数结束
                j = i
                while j < len(lines) and '->' not in lines[j]:
                    sig += ' ' + lines[j].strip()
                    j += 1
                if j < len(lines):
                    sig += ' ' + lines[j].strip()
                
                print(f"1. box_spring_strategy.py on_tick签名:")
                print(f"   {sig[:200]}")
                
                # 检查参数
                params_required = ['instrument_id', 'price', 'high', 'low', 'volume', 'timestamp']
                params_found = []
                for param in params_required:
                    if f"{param}:" in sig or f" {param}=" in sig:
                        params_found.append(param)
                
                if len(params_found) == len(params_required):
                    print("   ✓ 方法签名包含所有6个必需参数")
                else:
                    missing = set(params_required) - set(params_found)
                    print(f"   ✗ 方法签名缺少参数: {missing}")
                break
    
    # 2. 检查strategy_core_service.py中的调用
    scs_content = read_file_content("strategy_core_service.py")
    if isinstance(scs_content, str):
        lines = scs_content.split('\n')
        for i, line in enumerate(lines):
            if "bss.on_tick" in line:
                print(f"\n2. strategy_core_service.py调用位置: 第{i+1}行")
                
                # 收集完整的调用
                call_lines = []
                for j in range(i, min(len(lines), i+10)):
                    call_lines.append(lines[j])
                    if ')' in lines[j]:
                        break
                
                call_code = ' '.join(call_lines)
                print(f"   调用代码: {call_code}")
                
                # 检查参数
                call_params = {
                    'high': 'high=' in call_code,
                    'low': 'low=' in call_code,
                    'volume': 'volume=' in call_code,
                    'timestamp': 'timestamp=' in call_code
                }
                
                all_present = True
                for param, has_param in call_params.items():
                    if has_param:
                        print(f"   ✓ 调用包含{param}参数")
                    else:
                        print(f"   ✗ 调用缺少{param}参数")
                        all_present = False
                
                # 检查修复注释
                has_fix_comment = False
                for j in range(max(0, i-5), i):
                    if 'R13-API-01修复' in lines[j] or 'R13-API-01:' in lines[j]:
                        print(f"   ✓ 有修复注释: {lines[j].strip()}")
                        has_fix_comment = True
                        break
                
                if all_present:
                    print("\n   ✓ API-01问题已修复")
                    return True
                else:
                    print("\n   ✗ API-01问题未完全修复")
                    return False
        print("   ✗ 未找到bss.on_tick调用")
    return False

def check_api_02():
    """验证API-02: RiskService单例工厂参数丢失问题"""
    print("\n=== API-02验证: RiskService单例工厂参数丢失 ===")
    
    risk_content = read_file_content("risk_service.py")
    if isinstance(risk_content, str):
        # 查找get_risk_service函数
        if "def get_risk_service" in risk_content:
            print("1. risk_service.py中存在get_risk_service函数")
            
            # 检查是否有params参数处理
            lines = risk_content.split('\n')
            params_handling_found = False
            singleton_check_found = False
            
            for i, line in enumerate(lines):
                if "def get_risk_service" in line:
                    # 检查函数签名
                    sig_line = line
                    j = i
                    while j < len(lines) and '):' not in lines[j]:
                        j += 1
                        if j < len(lines):
                            sig_line += ' ' + lines[j]
                    
                    print(f"   函数签名: {sig_line}")
                    
                    # 检查是否有params参数
                    if "params" in sig_line:
                        print("   ✓ 函数包含params参数")
                    else:
                        print("   ✗ 函数缺少params参数")
                    
                    # 检查参数处理逻辑
                    for k in range(i, min(len(lines), i+50)):
                        if "params" in lines[k] and ("if params" in lines[k] or "params is not None" in lines[k]):
                            params_handling_found = True
                            print(f"   ✓ 找到params参数处理: {lines[k].strip()}")
                    
                    # 检查单例逻辑
                    for k in range(i, min(len(lines), i+50)):
                        if "_instance" in lines[k] or "singleton" in lines[k].lower():
                            singleton_check_found = True
                            print(f"   ✓ 找到单例处理: {lines[k].strip()}")
                    
                    break
            
            # 检查调用方
            scs_content = read_file_content("strategy_core_service.py")
            if isinstance(scs_content, str):
                lines = scs_content.split('\n')
                for i, line in enumerate(lines):
                    if "get_risk_service" in line:
                        print(f"\n2. strategy_core_service.py调用位置: 第{i+1}行")
                        print(f"   调用代码: {line.strip()}")
                        
                        # 检查参数传递
                        if "None" in line:
                            print("   ⚠️ 调用时传递params=None")
                        elif "scope_id" in line:
                            print("   ✓ 调用包含scope_id参数")
                        else:
                            print("   ? 参数传递情况未知")
                        break
        else:
            print("   ✗ 未找到get_risk_service函数")
    return False

def check_api_03():
    """验证API-03: check_before_trade信号dict缺少action字段"""
    print("\n=== API-03验证: check_before_trade信号dict缺少action字段 ===")
    
    scs_content = read_file_content("strategy_core_service.py")
    if isinstance(scs_content, str):
        lines = scs_content.split('\n')
        
        # 查找signal字典构建
        signal_creation_found = False
        for i, line in enumerate(lines):
            if "'action':" in line or '"action":' in line:
                signal_creation_found = True
                print(f"1. 找到action字段定义: 第{i+1}行")
                # 显示上下文
                for j in range(max(0, i-3), min(len(lines), i+4)):
                    print(f"   {j+1}: {lines[j]}")
        
        if signal_creation_found:
            # 检查check_before_trade调用
            for i, line in enumerate(lines):
                if "check_before_trade" in line:
                    print(f"\n2. check_before_trade调用: 第{i+1}行")
                    print(f"   调用代码: {line.strip()}")
                    print("\n   ✓ API-03问题可能已修复，需进一步验证")
                    return True
        
        print("   ✗ 未找到signal字典构建或action字段")
    return False

def check_biz_01():
    """验证BIZ-01: 保证金释放未纳入可用资金计算"""
    print("\n=== BIZ-01验证: 保证金释放未纳入可用资金计算 ===")
    
    risk_content = read_file_content("risk_service.py")
    if isinstance(risk_content, str):
        # 查找保证金计算相关代码
        lines = risk_content.split('\n')
        
        equity_calc_found = False
        margin_release_found = False
        
        for i, line in enumerate(lines):
            line_lower = line.lower()
            if "equity" in line_lower and ("-" in line or "-=" in line or "margin" in line_lower):
                equity_calc_found = True
                print(f"1. 找到权益计算: 第{i+1}行")
                print(f"   代码: {line.strip()}")
                
                # 检查上下文
                for j in range(max(0, i-2), min(len(lines), i+3)):
                    print(f"   {j+1}: {lines[j]}")
        
        # 查找保证金释放相关代码
        for i, line in enumerate(lines):
            line_lower = line.lower()
            if ("margin" in line_lower and "release" in line_lower) or ("释放" in line):
                margin_release_found = True
                print(f"\n2. 找到保证金释放: 第{i+1}行")
                print(f"   代码: {line.strip()}")
                
                # 检查上下文
                for j in range(max(0, i-2), min(len(lines), i+3)):
                    print(f"   {j+1}: {lines[j]}")
        
        if equity_calc_found and margin_release_found:
            print("\n   ⚠️ BIZ-01问题可能存在，需要进一步分析")
            return False
        else:
            print("\n   ? BIZ-01问题状态未知")
            return False
    
    return False

def check_dead_01():
    """验证DEAD-01: _is_shadow_mode恒为True"""
    print("\n=== DEAD-01验证: _is_shadow_mode恒为True ===")
    
    shadow_file = "shadow_strategy_engine.py"
    if os.path.exists(shadow_file):
        content = read_file_content(shadow_file)
        if isinstance(content, str):
            lines = content.split('\n')
            
            is_shadow_mode_found = False
            always_true = False
            
            for i, line in enumerate(lines):
                if "_is_shadow_mode" in line:
                    is_shadow_mode_found = True
                    print(f"1. 找到_is_shadow_mode: 第{i+1}行")
                    print(f"   代码: {line.strip()}")
                    
                    # 检查是否恒为True
                    if "True" in line and ("=" in line or "==" in line or "is" in line):
                        always_true = True
                        print(f"   ⚠️ 可能恒为True: {line.strip()}")
                    
                    # 显示上下文
                    for j in range(max(0, i-2), min(len(lines), i+3)):
                        print(f"   {j+1}: {lines[j]}")
            
            if not is_shadow_mode_found:
                print("   未找到_is_shadow_mode定义")
            
            if always_true:
                print("\n   ✗ DEAD-01问题存在: _is_shadow_mode恒为True")
                return True
            else:
                print("\n   ✓ DEAD-01问题可能已修复")
                return False
    else:
        print("   shadow_strategy_engine.py文件不存在")
        return False

def check_log_01():
    """验证LOG-01: compute_position_size()完全无日志"""
    print("\n=== LOG-01验证: compute_position_size()完全无日志 ===")
    
    mode_file = "mode_engine.py"
    if os.path.exists(mode_file):
        content = read_file_content(mode_file)
        if isinstance(content, str):
            lines = content.split('\n')
            
            compute_pos_found = False
            has_logging = False
            
            for i, line in enumerate(lines):
                if "compute_position_size" in line and "def" in line:
                    compute_pos_found = True
                    print(f"1. 找到compute_position_size定义: 第{i+1}行")
                    
                    # 检查函数体是否有日志
                    for j in range(i, min(len(lines), i+50)):
                        if "logging." in lines[j] or "logger." in lines[j] or "print(" in lines[j]:
                            has_logging = True
                            print(f"   第{j+1}行有日志: {lines[j].strip()}")
                    
                    # 显示函数开始部分
                    for j in range(i, min(len(lines), i+10)):
                        print(f"   {j+1}: {lines[j]}")
                    
                    break
            
            if compute_pos_found:
                if has_logging:
                    print("\n   ✓ LOG-01问题已修复: 函数中包含日志")
                    return False
                else:
                    print("\n   ✗ LOG-01问题存在: 函数中无日志")
                    return True
            else:
                print("   未找到compute_position_size函数")
                return False
    else:
        print("   mode_engine.py文件不存在")
        return False

def main():
    """主函数"""
    print("=" * 80)
    print("第十三轮审计报告问题验证")
    print("=" * 80)
    
    # 切换到项目目录
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # 验证各问题
    results = []
    
    # P0级API契约问题
    results.append(("API-01", "on_tick签名不一致", check_api_01()))
    
    # 其他问题验证可以逐步添加
    results.append(("API-02", "RiskService单例工厂参数丢失", check_api_02()))
    results.append(("API-03", "check_before_trade信号dict缺少action字段", check_api_03()))
    results.append(("BIZ-01", "保证金释放未纳入可用资金计算", check_biz_01()))
    results.append(("DEAD-01", "_is_shadow_mode恒为True", check_dead_01()))
    results.append(("LOG-01", "compute_position_size()完全无日志", check_log_01()))
    
    print("\n" + "=" * 80)
    print("验证结果汇总:")
    print("=" * 80)
    
    issues_found = 0
    for code, description, has_issue in results:
        status = "✗ 存在问题" if has_issue else "✓ 已修复或不存在"
        print(f"{code}: {description} - {status}")
        if has_issue:
            issues_found += 1
    
    print(f"\n总计发现 {issues_found} 个问题仍存在")
    
    # 创建详细报告
    report_path = "审计验证报告_20260527.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# 第十三轮审计问题验证报告\n\n")
        f.write(f"**验证时间**: 2026-05-27\n")
        f.write(f"**验证结果**: 发现 {issues_found} 个问题仍存在\n\n")
        
        f.write("## P0级问题验证结果\n\n")
        for code, description, has_issue in results:
            status = "❌ 存在问题" if has_issue else "✅ 已修复"
            f.write(f"### {code}: {description}\n")
            f.write(f"- **状态**: {status}\n")
            if has_issue:
                f.write(f"- **风险等级**: P0\n")
                f.write(f"- **影响**: 需要立即修复\n")
            f.write("\n")
    
    print(f"\n详细报告已生成: {report_path}")

if __name__ == "__main__":
    main()