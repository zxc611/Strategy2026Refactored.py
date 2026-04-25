"""
UI状态同步诊断脚本

用途：检查UI按钮状态与实际交易状态是否同步
运行时间：策略启动后立即执行
"""
import sys
import os
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def diagnose_ui_state():
    """诊断UI状态同步问题"""
    print("=" * 80)
    print("UI状态同步诊断")
    print("=" * 80)
    print(f"诊断时间: {datetime.now()}")
    print()
    
    # 尝试获取策略实例
    try:
        from t_type_bootstrap import _strategy_cache as strategy
        if not strategy:
            print("❌ 错误: 无法获取策略实例 (t_type_bootstrap._strategy_cache 为空)")
            return False
        
        print("✅ 成功获取策略实例")
        print()
        
        # 检查关键属性
        checks = [
            ("auto_trading_enabled", "自动交易启用标志"),
            ("my_trading", "交易启用标志"),
            ("trading", "平台交易标志"),
        ]
        
        print("【关键状态检查】")
        for attr_name, desc in checks:
            value = getattr(strategy, attr_name, None)
            status = "✅" if value else "❌"
            print(f"  {status} {desc:20s} ({attr_name:25s}): {value}")
        
        print()
        
        # 检查params中的配置
        if hasattr(strategy, 'params'):
            print("【Params配置检查】")
            param_checks = [
                ("auto_trading_enabled", "自动交易配置"),
                ("output_mode", "输出模式"),
                ("debug_output", "调试输出"),
                ("diagnostic_output", "诊断输出"),
            ]
            
            for attr_name, desc in param_checks:
                value = getattr(strategy.params, attr_name, None)
                print(f"  ℹ️  {desc:20s} ({attr_name:25s}): {value}")
        else:
            print("❌ 错误: strategy.params 不存在")
        
        print()
        
        # 检查UI相关属性
        print("【UI组件检查】")
        ui_attrs = [
            ("_ui_root", "UI根窗口"),
            ("_ui_btn_trade", "交易按钮"),
            ("_ui_btn_auto", "自动交易按钮"),
            ("_ui_btn_manual", "手动交易按钮"),
            ("_ui_running", "UI运行状态"),
        ]
        
        for attr_name, desc in ui_attrs:
            value = getattr(strategy, attr_name, None)
            exists = "✅" if value is not None else "❌"
            print(f"  {exists} {desc:20s} ({attr_name:25s}): {type(value).__name__ if value is not None else 'None'}")
        
        print()
        
        # 综合判断
        print("【综合诊断结果】")
        issues = []
        
        if not getattr(strategy, 'auto_trading_enabled', False):
            issues.append("⚠️  auto_trading_enabled 为 False，但当前是开盘时间")
        
        if not getattr(strategy, 'my_trading', False):
            issues.append("⚠️  my_trading 为 False，交易功能未启用")
        
        if not getattr(strategy, '_ui_running', False):
            issues.append("⚠️  UI 未在运行状态")
        
        if issues:
            print("发现以下问题:")
            for issue in issues:
                print(f"  {issue}")
            print()
            print("💡 建议: 重启策略后检查日志中的'初始交易状态'行")
            return False
        else:
            print("✅ 所有状态正常!")
            print()
            print("如果UI按钮点击仍无效，请检查:")
            print("  1. UI样式是否正确刷新（查看_refresh_output_mode_ui_styles日志）")
            print("  2. 按钮回调是否被正确绑定（查看UI创建日志）")
            print("  3. 是否有异常阻止了状态更新（查看error日志）")
            return True
            
    except Exception as e:
        print(f"❌ 诊断过程出错: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = diagnose_ui_state()
    sys.exit(0 if success else 1)
