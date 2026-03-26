"""
诊断服务模块 - CQRS 架构 Core 层辅助服务
来源：15_diagnosis.py
功能：策略运行诊断、断点检测、问题排查、健康报告
行数：~400 行 (保持原规模)

优化 v1.1 (2026-03-16):
- ✅ 集成 EventBus 发布诊断事件
- ✅ 添加自动修复建议生成
- ✅ 性能监控仪表板支持
"""
from __future__ import annotations

import time
import threading
import traceback
import logging
import queue
from functools import wraps
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple, Protocol, runtime_checkable
from dataclasses import dataclass, field


@dataclass
class DiagnoserConfig:
    """诊断器配置类"""
    max_logs: int = 100  # 最大日志条数
    health_weights: Dict[str, int] = field(default_factory=lambda: {
        'CRITICAL': 25,
        'HIGH': 12,
        'MEDIUM': 6,
        'LOW': 2,
        'ERROR': 15,
        'WARNING': 2
    })
    enable_async_log: bool = True  # 启用异步日志
    log_queue_size: int = 1000  # 日志队列大小
    event_publish_timeout: float = 5.0  # 事件发布超时（秒）
    enable_incremental_diagnosis: bool = False  # 启用增量诊断


class StrategyProtocol(Protocol):
    """策略协议接口（类型约束）"""
    infini: Optional[Any]
    market_center: Optional[Any]
    params: Optional[Any]
    future_instruments: List[str]
    option_instruments: Dict[str, Any]
    kline_data: Dict[str, Any]
    my_state: str
    my_is_running: bool
    my_trading: bool
    subscribe_flag: bool
    
    def output(self, msg: str) -> None: ...
    def subscribe(self, *args, **kwargs) -> None: ...


@dataclass
class DiagnosisReport(object):
    """诊断报告数据类"""

    def __init__(self, config: Optional[DiagnoserConfig] = None):
        self.config = config or DiagnoserConfig()
        self.timestamp = datetime.now().isoformat()
        self.breakpoints: List[Dict] = []
        self.warnings: List[str] = []
        self.errors: List[str] = []
        self.recommendations: List[str] = []
        self.logs: List[str] = []
        self.metrics: Dict[str, Any] = {}
        self._log_queue: Optional[queue.Queue] = None
        
        if self.config.enable_async_log:
            self._log_queue = queue.Queue(maxsize=self.config.log_queue_size)
    
    def add_breakpoint(self, layer: str, component: str, issue: str, 
                       impact: str, severity: str, fix: str = "") -> None:
        """添加断点
        
        Args:
            layer: 层级（DataSource/Instrument/Subscription等）
            component: 组件名
            issue: 问题描述
            impact: 影响
            severity: 严重程度（CRITICAL/HIGH/MEDIUM/LOW）
            fix: 修复建议
        """
        self.breakpoints.append({
            "layer": layer,
            "component": component,
            "issue": issue,
            "impact": impact,
            "severity": severity,
            "fix": fix
        })
    
    def add_warning(self, msg: str) -> None:
        """添加警告"""
        self.warnings.append(msg)
    
    def add_error(self, msg: str) -> None:
        """添加错误"""
        self.errors.append(msg)
    
    def add_recommendation(self, msg: str) -> None:
        """添加建议"""
        self.recommendations.append(msg)
    
    def add_log(self, msg: str, level: str = "INFO") -> None:
        """添加日志
        
        Args:
            msg: 日志内容
            level: 日志级别
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] [{level}] {msg}"
        
        # 异步日志模式
        if self._log_queue is not None:
            try:
                self._log_queue.put_nowait(log_entry)
            except queue.Full:
                # 队列满时丢弃旧日志
                logging.warning("[Diagnoser] Log queue full, discarding old logs")
                pass
        
        # 同步记录到列表（带截断）
        max_logs = self.config.max_logs
        if len(self.logs) >= max_logs:
            # 保留最近 50% 的日志 + 所有 ERROR/WARN 日志
            critical_logs = [l for l in self.logs if '[ERROR]' in l or '[WARN]' in l]
            other_logs = self.logs[len(critical_logs):]
            retained = critical_logs + other_logs[-(max_logs//2):]
            self.logs = retained
        
        self.logs.append(log_entry)
    
    def add_metric(self, key: str, value: Any) -> None:
        """添加性能指标
        
        Args:
            key: 指标名称
            value: 指标值
        """
        self.metrics[key] = value
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典
        
        Returns:
            Dict: 诊断报告字典
        """
        return {
            "timestamp": self.timestamp,
            "breakpoints": self.breakpoints,
            "warnings": self.warnings,
            "errors": self.errors,
            "recommendations": self.recommendations,
            "logs": self.logs[-100:],  # 只保留最近 100 条日志
            "metrics": self.metrics,
            "summary": {
                "breakpoint_count": len(self.breakpoints),
                "warning_count": len(self.warnings),
                "error_count": len(self.errors),
                "recommendation_count": len(self.recommendations)
            }
        }
    
    def get_health_score(self) -> float:
        """计算健康评分（0-100）
        
        Returns:
            float: 健康评分
        """
        base_score = 100.0
        weights = self.config.health_weights
        
        # 只计算断点，避免重复扣分
        for bp in self.breakpoints:
            weight = weights.get(bp['severity'], 5)
            base_score -= weight
        
        # 错误额外扣分（与断点不重复）
        base_score -= len(self.errors) * weights.get('ERROR', 15)
        
        # 警告轻微扣分
        base_score -= len(self.warnings) * weights.get('WARNING', 2)
        
        return max(0.0, min(100.0, base_score))


class StrategyDiagnoser:
    """策略诊断器 - Core 层辅助服务
    
    职责:
    - 全链路诊断（数据源、合约、订阅、K 线）
    - 断点检测
    - 问题排查
    - 健康评分
    - 自动修复建议
    
    设计原则:
    - Composition over Inheritance
    - 单一职责
    - 线程安全
    - 非侵入式诊断
    """

    def __init__(self, strategy_instance: Optional[StrategyProtocol] = None,
                 event_bus: Optional[Any] = None,
                 config: Optional[DiagnoserConfig] = None):
        """初始化诊断器

        Args:
            strategy_instance: 策略实例（符合 StrategyProtocol 类型约束）
            event_bus: 事件总线实例（可选）
            config: 诊断器配置（可选，使用默认配置）
        """
        self.strategy = strategy_instance
        self.config = config or DiagnoserConfig()
        self.report = DiagnosisReport(self.config)
        self._lock = threading.RLock()
        self._event_bus = event_bus
        self._last_diagnosis_result: Dict[str, Any] = {}  # 上次诊断结果缓存
        self._log_worker_thread: Optional[threading.Thread] = None
        self._stop_log_worker = threading.Event()

        # 检查策略实例是否已初始化
        self._strategy_initialized = self._check_strategy_initialization()

        # 启动日志工作线程（如果启用异步日志）
        if self.config.enable_async_log and self.report._log_queue:
            self._start_log_worker()
    
    def _check_strategy_initialization(self) -> bool:
        """检查策略实例是否已初始化

        Returns:
            bool: 策略是否已初始化
        """
        if self.strategy is None:
            return False

        # 检查关键属性是否存在
        required_attrs = ['infini', 'market_center', 'params', 'future_instruments',
                         'option_instruments', 'kline_data', 'my_state', 'my_is_running',
                         'my_trading', 'subscribe_flag']

        for attr in required_attrs:
            if not hasattr(self.strategy, attr):
                self._log(f"策略缺少属性: {attr}", "WARN")
                return False

        return True

    def _publish_event_with_retry(self, event_type: str, data: Dict[str, Any], max_retries: int = 3) -> None:
        """带重试机制的事件发布

        Args:
            event_type: 事件类型
            data: 事件数据
            max_retries: 最大重试次数
        """
        if not self._event_bus:
            return

        retry_count = 0
        last_error = None

        while retry_count < max_retries:
            try:
                # 使用动态类型创建事件
                event = type(event_type, (), {
                    'type': event_type,
                    'timestamp': datetime.now().isoformat(),
                    **data
                })()

                # 异步发布，不阻塞
                import functools
                publish_func = functools.partial(self._event_bus.publish, event, async_mode=True)

                # 在独立线程中发布
                thread = threading.Thread(target=publish_func, daemon=True)
                thread.start()

                # 等待线程完成（带超时）
                thread.join(timeout=self.config.event_publish_timeout)

                if thread.is_alive():
                    raise TimeoutError(f"Event publish timeout after {self.config.event_publish_timeout}s")

                return  # 成功发布

            except Exception as e:
                last_error = e
                retry_count += 1

                if retry_count < max_retries:
                    wait_time = 0.5 * (2 ** (retry_count - 1))  # 指数退避
                    logging.warning(f"[StrategyDiagnoser] Event publish failed (attempt {retry_count}/{max_retries}): {e}")
                    time.sleep(wait_time)
                else:
                    logging.error(f"[StrategyDiagnoser] Event publish failed after {max_retries} retries: {e}")
                    logging.debug(f"[StrategyDiagnoser] Failed event: {event_type}, data_keys={list(data.keys())}")

    def _start_log_worker(self) -> None:
        """启动日志工作线程"""
        def log_worker():
            while not self._stop_log_worker.is_set():
                try:
                    log_entry = self.report._log_queue.get(timeout=1.0)
                    # 根据日志条目中的级别使用相应的日志级别
                    if '[ERROR]' in log_entry:
                        logging.error(f"[Diagnoser] {log_entry}")
                    elif '[WARN]' in log_entry:
                        logging.warning(f"[Diagnoser] {log_entry}")
                    else:
                        logging.info(f"[Diagnoser] {log_entry}")
                except queue.Empty:
                    # 队列为空，继续等待（正常情况，使用DEBUG级别）
                    logging.debug("[Diagnoser] Log queue empty, waiting...")
                    continue
                except Exception as e:
                    logging.error(f"[Diagnoser] Log worker error: {e}")

        self._log_worker_thread = threading.Thread(target=log_worker, daemon=True)
        self._log_worker_thread.start()
    
    def _stop_log_workers(self) -> None:
        """停止日志工作线程"""
        if self._log_worker_thread:
            self._stop_log_worker.set()
            self._log_worker_thread.join(timeout=2.0)
    
    def _log(self, msg: str, level: str = "INFO") -> None:
        """记录日志

        Args:
            msg: 日志内容
            level: 日志级别
        """
        with self._lock:
            self.report.add_log(msg, level)

        # 输出到策略日志（同步）
        if self.strategy and hasattr(self.strategy, 'output'):
            try:
                self.strategy.output(f"[Diagnoser] {msg}")
            except (AttributeError, TypeError, ValueError) as e:
                logging.error(f"[Diagnoser] 输出日志失败：{e}")
                logging.error(f"[Diagnoser] {msg}")
        else:
            # 根据日志级别使用相应的日志方法
            log_func = {
                'DEBUG': logging.debug,
                'INFO': logging.info,
                'WARN': logging.warning,
                'WARNING': logging.warning,
                'ERROR': logging.error,
                'CRITICAL': logging.critical
            }.get(level.upper(), logging.info)

            log_func(f"[Diagnoser] {msg}")
    
    def _run_incremental_diagnosis(self, start_time: float) -> Dict[str, Any]:
        """运行增量诊断（仅检查变化的组件）"""
        self._log("\n【增量诊断模式】", "INFO")
        
        # 简化的增量检查：只检查关键状态变化
        if self.strategy:
            current_trading = getattr(self.strategy, 'my_trading', False)
            last_trading = self._last_diagnosis_result.get('state', {}).get('my_trading', False)
            
            if current_trading != last_trading:
                self._log(f"交易状态变化：{last_trading} -> {current_trading}", "INFO")
                self._diagnose_state()
            else:
                self._log("状态无显著变化，跳过详细诊断", "INFO")
        
        # 记录诊断耗时
        elapsed = time.time() - start_time
        self.report.add_metric('diagnosis_duration_ms', elapsed * 1000)
        self.report.add_metric('incremental_mode', True)
        
        self._log("=" * 60)
        self._log("增量诊断完成")
        self._log(f"健康评分：{self.report.get_health_score():.1f}/100")
        self._log("=" * 60)
        
        result = self.report.to_dict()
        self._last_diagnosis_result = result
        
        return result
    
    def run_full_diagnosis(self, force_full: bool = False) -> Dict[str, Any]:
        """运行完整诊断
        
        Args:
            force_full: 强制全量诊断（忽略增量诊断配置）
        
        Returns:
            Dict: 诊断报告
        """
        with self._lock:
            self._log("=" * 60)
            self._log("开始策略全链路诊断")
            self._log("=" * 60)
            
            start_time = time.time()
            
            # 增量诊断模式
            if (self.config.enable_incremental_diagnosis and 
                not force_full and 
                self._last_diagnosis_result):
                return self._run_incremental_diagnosis(start_time)
            
            if self.strategy is None:
                self._log("警告：策略实例为 None，仅运行基础检查", "WARN")
                self._diagnose_basic()
            else:
                self._diagnose_data_source()
                self._diagnose_instruments()
                self._diagnose_subscriptions()
                self._diagnose_klines()
                self._diagnose_state()
            
            # 记录诊断耗时
            elapsed = time.time() - start_time
            self.report.add_metric('diagnosis_duration_ms', elapsed * 1000)
            
            self._generate_recommendations()
            
            self._log("=" * 60)
            self._log("诊断完成")
            self._log(f"断点：{len(self.report.breakpoints)}")
            self._log(f"警告：{len(self.report.warnings)}")
            self._log(f"错误：{len(self.report.errors)}")
            self._log(f"健康评分：{self.report.get_health_score():.1f}/100")
            self._log("=" * 60)
            
            # 缓存诊断结果
            result = self.report.to_dict()
            self._last_diagnosis_result = result
            
            # 发布诊断完成事件（带重试）
            self._publish_event_with_retry('DiagnosisCompleted', result)
            
            return result
    
    def _diagnose_basic(self) -> None:
        """基础诊断"""
        self._log("\n【基础诊断】")
        
        # 检查 Python 版本
        import sys
        self._log(f"Python 版本：{sys.version}")
        self.report.add_metric('python_version', sys.version.split()[0])
        
        # 检查关键模块
        modules = ['threading', 'datetime', 'collections', 'typing']
        for mod in modules:
            try:
                __import__(mod)
                self._log(f"模块 {mod}: OK")
            except ImportError as e:
                error_msg = f"模块 {mod} 不可用"
                self.report.add_error(error_msg)
                logging.error(f"[Diagnoser] {error_msg}: {e}")
    
    def _diagnose_data_source(self) -> None:
        """诊断数据源"""
        self._log("\n【数据源诊断】")
        s = self.strategy
        
        # 检查 infini
        infini = getattr(s, 'infini', None)
        if infini is None:
            self.report.add_warning("infini 对象为 None")
            self.report.add_breakpoint(
                "DataSource", "infini", "infini 对象为 None",
                "无法访问交易平台", "CRITICAL",
                "检查平台连接配置"
            )
        else:
            self._log("infini 对象：OK")
        
        # 检查 market_center
        mc = getattr(s, 'market_center', None)
        if mc is None:
            self.report.add_warning("market_center 为 None")
            self.report.add_breakpoint(
                "DataSource", "market_center", "market_center 为 None",
                "无法获取行情数据", "HIGH",
                "检查 MarketCenter 初始化"
            )
        else:
            self._log("market_center: OK")
    
    def _diagnose_instruments(self) -> None:
        """诊断合约加载"""
        self._log("\n【合约加载诊断】")
        s = self.strategy
        
        # 检查 params
        params = getattr(s, 'params', None)
        if params is None:
            self.report.add_breakpoint(
                "Instrument", "params", "参数对象为 None",
                "无法获取配置", "CRITICAL",
                "检查参数初始化"
            )
            return
        
        # 检查关键参数
        key_params = ['subscribe_options', 'future_products', 'option_products']
        for p in key_params:
            val = getattr(params, p, None)
            if val is None:
                self.report.add_warning(f"参数 {p} 未设置")
            else:
                self._log(f"参数 {p}: {val}")
        
        # 检查合约列表
        fut_insts = getattr(s, 'future_instruments', None)
        if fut_insts is None:
            self.report.add_breakpoint(
                "Instrument", "future_instruments", "期货合约列表为 None",
                "无法计算期权宽度", "CRITICAL",
                "检查期货合约加载逻辑"
            )
        elif len(fut_insts) == 0:
            self.report.add_breakpoint(
                "Instrument", "future_instruments", "期货合约列表为空",
                "无标的期货数据", "HIGH",
                "检查合约代码配置"
            )
        else:
            self._log(f"期货合约数：{len(fut_insts)}")
        
        opt_insts = getattr(s, 'option_instruments', None)
        if opt_insts is None:
            self.report.add_breakpoint(
                "Instrument", "option_instruments", "期权合约字典为 None",
                "无法加载期权数据", "CRITICAL",
                "检查期权合约加载逻辑"
            )
        elif len(opt_insts) == 0:
            self.report.add_breakpoint(
                "Instrument", "option_instruments", "期权合约字典为空",
                "无期权数据可用", "HIGH",
                "检查期权合约代码配置"
            )
        else:
            self._log(f"期权合约数：{len(opt_insts)}")
    
    def _diagnose_subscriptions(self) -> None:
        """诊断订阅状态"""
        self._log("\n【订阅状态诊断】")
        s = self.strategy
        
        # 检查订阅方法
        if not hasattr(s, 'subscribe'):
            self.report.add_breakpoint(
                "Subscription", "subscribe", "缺少 subscribe 方法",
                "无法订阅行情", "CRITICAL",
                "确保继承 BaseStrategy"
            )
            return
        
        # 检查订阅标志
        subscribe_flag = getattr(s, 'subscribe_flag', False)
        if not subscribe_flag:
            self.report.add_warning("subscribe_flag 为 False")
            self.report.add_breakpoint(
                "Subscription", "subscribe_flag", "未设置订阅标志",
                "可能未成功订阅", "MEDIUM",
                "检查 subscribe() 调用"
            )
        else:
            self._log("订阅标志：OK")
    
    def _diagnose_klines(self) -> None:
        """诊断 K 线数据"""
        self._log("\n【K 线数据诊断】")
        s = self.strategy
        
        # 检查 K 线字典
        kline_data = getattr(s, 'kline_data', None)
        if kline_data is None:
            self.report.add_breakpoint(
                "Kline", "kline_data", "K 线字典为 None",
                "无法获取 K 线数据", "CRITICAL",
                "检查 K 线初始化"
            )
            return
        
        # 检查 K 线数量
        if len(kline_data) == 0:
            self.report.add_warning("K 线数据为空")
        else:
            self._log(f"K 线合约数：{len(kline_data)}")
            
            # 检查最新 K 线
            for inst_id, kline in list(kline_data.items())[:3]:
                if hasattr(kline, 'close'):
                    close_array = kline.close
                    if close_array and len(close_array) > 0:
                        latest_price = close_array[-1]
                        self._log(f"{inst_id} 最新价：{latest_price}")
                    else:
                        self.report.add_warning(f"{inst_id} K 线数据为空")
                else:
                    self.report.add_warning(f"{inst_id} K 线对象无效")
    
    def _diagnose_state(self) -> None:
        """诊断状态管理"""
        self._log("\n【状态管理诊断】")
        s = self.strategy
        
        # 检查关键状态
        state_attributes = ['my_state', 'my_is_running', 'my_trading']
        for attr in state_attributes:
            val = getattr(s, attr, None)
            if val is None:
                self.report.add_warning(f"状态 {attr} 未定义")
            else:
                self._log(f"{attr}: {val}")
        
        # 检查交易状态
        trading = getattr(s, 'my_trading', False)
        if not trading:
            self.report.add_warning("当前不在交易状态")
    
    def _generate_recommendations(self) -> None:
        """生成修复建议"""
        self._log("\n【生成修复建议】")
        
        # 根据断点生成建议
        for bp in self.report.breakpoints:
            if bp['severity'] == 'CRITICAL':
                self.report.add_recommendation(f"[紧急] {bp['fix']} - {bp['component']}: {bp['issue']}")
            elif bp['severity'] == 'HIGH':
                self.report.add_recommendation(f"[重要] {bp['fix']} - {bp['component']}")
        
        # 根据警告生成建议
        if len(self.report.warnings) > 5:
            self.report.add_recommendation("[建议] 检查策略初始化流程，确保所有组件正确加载")
        
        # 根据错误生成建议
        if len(self.report.errors) > 0:
            self.report.add_recommendation("[严重] 优先解决所有错误，然后处理警告")
        
        # 健康评分建议
        health_score = self.report.get_health_score()
        if health_score >= 90:
            self._log("健康状态：优秀", "INFO")
        elif health_score >= 70:
            self._log("健康状态：良好", "INFO")
            self.report.add_recommendation("[建议] 处理 HIGH 级别断点可提升健康度")
        elif health_score >= 50:
            self._log("健康状态：一般", "WARN")
            self.report.add_recommendation("[注意] 建议处理 CRITICAL 和 HIGH 级别断点")
        else:
            self._log("健康状态：较差", "ERROR")
            self.report.add_recommendation("[紧急] 立即处理所有 CRITICAL 级别断点")
    
    def _publish_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """发布事件
        
        Args:
            event_type: 事件类型
            data: 事件数据
        """
        if self._event_bus:
            try:
                event = type(event_type, (), {
                    'type': event_type,
                    'timestamp': datetime.now().isoformat(),
                    **data
                })()
                self._event_bus.publish(event, async_mode=True)
            except Exception as e:
                # 记录详细日志（包含错误信息、事件详情和堆栈跟踪）
                error_msg = f"Failed to publish {event_type}: {e}"
                logging.error(f"[StrategyDiagnoser] {error_msg}")
                logging.info(f"[StrategyDiagnoser] Event details: type={event_type}, data_keys={list(data.keys())}")
                logging.debug(f"[StrategyDiagnoser] Exception traceback: {traceback.format_exc()}")
    
    def get_quick_report(self) -> Dict[str, Any]:
        """获取快速报告
        
        Returns:
            Dict: 简化版诊断报告
        """
        with self._lock:
            return {
                "timestamp": datetime.now().isoformat(),
                "health_score": self.report.get_health_score(),
                "breakpoint_count": len(self.report.breakpoints),
                "warning_count": len(self.report.warnings),
                "error_count": len(self.report.errors),
                "critical_count": sum(1 for bp in self.report.breakpoints if bp['severity'] == 'CRITICAL')
            }


# 导出公共接口
__all__ = [
    'DiagnosisReport',
    'StrategyDiagnoser',
    # Exception handling decorators (新增)
    'handle_import_errors',
    'log_exceptions',
    'safe_call',
    'optional_import',
    'required_import',
]


# ============================================================================
# Exception Handling Decorators - 统一异常处理装饰器
# ============================================================================

def handle_import_errors(allow_failure=False, default_value=None):
    """Decorator for handling import errors with configurable failure tolerance.
    
    Args:
        allow_failure: If True, import failures are logged as warnings and return default_value.
                      If False, import failures are logged as errors and re-raised.
        default_value: Value to return when import fails and allow_failure is True.
    
    Returns:
        Decorated function with standardized import error handling.
    
    Example:
        @handle_import_errors(allow_failure=True, default_value=None)
        def load_market_data_service():
            from market_data_service import MarketDataService
            return MarketDataService
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ImportError as e:
                if allow_failure:
                    logging.warning(f"[import] Optional import failed: {e}")
                    return default_value
                else:
                    logging.error(f"[import] Required import failed: {e}")
                    logging.exception("[import] Traceback:")
                    raise
        return wrapper
    return decorator


def log_exceptions(log_level=logging.ERROR, reraise=True):
    """Decorator for logging exceptions with configurable behavior.
    
    Args:
        log_level: Logging level for exception messages (default: ERROR).
        reraise: If True, re-raise the exception after logging. 
                 If False, swallow the exception and return None.
    
    Returns:
        Decorated function with standardized exception logging.
    
    Example:
        @log_exceptions(log_level=logging.WARNING, reraise=False)
        def risky_operation():
            # This will log exceptions but not crash
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.log(log_level, f"[exception] {func.__name__} failed: {e}")
                logging.exception(f"[exception] {func.__name__} traceback:")
                
                if reraise:
                    raise
                return None
        return wrapper
    return decorator


def safe_call(default_value=None, catch_exceptions=(Exception,)):
    """Decorator for safely calling functions that might fail.
    
    This decorator catches specified exceptions and returns a default value,
    preventing exceptions from propagating.
    
    Args:
        default_value: Value to return when an exception occurs.
        catch_exceptions: Tuple of exception types to catch (default: all Exceptions).
    
    Returns:
        Decorated function that safely handles exceptions.
    
    Example:
        @safe_call(default_value=0, catch_exceptions=(ValueError, TypeError))
        def parse_int(value):
            return int(value)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except catch_exceptions:
                return default_value
        return wrapper
    return decorator


# Convenience functions for common patterns

def optional_import(func):
    """Convenience decorator for optional imports.
    
    Equivalent to: @handle_import_errors(allow_failure=True, default_value=None)
    """
    return handle_import_errors(allow_failure=True, default_value=None)(func)


def required_import(func):
    """Convenience decorator for required imports.
    
    Equivalent to: @handle_import_errors(allow_failure=False)
    """
    return handle_import_errors(allow_failure=False)(func)
