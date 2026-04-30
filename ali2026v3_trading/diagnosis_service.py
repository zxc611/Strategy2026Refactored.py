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
from contextlib import contextmanager
from functools import wraps
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple, Protocol, runtime_checkable
from dataclasses import dataclass, field
from collections import deque

from ali2026v3_trading.analytics_service import normalize_instrument_id


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
            # ✅ P2#119修复：正确截断逻辑 - 保留所有ERROR/WARN + 最近50%的其他日志
            critical_logs = [l for l in self.logs if '[ERROR]' in l or '[WARN]' in l]
            other_logs = [l for l in self.logs if '[ERROR]' not in l and '[WARN]' not in l]
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

        # ✅ P1#73修复：合并两轮相同遍历为一轮，缺失属性时记录日志并返回False
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

                # ✅ P1#74修复：直接调用publish而非创建新线程，避免线程创建/销毁开销
                self._event_bus.publish(event, async_mode=True)

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
    'DiagnosisProbeManager',
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
        def load_storage_manager():
            # ✅ 传递渠道唯一：使用工厂函数获取单例，禁止直接引用类
            from ali2026v3_trading.storage import get_instrument_data_manager
            return get_instrument_data_manager()
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


# ============================================================================
# Tick探针管理 - 集成到诊断服务
# ============================================================================

_tick_probe_stats = {
    'entry': {'count': 0, 'last_log': 0},
    'dispatched': {'count': 0, 'last_log': 0},
    'saved': {'count': 0, 'last_log': 0},
    'error': {'count': 0, 'last_log': 0, 'errors': []}
}

_tick_probe_interval = 30.0
_tick_probe_lock = threading.Lock()  # ✅ P2#120修复：保护_tick_probe_stats的并发访问

def record_tick_probe(stage: str, instrument_id: str, price: float, error_msg: Optional[str] = None) -> None:
    """记录Tick探针数据（错误立即输出，成功30秒汇总）"""
    now = time.time()
    if stage not in _tick_probe_stats:
        return
    
    # ✅ P2#120修复：加锁保护模块级全局变量的并发访问
    with _tick_probe_lock:
        stats = _tick_probe_stats[stage]
        stats['count'] += 1
        
        # 错误立即输出
        if stage == 'error' and error_msg:
            stats['errors'].append({'time': now, 'instrument_id': instrument_id, 'price': price, 'error': error_msg})
            if len(stats['errors']) > 100:
                stats['errors'] = stats['errors'][-50:]
            logging.error(f"[PROBE_TICK_ERROR] {instrument_id} @ {price}: {error_msg}")
        
        # 30秒汇总输出
        if now - stats.get('last_log', 0) >= _tick_probe_interval:
            count = stats['count']
            if count > 0:
                if stage == 'error':
                    errors = stats['errors']
                    if errors:
                        for err in errors[-5:]:
                            logging.error(f"[PROBE_TICK_SUMMARY] ERROR | {err['instrument_id']} @ {err['price']} | {err['error']}")
                        logging.error(f"[PROBE_TICK_SUMMARY] Total errors in last 30s: {len(errors)} (showing last 5)")
                else:
                    names = {'entry': 'Entry', 'dispatched': 'Dispatched', 'saved': 'Saved'}
                    logging.info(f"[PROBE_TICK_SUMMARY] {names.get(stage, stage)} | Count: {count} ticks in last 30s")
            stats['count'] = 0
            stats['last_log'] = now


# ============================================================================
# 重点监控合约诊断模块
# ============================================================================

# 监控合约定义。
# 在原有7期货+7期权基础上，补充商品期权额外监测节点，覆盖更多已确认存在的strike。
# 根据实盘标准格式配置（参考：期权代码映射表、启动日志和OptionStatus样本）
MONITORED_CONTRACTS = {
    # 期货（7个）
    'IH2605': {'exchange': 'CFFEX', 'name': '上证50股指期货', 'type': 'future'},
    'IF2605': {'exchange': 'CFFEX', 'name': '沪深300股指期货', 'type': 'future'},
    'IM2605': {'exchange': 'CFFEX', 'name': '中证1000股指期货', 'type': 'future'},
    'cu2605': {'exchange': 'SHFE', 'name': '铜期货', 'type': 'future'},
    'au2605': {'exchange': 'SHFE', 'name': '黄金期货', 'type': 'future'},
    'al2605': {'exchange': 'SHFE', 'name': '铝期货', 'type': 'future'},
    'zn2605': {'exchange': 'SHFE', 'name': '锌期货', 'type': 'future'},
    
    # 期权（7个）- 使用实盘标准格式和合理行权价
    # 股指期权：IH→HO, IF→IO, IM→MO （期权代码与期货代码不同）
    'HO2605-C-2800': {'underlying': 'IH2605', 'exchange': 'CFFEX', 'name': 'IH看涨2800', 'type': 'option'},  # IH的期权代码是HO
    'IO2605-C-4200': {'underlying': 'IF2605', 'exchange': 'CFFEX', 'name': 'IF看涨4200', 'type': 'option'},  # IF的期权代码是IO
    'MO2605-C-7000': {'underlying': 'IM2605', 'exchange': 'CFFEX', 'name': 'IM看涨7000', 'type': 'option'},  # IM的期权代码是MO
    # 商品期权：品种+月份+C/P+行权价 （大写C表示CALL，大写P表示PUT）
    'cu2605C90000': {'underlying': 'cu2605', 'exchange': 'SHFE', 'name': '铜看涨90000', 'type': 'option'},  # 行权价范围：70000-124000
    'au2605C1000': {'underlying': 'au2605', 'exchange': 'SHFE', 'name': '黄金看涨1000', 'type': 'option'},  # 行权价范围：560-1744
    'al2605C24000': {'underlying': 'al2605', 'exchange': 'SHFE', 'name': '铝看涨24000', 'type': 'option'},  # 行权价范围：18900-29400
    'zn2605C26000': {'underlying': 'zn2605', 'exchange': 'SHFE', 'name': '锌看涨26000', 'type': 'option'},  # 行权价范围：19200-30000

    # 商品期权扩展监测节点：优先选取日志中已出现或已核验存在的strike
    'cu2605C104000': {'underlying': 'cu2605', 'exchange': 'SHFE', 'name': '铜看涨104000', 'type': 'option'},
    'au2605C1032': {'underlying': 'au2605', 'exchange': 'SHFE', 'name': '黄金看涨1032', 'type': 'option'},
    'al2605C22600': {'underlying': 'al2605', 'exchange': 'SHFE', 'name': '铝看涨22600', 'type': 'option'},
    'al2605C25200': {'underlying': 'al2605', 'exchange': 'SHFE', 'name': '铝看涨25200', 'type': 'option'},
    'al2605C26200': {'underlying': 'al2605', 'exchange': 'SHFE', 'name': '铝看涨26200', 'type': 'option'},
    'zn2605C20000': {'underlying': 'zn2605', 'exchange': 'SHFE', 'name': '锌看涨20000', 'type': 'option'},
}

MONITORED_CONTRACT_SET = set(MONITORED_CONTRACTS.keys())
MONITORED_CONTRACT_COUNT = len(MONITORED_CONTRACTS)
MONITORED_DIAG_LABEL = f'{MONITORED_CONTRACT_COUNT}合约诊断'
_PARSE_TRACE_KEYS: deque = deque(maxlen=10000)
_PARSE_TRACE_LOCK = threading.RLock()


def is_monitored_contract(instrument_id: str) -> bool:
    """判断是否为监控合约 - 受diagnostic_output全局开关控制
    trade模式(diagnostic_output=False): 关闭所有诊断探针，消除日志洪水
    交易调试模式(diagnostic_output=True): 启用诊断探针(60s汇总节流)
    """
    try:
        from ali2026v3_trading.config_service import get_cached_params
        cached_params = get_cached_params()
        if cached_params and isinstance(cached_params, dict):
            strategy = cached_params.get('strategy')
            if strategy:
                params = getattr(strategy, 'params', None)
                if params:
                    return bool(getattr(params, 'diagnostic_output', False))
        return False
    except Exception:
        return False


def get_contract_info(instrument_id: str) -> Optional[Dict[str, Any]]:
    """获取监控合约信息"""
    return MONITORED_CONTRACTS.get(instrument_id)


def _should_trace_parse_contract(original_id: str, transformed_id: Optional[str] = None) -> bool:
    """判断是否需要追踪解析变形（入口标准化，内部不再重复）"""
    if original_id in MONITORED_CONTRACT_SET or transformed_id in MONITORED_CONTRACT_SET:
        return True
    return bool(original_id and transformed_id and original_id != transformed_id)


def diagnose_parse_transform(stage: str, original_id: str, transformed_id: str,
                             detail: Optional[str] = None, level: str = "INFO") -> None:
    """记录解析链中的合约变形。
    
    ID唯一修复：信任调用方已标准化，内部不再重复标准化。
    detail是描述文本，不做normalize_instrument_id处理。
    """
    original = str(original_id or '').strip()
    transformed = str(transformed_id or '').strip()
    detail_text = str(detail).strip() if detail else None
    
    if not _should_trace_parse_contract(original, transformed):
        return

    trace_key = (stage, original, transformed, detail_text)
    with _PARSE_TRACE_LOCK:
        if trace_key in _PARSE_TRACE_KEYS:
            return
        _PARSE_TRACE_KEYS.append(trace_key)

    message = f"[PROBE_PARSE_TRANSFORM] Stage={stage} | Original={original} | Transformed={transformed}"
    if detail_text:
        message += f" | Detail={detail_text}"

    log_func = {
        'DEBUG': logging.debug,
        'INFO': logging.info,
        'WARN': logging.warning,
        'WARNING': logging.warning,
        'ERROR': logging.error,
    }.get(str(level or 'INFO').upper(), logging.info)
    log_func(message)


def diagnose_parse_failure(stage: str, instrument_id: str, reason: str) -> None:
    """记录解析链失败。ID唯一修复：信任调用方已标准化"""
    instrument = str(instrument_id or '').strip()
    if instrument not in MONITORED_CONTRACT_SET:
        return
    logging.error("[PROBE_PARSE_FAILURE] Stage=%s | Instrument=%s | Reason=%s", stage, instrument, reason)


import re

_CONTRACT_PATTERN = re.compile(r'^([A-Za-z]{2,4})(\d{4})(?:-([CP])-(\d+)|([CP])(\d+))?$')


def validate_contract_format(instrument_id: str) -> tuple:
    """验证合约代码是否符合实盘标准格式
    
    Returns:
        (is_valid, expected_format, actual_format)
    """
    info = MONITORED_CONTRACTS.get(instrument_id)
    if not info:
        return False, "未知合约", instrument_id
    
    contract_type = info['type']
    
    if contract_type == 'future':
        match = _CONTRACT_PATTERN.match(instrument_id)
        expected = "期货标准格式: XX####"
        if match and not match.group(3) and not match.group(5):
            return True, expected, instrument_id
        return False, expected, instrument_id
    
    if contract_type == 'option':
        underlying = info.get('underlying', '')
        exchange = info.get('exchange', '')
        
        try:
            from ali2026v3_trading.subscription_manager import SubscriptionManager
            parsed = SubscriptionManager.parse_option(instrument_id)
            if parsed:
                if exchange == 'CFFEX':
                    prefix = parsed.get('product', '')
                    if underlying.startswith('IH') and prefix != 'HO':
                        return False, "股指期权应使用HO代码（IH→HO）", instrument_id
                    if underlying.startswith('IF') and prefix != 'IO':
                        return False, "股指期权应使用IO代码（IF→IO）", instrument_id
                    if underlying.startswith('IM') and prefix != 'MO':
                        return False, "股指期权应使用MO代码（IM→MO）", instrument_id
                    return True, "股指期权标准格式: XX####-C/P-#####", instrument_id
                return True, "商品期权标准格式: xx####C/P#####", instrument_id
        except (ValueError, KeyError, TypeError):
            pass
        
        if exchange == 'CFFEX':
            return False, "股指期权标准格式: XX####-C/P-#####", instrument_id
        return False, "商品期权标准格式: xx####C/P#####", instrument_id
    
    return False, "未知类型", instrument_id


def log_diagnostic_event(probe_name: str, instrument_id: str, **kwargs) -> None:
    """记录诊断事件（统一入口）
    
    Args:
        probe_name: 探针名称（如 PROBE_TICK_ENTRY）
        instrument_id: 合约代码
        **kwargs: 额外参数（会自动格式化为日志）
    """
    if not is_monitored_contract(instrument_id):
        return
    
    info = get_contract_info(instrument_id)
    contract_type = info['type'] if info else 'unknown'
    
    # 构建日志消息
    parts = [f"[{probe_name}] Contract={instrument_id} | Type={contract_type}"]
    for key, value in kwargs.items():
        parts.append(f"{key}={value}")
    
    message = " | ".join(parts)
    
    # 根据探针类型选择日志级别
    if 'FAILED' in probe_name or 'ERROR' in probe_name:
        logging.error(message)
    elif 'WARNING' in probe_name:
        logging.warning(message)
    else:
        logging.info(message)


# ============================================================================
# 8个环节的诊断函数
# ============================================================================

def diagnose_format_validation(instrument_id: str, is_valid_format: bool, 
                              expected_format: Optional[str] = None,
                              actual_format: Optional[str] = None) -> None:
    """环节0c: 标准格式核对状态检查（单个合约）
    
    Args:
        instrument_id: 合约代码
        is_valid_format: 是否符合标准格式
        expected_format: 期望的标准格式（可选）
        actual_format: 实际格式（可选）
    """
    if not is_monitored_contract(instrument_id):
        return
    
    status = "✅" if is_valid_format else "❌"
    
    if is_valid_format:
        logging.debug(f"[PROBE_FORMAT_VALIDATION] {instrument_id} | Format={status}")
    else:
        msg = f"[PROBE_FORMAT_VALIDATION_FAILED] {instrument_id} | Format={status}"
        if expected_format and actual_format:
            msg += f" | Expected={expected_format} | Actual={actual_format}"
        logging.error(msg)


def diagnose_config_file_presence(instrument_id: str, exists_in_file: bool) -> None:
    """环节0a: 配置表文件存在性检查（单个合约）
    
    Args:
        instrument_id: 合约代码
        exists_in_file: 是否在subscription_options_fixed.txt文件中
    """
    if not is_monitored_contract(instrument_id):
        return
    
    status = "✅" if exists_in_file else "❌"
    logging.info(f"[PROBE_CONFIG_FILE] {instrument_id} | InFile={status}")


def diagnose_config_loaded_status(instrument_id: str, loaded_in_memory: bool,
                                 in_cache: bool = False) -> None:
    """环节0b: 配置表加载状态检查（单个合约）
    
    Args:
        instrument_id: 合约代码
        loaded_in_memory: 是否已加载到内存（params_service）
        in_cache: 是否在instrument_cache中
    """
    if not is_monitored_contract(instrument_id):
        return
    
    if loaded_in_memory:
        cache_status = "✅" if in_cache else "⚠️"
        logging.debug(f"[PROBE_CONFIG_LOADED] {instrument_id} | Loaded=✅ | InCache={cache_status}")
    else:
        logging.error(f"[PROBE_CONFIG_LOADED_FAILED] {instrument_id} | Loaded=❌ | Reason=未加载到内存")


def diagnose_pre_registration(instrument_id: str, in_subscribe_list: bool, 
                             registered: bool) -> None:
    """环节0b: 预注册状态检查（单个合约）
    
    Args:
        instrument_id: 合约代码
        in_subscribe_list: 是否在订阅列表中
        registered: 是否已正式注册
    """
    if not is_monitored_contract(instrument_id):
        return
    
    if in_subscribe_list and not registered:
        # 在订阅列表但未注册 - 需要关注
        logging.warning(f"[PROBE_PRE_REGISTRATION] {instrument_id} | InSubscribeList=✅ | Registered=❌ | Status=待注册")
    elif in_subscribe_list and registered:
        # 正常流程
        logging.debug(f"[PROBE_PRE_REGISTRATION] {instrument_id} | InSubscribeList=✅ | Registered=✅")
    else:
        # 不在订阅列表
        logging.error(f"[PROBE_PRE_REGISTRATION_FAILED] {instrument_id} | InSubscribeList=❌ | Reason=未加入订阅列表")


def diagnose_subscription(instrument_id: str, contract_type: str, success: bool,
                         reason: Optional[str] = None) -> None:
    """环节1: 合约订阅诊断（单个合约）"""
    if not is_monitored_contract(instrument_id):
        return
    
    if success:
        logging.info(f"[PROBE_SUBSCRIBE] {instrument_id} | Type={contract_type} | Status=✅")
    else:
        logging.error(f"[PROBE_SUBSCRIBE_FAILED] {instrument_id} | Type={contract_type} | Reason={reason or 'Unknown'}")


def diagnose_registration(instrument_id: str, source: str, internal_id: int,
                         instrument_type: Optional[str] = None) -> None:
    """环节2: 合约注册诊断（单个合约）"""
    if not is_monitored_contract(instrument_id):
        return
    
    logging.info(f"[PROBE_REGISTRATION] {instrument_id} | Source={source} | "
                f"InternalID={internal_id}" + (f" | Type={instrument_type}" if instrument_type else ""))


def diagnose_id_lookup(instrument_id: str, internal_id: int, instrument_type: str) -> None:
    """环节3: ID转换诊断（单个合约）"""
    if not is_monitored_contract(instrument_id):
        return
    
    logging.debug(f"[PROBE_ID_LOOKUP] {instrument_id} -> InternalID={internal_id} | Type={instrument_type}")


_TICK_ENTRY_SUMMARY_INTERVAL = 60.0
_tick_entry_last_output_time = 0.0
_tick_entry_accum = {'count': 0, 'contracts': set(), 'last_price': 0.0, 'last_vol': 0}
_tick_entry_accum_lock = threading.Lock()


def diagnose_tick_entry(instrument_id: str, price: float, volume: int,
                       open_interest: float, contract_type: str) -> None:
    """环节4: Tick入口诊断 - 60s汇总节流模式
    trade模式: is_monitored_contract()返回False，直接跳过
    交易调试模式: 60秒汇总一次输出，避免每tick一条INFO洪水
    """
    if not is_monitored_contract(instrument_id):
        return
    global _tick_entry_last_output_time
    now = time.time()
    with _tick_entry_accum_lock:
        _tick_entry_accum['count'] += 1
        _tick_entry_accum['contracts'].add(instrument_id)
        _tick_entry_accum['last_price'] = price
        _tick_entry_accum['last_vol'] = volume
        if now - _tick_entry_last_output_time >= _TICK_ENTRY_SUMMARY_INTERVAL:
            count = _tick_entry_accum['count']
            n_contracts = len(_tick_entry_accum['contracts'])
            logging.info(f"[PROBE_TICK_ENTRY_SUMMARY] "
                        f"ticks={count} contracts={n_contracts} "
                        f"sample=({instrument_id} {contract_type} P={price} V={volume} OI={open_interest})")
            _tick_entry_accum['count'] = 0
            _tick_entry_accum['contracts'].clear()
            _tick_entry_last_output_time = now


def diagnose_tick_buffer(buffer_size: int, threshold: int, fill_rate: float) -> None:
    """环节5a: Tick缓冲状态诊断（汇总）"""
    logging.debug(f"[PROBE_TICK_BUFFER] Buffer={buffer_size}/{threshold} | FillRate={fill_rate:.1f}%")


def diagnose_tick_flush(flushed_count: int, monitored_count: int) -> None:
    """环节5b: Tick Flush诊断（汇总）"""
    if monitored_count > 0:
        logging.info(f"[PROBE_TICK_FLUSH] Flushed={flushed_count} ticks | Monitored={monitored_count}")


def diagnose_storage_enqueue(instrument_id: str, success: bool, reason: Optional[str] = None) -> None:
    """环节6: Storage入队诊断（单个合约）"""
    if not is_monitored_contract(instrument_id):
        return
    
    if success:
        logging.debug(f"[PROBE_STORAGE_ENQUEUE] {instrument_id} | Status=✅")
    else:
        logging.error(f"[PROBE_STORAGE_ENQUEUE_FAILED] {instrument_id} | Reason={reason or 'Unknown'}")


def diagnose_async_write(instrument_id: str, func_name: str, data_count: int) -> None:
    """环节7: 异步写入诊断（单个合约）"""
    if not is_monitored_contract(instrument_id):
        return
    
    logging.debug(f"[PROBE_ASYNC_WRITE] {instrument_id} | Func={func_name} | DataCount={data_count}")


def diagnose_duckdb_insert(instrument_id: str, data_type: str, count: int) -> None:
    """环节8: DuckDB写入诊断（单个合约）"""
    if not is_monitored_contract(instrument_id):
        return
    
    logging.debug(f"[PROBE_DUCKDB_INSERT] {instrument_id} | Type={data_type} | Count={count}")


class DiagnosisProbeManager:
    """12环节诊断探针统一管理器
    
    按照架构规范，所有实时探针调用应通过此管理器统一分发，
    避免在业务模块中分散调用diagnose_*函数。
    
    使用示例：
        from ali2026v3_trading.diagnosis_service import DiagnosisProbeManager
        
        # 环节1: 订阅探针
        DiagnosisProbeManager.on_subscribe('rb2505', 'future', True)
        
        # 环节2-3: 注册和ID转换探针
        DiagnosisProbeManager.on_register('rb2505', 'storage_register', 12345, 'future')
        
        # 环节4: Tick入口探针
        DiagnosisProbeManager.on_tick_entry('rb2505', 3500.0, 1000, 50000, 'future')
        
        # 环节6: Storage入队探针
        DiagnosisProbeManager.on_storage_enqueue('rb2505', True)
        
        # 环节7: 异步写入探针
        DiagnosisProbeManager.on_async_write('rb2505', '_save_tick_impl', 10)
    """
    
    @staticmethod
    def on_subscribe(instrument_id: str, contract_type: str, success: bool, reason: str = None):
        """环节1: 订阅探针"""
        diagnose_subscription(instrument_id, contract_type, success, reason)
    
    @staticmethod
    def on_register(instrument_id: str, source: str, internal_id: int, instrument_type: str):
        """环节2-3: 注册和ID转换探针"""
        diagnose_registration(instrument_id, source, internal_id, instrument_type)
        diagnose_id_lookup(instrument_id, internal_id, instrument_type)
    
    @staticmethod
    def on_tick_entry(instrument_id: str, price: float, volume: int, open_interest: float, contract_type: str):
        """环节4: Tick入口探针"""
        diagnose_tick_entry(instrument_id, price, volume, open_interest, contract_type)
        DiagnosisProbeManager.record_contract_tick(instrument_id, price, volume, open_interest, contract_type)
    
    @staticmethod
    def on_storage_enqueue(instrument_id: str, success: bool, reason: str = None):
        """环节6: Storage入队探针"""
        diagnose_storage_enqueue(instrument_id, success, reason)
    
    @staticmethod
    def on_async_write(instrument_id: str, func_name: str, data_count: int):
        """环节7: 异步写入探针"""
        diagnose_async_write(instrument_id, func_name, data_count)

    @staticmethod
    def on_parse_transform(stage: str, original_id: str, transformed_id: str,
                           detail: str = None, level: str = 'INFO'):
        """解析链变形探针。"""
        diagnose_parse_transform(stage, original_id, transformed_id, detail, level)

    @staticmethod
    def on_parse_failure(stage: str, instrument_id: str, reason: str):
        """解析链失败探针。"""
        diagnose_parse_failure(stage, instrument_id, reason)

    _startup_lock = threading.RLock()
    _startup_seq = 0
    _startup_run: Optional[Dict[str, Any]] = None
    _contract_watch_lock = threading.RLock()
    _contract_watch_run: Optional[Dict[str, Any]] = None
    _contract_watch_thread: Optional[threading.Thread] = None
    _contract_watch_stop = threading.Event()

    @classmethod
    def start_contract_watch(
        cls,
        subscribed_instruments: Optional[List[str]] = None,
        timeout_seconds: float = 30.0,
        summary_interval_seconds: float = 10.0,
        label: str = MONITORED_DIAG_LABEL,
    ) -> Dict[str, Any]:
        """启动重点合约订阅后到包观察器。"""
        watch_targets = []
        subscribed_set = set(str(item).strip() for item in (subscribed_instruments or []) if str(item).strip())

        for instrument_id, info in MONITORED_CONTRACTS.items():
            watch_targets.append({
                'instrument_id': instrument_id,
                'exchange': info.get('exchange', ''),
                'type': info.get('type', 'unknown'),
                'underlying': info.get('underlying', ''),
                'name': info.get('name', instrument_id),
                'in_subscribe_list': instrument_id in subscribed_set,
                'watch_started_at': time.time(),
                'first_tick_at': None,
                'last_tick_at': None,
                'first_price': None,
                'last_price': None,
                'tick_count': 0,
            })

        run = {
            'run_id': f"contract-watch-{int(time.time())}",
            'label': label,
            'started_at': time.time(),
            'started_wall': datetime.now().isoformat(),
            'timeout_seconds': float(timeout_seconds),
            'summary_interval_seconds': max(1.0, float(summary_interval_seconds)),
            'contracts': {item['instrument_id']: item for item in watch_targets},
            'finished': False,
        }

        with cls._contract_watch_lock:
            cls._contract_watch_run = run
            cls._contract_watch_stop.clear()

        logging.info("=" * 80)
        logging.info(
            "[ContractWatch] 启动 %s | run=%s | total=%d | in_subscribe_list=%d | timeout=%.1fs | interval=%.1fs",
            label,
            run['run_id'],
            len(run['contracts']),
            sum(1 for item in watch_targets if item['in_subscribe_list']),
            run['timeout_seconds'],
            run['summary_interval_seconds'],
        )
        for item in watch_targets:
            logging.info(
                "[ContractWatch] target=%s | type=%s | exchange=%s | underlying=%s | in_subscribe_list=%s | name=%s",
                item['instrument_id'],
                item['type'],
                item['exchange'],
                item['underlying'] or '-',
                'Y' if item['in_subscribe_list'] else 'N',
                item['name'],
            )
        logging.info("=" * 80)

        def _watch_loop() -> None:
            last_emit = 0.0
            while not cls._contract_watch_stop.wait(1.0):
                current_run = cls._get_contract_watch_run()
                if current_run is None:
                    return
                now = time.time()
                if now - last_emit >= current_run['summary_interval_seconds']:
                    cls.emit_contract_watch_summary(reason='periodic')
                    last_emit = now
                if now - current_run['started_at'] >= current_run['timeout_seconds']:
                    cls.emit_contract_watch_summary(reason='timeout', final=True)
                    return

        thread = threading.Thread(
            target=_watch_loop,
            name='ContractWatchSummary',
            daemon=True,
        )
        with cls._contract_watch_lock:
            cls._contract_watch_thread = thread
        thread.start()
        logging.info(
            "[ContractWatch] START_CONFIRMED run=%s thread=%s alive=%s timeout=%.1fs interval=%.1fs subscribed_targets=%d total_targets=%d",
            run['run_id'],
            thread.name,
            thread.is_alive(),
            run['timeout_seconds'],
            run['summary_interval_seconds'],
            sum(1 for item in watch_targets if item['in_subscribe_list']),
            len(watch_targets),
        )
        return run

    @classmethod
    def _get_contract_watch_run(cls) -> Optional[Dict[str, Any]]:
        with cls._contract_watch_lock:
            return cls._contract_watch_run

    @classmethod
    def record_contract_tick(
        cls,
        instrument_id: str,
        price: float,
        volume: int,
        open_interest: float,
        contract_type: str,
    ) -> None:
        """记录重点合约真实到包。"""
        instrument = str(instrument_id or '').strip()
        if not instrument:
            return

        with cls._contract_watch_lock:
            run = cls._contract_watch_run
            if run is None or run.get('finished'):
                return
            state = run['contracts'].get(instrument)
            if state is None:
                return

            now = time.time()
            first_tick = state['first_tick_at'] is None
            state['tick_count'] += 1
            state['last_tick_at'] = now
            state['last_price'] = price
            if first_tick:
                state['first_tick_at'] = now
                state['first_price'] = price

        if first_tick:
            elapsed = now - run['started_at']
            logging.info(
                "[ContractWatch] FIRST_TICK run=%s contract=%s type=%s price=%s vol=%s oi=%s elapsed=%.3fs",
                run['run_id'], instrument, contract_type, price, volume, open_interest, elapsed,
            )

    @classmethod
    def emit_contract_watch_summary(cls, reason: str, final: bool = False) -> None:
        """输出重点合约订阅后到包汇总。"""
        with cls._contract_watch_lock:
            run = cls._contract_watch_run
            if run is None:
                return

            now = time.time()
            total = len(run['contracts'])
            in_subscribe = [item for item in run['contracts'].values() if item['in_subscribe_list']]
            first_tick = [item for item in run['contracts'].values() if item['first_tick_at'] is not None]
            no_tick = [item for item in run['contracts'].values() if item['in_subscribe_list'] and item['first_tick_at'] is None]
            not_subscribed = [item for item in run['contracts'].values() if not item['in_subscribe_list']]
            elapsed = now - run['started_at']

            if final:
                run['finished'] = True
                cls._contract_watch_stop.set()

        logging.info("-" * 80)
        logging.info(
            "[ContractWatch] summary run=%s reason=%s final=%s elapsed=%.3fs total=%d in_subscribe=%d first_tick=%d no_tick=%d not_subscribed=%d",
            run['run_id'], reason, final, elapsed, total, len(in_subscribe), len(first_tick), len(no_tick), len(not_subscribed),
        )

        for item in sorted(first_tick, key=lambda current: current['first_tick_at'] or 0.0):
            first_elapsed = (item['first_tick_at'] or now) - run['started_at']
            last_elapsed = (item['last_tick_at'] or now) - run['started_at']
            logging.info(
                "[ContractWatch] OK contract=%s type=%s ticks=%d first=%.3fs last=%.3fs first_price=%s last_price=%s",
                item['instrument_id'], item['type'], item['tick_count'], first_elapsed, last_elapsed, item['first_price'], item['last_price'],
            )

        for item in no_tick:
            logging.warning(
                "[ContractWatch] NO_TICK contract=%s type=%s waited=%.3fs name=%s",
                item['instrument_id'], item['type'], elapsed, item['name'],
            )

        for item in not_subscribed:
            logging.warning(
                "[ContractWatch] NOT_IN_SUBSCRIBE_LIST contract=%s type=%s name=%s",
                item['instrument_id'], item['type'], item['name'],
            )

        logging.info("-" * 80)

    @classmethod
    def stop_contract_watch(cls, reason: str = 'stop') -> None:
        """停止重点合约观察器，并输出最终汇总。"""
        run = cls._get_contract_watch_run()
        if run is None:
            logging.warning("[ContractWatch] STOP_REQUEST reason=%s but no active run", reason)
            return
        logging.info(
            "[ContractWatch] STOP_REQUEST run=%s reason=%s finished=%s",
            run.get('run_id', '-'),
            reason,
            run.get('finished', False),
        )
        cls.emit_contract_watch_summary(reason=reason, final=True)

    @classmethod
    def begin_startup_timeline(cls, source: str, strategy_id: str = None, reset: bool = False) -> str:
        """开始或续用一次启动耗时诊断。"""
        with cls._startup_lock:
            run = cls._startup_run
            if reset or run is None or run.get('finished'):
                cls._startup_seq += 1
                run = {
                    'run_id': f"startup-{cls._startup_seq}",
                    'source': source,
                    'strategy_id': strategy_id or '',
                    'started_at': time.perf_counter(),
                    'started_wall': datetime.now().isoformat(),
                    'spans': [],
                    'events': [],
                    'finished': False,
                }
                cls._startup_run = run
                logging.info(
                    "[StartupProbe] begin run=%s source=%s strategy_id=%s",
                    run['run_id'], source, strategy_id or '-',
                )
            else:
                logging.info(
                    "[StartupProbe] resume run=%s source=%s strategy_id=%s",
                    run['run_id'], source, run.get('strategy_id') or '-',
                )

        cls.mark_startup_event(f"timeline.begin:{source}")
        return run['run_id']

    @classmethod
    def _require_startup_run(cls) -> Optional[Dict[str, Any]]:
        with cls._startup_lock:
            return cls._startup_run

    @classmethod
    def mark_startup_event(cls, name: str, detail: str = None) -> None:
        """记录启动链路中的关键事件。"""
        run = cls._require_startup_run()
        if run is None:
            return

        elapsed = time.perf_counter() - run['started_at']
        event = {
            'name': name,
            'detail': detail or '',
            'elapsed': elapsed,
            'wall_time': datetime.now().isoformat(),
        }
        with cls._startup_lock:
            current = cls._startup_run
            if current is None:
                return
            current['events'].append(event)

        if detail:
            logging.info(
                "[StartupProbe] event run=%s t=%.3fs name=%s detail=%s",
                run['run_id'], elapsed, name, detail,
            )
        else:
            logging.info(
                "[StartupProbe] event run=%s t=%.3fs name=%s",
                run['run_id'], elapsed, name,
            )

    @classmethod
    @contextmanager
    def startup_step(cls, name: str, detail: str = None):
        """记录启动链路中的一个耗时步骤。"""
        run = cls._require_startup_run()
        if run is None:
            yield
            return

        step_start = time.perf_counter()
        exc: Optional[Exception] = None
        logging.info(
            "[StartupProbe] step_begin run=%s step=%s detail=%s",
            run['run_id'], name, detail or '-',
        )
        try:
            yield
        except Exception as error:
            exc = error
            raise
        finally:
            elapsed = time.perf_counter() - step_start
            total_elapsed = time.perf_counter() - run['started_at']
            span = {
                'name': name,
                'detail': detail or '',
                'elapsed': elapsed,
                'status': 'error' if exc is not None else 'ok',
                'error': str(exc) if exc is not None else '',
                'finished_at': datetime.now().isoformat(),
                'total_elapsed': total_elapsed,
            }
            with cls._startup_lock:
                current = cls._startup_run
                if current is not None:
                    current['spans'].append(span)

            logging.info(
                "[StartupProbe] step_end run=%s step=%s status=%s elapsed=%.3fs total=%.3fs",
                run['run_id'], name, span['status'], elapsed, total_elapsed,
            )

    @classmethod
    def emit_startup_summary(cls, reason: str, final: bool = False, top_n: int = 15) -> None:
        """输出当前启动链路汇总，并按耗时倒序展示最慢步骤。"""
        with cls._startup_lock:
            run = cls._startup_run
            if run is None:
                return

            total_elapsed = time.perf_counter() - run['started_at']
            spans = list(run['spans'])
            events = list(run['events'])
            aggregate: Dict[str, Dict[str, Any]] = {}
            for span in spans:
                bucket = aggregate.setdefault(span['name'], {
                    'count': 0,
                    'total': 0.0,
                    'max': 0.0,
                    'errors': 0,
                })
                bucket['count'] += 1
                bucket['total'] += float(span['elapsed'])
                bucket['max'] = max(bucket['max'], float(span['elapsed']))
                if span['status'] != 'ok':
                    bucket['errors'] += 1

            sorted_steps = sorted(
                aggregate.items(),
                key=lambda item: item[1]['total'],
                reverse=True,
            )[:top_n]
            recent_events = events[-8:]

            if final:
                run['finished'] = True

        logging.info(
            "[StartupProbe] summary run=%s reason=%s total=%.3fs spans=%d events=%d final=%s",
            run['run_id'], reason, total_elapsed, len(spans), len(events), final,
        )
        for index, (step_name, stats) in enumerate(sorted_steps, start=1):
            avg = stats['total'] / stats['count'] if stats['count'] else 0.0
            logging.info(
                "[StartupProbe] top%d step=%s total=%.3fs avg=%.3fs max=%.3fs count=%d errors=%d",
                index, step_name, stats['total'], avg, stats['max'], stats['count'], stats['errors'],
            )
        for event in recent_events:
            if event['detail']:
                logging.info(
                    "[StartupProbe] recent_event t=%.3fs name=%s detail=%s",
                    event['elapsed'], event['name'], event['detail'],
                )
            else:
                logging.info(
                    "[StartupProbe] recent_event t=%.3fs name=%s",
                    event['elapsed'], event['name'],
                )

    @staticmethod
    def diagnose_subscribe_api_return(strategy_core, test_contracts: list = None) -> Dict[str, Any]:
        """诊断订阅API返回值 - 检查 sub_market_data 对不同期权的实际返回
        
        Args:
            strategy_core: 策略核心服务实例
            test_contracts: 测试合约列表，格式为 [(exchange, instrument_id, desc), ...]
            
        Returns:
            Dict: 诊断结果字典
        """
        if test_contracts is None:
            test_contracts = [
                ('SHFE', 'al2605C25200', 'AL期权-有tick'),
                ('SHFE', 'au2605C1032', 'AU期权-无tick'),
                ('SHFE', 'cu2605C104000', 'CU期权-无tick'),
                ('SHFE', 'zn2605C20000', 'ZN期权-无tick'),
            ]
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'contracts': {},
            'summary': {'total': 0, 'success': 0, 'failed': 0, 'no_return': 0}
        }
        
        logging.info("=" * 80)
        logging.info("[SubscribeReturnDiag] 开始诊断 sub_market_data 返回值")
        logging.info("=" * 80)
        
        sub_api = getattr(strategy_core, '_runtime_strategy_host', None)
        if sub_api is None:
            sub_api = strategy_core
        
        sub_method = getattr(sub_api, 'sub_market_data', None)
        if not callable(sub_method):
            logging.error("[SubscribeReturnDiag] sub_market_data 方法不可用")
            results['error'] = 'sub_market_data not callable'
            return results
        
        for exchange, inst_id, desc in test_contracts:
            results['summary']['total'] += 1
            contract_result = {
                'exchange': exchange,
                'instrument_id': inst_id,
                'desc': desc,
                'return_value': None,
                'return_type': None,
                'exception': None,
                'status': 'unknown'
            }
            
            try:
                ret = sub_method(exchange, inst_id)
                contract_result['return_value'] = str(ret)
                contract_result['return_type'] = type(ret).__name__
                
                if ret is None:
                    contract_result['status'] = 'no_return'
                    results['summary']['no_return'] += 1
                    logging.warning(
                        f"[SubscribeReturnDiag] {inst_id} ({desc}): "
                        f"返回值=None, 可能被忽略"
                    )
                elif ret is False:
                    contract_result['status'] = 'failed'
                    results['summary']['failed'] += 1
                    logging.error(
                        f"[SubscribeReturnDiag] {inst_id} ({desc}): "
                        f"返回值=False, 订阅失败"
                    )
                elif ret is True:
                    contract_result['status'] = 'success'
                    results['summary']['success'] += 1
                    logging.info(
                        f"[SubscribeReturnDiag] {inst_id} ({desc}): "
                        f"返回值=True, 订阅成功"
                    )
                else:
                    contract_result['status'] = 'other'
                    logging.info(
                        f"[SubscribeReturnDiag] {inst_id} ({desc}): "
                        f"返回值={ret}, 类型={type(ret).__name__}"
                    )
                    
            except Exception as e:
                contract_result['exception'] = str(e)
                contract_result['status'] = 'exception'
                results['summary']['failed'] += 1
                logging.error(
                    f"[SubscribeReturnDiag] {inst_id} ({desc}): "
                    f"异常={e}"
                )
            
            results['contracts'][inst_id] = contract_result
        
        logging.info("-" * 80)
        logging.info(
            f"[SubscribeReturnDiag] 汇总: "
            f"total={results['summary']['total']}, "
            f"success={results['summary']['success']}, "
            f"failed={results['summary']['failed']}, "
            f"no_return={results['summary']['no_return']}"
        )
        logging.info("=" * 80)
        
        return results

    @staticmethod
    def diagnose_option_metadata_diff(strategy_core) -> Dict[str, Any]:
        """诊断期权元数据差异 - 对比有tick和无tick期权的元数据字段
        
        Args:
            strategy_core: 策略核心服务实例
            
        Returns:
            Dict: 诊断结果字典
        """
        from ali2026v3_trading.params_service import get_params_service
        
        ps = get_params_service()
        
        test_contracts = [
            ('al2605C25200', 'AL期权-有tick'),
            ('au2605C1032', 'AU期权-无tick'),
            ('cu2605C104000', 'CU期权-无tick'),
            ('zn2605C20000', 'ZN期权-无tick'),
            ('HO2605-C-2800', 'HO指数期权-有tick'),
        ]
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'contracts': {},
            'diff_fields': {}
        }
        
        logging.info("=" * 80)
        logging.info("[OptionMetaDiff] 开始诊断期权元数据差异")
        logging.info("=" * 80)
        
        all_fields = set()
        for inst_id, desc in test_contracts:
            meta = ps.get_instrument_meta_by_id(inst_id)
            if meta:
                all_fields.update(meta.keys())
        
        key_fields = [
            'exchange', 'product', 'product_type', 'instrument_type',
            'price_tick', 'multiplier', 'trading_hours', 
            'underlying_symbol', 'underlying_id',
            'min_price_tick', 'contract_size', 'delivery_month',
            'option_type', 'strike_price', 'expiration_date'
        ]
        
        key_fields = [f for f in key_fields if f in all_fields]
        
        logging.info(f"[OptionMetaDiff] 关键字段: {key_fields}")
        logging.info("-" * 80)
        
        header = f"{'合约':<20} {'描述':<15}"
        for f in key_fields[:8]:
            header += f" {f[:12]:<12}"
        logging.info(header)
        logging.info("-" * 120)
        
        for inst_id, desc in test_contracts:
            meta = ps.get_instrument_meta_by_id(inst_id)
            
            row = f"{inst_id:<20} {desc:<15}"
            for f in key_fields[:8]:
                v = meta.get(f, 'N/A') if meta else 'N/A'
                row += f" {str(v)[:12]:<12}"
            logging.info(row)
            
            results['contracts'][inst_id] = {
                'desc': desc,
                'meta': meta,
                'found': meta is not None
            }
        
        logging.info("=" * 80)
        
        return results


def _get_runtime_state():
    """获取运行时状态，异常时返回安全默认值
    
    Returns:
        (runtime_strategy, runtime_core, runtime_subscribed, historical_in_progress, startup_ready)
    """
    runtime_strategy = None
    runtime_core = None
    runtime_subscribed: Set[str] = set()
    historical_in_progress = False
    startup_ready = False

    try:
        from ali2026v3_trading.config_service import get_cached_params

        cached_params = get_cached_params() or {}
        runtime_strategy = cached_params.get('strategy')
        runtime_core = getattr(runtime_strategy, 'strategy_core', None) if runtime_strategy is not None else None

        subscribed_source = runtime_core if runtime_core is not None else runtime_strategy
        runtime_subscribed = set(getattr(subscribed_source, '_subscribed_instruments', []) or [])
        historical_in_progress = bool(getattr(subscribed_source, '_historical_load_in_progress', False))
        startup_ready = bool(
            getattr(runtime_strategy, '_start_completed', False)
            or getattr(runtime_core, '_is_running', False)
            or runtime_subscribed
        )
    except Exception as exc:
        logging.debug(f"[{MONITORED_DIAG_LABEL}] 获取运行时状态失败: {exc}")

    return runtime_strategy, runtime_core, runtime_subscribed, historical_in_progress, startup_ready


def _emit_periodic_resource_ownership_snapshot(runtime_core) -> None:
    """输出一次周期诊断资源快照，避免在每个监控合约上重复扫描线程。"""
    strategy_id = getattr(runtime_core, 'strategy_id', 'unknown') if runtime_core else 'unknown'
    run_id = getattr(runtime_core, '_lifecycle_run_id', 'N/A') if runtime_core else 'N/A'
    phase = 'periodic-diagnosis'

    allowed_prefixes = (
        'Main', 'Thread-', 'APScheduler', 'ThreadPoolExecutor',
        'Storage-AsyncWriter[shared-service]', 'Storage-Cleanup[shared-service]',
        'SubAsyncWriter[shared-service]', 'SubRetry[shared-service]', 'SubCleanup[shared-service]',
        'TTypeService-Preload[shared-service]', 'onStop-worker',
    )

    threads = threading.enumerate()
    strategy_threads = []
    shared_threads = []
    system_threads = []

    for thread_obj in threads:
        name = thread_obj.name or ''
        if '[shared-service]' in name:
            shared_threads.append(name)
        elif any(name.startswith(prefix) for prefix in allowed_prefixes):
            system_threads.append(name)
        elif 'strategy' in name.lower() or strategy_id in name:
            strategy_threads.append(name)
        elif name and not name.startswith('Main'):
            system_threads.append(name)

    logging.info(
        f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}][phase={phase}]"
        f"[source_type=resource-ownership] "
        f"Thread scan: total={len(threads)}, shared-service={len(shared_threads)}, "
        f"strategy-instance={len(strategy_threads)}, system={len(system_threads)}"
    )

    if shared_threads:
        for name in shared_threads:
            logging.info(
                f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                f"[run_id={run_id}][source_type=resource-ownership] "
                f"Thread alive: {name} (expected: continues after strategy stop)"
            )

    if strategy_threads:
        for name in strategy_threads:
            logging.warning(
                f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                f"[run_id={run_id}][source_type=resource-ownership] "
                f"⚠️ LEAKED thread: {name} (expected: should be gone after strategy stop)"
            )
    else:
        logging.info(
            f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
            f"[run_id={run_id}][source_type=resource-ownership] "
            f"✅ No strategy-instance threads leaked"
        )

    scheduler_mgr = getattr(runtime_core, '_scheduler_manager', None) if runtime_core else None
    if scheduler_mgr and hasattr(scheduler_mgr, 'get_jobs_by_owner'):
        try:
            remaining_jobs = scheduler_mgr.get_jobs_by_owner(strategy_id)
            if remaining_jobs:
                job_ids = [job['job_id'] for job in remaining_jobs]
                logging.warning(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=strategy-job] "
                    f"⚠️ LEAKED scheduler jobs: {job_ids}"
                )
            else:
                logging.info(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=strategy-job] "
                    f"✅ No strategy-instance scheduler jobs leaked"
                )
        except Exception as exc:
            logging.debug(
                f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                f"[source_type=resource-ownership] Scheduler diagnosis error: {exc}"
            )

    storage_obj = getattr(runtime_core, '_storage', None) if runtime_core else None
    if storage_obj and hasattr(storage_obj, 'get_queue_stats'):
        try:
            qstats = storage_obj.get_queue_stats()
            qsize = qstats.get('current_queue_size', 0)
            if qsize > 0:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=shared-queue-drain] "
                    f"Storage queue backlog: {qsize} tasks (expected: drain continues)"
                )
            else:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=shared-queue-drain] "
                    f"✅ Storage queue empty"
                )
        except Exception as exc:
            logging.debug(
                f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                f"[source_type=resource-ownership] Storage queue diagnosis error: {exc}"
            )

    event_bus = getattr(runtime_core, '_event_bus', None) if runtime_core else None
    if event_bus and hasattr(event_bus, '_pending_events'):
        try:
            pending = getattr(event_bus, '_pending_events', 0)
            if pending > 0:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=event-tail] "
                    f"EventBus pending callbacks: {pending} (expected: drain in progress)"
                )
            else:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=event-tail] "
                    f"✅ EventBus pending callbacks empty"
                )
        except Exception as exc:
            logging.debug(
                f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                f"[source_type=resource-ownership] EventBus diagnosis error: {exc}"
            )


def run_14_contracts_periodic_diagnostic(storage=None, query_service=None) -> None:
    """周期性诊断：每30秒检查重点监控合约状态（汇总输出模式）
    
    成功合约：只列名单
    失败合约：详细列出原因
    
    诊断节点：
    - 环节0a: 配置表存在状态检查 (PROBE_CONFIG_PRESENCE)
    - 环节0b: 预注册状态检查 (PROBE_PRE_REGISTRATION)
    - 环节0c: 标准格式核对状态检查 (PROBE_FORMAT_VALIDATION)
    - 环节1-8: 原有诊断节点（订阅、注册、ID转换、Tick入口等）
    """
    logging.info("\n" + "="*100)
    logging.info("📊 [%s] 开始检查", MONITORED_DIAG_LABEL)
    logging.info("="*100)

    runtime_strategy, runtime_core, runtime_subscribed, historical_in_progress, startup_ready = _get_runtime_state()

    if historical_in_progress:
        logging.info(
            "[%s] 历史K线加载进行中，跳过本轮以避免启动期误报: subscribed=%%d" % MONITORED_DIAG_LABEL,
            len(runtime_subscribed),
        )
        logging.info("="*100)
        return

    if runtime_strategy is not None and not startup_ready:
        logging.info("[%s] 启动桥接尚未完成，跳过本轮以避免初始化误报", MONITORED_DIAG_LABEL)
        logging.info("="*100)
        return

    _emit_periodic_resource_ownership_snapshot(runtime_core)

    # 重新获取运行时状态（因为前面的诊断可能修改了状态）
    runtime_strategy, runtime_core, runtime_subscribed, historical_in_progress, startup_ready = _get_runtime_state()

    if historical_in_progress:
        logging.info(
            "[%s] 历史K线加载进行中，跳过本轮以避免启动期误报: subscribed=%%d" % MONITORED_DIAG_LABEL,
            len(runtime_subscribed),
        )
        logging.info("="*100)
        return

    if runtime_strategy is not None and not startup_ready:
        logging.info("[%s] 启动桥接尚未完成，跳过本轮以避免初始化误报", MONITORED_DIAG_LABEL)
        logging.info("="*100)
        return
    
    success_contracts = []  # 成功的合约名单
    failed_contracts = []   # 失败的合约详情
    
    for contract, info in MONITORED_CONTRACTS.items():
        try:
            # ========================================
            # 环节1-12: 完整诊断状态检测
            # ========================================
            # 环节1-4: 配置和预检查
            exists_in_file = False  # 环节1: 配置文件存在性
            loaded_in_memory = False  # 环节2: 内存加载状态
            in_subscribe_list = False  # 环节3: 订阅列表状态
            is_valid_format = False  # 环节4: 标准格式核对
            expected_format = None
            
            # 环节1: 检查配置文件
            try:
                import os
                config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ali2026v3_trading')
                config_lines = []
                for cfg_name in ['subscription_futures_fixed.txt', 'subscription_options_fixed.txt']:
                    cfg_path = os.path.join(config_dir, cfg_name)
                    if os.path.exists(cfg_path):
                        with open(cfg_path, 'r', encoding='utf-8') as f:
                            config_lines.extend(l.strip() for l in f if l.strip() and not l.startswith('#'))
                exists_in_file = any(
                    contract in line or line.endswith('.' + contract)
                    for line in config_lines
                )
            except Exception as e:
                logging.debug(f"[DIAG_CONFIG] 配置文件检查失败: {e}")
            
            # 环节2: 检查内存加载
            # ✅ 传递渠道唯一：通过公共API检查缓存，不直接访问私有属性
            if storage:
                try:
                    if hasattr(storage, '_params_service') and hasattr(storage._params_service, 'get_instrument_meta_by_id'):
                        loaded_in_memory = storage._params_service.get_instrument_meta_by_id(contract) is not None
                    else:
                        loaded_in_memory = storage._get_instrument_info(contract) is not None
                except Exception:
                    loaded_in_memory = False
            
            # 环节3: 订阅列表状态
            if runtime_subscribed:
                in_subscribe_list = contract in runtime_subscribed
            elif loaded_in_memory:
                # 运行时已加载到权威缓存时，视为已进入当前订阅/注册链路
                in_subscribe_list = True
            else:
                in_subscribe_list = exists_in_file
            
            # 环节4: 格式验证
            is_valid_format, expected_format, _ = validate_contract_format(contract)
            
            # 环节5-12: 运行时诊断
            subscribed = False  # 环节5: 订阅状态
            registered = False  # 环节6: 注册状态
            internal_id = "N/A"  # 环节7: ID转换
            has_internal_id = False
            tick_entry_count = 0  # 环节8: Tick入口计数
            tick_buffered = False  # 环节9: Tick缓冲状态
            storage_enqueued = False  # 环节10: Storage入队状态
            async_written = False  # 环节11: 异步写入状态
            duckdb_inserted = False  # 环节12: DuckDB写入状态
            kline_count = 0
            tick_count = 0
            failure_reasons = []
            
            if storage:
                # 环节2: 检查注册状态
                inst_info = storage._get_registered_instrument_info(contract)
                if inst_info:
                    registered = True
                    internal_id = str(inst_info.get('id', 'N/A'))
                    has_internal_id = (internal_id != 'N/A')
                    
                    # 环节1: 已进入运行时订阅集合或已正式注册
                    subscribed = in_subscribe_list or registered
                    
                    # 环节3: ID转换状态（已有internal_id即表示成功）
                    has_internal_id = True
                else:
                    subscribed = in_subscribe_list
                
                # 环节6-8: 检查Storage队列和DuckDB状态
                # 修复：_tick_queue → _write_queue，元素格式为(func_name, args, kwargs)
                # _save_tick_impl的args=(internal_id, instrument_type, chunk)
                if hasattr(storage, '_write_queue'):
                    try:
                        queue_items = list(storage._write_queue.queue) if hasattr(storage._write_queue, 'queue') else []
                        for item in queue_items:
                            if isinstance(item, tuple) and len(item) >= 2 and item[0] == '_save_tick_impl':
                                args = item[1] if len(item) > 1 else ()
                                # args[0]=internal_id(int), 通过storage反查instrument_id
                                if isinstance(args, (list, tuple)) and len(args) >= 1:
                                    try:
                                        internal_id = args[0]
                                        info_by_id = storage._get_info_by_id(int(internal_id))
                                        if info_by_id and info_by_id.get('instrument_id') == contract:
                                            storage_enqueued = True
                                            break
                                    except Exception:
                                        pass
                    except Exception:
                        pass
                
                # 环节8: 检查DuckDB中是否有数据
                if query_service:
                    try:
                        kline_count = query_service.get_kline_count(contract)
                        # 修复：get_tick_count_recent不存在，改用get_tick_count
                        tick_count = query_service.get_tick_count(contract)
                        duckdb_inserted = (kline_count > 0 or tick_count > 0)
                    except Exception as e:
                        failure_reasons.append(f"查询失败: {e}")
            
            # 环节4-5: Tick入口和缓冲状态
            # 修复：_tick_buffer在strategy_core_service(TickHandlerMixin)上，不在storage上
            # 通过runtime_core获取，_tick_buffer是列表，检查其中是否有该合约的tick
            tick_source = runtime_core if runtime_core is not None else runtime_strategy
            if tick_source and hasattr(tick_source, '_tick_buffer'):
                try:
                    buf = tick_source._tick_buffer
                    if isinstance(buf, list):
                        # _tick_buffer是tick字典列表，检查instrument_id字段
                        tick_entry_count = sum(1 for t in buf if isinstance(t, dict) and t.get('instrument_id') == contract)
                        tick_buffered = (tick_entry_count > 0)
                except Exception:
                    pass
            
            # 环节7: 异步写入状态
            # 修复：_async_write_count不存在，改用_queue_stats['total_written']
            if storage and hasattr(storage, '_queue_stats'):
                try:
                    async_written = storage._queue_stats.get('total_written', 0) > 0
                except Exception:
                    pass
            
            diagnose_pre_registration(contract, in_subscribe_list, registered)
            
            # ========================================
            # 综合状态判断
            # ========================================
            # 修复：区分K线合成状态和历史K线加载状态
            # - tick_count > 0: 实时Tick已入库（数据链路正常）
            # - kline_count > 0: K线已合成或历史K线已加载
            # 只要registered且有tick数据即认为数据链路正常
            # K线数据单独记录状态，不作为链路正常的必要条件
            has_tick_data = tick_count > 0
            has_kline_data = kline_count > 0
            is_success = registered and has_tick_data
            
            if is_success:
                success_contracts.append(contract)
            else:
                # 收集失败原因（按优先级排序）
                if not exists_in_file:
                    failure_reasons.insert(0, "❌ 不在配置文件中")
                elif not loaded_in_memory:
                    failure_reasons.insert(0, "❌ 配置表未加载到内存")
                elif not in_subscribe_list:
                    failure_reasons.insert(0, "❌ 未加入订阅列表")
                elif not is_valid_format:
                    failure_reasons.insert(0, f"❌ 格式错误: {expected_format}")
                elif in_subscribe_list and not registered:
                    failure_reasons.insert(0, "⚠️ 在订阅列表但未注册")
                elif not registered:
                    failure_reasons.insert(0, "❌ 合约未注册")
                else:
                    # 准确记录数据状态
                    if not has_tick_data:
                        failure_reasons.append("⚠️ 无Tick数据(实时链路异常)")
                    if not has_kline_data:
                        if has_tick_data:
                            failure_reasons.append("⚠️ 无K线数据(Tick已入库但K线未合成)")
                        else:
                            failure_reasons.append("⚠️ 无K线数据(无数据源)")
                
                failed_contracts.append({
                    'contract': contract,
                    # 环节0a-0d: 配置和预检查
                    'exists_in_file': exists_in_file,  # ✅ 环节0a: 配置文件存在性
                    'loaded_in_memory': loaded_in_memory,  # ✅ 环节0b: 内存加载状态
                    'in_subscribe_list': in_subscribe_list,  # ✅ 环节0c: 订阅列表状态
                    'is_valid_format': is_valid_format,  # ✅ 环节0d: 标准格式核对
                    # 环节1-8: 运行时诊断（实时检测）
                    'subscribed': subscribed,  # ✅ 环节1: 订阅状态
                    'registered': bool(registered),  # ✅ 环节2: 注册状态
                    'has_internal_id': has_internal_id,  # ✅ 环节3: ID转换状态
                    'tick_entry_count': tick_entry_count,  # ✅ 环节4: Tick入口计数
                    'tick_buffered': tick_buffered,  # ✅ 环节5: Tick缓冲状态
                    'storage_enqueued': storage_enqueued,  # ✅ 环节6: Storage入队状态
                    'async_written': async_written,  # ✅ 环节7: 异步写入状态
                    'duckdb_inserted': duckdb_inserted,  # ✅ 环节8: DuckDB写入状态
                    # 数据状态
                    'has_kline_data': has_kline_data,
                    'has_tick_data': has_tick_data,
                    'internal_id': str(internal_id),
                    'kline_count': int(kline_count),
                    'tick_count': int(tick_count),
                    'reasons': failure_reasons
                })
                    
        except Exception as e:
            failed_contracts.append({
                'contract': contract,
                # 环节0a-0d
                'exists_in_file': False,
                'loaded_in_memory': False,
                'in_subscribe_list': False,
                'is_valid_format': False,
                # 环节1-8（异常时全部为False）
                'subscribed': False,
                'registered': False,
                'has_internal_id': False,
                'tick_entry_count': 0,
                'tick_buffered': False,
                'storage_enqueued': False,
                'async_written': False,
                'duckdb_inserted': False,
                # 数据状态
                'has_kline_data': False,
                'has_tick_data': False,
                'internal_id': 'N/A',
                'kline_count': 0,
                'tick_count': 0,
                'reasons': [f"❌ 异常: {str(e)}"]
            })
    
    # ✅ 汇总输出：成功合约只列名单
    if success_contracts:
        logging.info(f"✅ 正常合约 ({len(success_contracts)}/{len(MONITORED_CONTRACTS)}):")
        # 按类型分组显示
        futures = [c for c in success_contracts if MONITORED_CONTRACTS[c]['type'] == 'future']
        options = [c for c in success_contracts if MONITORED_CONTRACTS[c]['type'] == 'option']
        
        if futures:
            logging.info(f"  期货: {', '.join(futures)}")
        if options:
            logging.info(f"  期权: {', '.join(options)}")
    
    # ✅ 详细输出：失败合约30秒汇总输出（避免被CRITICAL告警淹没）
    if failed_contracts:
        logging.warning(f"\n❌ 异常合约 ({len(failed_contracts)}/{len(MONITORED_CONTRACTS)}):")
        
        # 按失败原因分组统计
        reason_stats = {}
        for fail_info in failed_contracts:
            reasons = fail_info['reasons']
            primary_reason = reasons[0] if reasons else "未知原因"
            if primary_reason not in reason_stats:
                reason_stats[primary_reason] = []
            reason_stats[primary_reason].append(fail_info['contract'])
        
        # 输出每组统计
        for reason, contracts in reason_stats.items():
            logging.warning(f"  {reason} ({len(contracts)}个): {', '.join(contracts[:5])}{'...' if len(contracts) > 5 else ''}")
        
        # 输出12环节状态汇总（只显示有异常的环节）
        logging.warning(f"\n  [{MONITORED_CONTRACT_COUNT}Diagnosis] 📊 12环节状态汇总:")
        环节_stats = {
            '1-Config': sum(1 for f in failed_contracts if not f.get('exists_in_file', False)),
            '2-Memory': sum(1 for f in failed_contracts if not f.get('loaded_in_memory', False)),
            '3-Subscribe': sum(1 for f in failed_contracts if not f.get('in_subscribe_list', False)),
            '4-Format': sum(1 for f in failed_contracts if not f.get('is_valid_format', False)),
            '5-Subscribed': sum(1 for f in failed_contracts if not f.get('subscribed', False)),
            '6-Registered': sum(1 for f in failed_contracts if not f.get('registered', False)),
            '7-ID': sum(1 for f in failed_contracts if not f.get('has_internal_id', False)),
            '8-TickEntry': sum(1 for f in failed_contracts if f.get('tick_entry_count', 0) == 0),
            '9-TickBuffer': sum(1 for f in failed_contracts if not f.get('tick_buffered', False)),
            '10-Enqueue': sum(1 for f in failed_contracts if not f.get('storage_enqueued', False)),
            '11-AsyncWrite': sum(1 for f in failed_contracts if not f.get('async_written', False)),
            '12-DuckDB': sum(1 for f in failed_contracts if not f.get('duckdb_inserted', False)),
        }
        
        for stage_name, fail_count in 环节_stats.items():
            if fail_count > 0:
                status = '❌' if fail_count == len(failed_contracts) else '⚠️'
                logging.warning(f"    {stage_name:15s}: {fail_count}/{len(failed_contracts)} {status}")
    
    # 总结
    total = len(MONITORED_CONTRACTS)
    success_rate = (len(success_contracts) / total * 100) if total > 0 else 0
    logging.info(f"\n📈 诊断结果: {len(success_contracts)}/{total} 正常 ({success_rate:.1f}%)")
    
    # ✅ 新增：队列状态监控
    if storage and hasattr(storage, '_write_queue'):
        try:
            queue_size = storage._write_queue.qsize()
            max_size = storage._write_queue.maxsize
            usage_rate = (queue_size / max_size * 100) if max_size > 0 else 0
            
            if usage_rate > 90:
                logging.critical(f"🚨 [QUEUE_CRITICAL] 写入队列使用率超过90%: {usage_rate:.1f}% ({queue_size}/{max_size})")
            elif usage_rate > 80:
                logging.warning(f"⚠️ [QUEUE_WARNING] 写入队列使用率超过80%: {usage_rate:.1f}% ({queue_size}/{max_size})")
            elif usage_rate > 70:
                logging.info(f"[QUEUE_INFO] 写入队列使用率: {usage_rate:.1f}% ({queue_size}/{max_size})")
            else:
                logging.debug(f"[QUEUE_OK] 写入队列使用率: {usage_rate:.1f}% ({queue_size}/{max_size})")
            
            # 显示队列统计信息
            if hasattr(storage, '_queue_stats'):
                stats = storage._queue_stats
                logging.info(f"📊 队列统计: 接收={stats.get('total_received', 0)}, 写入={stats.get('total_written', 0)}, 丢弃={stats.get('drops_count', 0)}, 峰值={stats.get('max_queue_size_seen', 0)}")
        except Exception as e:
            logging.debug(f"[QUEUE_CHECK] 队列状态检查失败: {e}")
    
    logging.info("="*100)


class ResourceOwnershipScanner:
    """资源所有权诊断扫描器，用于检测线程、scheduler jobs、storage queue和event bus的资源泄漏情况"""

    @staticmethod
    def log_resource_ownership_table(strategy_core, phase: str = 'unknown') -> None:
        """输出资源所有权表，扫描线程列表并断言strategy-instance级线程已消失"""
        import threading as _threading

        strategy_id = getattr(strategy_core, 'strategy_id', 'unknown')
        run_id = getattr(strategy_core, '_lifecycle_run_id', 'N/A')

        ALLOWED_PREFIXES = (
            'Main', 'Thread-', 'APScheduler', 'ThreadPoolExecutor',
            'Storage-AsyncWriter[shared-service]', 'Storage-Cleanup[shared-service]',
            'SubAsyncWriter[shared-service]', 'SubRetry[shared-service]', 'SubCleanup[shared-service]',
            'TTypeService-Preload[shared-service]', 'onStop-worker',
        )

        threads = _threading.enumerate()
        strategy_threads = []
        shared_threads = []
        system_threads = []

        for t in threads:
            name = t.name or ''
            if '[shared-service]' in name:
                shared_threads.append(name)
            elif any(name.startswith(p) for p in ALLOWED_PREFIXES):
                system_threads.append(name)
            elif 'strategy' in name.lower() or strategy_id in name:
                strategy_threads.append(name)
            elif name and not name.startswith('Main'):
                system_threads.append(name)

        logging.info(
            f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}][phase={phase}]"
            f"[source_type=resource-ownership] "
            f"Thread scan: total={len(threads)}, shared-service={len(shared_threads)}, "
            f"strategy-instance={len(strategy_threads)}, system={len(system_threads)}"
        )

        if shared_threads:
            for name in shared_threads:
                logging.info(
                    f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"Thread alive: {name} (expected: continues after strategy stop)"
                )

        if strategy_threads:
            for name in strategy_threads:
                logging.warning(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"⚠️ LEAKED thread: {name} (expected: should be gone after strategy stop)"
                )
        else:
            if phase == 'stop':
                logging.info(
                    f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                    f"[run_id={run_id}][source_type=resource-ownership] "
                    f"✅ No strategy-instance threads leaked"
                )

        # 扩展诊断 - scheduler jobs, storage queue, event_bus pending events
        # 1. Scheduler jobs
        scheduler_mgr = getattr(strategy_core, '_scheduler_manager', None)
        if scheduler_mgr and hasattr(scheduler_mgr, 'get_jobs_by_owner'):
            try:
                remaining_jobs = scheduler_mgr.get_jobs_by_owner(strategy_id)
                if remaining_jobs:
                    job_ids = [j['job_id'] for j in remaining_jobs]
                    logging.warning(
                        f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=strategy-job] "
                        f"⚠️ LEAKED scheduler jobs: {job_ids}"
                    )
                else:
                    if phase == 'stop':
                        logging.info(
                            f"[ResourceOwnership][owner_scope=strategy-instance][strategy={strategy_id}]"
                            f"[run_id={run_id}][source_type=strategy-job] "
                            f"✅ No strategy-instance scheduler jobs leaked"
                        )
            except Exception as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] Scheduler diagnosis error: {e}"
                )

        # 2. Storage queue stats
        storage_obj = getattr(strategy_core, '_storage', None)
        if storage_obj and hasattr(storage_obj, 'get_queue_stats'):
            try:
                qstats = storage_obj.get_queue_stats()
                qsize = qstats.get('current_queue_size', 0)
                if qsize > 0:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=shared-queue-drain] "
                        f"Storage queue backlog: {qsize} tasks (expected: drain continues)"
                    )
                else:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=shared-queue-drain] "
                        f"✅ Storage queue empty"
                    )
            except Exception as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] Storage queue diagnosis error: {e}"
                )

        # 3. Event bus pending callbacks
        event_bus = getattr(strategy_core, '_event_bus', None)
        if event_bus and hasattr(event_bus, '_pending_events'):
            try:
                pending = getattr(event_bus, '_pending_events', 0)
                if pending > 0:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=event-tail] "
                        f"EventBus pending callbacks: {pending} (expected: drain in progress)"
                    )
                else:
                    logging.info(
                        f"[ResourceOwnership][owner_scope=shared-service][strategy={strategy_id}]"
                        f"[run_id={run_id}][source_type=event-tail] "
                        f"✅ EventBus pending callbacks empty"
                    )
            except Exception as e:
                logging.debug(
                    f"[ResourceOwnership][strategy={strategy_id}][run_id={run_id}]"
                    f"[source_type=resource-ownership] EventBus diagnosis error: {e}"
                )


class ControlActionLogger:
    """控制动作日志记录器，用于统一记录策略控制动作的进入、成功、失败和阻断状态"""

    _logger = logging.getLogger("ControlActionLogger")

    @staticmethod
    def log_control_action_enter(action_type, strategy_id, run_id, source):
        """记录控制动作进入"""
        ControlActionLogger._logger.info(
            f"[ControlActionLogger][owner_scope=control-action] ENTER {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}] [source={source}]"
        )

    @staticmethod
    def log_control_action_success(action_type, strategy_id, run_id, result=None, source=None):
        """记录控制动作成功"""
        base_msg = (
            f"[ControlActionLogger][owner_scope=control-action] SUCCESS {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}]"
        )
        if source is not None:
            base_msg += f" [source={source}]"
        if result is not None:
            base_msg += f" [result={result}]"
        ControlActionLogger._logger.info(base_msg)

    @staticmethod
    def log_control_action_fail(action_type, strategy_id, run_id, error, source=None):
        """记录控制动作失败"""
        base_msg = (
            f"[ControlActionLogger][owner_scope=control-action] FAIL {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}] [error={error}]"
        )
        if source is not None:
            base_msg += f" [source={source}]"
        ControlActionLogger._logger.error(base_msg)

    @staticmethod
    def log_control_action_blocked(action_type, strategy_id, run_id, reason, source=None):
        """记录控制动作被阻断"""
        base_msg = (
            f"[ControlActionLogger][owner_scope=control-action] BLOCKED {action_type} "
            f"[strategy_id={strategy_id}] [run_id={run_id}] [reason={reason}]"
        )
        if source is not None:
            base_msg += f" [source={source}]"
        ControlActionLogger._logger.info(base_msg)
