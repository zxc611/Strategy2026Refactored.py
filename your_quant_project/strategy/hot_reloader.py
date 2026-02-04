import sys
import time
import types
import gc
import logging
from contextlib import contextmanager
from typing import Dict, Any, Optional, List
import importlib.util
import random

# Use relative import if part of the package, else simple import
try:
    from .namespace_manager import DynamicNamespace, NamespaceLayer
except ImportError:
    # Fallback for direct execution
    from namespace_manager import DynamicNamespace, NamespaceLayer

logger = logging.getLogger(__name__)

class MigrationOrchestrator:
    """Manages the steps of migration."""
    pass

class TrafficDirector:
    """Controls traffic shifting between old and new versions."""
    def __init__(self):
        self._current_split = 0.0 # 0.0 = all old, 1.0 = all new
        self.old_module = None
        self.new_module = None

    def start_blue_green(self, old_module, new_module):
        self.old_module = old_module
        self.new_module = new_module
        self._current_split = 0.0
        logger.info(f"Started Blue/Green deployment. Old: {old_module}, New: {new_module}")

    def shift_traffic(self, new_module, percentage: int):
        self._current_split = percentage / 100.0
        logger.info(f"Traffic shifted to {percentage}% for new version.")
        # In a real system, this would update a router or proxy that dispatches events.
        # For this implementation, we assume the host system checks this director.
        time.sleep(0.5) # Simulate time to propagate

    def rollback(self):
        self._current_split = 0.0
        self.new_module = None
        logger.warning("Rolled back traffic to 100% old version.")

class HealthChecker:
    """Performs health checks on the module."""
    def check(self, module: types.ModuleType) -> bool:
        # Basic check: verifies module has expected attributes
        required_attrs = ['__init__'] # Add specific strategy requirements here
        for attr in required_attrs:
            if not hasattr(module, attr) and not hasattr(module, "__file__"):
                # __file__ check for simple modules
                logger.error(f"Health check failed: Missing attribute {attr}")
                return False
        return True

class IsolatedModuleLoader:
    """隔离模块加载器"""
    
    def __init__(self, namespace: DynamicNamespace, module_path: str):
        self.namespace = namespace
        self.module_path = module_path
        self.loaded_modules: Dict[str, types.ModuleType] = {}
        
    def find_module(self, fullname: str, path: Optional[List[str]] = None):
        """查找模块"""
        # 只处理我们关心的模块
        if fullname.startswith(self.namespace.module_name):
            return self
        return None
    
    def load_module(self, fullname: str) -> types.ModuleType:
        """加载模块到隔离命名空间"""
        if fullname in self.loaded_modules:
            return self.loaded_modules[fullname]
        
        # 创建新的模块对象
        module = types.ModuleType(fullname)
        
        # 设置自定义的__dict__来隔离命名空间
        # 注意：这里直接引用了 NamespaceLayer，确保已导入
        module.__dict__ = self.namespace.layers[NamespaceLayer.SCRATCH]
        
        # 必须确保 __builtins__ 存在，否则代码执行会失败
        if '__builtins__' not in module.__dict__:
            module.__dict__['__builtins__'] = __builtins__

        # 执行模块代码
        with open(self._get_module_path(fullname), 'r', encoding='utf-8') as f:
            code = compile(f.read(), fullname, 'exec')
            
        # 在隔离的命名空间中执行
        exec(code, module.__dict__)
        
        # 记录加载的模块
        self.loaded_modules[fullname] = module
        
        return module

    def _get_module_path(self, fullname: str) -> str:
        # 简化：假设只对应此时初始化的那个路径
        return self.module_path

class NamespaceManagerWrapper:
    """Adapter to make DynamicNamespace fit the ZeroDowntimeHotReloader API expectations"""
    def __init__(self):
        self.dns_map: Dict[str, DynamicNamespace] = {}

    def get_or_create(self, module_name: str) -> DynamicNamespace:
        if module_name not in self.dns_map:
            self.dns_map[module_name] = DynamicNamespace(module_name)
        return self.dns_map[module_name]

    def create_draft(self, module_path: str) -> DynamicNamespace:
        module_name = self._extract_name(module_path)
        dns = self.get_or_create(module_name)
        dns.clear_layer(NamespaceLayer.SCRATCH)
        return dns

    def cleanup_old_version(self, module_path: str):
        module_name = self._extract_name(module_path)
        if module_name in self.dns_map:
            self.dns_map[module_name].clear_layer(NamespaceLayer.STABLE)
            # Promote STAGING to STABLE implicitly or explicitly in the reloader logic
            # Here we assume reloader logic already handled promotion via user's flow
            pass

    def _extract_name(self, path: str) -> str:
        # Simplified logic
        return path.split("/")[-1].replace(".py", "")

class ZeroDowntimeHotReloader:
    """零停机热重载引擎"""
    
    def __init__(self):
        self.namespace_manager = NamespaceManagerWrapper()
        self.migration_orchestrator = MigrationOrchestrator()
        self.traffic_director = TrafficDirector()
        self.health_checker = HealthChecker()
        self._active_modules = {} # name -> module object

    @contextmanager
    def _phase(self, phase_name: str):
        logger.info(f"Entering phase: {phase_name}")
        try:
            yield
            logger.info(f"Phase {phase_name} completed successfully.")
        except Exception as e:
            logger.error(f"Phase {phase_name} failed: {e}")
            raise

    def get_current_module(self, module_path: str):
        name = self.namespace_manager._extract_name(module_path)
        return self._active_modules.get(name)

    def _extract_module_name(self, module_path: str) -> str:
         return self.namespace_manager._extract_name(module_path)

    def _warm_up(self, module: types.ModuleType):
        if hasattr(module, 'warmup'):
            module.warmup()
        elif hasattr(module, 'init'):
            # Potentially speculative init
            pass

    def _collect_metrics(self, duration: int) -> Dict[str, Any]:
        time.sleep(0.1) # Simulate logic
        return {"error_rate": 0.0, "latency": 10}

    def _validate_metrics(self, metrics: Dict[str, Any]) -> bool:
        return metrics["error_rate"] < 0.01

    def _force_gc(self):
        gc.collect()

    def _validate_memory_state(self):
        # Implementation of memory validation
        pass

    def hot_reload_module(self, module_path: str) -> bool:
        """零停机热重载"""
        
        module_name = self._extract_module_name(module_path)
        
        # 阶段1: 准备阶段（用户无感知）
        with self._phase("preparation"):
            # 1.1 创建隔离的草稿命名空间
            # Note: create_draft returns the DynamicNamespace wrapper, not just the dict
            dns = self.namespace_manager.create_draft(module_path)
            
            # 1.2 静默加载新版本
            # load_in_isolation now needs to work with strict file paths
            new_module = self._load_in_isolation(module_path, dns)
            
            # 1.3 预热新版本（初始化缓存等）
            self._warm_up(new_module)
            
            # 1.4 健康检查
            if not self.health_checker.check(new_module):
                return False
        
        # 阶段2: 分流阶段（逐渐切换流量）
        with self._phase("traffic_shifting"):
            # 2.1 启动蓝绿部署
            self.traffic_director.start_blue_green(
                old_module=self.get_current_module(module_path),
                new_module=new_module
            )
            
            # 2.2 逐渐增加新版本流量（1%, 5%, 25%, 50%, 100%）
            for percentage in [1, 5, 25, 50, 100]:
                self.traffic_director.shift_traffic(new_module, percentage)
                
                # 监控指标
                metrics = self._collect_metrics(duration=1)  # Reduced duration for demo
                if not self._validate_metrics(metrics):
                    self.traffic_director.rollback()
                    return False
        
        # 阶段3: 清理阶段
        with self._phase("cleanup"):
            # Update internal reference
            self._active_modules[module_name] = new_module

            # Promote layers in the underlying DynamicNamespace
            # dns is the DynamicNamespace object
            dns.promote(NamespaceLayer.SCRATCH, NamespaceLayer.STABLE)

            # 3.1 清理旧版本引用 (This is handled by promote+clear inside dns)
            # self.namespace_manager.cleanup_old_version(module_path)
            
            # 3.2 强制垃圾回收
            self._force_gc()
            
            # 3.3 验证内存状态
            self._validate_memory_state()
        
        return True
    
    def _load_in_isolation(self, module_path: str, dns: DynamicNamespace) -> types.ModuleType:
        """在隔离的命名空间中加载模块"""
        module_name = self._extract_module_name(module_path)
        
        # 使用自定义的加载器
        loader = IsolatedModuleLoader(dns, module_path)
        
        # 直接使用 loader 加载，绕过 sys.modules 检查，确保真正的隔离
        # 这是一个比 __import__ 更安全的方式，可以避免污染全局 sys.modules
        return loader.load_module(module_name)
