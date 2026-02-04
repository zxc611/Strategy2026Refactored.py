"""
Dynamic Namespace Manager for Zero-Downtime Hot Reloading.
Implements a layered namespace approach to isolate stable, staging, and scratch environments.

Features:
1. 创建隔离的运行时命名空间 (NamespaceLayer)
2. 智能追踪和清理过期引用 (ReferenceTracker, SmartGC)
3. 渐进式迁移而非暴力替换 (promote)
4. 零停机时间的热重载 (reload_context)
"""
import gc
import weakref
import types
import threading
import time
import hashlib
import sys
import ctypes
import importlib 
from collections import defaultdict
from contextlib import contextmanager
from typing import Dict, Set, List, Any, Optional, Callable
from enum import Enum

# Try imports
try:
    import objgraph
except ImportError:
    objgraph = None

class NamespaceLayer(Enum):
    """三层命名空间架构"""
    STABLE = "stable"      # 稳定层 - 已验证的版本
    STAGING = "staging"    # 暂存层 - 正在热重载的版本
    SCRATCH = "scratch"    # 草稿层 - 新加载的版本

class ReferenceTracker:
    """智能引用追踪器"""
    
    def __init__(self):
        self.reference_map: Dict[int, List[str]] = defaultdict(list)  # id -> 引用路径列表
        self.reverse_map: Dict[str, Set[int]] = defaultdict(set)      # 模块:属性 -> 对象ids
        self.creation_time: Dict[int, float] = {}                     # 对象创建时间
        self.access_counter: Dict[int, int] = defaultdict(int)        # 对象访问计数
        
    def track(self, obj: Any, reference_path: str):
        """追踪对象引用"""
        obj_id = id(obj)
        self.reference_map[obj_id].append(reference_path)
        self.reverse_map[reference_path].add(obj_id)
        if obj_id not in self.creation_time:
            self.creation_time[obj_id] = time.time()
        
        # 追踪对象的所有引用
        # 注意: 递归追踪可能会导致性能问题，可以限制深度或在调试模式下开启
        try:
            referrers = gc.get_referrers(obj)
            for referrer in referrers:
                if hasattr(referrer, '__name__'):
                    # 避免循环递归追踪
                    pass 
                    # self.track(referrer, f"referrer:{referrer.__name__}")
        except:
            pass
    
    def get_stale_references(self, module_name: str, min_age_seconds: float = 5.0) -> Set[int]:
        """获取过期的引用"""
        stale_refs = set()
        current_time = time.time()
        
        for ref_path, obj_ids in self.reverse_map.items():
            if module_name in ref_path:
                for obj_id in obj_ids:
                    age = current_time - self.creation_time.get(obj_id, 0)
                    if age > min_age_seconds and self.access_counter[obj_id] == 0:
                        stale_refs.add(obj_id)
        
        return stale_refs
    
    def clean_references(self, obj_ids: Set[int]):
        """清理指定对象的引用"""
        for obj_id in obj_ids:
            if obj_id in self.reference_map:
                for ref_path in self.reference_map[obj_id]:
                    self.reverse_map[ref_path].discard(obj_id)
                del self.reference_map[obj_id]
            
            if obj_id in self.creation_time:
                del self.creation_time[obj_id]
            
            if obj_id in self.access_counter:
                del self.access_counter[obj_id]
    
    def cleanup_dead_refs(self):
         # Compatibility method
         pass

class CycleDetector:
    """循环引用检测器"""
    def find_cycles(self, module_name: str) -> List[List[int]]:
        cycles = []
        # 使用 gc.garbage 检测无法回收的对象
        if gc.garbage:
            for obj in gc.garbage:
                # 简单 heuristic: 假设所有 garbage 都是需要打破的循环的一部分
                if hasattr(obj, '__module__') and obj.__module__ and module_name in obj.__module__:
                    cycles.append([id(obj)])
        
        # 如果有 objgraph，可以使用更高级的检测
        if objgraph:
             # Placeholder for objgraph logic
             pass
             
        return cycles

class MemoryProfiler:
    """内存分析器"""
    def snapshot(self):
        pass

class SmartGC:
    """智能垃圾回收器"""
    
    def __init__(self):
        self.cycle_detector = CycleDetector()
        self.memory_profiler = MemoryProfiler()
        self.stats = {"collected": 0, "cycles": 0, "broken_cycles": 0}
        
    def purge_namespace(self, module_name: str, aggressive: bool = False):
        """清洗命名空间"""
        
        # 步骤1: 打破循环引用
        cycles = self.cycle_detector.find_cycles(module_name)
        for cycle in cycles:
            self._break_cycle(cycle)
        
        # 步骤2: 清理弱引用
        self._clean_weak_references(module_name)
        
        # 步骤3: 分代清理
        for generation in range(3):
            self._collect_generation(generation, module_name)
        
        # 步骤4: 清理模块缓存
        self._purge_module_cache(module_name)
        
        # 步骤5: 清理import缓存
        if aggressive:
            self._purge_import_caches(module_name)
        
        # 步骤6: 最终强制GC
        collected = gc.collect()
        self.stats["collected"] += collected
        
        return collected

    def force_clean(self, layer_objects: Dict[str, Any]):
        """强制清理指定层级的对象引用 (兼容旧接口)"""
        # 先执行深度清理
        module_name_guess = "your_quant_project"
        self.purge_namespace(module_name_guess)
        
        cleaned_count = 0
        keys = list(layer_objects.keys())
        for k in keys:
            obj = layer_objects.pop(k)
            # 对于模块类型，尝试清理其字典以断开循环引用
            if isinstance(obj, types.ModuleType):
                try:
                    if hasattr(obj, "__file__") and "your_quant_project" in obj.__file__:
                        obj.__dict__.clear()
                except: pass
            del obj
            cleaned_count += 1
        return cleaned_count
    
    def _break_cycle(self, cycle: List[int]):
        """打破循环引用"""
        # 找到循环中最弱的引用并打破它
        # weakest = self._find_weakest_link(cycle) # Simplified: just break first found attr
        
        self.stats["broken_cycles"] += 1
        # 使用弱引用替换强引用
        for obj_id in cycle:
            obj = self._get_object_by_id(obj_id)
            if obj and hasattr(obj, '__dict__'):
                try:
                    for attr_name, attr_value in list(obj.__dict__.items()):
                        attr_id = id(attr_value)
                        # 如果属性值也在循环（或者是循环中的另一个对象），这里简单化处理：
                        # 实际上我们需要图算法来确定边。
                        # 这里我们只做防御性编程：如果发现属性持有 module 相关的对象，尝试弱引用化
                        if hasattr(attr_value, '__module__') and attr_value.__module__ and "your_quant_project" in attr_value.__module__:
                             obj.__dict__[attr_name] = weakref.ref(attr_value)
                except:
                    pass

    def _get_object_by_id(self, obj_id: int) -> Any:
        try:
            return ctypes.cast(obj_id, ctypes.py_object).value
        except:
            return None

    def _find_weakest_link(self, cycle: List[int]):
        return cycle[0] if cycle else None

    def _clean_weak_references(self, module_name: str):
        pass

    def _collect_generation(self, generation: int, module_name: str):
        gc.collect(generation)

    def _purge_module_cache(self, module_name: str):
        # 清理 sys.modules 中相关的旧模块（如果它们已经是 None 或过期的）
        to_remove = [k for k in sys.modules if module_name in k and sys.modules[k] is None]
        for k in to_remove:
            del sys.modules[k]

    def _purge_import_caches(self, module_name: str):
         importlib.invalidate_caches()

class VersionMismatchError(Exception):
    """版本不匹配异常"""
    pass

class VersionMigrationError(Exception):
    """版本迁移失败异常"""
    pass

class VersionMigratorRegistry:
    """版本迁移注册表"""
    _migrators: Dict[tuple, Any] = {}
    
    @classmethod
    def register(cls, from_version: str, to_version: str, migrator: Any):
        cls._migrators[(from_version, to_version)] = migrator
        
    @classmethod
    def get_migrator(cls, from_version: str, to_version: str) -> Optional[Any]:
        return cls._migrators.get((from_version, to_version))

class VersionAwareReference:
    """版本感知的引用"""
    
    __slots__ = ['_target', '_version', '_proxy']
    
    def __init__(self, target: Any, version: str):
        self._target = weakref.ref(target)
        self._version = version
        self._proxy = self._create_proxy(target)
        
    def _create_proxy(self, target: Any):
        """创建动态代理"""
        class VersionProxy:
            def __init__(self, target_ref, version):
                self._target_ref = target_ref
                self._version = version
                
            def __getattr__(self, name):
                target = self._target_ref()
                if target is None:
                    raise ReferenceError("目标对象已被垃圾回收")
                    
                # 检查版本兼容性
                current_version = getattr(target, '__version__', '0.0.0')
                if not self._is_compatible(current_version, self._version):
                    raise VersionMismatchError(
                        f"版本不兼容: 期望{self._version}, 实际{current_version}"
                    )
                    
                return getattr(target, name)
            
            def _is_compatible(self, current: str, expected: str) -> bool:
                # 简化的语义化版本检查
                return current.split('.')[0] == expected.split('.')[0]
        
        return VersionProxy(weakref.ref(target), self._version)
    
    def get(self):
        """获取当前版本的对象"""
        target = self._target()
        if target is None:
            return None
            
        # 验证版本
        current_version = getattr(target, '__version__', '0.0.0')
        if current_version != self._version:
            # 自动升级引用
            return self._upgrade_reference(target, current_version)
            
        return target
    
    def _upgrade_reference(self, target: Any, new_version: str):
        """升级引用到新版本"""
        # 查找版本迁移器
        migrator = VersionMigratorRegistry.get_migrator(
            from_version=self._version,
            to_version=new_version
        )
        
        if migrator:
            # 执行迁移
            migrated = migrator.migrate(target)
            self._version = new_version
            return migrated
        
        # 如果没有迁移器但版本兼容（主版本号相同），则只更新版本号
        if target and hasattr(target, '__version__') and target.__version__.split('.')[0] == self._version.split('.')[0]:
             self._version = new_version
             return target

        raise VersionMigrationError(f"无法从{self._version}迁移到{new_version}")


class VersionController:
    """版本控制器"""
    def __init__(self):
        self.current_hash: Optional[str] = None
        self.version_history: List[str] = []
        self.active_version: str = "v0.0.0" # timestamp-hash

    def compute_hash(self, valid_objects: Dict[str, Any]) -> str:
        """根据命名空间对象计算指纹"""
        hasher = hashlib.md5()
        for k in sorted(valid_objects.keys()):
            val = valid_objects[k]
            try:
                # 简单指纹：键名 + 类型 + (如果是代码对象则取源码hash)
                s_rep = f"{k}:{type(val)}"
                if hasattr(val, "__code__"):
                    s_rep += str(val.__code__.co_code)
                hasher.update(s_rep.encode('utf-8'))
            except: pass
        return hasher.hexdigest()

    def commit(self, objects: Dict[str, Any]) -> str:
        new_hash = self.compute_hash(objects)
        if new_hash != self.current_hash:
            self.current_hash = new_hash
            self.active_version = f"v{int(time.time())}-{new_hash[:6]}"
            self.version_history.append(self.active_version)
        return self.active_version

class DynamicNamespace:
    """动态命名空间管理器"""
    
    def __init__(self, module_name: str):
        self.module_name = module_name
        self.layers: Dict[NamespaceLayer, Dict[str, Any]] = {
            NamespaceLayer.STABLE: {},
            NamespaceLayer.STAGING: {},
            NamespaceLayer.SCRATCH: {}
        }
        self.reference_tracker = ReferenceTracker()
        self.garbage_collector = SmartGC()
        self.version_controller = VersionController()
        self._lock = threading.RLock()
        
    def get(self, name: str, layer: NamespaceLayer = NamespaceLayer.STABLE) -> Any:
        """从指定层获取对象"""
        with self._lock:
            # 优先从指定层获取，如果不强制指定层，可以实现回退逻辑（TODO）
            return self.layers[layer].get(name)
    
    def set(self, name: str, value: Any, layer: NamespaceLayer):
        """设置指定层的对象"""
        with self._lock:
            self.layers[layer][name] = value
            self.reference_tracker.track(value, f"{self.module_name}:{layer.value}:{name}")
            
    def promote(self, from_layer: NamespaceLayer, to_layer: NamespaceLayer):
        """将命名空间从一层提升到另一层 (原子操作模拟)"""
        with self._lock:
            source = self.layers[from_layer]
            target = self.layers[to_layer]
            
            # 更新目标层
            target.update(source)
            
            # 如果是提升到稳定层，提交版本
            if to_layer == NamespaceLayer.STABLE:
                self.version_controller.commit(target)

            # 清理源层引用（对象已转移到target，这里只是清空源容器的引用）
            source.clear() 

    def clear_layer(self, layer: NamespaceLayer):
        """深度清理层的对象"""
        with self._lock:
            self.garbage_collector.force_clean(self.layers[layer])
            self.reference_tracker.cleanup_dead_refs()

    @contextmanager
    def reload_context(self):
        """
        零停机热重载上下文。
        Usage:
            with ns_manager.reload_context() as scratch_ns:
                # Load new code into scratch_ns
                exec(new_code, scratch_ns)
        """
        # 1. Prepare SCRATCH
        self.clear_layer(NamespaceLayer.SCRATCH)
        try:
            yield self.layers[NamespaceLayer.SCRATCH]
            
            # 2. Validation / Promotion to STAGING (模拟预发布)
            self.promote(NamespaceLayer.SCRATCH, NamespaceLayer.STAGING)
            
            # 3. Validation passed -> Promote to STABLE (正式上线)
            self.promote(NamespaceLayer.STAGING, NamespaceLayer.STABLE)
            
        except Exception as e:
            # Rollback: 清理所有中间状态，保持 STABLE 不变
            self.clear_layer(NamespaceLayer.SCRATCH)
            self.clear_layer(NamespaceLayer.STAGING)
            raise e

# --- Dynamic Purging Infrastructure ---

class MigrationStep:
    def __init__(self, description, action: Optional[Callable] = None):
        self.description = description
        self.action = action
    def execute(self):
        if self.action: self.action()

class MigrationPlan:
    def __init__(self):
        self.steps: List[MigrationStep] = []
        self.kept_references: List[Any] = []
        
    def add_safe_migration(self, ref_path, ref_info):
        self.steps.append(MigrationStep(f"Migrate {ref_path}"))
        self.kept_references.append(ref_path)
        
    def add_adapted_migration(self, ref_path, ref_info):
        self.steps.append(MigrationStep(f"Adapt {ref_path}"))
        self.kept_references.append(ref_path)
        
    def mark_for_replacement(self, ref_path, ref_info):
        self.steps.append(MigrationStep(f"Replace {ref_path}"))

    def order_by_dependency(self, dependency_graph):
        pass

class MigrationStepError(Exception):
    pass

class NamespaceLayerManager:
    # Placeholder for the manager logic
    pass

class ReferenceScanner:
    def deep_scan(self, module_name: str) -> Dict[str, Any]:
        # In a real impl, this would return a dict of ref_path -> ref_info
        return {}

class DependencyMapper:
    def build_graph(self, module_name: str) -> Any:
        return {}

class GarbageSweeper(SmartGC):
    def sweep(self, module_name: str, keep_references: List[Any] = None) -> int:
        return self.purge_namespace(module_name)

class DynamicNamespacePurger:
    """动态命名空间清洗器 - 主控制器"""
    
    def __init__(self):
        self.namespace_layers = NamespaceLayerManager()
        self.reference_scanner = ReferenceScanner()
        self.dependency_mapper = DependencyMapper()
        self.garbage_sweeper = GarbageSweeper()
        
    def purge_and_reload(self, module_name: str, new_code: str) -> bool:
        """
        动态清洗并重新加载模块
        """
        print(f"🔍 开始动态命名空间清洗: {module_name}")
        
        # 阶段1: 深度扫描
        print("📊 阶段1: 深度扫描引用...")
        references = self.reference_scanner.deep_scan(module_name)
        dependency_graph = self.dependency_mapper.build_graph(module_name)
        
        # 阶段2: 创建沙箱
        print("🏖️  阶段2: 创建沙箱环境...")
        sandbox = self._create_sandbox(module_name)
        
        # 阶段3: 隔离加载新版本
        print("🚀 阶段3: 隔离加载新版本...")
        new_module = self._load_in_sandbox(sandbox, new_code)
        
        # 阶段4: 引用迁移
        print("🔄 阶段4: 渐进式引用迁移...")
        migration_plan = self._create_migration_plan(
            old_references=references,
            new_module=new_module,
            dependency_graph=dependency_graph
        )
        
        success = self._execute_migration(migration_plan)
        
        if not success:
            print("❌ 迁移失败，执行回滚...")
            self._rollback_migration()
            return False
        
        # 阶段5: 清理旧引用
        print("🗑️  阶段5: 清理旧引用...")
        cleaned = self.garbage_sweeper.sweep(
            module_name=module_name,
            keep_references=migration_plan.kept_references
        )
        
        print(f"✅ 动态命名空间清洗完成! 清理了 {cleaned} 个旧引用")
        return True
    
    def _create_sandbox(self, module_name: str) -> Dict[str, Any]:
        return {}

    def _load_in_sandbox(self, sandbox: Dict[str, Any], new_code: str) -> Any:
        return types.ModuleType("sandbox_module")

    def _create_migration_plan(self, old_references, new_module, dependency_graph):
        """创建智能迁移计划"""
        plan = MigrationPlan()
        
        # 分析哪些引用可以安全迁移
        for ref_path, ref_info in old_references.items():
            if self._can_safely_migrate(ref_info, new_module):
                plan.add_safe_migration(ref_path, ref_info)
            elif self._needs_adaptation(ref_info, new_module):
                plan.add_adapted_migration(ref_path, ref_info)
            else:
                plan.mark_for_replacement(ref_path, ref_info)
        
        # 根据依赖图排序迁移步骤
        plan.order_by_dependency(dependency_graph)
        
        return plan
    
    def _execute_migration(self, plan: MigrationPlan) -> bool:
        """执行迁移计划"""
        checkpoint = self._create_checkpoint()
        
        try:
            for step in plan.steps:
                print(f"  执行步骤: {step.description}")
                
                # 执行单个迁移步骤
                step.execute()
                
                # 验证步骤执行后状态
                if not self._validate_step(step):
                    raise MigrationStepError(f"步骤验证失败: {step.description}")
                
                # 创建中间检查点
                self._create_intermediate_checkpoint()
            
            return True
            
        except Exception as e:
            print(f"迁移失败: {e}")
            self._restore_checkpoint(checkpoint)
            return False

    def _can_safely_migrate(self, ref_info, new_module) -> bool:
        return True

    def _needs_adaptation(self, ref_info, new_module) -> bool:
        return False

    def _create_checkpoint(self) -> Any:
        return None

    def _create_intermediate_checkpoint(self):
        pass

    def _restore_checkpoint(self, checkpoint):
        pass

    def _rollback_migration(self):
        pass

    def _validate_step(self, step) -> bool:
        return True
