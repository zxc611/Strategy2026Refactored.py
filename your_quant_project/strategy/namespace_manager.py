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
from contextlib import contextmanager
from typing import Dict, Set, List, Any, Optional, Callable
from enum import Enum

class NamespaceLayer(Enum):
    """三层命名空间架构"""
    STABLE = "stable"      # 稳定层 - 已验证的版本
    STAGING = "staging"    # 暂存层 - 正在热重载的版本
    SCRATCH = "scratch"    # 草稿层 - 新加载的版本

class ReferenceTracker:
    """智能引用追踪器"""
    def __init__(self):
        # map: object_id -> list of logical paths
        self._registry: Dict[int, List[str]] = {}
        # map: object_id -> weakref to object
        self._refs: Dict[int, weakref.ref] = {}
        self._lock = threading.RLock()

    def track(self, obj: Any, path: str):
        """追踪对象引用"""
        if obj is None: return
        
        # 仅追踪复杂对象（类、函数、模块、实例），跳过基础类型
        if isinstance(obj, (int, float, str, bool)):
            return

        obj_id = id(obj)
        with self._lock:
            if obj_id not in self._registry:
                self._registry[obj_id] = []
                try:
                    # 创建弱引用，当对象被回收时回调（可选）
                    self._refs[obj_id] = weakref.ref(obj)
                except TypeError:
                    pass
            
            if path not in self._registry[obj_id]:
                self._registry[obj_id].append(path)

    def get_paths(self, obj: Any) -> List[str]:
        return self._registry.get(id(obj), [])
    
    def cleanup_dead_refs(self):
        """清理已死亡对象的记录"""
        with self._lock:
            dead_ids = [oid for oid, ref in self._refs.items() if ref() is None]
            for oid in dead_ids:
                self._registry.pop(oid, None)
                self._refs.pop(oid, None)

class SmartGC:
    """智能垃圾回收器"""
    def __init__(self):
        self.stats = {"collected": 0, "cycles": 0}

    def force_clean(self, layer_objects: Dict[str, Any]):
        """强制清理指定层级的对象引用"""
        cleaned_count = 0
        keys = list(layer_objects.keys())
        for k in keys:
            obj = layer_objects.pop(k)
            # 对于模块类型，尝试清理其字典以断开循环引用
            if isinstance(obj, types.ModuleType):
                try:
                    # 注意：仅清理应用层模块，避免破坏系统模块
                    if hasattr(obj, "__file__") and "your_quant_project" in obj.__file__:
                        obj.__dict__.clear()
                except: pass
            del obj
            cleaned_count += 1
        
        # 显式触发GC（建议在低频操作如热重载时调用）
        gc.collect()
        self.stats["collected"] += cleaned_count
        self.stats["cycles"] += 1
        return cleaned_count

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
