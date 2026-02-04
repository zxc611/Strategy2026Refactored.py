from __future__ import annotations
import threading
import time
import traceback
from typing import List, Dict, Any, Optional

class UIDiagnosisTool:
    """UI状态诊断工具"""
    
    def __init__(self):
        self.event_log: List[Dict[str, Any]] = []
        self.ui_state_snapshots: Dict[float, Any] = {}
        
    def log_event(self, event_name: str, details: Any = None) -> None:
        """记录UI事件"""
        entry = {
            'timestamp': time.time(),
            'event': event_name,
            'details': details,
            'ui_visible': self._check_ui_visible(),
            'thread_id': threading.get_ident(),
            'call_stack': self._get_call_stack(limit=5)
        }
        self.event_log.append(entry)
        # Using print for thread safety as discussed previously
        print(f"[UI事件] {event_name} - UI可见: {entry['ui_visible']}")
        
    def _check_ui_visible(self) -> bool:
        """检查UI是否可见"""
        try:
            # Try Tkinter first since the project uses Tkinter in ui_mixin.py
            import tkinter as tk
            # This is a heuristic check, might not be perfect for all setups
            # Requires access to the root window object, which might be tricky from here
            # We'll try to check if we can import logic from sys.modules
            return False 
            
            # The user code referenced PyQt5, keeping it as requested although project uses Tkinter
            # from PyQt5.QtWidgets import QApplication
            # app = QApplication.instance()
            # if app:
            #     windows = app.topLevelWidgets()
            #     visible_windows = [w for w in windows if w.isVisible()]
            #     return len(visible_windows) > 0
        except:
            pass
        return False
    
    def _get_call_stack(self, limit: int = 5) -> List[str]:
        """获取调用栈信息"""
        try:
            stack = traceback.extract_stack()[-limit:-1]
            return [f"{frame.filename}:{frame.lineno} - {frame.name}" for frame in stack]
        except Exception:
            return []
