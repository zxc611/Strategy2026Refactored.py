from __future__ import annotations

import threading

_shadow_engine = None
_shadow_engine_lock = threading.Lock()