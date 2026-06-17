# MODULE_ID: M2-413
import sys
import os
import inspect
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ali2026v3_trading.data.storage_lifecycle_mixin import StorageLifecycleService, _StorageLifecycleMixin
from ali2026v3_trading.data.storage_init_mixin import StorageInitService, _StorageInitMixin
from ali2026v3_trading.data.storage_async_writer_mixin import StorageAsyncWriterService, _StorageAsyncWriterMixin
from ali2026v3_trading.data.storage_core import _StorageCoreMixin


def test_lifecycle_service_exists():
    assert StorageLifecycleService is not None
    assert _StorageLifecycleMixin is StorageLifecycleService


def test_init_service_exists():
    assert StorageInitService is not None
    assert _StorageInitMixin is StorageInitService


def test_async_writer_service_exists():
    assert StorageAsyncWriterService is not None
    assert _StorageAsyncWriterMixin is StorageAsyncWriterService


def test_storage_core_no_mixin_inheritance():
    bases = _StorageCoreMixin.__bases__
    assert _StorageInitMixin not in bases, f"_StorageCoreMixin should NOT inherit _StorageInitMixin"
    assert _StorageAsyncWriterMixin not in bases, f"_StorageCoreMixin should NOT inherit _StorageAsyncWriterMixin"
    assert _StorageLifecycleMixin not in bases, f"_StorageCoreMixin should NOT inherit _StorageLifecycleMixin"


def test_storage_core_has_composition():
    assert hasattr(_StorageCoreMixin, '__getattr__')
    scm = _StorageCoreMixin.__new__(_StorageCoreMixin)
    assert type(scm).__name__ == '_StorageCoreMixin'


if __name__ == '__main__':
    test_lifecycle_service_exists()
    test_init_service_exists()
    test_async_writer_service_exists()
    test_storage_core_no_mixin_inheritance()
    test_storage_core_has_composition()
    print("ALL 5 ASSERTIONS PASSED for Fix 3+4+5 (Storage)")