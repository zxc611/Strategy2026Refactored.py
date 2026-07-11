# MODULE_ID: M2-425
"""
Test option sort alignment against design report (facade).

This module has been split into two focused modules:
  - test_option_sort_basic: basic sorting tests (Sections 2, 4.3, 5, 7, 8)
  - test_option_sort_decay: decay monitoring tests (Section 10)

This facade re-exports all public names for backward compatibility.
"""

from tests.test_option_sort_basic import *  # noqa: F401,F403
from tests.test_option_sort_decay import *  # noqa: F401,F403
