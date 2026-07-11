"""Reproduce strategy initialization blocking."""
import sys
import os
import time
import threading
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("=== Step 1: Import package ===")
t0 = time.time()
try:
    import ali2026v3_trading
    print(f"[OK] import ali2026v3_trading ({time.time()-t0:.2f}s)")
except Exception as e:
    print(f"[FAIL] import: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

print("\n=== Step 2: Import Strategy2026 ===")
t0 = time.time()
try:
    from ali2026v3_trading.strategy.strategy_2026 import Strategy2026
    print(f"[OK] import Strategy2026 ({time.time()-t0:.2f}s)")
except Exception as e:
    print(f"[FAIL] import Strategy2026: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

print("\n=== Step 3: Create StrategyCoreService ===")
t0 = time.time()
try:
    from ali2026v3_trading.strategy.strategy_core_service import StrategyCoreService
    core = StrategyCoreService()
    print(f"[OK] StrategyCoreService created ({time.time()-t0:.2f}s)")
except Exception as e:
    print(f"[FAIL] StrategyCoreService: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

print("\n=== Step 4: Test bind_platform_apis with mock strategy ===")
t0 = time.time()
class MockStrategy:
    """Mock strategy object for testing bind_platform_apis."""
    def __init__(self):
        self.sub_market_data = lambda exchange, instrument_id: None
        self.unsub_market_data = lambda exchange, instrument_id: None
        self.get_instrument = lambda instrument_id: None
        self.insert_order = lambda **kwargs: None
        self.cancel_order = lambda order_id: None
        self.get_position = lambda: {}
        self.get_orders = lambda: []
        self.market_center = None
        self.infini = None

mock = MockStrategy()
try:
    # Run with timeout to detect blocking
    result_holder = [None, None]  # [result, error]
    def _run():
        try:
            core.bind_platform_apis(mock)
            result_holder[0] = "OK"
        except Exception as e:
            result_holder[1] = e

    th = threading.Thread(target=_run, daemon=True)
    th.start()
    th.join(timeout=15.0)
    if th.is_alive():
        print(f"[BLOCKED] bind_platform_apis did not complete within 15s!")
        sys.exit(2)
    elif result_holder[1]:
        print(f"[FAIL] bind_platform_apis error: {result_holder[1]}")
        import traceback; traceback.print_exc()
    else:
        print(f"[OK] bind_platform_apis completed ({time.time()-t0:.2f}s)")
except Exception as e:
    print(f"[FAIL] bind_platform_apis: {e}")
    import traceback; traceback.print_exc()

print("\n=== Step 5: Test initialize (matching strategy_2026.py call) ===")
t0 = time.time()
try:
    result_holder = [None, None]
    def _run_init():
        try:
            # Match strategy_2026.py: self.strategy_core.initialize(self.config)
            result_holder[0] = core.initialize({'strategy': mock})
        except Exception as e:
            result_holder[1] = e

    th = threading.Thread(target=_run_init, daemon=True)
    th.start()
    th.join(timeout=120.0)
    if th.is_alive():
        print(f"[BLOCKED] initialize did not complete within 120s!")
        sys.exit(2)
    elif result_holder[1]:
        print(f"[FAIL] initialize error: {result_holder[1]}")
        import traceback; traceback.print_exc()
    else:
        print(f"[OK] initialize result={result_holder[0]} ({time.time()-t0:.2f}s)")
except Exception as e:
    print(f"[FAIL] initialize: {e}")
    import traceback; traceback.print_exc()

print("\n=== DONE ===")
