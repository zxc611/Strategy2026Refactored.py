import xml.etree.ElementTree as ET

tree = ET.parse('coverage.xml')
root = tree.getroot()

print(f"Total: lines-valid={root.attrib['lines-valid']}, lines-covered={root.attrib['lines-covered']}, line-rate={root.attrib['line-rate']}")

targets = [
    'signal_service', 'signal_generator', 'config_service',
    'lifecycle_manager', 'lifecycle_state_machine', 'lifecycle_state',
    'lifecycle_init', 'lifecycle_bind', 'lifecycle_callbacks',
    'lifecycle_monitor', 'lifecycle_platform', 'lifecycle_resource',
    'lifecycle_transition', 'lifecycle_parallel', 'lifecycle_parallel_ops',
    'order_service', 'order_executor', 'order_base',
    'strategy_core_service', 'risk_service_core', 'maintenance_service',
    'shared_utils', 'shared_utils_infra',
]

found = {}
for pkg in root.iter('package'):
    for cls in pkg.iter('class'):
        fname = cls.attrib.get('filename', '')
        for t in targets:
            if t in fname and t not in found:
                lr = float(cls.attrib.get('line-rate', '0'))
                lv = cls.attrib.get('lines-valid', '?')
                lc = cls.attrib.get('lines-covered', '?')
                found[t] = (fname, lr, lv, lc)
                break

for t in targets:
    if t in found:
        fname, lr, lv, lc = found[t]
        status = "PASS" if lr >= 0.80 else ("WARN" if lr >= 0.70 else "FAIL")
        print(f"  {fname}: {lr*100:.1f}% (lines {lc}/{lv}) [{status}]")
    else:
        print(f"  {t}: NOT FOUND in coverage")

lifecycle_total_valid = 0
lifecycle_total_covered = 0
for t in targets:
    if t in found and 'lifecycle' in t:
        _, _, lv, lc = found[t]
        if lv != '?' and lc != '?':
            lifecycle_total_valid += int(lv)
            lifecycle_total_covered += int(lc)

if lifecycle_total_valid > 0:
    lr = lifecycle_total_covered / lifecycle_total_valid
    status = "PASS" if lr >= 0.70 else "FAIL"
    print(f"\n  lifecycle aggregate: {lr*100:.1f}% ({lifecycle_total_covered}/{lifecycle_total_valid}) [{status}]")