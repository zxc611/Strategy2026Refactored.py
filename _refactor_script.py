import re
import sys

FILE_PATH = r"C:\Users\xu\AppData\Roaming\InfiniTrader_SimulationX64\pyStrategy\demo\ali2026v3_trading\risk_service.py"

CHECK_METHODS = [
    "check_before_trade",
    "_check_safety_meta_layer",
    "_check_strategy_status",
    "_check_rate_limit",
    "_check_position_limit",
    "_check_risk_ratio",
    "_check_risk_consistency",
    "_check_single_trade_risk",
    "_check_sharpe_iron_rule",
    "_check_e7_residual_block",
    "_check_capital_sufficiency_in_trade",
    "_check_spread_degradation",
    "_check_governance_violations",
    "_check_greeks_limits",
    "_check_consecutive_loss_protection",
    "_check_life_expectancy",
    "check_price_limit",
    "check_expiry_risk",
]

COMPUTE_METHODS = [
    "_compute_stress_test",
    "_compute_pnl_attribution",
    "compute_simplified_pnl",
    "calculate_risk_metrics",
    "compute_mode_position_size",
    "calculate_asymmetric_drawdown_limit",
    "compute_decision_score",
    "_compute_position_scale",
    "_compute_life_score",
    "_compute_cycle_resonance_score",
    "_compute_phase_quality_score",
    "_compute_greeks_usage_score",
    "_compute_consecutive_loss_score",
    "_compute_asymmetric_drawdown_score",
    "_compute_tri_validation_score",
    "_compute_alpha_decay_score",
    "_compute_cross_correlation_score",
    "_compute_liquidity_score",
]

CONFIG_METHODS = [
    "_get_signal_cooldown",
    "_get_max_signals_per_window",
    "_get_rate_limit_window",
    "_get_max_risk_ratio",
    "_get_total_position_limit",
    "_get_contract_multiplier",
]

def find_method_end(lines, start_idx, base_indent):
    end_idx = start_idx + 1
    while end_idx < len(lines):
        line = lines[end_idx]
        stripped = line.rstrip()
        if stripped == '':
            end_idx += 1
            continue
        current_indent = len(stripped) - len(stripped.lstrip())
        if current_indent <= base_indent and stripped.lstrip().startswith(('def ', 'class ', '@')):
            break
        if current_indent < base_indent and stripped.strip():
            break
        end_idx += 1
    while end_idx > start_idx + 1 and lines[end_idx - 1].strip() == '':
        end_idx -= 1
    return end_idx

def find_decorator_start(lines, def_idx):
    idx = def_idx - 1
    while idx >= 0:
        stripped = lines[idx].strip()
        if stripped.startswith('@'):
            idx -= 1
        elif stripped == '':
            idx -= 1
        else:
            break
    return idx + 1

def main():
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    all_methods = CHECK_METHODS + COMPUTE_METHODS + CONFIG_METHODS
    method_map = {}
    for m in CHECK_METHODS:
        method_map[m] = 'self._check_service'
    for m in COMPUTE_METHODS:
        method_map[m] = 'self._compute_service'
    for m in CONFIG_METHODS:
        method_map[m] = 'self._config_provider'

    removed_ranges = []

    for method_name in all_methods:
        delegate_target = method_map[method_name]
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith(f'def {method_name}('):
                indent = len(line) - len(line.lstrip())
                if indent < 4 or indent > 12:
                    continue
                base_indent_str = line[:indent]
                sig_match = re.match(r'^(\s*)def ' + re.escape(method_name) + r'\((.*?)\)(\s*->.*?)?:', line)
                if not sig_match:
                    continue
                params_str = sig_match.group(2)
                return_annotation = sig_match.group(3) or ''
                decorator_start = find_decorator_start(lines, i)
                method_end = find_method_end(lines, i, indent)
                decorators = []
                for di in range(decorator_start, i):
                    dline = lines[di].strip()
                    if dline.startswith('@'):
                        decorators.append(dline)
                param_names = []
                for p in params_str.split(','):
                    p = p.strip()
                    if not p or p.startswith('*') or p.startswith('/'):
                        if p:
                            param_names.append(p.split('=')[0].strip())
                        continue
                    name = p.split(':')[0].split('=')[0].strip()
                    if name and name != 'self':
                        param_names.append(name)
                args_call = ', '.join(param_names)
                new_lines = []
                for d in decorators:
                    new_lines.append(base_indent_str + d + '\n')
                new_lines.append(line.rstrip() + '\n')
                new_lines.append(f'{base_indent_str}    return {delegate_target}.{method_name}({args_call})\n')
                removed_ranges.append((decorator_start, method_end, new_lines))
                break

    safety_start = None
    safety_end = None
    for i, line in enumerate(lines):
        if line.strip().startswith('# ============') and i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if 'L-1' in next_line and 'SafetyMetaLayer' in next_line:
                safety_start = i
        if safety_start is not None and safety_end is None:
            if line.strip().startswith('def cleanup_safety_layer'):
                j = i + 1
                while j < len(lines):
                    if lines[j].strip() and not lines[j].startswith(' ') and not lines[j].startswith('\t'):
                        break
                    j += 1
                while j < len(lines) and lines[j].strip() == '':
                    j += 1
                safety_end = j
                break

    if safety_start is not None and safety_end is None:
        for i in range(safety_start, len(lines)):
            if lines[i].strip().startswith('_risk_service_lock'):
                safety_end = i
                break

    risk_metrics_line = None
    for i, line in enumerate(lines):
        if line.strip().startswith('class RiskMetrics'):
            j = i + 1
            while j < len(lines) and lines[j].strip():
                j += 1
            while j < len(lines) and lines[j].strip() == '':
                j += 1
            risk_metrics_line = j
            break

    import_block = (
        "\n# CC-03/AP-02修复: 子服务导入（延迟导入避免循环依赖）\n"
        "from ali2026v3_trading.risk_check_service import RiskCheckService\n"
        "from ali2026v3_trading.risk_compute_service import RiskComputeService\n"
        "from ali2026v3_trading.risk_circuit_breaker import SafetyMetaLayer, get_safety_meta_layer, cleanup_safety_layer\n"
        "from ali2026v3_trading.risk_config_provider import RiskConfigProvider\n"
    )

    init_insert = None
    for i, line in enumerate(lines):
        if 'self._risk_engine = None' in line and 'RiskEngine导入失败' in lines[i+1] if i+1 < len(lines) else False:
            init_insert = i + 2
            break
        if 'self._risk_engine' in line:
            init_insert = i + 1
            break

    init_block = (
        "\n"
        "        # CC-03/AP-02修复: 子服务实例化\n"
        "        self._check_service = RiskCheckService(self)\n"
        "        self._compute_service = RiskComputeService(self)\n"
        "        self._config_provider = RiskConfigProvider(self)\n"
    )

    edits = sorted(removed_ranges, key=lambda x: x[0], reverse=True)

    result_lines = list(lines)

    for start, end, new_content in edits:
        result_lines[start:end] = new_content

    if safety_start is not None and safety_end is not None:
        s_start = safety_start
        s_end = safety_end
        for start, end, _ in edits:
            if start < s_start:
                offset = len(new_content) - (end - start)
                pass
        result_lines[s_start:s_end] = []

    if risk_metrics_line is not None:
        result_lines.insert(risk_metrics_line, import_block)

    if init_insert is not None:
        adjusted = init_insert
        for start, end, new_content in edits:
            if end <= init_insert:
                adjusted += len(new_content) - (end - start)
        if safety_start is not None and safety_end is not None and safety_end <= init_insert:
            adjusted -= (safety_end - safety_start)
        result_lines.insert(adjusted, init_block)

    with open(FILE_PATH, 'w', encoding='utf-8') as f:
        f.writelines(result_lines)

    print(f"Done. Original: {len(lines)} lines, Result: {len(result_lines)} lines")
    print(f"SafetyMetaLayer removed: lines {safety_start}-{safety_end}")
    print(f"Methods replaced: {len(removed_ranges)}")

if __name__ == '__main__':
    main()
