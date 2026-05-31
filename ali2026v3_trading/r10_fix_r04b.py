import os

path = os.path.join(
    'c:', os.sep, 'Users', 'xu', 'AppData', 'Roaming',
    'InfiniTrader_SimulationX64', 'pyStrategy', 'demo',
    'ali2026v3_trading', 'mode_engine.py'
)

with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# Find the broken block and replace it
# The broken block starts with "with self._propagation_lock:" and ends with the except return
marker_start = '        with self._propagation_lock:  # R10-P0-04'
marker_end = "            return {'success': False, 'error': str(e)}"

idx_start = content.find(marker_start)
if idx_start == -1:
    print("ERROR: marker_start not found")
    exit(1)

# Find the end after start
idx_end = content.find(marker_end, idx_start)
if idx_end == -1:
    print("ERROR: marker_end not found")
    exit(1)

# Include the full end line
idx_end += len(marker_end)

old_block = content[idx_start:idx_end]
print(f"Found block from {idx_start} to {idx_end}, length={len(old_block)}")
print("First 100 chars:", repr(old_block[:100]))
print("Last 100 chars:", repr(old_block[-100:]))

new_block = """        with self._propagation_lock:  # R10-P0-04修复: 加锁防止并发
            try:
                from ali2026v3_trading.tvf_param_loader import get_tvf_param_loader
                loader = get_tvf_param_loader()
                yaml_params = loader.reload(config_path)
                if self._config is None:
                    return {'success': False, 'error': 'No active ModeConfig'}
                new_config = loader.apply_to_mode_config(self._config)
                self._config = new_config
                # P-24修复: 重载后更新已传播组件的参数
                for comp_name in self._propagated_components:
                    comp = self._component_registry.get(comp_name)
                    if comp is not None:
                        try:
                            if hasattr(comp, 'update_tvf_config'):
                                comp.update_tvf_config(new_config)
                            elif hasattr(comp, 'set_params'):
                                comp.set_params({'tvf_enabled': new_config.tvf_enabled})
                        except Exception as _e:
                            logging.warning('[P-24] 更新组件%s参数失败: %s', comp_name, _e)
                logging.info(
                    '[ModeEngine] TVF参数热重载成功: tvf_enabled=%s source=yaml propagated=%s',
                    new_config.tvf_enabled, self._propagated_components,
                )
                return {
                    'success': True,
                    'tvf_enabled': new_config.tvf_enabled,
                    'params_source': 'yaml' if yaml_params else 'default',
                }
            except Exception as e:
                logging.error('[ModeEngine] TVF参数热重载失败: %s', e)
                return {'success': False, 'error': str(e)}"""

content = content[:idx_start] + new_block + content[idx_end:]

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed reload_tvf_params successfully!")
