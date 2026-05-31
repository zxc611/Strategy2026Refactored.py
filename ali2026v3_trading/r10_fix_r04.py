
import os

path = os.path.join(
    'c:', os.sep, 'Users', 'xu', 'AppData', 'Roaming',
    'InfiniTrader_SimulationX64', 'pyStrategy', 'demo',
    'ali2026v3_trading', 'mode_engine.py'
)

with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

start = None
for i, line in enumerate(lines):
    if '_propagation_lock' in line and 'R10-P0-04' in line:
        start = i
        break

if start is None:
    print("ERROR: Could not find with _propagation_lock line")
    exit(1)

print(f"Found with block at line {start+1}")

end = None
for i in range(start+1, min(start+40, len(lines))):
    if lines[i].startswith('        except Exception as e:'):
        end = i
        break

if end is None:
    print("ERROR: Could not find except line")
    exit(1)

print(f"Found except at line {end+1}")

new_block = []
new_block.append('        with self._propagation_lock:  # R10-P0-04修复: 加锁防止并发\n')
new_block.append('            try:\n')

for i in range(start+1, end):
    line = lines[i]
    stripped = line.lstrip()
    if stripped == '':
        continue
    new_block.append('    ' + line)

new_block.append('            except Exception as e:\n')
for i in range(end+1, min(end+3, len(lines))):
    stripped = lines[i].lstrip()
    new_block.append('                ' + stripped)

new_lines = lines[:start] + new_block + lines[end+3:]

with open(path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"Fixed! Old lines: {len(lines)}, New lines: {len(new_lines)}")
