
import os

path = os.path.join(
    'c:', os.sep, 'Users', 'xu', 'AppData', 'Roaming',
    'InfiniTrader_SimulationX64', 'pyStrategy', 'demo',
    'ali2026v3_trading', 'mode_engine.py'
)

with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

print(f"Total lines: {len(lines)}")
print(f"Line 1125: {repr(lines[1124])}")
print(f"Line 1126: {repr(lines[1125])}")
print(f"Line 1127: {repr(lines[1126])}")
print(f"Line 1154: {repr(lines[1153])}")

new_lines = []
i = 0
while i < len(lines):
    line = lines[i]
    if i == 1124 and '_propagation_lock' in line and 'R10-P0-04' in line:
        new_lines.append(line)
        new_lines.append('            try:\n')
        i += 1
        if i < len(lines) and lines[i].strip() == '':
            i += 1
        while i < len(lines):
            cur = lines[i]
            stripped = cur.lstrip()
            if stripped.startswith('except Exception as e:') and cur.startswith('        except'):
                new_lines.append('            except Exception as e:\n')
                i += 1
                if i < len(lines):
                    new_lines.append('                ' + lines[i].lstrip())
                    i += 1
                if i < len(lines):
                    new_lines.append('                ' + lines[i].lstrip())
                    i += 1
                break
            else:
                if stripped:
                    new_lines.append('    ' + cur)
                else:
                    new_lines.append(cur)
                i += 1
    else:
        new_lines.append(lines[i])
        i += 1

with open(path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("Fixed reload_tvf_params indentation")
print(f"New total lines: {len(new_lines)}")
