# FIX-20260716-STOP: 项目扁平化后启动命令变为 python main.py，同时保留旧模块名兼容
# FIX-R R12-5: PowerShell 5.1中Get-Process返回的Process对象没有CommandLine属性
# 修复: 改用Get-CimInstance Win32_Process获取CommandLine
$_py_procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue
if ($_py_procs) {
    $_py_procs | Where-Object { $_.CommandLine -like '*ali2026v3_trading*' -or $_.CommandLine -like '*main.py*' } | ForEach-Object {
        Write-Host "[stop.ps1] 停止进程 PID=$($_.ProcessId) CMD=$($_.CommandLine.Substring(0, [Math]::Min(80, $_.CommandLine.Length)))"
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host "[stop.ps1] 未找到python进程"
}
