Get-Process -Name python -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like '*ali2026v3_trading*' } | Stop-Process -Force
