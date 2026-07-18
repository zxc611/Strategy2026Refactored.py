param([string]$Env = "production")
$env:TRADING_ENV = $Env
if (-not $env:ORDER_SIGN_KEY) {
    $env:ORDER_SIGN_KEY = -join ((1..32) | ForEach-Object { [char](Get-Random -Min 33 -Max 126) })
}
if (-not $env:AUDIT_HMAC_KEY) {
    $env:AUDIT_HMAC_KEY = -join ((1..32) | ForEach-Object { [char](Get-Random -Min 33 -Max 126) })
}
# FIX-20260716-START: 项目已扁平化，ali2026v3_trading 模块已迁移至根目录
python main.py
