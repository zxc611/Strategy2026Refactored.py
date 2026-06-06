param([string]$Env = "production")
$env:TRADING_ENV = $Env
python -m ali2026v3_trading.main
