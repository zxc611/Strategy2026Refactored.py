#!/bin/bash
ENV=${1:-production}
export TRADING_ENV=$ENV
python -m ali2026v3_trading.main
