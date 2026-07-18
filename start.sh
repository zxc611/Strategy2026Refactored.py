#!/bin/bash
ENV=${1:-production}
export TRADING_ENV=$ENV
if [ -z "$ORDER_SIGN_KEY" ]; then
    export ORDER_SIGN_KEY=$(head -c 32 /dev/urandom | base64 | head -c 32)
fi
if [ -z "$AUDIT_HMAC_KEY" ]; then
    export AUDIT_HMAC_KEY=$(head -c 32 /dev/urandom | base64 | head -c 32)
fi
# FIX-20260716-START: 项目已扁平化，ali2026v3_trading 模块已迁移至根目录
python main.py
