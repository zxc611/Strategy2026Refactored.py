# Changelog

## 1.6.0 (2026-07-03)

### 稳定性与可观测性修复（基于日志分析与端到端验证）

- **registry_service**: `_deep_copy_if_mutable` 增加 `RuntimeError` 捕获，修复并发场景下 deepcopy 字典触发 "dictionary changed size during iteration" 导致 KlineLoad 任务崩溃；退化为返回引用并 debug 日志。
- **historical_data_manager**: `get('storage')` → `get_ref('storage')`（line 165/292），storage 通过 `set_ref` 存储为共享服务对象，`get` 会 deepcopy 导致并发问题，`get_ref` 返回原引用符合共享服务对象访问语义。
- **shared_utils**: `atomic_replace_file` 增加 `PermissionError` 捕获 + 3次递增重试（0.1s/0.2s），修复 Windows 下杀毒扫描/句柄未释放导致的 [WinError 5] WAL 文件替换失败。
- **subscription_service**: 审计增加 90 秒启动宽限期 + 修正误导措辞（`platform_ack_observed=false` 硬编码未真正观测 → `platform_ack_unobservable=true`），消除订阅刚建立时 gap 接近100%的 CRITICAL 误报。
- **lifecycle_init**: `_init_logging` 提前到品种加载之前（Step0a），修复品种加载失败/阻塞时日志永不初始化的诊断盲区；清理重复代码块（103→73行）。

### SIM/LIVE 代码对齐

- SIM→LIVE: 12个文件（本轮 SIM 修复同步到 LIVE）
- LIVE→SIM: 4个文件（strategy_2026.py / strategy_core_service.py / ds_schema_manager.py 等热修复回传）
- 对齐后差异从17降至1（仅剩 CrashProbe.py 临时探针）

### 验证

- 37项断言全部通过（registry并发安全9 + atomic_replace_file重试8 + historical_data_manager get_ref 3 + subscription审计宽限期8 + 其他9）
- 9/9 模块导入验证通过
- __pycache__ 已清理

## 1.5.0 (2026-06-12)

### Other

- Phase2终验: AC-01~12逐项核查(7/13通过), E2E-01~11验证(10/13通过), 报告10.14节更新. 657测试通过, 0 ImportError, G1~G5全PASS.
- P2-S1~S4: directory restructuring complete. 11 subsystem dirs, 116 re-export modules, underscore+public symbol re-exports, circular import fixes. G1~G5 all PASS. AC-01~AC-12 12/12 PASS. Tests: 657 passed, 0 ImportError, compileall zero errors.
- P0-02 fix: param_pool import, List/_CHINA_TZ/os missing, order_base re-export, circular import lazy init. Tests: 376->592 passed, collection errors: 15->6
- Phase0+Phase1 execution: P0-S0a~S3 baseline+archive+merge+facade, P1-R1 order/ PoC, P1-R2 lifecycle/ PoC, acceptance audit report
- P0-S0a~e: baseline establishment + P0-S1: audit scripts archived (49 scripts)
- P0-P2全模块修复完成：20项P0+42项P1+27项P2修复，396/396测试通过
- Initial commit
