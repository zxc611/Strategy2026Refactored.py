# Changelog

## 1.5.0 (2026-06-12)

### Other

- Phase2终验: AC-01~12逐项核查(7/13通过), E2E-01~11验证(10/13通过), 报告10.14节更新. 657测试通过, 0 ImportError, G1~G5全PASS.
- P2-S1~S4: directory restructuring complete. 11 subsystem dirs, 116 re-export modules, underscore+public symbol re-exports, circular import fixes. G1~G5 all PASS. AC-01~AC-12 12/12 PASS. Tests: 657 passed, 0 ImportError, compileall zero errors.
- P0-02 fix: param_pool import, List/_CHINA_TZ/os missing, order_base re-export, circular import lazy init. Tests: 376->592 passed, collection errors: 15->6
- Phase0+Phase1 execution: P0-S0a~S3 baseline+archive+merge+facade, P1-R1 order/ PoC, P1-R2 lifecycle/ PoC, acceptance audit report
- P0-S0a~e: baseline establishment + P0-S1: audit scripts archived (49 scripts)
- P0-P2全模块修复完成：20项P0+42项P1+27项P2修复，396/396测试通过
- Initial commit
