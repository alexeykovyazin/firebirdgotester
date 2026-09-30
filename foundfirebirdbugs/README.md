# Found Firebird bugs

Public bug reports produced while testing Firebird with fb-loadgen. Each
subdirectory is one report: `BUG_REPORT.md` (symptom, versions, crash data,
minimal reproduction), `repro/` (commands), `evidence/` (logs and analysis).

Large binary artifacts (crash dumps) are not included — available on request.

| Date | Report | Affected |
|---|---|---|
| 2026-09-30 | [2026-09-30-fb3-lockmgr-get_owner_type-crash](2026-09-30-fb3-lockmgr-get_owner_type-crash/BUG_REPORT.md) | Firebird 3.0.x Windows Super: crash `Fatal lock interface error: Invalid lock type in get_owner_type()` under concurrent OLTP load (AV in engine12.dll, dump analyzed) |
