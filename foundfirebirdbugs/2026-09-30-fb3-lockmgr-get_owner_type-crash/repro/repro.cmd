@echo off
rem ============================================================
rem Minimal reproduction of the Firebird 3.0 lock-manager crash
rem "Fatal lock interface error: Invalid lock type in get_owner_type()"
rem See ..\BUG_REPORT.md for the full report and context.
rem
rem Prerequisites:
rem   * Firebird 3.0 Super (service or application mode), TCP/IP 3053
rem   * fb-loadgen v1.0.2 binary in PATH (https://github.com/alexeykovyazin/firebirdgotester)
rem   * SYSDBA / masterkey
rem ============================================================

set DSN=127.0.0.1/3053:C:\dbs\OLTPEMUL_CRASH.FDB
set ISQL=C:\Program Files\Firebird\Firebird_3_0\isql.exe

rem --- 1. Provision the test database (page size 8192 requires isql) ---
set FIREBIRD_ISQL=%ISQL%
fb-loadgen provision --dsn "%DSN%" --init-docs 3000 --working-mode SMALL_01 --page-size 8192
if errorlevel 1 exit /b 1

rem --- 2. Load for 10 minutes; the server usually crashes within 2-7 minutes ---
rem       (watch firebird.log: "Fatal lock interface error: Invalid lock type in get_owner_type()")
fb-loadgen --profile oltp-emul --dsn "%DSN%" ^
  --warmup 30 --main 600 --cooldown 20 ^
  --emul-invariant-every 30 ^
  --csv results_crash.csv

rem --- Short variant (crash on ~2nd minute): ---
rem fb-loadgen --profile oltp-emul --dsn "%DSN%" --warmup 20 --main 120 --cooldown 10 --conn-max 10 --tx-variants off --csv results_crash.csv
