# Firebird 3.0 (Windows, Super): crash "Fatal lock interface error: Invalid lock type in get_owner_type()" under concurrent OLTP load

**English abstract.** Firebird 3.0 SuperServer on Windows crashes reproducibly
under a concurrent OLTP workload (10–20 TCP connections, short read-committed
NOWAIT transactions executing stored procedures). Just before the crash the
server logs `Fatal lock interface error: Invalid lock type in get_owner_type()`;
a full-memory dump captured at the moment shows an unhandled access violation
(0xC0000005) in `engine12.dll` (faulting address `engine12.dll+0x27915a`,
build 3.0.13.33818; raw return addresses included). Reproduces in minutes on
two independent installs (HQbird 3.0.15.33884 service and plain Firebird
3.0.13.33818), with and without 2PC/limbo traffic. A complete memory dump,
all logs, and exact reproduction commands are attached in this folder.

---

## 1. Резюме

Под конкурентной OLTP-нагрузкой (генератор **fb-loadgen** v1.0.2, профиль
oltp-emul: 10–20 соединений, короткие read-committed NOWAIT-транзакции,
хранимые процедуры бизнес-нагрузки) Firebird 3.0 на Windows стабильно падает:

- в `firebird.log` перед смертью: **`Fatal lock interface error: Invalid lock
  type in get_owner_type()`**;
- процесс завершается (порождение краша — access violation, см. дамп);
- служба (в сервисном варианте) перезапускает процесс за 1–2 секунды, поэтому
  со стороны выглядит как «короткие всплески обрывов соединений»;
- нагрузочный клиент переживает краши (перестройка пула) и доходит до конца.

Воспроизводимость — **из минут в минуты**: 5 прогонов из 5 на двух независимых
установках, с включёнными и выключенными 2PC/limbo-завершениями, при 10 и 20
соединениях.

## 2. Среды воспроизведения

| | Установка A (сервис) | Установка B (dev-копия) |
|---|---|---|
| Версия | WI-V3.0.15.33884 **HQbird** | WI-V3.0.13.33818 (plain Firebird 3.0) |
| Режим запуска | Windows service, Super (по умолчанию) | `firebird.exe -a` (application) в консоли пользователя |
| Порт | 3053 | 3160 |
| Протокол клиента | TCP/IP (127.0.0.1) | TCP/IP (127.0.0.1) |
| OS | Windows 10/11 x64 (см. `evidence/os_info.txt`) | та же машина |
| БД | page size 8192, ODS 12.0, charset NONE | та же |

Примеры серверных строк версии: `WI-V3.0.15.33884 Firebird 3.0 HQbird/tcp
(test-node)/P15:C`.

## 3. Симптом и хронология

Сервисный случай (установка A), 10-минутный прогон 2026-09-30 12:42:37–12:53:28
(локальное время), нагрузка 2→20 соединений:

- серверные краши (записи `Fatal lock interface error` в firebird.log):
  **12:43:58, 12:45:47, 12:48:43, 12:52:39** — 4 краша за 10 минут;
- каждая смерть видна и клиентски: волна
  `connectex: No connection could be made` в логе нагрузки в те же секунды
  (12:43:24 … 12:52:39), всего 522 `connection_error`;
- PID слушателя 3053 менялся посреди прогона (24596 → 51220) — служба
  перезапускала процесс;
- в конце прогона 5 stop-таймаутов воркеров (сокеты, повисшие после
  последнего краша).

Dev-копия (установка B): краши в каждом прогоне на 2–7-й минуте
(4 из 4 прогонов), при 10 и 20 соединениях, с `--tx-variants off` и с
`emul-safe` (см. `evidence/devcopy_run*.log`).

До первого краша поведение полностью штатное: инвариантные проверки проходят,
известные классы ошибок (lock conflict / deadlock / «stuck in limbo» /
EX_SNAPSHOT_ISOLATION_REQUIRED) классифицируются корректно.

## 4. Краш-дамп и стек

Два полных дампа сняты procdump'ом на необработанном исключении:

- **установка B' (HQbird 3.0.15.33884, приложение)** —
  `dumps/firebird.exe_260930_154838.dmp` (661 МБ), краш в D6-профиле
  (20 соединений, emul-safe) на ~5-й минуте; для этого билда есть PDB
  (`plugins\engine12.pdb` HQbird), поэтому стек символизирован — основной;
- установка B (plain 3.0.13.33818) — `dumps/firebird.exe_260930_141400.dmp`
  (259 МБ); PDB для plain-сборки не поставляется, там только адреса
  (`evidence/crash_dump_analysis.txt`).

Контекст исключения (B'):

```
ExceptionCode   : 0xC0000005 (ACCESS_VIOLATION)
ExceptionAddress: ntdll.dll (fast-fail/terminate path after the engine AV)
ThreadID        : 57272
```

Символизированный стек падающего потока (B'; сырой скан стека, порядок —
расположение на стеке; полный список из 60 адресов —
`evidence/crash_stack_symbolized_hq3015.txt`, анализ дампа —
`evidence/crash_dump_hq3015.txt`):

```
Firebird::MemPool::releaseBlock            <- crash site (AV inside pool release)
Firebird::MemPool::alloc / allocate / allocate2 / Firebird::MemoryPool::calloc
Firebird::FreeObjects<LinkedList,LowLimits>::allocateBlock
Jrd::vec<Lock *>::newVector
hash_allocate
hash_get_lock
hash_remove_lock
internal_dequeue
LCK_release
TRA_release_transaction
purge_transactions
purge_attachment
Jrd::JAttachment::freeEngineData
Jrd::JAttachment::cancelOperation
Jrd::TipCache::setState / TRA_set_state / TRA_prepare
Jrd::jrd_rel::delPages / write_page / Jrd::BufferDesc::unLockIO
EXE_execute_db_triggers
Firebird::ThreadSync::getThread / Jrd::thread_db::thread_db
```

Читается так: при снятии лока (`LCK_release → internal_dequeue →
hash_remove_lock`) lock-хеш перевыделяет вектор (`hash_allocate →
vec<Lock*>::newVector → MemPool::alloc`), и падает `MemPool::releaseBlock` —
повреждение lock-менеджера/пула памяти; вокруг — разборка транзакций и
аттачмента (`TRA_release_transaction`, `purge_transactions`,
`purge_attachment`, `freeEngineData`), а также 2PC-путь (`TRA_prepare`,
`TipCache::setState`). Это согласуется с предсмертными записями сервера
`DEBUG_LCK_LIST: found not detached lock ... in deleting pool`.

Установка B (3.0.13, только адреса): кластер возвратов
`engine12.dll+0x18c4xx…0x18f2xx` — та же lock-область; сырой скан в
`evidence/crash_dump_analysis.txt`.

В момент снятия дампов procdump фиксирует серию first-chance
`E06D7363 (AVStatus_exception@Firebird)` (C++ статусы Firebird),
завершающуюся необработанным AV — фатальный путь
«Invalid lock type in get_owner_type()» проливается через status_exception.

Условия для стеков с именами: нужны PDB — HQbird поставляет их в комплекте
(`C:\HQbird\Firebird30\plugins\engine12.pdb`); plain-дистрибутив Firebird 3.0
для Windows PDB не содержит.

## 5. Минимальное воспроизведение

Нужен только генератор нагрузки (open-source, один бинарник):

- **fb-loadgen v1.0.2** — https://github.com/alexeykovyazin/firebirdgotester
  (Release v1.0.2, сборка: `go build`; либо собрать из тега v1.0.2).

Шаги (Windows x64, любой локальный Firebird 3.0 Super):

```bat
:: 1. Создать тестовую БД oltp-emul (3000 документов, page 8192).
::    Для page size 8192 требуется isql: указать FIREBIRD_ISQL на isql.exe
::    из установки Firebird 3.
set FIREBIRD_ISQL=C:\Program Files\Firebird\Firebird_3_0\isql.exe
fb-loadgen provision --dsn "127.0.0.1/3053:C:\dbs\OLTPEMUL_CRASH.FDB" ^
    --init-docs 3000 --working-mode SMALL_01 --page-size 8192

:: 2. Нагрузка: 20 соединений, extended-профиль, 10 минут main.
::    tx-variants можно оставить по умолчанию (emul-safe) ИЛИ выключить —
::    падает и так, и так.
fb-loadgen --profile oltp-emul ^
    --dsn "127.0.0.1/3053:C:\dbs\OLTPEMUL_CRASH.FDB" ^
    --warmup 30 --main 600 --cooldown 20 ^
    --emul-invariant-every 30 ^
    --csv results_crash.csv

:: 3. Наблюдать firebird.log: через 2–7 минут появляется
::    "Fatal lock interface error: Invalid lock type in get_owner_type()",
::    процесс завершается; в логе нагрузки в этот момент — волна
::    "connectex: No connection could be made".
```

Вариант с сокращённым циклом (краш на 2-й минуте): `--warmup 20 --main 120
--cooldown 10 --conn-max 10 --tx-variants off`.

Что нагрузки важнее всего: 8–20 одновременных соединений, NOWAIT
read-committed, интенсивные хранимые процедуры с конфликтами (update
conflicts/deadlocks — это нормальная часть нагрузки), секундный think-time.
Лимбо/2PC и обрывы соединений НЕ обязательны (падает и с `--tx-variants off`).

Для снятия дампа на Windows (нужен один раз):

```bat
procdump64 -accepteula -ma -e -f C0000005 -x <dir_for_dumps> ^
    "C:\...\firebird.exe" -a
:: …и запустить нагрузку на этот экземпляр; дамп пишется при AV автоматически.
```

(Для сервисной установки удобно вместо procdump настроить WER LocalDumps для
образа `firebird.exe` — от администратора:

```bat
reg add "HKLM\SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps\firebird.exe" ^
  /v DumpFolder /t REG_EXPAND_SZ /d C:\dumps /f
reg add "HKLM\SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps\firebird.exe" ^
  /v DumpType /t REG_DWORD /d 2 /f
reg add "HKLM\SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps\firebird.exe" ^
  /v DumpCount /t REG_DWORD /d 4 /f
```
)

## 6. Замечания к методике

- Установка B (dev-копия) — нестандартная (application-режим, изолированная
  `FIREBIRD_LOCK`, security3.fdb от другой установки): краши **в ней** сами по
  себе аргументом не являются. Но установка A — штатный сервис HQbird, без
  каких-либо нестандартных настроек, и падает так же. Весь репорт построен на
  случае A; дамп — из случая B (единственный способ снять его до перезапуска
  службы).
- XNET при воспроизведении не используется (клиенты по TCP/IP 127.0.0.1).
- Firebird 4.0.8 и 5.0.5/5.0.3 на той же машине под той же нагрузкой не падают
  (многочасовые прогоны, 2026-09-29/30) — проблема наблюдается только на 3.0.

## 7. Вложения

| Файл | Что |
|---|---|
| `dumps/firebird.exe_260930_154838.dmp` | полный дамп (661 МБ), HQbird 3.0.15, AV; **в репозиторий не включён** (размер), по запросу |
| `dumps/firebird.exe_260930_141400.dmp` | полный дамп (259 МБ), plain 3.0.13, AV; не включён, по запросу |
| `evidence/crash_stack_symbolized_hq3015.txt` | символизированный стек падающего потока (по `engine12.pdb` HQbird) |
| `evidence/crash_dump_hq3015.txt` | контекст исключения, регистры, скан стека — HQbird 3.0.15 |
| `evidence/crash_dump_analysis.txt` | контекст исключения, регистры, скан стека — plain 3.0.13 |
| `evidence/hqbird_fb3_firebird.log` | firebird.log сервисной установки: 4 × `Fatal lock interface error` в окне прогона |
| `evidence/devcopy_fb3_firebird.log`, `evidence/devcopy_firebird.conf` | лог и конфиг dev-копии |
| `evidence/run_fb3_full_console.log` | консольный лог 10-минутного прогона (статус-строки, отказ-волны, финальная сводка) |
| `evidence/run_fb3_full_final_summary.txt` | итоговая статистика прогона |
| `evidence/run_fb3_full_sql_errors.log` | полный лог ошибок SQL с PSQL-стеками процедур |
| `evidence/devcopy_run1/2/4_console.log` | прогоны dev-копии с крашами (разные conn-max, tx-variants on/off) |
| `evidence/dump_run_hq_console.log` | консольный лог прогона, в котором снят HQbird-дамп |
| `evidence/server_versions.txt`, `evidence/os_info.txt` | версии серверов и ОС |

## 8. Контакты

Отчёт подготовлен IBSurgeon при воспроизведении на нагрузочном стенде
(2026-09-30). Дополнительные данные/дампы — по запросу.
