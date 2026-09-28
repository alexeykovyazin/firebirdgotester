# Ревью кода fb-loadgen (firebirdgotester) — 2026-09-28

> **СТАТУС ВЫПОЛНЕНИЯ (2026-09-28, финал).** Все фазы 0–5 плана выполнены.
> Критические находки C1–C4 исправлены, мажоры M3–M5, M7–M20, M22–M24, M26–M31 и
> миноры Ф5.1–Ф5.6 закрыты. Верификация: `go build ./...`, `go vet ./...`,
> `gofmt -l` — чисто; весь тест-сьют зелёный; живые прогоны на FB5 и FB4:
> oltp-emul с `--emul-invariant-every 3` — самопроверка инвариантов «stock and
> money invariants OK» ×3 на обоих движках (раньше цикл молча отключался), FB4
> дополнительно показал корректную классификацию «stuck in limbo» как
> транзиентную ошибку с ретраем; write-heavy смоук на FB5 — 158 операций, 100%
> успех, полный цикл warmup→main→cooldown. Вне скоупа (требуют внешних
> решений): `-race` — только в CI linux-job (нет gcc на Windows); `git
> filter-repo` для ~60 МиБ истории — координированный force-push; тег на форке
> драйвера вместо псевдоверсии; перевод русских доков.
> Новое: `safego`-пакет, race smoke-тесты (ops/profile/emul/worker), тесты
> бакетов/перцентилей (worker), Firebird-джоба в CI, флаг
> `--allow-remote-unauthenticated`. **Плюс одна новая находка из полного прогона
> всех вариантов на лабе (2026-09-28/29): в классическом режиме (`--extended-load=false`)
> монитор памяти пинил единственный коннект заводского max-1 пула → цикл инвариантных
> самопроверок молча блокировался навсегда (BeginTx без таймаута, ошибки глотались).
> Фикс: `RunSidecars` сам поднимает пул до 6 (monitor+invariant+heavy+bulk+plusddl),
> первая ошибка BeginTx логируется, регресс-тест `TestRunSidecarsPoolsEnoughConnections`;
> подтверждено живыми прогонами на FB3/FB4/FB5.**

Полное ревью всех 75 Go-файлов (~22 тыс. строк) по четырём направлениям: движок нагрузки
(worker/ramp/ops/metrics/profile/db), контрольная плоскость (session/ui/schedule/config),
OLTP-эмуляция и диагностика (emul/main/lwmprobe/cmd/limbocrash/errlog/opslog/discover),
инфраструктура (git/CI/тесты/документация). Каждая находка проверена по исходнику с
указанием файла и строк. `go build ./...` и `go vet ./...` — чисто.

**Верификация 2026-09-28 (вторая итерация):** все критические находки и ключевые мажоры
сверены с исходниками построчно, включая исходники форка драйвера
(firebirdsql-go@a967ed8, transaction.go/consts.go) и эмпирику логов прогонов. В ходе
сверки скорректирован фикс C4 (нужен snapshot+nowait, а не просто SNAPSHOT — см. C4),
уточнены C1/C3, M11 и M28. План в конце обновлён соответственно.

Главный вывод: код зрелый и в многих местах дисциплинированный (lock-иерархия manager,
generation-счётчики, temp+rename, golden-тесты isql-парсера), но есть системный дефект —
**любое состояние вне собственного воркера считается однопоточным**, и несколько
механизмов **молча искажают результаты измерений**, то есть бьют по основному назначению
инструмента — достоверному измерению.

---

## КРИТИЧЕСКИЕ

### C1. Расписания interval/cron срабатывают ровно один раз за жизнь процесса
`schedule/engine.go:186` — `tick()` ставит `e.pending[s.ID] = true` перед запуском fire;
`schedule/engine.go:179` — тик пропускает всё, что в `pending`. `fire()` (строки 198–224)
обновляет `LastFire`/`NextRunAt`, но **никогда не делает `delete(e.pending, s.ID)`** —
единственная очистка сидит в ручном `TriggerNow` (engine.go:403). После первого
срабатывания расписание помечено pending навсегда. Подтверждено grep'ом: все вхождения
`pending` — 41,54,116,179,186,393,397,403; в `fire` удаления нет. Тесты не ловят,
потому что тикают один раз.
**Фикс:** `delete(e.pending, s.ID)` в `fire` под тем же lock, **включая early-return
ветку `st == nil`** (удалённое в процессе fire расписание — иначе вечная утечка в map,
сейчас `engine.go:203-205` просто возвращается). Регресс-тест должен тикать ≥2 раза.

### C2. Гонки данных: один немногопоточный `*rand.Rand` на всех воркеров (3 места)
- `ops/cache.go:23-24,42` + все `Random*` (229–372) — комментарий «(thread-safe)» ложный;
  `rand.Rand` от `rand.NewSource` не потокобезопасен. Ирония: `ops/txvariants.go:62` сам
  предупреждает «cache keeps a shared unguarded rng — do not repeat that».
- `profile/profile.go:40,58-75` — `WeightedSelector.Select()` дергается из каждого воркера
  (`worker/worker.go:312`).
- `ops/writes.go:18,26` + `emul/run.go:85-96` — та же схема в `WriteOperations` и `emul.Selector`.
Один `Cache`/`Profile` создаётся на сессию (`main.go:333`, `session/manager.go:881,951`) и
делится между всеми воркерами → порча состояния RNG, нерепродуцируемые распределения,
немедленный провал под `-race` на горячем пути.
**Фикс:** per-worker RNG (передавать `*rand.Rand`/seed в воркер) или мьютекс/atomic-обёртка.

### C3. Таймаут одной операции навсегда убивает воркер — нагрузка молча затухает
`worker/worker.go:345` — per-op `context.WithTimeout(w.ctx, w.txTimeout)`; `isCancelErr`
(650–662) считает `context.DeadlineExceeded` shutdown'ом → цикл `run()` (262–264)
получает `isCancelErr(err)==true` при `w.ctx.Err()==nil` и воркер выходит навсегда.
Дефолт `tx-timeout=10s` (config/config.go:126) — а сам инструмент **специально создаёт
конфликт локов** (write-heavy, rare-write, limbo): операция, стоящая в очереди на лок
дольше 10 секунд, — нормальный сценарий под генерируемой нагрузкой, а не ЧП. При этом
ramp-планировщик **не пересоздаёт** вышедших воркеров: `ensureWorkerCount`
(ramp/ramp.go:384-427) сверяет только количество, не здоровье — это признано в
комментариях V17 (worker.go:616-617). Каждый такой таймаут — минус воркер, TPS тихо
падает, замер искажён; в метриках путь не виден (ни RecordTransaction, ни лога).
**Фикс (двухчастный):** (1) оборачивать per-op таймаут в отдельный тип ошибки и
классифицировать как сбой операции (запись в метрики + продолжить цикл), а не как
shutdown; shutdown распознавать только по `w.ctx.Err() != nil`; (2) в
`ensureWorkerCount` — reap&replace: убирать из `s.workers` завершившиеся воркеры
(`Done()`) и досоздавать до target.

### C4. Инвариантная проверка — главный сигнал корректности — сама себя отключает
`emul/state.go:209` открывает проверочную транзакцию через `TxOptions()` (READ COMMITTED +
NOWAIT, кастомный уровень 1000 = `LevelReadCommittedNoWait` форка), хотя
`SnapshotTxOptions()` (`emul/run.go:161-166`) существует с докой «SRV_MAKE_INVNT_SALDO /
SRV_MAKE_MONEY_SALDO demand TIL = SNAPSHOT» — и **нигде не вызывается** (dead code;
второй вызов `TxOptions()` в fill.go:59 — легитимный, там снапшот не нужен). На движках,
где требование соблюдается, процедура бросает `EX_SNAPSHOT_ISOLATION_REQUIRED` /
`EX_NOWAIT_OR_TIMEOUT_REQUIRED`, и `invariantFailure` (state.go:291-296) глушит цикл
навсегда с сообщением «driver limitation on this engine».

**Важно (найдено при сверке с форком драйвера):** сообщение об ошибке говорит
«snapshot**+nowait** transaction» — сервер требует ОБА свойства. А `SnapshotTxOptions()`
возвращает `sql.LevelRepeatableRead` (=4), который в форке декодируется как SNAPSHOT
(concurrency) **+ WAIT** (transaction.go: `case 4 → ISOLATION_LEVEL_REPEATABLE_READ`,
waitMode по умолчанию `isc_tpb_wait`). Значит, наивная замена `TxOptions()` →
`SnapshotTxOptions()` чинит только половину — `EX_NOWAIT_OR_TIMEOUT_REQUIRED`, вероятно,
останется. В форке есть точный уровень: **`LevelSnapshotNoWait = 1250` (snapshot,
nowait)** (consts.go:673) — нужен именно он.
**Фикс:** `SnapshotTxOptions()` → `&sql.TxOptions{Isolation: sql.IsolationLevel(1250)}`
(с комментарием-привязкой к `firebirdsql.LevelSnapshotNoWait`, по образцу
`NoWaitIsolation=1000` в run.go:154), вызвать его в state.go:209, обновить доку функции.
Дополнительно: под NOWAIT конфликт локов с SRV_MAKE_*_SALDO станет штатным событием —
классифицировать lock-conflict/deadlock в инвариант-цикле как транзиентный (не
наращивать `failStreak`, иначе 3 конфликта подряд снова отключат цикл, уже с другой
формулировкой).

---

## МАЖОРНЫЕ

### Движок нагрузки
- **M1. Гонка в metrics.MetricsCollector** — агрегатные поля `totalOps/lastReportTime/
  maxWorkers/...` (`metrics/collector.go:26-40`) пишутся горутиной `collect()` и
  читаются/пишутся `GetReport()` (239-311, включая `mc.lastTotalOps = mc.totalOps` на
  311) и `Reset()` (396-406) без синхронизации; три мьютекса покрывают только карты.
- **M2. Гонка в ramp Scheduler getters** — `GetTargetWorkerCount/GetCurrentPhase/
  GetElapsedTime/GetPhaseProgress` (ramp.go:490-546) читают поля, которые пишет run-loop
  (244-357); геттеры дергаются из session/UI (`session/manager.go:1731-1734`, `main.go:259`).
- **M3. Утечка пула соединений: `rebuildConn` vs `Stop`** — worker.go:618-636 открывает
  новый `*sql.DB` без проверки `closed`; если Stop успел — cleanup пропускает закрытие
  (`stopClosed`), свежий пул с живым сокетом утекает.
- **M4. Брошенный `*sql.Tx` при неудаче 2PC-enlist** — worker.go:461-464 ставит
  `completed = true` до `completeTransaction`; `EnlistTwoPhase` (ops/txvariants.go:414-430)
  — единственная ошибка завершения, где ни Commit, ни Rollback не отправлен → tx и
  соединение (при `MaxOpenConns(1)`, db/connect.go:41) потеряны, серверная транзакция
  держит локи до таймаута, что триггерит C3.
- **M5. p95/p99 = 0ms для всех операций ≥ 2 с** — `metrics/collector.go:367-391`
  (`getBucketUpperBound`) знает бакеты только 0–8, воркеры пишут 0–13
  (`GetLatencyBucketMs`, worker.go:222); правильная таблица есть в worker.go:1063-1094,
  локальная копия не обновлялась.
- **M6. Percentile-математика врёт на малых выборках** — worker.go:1025-1048,
  collector.go:328-351: `cumulative >= target` при цели 0 (total<2/<20/<100) матчится на
  первом пустом бакете → одна транзакция 30 с = «p50=5ms». Нужно `>` или округление цели вверх.
- **M7. Cooldown начинается с burst до max** — ramp.go:349-357,372-377: цель считается от
  `max`, а не от текущего числа воркеров → мгновенный всплеск соединений в начале
  «спада нагрузки».
- **M8. Метрики-«min/avg/max» — это p50/среднее-из-трёх-перцентилей/p99** —
  collector.go:136-138; в отчётах (PerformanceSummary) вводит в заблуждение.
- **M9. Double-count и перезапись бакетов** — `RecordTransaction` (collector.go:183-201)
  инкрементит локальные карты, которые каждую секунду затираются `updateFromWorkerMetrics`
  (141-165).
- **M10. `Scheduler.Stop()` до `Start()` — вечный deadlock; двойной `Start()` — panic
  (второй `close(runDone)`)** — ramp.go:173-184,504-508.
- **M11. `ORDER BY RAND() ROWS 1` сортирует всю таблицу на каждую запись** —
  ops/writes.go:102-316 (5 мест); на растущей SALES становится доминирующей стоимостью и
  искажает замер. Уточнение по эмпирике: во всех логах прогонов на FB3/4/5 **нет ни одной
  ошибки RAND** («rand»-совпадения в smoke-логе — это имя view `v_random_find_clo_ord`),
  т.е. функция на тестируемых версиях работает; устаревший артефакт — комментарий
  ops_test.go:500 «Firebird doesn't have RAND()» (убрать, чтобы не вводил в заблуждение).

### Контрольная плоскость
- **M12. RunHistory возвращает shallow copies, `Sessions` мутируется на месте** —
  `Get`/`List` (session/run.go:292-296,330) возвращают `*r`; `UpdateSession` (357-375)
  пишет в тот же backing array; HTTP-хендлеры маршают копии вне лока → гонка под
  `-race` при живом UI.
- **M13. Параллельные писатели `fb-loadgen.ui.json` делят один `.tmp`** —
  config/persist.go:244-248 (`tmp := path + ".tmp"`); `SaveUISettings` без мьютекса,
  писатели конкурентны (PUT /api/config, PATCH /api/sessions, POST /api/discover,
  async provision) и держат лишь `RLock` (manager.go:317-324) → возможна порча файла
  настроек; ошибка rename отбрасывается.
- **M14. Все ошибки персистентности runs/schedules молча проглатываются** —
  session/run.go:204-221, schedule/engine.go:144-158: голые `return` на marshal/MkdirAll/
  WriteFile/Rename. Диск полон / AV держит файл (Windows) → история и расписания тихо
  перестают сохраняться, ноль в логах.
- **M15. Фоновые горутины без recover** — ui/server.go:403, ui/handlers_emul.go:214-247,
  session/manager.go:1040 (watchCompletion), schedule/engine.go:192,400. Паника в любой
  из них убивает весь процесс со всеми активными прогонами.
- **M16. emul.Extended() — process-lifetime singleton, никогда не сбрасывается** —
  emul/extended.go:261-268; UI/сессионный движок гоняет много сессий в одном процессе,
  в живом состоянии и финальном отчёте каждой новой сессии — кумулятивные счётчики всех
  предыдущих прогонов. (Для сравнения: worker-метрики сбрасываются, worker.go:1112-1116.)

### OLTP-эмуляция / extended load
- **M17. plusddl: локальная модель колонок и счётчики обновляются вне зависимости от
  успеха DDL** — emul/plusddl.go:129-136 (ADD), 152-158 (DROP), 174-184 (таблицы).
  Несостоявшийся DROP удаляет колонку из `wt.columns` → колонка навсегда «забыта», TST_-
  колонки текут; несостоявшийся CREATE всё равно инкрементит TablesCreated. Счётчики
  extended load в отчётах недостоверны.
- **M18. plusddl выполняет DDL с context.Background()** — plusddl.go:278-281; стейтмент,
  заблокированный на metadata-локе (ровно то, что sidecar провоцирует), вечно висит,
  `case <-ctx.Done()` не сработает → остановка прогона/сессии подвисает.
- **M19. heavyops: 3 из 4 соединений пула закреплены навсегда** — heavyops.go:128-149
  (`SetMaxOpenConns(4)`, heavy+bulk+monitor по одному пину) → инвариантам и plusddl
  остаётся одно соединение на двоих; долгий SRV_MAKE_*_SALDO блокирует plusddl. Плюс
  глобально перетирает емкость пула, заданную вызывающим; тихое отключение sidecar при
  таймауте `pool.Conn` (135-138) без лога/счётчика.
- **M20. limbo: разрешение засчитывается без верификации, если re-list упал** —
  emul/limbo.go:91-93: `verr != nil` → проверка пропущена → `LimboResolved.Add(1)`,
  вопреки собственному комментарию. Также тихое отключение, если `NewMaintenanceManager`
  упал (53-56) — без лога и счётчика.
- **M21. fill.go: `done` считает юниты, а не документы** — emul/fill.go:63-96; ранний
  выход `done >= initDocs` без финальной сверки doc_list → база может быть недозаполнена
  при «fill complete».
- **M22. Bootstrap-провал глушит heavy/bulk, но оставляет PlusDDL и limbo recovery** —
  main.go:317-327 (и session/emul.go:75+): `groupedDML` plusddl использует
  `NEXT VALUE FOR EL_BULK_SEQ` (plusddl.go:234) — раунды вечно падают молча.

### CLI / диагностика
- **M23. Неизвестная подкоманда молча запускает полный бенчмарк** — main.go:33-49:
  диспетчеризуются только `provision`/`lwmprobe`; `fb-loadgen provisionn` (опечатка) уходит
  в дефолтный прогон против localhost/3050. Плюс `runUI` делает `os.Exit(0)` (212),
  пропуская `defer engine.Stop()` (161).
- **M24. scripts/lwm_probe.py: висячие ctypes-буферы** — строки 146-152:
  `ctypes.cast(buf, c_void_p)` не удерживает `buf`, переменная перебивается в цикле →
  7 из 8 буферов собираемы до `isc_dsql_fetch` → UB/мусорные показания. Плюс: статус
  `isc_commit_transaction` не проверяется (164), `--rate <= 0` → busy-loop (250-251).

### Инфраструктура
- **M25. ~60 МиБ мусора в git-истории** — EMPLOYEE.FDB ×6 (≈42 МБ), fb-loadgen.exe ×8
  (≈76 МБ несжато), pptx/pdf: `git count-objects -vH` = 60.12 MiB loose; удалены в
  927144c, но каждый новый клон платит за историю. Лечение: `git filter-repo` + force-push
  (координированно).
- **M26. В CI нет Firebird, а интеграционный тест — вакуумный PASS** — ci.yml без
  service container; `integration_test.go:38-43` при недоступной БД **логирует и делает
  `return`** (не fail, не skip) → DB-путь (db/, emul/, ops/) никогда не тестируется, CI
  зелёный впустую.
- **M27. 21 файл не проходит `gofmt -l`; в CI нет gofmt/staticcheck/golangci-lint** —
  config/config.go, emul/extended.go, session/manager.go, worker/worker.go и др.

### Безопасность (приемлемо только для строго localhost, что кодом не обеспечивается)
Дефолт проверен: `--ui-addr` = `127.0.0.1:9000` (config.go:131), т.е. из коробки слушаем
только localhost; риск включается, как только адрес переопределён. Для read-эндпоинтов
уже есть `--ui-auth-all` (config.go:139) — защита ниже должна опираться на те же флаги.
- **M28. Все мутирующие эндпоинты без аутентификации по умолчанию** — ui/server.go:167-179,
  `--ui-token ""` (config.go:132); при `--ui-addr` на ненакальном интерфейсе — удалённое
  безымянное управление, включая запись пароля на диск и SSRF через вебхуки
  (schedule/notify.go:90-125 — произвольный URL без валидации, из POST /api/schedules).
- **M29. CSRF: JSON читается без проверки Content-Type** — server.go:253,349,372,
  handlers_emul.go:157,331; cross-site `text/plain` POST из любой открытой вкладки рулит
  контрольной плоскостью на 127.0.0.1:9000.
- **M30. Пароли открытым текстом** — `fb-loadgen.ui.json` хранит `pass` (persist.go:44,
  дефолт `masterkey`:60); 0600 на Windows игнорируется; пароль печатается в консоль
  (main.go:341,434 → config.go:254-257).
- **M31. `discoverDir` override противоречит задокументированному allowlist** —
  api/openapi.yaml:975-977 обещает jail; manager.go:341-350 присваивает корень напрямую,
  без проверки вмещения. `POST /api/discover {"discoverDir": "C:\\"}` перенацеливает discovery.

---

## МИНОРЫ (сводно)

- Метрики/reporter: `SetReportInterval` не влияет на живой тикер (reporter.go:58,400-405);
  ошибки записи в файлы игнорируются; `GetTPSInterval` может уйти в минус при Reset
  (worker.go:1000-1010).
- ramp: `SpikeManager` геттеры без локов (ramp.go:614-649); `SpikeProfile` читает
  spike-конфиг без мьютекса (spike.go:276-358); per-op пересборка WeightedSelector в
  spike (spike.go:126-180).
- ops: `ClassifyError(nil)` = «expected» (errors.go:38-41); error-string matching по
 _common_ подстрокам («paid», «cust_no») маскирует реальные отказы (errors.go:55-84);
  `err == sql.ErrNoRows` вместо `errors.Is` (writes.go:111 и ещё 4); Sprintf-SQL в
  connect.go:76-83 и DSN без экранирования в txvariants.go:420-427.
- emul: `classifyUnitError` матч «ex_» ловит `RDB$INDEX_xx` → реальные отказы считаются
  бизнес-reject (run.go:229-237); идентификаторы конкатенацией без валидации
  (run.go:176, invariant.go:24); коммит инвариант-цикла падает молча (state.go:230-232);
  isqlscript глотает `SET TRANSACTION` и принимает мусорный терминатор (isqlscript.go:39,150-160).
- CLI/пробники: lwmprobe `-rate <= 0` → panic (lwmprobe.go:281), запросы без ctx (156-157),
  `ReadAt` err игнорируется (396); limbocrash: непроверенные `-iso`/`-resolve`, DSN без
  экранирования (cmd/limbocrash/main.go:42-88); provision глотает первую ошибку Open
  (provision.go:102-111); createdb: пароль в argv, путь без экранирования (createdb.go:64,80).
- opslog/errlog: PREPARE считается терминальным → prepared-then-abandoned 2PC не
  репортится (opslog.go:632); потеря буфера при short write (errlog.go:48-55); сбой
  ротации убивает логгер молча (opslog.go:503-506).
- discover: ошибки обхода молча = «пусто» (discover.go:60-63).
- UI/session: `emulJobs` map никогда не чистится (handlers_emul.go:139-142); мёртвый код —
  `ui.Server.ListenAndServe` в обход CORS (server.go:204-207), `runStore` (run.go:105-108),
  `provisionJob.ctx`; дубли `asInt`/`randomHex`/`normalizeAddr` (manager.go:619,
  handlers_schedule.go:287, run.go:521, notify.go:15, server.go:209, main.go:215).
- OpenAPI: задокументированные 404 фактически 400 по всем {id}-эндпоинтам
  (openapi.yaml:174-528 vs server.go:415-487); невалидный `paths.EmulState` (67-107);
  дубль ключа 404 (391-401); `{jobId}` ссылается на параметр `file` (380-384); в enum
  профиля нет `oltp-emul`; PATCH-дока противоречит коду (валид → 400, сессия остаётся
  Idle с невалидным конфигом, manager.go:599-616).
- Git: `*.dmp` не в .gitignore — 3 минидампа (~10 МБ) и 3 новых stack-*.txt лежат
  untracked в cmd/limbocrash (в одном `git add -A` от коммита).
- CI: actions/checkout@v4 и setup-go@v5 устарели (node20 deprecation с 2026-09-16);
  push-триггер только main (ветка extended-load без CI); нет workflow_dispatch/concurrency.
- Тесты: ноль тестов в `metrics/`, `profile/`, `cmd/limbocrash`; `ui/` — 1 тест-файл на
  5 исходников; `TestOutputFormats` — вечный skip.
- Доки: IMPROVEMENTS.md (выполнен 2026-08-29) vs IMPROVEMENTS_PLAN.md (2026-09-19) —
  путаница имён; PRESENTATION_PLAN.md говорит «~16 слайдов» (фактически 23); 8 из 12
  корневых .md — исторические планы; 2 дока полностью на русском среди английских.
- go.mod: replace на форк по pseudo-version (a967ed8) — без тега; апстрим-фиксы не
  протекают. Рекомендация: тегнуть форк (v0.9.17-ibsurgeon.1).
- Горячий путь: `os.Getenv` на каждую записанную транзакцию (worker.go:786-788,
  txvariants.go:123-126); worker.go:531-532 мёртвый `dur := func()...`.

Проверено и чисто: красaction паролей в GET /api/config (persist.go:112-125), path
traversal в download-хендлерах закрыт тройной проверкой (reports.go:93-107),
lock-иерархия `m.mu → s.mu` консистентна, monitor.go без утечек, bulkRound без паник,
path-containment discover корректен, CI уже гоняет `-race` и строит 3 ОС, релиз имеет
5-таргетную кросс-сборку с sha256, секретов в репо нет, README-флаги сверены с main.go.

---

# ПЛАН УЛУЧШЕНИЙ

## Фаза 0 — быстрые победы (≈ полдня)
1. `schedule/engine.go`: `delete(e.pending, s.ID)` в `fire` под lock — в основной ветке
   **и в early-return `st == nil`**; регресс-тест, который тикает ≥2 раза и ждет двух
   срабатываний (C1).
2. `emul/run.go` + `emul/state.go:209`: `SnapshotTxOptions()` → `IsolationLevel(1250)`
   (`LevelSnapshotNoWait` форка, snapshot+nowait), вызов её в инвариант-цикле, обновление
   доки; lock-conflict в инвариант-цикле считать транзиентным (без наращивания
   `failStreak`) (C4).
3. `scripts/lwm_probe.py:146-152`: держать буферы в списке; проверять статус коммита;
   валидация `--rate > 0` (M24).
4. `main.go`: ошибка на неизвестную подкоманду (M23, первая половина).
5. `.gitignore`: `*.dmp`; решить судьбу `cmd/limbocrash/stack-*.txt` (коммитить или игнор).
6. `gofmt -w .` (21 файл — проверено) + gofmt-check в CI.
7. Убрать stale-комментарий ops_test.go:500 про «нет RAND()» (M11).

## Фаза 1 — корректность движка нагрузки (2–3 дня)
1. RNG: один `*rand.Rand` делится между всеми воркерами (C2). Варианты в порядке
   предпочтения: (а) per-worker `*rand.Rand` — протянуть в `Worker` и передавать в
   методы `Cache.Random*`/`WeightedSelector`/`WriteOperations` (сигнатуры op-замыканий
   уже получают `w.cache`; rng — следующее поле); заодно открывает дорогу флагу `-seed`
   для воспроизводимых прогонов — полезно тестировщику; (б) минимальный вариант —
   мьютекс-обёртка над rng; (в) дешёвая замена — топ-уровневые функции `math/rand/v2`
   (потокобезопасны из коробки, go 1.24 уже в go.mod). Выбрать (а), если не мешает —
   она же решает per-op пересборку селектора в spike.go.
2. Политика таймаутов (C3, двухчастная): отдельный тип ошибки для per-op
   `DeadlineExceeded` (не shutdown; записывать в метрики как сбой операции); shutdown —
   только по `w.ctx.Err() != nil`; reap&replace завершившихся воркеров в
   `ensureWorkerCount` (health-check `Done()`, досоздание до target).
3. Утечки: `rebuildConn` перепроверяет `closed` под тем же локом и закрывает `fresh`,
   если Stop успел (M3); при ошибке `EnlistTwoPhase` — явный `tx.Rollback()` (M4).
4. Метрики: **единый источник таблицы бакетов** — collector.go:367-391 знает только 0–8,
   worker.go:1063-1094 — правильную 0–13; вынести в одну функцию одного пакета, вторую
   удалить (M5); `>` вместо `>=` в перцентилях в обоих местах (worker.go:1037-1045,
   collector.go:328-351) (M6); атомики/мьютекс на агрегаты collector + корректные Reset
   (M1); убрать double-count/overwrite локальных карт (M9); честные min/avg/max вместо
   p50/среднего-перцентилей/p99 (M8).
5. ramp: cooldown от текущего числа воркеров, а не от `max` (M7); защита Stop-до-Start
   (закрыть `runDone` при создании или флаг started) и повторного Start (M10); заменить
   `ORDER BY RAND() ROWS 1` на выборку по диапазону первичного ключа/индексу (M11).
6. **Новый тест-гарант: race smoke test** — горутины N≥8 делят один `ops.Cache`,
   `WeightedSelector` и `WriteOperations`, гоняют `Random*`/`Select`/`Insert*` под
   `-race`. Без него CI-`-race` останется зелёным просто потому, что sharing не
   покрыт тестами — C2 не будет доказанно закрыт.

## Фаза 2 — контрольная плоскость и emul (2 дня)
1. `RunHistory.Get/List`: глубокая копия `Sessions` (M12).
2. `SaveUISettings`: package-мьютекс + уникальное имя tmp (`*.tmp-PID` или в той же
   дире через `os.CreateTemp`) + логировать ошибки rename (M13).
3. Персистентность runs/schedules: не глотать ошибки — логировать и surfaced в статус
   (M14); валидация Version при загрузке.
4. `recover()` во всех фоновых горутинах (старт сессии, provision, watchCompletion, fire)
   (M15).
5. `emul.Extended()`: сброс счётчиков на старте сессии (M16); plusddl — менять модель/
   счётчики только при успехе execDDL (возврат error) (M17); `ExecContext(ctx, ...)` (M18);
   bootstrap-провал глушит plusddl+limbo тоже (M22); heavyops — не ужимать пул до 4,
   логировать тихое отключение sidecar (M19); limbo — считать только верифицированные
   разрешения (M20); fill — финальная сверка doc_list (M21).

## Фаза 3 — безопасность localhost-инструмента (1 день)
1. Явный opt-in на безымянный режим: если `--ui-token` пуст и адрес не 127.0.0.1/::1 —
   отказываться стартовать без `--allow-remote-unauthenticated` (дефолт-банд уже
   localhost:9000 — проверено; проверять именно хост-часть разобранного адреса) (M28).
2. Проверка `Content-Type: application/json` — **только при непустом теле**: API.md и UI
   шлют POST без тела (start/stop/pause/resume; curl в API.md:32 без заголовка, UI
   app.js:41 ставит заголовок всегда) — жёсткая проверка на все мутирующие эндпоинты
   сломает задокументированное использование (M29). Плюс `Origin`-чек для браузерных
   запросов; `http.MaxBytesReader` на все декодеры.
3. `crypto/subtle.ConstantTimeCompare` для токена; не печатать пароль в консоль
   (main.go:341,434) (M30); allowlist-проверка `discoverDir` по документации (M31);
   ограничение схем webhook (`http/https`) + опция запрета приватных CIDR.

## Фаза 4 — инфраструктура и тесты (2–3 дня)
1. CI: отдельный **linux-only** job с Firebird в service container (docker-сервисы на
   windows/macos-раннерах недоступны, основная матрица остаётся без БД); в идеале
   матрица контейнеров 3/4/5 — инструмент целится во все три. `TestBasicExecution`:
   env-guard `FIREBIRD_TEST_DSN` — не задан → `t.Skip`, задан, но БД недоступна → **fail**
   (сейчас — вакуумный PASS, integration_test.go:38-43) (M26).
2. golangci-lint (staticcheck, errcheck, govet, gofmt) в CI; bump actions (checkout@v5+,
   setup-go@v6); workflow_dispatch + concurrency; push на все ветки.
3. Тесты на `metrics/` (бакеты 0–13, перцентили на малых выборках, Reset) и `profile/`
   (веса, spike) — именно там сейчас ноль; плюс race smoke test из Фазы 1.
4. OpenAPI: 404 вместо 400 по {id}-эндпоинтам (или правка спеки), убрать `paths.EmulState`,
   дубль 404, `{jobId}`-параметр, `oltp-emul` в enum; прогон через strict-валидатор.
5. Опционально и координированно: `git filter-repo` — вычистить ~60 МиБ истории (M25);
   до тех пор не коммитить дампы.
6. Доки: перенести IMPROVEMENTS.md / OLTP_*_PLAN.md / EXTENDED_LOAD_PLAN.md /
   PRESENTATION_PLAN.md в `docs/history/`, поправить «16 слайдов», унифицировать язык.

## Фаза 5 — полировка (по мере надобности)
- Мелкие фиксы из «миноров»: errcheck-волны (проглатываемые ошибки записи/ротации
  логов), дедуп `asInt`/`randomHex`/`normalizeAddr`, чистка мёртвого кода,
  `errors.Is` для `sql.ErrNoRows`, word-boundary матчинг в `classifyUnitError`,
  тег на форке драйвера вместо pseudo-version.
- Прогнать весь пакет под `-race` локально на живом Firebird (после Фазы 1) — сейчас
  CI-`-race` зелёный только потому, что гонки не покрыты тестами.

## Порядок и критерий приёмки
Фазы 0–1 обязательны до следующего публичного прогона измерений: C1–C4 напрямую
искажают результаты (молча затухающая нагрузка, недостоверные счётчики/перцентили,
одноразовые расписания, отключённый инвариант-контроль). Критерии приёмки Фазы 1:
(1) race smoke test из п.6 зелёный под `-race`; (2) интеграционный smoke на живой БД
(write-heavy с намеренным конфликтом локов) показывает стабильное число активных
воркеров при `tx-timeout=10s` — воркеры не «испаряются»; (3) инвариант-статус в
`/api/emul/state` доходит до «ok» на БД с требованием snapshot+nowait, не «disabled».
Проверка C4 по исходникам форка выполнена (transaction.go/consts.go), но финальная
валидация — только на живом OLTPEMUL-базе с этими процедурами.
