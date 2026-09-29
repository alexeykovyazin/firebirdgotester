# План: исправление дефектов, найденных 2-часовым soak-прогоном v1.0.1

Дата: 2026-09-29. Основа: soak на ubuntu-5 (FB 4.0.8 HQbird, 2h, чистое
завершение) + локальная репродукция на Windows (FB4/3054, клин процесса,
delve-дамп). Все ссылки на строки проверены по исходникам на момент
написания. Статус: план не выполнялся.

## 0. Дефекты и доказательства

### D1. Инвариантный self-check самоотключается на FB4

- Факт: `[emul-inv] disabled: invariant check failed 3 times in a row:
  emul: invariant SRV_MAKE_INVNT_SALDO failed: exception 8
  EX_CANT_LOCK_SEMAPHORE_RECORD` — на ubuntu-5 (74-я проверка из ~120) и в
  локальной 15-минутке. До отключения — 74× "invariants OK".
- Причина (исходник): `invariantLockConflict` (emul/state.go:323-328) считает
  transient'ом только `lock conflict` / `deadlock` / `no wait`. Сообщение
  сервера `can`t lock semaphores.id=N, deferred` (отложенный конфликт
  блокировок на записи `semaphores` внутри SRV_MAKE_*_SALDO) под эти
  паттерны не подпадает → считается жёстким отказом → 3 страйка подряд →
  цикл выключен до конца прогона (`maxInvariantFailures`), финальный отчёт
  несёт `invariants=disabled`.
- Это тот же класс, который фиксировали в C4 (SnapshotTxOptions), но с
  другим текстом ошибки.

### D2. Экскурсии пула и churn на Stop

- Факт (ubuntu-5, 2h): пул 20 → 3-5 (≈10 мин) → 17 → 3-5; **124**
  `Failed to remove worker N: stop timed out`; attachment-churn 11
  подключений за 30 с при живом счётчике аттачментов ~9; worker-ID дошли до
  2373+ при максимуме 20 одновременных.
- Механика (исходник): ramp при down-walk отвязывает лишних воркеров и
  зовёт `stopWorkers` (ramp/ramp.go:540-551). `Worker.Stop` держит бюджет
  5s; воркер, сидящий в op с ожиданием блокировки 30-60s, не успевает
  ответить → stop timed out → воркер отвязан, но коннект закроется только
  когда op вернётся (до 60s позже). Пока op висит, up-walk создаёт новых
  воркеров (churn), а `len(s.workers)` (метрика `Workers: N/M`,
  ramp.GetCurrentWorkerCount) отражает bookkeeping, а не фактические
  коннекты. Добавлена env-гейт-диагностика `FB_RAMP_DEBUG=1` (ramp/ramp.go,
  по шагу walk: roll/step/walkTarget/live/min/max) — пока не закоммичена.
- Не подтверждено: исправен ли сам walkTarget (справедливый ±1) или он тоже
  проваливается вниз. Трейс Phase A отвечает точно.

### D3. Полный клин процесса (локально; на лабе не воспроизвёлся за 2h)

- Факт: 15-минутный прогон на FB4/3054 замер на ~6-й минуте, не завершился.
  delve-дамп живого процесса: все ~19 воркеров стоят в
  `firebirdsql.(*wireChannel).Read` ← `recvPackets`; 17+ утёкших
  `sql.Tx.awaitDone`; reporter и scheduler замерли на `aggMu` /
  `stateMu`; стеки воркеров в дампе обрезаны по глубине (dlv/Windows) —
  точное место вызова не видно.
- Что известно по исходникам форка: blocking-read обёрнуты
  `withCancelWatcher` (statement.go:103-122): на ctx.Done шлётся
  `op_cancel(fb_cancel_raise)`; fallback `enforceDeadline`
  (statement.go:140-147) ставит socket deadline = ctx.Deadline()+3s.
  Покрыты execute (statement.go:200-238), query (:301-…), fetch (:333-…).
  НО: `op_cancel raise` сервер обрабатывает только в точках между
  операциями — **ожидание блокировки внутри выполняемого SP не
  прерывается**; `abandonReadTimeout = 10s` (wireprotocol.go:1976) ограничивает
  только служебное чтение после cancel. Следовательно op, попавший в
  серверное ожидание без уважаемого lock_timeout, может висеть дольше
  любого клиентского таймаута → per-op timeout (10s) и Stop (5s) не работают,
  worker никогда не выходит → каскадный клин всего, что требует тех же
  локов.
- Открытый вопрос Phase A: почему сработал не `enforceDeadline`
  (socket deadline должен был разорвать read на 13-й секунде)? Гипотезы:
  (а) застрявший read — не statement-путь, а commit/rollback/сервисный
  вызов без ctx-обвязки; (б) deadline был снят `cancelAndDrain`
  (statement.go:154: `SetDeadline(time.Time{})` перед служебным чтением);
  (в) стеки дампa неполны и read на самом деле другой. Определить точно.

## Фаза A — диагностика (без изменений кода, ~0.5 дня)

Цель: закрыть открытые вопросы до правок.

1. **Полная глубина стеков**: на локальной 15-минутке (она клинится
   надёжно) — `dlv attach <pid>`, затем `goroutine <id> stack` по каждому
   воркеру в recvPackets: по-горoutine-ный `stack` часто даёт полную
   глубину там, где групповой `goroutines -t` обрезает. Записать точное
   место: какой вызов форка паркуется и есть ли над ним
   enforceDeadline/ctx.
2. **Трейс picker'а**: `FB_PICKER_DEBUG=1` на 15-минутке; сверить
   нарисованные lock_timeout (emul-safe: 2000+n мс) с фактическими
   задержками (p95=30s, max=60s в soak). Если нарисованные ≤10s, а
   фактические 30-60s — сервер (FB 4.0.8 HQbird) игнорирует TPB
   lock_timeout для части ожиданий (semaphores deferred) — отдельный
   документ для отчёта IBSurgeon/HQbird.
3. **FB_RAMP_DEBUG=1** на той же 15-минутке: сверить walkTarget vs live
   по секундам. Критерий: если walkTarget гуляет по всему [2,20], а live
   отстаёт/проваливается — дефект в ensureWorkerCount/Stop-пути; если сам
   walkTarget проваливается — дефект в walk (rng/мин-макс).
4. Зафиксировать в плане-отчёте: withCancelWatcher НЕ течёт (join
   statement.go:120); >41k goroutine-ID за 2h — это по одному watcher'у на
   statement (churn, не утечка). Утечка — `sql.Tx.awaitDone` (по одному на
   отменённую tx на занятом коннекте; разрешается сами после D3-фикса).

## Фаза B — форк: hard-drop по истечении ctx (E:\Projects_2026\firebirdsql, ветка extended-load-intents)

Цель: сделать клиентскую отмену реально работающей. Опционально (DSN-параметр),
поведение по умолчанию не меняется — другие потребители форка не пострадают.

1. **Дизайн**: новый DSN-параметр `cancel_hard_drop=true` (+ тайминг
   `cancel_hard_drop_grace=<ms>`, по умолчанию 3000). В
   `withCancelWatcher` (statement.go:103): по ctx.Done — как и сейчас
   `op_cancel(fb_cancel_raise)`; если через grace read не вернулся
   (healthy-путь: op_cancel работает и fn уже вернулся → watcher вышел по
   `stop`, grace не тикает) — **закрыть сокет** (`wp.conn.Close()` /
   wireChannel close, через sync.Once). Main-read вернёт сетевую ошибку →
   database/sql пометит коннект битым → у fb-loadgen воркера сработает
   существующий путь `isDeadConnErr → rebuildConn in place`
   (worker/worker.go:305-311). Закрытие сокета не касается несинхронизированного
   write-буфера, инвариант join-а (комментарий statement.go:89-98) не
   нарушается; двойное закрытие — через sync.Once.
2. **Покрытие**: execute/query/fetch (уже обёрнуты); проверить пути
   BeginTx/Commit/Rollback: Commit/Rollback в database/sql идут без ctx —
   оставить как есть (op короткий), но задокументировать. Service-вызовы
   (Services API) — вне скоупа этого раунда.
3. **Тесты форка**:
   - Юнит: стаб-сервер, который игнорирует op_cancel и отвечает только
     через N секунд; ctx с deadline 1s, grace 500ms → assert: ошибка
     возвратилась ≤2s, повторное использование коннекта даёт dead-conn
     ошибку, goroutine watcher не остался. Существующий приём из
     dirtyconn_test.go:178-179 (подмена abandonReadTimeout) годится и для
     grace.
   - Юнит: healthy-путь (сервер отвечает мгновенно) — поведение
     бит-в-бит как раньше, сокет НЕ закрывается.
   - Live (`*_live_test.go`, по образцу TestLiveProbeResolveLimboOutput):
     FB5/FB4 — SP с заведомо долгим ожиданием блокировки (UPDATE
     залоченной строки в WAIT-транзакции), ctx 2s, hard-drop → op вернулся
     ≤5s, сервер sees dead attachment, следующая команда на том же
     *sql.DB — новый коннект.
4. **Публикация**: коммит в `extended-load-intents`, push в
   IBSurgeon/firebirdsql-go, тег **v0.9.20-ib.2**, `go mod edit -replace …
   @v0.9.20-ib.2` + `go mod tidy`, пересборка.

## Фаза C — fb-loadgen: классификация и ramp

1. **C1. Инвариант transient** (emul/state.go:323-328): в
   `invariantLockConflict` добавить паттерн `lock semaphores` (покрывает
   `can`t lock semaphores … deferred` в обеих кодировках апострофа) —
   конфликт остаётся transient, 3-страйк не накапливается. Golden-тест на
   точную строку из soak-лога (exception 8 … can`t lock semaphores.id=1,
   deferred) + на "не-лок" ошибку (должна остаться жёсткой).
2. **C2. Ramp-диагностика уже добавлена** (`FB_RAMP_DEBUG`, ramp/ramp.go)
   — закоммитить. Правки политики Stop В ЭТОМ раунде не делать: после
   hard-drop (Фаза B) Stop-таймауты должны исчезнуть сами — воркер на
   отменённом op получает dead-conn, rebuild-ится и либо продолжает
   (down-walk отменит ctx на следующей итерации), либо выходит по ctx.
   Если трейс Phase A покажет дефект в walkTarget — отдельным пунктом по
   его результатам.
3. **C3. Метрика pool-vs-bookkeeping** (маленькое, из наблюдений D2):
   добавить в статусную строку фактическое число коннектов пула воркеров
   (`db.Stats().OpenConnections`) рядом с `Workers: N/M` — закрывает класс
   «метрика оторвалась от реальности» навсегда.

## Фаза D — тестирование и приёмка

Матрица прогонов (все — с собранным фикс-бинарником):

| # | Где | Сервер | Длительность | Конфиг | Что проверяем |
|---|-----|--------|--------------|--------|---------------|
| D1 | юнит/CI | — | — | `go test ./...`, `-race` смоук | новые golden-тесты C1, тесты форка B3 |
| D2 | Windows локально (HONOR2025) | FB4/3054 OLTPEMUL_FB4.FDB | 15 мин ×3 | как клинившаяся репродукция, `FB_RAMP_DEBUG=1 FB_PICKER_DEBUG=1` | прогон **завершается** (клин исчез), инвариант не отключается, пул ходит по [2,20] |
| D3 | **dellg15 (Windows-лаба)** | FB5 5.0.5.1880 `C:\HQbird\Firebird50`, порт 3055 | 15 мин, затем 2h | `--no-limbo` **обязательно** (репликация включена — prepare-then-die роняет сервер, см. FB5_LIMBO_CRASH_REPRO_PLAN.md) | Windows-сторона: завершение, инварианты OK до конца, Stop без таймаутов; при повторном клине — полный дамп горутин (dlv есть на dellg15, procdump установлен) |
| D4 | dellg15 | FB4/FB3 (отдельные порты, см. план краша §матрица) | 15 мин каждый | emul-safe | версионная матрица Windows, отсутствие регрессий классификации |
| D5 | ubuntu-5 (повтор soak) | FB 4.0.8 HQbird, тот же OLTPEMUL_SOAK_V101.FDB | 2h | те же параметры, что 2026-09-29 | **инварианты включены до конца** (не disabled), пул без 10-минутных экскурсий к 3-5, stop-timed-out ≤ единиц (было 124), run завершён, limbo RESOLVED-подсчёт работает |
| D6 | FB3/3053 | OLTPEMUL_FB3.FDB | 10 мин | extended | регрессий классификации нет (kinds не поехали) |

Доступ к dellg15: `dellg15.local` (mDNS резолвится с этой машины), SSH:22
отвечает; учетные данные — как в сессии краш-репродукции (там снимали
procdump 12.01). Перед прогонами подтвердить хост-ключ
(`StrictHostKeyChecking=accept-new`).

### Критерии приёмки по дефектам

- **D1 закрыт, если**: в D5 (2h) и D2 (15 мин ×3) финальный отчёт содержит
  `invariants=` OK (не disabled); golden-тест зелёный.
- **D2 закрыт, если**: в D5 stop-timed-out ≤ 5 за 2h (было 124) и live-пул
  (по `OpenConnections` из C3) ходит по всему диапазону walkTarget; трейс
  FB_RAMP_DEBUG приложен к PR-описанию.
- **D3 закрыт, если**: D2 ×3 завершились естественно; стресс-повтор
  клинившегося сценария ×5 подряд без клина; форк-тест B3 (игнорирующий
  op_cancel сервер) зелёный.
- Регресс: D4/D6 не показывают новых kind=unexpected; score classic-прогонов
  в пределах обычного разброса.

## Порядок и риск

A → C1 (+C2 коммит, C3) → D2 (локальная верификация C1) → B (форк: правка,
тесты, тег ib.2, re-pin) → D2 повторно с новым форком → D3-D6 → итоговый
отчёт. Риск фазы B локализован opt-in параметром: дефолтное поведение
драйвера не меняется; при проблемах фолбэк — убрать параметр из DSN
fb-loadgen (одно место формирования строки подключения).

Из скоупа сознательно исключены: классификация FB3 `cannot update erased
record` (docs/BACKLOG.md B1 — отдельный пункт), изменение дефолтного
cancel-поведения форка, политики Stop/retry в ramp (пересматриваются
только по результатам трейса Phase A).
