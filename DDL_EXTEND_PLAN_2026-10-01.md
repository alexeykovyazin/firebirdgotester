# План: DDL-нагрузка фазы 2 — изменение типов полей, alter процедур с изменением сигнатур (2026-10-01)

Расширение `--plusddl` (emul/plusddl.go): сегодня sidecar гоняет ADD/ALTER TYPE/DROP
собственных колонок на рабочих таблицах и RECREATE/DROP TABLE с триггерами.
План добавляет второй эшелон DDL-давления: **конверсии типов полей** и
**alter процедур с изменением списка параметров** под живым конкурентным
нагрузком — то есть гонки DDL↔DML, перекомпиляция prepared statements и
инвалидация зависимостей.

Принцип тот же, что у всего extended-набора: ожидаемые под нагрузкой
ошибки метаданных — часть теста, классифицируются отдельно и не портят
статистику и инварианты.

---

## 1. База (уже есть, не трогаем)

- `StartDDLSidecar` — тикер EverySec, каждая DDL в собственной транзакции;
  deliberate rollback (P8) с проверкой через rdb$relation_fields;
- колонки только с `ColPrefix` (TST_), модель колонок в памяти + re-discover
  после рестарта (plusddl.go:73);
- RECREATE TABLE TST_<n> + 6 триггеров + grouped DML (plusddl.go:168);
- счётчики `ExtendedCounters` (DDLRounds, ColumnsAdded/Altered/Dropped,
  TablesCreated/Dropped), opslog-записи DDL;
- execDDL молча глотает все ошибки как «expected under load».

## 2. Фаза 1 — конверсии типов полей (victim-таблица)

**Объект**: dedicated `EL_DDL_VICTIM` в aux-схеме (provision), НЕ рабочая
таблица юнитов — чтобы в фазе 1 не пачкать per-unit статистику.
Колонки: `ID INTEGER PK`, `VAL VARCHAR(64)`, `NUM NUMERIC(12,2)`,
`CNT INTEGER`.

**Циклы ALTER COLUMN TYPE** (по одному шагу за раунд, чередуем):

| Семейство | Вперёд | Назад | Ожидаемые отказы |
|---|---|---|---|
| VARCHAR длина | VAL VARCHAR(64)→(128)→(256) | ↓ на шаг, перед понижением `UPDATE VAL=NULL` на длинных строках | «new length shorter…» при игнорировании подготовки (проверяем expected-fail) |
| Целые | CNT INTEGER→BIGINT | BIGINT→INTEGER после `UPDATE CNT=0` | выход за диапазон при понижении без подготовки |
| NUMERIC | NUMERIC(12,2)→(15,4) | →(12,2) | precision ниже |
| NULL-биты | VAL DROP NOT NULL / SET NOT NULL | ↑ | SET NOT NULL при NULL-данных |

- перед понижением — нормализующий UPDATE (иначе шаг классифицируется как
  expected-fail, что само по себе полезно: он проверяет отказ-путь);
- **модель состояния**: sidecar хранит текущий тип каждой колонки; при старте —
  discover из rdb$relation_fields + rdb$fields (type/length/scale/nullable);
- конкурентный DML-пинг (в том же тикере, между alter'ами): короткий
  INSERT/SELECT/UPDATE по victim — ловит «object … is in use», «column
  unknown», «lock conflict» во время конверсии;
- счётчики: `TypeAlterOK`, `TypeAlterExpectedFail` (+ текущий тип — в сводку).

**Конфиг**: `PlusDDL.AlterTypes bool`, флаг `--ddl-types` (default off;
требует `--plusddl`). Тикер общий с plusddl (каждый N-й раунд — type-шаг).

## 3. Фаза 2 — alter процедур с изменением сигнатуры

**Объект**: `SP_ELT_VICTIM` в aux-схеме, две сигнатуры, попеременно:

```sql
-- A: 1 вход, 1 выход
CREATE PROCEDURE SP_ELT_VICTIM(IN1 INTEGER) RETURNS (OUT1 INTEGER) AS ...
-- B: 2 входа, 2 выхода
ALTER PROCEDURE SP_ELT_VICTIM(IN1 INTEGER, IN2 VARCHAR(32))
  RETURNS (OUT1 INTEGER, OUT2 VARCHAR(32)) AS ...
```

Тело пишет строку в `EL_DDL_LOG` (вызов «настоящий», не пустышка).

- **DDL-сторона**: ALTER PROCEDURE раз в N раундов (чередование A/B).
- **Caller-сторона**: отдельная горутина с интервалом
  `--ddl-proc-call-every` (default 2 c): вызывает victim динамически;
  актуальную сигнатуру кэширует из `rdb$procedure_parameters`, при ошибке
  «count of … does not match» — refresh кэша и повтор на следующем тике.
- **Ожидаемые ошибки** (классифицируются, не unexpected):
  - «count of column list or variable list does not match» (гонка
    сигнатуры);
  - «object SP_ELT_VICTIM is in use» (ALTER при живых prepared);
  - «lock conflict on no wait transaction»;
  - «procedure unknown» / «token unknown» при самых жёстких гонках.
- Счётчики: `ProcAlterOK`, `ProcAlterExpectedFail`, `ProcCallOK`,
  `ProcCallRaceErr` (разбивка по 4 видам выше).
- FB4+ (отдельная фаза): `ALTER PACKAGE BODY` — packages есть только там;
  feature-detect по rdb$packages, в FB3 шаг пропускается.

**Конфиг**: `PlusDDL.AlterProcs bool` (`--ddl-procs`, default off),
`PlusDDL.ProcCallEverySec int` (`--ddl-proc-call-every`, default 2).
Discover при рестарте: текущая сигнатура из rdb$procedure_parameters.

## 4. Фаза 3 (опция, off) — каскад на живые юниты

Alter процедуры, которые бизнес-юниты вызывают реально (обёртка вокруг
`sp_get_test_time_dts`). Юниты начнут ловить mismatch/in-use → нужно
расширить классификацию юнит-ошибок (`classifyUnitError`): эти тексты →
`OutcomeRejected` (transient), не Failure. Флаг `--ddl-live-cascade`,
по умолчанию off; приёмка: доля rejected < 5%, инварианты живы.

## 5. Классификация DDL-ошибок (общий helper)

`ddlExpectedError(err)` в plusddl.go: по подстрокам —
`is in use`, `lock conflict`, `no wait`, `unsuccessful metadata update`,
`does not match`, `column unknown`, `procedure unknown`, `too short`,
`unknown column`. Используется sidecar'ами для пометки expected (в opslog
маркер `expected=1`) и, с фазы 3, в юнит-классификации. Никаких молчаливых
проглатываний: каждая ошибка видна в логе и в счётчиках.

## 6. Сводка/счётчики

- ExtendedCounters += TypeAlterOK/TypeAlterExpectedFail, ProcAlterOK/
  ProcAlterExpectedFail, ProcCallOK/ProcCallRaceErr;
- ExtendedJSON + SnapshotJSON — те же поля; блок «DDL» в финальной сводке
  (summary package подхватит автоматически через ExtendedJSON);
- opslog: DDL-записи получают `kind=type|procsig|caller`.

## 7. Верификация

- Юнит-тесты: генераторы DDL-строк (golden по типам/сигнатурам), discover
  victim-типа и сигнатуры на fake-rows, ddlExpectedError-классификация
  (golden по текстам FB3/4/5).
- Матрица прогонов (15 мин каждый): FB3/FB4/FB5, extended + `--plusddl
  --ddl-types --ddl-procs`. Приёмка:
  - инварианты 0 disabled; TPS не в ноль после каждого раунда DDL;
  - ни одна DDL-race ошибка не попала в unexpected;
  - рестарт sidecar — discover подхватил тип/сигнатуру без дублей;
  - firebird.log без новых видов ошибок; **FB3 отдельно**: известный
    LM-крэш (foundfirebirdbugs/2026-09-30-…) может срабатывать чаще под
    DDL — фиксируем как продолжение того репорта, не как новую проблему.
- dellg15: DDL-флаги НЕ включать (репликация; вне скоупа).

## 8. Порядок работ

1. Фаза 1 (типы) + счётчики + discover + сводка.
2. Фаза 2 (сигнатуры) + caller + классификация + тесты.
3. Матрица FB3/4/5 + докручивание expected-списков по фактическим текстам.
4. Доки: README (флаги), EXTENDED_LOAD_PLAN.md — пометить Phase 5.
5. (Опции, по запросу) Фаза 3 бизнес-каскад; домены (ALTER DOMAIN);
   индексы (CREATE/DROP INDEX на victim); пакеты FB4+.
