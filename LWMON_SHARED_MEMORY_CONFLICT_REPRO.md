# HQbird LightWeight Monitoring: конфликт версии разделяемой памяти между инстансами

*Ошибка `LWMonMemory: inconsistent shared memory type/version`, её воспроизведение на чистой машине и разбор скрипта-репродьюсера*

---

## Предложение

На машине с несколькими инстансами Firebird от **разных сборок HQbird** подсистема LightWeight Monitoring (LWMon) хранит свои данные в **единой именованной области разделяемой памяти, глобальной для всей машины** (а не для конкретного инстанса). Первая сборка, создавшая область, записывает в неё формат своей структуры данных. Инстансы сборки с другой версией формата при **каждом присоединении (attachment)** не могут инициализировать мониторинг и пишут в `firebird.log`:

```
LightWeight Monitoring: Cannot initialize the shared memory region
LWMonMemory: inconsistent shared memory type/version; found 160/2:2, expected 160/2:3
```

Присоединение при этом **завершается успешно** — клиент ошибки не видит.

В документе: симптомы, диагностика на работающем сервере (раздел 2), детерминированное воспроизведение на чистой машине — сначала двумя вызовами isql, затем Python-скриптом на чистом ctypes (раздел 3), построчный разбор этого скрипта (раздел 4) и меры устранения (раздел 5).

---

## 1. Симптомы

### 1.1. Серверная часть

В `firebird.log` инстанса, чей ожидаемый формат LWMon не совпадает с форматом, в котором создана область, появляются записи, по одной на каждое присоединение:

```
LightWeight Monitoring: Cannot initialize the shared memory region
LWMonMemory: inconsistent shared memory type/version; found 160/2:2, expected 160/2:3
```

На сервере с регулярной активностью (фоновые задания, мониторинг, приложения с частыми подключениями) лог растёт на сотни-тысячи строк в час. Первая компонента (`160` — идентификатор типа структуры) у конфликтующих сборок совпадает; конфликтует **версия формата** — вторая компонента (`2:2` против `2:3`).

Само присоединение при этом завершается успешно: `isql`, приложения и скрипты работают как обычно, ошибка LWMon видна только в логе сервера.

### 1.2. Зеркальная ошибка во втором инстансе

На той же машине второй инстанс другой сборки пишет **зеркальную** ошибку в свой лог:

```
LWMonMemory: inconsistent shared memory type/version; found 160/2:3, expected 160/2:2
```

То есть каждый инстанс «ожидает» свой формат и «находит» чужой. Это диагностический признак именно конфликта двух сборок, а не порчи одной установки.

---

## 2. Диагностика на работающем сервере

1. **Найти все инстансы** (службы Windows и прослушиваемые порты):

   ```bat
   sc query state= all | findstr /i firebird
   netstat -ano | findstr LISTENING | findstr /r ":30[0-9][0-9]"
   ```

   По службе (`ImagePath` в `HKLM\SYSTEM\CurrentControlSet\Services\<служба>`) определить каталог установки каждого инстанса.

2. **Проверить логи каждого инстанса** на пары `found/expected`:

   ```bash
   grep -c LWMonMemory "C:/HQbird/<каталог>/firebird.log"
   grep LWMonMemory "C:/HQbird/<каталог>/firebird.log" | tail -2
   ```

   Зеркальные пары (`found 160/2:2, expected 160/2:3` в одном логе и `found 160/2:3, expected 160/2:2` в другом) однозначно указывают на конфликт двух сборок.

3. **Быстрая go/no-go проверка скриптом** (раздел 3.4): 15 секунд против каждого инстанса; счётчик новых `LWMonMemory`-строк в выводе — точная мера конфликта.

---

## 3. Воспроизведение на чистой машине

### 3.1. Требуемое окружение

- Windows x64.
- **Два разных релиза HQbird для Firebird 5.0** (сборки с разными версиями формата LWMon — проверить практически: если репродьюсер на шаге 4 не даёт ошибок, у выбранных сборок формат совпадает и нужен другой релиз).
- Установка в два **раздельных каталога** как два именованных инстанса с разными портами, например:

  | Инстанс | Каталог (пример) | Порт |
  |---|---|---|
  | A — более старый релиз | `C:\HQbird\FirebirdA` | 3055 |
  | B — более новый релиз | `C:\HQbird\FirebirdB` | 3065 |

  Порт задаётся в `firebird.conf` каждого каталога: `RemoteServicePort = 3055` / `3065`.
- Python 3.8+ (только стандартная библиотека) — для скрипта-репродьюсера; для isql-минимума (3.3) не нужен.
- Права администратора — для установки и запуска служб.

### 3.2. Подготовка

1. Установить обе сборки, запустить обе службы.
2. Создать тестовую базу через инстанс A (каталог `C:/tmp` должен существовать):

   ```bat
   echo CREATE DATABASE 'localhost/3055:C:/tmp/lwm_repro.fdb' USER SYSDBA PASSWORD 'masterkey' PAGE_SIZE 4096; | C:\HQbird\FirebirdA\isql.exe -quiet
   ```

3. **Зафиксировать формат области за инстансом A**: выполнить одно подключение к A. Первая успешная инициализация LWMon создаст область в формате сборки A:

   ```bat
   echo QUIT; | C:\HQbird\FirebirdA\isql.exe -user SYSDBA -pass masterkey -quiet localhost/3055:C:/tmp/lwm_repro.fdb
   ```

### 3.3. Воспроизведение (isql-минимум)

1. Подключиться к инстансу **B** — той сборке, чей формат не совпадает с созданным:

   ```bat
   echo QUIT; | C:\HQbird\FirebirdB\isql.exe -user SYSDBA -pass masterkey -quiet localhost/3065:C:/tmp/lwm_repro.fdb
   ```

   Подключение пройдёт успешно (код возврата 0).

2. Посмотреть лог инстанса B:

   ```bat
   findstr /c:"LWMonMemory" C:\HQbird\FirebirdB\firebird.log
   ```

   **Ожидание:** две новые строки —

   ```
   LightWeight Monitoring: Cannot initialize the shared memory region
   LWMonMemory: inconsistent shared memory type/version; found 160/2:X, expected 160/2:Y
   ```

3. **Контроль**: то же подключение к инстансу A — в его логе новые строки не появляются (формат области родной для A).

4. **Зеркальный опыт** (опционально): остановить обе службы, запустить сначала B и подключиться к нему (область создастся в формате B), затем запустить A и подключиться к A — теперь ошибку пишет лог A с зеркальной парой `found/expected`.

### 3.4. Воспроизведение скриптом (измеримая форма)

isql-минимум доказывает факт. Скрипт `lwm_probe.py` (раздел 4) делает то же самое циклически и **измеряет**: число циклов, успехи/ошибки клиента, число новых LWMon-строк в логе за время прогона.

```bat
python lwm_probe.py --dsn localhost/3065:C:/tmp/lwm_repro.fdb ^
    --dll C:/HQbird/FirebirdB/fbclient.dll --action select ^
    --rate 5 --duration 15 --fblog C:/HQbird/FirebirdB/firebird.log
```

**Ожидаемый вывод** (эталон прогонки, 15 секунд при 5 циклах/с):

```
lwm_probe.py: target=localhost/3065:C:/tmp/lwm_repro.fdb action=select rate=5.0/s duration=15.0s dll=C:/HQbird/FirebirdB/fbclient.dll

=== lwm_probe.py summary: localhost/3065:C:/tmp/lwm_repro.fdb action=select
cycles=75 ok=75 (100.0%) reset=0 other=0
new lines in C:/HQbird/FirebirdB/firebird.log during run: LWMonMemory=74
```

Считываемое: **75 клиентских циклов — 100% успеха** при **74 новых LWMon-строках на сервере** — по одной на каждое присоединение (первая могла попасть в окно наблюдения не полностью). Ошибка воспроизведена.

**Негативные контроли** (все они должны давать `LWMonMemory=0`):

- тот же скрипт против инстанса A (родной формат области);
- любой инстанс на машине с единственной сборкой HQbird;
- обычный Firebird без HQbird.

Параметры `--action` по интересу: `attach` — только присоединение; `select` — `RDB$DATABASE`; `mon` — `MON$ATTACHMENTS`; `monmem` — агрегат по `MON$MEMORY_USAGE` (проверка, что мониторинг-запросы сервер обрабатывает нормально даже при несостоявшейся инициализации LWMon). `--rate`/`--duration` задают интенсивность; `--fblog` включает счётчик новых строк (необязателен, если скрипт запускается не на сервере).

---

## 4. Разбор скрипта `lwm_probe.py`

Скрипт намеренно минимален: только стандартная библиотека Python, работа через **ctypes** напрямую с `fbclient.dll`, без внешних зависимостей и без установки клиентского ПО. Каждый цикл — одно свежее сетевое присоединение и максимум один однострочный SELECT. Ни таблиц, ни вставок, ни прав администратора — скрипт ничего не меняет ни на сервере, ни в области (он только читает её при инициализации, в отличие от создающей сборки).

### 4.1. Константы ABI из заголовков Firebird

```python
ISC_STATUS = ctypes.c_ssize_t          # intptr_t (8 байт на win64)
ISC_STATUS_ARRAY = ISC_STATUS * 20

SQL_LONG, SQL_INT64 = 496, 580
```

Из `include/firebird/impl/types_pub.h` набора разработчика: `typedef intptr_t ISC_STATUS;` и вектор статуса длиной 20 элементов. На 64-битной Windows `intptr_t` — это **8 байт**, а не 4; это первая из двух ловушек, на которых ломаются ctypes-обёртывания Firebird (см. 4.3). Коды типов колонок `SQL_LONG` (4 байта) и `SQL_INT64` (8 байт) — из `sqlda_pub.h`; агрегаты (`COUNT`, `MAX`) сервер часто возвращает как `INT64`, даже если аргумент был `INTEGER`.

Набор доступных запросов:

```python
QUERIES = {
    "select": ("SELECT 1 FROM RDB$DATABASE", 1),
    "mon":    ("SELECT COUNT(*) FROM MON$ATTACHMENTS", 1),
    "monmem": ("select coalesce(max(case when mon$stat_group = 0 ... from mon$memory_usage", 4),
}
```

Каждому действию сопоставлен текст запроса и число колонок результата (для `monmem` — четыре: уровни database / attachment / transaction / statement).

### 4.2. Структуры XSQLVAR / XSQLDA

Точное зеркало C-определений из `include/firebird/impl/sqlda_pub.h`:

```python
class XSQLVAR(ctypes.Structure):
    _fields_ = [
        ("sqltype", ctypes.c_short), ("sqlscale", ctypes.c_short),
        ("sqlsubtype", ctypes.c_short), ("sqllen", ctypes.c_short),
        ("sqldata", ctypes.c_void_p), ("sqlind", ctypes.POINTER(ctypes.c_short)),
        ("sqlname_length", ctypes.c_short), ("sqlname", ctypes.c_char * 32),
        ("relname_length", ctypes.c_short), ("relname", ctypes.c_char * 32),
        ("ownname_length", ctypes.c_short),  ("ownname", ctypes.c_char * 32),
        ("aliasname_length", ctypes.c_short), ("aliasname", ctypes.c_char * 32),
    ]

class XSQLDA(ctypes.Structure):
    _fields_ = [
        ("version", ctypes.c_short), ("sqldaid", ctypes.c_char * 8),
        ("sqldabc", ctypes.c_int), ("sqln", ctypes.c_short), ("sqld", ctypes.c_short),
        ("sqlvar", XSQLVAR * 8),
    ]
```

Выравнивание естественное, у ctypes и C совпадает, поэтому `ctypes.Structure` ложится на бинарный макет один в один. Семантика полей, используемых скриптом:

- `sqln` — сколько описателей колонок **выделено** (скрипт всегда аллоцирует 8); `sqld` — сколько колонок **вернул** сервер после `prepare`;
- `sqldata` — указатель на буфер значения; `sqlind` — NULL-индикатор (`0` = значение NULL). В запросах скрипта NULL невозможен (константы и `COALESCE`), индикатор выделяется, но не проверяется.

### 4.3. Две ловушки ctypes

**Ловушка 1 — restype по умолчанию.** Функции `isc_*` на самом деле *возвращают* указатель на вектор статуса. ctypes без явного `restype` считает возвращаемое значение `c_int` (4 байта) и обрезает старшие байты 64-битного адреса — вместо указателя получается мусор. Симптом: каждая успешная функция «возвращает не-ноль». Поэтому всем функциям явно назначается полный тип возврата:

```python
for name in ("isc_attach_database", "isc_detach_database", "isc_start_transaction",
             "isc_commit_transaction", "isc_rollback_transaction",
             "isc_dsql_allocate_statement", "isc_dsql_prepare", "isc_dsql_execute",
             "isc_dsql_fetch", "isc_dsql_free_statement"):
    fn = getattr(d, name)
    fn.restype = ISC_STATUS          # функции возвращают указатель на вектор статуса
```

**Ловушка 2 — проверка статуса.** Начиная с Firebird 3 **успешный** вызов оставляет в векторе пару `{1, 0}`: первый элемент — `arg_gds` (1), второй — код ошибки (**0** = ошибки нет). Наивная проверка `if status[0]:` срабатывает на *каждом* успешном вызове (воспроизведённый симптом — «unknown ISC error 0» на 100% успешных циклах). Правильная проверка:

```python
def fb_error(status):
    """Firebird >=3 success leaves {1, 0} (arg_gds with code 0) in the vector;
    a real error is {1, <code>...}. So test status[1], not status[0]."""
    return status[0] != 0 and status[1] != 0
```

Исключение — `isc_dsql_fetch`: она возвращает результат **напрямую** (`0` — строка получена, `100` — конец потока), без соглашения `{1, 0}`.

### 4.4. Обёртка над fbclient.dll

```python
class FbClient:
    def __init__(self, dll_path):
        self.dll = ctypes.CDLL(dll_path)
        ...
        d.isc_attach_database.argtypes = [P(ISC_STATUS), ctypes.c_short, ctypes.c_char_p,
                                          P(ctypes.c_void_p), ctypes.c_short, ctypes.c_char_p]
        ...
```

Каждой функции задаются `argtypes`: вектор статуса передаётся как указатель на `ISC_STATUS_ARRAY`, дескрипторы БД/транзакции/оператора — как `c_void_p`, передаваемые `ctypes.byref` (в Firebird это выходные handle-параметры). `--dll` позволяет выбрать библиотеку конкретной установки; для сетевого присоединения подойдёт и любой другой недавний клиент — инициализация LWMon выполняется **сервером** при каждом присоединении, поэтому воспроизведение работает и с удалённой машины (без `--fblog`, если нет доступа к файлу лога).

### 4.5. Database Parameter Block (DPB)

```python
def make_dpb(user, password):
    return bytes([isc_dpb_version1, isc_dpb_user_name, len(user), *user.encode(),
                  isc_dpb_password, len(password), *password.encode()])
```

DPB — плоский буфер байтов: сначала `isc_dpb_version1` (1), затем триплеты «код-параметра, длина-cstring, значение» (`isc_dpb_user_name = 28`, `isc_dpb_password = 29`). Пароль передаётся открытым текстом — стандарт для DPB; скрипт предназначен для контролируемой среды (тестовые SYSDBA/masterkey).

### 4.6. Присоединение и отсоединение

```python
def attach(self, dsn, dpb):
    status = ISC_STATUS_ARRAY()
    db = ctypes.c_void_p()
    self.dll.isc_attach_database(status, len(dsn), dsn.encode(), ctypes.byref(db),
                                 len(dpb), dpb)
    return status, db
```

Именно `isc_attach_database` и является точкой воспроизведения: на сервере в его ходе выполняется инициализация LWMon, при несовпадении версии области пишется лог-строка. Отсоединение — `isc_detach_database(dst, ctypes.byref(db))` со своим вектором статуса: ошибка отсоединения учитывается, только если самой ошибки присоединения/запроса не было.

Главный цикл классифицирует исход каждого цикла:

```python
def is_reset(msg):
    m = msg.lower()
    return any(s in m for s in ("forcibly closed", "wsarecv", "wsasend", "broken pipe",
                                "unable to complete network", "connection was aborted",
                                "read tcp", "write tcp"))
```

`ok` / `reset` (транспортный разрыв) / `other` — счётчики, по которым считается сводка. В эталонном воспроизведении оба счётчика ошибок обязаны остаться нулями.

### 4.7. Выполнение запроса: `run_query` пошагово

```python
status = ISC_STATUS_ARRAY()
tr = ctypes.c_void_p()
fbc.dll.isc_start_transaction(status, ctypes.byref(tr), 1, ctypes.byref(db), 0, None)
if fb_error(status):
    raise RuntimeError(fbc.error_text(status))
```

Старт транзакции: `1` — число дескрипторов БД, `0, None` — длина TPB и сам TPB, то есть транзакция **по умолчанию** (для однострочного чтения семантика изоляции не важна — скрипт не создаёт ни конфликтов, ни блокировок).

```python
stmt = ctypes.c_void_p()
fbc.dll.isc_dsql_allocate_statement(status, ctypes.byref(db), ctypes.byref(stmt))
```

Выделение оператора в контексте БД.

```python
out = XSQLDA()
out.version, out.sqln = 1, 8
for i in range(8):
    buf = ctypes.create_string_buffer(64)
    ind = ctypes.c_short(0)
    out.sqlvar[i].sqltype = SQL_LONG
    out.sqlvar[i].sqldata = ctypes.cast(buf, ctypes.c_void_p)
    out.sqlvar[i].sqlind = ctypes.pointer(ind)
```

Выходная SQLDA: `version = SQLDA_VERSION1 (1)`, восемь заранее выделенных описателей — каждому по 64-байтному буферу значения и NULL-индикатору. Размер 64 покрывает и 4-байтовые `INTEGER`, и 8-байтовые `INT64`.

```python
fbc.dll.isc_dsql_prepare(status, ctypes.byref(tr), ctypes.byref(stmt),
                         len(stmt_text), stmt_text.encode(), 3, ctypes.byref(out))
```

Подготовка: диалект `3`; после подготовки сервер заполняет `out.sqld` (фактическое число колонок) и уточняет описатели.

```python
fbc.dll.isc_dsql_execute(status, ctypes.byref(tr), ctypes.byref(stmt), 1, ctypes.byref(out))
```

Выполнение с той же выходной SQLDA; `1` — версия DA (`SQLDA_VERSION1`).

```python
rc = fbc.dll.isc_dsql_fetch(status, ctypes.byref(stmt), 1, ctypes.byref(out))
if rc not in (0, 100):          # 0 = строка, 100 = конец потока
    raise RuntimeError(fbc.error_text(status))
```

Выборка единственной строки; `100` — нормальный конец для запросов без строк.

```python
fbc.dll.isc_dsql_free_statement(status, ctypes.byref(stmt), 2)   # DSQL_drop
fbc.dll.isc_commit_transaction(status, ctypes.byref(tr))
```

Освобождение оператора (`DSQL_drop = 2`) и коммит — цикл не оставляет после себя ни открытых транзакций, ни подготовленных операторов.

Декодирование результата:

```python
row = []
for i in range(out.sqld):
    v = out.sqlvar[i]
    raw = ctypes.string_at(v.sqldata, 8)
    row.append(int.from_bytes(raw[:4 if v.sqltype == SQL_LONG else 8], "little"))
return row
```

Число колонок берётся из `out.sqld`; ширина — по фактическому `sqltype`, порядок байтов little-endian. Значения скрипт не публикует (они не нужны для статистики), декодирование служит гарантией, что сервер вернул данные целиком.

### 4.8. Расшифровка ошибок: `fb_interpret`

```python
def error_text(self, status):
    s = ctypes.cast(status, ctypes.POINTER(ISC_STATUS))
    buf = ctypes.create_string_buffer(512)
    self.dll.fb_interpret(buf, 512, ctypes.byref(s))
    return buf.value.decode("utf-8", "replace")
```

`fb_interpret` читает вектор статуса элемент за элементом, записывает человекочитаемое сообщение в буфер и **продвигает** переданный указатель (поэтому он передаётся по ссылке). Сообщения скрипта одноэлементные, одного вызова достаточно.

### 4.9. Вотчер firebird.log

```python
def watch_log(path, counter, stop):
    offset = os.path.getsize(path) if os.path.exists(path) else 0
    while not stop.is_set():
        ...
        size = os.path.getsize(path)
        if size < offset:      # файл усечён/ротирован — читаем заново с нуля
            offset = 0
        if size > offset:
            with open(path, "rb") as f:
                f.seek(offset)
                chunk = f.read(size - offset)
            offset = size
            counter["lwm"] += chunk.count(b"LWMonMemory")
        stop.wait(0.5)
```

Фоновый поток-демон раз в полсекунды дочитывает прирост лога. Базовая позиция = **текущий размер файла на старте**: считаются только строки, появившиеся *во время* прогона (начальное смещение с нуля завысило бы счётчик на всю историю файла — это исправленная ошибка первой версии). Считается маркер `LWMonMemory` — отказ инициализации LWMon, то есть сама воспроизводимая ошибка.

### 4.10. Планировщик циклов

```python
deadline = time.monotonic() + args.duration
interval = 1.0 / args.rate if args.rate > 0 else 0
next_at = time.monotonic()
while time.monotonic() < deadline:
    ...цикл...
    next_at += interval
    time.sleep(max(0.0, next_at - time.monotonic()))
```

Заданный темп выдерживается по **монотонным** часам (не зависящим от перевода системного времени) с накопительным планированием `next_at`: медленный цикл не сдвигает график, а наверстывается. `KeyboardInterrupt` корректно завершает прогон с промежуточной сводкой.

---

## 5. Устранение

По убыванию радикальности:

1. **Выровнять сборки HQbird** на машине — оставить/обновить инстансы до одной версии. Устраняет причину полностью.
2. **Управлять владельцем области**, если две сборки должны сосуществовать: область живёт, пока ею пользуются; когда **все** инстансы машины останавливаются, она освобождается, и следующий создавший её инстанс записывает формат своей сборки. Поэтому после полного перезапуска порядок запуска определяет, чей формат получит область. Требует прав администратора; решение тактическое — порядок придётся соблюдать после каждого полного перезапуска.
3. **Отключить LightWeight Monitoring** на инстансах, где функциональность не используется (настройка HQbird соответствующего инстанса).
4. **Наблюдение**: скрипт из раздела 3.4 удобно применять как регрессионную проверку после любых манипуляций со службами — 15 секунд, счётчик `LWMonMemory` в выводе должен быть нулевым на каждом инстансе.

---

## 6. Приложение: полный листинг `lwm_probe.py`

```python
#!/usr/bin/env python3
"""Minimal reproducer of the HQbird LightWeight Monitoring (LWMon) shared
memory version conflict, pure Python via ctypes on fbclient.dll (no external
dependencies).

A loop of fresh wire attachments, each optionally followed by one single-row
SELECT (RDB$DATABASE or a MON$ view). No tables, no inserts, no transactions
beyond the implicit read.

Background: on a Windows server with two different HQbird builds installed as
separate instances, the machine-global LWMon shared-memory region holds the
struct format of whichever build created it. The other build then fails the
per-attachment LWMon initialization and logs

    LWMonMemory: inconsistent shared memory type/version; found 160/2:2, expected 160/2:3
    LightWeight Monitoring: Cannot initialize the shared memory region

for essentially every attachment. The attachment itself still succeeds —
the client sees no error. This probe reproduces and measures that
per-attachment LWMon failure deterministically: one new log line per attach
cycle, zero client-visible failures.

Example:
  python lwm_probe.py --dsn localhost/3055:C:/tmp/lwm_repro.fdb ^
      --dll C:/HQbird/Firebird50/fbclient.dll --action select --rate 5 ^
      --duration 30 --fblog C:/HQbird/Firebird50/firebird.log
"""

import argparse
import ctypes
import os
import threading
import time

ISC_STATUS = ctypes.c_ssize_t          # intptr_t (8 bytes on win64)
ISC_STATUS_ARRAY = ISC_STATUS * 20

isc_dpb_version1 = 1
isc_dpb_user_name = 28
isc_dpb_password = 29

SQL_LONG, SQL_INT64 = 496, 580

QUERIES = {
    "select": ("SELECT 1 FROM RDB$DATABASE", 1),
    "mon": ("SELECT COUNT(*) FROM MON$ATTACHMENTS", 1),
    "monmem": ("""select
                  coalesce(max(case when mon$stat_group = 0 then mon$max_memory_used end), 0),
                  coalesce(max(case when mon$stat_group = 1 then mon$max_memory_used end), 0),
                  coalesce(max(case when mon$stat_group = 2 then mon$max_memory_used end), 0),
                  coalesce(max(case when mon$stat_group = 3 then mon$max_memory_used end), 0)
                from mon$memory_usage""", 4),
}


class XSQLVAR(ctypes.Structure):
    _fields_ = [
        ("sqltype", ctypes.c_short), ("sqlscale", ctypes.c_short),
        ("sqlsubtype", ctypes.c_short), ("sqllen", ctypes.c_short),
        ("sqldata", ctypes.c_void_p), ("sqlind", ctypes.POINTER(ctypes.c_short)),
        ("sqlname_length", ctypes.c_short), ("sqlname", ctypes.c_char * 32),
        ("relname_length", ctypes.c_short), ("relname", ctypes.c_char * 32),
        ("ownname_length", ctypes.c_short), ("ownname", ctypes.c_char * 32),
        ("aliasname_length", ctypes.c_short), ("aliasname", ctypes.c_char * 32),
    ]


class XSQLDA(ctypes.Structure):
    _fields_ = [
        ("version", ctypes.c_short), ("sqldaid", ctypes.c_char * 8),
        ("sqldabc", ctypes.c_int), ("sqln", ctypes.c_short), ("sqld", ctypes.c_short),
        ("sqlvar", XSQLVAR * 8),
    ]


class FbClient:
    def __init__(self, dll_path):
        self.dll = ctypes.CDLL(dll_path)
        P = ctypes.POINTER
        d = self.dll
        for name in ("isc_attach_database", "isc_detach_database", "isc_start_transaction",
                     "isc_commit_transaction", "isc_rollback_transaction",
                     "isc_dsql_allocate_statement", "isc_dsql_prepare", "isc_dsql_execute",
                     "isc_dsql_fetch", "isc_dsql_free_statement"):
            fn = getattr(d, name)
            fn.restype = ISC_STATUS          # these return the status vector pointer
        d.isc_attach_database.argtypes = [P(ISC_STATUS), ctypes.c_short, ctypes.c_char_p,
                                          P(ctypes.c_void_p), ctypes.c_short, ctypes.c_char_p]
        d.isc_detach_database.argtypes = [P(ISC_STATUS), P(ctypes.c_void_p)]
        d.isc_start_transaction.argtypes = [P(ISC_STATUS), P(ctypes.c_void_p), ctypes.c_short,
                                            P(ctypes.c_void_p), ctypes.c_short, ctypes.c_char_p]
        d.isc_commit_transaction.argtypes = [P(ISC_STATUS), P(ctypes.c_void_p)]
        d.isc_rollback_transaction.argtypes = [P(ISC_STATUS), P(ctypes.c_void_p)]
        d.isc_dsql_allocate_statement.argtypes = [P(ISC_STATUS), P(ctypes.c_void_p), P(ctypes.c_void_p)]
        d.isc_dsql_prepare.argtypes = [P(ISC_STATUS), P(ctypes.c_void_p), P(ctypes.c_void_p),
                                       ctypes.c_short, ctypes.c_char_p, ctypes.c_short, P(XSQLDA)]
        d.isc_dsql_execute.argtypes = [P(ISC_STATUS), P(ctypes.c_void_p), P(ctypes.c_void_p),
                                       ctypes.c_short, P(XSQLDA)]
        d.isc_dsql_fetch.restype = ISC_STATUS
        d.isc_dsql_fetch.argtypes = [P(ISC_STATUS), P(ctypes.c_void_p), ctypes.c_short, P(XSQLDA)]
        d.isc_dsql_free_statement.argtypes = [P(ISC_STATUS), P(ctypes.c_void_p), ctypes.c_short]
        d.fb_interpret.argtypes = [ctypes.c_char_p, ctypes.c_uint, P(P(ISC_STATUS))]

    def error_text(self, status):
        s = ctypes.cast(status, ctypes.POINTER(ISC_STATUS))
        buf = ctypes.create_string_buffer(512)
        self.dll.fb_interpret(buf, 512, ctypes.byref(s))
        return buf.value.decode("utf-8", "replace")

    def attach(self, dsn, dpb):
        status = ISC_STATUS_ARRAY()
        db = ctypes.c_void_p()
        self.dll.isc_attach_database(status, len(dsn), dsn.encode(), ctypes.byref(db),
                                     len(dpb), dpb)
        return status, db


def fb_error(status):
    """Firebird >=3 success leaves {1, 0} (arg_gds with code 0) in the vector;
    a real error is {1, <code>...}. So test status[1], not status[0]."""
    return status[0] != 0 and status[1] != 0


def is_reset(msg):
    m = msg.lower()
    return any(s in m for s in ("forcibly closed", "wsarecv", "wsasend", "broken pipe",
                                "unable to complete network", "connection was aborted",
                                "read tcp", "write tcp"))


def make_dpb(user, password):
    return bytes([isc_dpb_version1, isc_dpb_user_name, len(user), *user.encode(),
                  isc_dpb_password, len(password), *password.encode()])


def run_query(fbc, db, stmt_text):
    """Execute one statement returning up to 8 integer columns; returns list of ints."""
    status = ISC_STATUS_ARRAY()
    tr = ctypes.c_void_p()
    fbc.dll.isc_start_transaction(status, ctypes.byref(tr), 1, ctypes.byref(db), 0, None)
    if fb_error(status):
        raise RuntimeError(fbc.error_text(status))
    stmt = ctypes.c_void_p()
    fbc.dll.isc_dsql_allocate_statement(status, ctypes.byref(db), ctypes.byref(stmt))
    out = XSQLDA()
    out.version, out.sqln = 1, 8
    for i in range(8):
        buf = ctypes.create_string_buffer(64)
        ind = ctypes.c_short(0)
        out.sqlvar[i].sqltype = SQL_LONG
        out.sqlvar[i].sqldata = ctypes.cast(buf, ctypes.c_void_p)
        out.sqlvar[i].sqlind = ctypes.pointer(ind)
    fbc.dll.isc_dsql_prepare(status, ctypes.byref(tr), ctypes.byref(stmt),
                             len(stmt_text), stmt_text.encode(), 3, ctypes.byref(out))
    if fb_error(status):
        raise RuntimeError(fbc.error_text(status))
    fbc.dll.isc_dsql_execute(status, ctypes.byref(tr), ctypes.byref(stmt), 1, ctypes.byref(out))
    if fb_error(status):
        raise RuntimeError(fbc.error_text(status))
    rc = fbc.dll.isc_dsql_fetch(status, ctypes.byref(stmt), 1, ctypes.byref(out))
    if rc not in (0, 100):
        raise RuntimeError(fbc.error_text(status))
    fbc.dll.isc_dsql_free_statement(status, ctypes.byref(stmt), 2)  # DSQL_drop
    fbc.dll.isc_commit_transaction(status, ctypes.byref(tr))
    row = []
    for i in range(out.sqld):
        v = out.sqlvar[i]
        raw = ctypes.string_at(v.sqldata, 8)
        row.append(int.from_bytes(raw[:4 if v.sqltype == SQL_LONG else 8], "little"))
    return row


def watch_log(path, counter, stop):
    offset = os.path.getsize(path) if os.path.exists(path) else 0
    while not stop.is_set():
        try:
            size = os.path.getsize(path)
            if size < offset:
                offset = 0
            if size > offset:
                with open(path, "rb") as f:
                    f.seek(offset)
                    chunk = f.read(size - offset)
                offset = size
                counter["lwm"] += chunk.count(b"LWMonMemory")
        except OSError:
            pass
        stop.wait(0.5)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dsn", required=True, help="host/port:server-side-path")
    ap.add_argument("--dll", default=r"C:\HQbird\Firebird50\fbclient.dll")
    ap.add_argument("--user", default="SYSDBA")
    ap.add_argument("--pass", dest="password", default="masterkey")
    ap.add_argument("--action", default="select", choices=["attach", "select", "mon", "monmem"])
    ap.add_argument("--rate", type=float, default=5.0, help="cycles per second")
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--fblog", default="", help="firebird.log to watch for new LWMon lines")
    args = ap.parse_args()

    fbc = FbClient(args.dll)
    dpb = make_dpb(args.user, args.password)
    stmt_text = QUERIES.get(args.action, ("", 1))[0]

    print(f"lwm_probe.py: target={args.dsn} action={args.action} rate={args.rate}/s "
          f"duration={args.duration}s dll={args.dll}")

    counter = {"lwm": 0}
    stop = threading.Event()
    if args.fblog:
        threading.Thread(target=watch_log, args=(args.fblog, counter, stop), daemon=True).start()

    ok = reset = other = 0
    deadline = time.monotonic() + args.duration
    interval = 1.0 / args.rate if args.rate > 0 else 0
    next_at = time.monotonic()
    try:
        while time.monotonic() < deadline:
            err = None
            status, db = fbc.attach(args.dsn, dpb)
            if fb_error(status):
                err = fbc.error_text(status)
            else:
                if stmt_text:
                    try:
                        run_query(fbc, db, stmt_text)
                    except RuntimeError as e:
                        err = str(e)
                dst = ISC_STATUS_ARRAY()
                fbc.dll.isc_detach_database(dst, ctypes.byref(db))
                if fb_error(dst) and not err:
                    err = fbc.error_text(dst)
            if err:
                if is_reset(err):
                    reset += 1
                    if reset <= 3:
                        print(f"[{time.strftime('%H:%M:%S')}] reset: {err}")
                else:
                    other += 1
                    if other <= 3:
                        print(f"[{time.strftime('%H:%M:%S')}] other: {err}")
            else:
                ok += 1
            next_at += interval
            time.sleep(max(0.0, next_at - time.monotonic()))
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()

    total = ok + reset + other
    print(f"\n=== lwm_probe.py summary: {args.dsn} action={args.action}")
    print(f"cycles={total} ok={ok} ({100*ok/max(total,1):.1f}%) reset={reset} other={other}")
    if args.fblog:
        print(f"new lines in {args.fblog} during run: LWMonMemory={counter['lwm']}")


if __name__ == "__main__":
    main()
```
