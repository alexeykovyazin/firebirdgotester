# limbocrash — Firebird crash: prepared transaction of a dead connection in a replicated database

Minimal reproducer found 2026-09-27 (plan: `FB5_LIMBO_CRASH_REPRO_PLAN.md`).

## Steps

1. A database with replication on:
   - `replication.conf`: `database = <path> { journal_directory = <dir> }`
   - `create table LIMBO_T (ID bigint, WORKER int, TS timestamp default localtimestamp);`
   - `alter database enable publication; alter database include all to publication;`
2. One connection (TCP): start a transaction, `insert into LIMBO_T (ID, WORKER) values (1, 0)`.
3. `isc_prepare_transaction` (op_prepare, single database).
4. Close the socket without detach (the client "dies").

```
limbocrash -init -host localhost:3095 -db C:\repro\R.FDB -password masterkey
limbocrash -host localhost:3095 -db C:\repro\R.FDB -password masterkey -mode die -loops 3
```

`-mode drop` (close without prepare) and `-mode commit` are the controls.

Python version (`limbocrash.py`, needs `pip install firebird-driver` and fbclient):
a child process connects, inserts, prepares and ends with `os._exit()`; the
parent keeps a watch connection open, so a crash is seen even when a service
manager restarts the server at once.

```
set ISC_PASSWORD=masterkey
python limbocrash.py --dsn localhost/3095:C:\repro\R.FDB --init
python limbocrash.py --dsn localhost/3095:C:\repro\R.FDB --mode die --loops 3
```

Checked 2026-09-27 against HQbird 5.0.5.1881 (Super, Windows) with the
Firebird 3.0 fbclient: commit 3/3 and drop 3/3 alive, die 3/3 CRASHED (the
service manager logged three unexpected ends). In Classic the watch connection
has its own process and survives; there check `firebird.log`.

## Result

| Build | Mode | OS | Result (3 tries each) |
|---|---|---|---|
| Firebird 5.0.4.1812 (vanilla) | Super | Linux (Ubuntu 24.04) | SIGSEGV 3/3 |
| Firebird 5.0.4.1812 (vanilla) | Super | Windows 11 | error logged 3/3, no crash; later the engine never shuts down ("engine shutdown is in progress with some database(s) attached") |
| Firebird 5.0.4.1812 (vanilla) | SuperClassic | Windows 11 | 0xC0000005 3/3 |
| Firebird 5.0.4.1812 (vanilla) | Classic | Windows 11 | 0xC0000005 in the worker process 3/3 |
| Firebird 4.0.7.3271 (vanilla) | Super | Windows 11 | 0xC0000005 3/3 |
| HQbird 5.0.5.1881 | Super | Windows 11 | 0xC0000005 3/3 |

Every time `firebird.log` first gets:

```
Failure working with transactions list: transaction to unlink is missing in the attachment
```

No error without replication, without a change in the transaction, without
the prepare, or with an ordinary commit. The isolation level does not matter.

## Cause (from the stacks)

Two stacks, the same path: `stack-5.0.4-linux.txt` (vanilla 5.0.4, Linux core,
gdb with the kit's debug symbols) and `stack-hqbird-5.0.5.1881-windows.txt`
(HQbird 5.0.5.1881, Windows full dump from procdump at the first-chance
ACCESS_VIOLATION, cdb with the kit's pdb; the fault address is 0x2a0 inside
`RtlEnterCriticalSection`, the Linux one 0x2a8 inside `pthread_mutex_lock`:
the mutex of a null memory pool).

1. The server purges the dead attachment: `purge_transactions` (jrd.cpp:8259)
   calls `TRA_release_transaction` for the prepared (limbo) transaction.
2. `TRA_release_transaction` unlinks it from the attachment, then calls
   `tra_replicator->dispose()` (tra.cpp:1330); the pointer is cleared only after
   `dispose()` returns.
3. `Replication::Replicator::Transaction` holds `RefPtr<ITransaction>` to the
   same transaction — here the last reference. Its destructor releases it:
   `JTransaction::release` → `freeEngineData` → **`TRA_release_transaction` for
   the same `jrd_tra` again**.
4. The nested call does not find the transaction in the attachment's list (the
   log line; `tra_abort` only logs in release builds) and calls
   `tra_replicator->dispose()` on the object that is already being destroyed:
   double destruction, `MemPool::releaseBlock(this=0x0)`, SIGSEGV /
   ACCESS_VIOLATION. Where the process survives (Super on Windows), the
   attachment is never freed and the engine cannot shut down.

In a normal commit or rollback the client's own `JTransaction` reference keeps
the interface alive, so the replicator's reference is not the last one.

## Impact

- The whole server process ends (Super, SuperClassic): every database of the
  instance is dropped; a service restarts it.
- A commit in flight at that moment can reach the replica through the journal
  and be lost on the master (seen on the g15 lab: +7 rows on the replica).
- Any client that prepares a transaction (two-phase commit, e.g. a transaction
  coordinator) and loses its connection before commit triggers it.

## Draft for the Firebird tracker (not published)

**Title:** Server crash (double release of the replicated transaction) when a
connection with a prepared transaction dies in a replicated database

**Body:** the steps, the table and the cause above; attach `stack-5.0.4-linux.txt`
and `main.go`. Suggested direction: in `TRA_release_transaction` take
`tra_replicator` into a local and clear the field before `dispose()`, and make
sure the replicator's `RefPtr<ITransaction>` cannot be the reference that frees
the engine transaction during its own release.
