# Recommended firebirdsql fork patch: LevelSnapshotNoWait

The oltp-emul invariant self-checks (`SRV_MAKE_INVNT_SALDO`,
`SRV_MAKE_MONEY_SALDO`) require a transaction started with
`isc_tpb_nowait` **and** snapshot isolation. `IBSurgeon/firebirdsql-go`
currently hard-wires each TPB in `tpbForIsolationLevel` (transaction.go)
with no snapshot+nowait combination:

- FB 4.0 / 5.0: READ COMMITTED NOWAIT passes the SP checks (engine reports
  a lock-timeout value for NOWAIT), so invariant checks work.
- FB 3.0: the same transaction is rejected by
  `SP_CHECK_NOWAIT_OR_TIMEOUT` (EX_NOWAIT_OR_TIMEOUT_REQUIRED), so fb-loadgen
  disables invariant checks on that engine (see `startEmulSidecars` in
  main.go).

Recommended addition to the fork (mirrors LevelReadCommittedNoWait = 1000):

```go
// consts.go
const (
    // LevelSnapshotNoWait starts a SNAPSHOT (concurrency) transaction with
    // NOWAIT lock resolution.
    LevelSnapshotNoWait = 1001
)

// driver_go18.go, in BeginTx, next to the LevelReadCommittedNoWait case:
case LevelSnapshotNoWait:
    return fc.begin(ISOLATION_LEVEL_SNAPSHOT_NOWAIT)

// consts.go isolation list
ISOLATION_LEVEL_SNAPSHOT_NOWAIT

// transaction.go, tpbForIsolationLevel:
case ISOLATION_LEVEL_SNAPSHOT_NOWAIT:
    return []byte{
        byte(isc_tpb_version3),
        byte(isc_tpb_write),
        byte(isc_tpb_nowait),
        byte(isc_tpb_concurrency),
    }, nil
```

Then in fb-loadgen switch `emul.SnapshotTxOptions()` to
`firebirdsql.LevelSnapshotNoWait`.
