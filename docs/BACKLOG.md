# Backlog — planned verification & improvements

Items parked for a future session. Each carries enough context to be picked
up cold; nothing here blocks releases.

## B1. Classify Firebird 3.0 "cannot update erased record" as expected under load

**Status:** proposed (2026-09-28, from the full lab matrix run), not implemented.
**Priority:** minor — statistics hygiene only; no crash or corruption risk
observed, runs always completed with invariants OK.

### Symptom

On Firebird 3.0 (lab port 3053, `OLTPEMUL_FB3.FDB`), every extended-load
oltp-emul run of ~10 minutes produces roughly **18 `kind=unexpected` errors**
with the message:

```
cannot update erased record
```

They come from stored-procedure units (SP_* business units) under NOWAIT
races: between the statements inside the procedure, another worker erases
and commits the row; on FB 3.0 the error propagates out of the procedure to
the client instead of being absorbed as an update conflict the way it is on
FB 4.0 / 5.0. The unit fails, its transaction is rolled back, the worker
continues — but the event lands in the *unexpected* bucket and in the
per-unit *Failure* column instead of *Conflict*, polluting the statistics.

### Hypothesis

FB 3.0 aborts the surrounding PSQL block with the raw GDSCODE when the row
an earlier statement read is erased mid-procedure; FB 4.0+ re-checks and
surfaces the same logical event as the familiar update-conflict class
("record from transaction … is not visible / no wait"). If true, the FB3
message is the same expected NOWAIT race, just a different engine code path.

### Verification plan (when picked up)

1. **Reproduce**: FB3/3053, extended preset, `--emul-invariant-every 30`,
   ~15 min run; collect `*_sql_errors.log` + ops.log. Confirm the message and
   that each occurrence coincides with NOWAIT conflict activity in the same
   window (correlate timestamps with concurrent conflict outcomes).
2. **Prove harmlessness**: for a sample of occurrences verify the ops-log
   pairing shows the unit's transaction ROLLED BACK; run
   `srv_make_invnt_saldo` / `srv_make_money_saldo` after the run — stock and
   money must balance (no state corruption).
3. **Version gate**: grep all historical FB4/FB5 run logs for the message —
   expected zero hits. If FB3-only, decide whether to gate the
   classification on the server version or classify by message alone
   (message-only is simpler and harmless if other versions never emit it).
4. **Implement**: extend the emul error classification (string fallback in
   `emul/run.go` `classifyUnitError`, next to the "stuck in limbo" →
   conflict case) to map "cannot update erased record" → `OutcomeConflict`.
   Add a unit test with the exact message text as a golden entry.
5. **Re-verify**: FB3 extended run — unexpected ≈ 0 (only genuinely new
   errors), conflicts absorb the count, invariants OK. FB4/FB5 runs
   unchanged. Sanity check: a real failure (e.g. dropped server mid-run)
   must still classify as unexpected — the mapping is message-exact, not a
   wildcard.

### Acceptance

FB3 extended run with `unexpected = 0` and invariants OK; unit-test green;
no change in FB4/FB5 classification behavior.
