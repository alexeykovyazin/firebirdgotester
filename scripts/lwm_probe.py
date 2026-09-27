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
