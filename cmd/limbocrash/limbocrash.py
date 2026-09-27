#!/usr/bin/env python3
"""Firebird crash: a prepared transaction of a dead connection in a replicated database.

Python version of limbocrash (see README.md in this folder). Needs the
firebird-driver package (pip install firebird-driver) and fbclient.

Each loop starts a child process that connects, inserts one row, prepares the
transaction (isc_prepare_transaction) and then ends at once with os._exit():
the operating system closes the socket, no detach is sent. The parent then
checks with a fresh connection whether the server still answers.

The database must be listed in the server's replication.conf, e.g.

    database = C:\\repro\\R.FDB
    {
        journal_directory = C:\\repro\\R.journal
    }

    python limbocrash.py --dsn localhost/3050:C:\\repro\\R.FDB --init
    python limbocrash.py --dsn localhost/3050:C:\\repro\\R.FDB --loops 3

--mode drop (end without prepare) and --mode commit are the controls: they do
not crash the server. The password comes from --password or ISC_PASSWORD.
"""
import argparse
import os
import subprocess
import sys
import time

from firebird.driver import connect, driver_config


def open_con(a):
    if a.fbclient:
        driver_config.fb_client_library.value = a.fbclient
    return connect(a.dsn, user=a.user, password=a.password)


def init(a):
    with open_con(a) as con:
        cur = con.cursor()
        cur.execute("select count(*) from rdb$relations where rdb$relation_name = 'LIMBO_T'")
        if not cur.fetchone()[0]:
            cur.execute("create table LIMBO_T (ID bigint, WORKER int, TS timestamp default localtimestamp)")
            con.commit()
        cur.execute("alter database enable publication")
        cur.execute("alter database include all to publication")
        con.commit()
    print("init: table LIMBO_T and publication ready")


def child(a):
    con = open_con(a)
    con.begin()
    con.cursor().execute("insert into LIMBO_T (ID, WORKER) values (?, 0)", (a.loop,))
    if a.mode == "die":
        # TransactionManager has no prepare() (only DistributedTransactionManager
        # has); its ITransaction does: isc_prepare_transaction2, no message.
        con.main_transaction._tra.prepare()
    elif a.mode == "commit":
        con.commit()
        con.close()
    # die: prepared, now the process dies; drop: the transaction is still active.
    # Either way no rollback and no detach reach the server.
    os._exit(0)


def ask(con):
    cur = con.cursor()
    cur.execute("select 1 from rdb$database")
    cur.fetchone()
    con.commit()


def reconnect(a, timeout=60):
    t0 = time.time()
    while True:
        try:
            return open_con(a)
        except Exception:
            if time.time() - t0 > timeout:
                raise
            time.sleep(1)


def check(a, watch):
    """A watch connection kept open across loops: if the server process ended,
    it is broken even when a service manager has already restarted the server."""
    try:
        ask(watch)
        return "alive (watch connection still works)", watch
    except Exception as e:
        msg = str(e).splitlines()[0][:100]
        return "CRASHED (watch connection lost: %s)" % msg, reconnect(a)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dsn", required=True, help="host/port:path of a database listed in replication.conf")
    p.add_argument("--user", default="SYSDBA")
    p.add_argument("--password", default=os.environ.get("ISC_PASSWORD", ""))
    p.add_argument("--fbclient", default="", help="path to fbclient library, if not found by default")
    p.add_argument("--mode", choices=["die", "drop", "commit"], default="die")
    p.add_argument("--loops", type=int, default=3)
    p.add_argument("--pause", type=float, default=3.0, help="seconds between loops")
    p.add_argument("--init", action="store_true", help="create LIMBO_T and turn the publication on, then exit")
    p.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--loop", type=int, default=0, help=argparse.SUPPRESS)
    a = p.parse_args()

    if a.child:
        child(a)
    if a.init:
        init(a)
        return 0
    env = dict(os.environ, ISC_PASSWORD=a.password)
    watch = open_con(a)
    for i in range(1, a.loops + 1):
        args = [sys.executable, os.path.abspath(__file__), "--child", "--loop", str(i),
                "--dsn", a.dsn, "--user", a.user, "--mode", a.mode]
        if a.fbclient:
            args += ["--fbclient", a.fbclient]
        r = subprocess.run(args, env=env, capture_output=True, text=True)
        err = (r.stderr.strip().splitlines() or [""])[-1][:120]
        time.sleep(1)  # the server purges the dead attachment
        state, watch = check(a, watch)
        print("%s loop=%d mode=%s child=%s%s; server %s" % (
            time.strftime("%H:%M:%S"), i, a.mode, r.returncode, " (%s)" % err if err else "", state), flush=True)
        time.sleep(a.pause)
    return 0


if __name__ == "__main__":
    sys.exit(main())
