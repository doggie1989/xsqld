import argparse, json, signal, time
import common as C

STOP = False


def _sig(signum, frame):
    global STOP
    STOP = True


def apply_batch(con, rows):
    """Apply a changelog batch inside the caller's transaction.

    The mirror is a current-state copy, so only the NET effect of the batch matters:
    for each pk the last operation in seq order wins. This is identical to sequential
    apply for final state, and lets us use set-based staging (fast, low memory churn).
    """
    net = {}
    for seq, tbl, op, pk, payload, ts in rows:
        net[pk] = (op, payload)
    upserts, deletes = [], []
    for pk, (op, payload) in net.items():
        if op in ("I", "U"):
            p = json.loads(payload)
            upserts.append((p["id"], p["user_id"], p["amount_cents"],
                            p["status"], p["channel"], p["created_at"]))
        elif op == "D":
            deletes.append((pk,))
        else:
            raise RuntimeError(f"unknown op {op}")
    if upserts:
        con.execute("CREATE OR REPLACE TEMP TABLE _stg(id BIGINT, user_id BIGINT, "
                    "amount_cents BIGINT, status INTEGER, channel INTEGER, created_at BIGINT)")
        con.executemany("INSERT INTO _stg VALUES (?,?,?,?,?,?)", upserts)
        con.execute("INSERT INTO orders_native SELECT * FROM _stg "
                    "ON CONFLICT (id) DO UPDATE SET user_id=excluded.user_id, "
                    "amount_cents=excluded.amount_cents, status=excluded.status, "
                    "channel=excluded.channel, created_at=excluded.created_at")
        con.execute("DROP TABLE _stg")
    if deletes:
        con.execute("CREATE OR REPLACE TEMP TABLE _del(id BIGINT)")
        con.executemany("INSERT INTO _del VALUES (?)", deletes)
        con.execute("DELETE FROM orders_native WHERE id IN (SELECT id FROM _del)")
        con.execute("DROP TABLE _del")


def verify(sc, dc):
    s_cnt, s_sum = sc.execute("SELECT count(*), COALESCE(sum(amount_cents),0) FROM orders").fetchone()
    d_cnt, d_sum = dc.execute("SELECT count(*), COALESCE(sum(amount_cents),0) FROM orders_native").fetchone()
    return {"sqlite_count": s_cnt, "duckdb_count": d_cnt,
            "sqlite_sum": s_sum, "duckdb_sum": d_sum,
            "drift": (s_cnt != d_cnt) or (s_sum != d_sum)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--interval", type=float, default=0.2)
    ap.add_argument("--verify-every", type=float, default=5.0)
    ap.add_argument("--duration", type=float, default=0, help="0 = run forever")
    ap.add_argument("--prune", action="store_true", help="prune applied changelog via xsqld")
    args = ap.parse_args()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    sc = C.sqlite_ro()
    dc = C.duckdb_connect()
    wm = C.get_watermark(dc)
    print(f"syncer start watermark={wm}", flush=True)

    applied = 0
    last_ts = None
    last_verify = 0
    t_start = time.time()
    last_report = t_start

    while not STOP:
        if args.duration and time.time() - t_start > args.duration:
            break
        rows = sc.execute(
            "SELECT seq,tbl,op,pk,payload,ts FROM xsqld_changelog WHERE seq>? ORDER BY seq LIMIT ?",
            (wm, args.batch)).fetchall()
        if not rows:
            time.sleep(args.interval)
        else:
            dc.execute("BEGIN TRANSACTION")
            apply_batch(dc, rows)
            wm = rows[-1][0]
            last_ts = rows[-1][5]
            dc.execute("INSERT OR REPLACE INTO xsqld_meta VALUES('watermark',?)", [str(wm)])
            dc.execute("COMMIT")
            applied += len(rows)
            if args.prune:
                try:
                    C.hrana(f"DELETE FROM xsqld_changelog WHERE seq <= {wm}")
                except Exception as e:
                    print("prune error:", e, flush=True)

        now = time.time()
        if now - last_verify >= args.verify_every:
            v = verify(sc, dc)
            pend_cnt, min_ts = sc.execute(
                "SELECT COUNT(*), MIN(ts) FROM xsqld_changelog WHERE seq>?", (wm,)).fetchone()
            lag = (now - min_ts) if pend_cnt else 0.0
            v.update(watermark=wm, applied=applied, pending=pend_cnt, lag_s=round(lag, 3))
            print("METRICS " + json.dumps(v), flush=True)
            last_verify = now

    # final state on graceful stop
    v = verify(sc, dc)
    v.update(watermark=wm, applied=applied, stopped=True)
    print("FINAL " + json.dumps(v), flush=True)
    sc.close()
    dc.close()


if __name__ == "__main__":
    main()