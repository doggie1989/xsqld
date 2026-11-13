import argparse, os, random, sqlite3, time
import common as C


def gen_fixture(n):
    if os.path.exists(C.SQLITE_PATH):
        with sqlite3.connect(C.SQLITE_PATH) as c:
            cnt = c.execute("SELECT count(*) FROM orders").fetchone()[0]
        if cnt >= n:
            print(f"fixture exists with {cnt} rows, skip generation", flush=True)
            return
    os.makedirs(os.path.dirname(C.SQLITE_PATH), exist_ok=True)
    if os.path.exists(C.SQLITE_PATH):
        os.remove(C.SQLITE_PATH)
    db = sqlite3.connect(C.SQLITE_PATH)
    db.execute("PRAGMA journal_mode=OFF")
    db.execute("PRAGMA synchronous=OFF")
    db.execute("PRAGMA cache_size=-65536")
    db.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, "
               "amount_cents INTEGER NOT NULL, status INTEGER NOT NULL, "
               "channel INTEGER NOT NULL, created_at INTEGER NOT NULL)")
    r = random.Random(42)
    pool = []
    for _ in range(1_000_000):
        uid = r.randrange(100_000) if r.random() < 0.3 else r.randrange(5_000_000)
        amount = min(int(r.lognormvariate(8.5, 1.0)), 10_000_000)
        status = r.choices(range(6), weights=[5, 10, 10, 65, 7, 3])[0]
        pool.append((uid, amount, status, r.randrange(4)))
    base, span = 1763000000, 365 * 86400
    t0 = time.time()
    chunk = 100_000
    with db:
        for start in range(1, n + 1, chunk):
            end = min(start + chunk, n + 1)
            db.executemany("INSERT INTO orders VALUES(?,?,?,?,?,?)",
                           ((i, *pool[r.randrange(len(pool))], base + r.randrange(span))
                            for i in range(start, end)))
            if (start // chunk) % 10 == 0:
                print(f"  {start}/{n} rows {time.time()-t0:.0f}s", flush=True)
    db.execute("CREATE INDEX idx_orders_user ON orders(user_id, created_at)")
    db.execute("CREATE INDEX idx_orders_time ON orders(created_at)")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    db.close()
    print(f"fixture done {time.time()-t0:.0f}s, {os.path.getsize(C.SQLITE_PATH)/1e6:.0f} MB", flush=True)


def split_statements(sql):
    lines = [ln for ln in sql.splitlines(keepends=True) if not ln.lstrip().startswith("--")]
    sql = "".join(lines)
    stmts, buf = [], ""
    for line in sql.splitlines(keepends=True):
        buf += line
        if sqlite3.complete_statement(buf):
            s = buf.strip()
            if s:
                stmts.append(s)
            buf = ""
    if buf.strip():
        stmts.append(buf.strip())
    return stmts


def install_schema():
    sql = open(os.path.join(C.HTAP_DIR, "schema.sql")).read()
    stmts = split_statements(sql)
    for s in stmts:
        C.hrana(s)
        print("installed:", s.split("(")[0].split("ON")[0].strip()[:60], flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--skip-gen", action="store_true")
    args = ap.parse_args()

    if not args.skip_gen:
        print("== generate fixture ==", flush=True)
        gen_fixture(args.rows)

    print("== install changelog + triggers (via xsqld) ==", flush=True)
    install_schema()

    print("== init DuckDB mirror (CTAS) ==", flush=True)
    os.makedirs(C.HTAP_DIR, exist_ok=True)
    con = C.duckdb_connect()
    C.init_duckdb_schema(con)
    t0 = time.time()
    wm = C.rebuild_mirror(con)
    cnt = con.execute("SELECT count(*) FROM orders_native").fetchone()[0]
    con.close()
    print(f"mirror built: {cnt} rows, watermark={wm}, {time.time()-t0:.0f}s", flush=True)
    print("SETUP DONE", flush=True)


if __name__ == "__main__":
    main()