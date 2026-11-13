import json, os, sqlite3, time, urllib.request

BASE = os.path.expanduser("~/xsqld-bench")
HTAP_DIR = os.path.join(BASE, "htap-p0")
NS = "htap"
SQLITE_PATH = os.path.join(BASE, "data", "dbs", NS, "data")
DUCKDB_PATH = os.path.join(HTAP_DIR, "orders.duckdb")
HRANA = "http://127.0.0.1:8080/v2/pipeline"
HOST = NS + ".db"

ORDER_COLS = ["id", "user_id", "amount_cents", "status", "channel", "created_at"]


def hrana(sql, args=None, timeout=120):
    stmt = {"sql": sql}
    if args:
        stmt["args"] = args
    body = json.dumps({"requests": [{"type": "execute", "stmt": stmt}, {"type": "close"}]}).encode()
    req = urllib.request.Request(HRANA, data=body, headers={"Host": HOST, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.load(r)
    res = d["results"][0]
    if res["type"] == "error":
        raise RuntimeError(res["error"]["message"])
    return res["response"]["result"]


def hrana_batch(statements, timeout=300):
    reqs = [{"type": "execute", "stmt": {"sql": s}} for s in statements]
    reqs.append({"type": "close"})
    body = json.dumps({"requests": reqs}).encode()
    req = urllib.request.Request(HRANA, data=body, headers={"Host": HOST, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.load(r)
    for res in d["results"]:
        if res["type"] == "error":
            raise RuntimeError(res["error"]["message"])
    return d


def sqlite_ro():
    c = sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro", uri=True, timeout=30)
    c.execute("PRAGMA query_only=ON")
    return c


def duckdb_connect(read_only=False):
    import duckdb
    c = duckdb.connect(DUCKDB_PATH, read_only=read_only)
    c.execute("SET memory_limit='512MB'")
    c.execute("SET threads=1")
    c.execute("SET preserve_insertion_order=false")
    return c


def iarg(v):
    return {"type": "integer", "value": str(v)}


MIRROR_DDL = (
    "CREATE TABLE IF NOT EXISTS orders_native("
    "id BIGINT PRIMARY KEY, user_id BIGINT, amount_cents BIGINT, "
    "status INTEGER, channel INTEGER, created_at BIGINT)"
)
META_DDL = "CREATE TABLE IF NOT EXISTS xsqld_meta(k VARCHAR PRIMARY KEY, v VARCHAR)"


def init_duckdb_schema(con):
    con.execute(MIRROR_DDL)
    con.execute(META_DDL)
    con.execute("INSERT OR REPLACE INTO xsqld_meta VALUES('watermark','0')")


def rebuild_mirror(con):
    """Full CTAS rebuild from SQLite (source of truth). Resets watermark to current max seq."""
    con.execute("INSTALL sqlite")
    con.execute("LOAD sqlite")
    con.execute(f"ATTACH '{SQLITE_PATH}' AS s (TYPE sqlite, READ_ONLY)")
    con.execute("CREATE OR REPLACE TABLE orders_native("
                "id BIGINT PRIMARY KEY, user_id BIGINT, amount_cents BIGINT, "
                "status INTEGER, channel INTEGER, created_at BIGINT)")
    con.execute("INSERT INTO orders_native SELECT id,user_id,amount_cents,status,channel,created_at FROM s.orders")
    con.execute("DETACH s")
    with sqlite_ro() as sc:
        row = sc.execute("SELECT COALESCE(MAX(seq),0) FROM xsqld_changelog").fetchone()
        wm = row[0]
    con.execute("INSERT OR REPLACE INTO xsqld_meta VALUES('watermark',?)", [str(wm)])
    return wm


def get_watermark(con):
    row = con.execute("SELECT v FROM xsqld_meta WHERE k='watermark'").fetchone()
    return int(row[0]) if row else 0