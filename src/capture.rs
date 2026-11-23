//! Per-database change capture: thin outbox schema, row-level trigger generation, and table filtering.
//!
//! SeekDB requires a trigger to live in the same database as its table (`OB_ERR_TRIGGER_IN_WRONG_SCHEMA`,
//! error 1435), so every namespace gets its own outbox rather than one global changelog.
//!
//! The outbox carries no payload. `AS OF SNAPSHOT` lets the sync worker re-read row state at a consistent
//! commit SCN, which keeps the per-row trigger body down to a single INSERT (measured ~11% write tax on
//! single-row OLTP versus ~14% when the body also builds a JSON payload).

/// Tables the mirror must never capture. `ATTACH ... (TYPE mysql)` against SeekDB exposes 307 internal
/// tables and views, and it does not scope them to the attached database, so filtering has to be explicit.
pub fn is_internal_table(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.starts_with("__all")
        || lower.starts_with("__ft")
        || lower.starts_with("__recycle")
        || lower.starts_with("dba_")
        || lower.starts_with("v$")
        || lower.starts_with("gv$")
        || lower.starts_with("information_schema")
        || lower == "proc"
        || lower.starts_with("xsqld_")
}

pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

/// FNV-1a, matching the naming scheme the SQLite-era capture used so trigger names stay stable across rebuilds.
fn hash_name(name: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in name.to_ascii_lowercase().bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

pub const OUTBOX_TABLE: &str = "xsqld_changelog";
pub const META_TABLE: &str = "xsqld_htap_meta";

/// `seq` is only a replay ordering hint. The watermark is a commit SCN, never `MAX(seq)`: SeekDB has
/// concurrent writers, so AUTO_INCREMENT allocation order is not commit order, and a seq-based watermark
/// would permanently skip rows whose seq was allocated early but committed late.
pub fn outbox_ddl() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {OUTBOX_TABLE}(\
         seq BIGINT NOT NULL AUTO_INCREMENT,\
         tbl VARCHAR(128) NOT NULL,\
         op VARCHAR(2) NOT NULL,\
         pk VARCHAR(2048) NOT NULL,\
         ts BIGINT NOT NULL,\
         PRIMARY KEY(seq))"
    )
}

pub fn meta_ddl() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {META_TABLE}(\
         id INT NOT NULL,\
         watermark_scn BIGINT NOT NULL DEFAULT 0,\
         schema_fingerprint VARCHAR(64) NOT NULL DEFAULT '',\
         PRIMARY KEY(id))"
    )
}

pub fn meta_seed_ddl() -> String {
    format!("INSERT IGNORE INTO {META_TABLE}(id, watermark_scn) VALUES(1, 0)")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<String>,
    /// Primary key columns in key order. Empty means the table has no declared key.
    pub key: Vec<String>,
}

impl TableSchema {
    /// Identity used for replay. A table without a declared primary key is keyed on its whole row, so
    /// "sync every table by default" still holds; updates then retire the old row and insert the new one.
    pub fn identity_columns(&self) -> &[String] {
        if self.key.is_empty() { &self.columns } else { &self.key }
    }

    pub fn trigger_names(&self) -> [String; 3] {
        let h = hash_name(&self.name);
        [
            format!("xsqld_cap_{h:016x}_i"),
            format!("xsqld_cap_{h:016x}_u"),
            format!("xsqld_cap_{h:016x}_d"),
        ]
    }
}

fn json_array(alias: &str, columns: &[String]) -> String {
    let cells: Vec<String> = columns.iter().map(|c| format!("{alias}.{}", quote_ident(c))).collect();
    format!("JSON_ARRAY({})", cells.join(","))
}

fn insert_stmt(table: &str, op: &str, alias: &str, columns: &[String]) -> String {
    format!(
        "INSERT INTO {OUTBOX_TABLE}(tbl,op,pk,ts) VALUES({},{},{},UNIX_TIMESTAMP())",
        quote_literal(table),
        quote_literal(op),
        json_array(alias, columns)
    )
}

/// `NOT (OLD.k <=> NEW.k)` is the null-safe inequality; `OLD.k <> NEW.k` would silently miss key changes
/// where either side is NULL.
fn key_changed(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("NOT (OLD.{q} <=> NEW.{q})")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Three AFTER ... FOR EACH ROW triggers. SeekDB has no statement-level triggers, so bulk writes pay one
/// PL invocation per row; `drop_triggers` is the maintenance escape hatch for that (measured 6.15x recovery).
pub fn capture_triggers(table: &TableSchema) -> Vec<String> {
    let id = table.identity_columns();
    let t = quote_ident(&table.name);
    let names = table.trigger_names();
    let ins = insert_stmt(&table.name, "I", "NEW", id);
    let del_old = insert_stmt(&table.name, "D", "OLD", id);

    vec![
        format!(
            "CREATE TRIGGER {} AFTER INSERT ON {t} FOR EACH ROW BEGIN {ins}; END",
            quote_ident(&names[0])
        ),
        format!(
            "CREATE TRIGGER {} AFTER UPDATE ON {t} FOR EACH ROW BEGIN \
             IF {} THEN {del_old}; END IF; {ins}; END",
            quote_ident(&names[1]),
            key_changed(id)
        ),
        format!(
            "CREATE TRIGGER {} AFTER DELETE ON {t} FOR EACH ROW BEGIN {del_old}; END",
            quote_ident(&names[2])
        ),
    ]
}

pub fn drop_triggers(table: &TableSchema) -> Vec<String> {
    table
        .trigger_names()
        .iter()
        .map(|n| format!("DROP TRIGGER IF EXISTS {}", quote_ident(n)))
        .collect()
}

/// Reads the outbox prefix committed at or before `scn`. `ORA_ROWSCN` must not appear here: SeekDB rejects
/// it together with `AS OF SNAPSHOT` (error 1235, "rowscn used with snapshot query").
pub fn outbox_read_sql(scn: u64, limit: u64) -> String {
    format!("SELECT seq,tbl,op,pk FROM {OUTBOX_TABLE} AS OF SNAPSHOT {scn} ORDER BY seq LIMIT {limit}")
}

pub fn current_scn_sql() -> &'static str {
    "SELECT CURRENT_SCN()"
}

/// Prune exactly the rows that were applied, in chunks.
///
/// Neither a `seq <= max` range nor "the whole snapshot prefix" is safe. A range drops rows allocated
/// early but committed late; the whole prefix drops rows beyond the batch limit that were never applied.
/// Both lose data silently.
pub fn outbox_prune_sql(seqs: &[i64]) -> Vec<String> {
    seqs.chunks(500)
        .map(|chunk| {
            let list = chunk.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(",");
            format!("DELETE FROM {OUTBOX_TABLE} WHERE seq IN ({list})")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn orders() -> TableSchema {
        TableSchema {
            name: "orders".into(),
            columns: vec!["id".into(), "uid".into(), "amt".into()],
            key: vec!["id".into()],
        }
    }

    #[test]
    fn internal_tables_are_filtered() {
        for name in [
            "__all_table",
            "__all_virtual_table_mgr",
            "DBA_OB_DATABASES",
            "V$OB_MEMORY",
            "GV$OB_SQL_AUDIT",
            "__ft_dict_ik_utf8",
            "proc",
            "xsqld_changelog",
            "xsqld_htap_meta",
        ] {
            assert!(is_internal_table(name), "{name} must be filtered");
        }
        for name in ["orders", "row_t", "Users", "t_row"] {
            assert!(!is_internal_table(name), "{name} must be captured");
        }
    }

    #[test]
    fn filtering_is_case_insensitive() {
        assert!(is_internal_table("__ALL_TABLE"));
        assert!(is_internal_table("dba_ob_databases"));
    }

    #[test]
    fn identifiers_and_literals_are_escaped() {
        assert_eq!(quote_ident("a`b"), "`a``b`");
        assert_eq!(quote_literal("a'b"), "'a''b'");
        assert_eq!(quote_literal("a\\b"), "'a\\\\b'");
    }

    #[test]
    fn three_triggers_are_row_level_after() {
        let sqls = capture_triggers(&orders());
        assert_eq!(sqls.len(), 3);
        for (sql, event) in sqls.iter().zip(["INSERT", "UPDATE", "DELETE"]) {
            assert!(
                sql.contains(&format!("AFTER {event} ON `orders` FOR EACH ROW")),
                "{sql}"
            );
            assert!(sql.contains("INSERT INTO xsqld_changelog(tbl,op,pk,ts)"));
        }
    }

    #[test]
    fn outbox_carries_no_payload() {
        for sql in capture_triggers(&orders()) {
            assert!(!sql.contains("payload"), "thin outbox must not build a payload: {sql}");
            assert!(!sql.contains("JSON_OBJECT"), "{sql}");
        }
    }

    #[test]
    fn update_retires_old_key_only_when_key_changed() {
        let sqls = capture_triggers(&orders());
        let upd = &sqls[1];
        assert!(upd.contains("IF NOT (OLD.`id` <=> NEW.`id`) THEN"), "{upd}");
        assert!(upd.contains("'D'"), "{upd}");
        assert!(upd.contains("'I'"), "{upd}");
        let plain_ins = &sqls[0];
        assert!(!plain_ins.contains("'D'"), "insert must not emit a delete");
    }

    #[test]
    fn composite_key_uses_null_safe_inequality_on_every_column() {
        let t = TableSchema {
            name: "line".into(),
            columns: vec!["a".into(), "b".into(), "v".into()],
            key: vec!["a".into(), "b".into()],
        };
        let upd = &capture_triggers(&t)[1];
        assert!(
            upd.contains("NOT (OLD.`a` <=> NEW.`a`) OR NOT (OLD.`b` <=> NEW.`b`)"),
            "{upd}"
        );
        assert!(upd.contains("JSON_ARRAY(NEW.`a`,NEW.`b`)"), "{upd}");
        assert!(!upd.contains("NEW.`v`"), "non-key column must not be in the identity");
    }

    #[test]
    fn keyless_table_is_keyed_on_whole_row() {
        let t = TableSchema {
            name: "nokey".into(),
            columns: vec!["x".into(), "y".into()],
            key: vec![],
        };
        assert_eq!(t.identity_columns(), &["x".to_string(), "y".to_string()]);
        let upd = &capture_triggers(&t)[1];
        assert!(upd.contains("JSON_ARRAY(OLD.`x`,OLD.`y`)"), "{upd}");
        assert!(upd.contains("JSON_ARRAY(NEW.`x`,NEW.`y`)"), "{upd}");
    }

    #[test]
    fn trigger_names_are_stable_and_fit_mysql_limit() {
        let t = orders();
        let a = t.trigger_names();
        assert_eq!(a, t.trigger_names());
        for n in &a {
            assert!(n.len() <= 64, "{n} exceeds MySQL identifier limit");
            assert!(n.starts_with("xsqld_cap_"));
        }
        let other = TableSchema {
            name: "ORDERS".into(),
            ..t.clone()
        };
        assert_eq!(
            other.trigger_names(),
            a,
            "naming is case-insensitive like SeekDB table names"
        );
    }

    #[test]
    fn drop_covers_all_three_triggers() {
        let t = orders();
        let drops = drop_triggers(&t);
        assert_eq!(drops.len(), 3);
        for (d, n) in drops.iter().zip(t.trigger_names()) {
            assert_eq!(d, &format!("DROP TRIGGER IF EXISTS `{n}`"));
        }
    }

    #[test]
    fn outbox_read_uses_snapshot_and_never_rowscn() {
        let sql = outbox_read_sql(1795362807293731001, 5000);
        assert!(sql.contains("AS OF SNAPSHOT 1795362807293731001"), "{sql}");
        assert!(sql.contains("ORDER BY seq LIMIT 5000"), "{sql}");
        assert!(
            !sql.to_ascii_uppercase().contains("ORA_ROWSCN"),
            "SeekDB rejects rowscn + snapshot: {sql}"
        );
    }

    #[test]
    fn watermark_is_scn_not_seq() {
        assert!(meta_ddl().contains("watermark_scn BIGINT"));
        assert!(!meta_ddl().contains("pruned_seq"));
        assert_eq!(current_scn_sql(), "SELECT CURRENT_SCN()");
    }

    #[test]
    fn outbox_ddl_is_mysql_dialect() {
        let ddl = outbox_ddl();
        assert!(ddl.contains("BIGINT NOT NULL AUTO_INCREMENT"), "{ddl}");
        assert!(!ddl.contains("AUTOINCREMENT"), "SQLite spelling must not leak: {ddl}");
        assert!(!ddl.contains("sqlite_sequence"));
    }
}

/// Live checks against a real SeekDB. Ignored by default so `cargo test` needs no server; run with
/// `cargo test -- --ignored` and XSQLD_TEST_MYSQL_HOST/PORT/USER/PASSWORD pointing at SeekDB.
///
/// String assertions above only prove the SQL is generated as intended. These prove SeekDB accepts it and
/// that capture actually fires — every check asserts an outbox row count, so a silently inert trigger fails.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod live {
    use super::*;
    use mysql_async::prelude::Queryable;
    use mysql_async::{Conn, OptsBuilder};

    const DB: &str = "xsqld_capture_it";

    async fn conn() -> Conn {
        let host = std::env::var("XSQLD_TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port: u16 = std::env::var("XSQLD_TEST_MYSQL_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(12981);
        let user = std::env::var("XSQLD_TEST_MYSQL_USER").unwrap_or_else(|_| "root".into());
        let pass = std::env::var("XSQLD_TEST_MYSQL_PASSWORD").unwrap_or_default();
        let opts = OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port)
            .user(Some(user))
            .pass(if pass.is_empty() { None } else { Some(pass) });
        Conn::new(opts).await.expect("connect to SeekDB")
    }

    async fn reset(c: &mut Conn) {
        c.query_drop(format!("DROP DATABASE IF EXISTS {DB}")).await.unwrap();
        c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
        c.query_drop(format!("USE {DB}")).await.unwrap();
        c.query_drop(outbox_ddl()).await.unwrap();
        c.query_drop(meta_ddl()).await.unwrap();
        c.query_drop(meta_seed_ddl()).await.unwrap();
    }

    async fn outbox(c: &mut Conn) -> Vec<(i64, String, String, String)> {
        c.query(format!("SELECT seq,tbl,op,pk FROM {OUTBOX_TABLE} ORDER BY seq"))
            .await
            .unwrap()
    }

    async fn clear(c: &mut Conn) {
        c.query_drop(format!("DELETE FROM {OUTBOX_TABLE}")).await.unwrap();
    }

    async fn make_table(c: &mut Conn, t: &TableSchema) {
        c.query_drop(format!("DROP TABLE IF EXISTS {}", quote_ident(&t.name)))
            .await
            .unwrap();
        let cols = t
            .columns
            .iter()
            .map(|n| format!("{} VARCHAR(64)", quote_ident(n)))
            .collect::<Vec<_>>()
            .join(",");
        let pk = if t.key.is_empty() {
            String::new()
        } else {
            format!(
                ", PRIMARY KEY({})",
                t.key.iter().map(|k| quote_ident(k)).collect::<Vec<_>>().join(",")
            )
        };
        c.query_drop(format!("CREATE TABLE {}({cols}{pk})", quote_ident(&t.name)))
            .await
            .unwrap();
        for sql in capture_triggers(t) {
            c.query_drop(sql)
                .await
                .expect("SeekDB must accept the generated trigger");
        }
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn generated_ddl_and_triggers_are_accepted_and_capture_fires() {
        let mut c = conn().await;
        reset(&mut c).await;
        let t = TableSchema {
            name: "orders".into(),
            columns: vec!["id".into(), "uid".into(), "amt".into()],
            key: vec!["id".into()],
        };
        make_table(&mut c, &t).await;

        clear(&mut c).await;
        c.query_drop("INSERT INTO orders(id,uid,amt) VALUES('1','u1','10')")
            .await
            .unwrap();
        let ob = outbox(&mut c).await;
        assert_eq!(ob.len(), 1, "INSERT must produce exactly one outbox row");
        assert_eq!(ob[0].2, "I");
        assert_eq!(ob[0].1, "orders");

        clear(&mut c).await;
        c.query_drop("UPDATE orders SET amt='20' WHERE id='1'").await.unwrap();
        let ob = outbox(&mut c).await;
        assert_eq!(ob.len(), 1, "key-preserving UPDATE is a single upsert");
        assert_eq!(ob[0].2, "I");

        clear(&mut c).await;
        c.query_drop("UPDATE orders SET id='2' WHERE id='1'").await.unwrap();
        let ops: Vec<String> = outbox(&mut c).await.into_iter().map(|r| r.2).collect();
        assert_eq!(ops, vec!["D", "I"], "key-changing UPDATE must retire the old key");

        clear(&mut c).await;
        c.query_drop("DELETE FROM orders WHERE id='2'").await.unwrap();
        let ob = outbox(&mut c).await;
        assert_eq!(ob.len(), 1);
        assert_eq!(ob[0].2, "D");

        clear(&mut c).await;
        c.query_drop("INSERT INTO orders(id,uid,amt) VALUES('10','a','1'),('11','b','2'),('12','c','3')")
            .await
            .unwrap();
        assert_eq!(
            outbox(&mut c).await.len(),
            3,
            "every row of a multi-row INSERT must be captured"
        );

        clear(&mut c).await;
        c.query_drop("BEGIN").await.unwrap();
        c.query_drop("INSERT INTO orders(id,uid,amt) VALUES('99','x','9')")
            .await
            .unwrap();
        c.query_drop("ROLLBACK").await.unwrap();
        assert_eq!(
            outbox(&mut c).await.len(),
            0,
            "rolled-back writes must leave no outbox residue"
        );

        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn composite_and_keyless_tables_capture_identity() {
        let mut c = conn().await;
        reset(&mut c).await;

        let comp = TableSchema {
            name: "line".into(),
            columns: vec!["a".into(), "b".into(), "v".into()],
            key: vec!["a".into(), "b".into()],
        };
        make_table(&mut c, &comp).await;
        clear(&mut c).await;
        c.query_drop("INSERT INTO line(a,b,v) VALUES('1','2','x')")
            .await
            .unwrap();
        let ob = outbox(&mut c).await;
        assert_eq!(ob.len(), 1);
        let pk: serde_json::Value = serde_json::from_str(&ob[0].3).expect("pk must be a JSON array");
        assert_eq!(
            pk,
            serde_json::json!(["1", "2"]),
            "composite key must encode both columns in order"
        );

        let nokey = TableSchema {
            name: "nokey".into(),
            columns: vec!["x".into(), "y".into()],
            key: vec![],
        };
        make_table(&mut c, &nokey).await;
        clear(&mut c).await;
        c.query_drop("INSERT INTO nokey(x,y) VALUES('p','q')").await.unwrap();
        c.query_drop("UPDATE nokey SET y='r' WHERE x='p'").await.unwrap();
        let ops: Vec<String> = outbox(&mut c).await.into_iter().map(|r| r.2).collect();
        assert_eq!(
            ops,
            vec!["I", "D", "I"],
            "a keyless table is keyed on the whole row, so any UPDATE retires it"
        );

        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn snapshot_prefix_read_works_and_rowscn_is_rejected() {
        let mut c = conn().await;
        reset(&mut c).await;
        let t = TableSchema {
            name: "snap".into(),
            columns: vec!["id".into(), "v".into()],
            key: vec!["id".into()],
        };
        make_table(&mut c, &t).await;
        c.query_drop("INSERT INTO snap(id,v) VALUES('1','before')")
            .await
            .unwrap();
        let scn: i64 = c.query_first(current_scn_sql()).await.unwrap().expect("CURRENT_SCN()");
        assert!(scn > 0);
        c.query_drop("INSERT INTO snap(id,v) VALUES('2','after')")
            .await
            .unwrap();

        let prefix: Vec<(i64, String, String, String)> = c.query(outbox_read_sql(scn as u64, 1000)).await.unwrap();
        assert_eq!(
            prefix.len(),
            1,
            "the snapshot prefix must exclude writes committed after the SCN"
        );
        assert_eq!(prefix[0].3, "[\"1\"]");

        let bad = format!("SELECT seq, ORA_ROWSCN FROM {OUTBOX_TABLE} AS OF SNAPSHOT {scn}");
        let err = c
            .query_drop(bad)
            .await
            .expect_err("SeekDB rejects ORA_ROWSCN with AS OF SNAPSHOT");
        assert!(
            err.to_string().contains("1235") || err.to_string().contains("rowscn"),
            "{err}"
        );

        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn maintenance_window_really_removes_capture_and_restores_it() {
        let mut c = conn().await;
        reset(&mut c).await;
        let t = TableSchema {
            name: "bulk".into(),
            columns: vec!["id".into(), "v".into()],
            key: vec!["id".into()],
        };
        make_table(&mut c, &t).await;

        let values: Vec<String> = (0..500).map(|i| format!("('{i}','x')")).collect();
        let insert = format!("INSERT INTO bulk(id,v) VALUES{}", values.join(","));

        clear(&mut c).await;
        c.query_drop(&insert).await.unwrap();
        assert_eq!(
            outbox(&mut c).await.len(),
            500,
            "capture must be active before the window"
        );

        for sql in drop_triggers(&t) {
            c.query_drop(sql).await.unwrap();
        }
        c.query_drop("DELETE FROM bulk").await.unwrap();
        clear(&mut c).await;
        c.query_drop(&insert).await.unwrap();
        assert_eq!(
            outbox(&mut c).await.len(),
            0,
            "DROP TRIGGER must genuinely stop capture, not just look like it"
        );

        for sql in capture_triggers(&t) {
            c.query_drop(sql).await.unwrap();
        }
        c.query_drop("DELETE FROM bulk").await.unwrap();
        clear(&mut c).await;
        c.query_drop(&insert).await.unwrap();
        assert_eq!(
            outbox(&mut c).await.len(),
            500,
            "rebuilding triggers must restore capture"
        );

        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn cross_database_trigger_is_rejected() {
        let mut c = conn().await;
        reset(&mut c).await;
        c.query_drop("CREATE DATABASE IF NOT EXISTS xsqld_capture_it_other")
            .await
            .unwrap();
        c.query_drop("CREATE TABLE IF NOT EXISTS xsqld_capture_it_other.t(id VARCHAR(8) PRIMARY KEY)")
            .await
            .unwrap();
        let sql = format!(
            "CREATE TRIGGER {} AFTER INSERT ON xsqld_capture_it_other.t FOR EACH ROW BEGIN SELECT 1; END",
            quote_ident("xsqld_cross")
        );
        let err = c
            .query_drop(sql)
            .await
            .expect_err("trigger must not be creatable across databases");
        assert!(
            err.to_string().contains("1435") || err.to_string().contains("wrong schema"),
            "{err}"
        );
        c.query_drop("DROP DATABASE xsqld_capture_it_other").await.unwrap();
        c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }
}
