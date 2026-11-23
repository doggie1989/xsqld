//! DuckDB columnar mirror: attachment, per-table snapshot, and deduplicated incremental apply.
//!
//! The snapshot projection carries three guards, each for a lossy mapping measured against SeekDB 1.4.0
//! with the DuckDB `mysql` extension. `DECIMAL(p,s)` with p > 38 becomes DOUBLE and loses digits
//! (`12345678901234567890123456789012345.1234567890` came back as `1.234567890123457e+34`). Zero dates
//! become NULL regardless of `mysql_incomplete_dates_as_nulls`. Both are read as CHAR instead, so the
//! mirror holds text and the router must refuse arithmetic on those columns. `BIT(n)` is read as UNSIGNED
//! because CHAR would return raw non-UTF8 bytes.

use crate::capture::quote_ident;

/// DuckDB's DECIMAL ceiling. Above this the mysql extension silently falls back to DOUBLE.
pub const MAX_DECIMAL_PRECISION: u32 = 38;

#[derive(Debug, Clone)]
pub struct Source {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl Source {
    /// The extension accepts exactly `host, user, passwd, db, port, socket, connect_timeout` —
    /// `password=` and `database=` are rejected as unrecognized parameters.
    pub fn attach_sql(&self, alias: &str) -> String {
        let mut parts = vec![
            format!("host={}", self.host),
            format!("port={}", self.port),
            format!("user={}", self.user),
            format!("db={}", self.database),
        ];
        if !self.password.is_empty() {
            parts.push(format!("passwd={}", self.password));
        }
        format!(
            "ATTACH '{}' AS {} (TYPE mysql, READ_ONLY)",
            parts.join(" "),
            quote_ident(alias)
        )
    }
}

#[derive(Debug, Clone)]
pub struct SourceColumn {
    pub name: String,
    /// `information_schema.columns.DATA_TYPE`, lower case (e.g. "decimal", "datetime", "bit").
    pub data_type: String,
    pub numeric_precision: Option<u32>,
}

impl SourceColumn {
    /// Columns whose mysql-extension mapping loses information and must be read as text instead.
    pub fn needs_text_cast(&self) -> bool {
        let dt = self.data_type.to_ascii_lowercase();
        match dt.as_str() {
            "decimal" | "numeric" => self.numeric_precision.unwrap_or(0) > MAX_DECIMAL_PRECISION,
            // zero dates are silently NULL-ed by the extension in both flag settings
            "date" | "datetime" | "timestamp" => true,
            // BIT(n) maps to BLOB and BIT(1) to BOOLEAN, neither matches MySQL's numeric semantics
            "bit" => true,
            _ => false,
        }
    }

    fn projection(&self) -> String {
        let q = quote_ident(&self.name);
        if self.data_type.eq_ignore_ascii_case("bit") {
            // CAST(BIT AS CHAR) returns the raw bytes (b'10101010' -> 0xAA), which DuckDB rejects as
            // invalid UTF-8 and pymysql cannot decode either. UNSIGNED gives the numeric value MySQL uses.
            format!("CAST({q} AS UNSIGNED) AS {q}")
        } else if self.needs_text_cast() {
            format!("CAST({q} AS CHAR) AS {q}")
        } else {
            q
        }
    }
}

/// A plain CTAS is only safe when no column needs a text cast. Otherwise the projection has to go through
/// `mysql_query`, which is the only way to push a source-side CAST into SeekDB.
pub fn snapshot_sql(alias: &str, table: &str, columns: &[SourceColumn], mirror: &str, timeout_us: u64) -> String {
    let target = quote_ident(mirror);
    let proj = if columns.iter().any(SourceColumn::needs_text_cast) {
        columns
            .iter()
            .map(SourceColumn::projection)
            .collect::<Vec<_>>()
            .join(",")
    } else {
        "*".to_string()
    };
    // The query_timeout hint is mandatory, not an optimization: SeekDB's default ob_query_timeout is 10s
    // and a 10M-row snapshot takes ~9-12s, so a plain CTAS fails right at the boundary. A per-query hint is
    // the only reliable lever — SET SESSION through the extension lands on a different pooled connection
    // and does not stick, and changing GLOBAL would affect the whole server.
    let inner = format!(
        "SELECT /*+ query_timeout({timeout_us}) */ {proj} FROM {}",
        quote_ident(table)
    );
    format!(
        "CREATE OR REPLACE TABLE {target} AS SELECT * FROM mysql_query({}, {})",
        sql_string(alias),
        sql_string(&inner)
    )
}

pub fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub fn mirror_name(table: &str) -> String {
    format!("m_{}", table.to_ascii_lowercase())
}

/// Deduplicate the changelog before applying it.
///
/// This is a correctness requirement, not an optimization: DuckDB 2.0.1's MERGE does **not** raise when
/// several source rows match one target row (the documentation says it does). It silently keeps the first
/// match, so replaying a changelog without dedup would retain the *oldest* value for a key — exactly
/// backwards. `ORDER BY seq DESC` makes the newest change win.
pub fn dedup_sql(staging: &str, key_cols: &[String], out: &str) -> String {
    let partition = key_cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(",");
    format!(
        "CREATE OR REPLACE TEMP TABLE {} AS SELECT * FROM {} \
         QUALIFY row_number() OVER (PARTITION BY {partition} ORDER BY seq DESC) = 1",
        quote_ident(out),
        quote_ident(staging)
    )
}

/// Apply a deduplicated staging table. `op` is 'I' or 'D'.
///
/// A delete for a row that is absent must be a no-op, hence `WHEN NOT MATCHED AND s.op <> 'D'`.
pub fn merge_sql(mirror: &str, staging: &str, key_cols: &[String], value_cols: &[String]) -> String {
    // Null-safe equality: a keyless table is keyed on its whole row, so key columns can legitimately be
    // NULL. With `=` those rows never match and deletes silently do nothing.
    let on = key_cols
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("t.{q} IS NOT DISTINCT FROM s.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let updates = value_cols
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("{q} = s.{q}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let insert_cols = key_cols
        .iter()
        .chain(value_cols.iter())
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(",");
    let insert_vals = key_cols
        .iter()
        .chain(value_cols.iter())
        .map(|c| format!("s.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(",");

    let mut sql = format!(
        "MERGE INTO {} AS t USING {} AS s ON {on} \
         WHEN MATCHED AND s.op = 'D' THEN DELETE",
        quote_ident(mirror),
        quote_ident(staging)
    );
    if !updates.is_empty() {
        sql.push_str(&format!(" WHEN MATCHED THEN UPDATE SET {updates}"));
    }
    sql.push_str(&format!(
        " WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    ));
    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::is_internal_table;

    fn col(name: &str, dt: &str, prec: Option<u32>) -> SourceColumn {
        SourceColumn {
            name: name.into(),
            data_type: dt.into(),
            numeric_precision: prec,
        }
    }

    fn src() -> Source {
        Source {
            host: "127.0.0.1".into(),
            port: 12981,
            user: "root".into(),
            password: String::new(),
            database: "cg".into(),
        }
    }

    #[test]
    fn attach_uses_the_parameter_names_the_extension_accepts() {
        let sql = src().attach_sql("src");
        assert!(sql.contains("host=127.0.0.1"), "{sql}");
        assert!(sql.contains("port=12981"), "{sql}");
        assert!(sql.contains("db=cg"), "{sql}");
        assert!(sql.contains("(TYPE mysql, READ_ONLY)"), "{sql}");
        assert!(!sql.contains("database="), "extension rejects database=: {sql}");
        assert!(!sql.contains("password="), "extension rejects password=: {sql}");
        assert!(!sql.contains("passwd="), "empty password must be omitted: {sql}");
    }

    #[test]
    fn attach_includes_password_only_when_set() {
        let mut s = src();
        s.password = "secret".into();
        assert!(s.attach_sql("src").contains("passwd=secret"));
    }

    #[test]
    fn wide_decimal_is_cast_but_narrow_decimal_is_not() {
        assert!(col("d", "decimal", Some(65)).needs_text_cast());
        assert!(!col("d", "decimal", Some(38)).needs_text_cast());
        assert!(!col("d", "decimal", Some(20)).needs_text_cast());
        assert!(!col("i", "bigint", Some(20)).needs_text_cast());
    }

    #[test]
    fn temporal_and_bit_columns_are_cast_because_their_mapping_is_lossy() {
        for dt in ["date", "datetime", "timestamp", "bit"] {
            assert!(col("c", dt, None).needs_text_cast(), "{dt} must be cast");
        }
        assert!(
            !col("c", "time", None).needs_text_cast(),
            "TIME already maps to VARCHAR"
        );
        assert!(!col("c", "varchar", None).needs_text_cast());
    }

    #[test]
    fn clean_table_selects_star_but_still_carries_the_timeout_hint() {
        let cols = vec![col("id", "int", Some(11)), col("s", "varchar", None)];
        let sql = snapshot_sql("src", "row_t", &cols, "m_row_t", 600_000_000);
        assert!(sql.contains("mysql_query('src',"), "{sql}");
        assert!(
            sql.contains("/*+ query_timeout(600000000) */"),
            "hint is mandatory, not optional: {sql}"
        );
        assert!(sql.contains("*/ * FROM `row_t`"), "clean table must not cast: {sql}");
        assert!(!sql.contains("AS CHAR"), "{sql}");
    }

    #[test]
    fn lossy_table_routes_through_mysql_query_with_source_side_cast() {
        let cols = vec![
            col("id", "int", Some(11)),
            col("d", "decimal", Some(65)),
            col("dt", "datetime", None),
        ];
        let sql = snapshot_sql("src", "t", &cols, "m_t", 600_000_000);
        assert!(sql.contains("mysql_query('src',"), "{sql}");
        assert!(sql.contains("CAST(`d` AS CHAR) AS `d`"), "{sql}");
        assert!(sql.contains("CAST(`dt` AS CHAR) AS `dt`"), "{sql}");
        assert!(sql.contains("*/ `id`,CAST"), "clean column must not be cast: {sql}");
    }

    #[test]
    fn bit_is_cast_to_unsigned_not_char() {
        let cols = vec![col("b", "bit", Some(8))];
        let sql = snapshot_sql("src", "t", &cols, "m_t", 600_000_000);
        assert!(sql.contains("CAST(`b` AS UNSIGNED) AS `b`"), "{sql}");
        assert!(
            !sql.contains("CAST(`b` AS CHAR)"),
            "CHAR yields raw non-UTF8 bytes: {sql}"
        );
    }

    #[test]
    fn bundled_engine_version_matches_the_pin() {
        let c = duckdb::Connection::open_in_memory().unwrap();
        let v: String = c.query_row("SELECT version()", [], |r| r.get(0)).unwrap();
        // duckdb-rs encodes the engine version as 1.MAJOR_MINOR_PATCH.x, so =1.20000.2 is DuckDB 2.0.0.
        assert!(v.starts_with("v2.0."), "bundled DuckDB drifted from the pin: {v}");
    }

    #[test]
    fn snapshot_escapes_quotes_in_the_inner_statement() {
        let cols = vec![col("d", "decimal", Some(65))];
        let sql = snapshot_sql("src", "we'ird", &cols, "m_t", 600_000_000);
        assert!(sql.contains("we''ird"), "inner literal must escape the quote: {sql}");
    }

    #[test]
    fn dedup_keeps_the_newest_change_per_key() {
        let sql = dedup_sql("stg", &["id".into()], "stg_dedup");
        assert!(
            sql.contains("QUALIFY row_number() OVER (PARTITION BY `id` ORDER BY seq DESC) = 1"),
            "{sql}"
        );
        assert!(
            sql.contains("CREATE OR REPLACE TEMP TABLE `stg_dedup` AS SELECT * FROM `stg`"),
            "{sql}"
        );
    }

    #[test]
    fn dedup_partitions_on_every_key_column() {
        let sql = dedup_sql("stg", &["a".into(), "b".into()], "d");
        assert!(sql.contains("PARTITION BY `a`,`b`"), "{sql}");
    }

    #[test]
    fn merge_deletes_updates_and_inserts_with_delete_of_absent_row_as_noop() {
        let sql = merge_sql("m_t", "d", &["id".into()], &["v".into(), "n".into()]);
        assert!(sql.contains("ON t.`id` IS NOT DISTINCT FROM s.`id`"), "{sql}");
        assert!(sql.contains("WHEN MATCHED AND s.op = 'D' THEN DELETE"), "{sql}");
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET `v` = s.`v`, `n` = s.`n`"),
            "{sql}"
        );
        assert!(
            sql.contains("WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT (`id`,`v`,`n`) VALUES (s.`id`,s.`v`,s.`n`)"),
            "{sql}"
        );
        let delete_pos = sql.find("THEN DELETE").unwrap();
        let update_pos = sql.find("THEN UPDATE").unwrap();
        assert!(delete_pos < update_pos, "the delete branch must be tested first: {sql}");
    }

    #[test]
    fn merge_on_key_only_table_has_no_update_branch() {
        let sql = merge_sql("m_t", "d", &["x".into(), "y".into()], &[]);
        assert!(!sql.contains("THEN UPDATE"), "nothing to update: {sql}");
        assert!(
            sql.contains("ON t.`x` IS NOT DISTINCT FROM s.`x` AND t.`y` IS NOT DISTINCT FROM s.`y`"),
            "{sql}"
        );
        assert!(sql.contains("INSERT (`x`,`y`) VALUES (s.`x`,s.`y`)"), "{sql}");
    }

    #[test]
    fn mirror_names_are_lowercased_and_prefixed() {
        assert_eq!(mirror_name("Row_T"), "m_row_t");
        assert!(!is_internal_table(&mirror_name("orders")));
    }
}

/// Live checks against SeekDB + an in-process DuckDB. Ignored by default; run with
/// `cargo test -- --ignored --test-threads=1`. The shell must not export an `http_proxy` containing
/// credentials: DuckDB's extension installer cannot parse userinfo in a proxy URL.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod live {
    use super::*;
    use crate::capture::is_internal_table;
    use duckdb::Connection;
    use mysql_async::prelude::Queryable;
    use mysql_async::{Conn, OptsBuilder};

    const DB: &str = "xsqld_mirror_it";

    fn col(name: &str, dt: &str, prec: Option<u32>) -> SourceColumn {
        SourceColumn {
            name: name.into(),
            data_type: dt.into(),
            numeric_precision: prec,
        }
    }

    fn source() -> Source {
        Source {
            host: std::env::var("XSQLD_TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            port: std::env::var("XSQLD_TEST_MYSQL_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(12981),
            user: std::env::var("XSQLD_TEST_MYSQL_USER").unwrap_or_else(|_| "root".into()),
            password: std::env::var("XSQLD_TEST_MYSQL_PASSWORD").unwrap_or_default(),
            database: DB.into(),
        }
    }

    async fn mysql() -> Conn {
        let s = source();
        let opts = OptsBuilder::default()
            .ip_or_hostname(s.host)
            .tcp_port(s.port)
            .user(Some(s.user))
            .pass(if s.password.is_empty() { None } else { Some(s.password) });
        let mut c = Conn::new(opts).await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS {DB}")).await.unwrap();
        c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
        c.query_drop(format!("USE {DB}")).await.unwrap();
        c
    }

    fn duck() -> Connection {
        // The SQLite-era config set enable_external_access(false); that blocks the mysql extension, so the
        // mirror deliberately differs here.
        let cfg = duckdb::Config::default()
            .enable_external_access(true)
            .unwrap()
            .enable_autoload_extension(false)
            .unwrap()
            .with("memory_limit", "512MB")
            .unwrap()
            .with("preserve_insertion_order", "false")
            .unwrap()
            .threads(2)
            .unwrap();
        let c = Connection::open_in_memory_with_flags(cfg).unwrap();
        if c.execute_batch("LOAD mysql").is_err() {
            c.execute_batch("INSTALL mysql").expect("install mysql extension");
            c.execute_batch("LOAD mysql").expect("load mysql extension");
        }
        let loaded: bool = c
            .query_row(
                "SELECT loaded FROM duckdb_extensions() WHERE extension_name='mysql_scanner'",
                [],
                |r| r.get::<_, bool>(0),
            )
            .unwrap_or(false);
        assert!(loaded, "mysql extension must actually be loaded");
        c.execute_batch(&source().attach_sql("src")).expect("ATTACH SeekDB");
        c
    }

    fn text(c: &Connection, sql: &str) -> String {
        c.query_row(sql, [], |r| r.get::<_, Option<String>>(0))
            .unwrap()
            .unwrap_or_else(|| "<NULL>".into())
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn snapshot_preserves_values_that_the_extension_would_silently_corrupt() {
        let mut m = mysql().await;
        m.query_drop(
            "CREATE TABLE lossy(id INT PRIMARY KEY, d DECIMAL(65,10), dt DATETIME, b BIT(8), narrow DECIMAL(20,4))",
        )
        .await
        .unwrap();
        m.query_drop("SET SESSION sql_mode=''").await.unwrap();
        m.query_drop(
            "INSERT INTO lossy VALUES(1, 12345678901234567890123456789012345.1234567890, \
             '0000-00-00 00:00:00', b'10101010', 1.2500)",
        )
        .await
        .unwrap();

        let cols = vec![
            col("id", "int", Some(11)),
            col("d", "decimal", Some(65)),
            col("dt", "datetime", None),
            col("b", "bit", Some(8)),
            col("narrow", "decimal", Some(20)),
        ];
        let d = duck();
        d.execute_batch(&snapshot_sql("src", "lossy", &cols, "m_lossy", 600_000_000))
            .unwrap();

        assert_eq!(
            text(&d, "SELECT d FROM m_lossy WHERE id=1"),
            "12345678901234567890123456789012345.1234567890",
            "DECIMAL(65,10) must survive exactly, not become a DOUBLE"
        );
        assert_eq!(
            text(&d, "SELECT dt FROM m_lossy WHERE id=1"),
            "0000-00-00 00:00:00",
            "zero date must survive, not become NULL"
        );
        let bitval: i64 = d
            .query_row("SELECT b FROM m_lossy WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(bitval, 170, "BIT(8) b'10101010' must survive as its numeric value");
        let narrow: f64 = d
            .query_row("SELECT narrow FROM m_lossy WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert!((narrow - 1.25).abs() < 1e-9, "DECIMAL(20,4) stays numeric");

        // The unguarded path must actually differ, otherwise the guard test is vacuous.
        d.execute_batch("CREATE OR REPLACE TABLE naive AS FROM src.lossy")
            .unwrap();
        let naive = text(&d, "SELECT CAST(d AS VARCHAR) FROM naive WHERE id=1");
        assert_ne!(
            naive, "12345678901234567890123456789012345.1234567890",
            "plain CTAS must lose precision, proving the guard is doing work"
        );
        assert_eq!(
            text(&d, "SELECT CAST(dt AS VARCHAR) FROM naive WHERE id=1"),
            "<NULL>",
            "plain CTAS must NULL the zero date, proving the guard is doing work"
        );

        m.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn dedup_and_merge_apply_newest_value_and_deletes() {
        let mut m = mysql().await;
        m.query_drop("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(32), n INT)")
            .await
            .unwrap();
        m.query_drop("INSERT INTO t VALUES(1,'orig',10),(2,'keep',20)")
            .await
            .unwrap();

        let cols = vec![
            col("id", "int", Some(11)),
            col("v", "varchar", None),
            col("n", "int", Some(11)),
        ];
        let d = duck();
        d.execute_batch(&snapshot_sql("src", "t", &cols, "m_t", 600_000_000))
            .unwrap();
        let cnt: i64 = d.query_row("SELECT COUNT(*) FROM m_t", [], |r| r.get(0)).unwrap();
        assert_eq!(cnt, 2);

        // Changelog with two changes to key 1 (must keep seq=102), an insert of 3, and a delete of 2.
        d.execute_batch("CREATE OR REPLACE TEMP TABLE stg(seq BIGINT, id INTEGER, op VARCHAR, v VARCHAR, n INTEGER)")
            .unwrap();
        d.execute_batch(
            "INSERT INTO stg VALUES(101,1,'I','older',11),(102,1,'I','newest',12),\
             (103,3,'I','added',30),(104,2,'D',NULL,NULL),(105,99,'D',NULL,NULL)",
        )
        .unwrap();
        d.execute_batch(&dedup_sql("stg", &["id".into()], "stg_d")).unwrap();
        let dedup_cnt: i64 = d.query_row("SELECT COUNT(*) FROM stg_d", [], |r| r.get(0)).unwrap();
        assert_eq!(dedup_cnt, 4, "five changes over four distinct keys must dedup to four");

        d.execute_batch(&merge_sql("m_t", "stg_d", &["id".into()], &["v".into(), "n".into()]))
            .unwrap();

        assert_eq!(
            text(&d, "SELECT v FROM m_t WHERE id=1"),
            "newest",
            "without dedup MERGE silently keeps the first match, i.e. the OLDEST value"
        );
        let n: i64 = d.query_row("SELECT n FROM m_t WHERE id=1", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 12);
        assert_eq!(text(&d, "SELECT v FROM m_t WHERE id=3"), "added");
        let gone: i64 = d
            .query_row("SELECT COUNT(*) FROM m_t WHERE id=2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(gone, 0, "delete must remove the row");
        let absent: i64 = d
            .query_row("SELECT COUNT(*) FROM m_t WHERE id=99", [], |r| r.get(0))
            .unwrap();
        assert_eq!(absent, 0, "delete of an absent row must be a no-op, not an insert");
        let total: i64 = d.query_row("SELECT COUNT(*) FROM m_t", [], |r| r.get(0)).unwrap();
        assert_eq!(total, 2);

        // Prove the dedup is load-bearing. Depending on engine version and plan shape, an undeduplicated
        // MERGE either raises "cannot affect the same target row more than once" or silently keeps the
        // first match. Both outcomes are wrong for changelog replay, so either one proves the point.
        d.execute_batch("CREATE OR REPLACE TABLE m_naive AS FROM src.t")
            .unwrap();
        d.execute_batch("CREATE OR REPLACE TEMP TABLE stg_raw AS SELECT * FROM stg")
            .unwrap();
        let naive = d.execute_batch(&merge_sql(
            "m_naive",
            "stg_raw",
            &["id".into()],
            &["v".into(), "n".into()],
        ));
        match naive {
            Err(e) => assert!(
                e.to_string().contains("more than once") || e.to_string().contains("deduplicated"),
                "unexpected MERGE failure: {e}"
            ),
            Ok(()) => assert_eq!(
                text(&d, "SELECT v FROM m_naive WHERE id=1"),
                "older",
                "an undeduplicated MERGE must keep the first (oldest) match, proving dedup is required"
            ),
        }

        m.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs a live SeekDB"]
    async fn attach_does_not_scope_tables_so_filtering_is_required() {
        let mut m = mysql().await;
        m.query_drop("CREATE TABLE mine(id INT PRIMARY KEY)").await.unwrap();
        let d = duck();
        let names: Vec<String> = {
            let mut stmt = d
                .prepare("SELECT table_name FROM information_schema.tables WHERE table_catalog='src'")
                .unwrap();
            let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
            rows.map(|r| r.unwrap()).collect()
        };
        let internal = names.iter().filter(|n| is_internal_table(n)).count();
        assert!(
            internal > 0,
            "SeekDB must expose internal tables through ATTACH, got {} names",
            names.len()
        );
        let kept: Vec<String> = names.iter().filter(|n| !is_internal_table(n)).cloned().collect();
        assert!(kept.iter().any(|n| n.eq_ignore_ascii_case("mine")));
        assert!(!kept.iter().any(|n| is_internal_table(n)));
        m.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    }
}
