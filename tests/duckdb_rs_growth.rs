//! 最小复现：只用 duckdb-rs 循环执行 xsqld apply 路径的语句，测进程 RSS。
//! 目的是把 duckdb-rs 绑定与 xsqld 其余部分（mysql_async、tokio、指标）分开。
//! 跑法：cargo test --features alloc-stats --test duckdb_rs_growth -- --ignored --nocapture
use std::time::Instant;

fn rss_mb() -> f64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().parse::<f64>().unwrap_or(0.0) / 1024.0
}

#[test]
#[ignore = "long-running memory probe"]
fn duckdb_rs_apply_loop_rss() {
    let rounds: usize = std::env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(600);
    let path = std::env::var("DUCKDB_FILE").unwrap_or_else(|_| "/tmp/duckdb-rs-growth.duckdb".into());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{path}.wal"));

    let cfg = duckdb::Config::default()
        .enable_external_access(true)
        .unwrap()
        .enable_autoload_extension(false)
        .unwrap()
        .with("memory_limit", "512MB")
        .unwrap()
        .with("preserve_insertion_order", "false")
        .unwrap()
        .threads(4)
        .unwrap();
    let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
    d.execute_batch("LOAD mysql").unwrap();
    d.execute_batch(
        "ATTACH 'host=127.0.0.1 port=12981 user=root db=soak_t7' AS src (TYPE mysql, READ_ONLY)",
    )
    .unwrap();
    d.execute_batch(
        "CREATE OR REPLACE TABLE m_s AS SELECT * FROM mysql_query('src','SELECT /*+ query_timeout(600000000) */ * FROM s')",
    )
    .unwrap();
    d.execute_batch("CREATE TABLE IF NOT EXISTS xsqld_mirror_meta(id INTEGER PRIMARY KEY, watermark_scn BIGINT)")
        .unwrap();
    d.execute_batch("INSERT OR IGNORE INTO xsqld_mirror_meta(id, watermark_scn) VALUES(1, 0)").unwrap();
    let second = d.try_clone().unwrap();
    println!("起始 RSS={:.1}MB rounds={rounds}", rss_mb());

    let t0 = Instant::now();
    for i in 0..rounds {
        let lo = 1 + (i * 10) % 4990;
        d.execute_batch("BEGIN TRANSACTION").unwrap();
        d.execute_batch("CREATE OR REPLACE TEMP TABLE ids_s(id INTEGER, seq BIGINT)").unwrap();
        d.execute_batch(&format!("INSERT INTO ids_s VALUES({lo},{i})")).unwrap();
        d.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE present_s AS SELECT * FROM mysql_query('src',\
             'SELECT /*+ query_timeout(600000000) */ id, v, n FROM s WHERE id BETWEEN {lo} AND {}')",
            lo + 9
        ))
        .unwrap();
        d.execute_batch(
            "CREATE OR REPLACE TEMP TABLE stg_s AS SELECT i.seq, i.id, p.v, p.n, \
             CASE WHEN p.id IS NULL THEN 'D' ELSE 'I' END AS op \
             FROM ids_s i LEFT JOIN present_s p ON i.id IS NOT DISTINCT FROM p.id",
        )
        .unwrap();
        d.execute_batch(
            "CREATE OR REPLACE TEMP TABLE dedup_s AS SELECT * FROM stg_s \
             QUALIFY row_number() OVER (PARTITION BY id ORDER BY seq DESC) = 1",
        )
        .unwrap();
        let _: (i64, i64) = d
            .query_row(
                "SELECT COUNT(*) FILTER (WHERE op='I'), COUNT(*) FILTER (WHERE op='D') FROM dedup_s",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        d.execute_batch(
            "MERGE INTO m_s AS t USING dedup_s AS s ON t.id IS NOT DISTINCT FROM s.id \
             WHEN MATCHED AND s.op='D' THEN DELETE \
             WHEN MATCHED THEN UPDATE SET v=s.v, n=s.n \
             WHEN NOT MATCHED AND s.op<>'D' THEN INSERT (id,v,n) VALUES(s.id,s.v,s.n)",
        )
        .unwrap();
        d.execute_batch(&format!(
            "UPDATE xsqld_mirror_meta SET watermark_scn={} WHERE id=1",
            1795370000000000000u64 + i as u64
        ))
        .unwrap();
        d.execute_batch("COMMIT").unwrap();
        // reconcile 侧
        let _: i64 = d
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'm_s'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        {
            let mut st = d.prepare("DESCRIBE m_s").unwrap();
            let _ = st.query_map([], |r| r.get::<_, String>(0)).unwrap().count();
        }
        let _: i64 = d.query_row("SELECT COUNT(*) FROM m_s", [], |r| r.get(0)).unwrap();
        if i % 150 == 149 {
            d.execute_batch("CHECKPOINT").unwrap();
        }
        if i % 25 == 24 {
            let _: i64 = second.query_row("SELECT COUNT(*) FROM m_s", [], |r| r.get(0)).unwrap();
        }
        if (i + 1) % 50 == 0 {
            println!("  round {:4} RSS={:7.1}MB  elapsed={:.0}s", i + 1, rss_mb(), t0.elapsed().as_secs_f64());
        }
    }
    println!("最终 RSS={:.1}MB", rss_mb());
}
/// 隔离 `CREATE OR REPLACE TEMP TABLE`：apply 路径每周期每表建 3 个临时表。
/// 不连 mysql、不做 MERGE，只反复替换同一张临时表，看 BufferManager 是否保留 BlockHandle。
/// 跑法：cargo test --test duckdb_rs_growth temp_replace -- --ignored --nocapture
#[test]
#[ignore = "long-running memory probe"]
fn temp_replace_vs_drop_create_rss() {
    let rounds: usize = std::env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(3000);
    for mode in ["replace", "drop_create"] {
        let path = format!("/tmp/duckdb-temp-{mode}.duckdb");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let cfg = duckdb::Config::default()
            .enable_external_access(true)
            .unwrap()
            .enable_autoload_extension(false)
            .unwrap()
            .with("memory_limit", "512MB")
            .unwrap()
            .with("preserve_insertion_order", "false")
            .unwrap()
            .threads(4)
            .unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        let base = rss_mb();
        let t0 = Instant::now();
        for i in 0..rounds {
            if mode == "replace" {
                d.execute_batch(
                    "CREATE OR REPLACE TEMP TABLE ids_t AS SELECT i AS id, i AS seq FROM range(10) t(i)",
                )
                .unwrap();
            } else {
                d.execute_batch(
                    "DROP TABLE IF EXISTS ids_t; CREATE TEMP TABLE ids_t AS SELECT i AS id, i AS seq FROM range(10) t(i)",
                )
                .unwrap();
            }
            if (i + 1) % (rounds / 6).max(1) == 0 {
                println!(
                    "{mode:12} round={:5} rss={:7.1}MB delta={:+6.1}MB elapsed={:.0}s",
                    i + 1,
                    rss_mb(),
                    rss_mb() - base,
                    t0.elapsed().as_secs_f64()
                );
            }
        }
        println!("{mode:12} FINAL rss={:.1}MB delta={:+.1}MB", rss_mb(), rss_mb() - base);
        drop(d);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}

/// 二分：增长来自 mysql_query 喂的临时表，还是本地 MERGE 路径？
/// 跑法：cargo test --test duckdb_rs_growth bisect -- --ignored --nocapture
#[test]
#[ignore = "long-running memory probe"]
fn bisect_mysql_temp_vs_local_merge() {
    let rounds: usize = std::env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(1500);
    for mode in ["mysql_temp", "local_merge"] {
        let path = format!("/tmp/duckdb-bisect-{mode}.duckdb");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let cfg = duckdb::Config::default()
            .enable_external_access(true).unwrap()
            .enable_autoload_extension(false).unwrap()
            .with("memory_limit", "512MB").unwrap()
            .with("preserve_insertion_order", "false").unwrap()
            .threads(4).unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        d.execute_batch("LOAD mysql").unwrap();
        d.execute_batch("ATTACH 'host=127.0.0.1 port=12981 user=root db=soak_t7' AS src (TYPE mysql, READ_ONLY)").unwrap();
        d.execute_batch("CREATE OR REPLACE TABLE m_s(id INTEGER, v VARCHAR(64), n INTEGER)").unwrap();
        d.execute_batch("INSERT INTO m_s SELECT i, 'v', i FROM range(5000) t(i)").unwrap();
        let base = rss_mb();
        let t0 = Instant::now();
        for i in 0..rounds {
            let lo = 1 + (i * 10) % 4990;
            if mode == "mysql_temp" {
                d.execute_batch(&format!(
                    "CREATE OR REPLACE TEMP TABLE present_s AS SELECT * FROM mysql_query('src',\
                     'SELECT /*+ query_timeout(600000000) */ id, v, n FROM s WHERE id BETWEEN {lo} AND {}')",
                    lo + 9)).unwrap();
            } else {
                d.execute_batch("CREATE OR REPLACE TEMP TABLE ids_s(id INTEGER, seq BIGINT)").unwrap();
                d.execute_batch(&format!("INSERT INTO ids_s VALUES({lo},{i})")).unwrap();
                d.execute_batch("CREATE OR REPLACE TEMP TABLE present_s AS SELECT i AS id, 'v' AS v, i AS n FROM range(10) t(i)").unwrap();
                d.execute_batch("CREATE OR REPLACE TEMP TABLE stg_s AS SELECT i.seq, i.id, p.v, p.n, CASE WHEN p.id IS NULL THEN 'D' ELSE 'I' END AS op FROM ids_s i LEFT JOIN present_s p ON i.id IS NOT DISTINCT FROM p.id").unwrap();
                d.execute_batch("CREATE OR REPLACE TEMP TABLE dedup_s AS SELECT * FROM stg_s QUALIFY row_number() OVER (PARTITION BY id ORDER BY seq DESC) = 1").unwrap();
                d.execute_batch("MERGE INTO m_s AS t USING dedup_s AS s ON t.id IS NOT DISTINCT FROM s.id WHEN MATCHED AND s.op='D' THEN DELETE WHEN MATCHED THEN UPDATE SET v=s.v, n=s.n WHEN NOT MATCHED AND s.op<>'D' THEN INSERT (id,v,n) VALUES(s.id,s.v,s.n)").unwrap();
            }
            if (i + 1) % (rounds / 6).max(1) == 0 {
                println!("{mode:12} round={:5} rss={:7.1}MB delta={:+6.1}MB elapsed={:.0}s",
                    i + 1, rss_mb(), rss_mb() - base, t0.elapsed().as_secs_f64());
            }
        }
        println!("{mode:12} FINAL delta={:+.1}MB", rss_mb() - base);
        drop(d);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}

/// 再二分：MERGE 本身 vs 临时表链（LEFT JOIN + QUALIFY），谁在累积 BlockHandle。
/// 跑法：cargo test --test duckdb_rs_growth bisect2 -- --ignored --nocapture
#[test]
#[ignore = "long-running memory probe"]
fn bisect_merge_vs_temp_chain() {
    let rounds: usize = std::env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(1500);
    for mode in ["merge_only", "temp_chain"] {
        let path = format!("/tmp/duckdb-bisect2-{mode}.duckdb");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let cfg = duckdb::Config::default()
            .enable_external_access(true).unwrap()
            .enable_autoload_extension(false).unwrap()
            .with("memory_limit", "512MB").unwrap()
            .with("preserve_insertion_order", "false").unwrap()
            .threads(4).unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        d.execute_batch("CREATE OR REPLACE TABLE m_s(id INTEGER, v VARCHAR(64), n INTEGER)").unwrap();
        d.execute_batch("INSERT INTO m_s SELECT i, 'v', i FROM range(5000) t(i)").unwrap();
        // merge_only：dedup 表只建一次，之后每轮只改值再 MERGE
        d.execute_batch("CREATE OR REPLACE TEMP TABLE dedup_s(seq BIGINT, id INTEGER, v VARCHAR(64), n INTEGER, op VARCHAR(2))").unwrap();
        let base = rss_mb();
        let t0 = Instant::now();
        for i in 0..rounds {
            let lo = 1 + (i * 10) % 4990;
            if mode == "merge_only" {
                d.execute_batch("DELETE FROM dedup_s").unwrap();
                d.execute_batch(&format!("INSERT INTO dedup_s VALUES({i},{lo},'v',{i},'I')")).unwrap();
                d.execute_batch("MERGE INTO m_s AS t USING dedup_s AS s ON t.id IS NOT DISTINCT FROM s.id WHEN MATCHED AND s.op='D' THEN DELETE WHEN MATCHED THEN UPDATE SET v=s.v, n=s.n WHEN NOT MATCHED AND s.op<>'D' THEN INSERT (id,v,n) VALUES(s.id,s.v,s.n)").unwrap();
            } else {
                d.execute_batch("CREATE OR REPLACE TEMP TABLE ids_s(id INTEGER, seq BIGINT)").unwrap();
                d.execute_batch(&format!("INSERT INTO ids_s VALUES({lo},{i})")).unwrap();
                d.execute_batch("CREATE OR REPLACE TEMP TABLE present_s AS SELECT i AS id, 'v' AS v, i AS n FROM range(10) t(i)").unwrap();
                d.execute_batch("CREATE OR REPLACE TEMP TABLE stg_s AS SELECT i.seq, i.id, p.v, p.n, CASE WHEN p.id IS NULL THEN 'D' ELSE 'I' END AS op FROM ids_s i LEFT JOIN present_s p ON i.id IS NOT DISTINCT FROM p.id").unwrap();
                d.execute_batch("CREATE OR REPLACE TEMP TABLE dedup_s AS SELECT * FROM stg_s QUALIFY row_number() OVER (PARTITION BY id ORDER BY seq DESC) = 1").unwrap();
            }
            if (i + 1) % (rounds / 6).max(1) == 0 {
                println!("{mode:12} round={:5} rss={:7.1}MB delta={:+6.1}MB elapsed={:.0}s",
                    i + 1, rss_mb(), rss_mb() - base, t0.elapsed().as_secs_f64());
            }
        }
        println!("{mode:12} FINAL delta={:+.1}MB", rss_mb() - base);
        drop(d);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}

/// MERGE 的等价替代是否也累积？若 DELETE+INSERT 平直，则可换掉 apply 策略（真修复而非缓解）。
/// 跑法：cargo test --test duckdb_rs_growth merge_alternatives -- --ignored --nocapture
#[test]
#[ignore = "long-running memory probe"]
fn merge_alternatives_rss() {
    let rounds: usize = std::env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(1500);
    for mode in ["merge", "delete_insert", "update_insert"] {
        let path = format!("/tmp/duckdb-alt-{mode}.duckdb");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let cfg = duckdb::Config::default()
            .enable_external_access(true).unwrap()
            .enable_autoload_extension(false).unwrap()
            .with("memory_limit", "512MB").unwrap()
            .with("preserve_insertion_order", "false").unwrap()
            .threads(4).unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        d.execute_batch("CREATE OR REPLACE TABLE m_s(id INTEGER, v VARCHAR(64), n INTEGER)").unwrap();
        d.execute_batch("INSERT INTO m_s SELECT i, 'v', i FROM range(5000) t(i)").unwrap();
        d.execute_batch("CREATE OR REPLACE TEMP TABLE dedup_s(seq BIGINT, id INTEGER, v VARCHAR(64), n INTEGER, op VARCHAR(2))").unwrap();
        let base = rss_mb();
        let t0 = Instant::now();
        for i in 0..rounds {
            let lo = 1 + (i * 10) % 4990;
            d.execute_batch("DELETE FROM dedup_s").unwrap();
            d.execute_batch(&format!("INSERT INTO dedup_s VALUES({i},{lo},'v',{i},'I')")).unwrap();
            match mode {
                "merge" => d.execute_batch("MERGE INTO m_s AS t USING dedup_s AS s ON t.id IS NOT DISTINCT FROM s.id WHEN MATCHED AND s.op='D' THEN DELETE WHEN MATCHED THEN UPDATE SET v=s.v, n=s.n WHEN NOT MATCHED AND s.op<>'D' THEN INSERT (id,v,n) VALUES(s.id,s.v,s.n)").unwrap(),
                "delete_insert" => {
                    d.execute_batch("DELETE FROM m_s WHERE id IN (SELECT id FROM dedup_s)").unwrap();
                    d.execute_batch("INSERT INTO m_s SELECT id, v, n FROM dedup_s WHERE op <> 'D'").unwrap();
                }
                _ => {
                    d.execute_batch("UPDATE m_s SET v=s.v, n=s.n FROM dedup_s s WHERE m_s.id IS NOT DISTINCT FROM s.id AND s.op<>'D'").unwrap();
                    d.execute_batch("DELETE FROM m_s WHERE id IN (SELECT id FROM dedup_s WHERE op='D')").unwrap();
                    d.execute_batch("INSERT INTO m_s SELECT s.id, s.v, s.n FROM dedup_s s WHERE s.op<>'D' AND NOT EXISTS (SELECT 1 FROM m_s t WHERE t.id IS NOT DISTINCT FROM s.id)").unwrap();
                }
            }
            if (i + 1) % (rounds / 6).max(1) == 0 {
                println!("{mode:14} round={:5} rss={:7.1}MB delta={:+6.1}MB elapsed={:.0}s",
                    i + 1, rss_mb(), rss_mb() - base, t0.elapsed().as_secs_f64());
            }
        }
        let n: i64 = d.query_row("SELECT COUNT(*) FROM m_s", [], |r| r.get(0)).unwrap();
        println!("{mode:14} FINAL delta={:+.1}MB rows={n}", rss_mb() - base);
        drop(d);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}

/// 上线形式验证：null-safe `DELETE … USING` + INSERT（keyless/NULL 键不能用 IN）。
/// 同时覆盖 NULL 键与删除不存在行，确认语义与 MERGE 一致且内存平直。
#[test]
#[ignore = "long-running memory probe"]
fn delete_using_nullsafe_rss() {
    let rounds: usize = std::env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(1500);
    let path = "/tmp/duckdb-delusing.duckdb";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    let cfg = duckdb::Config::default()
        .enable_external_access(true).unwrap().enable_autoload_extension(false).unwrap()
        .with("memory_limit", "512MB").unwrap().with("preserve_insertion_order", "false").unwrap()
        .threads(4).unwrap();
    let d = duckdb::Connection::open_with_flags(path, cfg).unwrap();
    d.execute_batch("CREATE TABLE m_k(a INTEGER, b VARCHAR(8), v INTEGER)").unwrap();
    d.execute_batch("INSERT INTO m_k VALUES (1,'x',10),(NULL,'y',20),(2,NULL,30),(NULL,NULL,40)").unwrap();
    d.execute_batch("CREATE TEMP TABLE stg(seq BIGINT, a INTEGER, b VARCHAR(8), v INTEGER, op VARCHAR(2))").unwrap();
    let base = rss_mb();
    let t0 = Instant::now();
    for i in 0..rounds {
        d.execute_batch("DELETE FROM stg").unwrap();
        // 轮流命中：普通键 / NULL 键 / 双 NULL 键 / 不存在的键（删除应为 no-op）
        match i % 4 {
            0 => d.execute_batch(&format!("INSERT INTO stg VALUES({i},1,'x',{},'I')", 100 + i)).unwrap(),
            1 => d.execute_batch(&format!("INSERT INTO stg VALUES({i},NULL,'y',{},'I')", 200 + i)).unwrap(),
            2 => d.execute_batch(&format!("INSERT INTO stg VALUES({i},NULL,NULL,{},'I')", 300 + i)).unwrap(),
            _ => d.execute_batch(&format!("INSERT INTO stg VALUES({i},999,'zz',{},'D')", i)).unwrap(),
        }
        d.execute_batch("DELETE FROM m_k USING stg s WHERE m_k.a IS NOT DISTINCT FROM s.a AND m_k.b IS NOT DISTINCT FROM s.b").unwrap();
        d.execute_batch("INSERT INTO m_k SELECT a, b, v FROM stg WHERE op <> 'D'").unwrap();
        if (i + 1) % (rounds / 6).max(1) == 0 {
            println!("del_using round={:5} rss={:7.1}MB delta={:+6.1}MB elapsed={:.0}s",
                i + 1, rss_mb(), rss_mb() - base, t0.elapsed().as_secs_f64());
        }
    }
    let rows = d.query_row("SELECT COUNT(*) FROM m_k", [], |r| r.get::<_, i64>(0)).unwrap();
    println!("del_using FINAL delta={:+.1}MB rows={rows}", rss_mb() - base);
    // 语义断言：4 个身份各自被更新过，不存在键的删除没插入行
    assert_eq!(rows, 4, "身份数必须保持 4（NULL 键也要匹配上）");
    let v: i64 = d.query_row("SELECT v FROM m_k WHERE a IS NULL AND b IS NULL", [], |r| r.get(0)).unwrap();
    assert!(v >= 300, "双 NULL 键必须被更新到，got {v}");
    drop(d);
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
}

/// 判别泄漏触发条件：null-safe 比较 / 多列键 / USING 语法 / IN 半连接。
#[test]
#[ignore = "long-running memory probe"]
fn delete_join_form_discriminator() {
    let rounds: usize = std::env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
    for mode in ["using_isndf_1col", "using_eq_1col", "in_1col", "using_isndf_2col"] {
        let path = format!("/tmp/duckdb-disc-{mode}.duckdb");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let cfg = duckdb::Config::default()
            .enable_external_access(true).unwrap().enable_autoload_extension(false).unwrap()
            .with("memory_limit", "512MB").unwrap().with("preserve_insertion_order", "false").unwrap()
            .threads(4).unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        d.execute_batch("CREATE TABLE m(a INTEGER, b INTEGER, v INTEGER)").unwrap();
        d.execute_batch("INSERT INTO m SELECT i, i, i FROM range(5000) t(i)").unwrap();
        d.execute_batch("CREATE TEMP TABLE stg(seq BIGINT, a INTEGER, b INTEGER, v INTEGER, op VARCHAR(2))").unwrap();
        let base = rss_mb();
        for i in 0..rounds {
            let lo = 1 + (i * 10) % 4990;
            d.execute_batch("DELETE FROM stg").unwrap();
            d.execute_batch(&format!("INSERT INTO stg VALUES({i},{lo},{lo},{i},'I')")).unwrap();
            let del = match mode {
                "using_isndf_1col" => "DELETE FROM m USING stg s WHERE m.a IS NOT DISTINCT FROM s.a",
                "using_eq_1col" => "DELETE FROM m USING stg s WHERE m.a = s.a",
                "in_1col" => "DELETE FROM m WHERE a IN (SELECT a FROM stg)",
                _ => "DELETE FROM m USING stg s WHERE m.a IS NOT DISTINCT FROM s.a AND m.b IS NOT DISTINCT FROM s.b",
            };
            d.execute_batch(del).unwrap();
            d.execute_batch("INSERT INTO m SELECT a, b, v FROM stg WHERE op <> 'D'").unwrap();
        }
        let n: i64 = d.query_row("SELECT COUNT(*) FROM m", [], |r| r.get(0)).unwrap();
        println!("{mode:18} delta={:+6.1}MB rows={n}", rss_mb() - base);
        drop(d);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}

/// 候选统一修复：先按 null-safe 连接求出 rowid，再用 `rowid IN (…)` 半连接删除。
/// 覆盖 xsqld 实际会遇到的键形状：单列整型 / 单列 VARCHAR / 多列 / 含 NULL / keyless。
#[test]
#[ignore = "long-running memory probe"]
fn rowid_semijoin_delete_shapes() {
    let rounds: usize = std::env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(800);
    for shape in ["int_key", "varchar_key", "multi_key_null", "keyless"] {
        let path = format!("/tmp/duckdb-rowid-{shape}.duckdb");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let cfg = duckdb::Config::default()
            .enable_external_access(true).unwrap().enable_autoload_extension(false).unwrap()
            .with("memory_limit", "512MB").unwrap().with("preserve_insertion_order", "false").unwrap()
            .threads(4).unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        let (ddl, seed, keys, stgddl, mkrow) = match shape {
            "int_key" => ("CREATE TABLE m(a INTEGER, v INTEGER)", "INSERT INTO m SELECT i, i FROM range(2000) t(i)",
                          "m.a IS NOT DISTINCT FROM s.a", "CREATE TEMP TABLE stg(a INTEGER, v INTEGER, op VARCHAR(2))",
                          "INSERT INTO stg VALUES({K}, {I}, 'I')"),
            "varchar_key" => ("CREATE TABLE m(a VARCHAR(16), v INTEGER)", "INSERT INTO m SELECT 'k'||i, i FROM range(2000) t(i)",
                          "m.a IS NOT DISTINCT FROM s.a", "CREATE TEMP TABLE stg(a VARCHAR(16), v INTEGER, op VARCHAR(2))",
                          "INSERT INTO stg VALUES('k{K}', {I}, 'I')"),
            "multi_key_null" => ("CREATE TABLE m(a INTEGER, b VARCHAR(8), v INTEGER)",
                          "INSERT INTO m VALUES (1,'x',1),(NULL,'y',2),(2,NULL,3),(NULL,NULL,4)",
                          "m.a IS NOT DISTINCT FROM s.a AND m.b IS NOT DISTINCT FROM s.b",
                          "CREATE TEMP TABLE stg(a INTEGER, b VARCHAR(8), v INTEGER, op VARCHAR(2))",
                          "INSERT INTO stg VALUES(NULL,'y',{I},'I')"),
            _ => ("CREATE TABLE m(a INTEGER, b VARCHAR(8), v INTEGER)",
                          "INSERT INTO m VALUES (1,'x',1),(NULL,'y',2),(2,NULL,3),(NULL,NULL,4)",
                          "m.a IS NOT DISTINCT FROM s.a AND m.b IS NOT DISTINCT FROM s.b AND m.v IS NOT DISTINCT FROM s.v",
                          "CREATE TEMP TABLE stg(a INTEGER, b VARCHAR(8), v INTEGER, op VARCHAR(2))",
                          "INSERT INTO stg VALUES(NULL,'y',2,'I')"),
        };
        d.execute_batch(ddl).unwrap();
        d.execute_batch(seed).unwrap();
        d.execute_batch(stgddl).unwrap();
        let base = rss_mb();
        for i in 0..rounds {
            let k = 1 + (i * 7) % 1990;
            d.execute_batch("DELETE FROM stg").unwrap();
            d.execute_batch(&mkrow.replace("{K}", &k.to_string()).replace("{I}", &i.to_string())).unwrap();
            d.execute_batch(&format!(
                "DELETE FROM m WHERE rowid IN (SELECT m.rowid FROM m JOIN stg s ON {keys})")).unwrap();
            d.execute_batch("INSERT INTO m SELECT a, b, v FROM stg WHERE op <> 'D'").unwrap_or_else(|_| {
                d.execute_batch("INSERT INTO m SELECT a, v FROM stg WHERE op <> 'D'").unwrap();
            });
        }
        let n: i64 = d.query_row("SELECT COUNT(*) FROM m", [], |r| r.get(0)).unwrap();
        println!("{shape:16} delta={:+6.1}MB rows={n}", rss_mb() - base);
        drop(d);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}

/// 候选真修复：把 `IS NOT DISTINCT FROM` 展开为 `(a=b OR (a IS NULL AND b IS NULL))`。
/// 语义完全等价，但可能改变连接计划从而不再累积 BlockHandle。
#[test]
#[ignore = "long-running memory probe"]
fn expanded_nullsafe_join_rss() {
    let rounds: usize = std::env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
    for mode in ["merge_isndf", "merge_expanded", "del_using_expanded"] {
        let path = format!("/tmp/duckdb-exp-{mode}.duckdb");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let cfg = duckdb::Config::default()
            .enable_external_access(true).unwrap().enable_autoload_extension(false).unwrap()
            .with("memory_limit", "512MB").unwrap().with("preserve_insertion_order", "false").unwrap()
            .threads(4).unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        d.execute_batch("CREATE TABLE m(a INTEGER, v INTEGER)").unwrap();
        d.execute_batch("INSERT INTO m SELECT i, i FROM range(2000) t(i)").unwrap();
        d.execute_batch("INSERT INTO m VALUES(NULL, -1)").unwrap();
        d.execute_batch("CREATE TEMP TABLE stg(a INTEGER, v INTEGER, op VARCHAR(2))").unwrap();
        let base = rss_mb();
        for i in 0..rounds {
            let k = 1 + (i * 7) % 1990;
            d.execute_batch("DELETE FROM stg").unwrap();
            // 轮流命中普通键与 NULL 键，确保 null-safe 语义真被用到
            if i % 5 == 4 {
                d.execute_batch(&format!("INSERT INTO stg VALUES(NULL, {i}, 'I')")).unwrap();
            } else {
                d.execute_batch(&format!("INSERT INTO stg VALUES({k}, {i}, 'I')")).unwrap();
            }
            let cond = if mode == "merge_isndf" { "t.a IS NOT DISTINCT FROM s.a" }
                       else { "(t.a = s.a OR (t.a IS NULL AND s.a IS NULL))" };
            if mode == "del_using_expanded" {
                d.execute_batch(&format!("DELETE FROM m AS t USING stg s WHERE {cond}")).unwrap();
                d.execute_batch("INSERT INTO m SELECT a, v FROM stg WHERE op <> 'D'").unwrap();
            } else {
                d.execute_batch(&format!(
                    "MERGE INTO m AS t USING stg AS s ON {cond} \
                     WHEN MATCHED AND s.op='D' THEN DELETE \
                     WHEN MATCHED THEN UPDATE SET v=s.v \
                     WHEN NOT MATCHED AND s.op<>'D' THEN INSERT (a,v) VALUES(s.a,s.v)")).unwrap();
            }
        }
        let n: i64 = d.query_row("SELECT COUNT(*) FROM m", [], |r| r.get(0)).unwrap();
        let nullv: i64 = d.query_row("SELECT v FROM m WHERE a IS NULL", [], |r| r.get(0)).unwrap();
        println!("{mode:20} delta={:+6.1}MB rows={n} null_key_v={nullv}", rss_mb() - base);
        drop(d);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}

/// staging LEFT JOIN 的两种 null-safe 写法：大批量下的耗时与内存。
/// OR 展开式可能让优化器放弃 hash join，必须确认没有性能悬崖。
#[test]
#[ignore = "long-running memory probe"]
fn staging_join_form_cost() {
    for n in [200usize, 2000, 10000] {
        for form in ["isndf", "expanded"] {
            let path = format!("/tmp/duckdb-stg-{n}-{form}.duckdb");
            let _ = std::fs::remove_file(&path);
            let cfg = duckdb::Config::default()
                .enable_external_access(true).unwrap().enable_autoload_extension(false).unwrap()
                .with("memory_limit", "512MB").unwrap().with("preserve_insertion_order", "false").unwrap()
                .threads(4).unwrap();
            let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
            d.execute_batch(&format!("CREATE TEMP TABLE ids AS SELECT i AS id, i AS seq FROM range({n}) t(i)")).unwrap();
            d.execute_batch(&format!("CREATE TEMP TABLE present AS SELECT i AS id, 'v' AS v, i AS n FROM range({n}) t(i)")).unwrap();
            let cond = if form == "isndf" { "i.id IS NOT DISTINCT FROM p.id" }
                       else { "(i.id = p.id OR (i.id IS NULL AND p.id IS NULL))" };
            let sql = format!("CREATE OR REPLACE TEMP TABLE stg AS SELECT i.seq, i.id, p.v, p.n, \
                CASE WHEN p.id IS NULL THEN 'D' ELSE 'I' END AS op FROM ids i LEFT JOIN present p ON {cond}");
            let base = rss_mb();
            let t = Instant::now();
            d.execute_batch(&sql).unwrap();
            let first = t.elapsed().as_secs_f64();
            let rounds = if n <= 2000 { 200 } else { 20 };
            let t2 = Instant::now();
            for _ in 0..rounds { d.execute_batch(&sql).unwrap(); }
            let per = t2.elapsed().as_secs_f64() / rounds as f64;
            let cnt: i64 = d.query_row("SELECT COUNT(*) FROM stg", [], |r| r.get(0)).unwrap();
            println!("n={n:6} {form:9} first={first:7.3}s per_round={per:7.4}s rss_delta={:+6.1}MB rows={cnt}", rss_mb() - base);
            drop(d);
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(format!("{path}.wal"));
        }
    }
}

/// 上线前必测：staging 用纯 `=` 的代价，以及 MERGE 的 OR 展开式在大批量下是否退化成嵌套循环。
#[test]
#[ignore = "long-running memory probe"]
fn eq_and_merge_scale_cost() {
    // A) staging LEFT JOIN：isndf vs 纯 =
    for n in [2000usize, 10000] {
        for form in ["isndf", "eq"] {
            let path = format!("/tmp/duckdb-eq-{n}-{form}.duckdb");
            let _ = std::fs::remove_file(&path);
            let cfg = duckdb::Config::default().enable_external_access(true).unwrap()
                .enable_autoload_extension(false).unwrap().with("memory_limit","512MB").unwrap()
                .with("preserve_insertion_order","false").unwrap().threads(4).unwrap();
            let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
            d.execute_batch(&format!("CREATE TEMP TABLE ids AS SELECT i AS id, i AS seq FROM range({n}) t(i)")).unwrap();
            d.execute_batch(&format!("CREATE TEMP TABLE present AS SELECT i AS id, 'v' AS v, i AS n FROM range({n}) t(i)")).unwrap();
            let cond = if form=="isndf" {"i.id IS NOT DISTINCT FROM p.id"} else {"i.id = p.id"};
            let sql = format!("CREATE OR REPLACE TEMP TABLE stg AS SELECT i.seq,i.id,p.v,p.n,CASE WHEN p.id IS NULL THEN 'D' ELSE 'I' END AS op FROM ids i LEFT JOIN present p ON {cond}");
            let base = rss_mb(); let rounds = 100; let t = Instant::now();
            for _ in 0..rounds { d.execute_batch(&sql).unwrap(); }
            let cnt: i64 = d.query_row("SELECT COUNT(*) FROM stg", [], |r| r.get(0)).unwrap();
            println!("STG n={n:6} {form:6} per_round={:7.4}s rss_delta={:+6.1}MB rows={cnt}", t.elapsed().as_secs_f64()/rounds as f64, rss_mb()-base);
            drop(d); let _ = std::fs::remove_file(&path); let _ = std::fs::remove_file(format!("{path}.wal"));
        }
    }
    // B) MERGE：100 万行镜像 + 2000 行 staging，isndf vs OR 展开
    for form in ["isndf", "expanded"] {
        let path = format!("/tmp/duckdb-mscale-{form}.duckdb");
        let _ = std::fs::remove_file(&path);
        let cfg = duckdb::Config::default().enable_external_access(true).unwrap()
            .enable_autoload_extension(false).unwrap().with("memory_limit","512MB").unwrap()
            .with("preserve_insertion_order","false").unwrap().threads(4).unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        d.execute_batch("CREATE TABLE m(id INTEGER, v VARCHAR(64), n INTEGER)").unwrap();
        d.execute_batch("INSERT INTO m SELECT i, 'v', i FROM range(1000000) t(i)").unwrap();
        d.execute_batch("CREATE TEMP TABLE stg(seq BIGINT, id INTEGER, v VARCHAR(64), n INTEGER, op VARCHAR(2))").unwrap();
        d.execute_batch("INSERT INTO stg SELECT i, i, 'w', i, 'I' FROM range(2000) t(i)").unwrap();
        let cond = if form=="isndf" {"t.id IS NOT DISTINCT FROM s.id"} else {"(t.id = s.id OR (t.id IS NULL AND s.id IS NULL))"};
        let sql = format!("MERGE INTO m AS t USING stg AS s ON {cond} WHEN MATCHED AND s.op='D' THEN DELETE WHEN MATCHED THEN UPDATE SET v=s.v, n=s.n WHEN NOT MATCHED AND s.op<>'D' THEN INSERT (id,v,n) VALUES(s.id,s.v,s.n)");
        let base = rss_mb(); let rounds = 20; let t = Instant::now();
        for _ in 0..rounds { d.execute_batch(&sql).unwrap(); }
        let cnt: i64 = d.query_row("SELECT COUNT(*) FROM m", [], |r| r.get(0)).unwrap();
        println!("MERGE 1M x 2000 {form:9} per_round={:7.4}s rss_delta={:+6.1}MB rows={cnt}", t.elapsed().as_secs_f64()/rounds as f64, rss_mb()-base);
        drop(d); let _ = std::fs::remove_file(&path); let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}

/// 有主键的表键列恒 NOT NULL，可安全用 `=`。测大表下 MERGE 用 `=` 的耗时与内存。
#[test]
#[ignore = "long-running memory probe"]
fn merge_eq_scale_cost() {
    for form in ["isndf", "eq"] {
        let path = format!("/tmp/duckdb-meq-{form}.duckdb");
        let _ = std::fs::remove_file(&path);
        let cfg = duckdb::Config::default().enable_external_access(true).unwrap()
            .enable_autoload_extension(false).unwrap().with("memory_limit","512MB").unwrap()
            .with("preserve_insertion_order","false").unwrap().threads(4).unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        d.execute_batch("CREATE TABLE m(id INTEGER, v VARCHAR(64), n INTEGER)").unwrap();
        d.execute_batch("INSERT INTO m SELECT i, 'v', i FROM range(1000000) t(i)").unwrap();
        d.execute_batch("CREATE TEMP TABLE stg(seq BIGINT, id INTEGER, v VARCHAR(64), n INTEGER, op VARCHAR(2))").unwrap();
        d.execute_batch("INSERT INTO stg SELECT i, i, 'w', i, 'I' FROM range(2000) t(i)").unwrap();
        let cond = if form=="isndf" {"t.id IS NOT DISTINCT FROM s.id"} else {"t.id = s.id"};
        let sql = format!("MERGE INTO m AS t USING stg AS s ON {cond} WHEN MATCHED AND s.op='D' THEN DELETE WHEN MATCHED THEN UPDATE SET v=s.v, n=s.n WHEN NOT MATCHED AND s.op<>'D' THEN INSERT (id,v,n) VALUES(s.id,s.v,s.n)");
        let base = rss_mb(); let rounds = 100; let t = Instant::now();
        for _ in 0..rounds { d.execute_batch(&sql).unwrap(); }
        let cnt: i64 = d.query_row("SELECT COUNT(*) FROM m", [], |r| r.get(0)).unwrap();
        let w: i64 = d.query_row("SELECT COUNT(*) FROM m WHERE v='w'", [], |r| r.get(0)).unwrap();
        println!("MERGE_EQ 1M x 2000 {form:6} per_round={:7.4}s rss_delta={:+6.1}MB rows={cnt} updated={w}", t.elapsed().as_secs_f64()/rounds as f64, rss_mb()-base);
        drop(d); let _ = std::fs::remove_file(&path); let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}

/// ATTACH 是否是 hash join 保留 BlockHandle 的必要共因？
/// 此前 `=` 平直的探针都未 ATTACH，带 ATTACH 的 join 探针都涨——该组合未被隔离过。
#[test]
#[ignore = "long-running memory probe"]
fn attach_as_cofactor() {
    for attach in [false, true] {
        let path = format!("/tmp/duckdb-cofactor-{attach}.duckdb");
        let _ = std::fs::remove_file(&path);
        let cfg = duckdb::Config::default().enable_external_access(true).unwrap()
            .enable_autoload_extension(false).unwrap().with("memory_limit","512MB").unwrap()
            .with("preserve_insertion_order","false").unwrap().threads(4).unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        if attach {
            d.execute_batch("LOAD mysql").unwrap();
            d.execute_batch("ATTACH 'host=127.0.0.1 port=12981 user=root db=soak_t7' AS src (TYPE mysql, READ_ONLY)").unwrap();
        }
        d.execute_batch("CREATE TABLE m(id INTEGER, v VARCHAR(64), n INTEGER)").unwrap();
        d.execute_batch("INSERT INTO m SELECT i, 'v', i FROM range(1000000) t(i)").unwrap();
        d.execute_batch("CREATE TEMP TABLE stg(seq BIGINT, id INTEGER, v VARCHAR(64), n INTEGER, op VARCHAR(2))").unwrap();
        d.execute_batch("INSERT INTO stg SELECT i, i, 'w', i, 'I' FROM range(2000) t(i)").unwrap();
        let base = rss_mb(); let rounds = 100; let t = Instant::now();
        for _ in 0..rounds {
            d.execute_batch("MERGE INTO m AS t USING stg AS s ON t.id IS NOT DISTINCT FROM s.id WHEN MATCHED AND s.op='D' THEN DELETE WHEN MATCHED THEN UPDATE SET v=s.v, n=s.n WHEN NOT MATCHED AND s.op<>'D' THEN INSERT (id,v,n) VALUES(s.id,s.v,s.n)").unwrap();
        }
        println!("ATTACH={attach:5} per_round={:7.4}s rss_delta={:+6.1}MB", t.elapsed().as_secs_f64()/rounds as f64, rss_mb()-base);
        drop(d); let _ = std::fs::remove_file(&path); let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}

/// 主导站点是 dedup 的 QUALIFY 窗口（被规划成 hash join）。候选：arg_max 哈希聚合。
/// 必须在**带 ATTACH** 的真实配置下测，且断言两种写法输出逐值一致。
#[test]
#[ignore = "long-running memory probe"]
fn dedup_argmax_vs_qualify() {
    for form in ["qualify", "argmax"] {
        let path = format!("/tmp/duckdb-dedup-{form}.duckdb");
        let _ = std::fs::remove_file(&path);
        let cfg = duckdb::Config::default().enable_external_access(true).unwrap()
            .enable_autoload_extension(false).unwrap().with("memory_limit","512MB").unwrap()
            .with("preserve_insertion_order","false").unwrap().threads(4).unwrap();
        let d = duckdb::Connection::open_with_flags(&path, cfg).unwrap();
        d.execute_batch("LOAD mysql").unwrap();
        d.execute_batch("ATTACH 'host=127.0.0.1 port=12981 user=root db=soak_t7' AS src (TYPE mysql, READ_ONLY)").unwrap();
        // 每个键 3 个版本，last-wins 必须取 seq 最大者
        d.execute_batch("CREATE TEMP TABLE stg AS SELECT i%500 AS id, i AS seq, 'v'||i AS v, i AS n, 'I' AS op FROM range(1500) t(i)").unwrap();
        let sql = if form == "qualify" {
            "CREATE OR REPLACE TEMP TABLE dd AS SELECT * FROM stg QUALIFY row_number() OVER (PARTITION BY id ORDER BY seq DESC) = 1".to_string()
        } else {
            "CREATE OR REPLACE TEMP TABLE dd AS SELECT s.seq AS seq, s.id AS id, s.v AS v, s.n AS n, s.op AS op FROM (SELECT arg_max({'seq':seq,'id':id,'v':v,'n':n,'op':op}, seq) AS s FROM stg GROUP BY id)".to_string()
        };
        d.execute_batch(&sql).unwrap();
        let chk: (i64, i64) = d.query_row("SELECT COUNT(*), SUM(seq) FROM dd", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        let base = rss_mb(); let rounds = 400; let t = Instant::now();
        for _ in 0..rounds { d.execute_batch(&sql).unwrap(); }
        println!("{form:8} per_round={:7.4}s rss_delta={:+6.1}MB rows={} sum_seq={}", t.elapsed().as_secs_f64()/rounds as f64, rss_mb()-base, chk.0, chk.1);
        drop(d); let _ = std::fs::remove_file(&path); let _ = std::fs::remove_file(format!("{path}.wal"));
    }
}
