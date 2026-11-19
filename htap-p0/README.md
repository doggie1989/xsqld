# Rust 单进程 HTAP

SQLite 仍是业务数据的唯一真相源，DuckDB 是可重建的分析镜像。同步和分析均内嵌于 `xsqld`，不需要 Python、独立同步器、DuckDB 扩展或外部压测程序。

## 启动与使用

在仓库目录中使用 Rust 1.99.1 编译并启动：

```bash
cargo build --release --locked
./target/release/xsqld --db-path ./data.sqld
```

创建一个命名空间并写入业务表（默认同步所有表，下面只是示例）：

```bash
curl -sS -X POST http://127.0.0.1:8081/v1/namespaces/htap/create \
  -H 'Content-Type: application/json' -d '{}'
curl -sS http://127.0.0.1:8080/v2/pipeline -H 'Host: htap.db' \
  -H 'Content-Type: application/json' \
  -d '{"requests":[{"type":"execute","stmt":{"sql":"CREATE TABLE orders(id INTEGER PRIMARY KEY,user_id INTEGER NOT NULL,amount_cents INTEGER NOT NULL,status INTEGER NOT NULL,channel INTEGER NOT NULL,created_at INTEGER NOT NULL)"}},{"type":"execute","stmt":{"sql":"INSERT INTO orders VALUES(1,10,1250,1,2,1763000000)"}},{"type":"close"}]}'
curl -sS http://127.0.0.1:8080/v2/analytics -H 'Host: htap.db' \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT status,COUNT(*) AS n,SUM(amount_cents) AS total FROM orders_native GROUP BY status","args":[]}'
```

第一次分析请求自动安装捕获、生成同目录下的 `data.duckdb` 镜像并启动内置 Rust 工作线程。之后继续通过原有 `/v2/pipeline` 写入，通过 `/v2/analytics` 分析；不需要另起进程。位置参数使用 Hrana 值编码，例如 `{"sql":"SELECT * FROM orders_native WHERE user_id=?","args":[{"type":"integer","value":"10"}]}`。

## 一致性与生命周期

- SQLite 触发器在业务事务内写持久化 outbox；回滚不会产生可同步的变更。
- 初次全量数据和水位来自同一个 SQLite 读事务，不会跳过快照期间的并发写入。
- 提交通知唤醒 Rust 线程；原生 DuckDB Appender 批量装载、批内按主键取最后操作，再集合式应用更新和删除。
- 每批覆盖一个完整的已提交 SQLite 快照，不拆开源事务；镜像数据和水位在同一个 DuckDB 事务提交。
- 只有 DuckDB 提交成功后，原有 SQLite 写线程才裁剪已应用日志并持久化裁剪水位。
- 分析先等待请求开始时观察到的源水位，再从同一个 DuckDB 只读事务返回结果和 `watermark`；同步和多条分析查询可以共存。
- 重启后首次打开已启用捕获的命名空间时自动恢复工作线程并复用持久化水位；镜像缺失、来源变化或水位落后于已裁日志时全量重建。删除命名空间会先关闭工作线程与分析连接，再删除对应文件。

## 同步范围

默认同步命名空间里的**所有普通表**，包括捕获安装之后新建的表：

- 每张表 `t` 镜像为视图 `t_native`，底层是按主键（无主键则 rowid）组织的列存表；支持复合主键与 `WITHOUT ROWID`。
- 镜像列统一用 DuckDB `VARIANT`，因此 SQLite 的动态类型被逐值保留：同一列里的整数、浮点、文本、BLOB、NULL 各自按原类型返回。
- `CREATE TABLE` / `ALTER TABLE` / `DROP TABLE` 在该语句自己的事务内刷新捕获并轮换源代号，镜像随之全量重建，不会漏同步；重命名后的表按新名镜像，删除的表视图消失。
- 虚拟表不同步（SQLite 不允许在虚拟表上建触发器），`sqlite_` 系统表与虚拟表影子表也排除在外。
- 不改主键的 UPDATE 只写一行 outbox；改主键时才额外写一行旧键删除。

## 边界

不能直接修改内部日志、元数据、日志序列或捕获触发器（`xsqld_capture_<hash>_{i,u,d}`），也不能在内部表上安装客户端触发器。客户端执行 `ALTER TABLE` / `DROP TABLE` 时，SQLite 自身对捕获触发器和 `sqlite_sequence` 的维护只在该语句执行期间放行，语句结束即收回。

分析只接受单条 SELECT/CTE，不开放多语句、写操作、外部文件/网络读取或扩展自动加载；沿用 HTTP 请求超时和响应大小限制。镜像列按值本身的 SQLite 类型返回；DuckDB 自身产生的 DECIMAL 与超出 i64 的整数用文本返回，时间、列表等其他 DuckDB 专属类型请显式 CAST 为 VARCHAR。

DuckDB 每个活跃命名空间的托管内存上限由 `--htap-memory-limit-mb` 配置（默认 512 MiB），内置 worker 空闲 `--htap-worker-idle-ms` 后释放共享池线程（默认 10 秒）；DuckDB 线程数不超过 `--parallel-max-ranges`。内存配置不等于进程 RSS 硬限制。底层文件损坏会报告错误，不会自动删除数据库文件。

## 测试

```bash
cargo test --locked
cargo test --locked htap::tests
```

Rust 测试覆盖快照/水位竞争、全字段镜像、主键变更、回滚、重启续传、并发只读查询、危险语句拒绝及命名空间删除重建，以及全表同步：运行时建表/加列/改名/删表、复合主键与 `WITHOUT ROWID`、混合类型列、AUTOINCREMENT 表的重命名与删除。`results/` 保留的是旧 Python P0 的历史记录，不代表当前 Rust 实现的性能或验收结果。
