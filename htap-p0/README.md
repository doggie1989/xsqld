# HTAP P0 原型 — xsqld + DuckDB 外部同步器

验证 xsqld HTAP 的核心一致性契约:**SQLite 为 OLTP 真相源,DuckDB 列存镜像服务分析;≤1s 延迟可接受,任何崩溃后最终必须一致(不丢/不重/不发散)。**

## 架构
```
xsqld(唯一写者)──写入 orders──> SQLite 触发器 ──同事务──> xsqld_changelog(outbox)
                                                              │ 只读
                                          syncer.py(独立进程)──┘
                                              │ 幂等 upsert + 水位同事务
                                              ▼
                                          DuckDB orders_native(列存镜像,可重建)
```
- 捕获:SQLite 触发器(I/U/D),在 xsqld 事务内触发 → 原子持久。
- 应用:syncer 读 changelog(seq>水位),净效果(批内同 pk 最后操作生效)+ staging 表 `ON CONFLICT` upsert,水位与数据同 DuckDB 事务落盘 → exactly-once。
- 校验:count+sum 对账;损坏则 CTAS 重建。
- P0 不写 SQLite 业务数据(xsqqld 唯一写者);裁剪 changelog 经 xsqld(`--prune`)。

## 文件
- `common.py` — 配置/连接/hrana/DuckDB 镜像初始化与重建
- `schema.sql` — changelog 表 + 3 个触发器
- `setup.py` — 生成 5M 订单 + 装触发器 + CTAS 物化(`--rows N`,`--skip-gen`)
- `syncer.py` — 同步器主循环(`--batch --interval --verify-every --duration --prune`)
- `verify.py` — 独立对账(syncer 停止时用;`--detail` 加按状态对比)
- `crash_test.py` — 崩溃矩阵(a/b/c/idem)
- `overhead.py` — 写入开销 + 内存测量

## 运行
```bash
python3 -m pip install --user duckdb
python3 setup.py --rows 5000000     # 建表+触发器+物化
python3 syncer.py &                 # 起同步器(看 METRICS:lag/pending/drift)
# 用 Go 压测器写:../xsqld-bench-linux -mode ord-write -workers 4 -host htap.db
python3 verify.py --detail          # 对账(需先停 syncer,DuckDB 单写锁)
python3 crash_test.py all           # 崩溃恢复矩阵
python3 overhead.py                 # 写入开销
```

## 实测结论(1GB Lightsail,xsqld 0.1.3,5M 订单)
- **一致性**:INSERT/UPDATE/DELETE 全正确镜像;崩溃矩阵 A(杀 syncer)/B(kill -9 xsqld,重启 0.1s,零丢行)/C(DuckDB 损坏重建)/幂等(全量重放 6.8 万条)全部 count+sum+水位精确一致。
- **延迟**:中等负载 lag <1s;持续重突发(>1200/s)下短暂超出后收敛(DuckDB 点 upsert ~1000/s 是此机极限)。
- **写入开销**:吞吐仅降 **9.9**(fsync 瓶颈,触发器同事务共享 fsync);WAL 体积放大(changelog ~150B/行)。
- **内存**:xsqld 18MB + syncer 90MB,1GB 共存无 OOM。

## 已知限制 / P1 方向
- DuckDB 单进程写锁:syncer 运行时外部无法另开连接(P1 内嵌同进程解决)。
- 高频点 upsert 吞吐有限 → P1 用 SeekDB 式**两层 delta+周期合并**(批量追加远快于 upsert)。
- 仅 rowid 表(触发器/preupdate hook 限制);WITHOUT ROWID/虚拟表不支持。
- P1 捕获换 preupdate hook(免触发器,但需 libclang 构建);路由用显式 `/v2/analytics` 端点。