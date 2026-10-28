# 架构与实现

本文说明 mylox 内部怎么工作：表结构从哪里来、DDL 怎么演进、多 pipeline 怎么调度、
事务与位点怎么原子提交。想先跑起来可以看 [README](../README.md)，
handler 怎么写看 [用法](usage.md)，配置项见 [配置](configuration.md)。

## 1. Schema 混合获取机制

### 1.1 初始 schema
启动时对从库执行**一次** `information_schema` 查询（`COLUMNS` + `STATISTICS`），
得到全部被过滤命中的表结构，写入 `mylox_schema_history`（`source = bootstrap`），
同时装入内存时间线。锚点位上重复启动不会重复写入：会先比对
`load_latest_schemas()`，结构相同则跳过。

### 1.2 DDL 演进（主路径：本地 sqlglot）
binlog 的 `QueryEvent` 里出现 DDL 时：

```
SQL ──SqlglotDDLParser──▶ DDLPlan ──SchemaManager──▶ SchemaRegistry.put(version@pos)
                                          │
                                          └──▶ mylox_schema_history (source = ddl_sqlglot)
```

* 支持 `CREATE TABLE`（含内联/表级主键、`AFTER`/`FIRST` 列顺序）、
  `ALTER TABLE ADD/DROP/MODIFY/CHANGE/RENAME COLUMN`、`ADD/DROP PRIMARY KEY`、
  `RENAME TABLE`、`RENAME COLUMN`、`DROP TABLE db.a, db.b`、`DROP DATABASE`、
  `CREATE TABLE ... LIKE`；
* 索引/唯一键/外键/`ENGINE=`/`ALGORITHM=`/`LOCK=`/`CONVERT TO CHARACTER SET`
  等**不影响列结构**的语句记为 ignored，不写历史、不查库；
* 列顺序严格按 MySQL 语义维护——`binlog_row_metadata` 非 FULL 时行数据是按下标映射的。

### 1.3 DDL 兜底（回退查询一次 information_schema）
以下情况走兜底：sqlglot 抛异常、语句被解析成 `Command`（如 `CREATE TABLE ... PARTITION BY`）、
`CREATE TABLE ... AS SELECT`、`ALTER` 出现无法识别的子句、列在模型里找不到（模型漂移）等。
兜底只对**受影响的表**发一次 `information_schema` 查询，并把结果写入历史
（`source = ddl_fallback`）。若该位点早于 bootstrap 快照（说明在补历史），
会标记为 `ddl_fallback_approx` 并打 WARNING。

### 1.4 位点与 schema 的对应关系
`SchemaRegistry` 为每张表维护 `[版本@位点]` 列表：

* 行事件 → `schema_at(db, table, 事件位点)`：内存二分，**不再查 PG**；
* 只有当位点早于 bootstrap 快照时，才会**按表一次性**加载 `mylox_schema_history`
  （`ORDER BY id`，避免 `mysql-bin.999999` / `mysql-bin.1000000` 字符串排序错误），
  之后全部命中内存；
* 同一 DDL 重放（同一位点）会**替换**该位点的版本，天然幂等，重启不会写重复历史。

---

## 2. 多 Pipeline 语义

* 每个 pipeline 一条独立 checkpoint（`mylox_checkpoint`），业务写入与位点更新在**同一个 PG 事务**里提交；
* 单条行事件只投递给「尚未消费」的 pipeline：
  * 文件位点模式：`事件位点 >= pipeline 位点`；
  * GTID 模式：事件 GTID **不在**该 pipeline 的 executed 集合中；
* Reader 起点：
  * 文件位点模式：所有已初始化 pipeline 中**最早**的位点；
  * GTID 模式（`server_uuid` 变化时自动切换）：所有已初始化 pipeline 的 GTID **交集**
    （保证最慢的消费方不缺数据，快的靠 `gtid ∈ executed` 自行过滤）；
* 未初始化的 pipeline 在启动时冻结锚点（`STOP REPLICA`），
  追赶到达锚点后调用 `on_init` 并 `START REPLICA` 合流；全部未初始化则直接冷启动；
* `handle_row` 抛异常 → `ROLLBACK TO SAVEPOINT` → 摘除该 pipeline，其余继续推进；
  全部 pipeline 都失败（且无待初始化）才终止进程。策略由 `EngineConfig.on_pipeline_error`
  控制（`drop` / `raise`）；
* MySQL 的 `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` 会同步映射到 PG 事务，
  避免 MySQL 回滚掉的中间行被写进下游；
* 命中过滤规则的事务虽然不投递数据，但**仍然推进位点**，避免 checkpoint 长期落后而反复重放；
* 非事务引擎（MyISAM）的行事件没有 `XidEvent`，会在事件末尾立即提交并推进位点；
* 取不到可信 schema 的行事件**不投递、也不推进位点**：
  * 默认 `on_schema_error="drop"`：把会收到该事件的 pipeline 摘除（位点停在原地，修好原因后重启即可追赶），
    若所有 pipeline 都被摘除则报错退出——宁可停，也不静默丢数据；
  * `on_schema_error="skip"`：只记 ERROR、跳过该行并继续（该行随后会被位点越过，等于丢弃）；
* 恢复方式：确认 `mylox_schema_history` 缺失/漂移的原因后，重置对应 pipeline 的 `mylox_checkpoint`
  （或删除该行）让它重新走一次 `on_init` 全量初始化。

---

## 3. PostgreSQL 状态表

表名统一用 `mylox_` 前缀（常量见 `mylox/state/store.py` 的 `CHECKPOINT_TABLE` /
`SCHEMA_HISTORY_TABLE`）：

```sql
mylox_checkpoint(pipeline PK, server_uuid, server_id, log_file, log_pos,
                 gtid_set, is_initialized, updated_at)

mylox_schema_history(id BIGSERIAL PK, namespace, log_file, log_pos, gtid_set,
                     db_name, table_name, columns JSONB, primary_keys JSONB,
                     source, ddl_text, recorded_at)
```

* 两张表由 `PgStateStore.ensure_tables()` 在启动时 `CREATE TABLE IF NOT EXISTS` 自动创建，
  使用方不需要手工建表；
* `columns` 为列字典数组（`name/column_type/nullable/default/extra/ordinal`）；
  读取时兼容旧格式（纯列名字符串数组）；
* `source ∈ {bootstrap, ddl_sqlglot, ddl_fallback, ddl_fallback_approx, history}`；
* 启动时会自动 `ADD COLUMN IF NOT EXISTS namespace/source/ddl_text`，
  老版本 mylox 建的表可以原地升级；
* 唯一索引 `uq_mylox_schema_hist_version (namespace, db_name, table_name, log_file, log_pos, source)`
  保证同一版本的重复写入是幂等的；查询索引 `idx_mylox_schema_hist_lookup (namespace, db_name, table_name, id)`
  服务于「补历史」时的按表加载。

---

## 4. 代码阅读路线

按依赖顺序读，每步只有一个主题；括号里是"读的时候要确认的不变量"。

| # | 文件 | 看什么 |
| --- | --- | --- |
| 1 | `engine/handler.py` | `RowChange` / `CDCHandler`：业务侧唯一要实现的东西；`handle_row` 是**每个 pipeline 调一次**，包在 SAVEPOINT 里 |
| 2 | `gtid.py` | `GtidSet` 区间合并/包含（P0-1、P0-5 的基础），`intersection()` 用于 GTID 模式选点 |
| 3 | `utils/positions.py` | `BinlogPosition`：文件名按**数字后缀**比较，不是字符串 |
| 4 | `utils/rowimage.py` | `columns_present_bitmap` 清洗：`MINIMAL`/`FULL` 两种 row image 都要正确 |
| 5 | `schema/model.py` → `schema/registry.py` | 不可变 `TableSchema`；`TableTimeline` 是 `[版本@位点]` 列表，`schema_at()` 二分查找、`put()` 同位点替换（幂等） |
| 6 | `schema/plan.py` → `schema/evolve.py` | DDL 的中间表示与**纯函数**演进；所有失败都抛 `IncompleteDDL`（= 该兜底了），列顺序严格按 MySQL 语义 |
| 7 | `schema/ddl.py` + `schema/salvage.py` | `SqlglotDDLParser`：每种 DDL 怎么变成 `DDLPlan`；哪些语句故意标 `needs_fallback`（保守优先） |
| 8 | `schema/loader.py` | 从库 `information_schema` 读取；`SHOW MASTER STATUS` → `SHOW BINARY LOG STATUS` 回退，`STOP SLAVE` → `STOP REPLICA` 回退 |
| 9 | `schema/manager.py` | **混合获取的编排**：`bootstrap()` / `handle_ddl()`（主路径）/ `_fallback()`（兜底）/ `schema_for()`（查找顺序）/ `refresh_tables()`（禁止把实时快照盖到旧位点） |
| 10 | `state/store.py` | 两条连接：业务事务连接（autocommit=False）+ 元数据连接（autocommit=True）；`advance_checkpoints()` 是 upsert 且**不提交** |
| 11 | `engine/pipeline.py` | 活跃集判定（位点 / GTID）、`start_position()`、`gtid_position()`（交集）、`validate_gtid_resume()` |
| 12 | `engine/engine.py` | 主循环。重点：`_prepare()`（锚点/初始化时机）、`_consume()`（事件分发 + `handled` 位点）、`_handle_query_event()`、`_handle_rows_event()`、`_dispatch()`（SAVEPOINT 隔离）、`_commit_transaction()`（先提交后动内存）、`_resolve_schema()`（不可信就拒绝解码） |
| 13 | `config.py` / `filtering.py` | 三个策略开关的默认值，以及过滤规则的两种语义 |

三个"读代码时值得盯住"的设计决定：

1. **列顺序即契约**：`binlog_row_metadata=MINIMAL` 时 binlog 不带列名，行数据按下标映射，
   所以 `TableSchema.columns` 的顺序必须与 MySQL 完全一致（`ADD COLUMN AFTER/FIRST` 都要还原）。
2. **位点只能由"已完整处理"的事件推进**：事务内任何一行解码失败（`_txn_blocked`）或处理失败（摘除 pipeline），
   该 pipeline 的 checkpoint 都不许越过它；内存位点永远不领先于持久位点。
3. **宁可停，不猜**：schema 不可信、源库出现 statement 格式 DML、pipeline 业务报错——
   默认都是"停下/摘除并告警"，而不是静默跳过。三个开关（`on_schema_error`、`on_statement_dml`、`on_pipeline_error`）可以调整。

---
