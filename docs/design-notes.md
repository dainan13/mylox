# 设计取舍与缺陷修复对照

mylox 是从项目早期的 `cdc_lib.py` 重构而来的。`cdc_lib` **从未在生产使用过**，
所以 mylox 不提供也不需要任何从它迁移数据或位点的路径（直接新建 `mylox_*` 表、
重新初始化即可）。下面两张表是设计取舍的说明：为什么现在的结构是这样，
以及每个已知缺陷是怎么修的。

## 1. 与最初实现的差异

| 主题 | 最初实现（cdc_lib） | mylox |
| --- | --- | --- |
| 表结构来源 | 主库 `_repcdc` 影子表 + 从库影子表 | 内存 schema 时间线 + 从库 `information_schema` 快照 |
| DDL 处理 | 主库执行改写后的影子 DDL，再从影子表读列 | sqlglot 本地解析 SQL，直接演进内存模型 |
| DDL 解析失败 | 无兜底 | 回退查询一次从库 `information_schema`，并写入历史 |
| 需要的 MySQL 连接 | 主库 + 从库 | **只需要从库**（binlog + 元数据） |
| 每行变更取 schema | 每行查一次 PG `cdc_schema_history` | 纯内存二分查找，落后位点才一次性加载历史 |
| GTID 记录 | 存单个 GTID 到 `gtid_set` 字段（切库会错位/丢数据） | 完整 executed GTID 集合（区间合并、可判断包含） |
| DDL 是否推进位点 | 否（重启后会重复执行 DDL） | 是（DDL 作为隐式事务推进 checkpoint） |
| Pipeline 容错 | 无（一个业务抛异常全部挂掉） | 每个 pipeline 一个 SAVEPOINT，失败即摘除，其余继续 |
| `on_init` 异常 | 从库可能一直停在 `STOP SLAVE` | `finally` 保证 `START REPLICA` |
| schema history 写入 | 复用业务事务连接并 `commit()`（会破坏事务边界） | 独立 autocommit 连接 |
| `server_id` | 硬编码 `99999` | 可配置，默认随机 |
| 断线 | 直接退出 | 指数退避重连，并从 PG 重新加载位点 |
| 表过滤 | include/exclude 两套语义 | `.gitignore` 语义（最后匹配生效、`!` 可重新包含），保留 include 模式 |

> 检查清单见 `代码评估.txt`：P0-1 … P0-6、P1-7 … P1-13 均已处理，逐条对应关系见第 8 节。

---

## 2. 缺陷与修复对照

| 编号 | 问题 | 处理 |
| --- | --- | --- |
| P0-1 | 单个 GTID 当 `gtid_set` 用 | `mylox.gtid.GtidSet` 区间集合；checkpoint 存完整 executed 集合 |
| P0-2 | DDL 不推进 checkpoint | `_handle_query_event` 把 DDL 当隐式事务，立即推进活跃 pipeline 位点 |
| P0-3 | 每行查 PG 历史 schema | `SchemaRegistry` 内存时间线；仅「早于快照」时按表加载一次 |
| P0-4 | pipeline 无隔离、`on_init` 不复位复制 | SAVEPOINT 隔离 + 摘除策略；`_run_on_init` 的 `finally` 保证 `START REPLICA` |
| P0-5 | GTID 模式仍比 log_file | GTID 模式按 `gtid ∉ executed` 判定；文件位点比较改为数字后缀 |
| P0-6 | `record_schema_history` 提交业务事务 | schema history 走独立 autocommit 连接 |
| P1-7 | `server_id=99999` 硬编码 | `EngineConfig.server_id`，默认随机 |
| P1-8 | `!` 语义不是 gitignore | 支持 `gitignore`（最后匹配生效、`!` 重新包含）与 `include` 两种模式 |
| P1-9 | ALTER 外键未移除 | 影子表已删除，不再需要改写 DDL |
| P1-10 | 多表 DDL 只记最后一张 | `DDLPlan.changes` 逐表处理（`DROP TABLE a, b` / `RENAME TABLE a TO b, c TO d`） |
| P1-11 | `current_gtid` 未重置 | 事务边界处重置 `_session_gtid` |
| P1-12 | 历史排序用字符串比 log_file | `ORDER BY id ASC`，位点过滤在 Python 侧做 |
| P1-13 | 无重连 | `run()` 退避重连 + `PgStateStore.reconnect()` + 从 PG 重新加载位点 |

---
