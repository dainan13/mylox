# 版本兼容性

mylox 依赖 MySQL 的 binlog 格式与 `information_schema`，两者在不同大版本之间
有若干不兼容的语句/变量。这张表是实测结论，以及 mylox 对应的处理方式。

实测环境：MySQL 5.7.44（主 5701 / 从 5702、5703）、MySQL 8.4.7（主 8401 / 从 8402、8403）、
PostgreSQL 18.6，两个 MySQL 版本各跑一遍完整用例。

## 1. MySQL 5.7 与 8.4 的差异

测试环境：`192.168.13.62`，MySQL 5.7.44（主 5701 / 从 5702、5703）与 MySQL 8.4.7（主 8401 / 从 8402、8403），
PostgreSQL 18.6。两个版本各跑一遍完整用例：**187 passed（178 单测 + 9 集成），EXIT=0**。

### 1.1 版本差异与 mylox 的处理

| 能力 | MySQL 5.7.44 | MySQL 8.4.7 | mylox 怎么做 |
| --- | --- | --- | --- |
| 取自库 binlog 位点 | `SHOW MASTER STATUS` ✓ | ✗ 语法错误 1064 → `SHOW BINARY LOG STATUS` ✓ | `master_status()` 依次尝试两条语句 |
| 复制状态 | `SHOW REPLICA STATUS` ✗ → `SHOW SLAVE STATUS` ✓ | `SHOW REPLICA STATUS` ✓ | 暂停/恢复先试 `STOP/START REPLICA` 再回退 `STOP/START SLAVE`（实测两版本各自命中的语句已确认） |
| `binlog_row_metadata` | **不存在** | `MINIMAL` | 两者都拿不到列名 → 一律按**下标**映射（`UNKNOWN_COL<i>`）；实测两版本解码结果完全一致 |
| 列类型字符串 | `int(11)` 保留显示宽度 | `int` | `column_type` 如实保存；`example` 的类型映射对两种写法都输出 `integer` |
| `RENAME COLUMN` | ✗（需 `CHANGE COLUMN old new`） | ✓ | 解析器两条语法都支持；集成测试按版本选择可用写法 |
| GTID | `gtid_mode=ON`、`last_committed`/`sequence_number` ✓ | ✓ | 同一套 `GtidSet` 逻辑，`auto_position` 续传均实测通过 |
| `JSON` / `DATETIME(3)` / `DECIMAL` / `UNSIGNED` / utf8mb4 文本 | — | — | 两版本解出的 Python 值**逐字段相同** |

### 1.2 实测中发现并修掉的两个真问题

1. **BLOB/BINARY 里的非 UTF-8 字节会让引擎永久卡死。**
   `pymysqlreplication` 默认对取不到字符集的列用 `utf-8` **strict** 解码，遇到 `0xff` 抛
   `UnicodeDecodeError`；异常从行事件里冒出来会被当成"可重连的瞬时故障"，于是**每次重连都在同一条事件上再炸一次**，
   位点永远不推进。现在：
   * `EngineConfig.ignore_decode_errors`（默认 `True`）让驱动改用 `errors="ignore"`，不再抛；
   * 即使还有别的解码/解析异常，也会走 `_handle_undecodable_event()`：按
     `on_schema_error` 策略**摘除相关 pipeline**（位点不动，可后续追赶），而不是无限重连；
   * bootstrap 时对每张含二进制列的表打一次 WARNING。
   * 残余限制：驱动无法返回 BLOB 原始字节，二进制列的值是**有损文本**；
     需要字节级保真请在源端存 hex/文本，或让 handler 忽略这些列。
2. **`BIGINT UNSIGNED` 被解成负数（静默数据损坏）。**
   列的有符号性只在 `binlog_row_metadata=FULL` 时才进 binlog；`MINIMAL`（8.x 默认）与 5.7（无此变量）下
   驱动按有符号读，`18446744073709551615` 变成 `-1`。mylox 有 schema，所以直接按
   `column_type` 里的 `unsigned` 用掩码还原（`schema/model.py:fix_unsigned_values`）：
   实测 5.7/8.4 均得到 `18446744073709551615` / `4294967295` / `65535`。
   （`_UNSIGNED` 列上出现负值只可能来自这种回绕，所以回填是安全的。）

### 1.3 其他实测结论

| 现象 | 影响 | mylox 的处理 |
| --- | --- | --- |
| 从库 `read_only=ON`（本环境未开 `super_read_only`） | 引擎不应往源库写 | mylox 只读从库（binlog + `information_schema`），唯一的"写"是 `STOP/START REPLICA`；**不需要主库连接** |
| 从库 binlog 的 `Executed_Gtid_Set` 含主库 uuid 区间 | 跨从库按位点续传必然错位 | `server_uuid` 变化自动切 GTID 模式，`auto_position` 用各 pipeline executed 集合的**交集**；两台从库坐标不同也实测通过 |
| PG 18.6：`ADD COLUMN IF NOT EXISTS` / `DISTINCT ON` / JSONB / `ON CONFLICT` 均可用 | 老版本 mylox 建的表可原地升级 | `ensure_tables()` 自动补列/改名；唯一索引 `(namespace,db,table,log_file,log_pos,source)` 保证重放幂等 |
| 过滤规则写成了另一种方言 | 可能一个表都匹配不上 | 启动时打印生效规则；一条都没匹配上时 WARNING 提示检查方言（共享服务器上目标库为空时也会出现，属正常） |

> 想让 mylox 额外做**列名校验**（而不只是下标），可以在源端把 `binlog_row_metadata` 设为 `FULL`
> （MySQL 8 支持 `SET GLOBAL`，5.7 不支持该变量）；binlog 里带上列名后，
> `on_unknown` 回调会把对不上的列报出来，等于多一层漂移告警，同时 unsigned 也由驱动直接读对。
> 代价是 binlog 体积变大。

## 2. 支持的版本范围

| 组件 | 实测版本 | 说明 |
| --- | --- | --- |
| MySQL | 5.7.44、8.4.7 | 需要 `log_bin=ON`、`binlog_format=ROW`、`gtid_mode=ON`、`log_slave_updates=ON`；5.6/8.0 未实测，但用到的语句都有回退分支 |
| PostgreSQL | 16.15、18.6 | 需要 `CREATE` 权限（自建 `mylox_checkpoint` / `mylox_schema_history`），JSONB 可用 |
| Python | 3.10+ | CI 矩阵覆盖 3.10–3.13；开发机在 3.14 上跑过全部用例 |
| sqlglot | >= 20 | 只用于把 DDL 文本转成 `DDLPlan`，不参与数据路径 |
| PyMySQL / mysql-replication | >= 1.0 | `mysql-replication` 提供 binlog 解析；`ignore_decode_errors` 需要 1.0+ |
| psycopg | >= 3.1 | 只用 v3（`psycopg`，不是 `psycopg2`） |
