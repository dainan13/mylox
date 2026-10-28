# 运维与排障

## 1. 部署拓扑

```
MySQL 主库 ──复制──▶ MySQL 从库 ──binlog──▶ mylox ──▶ PostgreSQL
   (写入)              (binlog 源)          (只读)
```

mylox **只需要从库连接**：binlog 流、`information_schema`、`STOP/START REPLICA` 都打在从库上；
主库不需要任何连接。PostgreSQL 用作业务写入与状态存储。

要求：

| 项 | 要求 |
| --- | --- |
| 从库 binlog | `log_bin=ON`、`binlog_format=ROW`、`log_slave_updates=ON`、`gtid_mode=ON` |
| 从库账号权限 | `SELECT`（含 `information_schema`）、`REPLICATION SLAVE`、`REPLICATION CLIENT`、`REPLICATION SLAVE ADMIN`（或 `SUPER`，用于 STOP/START REPLICA） |
| PG 账号权限 | 目标库 `CREATE`（首次自建两张状态表）、业务表读写 |
| 网络 | mylox → 从库 3306/自定义端口；mylox → PG 5432 |

## 2. 启动行为

| 场景 | 行为 |
| --- | --- |
| 全部 pipeline 未初始化 | 冷启动：`STOP REPLICA` → 取锚点 → bootstrap schema → `on_init` 全量装载 → `mark_initialized` → `START REPLICA` → 从锚点开始出流 |
| 部分 pipeline 未初始化 | 从最早的已初始化位点开始追赶；追到锚点时暂停并初始化新 pipeline，随后 `START REPLICA` 合流 |
| 全部已初始化、同从库 | 从最早位点（文件位点模式）或 executed GTID 交集（GTID 模式）续读 |
| 从库 uuid 变化（切库） | 自动切 GTID 模式，`auto_position` 用各 pipeline executed 集合的交集；没有 GTID 集合时**拒绝猜测**并报错 |
| 断线 | 指数退避重连（默认 1s→30s），并从 PG 重新加载位点 |

`pause_slave_on_init`（默认 `True`）决定初始化前是否 `STOP REPLICA`。
不暂停时锚点与快照之间的一致性没有保证，只建议在测试/只读负载下关掉。

## 3. 信号与退出

* `SIGINT` / `SIGTERM`：在当前事务边界或下一次心跳后优雅退出，并把已暂停的复制恢复；
* 再次收到信号：立即退出；
* 未启用 `slave_heartbeat_seconds` 时，从库长期无写入会卡在 socket 读取上，
  导致 `Ctrl+C` 无响应——默认心跳 1 秒就是为此。

## 4. 排障

### 常见问题

1. **过滤规则写反了会「什么都不复制」，而且不报错。** 默认 `gitignore` 模式下
   `filter_rules=["shop.*"]` 的含义是**排除** shop 库；要「只要 shop」得写
   `["*", "!shop.*"]`，或者设 `EngineConfig(filter_mode="include")` 用 `["shop.*"]`。
   现在 mylox 会在启动时打印生效的过滤规则，并在 bootstrap 命中 **0 张表**时直接告警：
   ```
   WARNING no table matched the filter rules, so nothing will be replicated:
           mode=gitignore rules=['shop.*'] (gitignore mode ignores plain patterns and uses '!' to re-include)
   ```
2. **`pymysqlreplication` 会打印 `Setting The Variable Value BINLOG_ROW_METADATA = FULL ...`**
   （MySQL 5.7 上换成 `use python-mysql-replication version Before 1.0 version`）。
   这只是它在读取该变量后给出的**建议**，不会真的修改服务器参数（实测核对过，仍是 `MINIMAL`）。
   mylox 在 `MINIMAL` 下按下标映射列，本来就不依赖它；
   若想额外做**列名校验**，可以自行在源端 `SET GLOBAL binlog_row_metadata=FULL`（代价是 binlog 变大）。

---

### 更细的排查顺序

1. 看启动日志里的 `table filter: mode=... rules=[...]` 和 `Schema bootstrap ... N tables loaded`；
   N 为 0 就是没匹配到表（见上）；
2. 看 `mylox_schema_history` 里该表的 `source`：`bootstrap` / `ddl_sqlglot` / `ddl_fallback`；
3. 看 `mylox_checkpoint` 的位点是否在推进：不推进通常是「行事件解码失败」或「pipeline 被摘除」；
4. 看从库 `SHOW REPLICA STATUS`：`Slave_SQL_Running` 必须是 `Yes`（mylox 只读从库，不会写它）。

## 5. 已知限制 / 后续可做

* **statement-based / MIXED**：只支持 `binlog_format=ROW`。`QueryEvent` 里出现 DML 说明源库在用 statement 格式，
  这些行无法捕获，默认直接 `FatalEngineError`（宁可报错也不静默丢数据），可用
  `EngineConfig.on_statement_dml="warn"` 降级为告警；告警模式下该事务**不推进位点**，便于人工修复后重放；
* 兜底查询拿到的是「当前时刻」的 `information_schema`，在补历史时属于近似值（标记 `ddl_fallback_approx` 并告警）。
  正常情况下历史里已有该 DDL 的记录，不会走到这里；
* `mylox_checkpoint` 的主键列叫 `pipeline`；老版本建的、主键列还叫 `cdc_pipeline` 的表
  会在启动时自动 `ALTER TABLE ... RENAME COLUMN` 迁移（幂等，无需人工干预）。
* **二进制列（BLOB/BINARY）的取值是有损文本**：驱动无法返回原始字节，mylox 只能在 bootstrap 时告警
  （见[兼容性 §1.2](compatibility.md#12-实测中发现并修掉的两个真问题)）；需要字节级保真请在源端存 hex/文本，或让 handler 忽略这些列；
* 未实现「落后的 pipeline 单独追赶」的调度（当前与主流程共用一条流）；
* 暂无 Prometheus/监控埋点，只有结构化日志与 `on_schema_warning` 回调。


---

## 6. 数据恢复

- 某个 pipeline 被摘除（业务报错 / schema 不可信 / 事件无法解码）：位点停在原地，
  修好原因后重启即可从该位点继续投递；也可以删掉 `mylox_checkpoint` 里该行让它重新走一次
  `on_init` 全量初始化；
- 误把过滤规则写反导致「什么都没复制」：改正后重启；期间的数据会从旧位点补上；
- 状态表被清空：删掉 `mylox_checkpoint` 行会触发重新初始化（会全量重灌），
  只删 `mylox_schema_history` 则会让历史版本丢失，落后位点的补历史会退化为兜底近似。
