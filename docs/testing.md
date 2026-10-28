# 测试

测试分两层：**单元测试**（不需要数据库，用伪造的 binlog 事件驱动整个引擎）
和**集成测试**（真实 MySQL 主从 + PostgreSQL）。

## 1. 单元测试

```bash
pytest test -q --ignore=test/test_integration.py
```

覆盖：GTID 区间运算、binlog 位点比较、位图清洗、过滤规则、schema 时间线、
DDL 解析与演进、配置校验、pipeline 活跃集、引擎端到端（事务原子性、DDL 推进位点、
pipeline 隔离、锚点初始化、重连、GTID 切换）、示例代码。

`test/fakes.py` 里的伪造对象是关键：`FakeStream` 按 `(event, log_file, log_pos)` 回放，
事务型 `FakeStateStore` 把 checkpoint 更新缓冲到 commit（模拟真实事务语义），
`FakeLoader` 提供 `information_schema` 结果。所以「提交失败不推进内存位点」这类
关键行为在没有数据库时也能测。

## 2. 集成测试

需要一对 MySQL 主从（最好再加第二台从库用于切换用例）和一个 PostgreSQL 库。
真实从库通常是 `super_read_only`，所以**写操作都在主库做**，引擎只读从库。

```bash
export MYLOX_TEST_MYSQL_HOST=...            # 从库（binlog 源 + information_schema）
export MYLOX_TEST_MYSQL_PORT=3307
export MYLOX_TEST_MYSQL_USER=...  MYLOX_TEST_MYSQL_PASSWORD=...
export MYLOX_TEST_MYSQL_MASTER_PORT=3306    # 主库（真正写数据的库）
export MYLOX_TEST_MYSQL_SECOND_PORT=3308    # 可选：第二台从库，用于切换/GTID 续传用例
# host/user/password 默认复用从库的，只有端口需要单独给
export MYLOX_TEST_PG_HOST=...  MYLOX_TEST_PG_PORT=5432
export MYLOX_TEST_PG_USER=...  MYLOX_TEST_PG_PASSWORD=...  MYLOX_TEST_PG_DATABASE=...
export MYLOX_TEST_ALLOW_REPLICA_CONTROL=1   # 可选：开启 STOP/START REPLICA 用例
pytest test/test_integration.py -q
```

前置条件（不满足会自动 skip）：从库 `log_bin=ON`、`binlog_format=ROW`、
`log_slave_updates=ON`、`gtid_mode=ON`，复制线程健康。

集成测试会自建临时 MySQL 库 `mylox_it_<random>`（跑完 DROP）与 PG namespace
（跑完删掉测试行），并在主库上灌 `INSERT/UPDATE/DELETE` 与若干 DDL；
用 `WAIT_FOR_EXECUTED_GTID_SET()` 等复制追平，保证断言时两库一致。
PG 里只保留 `mylox_checkpoint` / `mylox_schema_history` 两张状态表。

`MYLOX_TEST_ALLOW_REPLICA_CONTROL=1` 会开启两个用例，它们会在从库上执行
`STOP REPLICA`/`START REPLICA`（5.7 上自动回退 `STOP SLAVE`/`START SLAVE`），
复制会中断约 1~2 秒，结束时保证恢复。

## 3. 实测结果（MySQL 5.7.44 与 8.4.7 + PostgreSQL 18.6）

两个 MySQL 版本各跑一遍：**187 passed（178 单测 + 9 集成）**，`EXIT=0`，跑完主从库与 PG 都无残留：

| 集成用例 | 验证到的点 |
| --- | --- |
| `test_state_store_roundtrip` | 状态表建表/索引、JSONB 往返、唯一索引去重、checkpoint upsert |
| `test_loader_reads_columns_and_primary_keys` | 从库 `information_schema` 读列/可空/类型；**8.4 上 `SHOW MASTER STATUS` 回退到 `SHOW BINARY LOG STATUS`** |
| `test_loader_reads_composite_primary_key_order` | 复合主键按**键顺序**（`PRIMARY KEY (b,a)` ≠ 列顺序）读取 |
| `test_sqlglot_evolution_matches_information_schema` | 8 条真实 DDL（加列/`FIRST`/`AFTER`/`CHANGE`/`MODIFY`/`DROP`/改列名/外键）逐条比对「模型 == MySQL 自己的视图」，含 nullable；5.7 自动改用 `CHANGE COLUMN` 完成改名 |
| `test_fallback_recovers_from_an_unparsable_ddl` | `CREATE TABLE ... PARTITION BY`（sqlglot 无法建模）走兜底并写 `ddl_fallback` |
| `test_engine_replays_live_binlog` | 真实 binlog 回放：`INSERT/UPDATE/DELETE` + `ALTER`，**拿不到列名时按下标映射出正确列名**，UPDATE 只出变更列，位点推进，历史里有 bootstrap + ddl_sqlglot |
| `test_replica_control_commands` | 暂停/恢复（**8.4 命中 `STOP REPLICA`，5.7 回退 `STOP SLAVE`**）、暂停后锚点稳定、恢复后复制健康 |
| `test_engine_cold_start_with_a_real_anchor` | **生产首跑全流程**：`STOP REPLICA` → 取锚点 → bootstrap → `on_init` → `START REPLICA` → 从锚点出流 → 三个变更都收到 → `request_stop()` 后干净退出，复制恢复 |
| `test_failover_to_a_second_replica` | **切从库 + GTID 续传**：在从库 A 上消费并落位点 → 重跑不重复投递 → 切到 uuid 和 binlog 位点都不同的从库 B：自动进 GTID 模式（`start is None`，`auto_position` == 已消费集合），只投递切换后的新事务，executed 集合单调增长 |

> 两个版本各跑一遍：**187 passed（178 单测 + 9 集成）**，`EXIT=0`。
> 版本差异见 [兼容性](compatibility.md)，测试环境的坑见 [运维与排障](operations.md)。

## 4. 测试清单（集成）

| 用例 | 证明什么 |
| --- | --- |
| `test_state_store_roundtrip` | 状态表建表/索引、JSONB 往返、唯一索引去重、checkpoint upsert |
| `test_loader_reads_columns_and_primary_keys` | `information_schema` 读列/可空/类型；8.4 上 `SHOW MASTER STATUS` 回退 |
| `test_loader_reads_composite_primary_key_order` | 复合主键按**键顺序**读取 |
| `test_sqlglot_evolution_matches_information_schema` | 8 条真实 DDL 逐条比对「内存模型 == MySQL 自己的视图」 |
| `test_fallback_recovers_from_an_unparsable_ddl` | sqlglot 无法建模的 DDL 走兜底并写 `ddl_fallback` |
| `test_engine_replays_live_binlog` | 真实 binlog 回放、按下标映射、UPDATE 只出变更列、位点推进 |
| `test_replica_control_commands` | 暂停/恢复复制、暂停期间锚点稳定 |
| `test_engine_cold_start_with_a_real_anchor` | 生产首跑：STOP → 锚点 → bootstrap → on_init → START → 出流 → 优雅退出 |
| `test_failover_to_a_second_replica` | 切到另一台从库：自动 GTID 模式、不重复投递、executed 集合增长 |
