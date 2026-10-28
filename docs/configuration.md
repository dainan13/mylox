# 配置

## 1. 连接设置

```python
from mylox import MySQLSettings, PostgresSettings

mysql = MySQLSettings(host="replica", port=3307, user="cdc", password="...", charset="utf8mb4")
pg = PostgresSettings(host="pg", port=5432, dbname="dw", user="cdc", password="...")
```

两个类都可以从字典构造，未知键会进入 `extra` 并原样透传给驱动：

```python
MySQLSettings.from_dict({"host": "...", "user": "...", "passwd": "...", "port": "3307", "ssl": {...}})
PostgresSettings.from_dict({"dbname": "...", "user": "...", "sslmode": "require"})
```

也可以用 `mylox.env_settings("MYLOX_MYSQL")` 读取 `MYLOX_MYSQL_*` 环境变量后交给 `from_dict()`
（`example/mirror_to_pg.py` 就是这么做的）。数字型字段接受字符串并做校验。

`MySQLSettings` 会为三种连接生成不同的参数：元数据查询用 dict 游标 + `autocommit=True`，
全量装载用 `SSDictCursor`（流式、不占内存），binlog 流不设置 `cursorclass`。
`PostgresSettings.connect_kwargs(autocommit=...)` 生成 psycopg v3 的连接参数。

## 2. `EngineConfig`

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `namespace` | PG 库名 | 历史表隔离命名空间（替代旧影子表 namespace） |
| `filter_rules` / `filter_mode` | `[]` / `gitignore` | 表过滤规则 |
| `server_id` | 随机 | binlog 客户端 id，多实例共存时必须唯一 |
| `slave_heartbeat_seconds` | `1.0` | 心跳，保证无写入时也能响应 Ctrl+C |
| `pause_slave_on_init` | `True` | 初始化前冻结从库取锚点 |
| `strict_pause` | `False` | 暂停失败时是否直接报错（默认仅告警） |
| `on_pipeline_error` | `drop` | 失败 pipeline 的处理策略 |
| `on_schema_error` | `drop` | 取不到可信 schema 时：`drop` 摘除相关 pipeline（不丢数据、可后续追赶）/ `skip` 跳过该行继续 |
| `reconnect` / `reconnect_*_backoff` | `True` / `1s` / `30s` | 断线重连退避 |
| `schema_fallback_enabled` | `True` | 是否允许回退查询 `information_schema` |
| `bootstrap_schema` | `True` | 启动是否拉取初始快照 |
| `record_schema_history` | `True` | 是否写 `mylox_schema_history` |
| `on_statement_dml` | `fail` | 源库出现 statement 格式 DML 时的策略：`fail` / `warn` / `ignore` |
| `stream_options` | `{}` | 透传给 `BinLogStreamReader` 的额外参数 |

---

## 3. 过滤规则的两种「方言」

`TableFilter` 支持两种模式，**写法相反**，这是最容易踩的坑：

```python
# 默认：gitignore 语义。普通 pattern = 排除；! 前缀 = 重新包含。
EngineConfig(filter_rules=["*", "!shop.*"])          # 只要 shop 库
EngineConfig(filter_rules=["arch2018.*", "!arch2018.keep_me"])   # 排除 arch2018，除了 keep_me

# include 语义。普通 pattern = 包含；! = 排除。
EngineConfig(filter_rules=["shop.*"], filter_mode="include")
EngineConfig(filter_rules=["shop.*", "!shop.tmp_*"], filter_mode="include")
```

规则按顺序匹配、**最后命中的生效**；pattern 同时匹配 `db.table` 和单独的 `table`，
所以 `["*.log"]` 会排除任意库里名为 `log` 的表。匹配是区分大小写的
（`fnmatch.fnmatchcase`，不随平台变化）。

两种方言下「只要 shop 库」的等价写法：

| 目标 | gitignore（默认） | include |
| --- | --- | --- |
| 只要 `shop.*` | `["*", "!shop.*"]` | `["shop.*"]` |
| 只要 `shop.*` 但排除 `tmp_*` | `["*", "!shop.*", "shop.tmp_*"]` | `["shop.*", "!shop.tmp_*"]` |
| 排除 `arch2018.*`，但保留一张 | `["arch2018.*", "!arch2018.keep"]` | `["*", "!arch2018.*", "!important"]`←不直观，建议用默认模式 |

也可以用 `TableFilter(rules, mode=..., default=...)` 直接构造，
`default` 控制「一条规则都不命中」时的默认值。

### 排障提示

- 启动日志会打印生效的过滤规则（`table filter: mode=... default_allowed=... rules=[...]`）；
- bootstrap 时如果**一条都没命中**，会打 WARNING 提示检查方言
  （共享服务器上目标库还没建表时也会出现，属正常）；
- 命中 0 张表时不会有任何数据被复制，且不会报错——这是最容易「静默无效」的地方。

## 4. 表过滤之外的选择

`TableFilter` 只决定「哪些表进入 schema 模型、哪些行事件被投递」。想在业务侧再区分，
用 `handle_row(change, pipeline, cur)` 里的 `pipeline` 和 `change.db/table` 自己分流即可。
