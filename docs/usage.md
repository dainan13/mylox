# 编写 Handler

mylox 只负责"从 MySQL binlog 拿变更、按 pipeline 分发、和 PostgreSQL 位点原子提交"；
**变更怎么写进你的目标表，由你实现 `CDCHandler` 决定**。

## 1. 最小可运行示例

```python
from mylox import CDCEngine, CDCHandler, EngineConfig, MySQLSettings, PostgresSettings, RowChange


class Handler(CDCHandler):
    def handle_row(self, change: RowChange, pipeline: str, cur) -> None:
        if change.action == "INSERT":
            cur.execute(
                "INSERT INTO mirror (id, payload) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                (change.after["id"], change.to_dict()),
            )


engine = CDCEngine(
    MySQLSettings(host="replica", user="cdc", password="..."),
    PostgresSettings(dbname="dw", user="cdc", password="..."),
    pipelines=["orders"],
    handler=Handler(),
    config=EngineConfig(filter_rules=["*", "!shop.*"]),
)
engine.run()
```

`example/mirror_to_pg.py` 是一个更完整的例子：按源表建镜像表、`on_init` 全量装载、
INSERT/UPDATE/DELETE 幂等落地、表结构随 DDL 演进。

## 2. `handle_row(change, pipeline, cur)`

引擎对**每个活跃 pipeline 各调用一次**，每次调用都包在一个 PostgreSQL SAVEPOINT 里：
某个 pipeline 抛异常只回滚它自己的写入，其余 pipeline 继续；失败的那个会被**摘除**
（位点停在原地，修好后重启即可从该位点继续）。

`cur` 是共享业务事务上的游标。**不要**在 handler 里 commit / rollback——事务边界由引擎管。

### `RowChange` 字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `db` / `table` | `str` | 源库表名 |
| `action` | `ChangeAction` | `INSERT` / `UPDATE` / `DELETE`（等于 `"INSERT"` 等字符串，可直接比较） |
| `timestamp` | `int \| None` | binlog 事件时间戳（epoch 秒） |
| `primary_keys` | `tuple[str, ...]` | 该表主键列（顺序按 MySQL 的键顺序） |
| `before` | `dict \| None` | 变更前的值；**只包含 row image 里真实出现的列** |
| `after` | `dict \| None` | INSERT 的新值（UPDATE 的 `after` 为 `None`，见下） |
| `modify` | `dict \| None` | UPDATE 中**真的变化了**的列 → 新值 |
| `log_file` / `log_pos` / `gtid` | — | 事件坐标，便于审计 |

`before` / `after` / `modify` 的取值已经过 `stringify_bytes()`：
合法的 UTF-8 字节变成 `str`，非法字节**保留 `bytes`**（不再替换成乱码）。
JSON 列是 `dict`/`list`，`DECIMAL` 是 `Decimal`，`DATETIME` 是 `datetime`，
`BIGINT UNSIGNED` 会按 schema 修正回无符号值。

> **UPDATE 只给 `before` + `modify`，不给 `after`。** 这是刻意的：`binlog_row_image=MINIMAL`
> 时拿不到完整的新行，拼一个"看起来完整"的 `after` 会把没变的列误写成 `None`。
> 需要完整行时，用 `before` 为主键定位、`modify` 做增量更新（见下）。

### 幂等写法（推荐）

位点提交与业务写入在同一个 PG 事务里，正常不会重复；但**重连/重放**是 at-least-once 语义，
所以落地语句最好幂等：

```python
def handle_row(self, change, pipeline, cur):
    key = change.key_values()                       # {pk: value}
    where = " AND ".join("%s = %%s" % k for k in change.primary_keys)

    if change.action == "DELETE":
        cur.execute("DELETE FROM dw.orders WHERE " + where, tuple(key.values()))
    elif change.action == "UPDATE":
        sets = ", ".join("%s = %%s" % c for c in change.modify)
        cur.execute(
            "UPDATE dw.orders SET %s WHERE %s" % (sets, where),
            tuple(change.modify.values()) + tuple(key.values()),
        )
    else:
        cur.execute(
            "INSERT INTO dw.orders (...) VALUES (...) ON CONFLICT (id) DO UPDATE SET ...",
            (...),
        )
```

## 3. `on_init(pipelines, ctx)`

pipeline 在冻结锚点上做全量初始化时调用一次（冷启动、或改配置后新增 pipeline）。
`ctx.pipelines` 是本次要初始化的 pipeline 集合，`ctx.anchor` 是锚点坐标，
`ctx.all_schemas()` 返回当前被过滤命中的表结构模型。

`on_init` **必须返回才能恢复复制**：引擎在 `finally` 里 `START REPLICA`，所以这里抛异常
不会把从库永久停在 STOP 状态（异常会导致这些 pipeline 被摘除）。

### 两条连接

```python
def on_init(self, pipelines, ctx):
    # 1) 读源库：流式游标，不会把整表读进内存
    with ctx.mysql_cursor(streaming=True) as cur:
        cur.execute("SELECT * FROM shop.orders")
        rows = list(cur)

    # 2) 写目标库：独立连接、autocommit，退出时 commit + close
    with ctx.pg_cursor() as cur:
        for row in rows:
            cur.execute("INSERT INTO dw.orders ... ON CONFLICT ... DO NOTHING", (...))
```

两者都是**独立连接**（不是引擎的业务事务连接），所以全量装载期间不会拖住增量事务。
`ctx.slave_connection()` / `ctx.pg_connection()` 返回裸连接，需要自己关闭。

### 一致性

引擎取锚点时会 `STOP REPLICA`，此时的源库快照与锚点严格对齐：`on_init` 装载「锚点时刻」的数据，
之后从锚点开始投递增量，不会重不会漏。`pause_slave_on_init=False` 时这个保证不成立（仅测试用）。

## 4. 其他回调

| 回调 | 何时调用 |
| --- | --- |
| `on_pipeline_failed(pipeline, error, change)` | pipeline 被摘除时（handler 抛异常 / schema 不可信 / 事件无法解码） |
| `on_schema_warning(message, detail)` | schema 相关告警：DDL 无法建模走了兜底、列数漂移、取不到历史版本等 |

两个都应该是「记录 + 告警」，抛异常只会被吞掉并记日志。

## 5. 哪些表会进来

`handle_row` 只会被**过滤规则命中**的表触发。规则默认是 gitignore 语义，注意别写反：

```python
EngineConfig(filter_rules=["*", "!shop.*"])   # 只要 shop 库
EngineConfig(filter_rules=["arch2018.*"])     # 排除 arch2018 库（不是"只要"！）
```

详见[配置 §过滤规则](configuration.md#3-过滤规则的两种方言)。

## 6. 多个 pipeline 怎么分工

每个 pipeline 有自己的位点、自己的 `handle_row` 调用（同一个 `change` 传多次）。
聚合型业务通常这样用：

```python
class Handler(CDCHandler):
    def handle_row(self, change, pipeline, cur):
        if pipeline == "orders_wide":
            self._upsert_orders_wide(change, cur)
        elif pipeline == "inventory_wide":
            self._upsert_inventory_wide(change, cur)
```

一个 pipeline 出问题不影响其它 pipeline 的位点推进；被摘除的 pipeline 保留位点，
修好后重启即从原处追赶。
