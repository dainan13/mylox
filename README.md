# mylox

[![PyPI](https://img.shields.io/pypi/v/mylox.svg)](https://pypi.org/project/mylox/)
[![Status](https://img.shields.io/badge/status-beta-orange)](CHANGELOG.md)
[![Python versions](https://img.shields.io/pypi/pyversions/mylox.svg)](https://pypi.org/project/mylox/)
[![CI](https://github.com/dainan13/mylox/actions/workflows/ci.yml/badge.svg)](https://github.com/dainan13/mylox/actions/workflows/ci.yml)
[![License](https://img.shields.io/pypi/l/mylox.svg)](LICENSE)

**MySQL binlog → PostgreSQL 变更数据捕获（CDC）。**
从 MySQL 从库的 binlog 读行变更，按 pipeline 分发给你的 handler 写进 PostgreSQL；
表结构靠「`information_schema` 快照 + sqlglot 本地解析 DDL」维护，**不需要在 MySQL 里建影子表**。

```bash
pip install mylox
```

> **当前是 beta（0.8.x）。** 核心路径已在 MySQL 5.7.44 / 8.4.7 与 PostgreSQL 16 / 18 上
> 用 178 个单测 + 9 个真机集成用例验证；但 1.0 之前 API（尤其是 `CDCHandler` 的签名和
> `EngineConfig` 的字段）仍可能微调，升级前请看一眼 [CHANGELOG](CHANGELOG.md)。

## 它解决什么问题

做 MySQL → PostgreSQL 实时同步，绕不开三件麻烦事：

1. **表结构会变。** mylox 启动时拉一次 `information_schema`，之后直接解析 binlog 里的
   DDL 文本演进内存模型，解析不了才回退查一次库——不再需要维护影子表，
   也不需要主库连接（**只需要从库**）。
2. **位点不能丢。** 业务写入和位点更新在**同一个 PostgreSQL 事务**里提交；
   每行数据的 schema 从内存时间线按位点解析，不查库。
3. **一个业务不能拖垮全部。** 多个 pipeline 共享一条 binlog 流，各有独立位点；
   某个 pipeline 的业务代码抛异常只会摘除它自己，其余继续推进。

再加上 GTID 支持（换从库自动切 `auto_position` 续传）、断线退避重连，
以及 MySQL 5.7 / 8.4 的语句差异回退，就是一个能长期跑的同步进程。

## 30 秒上手

```python
from mylox import CDCEngine, CDCHandler, EngineConfig, MySQLSettings, PostgresSettings, RowChange


class Mirror(CDCHandler):
    """把 shop.orders 的行变更 upsert 进 dw.orders。"""

    def on_init(self, pipelines, ctx):
        # 冻结锚点上的全量装载：流式读源库，独立连接写目标库
        with ctx.mysql_cursor(streaming=True) as cur:
            cur.execute("SELECT * FROM shop.orders")
            rows = list(cur)
        with ctx.pg_cursor() as cur:
            for row in rows:
                cur.execute(
                    "INSERT INTO dw.orders (id, amount) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                    (row["id"], row["amount"]),
                )

    def handle_row(self, change: RowChange, pipeline: str, cur) -> None:
        if change.action == "DELETE":
            cur.execute("DELETE FROM dw.orders WHERE id = %s", (change.before["id"],))
        elif change.action == "UPDATE":
            sets = ", ".join("%s = %%s" % column for column in change.modify)
            cur.execute(
                "UPDATE dw.orders SET %s WHERE id = %%s" % sets,
                tuple(change.modify.values()) + (change.before["id"],),
            )
        else:
            cur.execute(
                "INSERT INTO dw.orders (id, amount) VALUES (%s, %s) "
                "ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount",
                (change.after["id"], change.after["amount"]),
            )


engine = CDCEngine(
    MySQLSettings(host="replica", port=3307, user="cdc", password="***"),
    PostgresSettings(host="pg", port=5432, dbname="dw", user="cdc", password="***"),
    pipelines=["orders"],
    handler=Mirror(),
    config=EngineConfig(filter_rules=["*", "!shop.*"]),  # 只要 shop 库（gitignore 语义，见下）
)
engine.run()
```

`handle_row` 里**不要 commit/rollback**——事务边界由引擎管理，每个 pipeline 的调用
都包在 SAVEPOINT 里。完整可运行的例子（按源表建镜像表、类型映射、幂等 upsert）见
[`example/mirror_to_pg.py`](example/mirror_to_pg.py)。

> ⚠️ 过滤规则默认是 **gitignore 语义**：普通 pattern 表示*排除*，`!` 前缀表示重新包含。
> 「只要 shop 库」要写 `["*", "!shop.*"]`，而 `["shop.*"]` 是**排除** shop 库。
> 两种方言的对照表见[配置](docs/configuration.md#3-过滤规则的两种方言)。

## 环境要求

| 组件 | 要求 |
| --- | --- |
| MySQL | 5.7 / 8.4 实测通过；需要 `log_bin=ON`、`binlog_format=ROW`、`gtid_mode=ON`、`log_slave_updates=ON` |
| PostgreSQL | 16 / 18 实测通过；首次运行会自动建 `mylox_checkpoint`、`mylox_schema_history` |
| Python | 3.10+（本地在 3.14 上全量验证） |
| 连接 | 只需要**从库**：binlog 流 + `information_schema` + `STOP/START REPLICA`；主库不需要连接 |

从库账号需要 `SELECT`、`REPLICATION SLAVE`、`REPLICATION CLIENT`、`REPLICATION SLAVE ADMIN`
（或 `SUPER`，用于初始化时暂停复制）。

## 文档

| 文档 | 内容 |
| --- | --- |
| [架构与实现](docs/architecture.md) | schema 混合获取、时间线、引擎主循环、事务与位点、状态表、代码阅读路线 |
| [编写 Handler](docs/usage.md) | `RowChange` 字段、幂等写法、`on_init` 全量装载、失败回调、多 pipeline 分工 |
| [配置](docs/configuration.md) | 连接设置、`EngineConfig` 全量配置、过滤规则的两种方言 |
| [运维与排障](docs/operations.md) | 部署拓扑、启动行为、信号与退出、排障清单、已知限制、数据恢复 |
| [版本兼容性](docs/compatibility.md) | MySQL 5.7 与 8.4 的语句差异、实测结论、支持的版本范围 |
| [测试](docs/testing.md) | 单元/集成测试怎么跑、环境变量、每个用例证明什么、实测结果 |
| [开发与发布](docs/development.md) | 本地环境、目录结构、日常命令、发版流程（GitHub Actions → PyPI） |
| [设计取舍与缺陷对照](docs/design-notes.md) | 为什么这么设计、每个已知缺陷是怎么修的 |

## 项目结构

```
src/mylox/
├── schema/     # ★ 混合 schema：model 时间线 / DDL 解析 / information_schema 读取 / 编排
├── engine/     # binlog 主循环、pipeline 状态、handler API、reader 工厂
├── state/      # PostgreSQL 位点与 schema 历史（双连接：业务事务 + 元数据）
├── utils/      # 位点比较、位图清洗、序列化
├── gtid.py filtering.py config.py errors.py constants.py
docs/           # 用户与开发文档（不打进 wheel）
example/        # 可运行示例
test/           # 单测（伪 binlog 事件）+ 集成测试（真实主从）
```

## 测试状态

两个 MySQL 版本各跑一遍：**187 passed（178 单测 + 9 集成）**，
环境为 MySQL 5.7.44 / 8.4.7（各一主两从）+ PostgreSQL 18.6。
单测不需要数据库：

```bash
pytest test -q --ignore=test/test_integration.py
```

集成测试的拓扑要求、环境变量与每个用例的断言见[测试](docs/testing.md)。

## 贡献 / 许可

欢迎 issue 和 PR，先看一眼 [CONTRIBUTING.md](CONTRIBUTING.md)。
本项目使用 [MIT 许可证](LICENSE)。
