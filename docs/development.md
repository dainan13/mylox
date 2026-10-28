# 开发与发布

## 1. 本地环境

```bash
git clone https://github.com/dainan13/mylox.git
cd mylox
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"      # 运行 + 测试 + lint + 打包工具
```

`[dev]` extra 包含 `pytest`、`ruff`、`build`、`twine`。只要运行库用 `pip install -e .`，
只用测试用 `pip install -e ".[test]"`。

## 2. 目录结构

```
.
├── .github/workflows/
│   ├── ci.yml                    # push/PR：lint + 3.10–3.13 单测 + 打包校验
│   └── release.yml               # 推 v* tag：verify → build → GitHub Release → PyPI
├── docs/
│   ├── architecture.md           # 架构：schema 混合获取、引擎循环、事务与位点
│   ├── usage.md                  # 怎么写 handler（RowChange / on_init / 幂等 / 回调）
│   ├── configuration.md          # 连接设置、EngineConfig、过滤规则的两种方言
│   ├── operations.md             # 部署拓扑、启动行为、排障、已知限制、数据恢复
│   ├── compatibility.md          # MySQL 5.7/8.4 差异与实测结论、支持版本范围
│   ├── testing.md                # 单元/集成测试、环境变量、用例清单、实测结果
│   ├── development.md            # 本文件：本地环境、日常命令、发版流程
│   └── design-notes.md           # 设计取舍与缺陷修复对照
├── example/
│   └── mirror_to_pg.py           # 可运行示例：镜像表 + 类型映射 + 幂等 upsert
├── src/mylox/
│   ├── __init__.py               # 公共 API（__version__ 取自已安装的发行版）
│   ├── py.typed                  # PEP 561 类型标记
│   ├── config.py                 # MySQLSettings / PostgresSettings / EngineConfig
│   ├── constants.py              # source 常量、DDL 关键字、系统库名
│   ├── errors.py                 # 异常层级
│   ├── filtering.py              # TableFilter（gitignore / include 两种方言）
│   ├── gtid.py                   # GtidSet 区间运算、交并集
│   ├── logging_utils.py
│   ├── utils/
│   │   ├── positions.py          # BinlogPosition（按数字后缀比较文件名）
│   │   ├── rowimage.py           # columns_present_bitmap 清洗、列名映射
│   │   └── serialize.py          # bytes → str（无法解码时保留 bytes）
│   ├── schema/                   # ★ schema 混合获取
│   │   ├── model.py              # ColumnSchema / TableSchema、unsigned 修正
│   │   ├── registry.py           # SchemaRegistry：每表一条按位点排序的时间线
│   │   ├── plan.py               # DDLPlan / DDLChange / AlterOp（不依赖 sqlglot）
│   │   ├── ddl.py                # SqlglotDDLParser：SQL → DDLPlan
│   │   ├── salvage.py            # 无法建模的 DDL 的纯文本兜底（表名提取 / RENAME TABLE）
│   │   ├── evolve.py             # AlterOp 序列 → 新 TableSchema（纯函数）
│   │   ├── loader.py             # InformationSchemaLoader：从库 information_schema
│   │   └── manager.py            # SchemaManager：编排快照 / 主路径 / 兜底 / 历史
│   ├── state/
│   │   ├── models.py             # Checkpoint / SchemaHistoryRecord
│   │   └── store.py              # PgStateStore：事务连接 + 元数据连接
│   └── engine/
│       ├── handler.py            # CDCHandler / RowChange / ChangeAction
│       ├── context.py            # InitContext（on_init 用的连接工厂）
│       ├── pipeline.py           # PipelineState / PipelineSet（活跃集判定）
│       ├── reader.py             # BinLogStreamReader 工厂
│       └── engine.py             # CDCEngine 主循环
├── test/
│   ├── fakes.py                  # 内存替身（伪造 binlog 事件、PG store、MySQL loader）
│   ├── conftest.py               # 集成测试的环境变量与 fixture
│   ├── test_gtid.py / test_positions.py / test_rowimage.py / test_filtering.py
│   ├── test_registry.py / test_evolve.py / test_ddl.py / test_salvage.py
│   ├── test_loader_sql.py / test_state_models.py / test_pipeline.py
│   ├── test_schema_manager.py / test_config.py / test_example.py
│   ├── test_engine.py            # 端到端（伪 binlog 事件驱动）
│   └── test_integration.py       # 真实 MySQL 主从 + PG（设了环境变量才跑）
├── pyproject.toml                # 元数据、依赖、pytest/ruff 配置
├── MANIFEST.in                   # sdist 里包含哪些文件（docs/test/example 会打进去）
├── requirements.txt              # 仅运行依赖（给不用 PEP 517 的场景）
├── requirements-dev.txt          # 运行 + 测试/打包工具
├── README.md  CHANGELOG.md  CONTRIBUTING.md  LICENSE
└── .gitignore
```

---

补充说明：

* `docs/` 只进源码仓库，不会打进 wheel；
* `test/` 里的 `fakes.py` 是纯 Python 替身，任何单元测试都不需要数据库；
* `example/` 是一个可直接运行的完整例子，也被 `test/test_example.py` 覆盖。

## 3. 日常命令

```bash
ruff check                 # 静态检查（配置在 pyproject.toml）
ruff check --fix           # 自动修 import 顺序等
pytest test -q --ignore=test/test_integration.py    # 单元测试，秒级
pytest test -q             # 全部（集成用例需环境变量，否则自动 skip）
python -m build            # 打 sdist + wheel
python -m twine check dist/*                        # 校验元数据能否上 PyPI
```

代码风格：`line-length = 120`、`from __future__ import annotations`、
对外函数写类型注解、纯逻辑与 I/O 分离（能被单测覆盖的部分不碰数据库）。

## 4. 版本号与 CHANGELOG

* 版本号在 `pyproject.toml` 的 `[project] version`（目前手工维护，未用动态版本）；
* 发版时：改版本号 → 更新 `CHANGELOG.md`（Keep a Changelog 格式）→ 提交 →
  打 tag `vX.Y.Z` 并推送 → GitHub Actions 自动发布；
* 打包产物里包含 `py.typed`，类型检查器可以直接用。

## 5. 发布流程（GitHub Actions → PyPI）

`.github/workflows/release.yml` 在 **推送 `v*` tag** 时触发，串行三步：

```
verify (ruff + 单元测试) → build (sdist+wheel + twine check) → GitHub Release → PyPI
```

发布到 PyPI 用的是 **Trusted Publishing（OIDC）**，仓库里没有也不需要任何 API token：

* PyPI 上已配置 *pending publisher*：项目名 `mylox`、Publisher `GitHub`、
  Repository `dainan13/mylox`、Workflow `release.yml`、Environment `pypi`；
* workflow 的 publish job 必须使用 `environment: name: pypi` 和 `permissions: id-token: write`，
  两者与上面的登记一一对应，否则 OIDC 校验会失败；
* 首次发布成功后，PyPI 会把 pending publisher 转成正式 publisher。

发一版新版本：

```bash
# 1. 改 pyproject.toml 的 version 与 CHANGELOG.md
# 2. 提交并打 tag
git add -A && git commit -m "release: v0.8.0"
git tag v0.8.0
git push origin main --tags
```

`release.yml` 会在 Actions 里跑完 verify → build → 建 GitHub Release（附带 sdist/wheel，
自动生成 release notes）→ 上传 PyPI。若只想重试发布，在 Actions 里重跑同一个 tag 的 workflow 即可。

> 手动发布（不推荐，会绕过校验）：`python -m build && python -m twine upload dist/*`。

## 6. 支持的 Python 版本

`requires-python = ">=3.10"`。CI 矩阵覆盖 3.10–3.13，开发机在 3.14 上跑过全部用例。
代码里统一用 `from __future__ import annotations`，不要在运行时求值注解。
