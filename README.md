# dv — 数据集版本库

基于 Python 标准库 `sqlite3` 的追加式数据集版本库。提供方持续发布新的
schema 版本并始终按最新版本写入；消费者钉（pin）在某个版本上，读到的每条
记录都按自己钉住的版本呈现，直到显式升级。

## 数据模型

- **Schema**：一组按名字识别的列，版本号从 1 递增。
- **Column**：`name`、`type`（`int64` / `float64` / `string`）、
  `nullable`、`default`（缺省为 `NO_DEFAULT` 表示无默认值）、数值列可带
  闭区间 `[min, max]`。
- **记录**：只追加，不修改不删除；以版本 w 写入的记录在 w 的每列上都有
  值或 null。

## 跨版本读取规则（w 写入 → r 读出）

对 r 的每一列 c：

1. w 没有 c：取 r 中 c 的默认值；无默认值时，可空则为 null，否则不可读。
2. w 有 c 且值为 null：r 中 c 可空则为 null，否则不可读（默认值不能顶替
   显式 null）。
3. w 有 c 且值非 null：同类型原样保留；`int64→float64` 要求 |v| ≤ 2^53；
   `float64→int64` 要求 v 是整数且 |v| ≤ 2^53；其他跨类型一律不可读。
   呈现值必须满足 r 中 c 的范围。
4. w 多出来的列丢弃。

## API

```python
from dv import Column, Schema, Repository, INT64, FLOAT64, STRING

repo = Repository("repo.db")            # 或 Repository() 使用内存库
repo.publish_version("ds", Schema([Column("x", INT64, min=0, max=100)]))  # -> 1
repo.push_records("ds", [{"x": 10}])    # 按最新版本写入，整批原子
repo.pin("consumer", "ds")              # 钉到当前最新版本 -> 1
repo.read_snapshot("consumer", "ds")    # -> [{"x": 10}]
```

- `publish_version(dataset, schema) -> int`：发布新版本。仅在以下两种情况
  拒绝（抛 `PublishRejected`），其余情况必须接受：
  1. 存量记录里至少有一条不能按新版本读出；
  2. 存在某个被钉住的版本，任何以新版本写入的合法记录都不可能按它读出
     （逐列判定：缺列时需默认值或可空；同名列需存在合法值——类型可转换、
     区间交集非空，或双侧可空走 null 路径）。
- `push_records(dataset, records)`：校验每条记录符合最新 schema，且每条
  记录都能按所有被钉住的版本读出；任一不满足则整批不写
  （`InvalidRecord` / `PushRejected`）。
- `pin(consumer, dataset) -> int`：首次授权与显式升级共用，钉到当前最新
  版本。
- `read_snapshot(consumer, dataset)`：按钉住的版本读出全部记录；由上述
  两道关卡保证绝不跳过、改写、补 null，也不会失败。

## 硬性要求的保证方式

设 P 为所有被钉住的版本加上最新版本：

- `publish_version` 在发布前逐条检查存量记录对新版本可读（条件 1），并
  对每个被钉住的版本做“存在合法可写记录”的可行性判定（条件 2）。
- `push_records` 在写入前逐条检查整批记录对每个被钉住版本可读。
- 因此库中每条记录在任何时刻都能按 P 中每个版本读出，
  `read_snapshot` 不可能失败。

## 存储

单个 SQLite 文件（或 `:memory:`），表：`datasets`、`schema_versions`、
`columns`、`records`、`record_values`、`pins`。记录值按列存储并带类型
标签（`int` / `float` / `str` / `null`），读取时按写入版本与目标版本的
schema 做转换。

## 测试

```sh
python3 -m unittest discover -s tests -v
```

包含题目要求的场景：消费者钉在 v1，v2 放宽 int64 列范围，v3 将该列改为
float64；升级前按 v3 写入超出 v1 范围的值被 `PushRejected` 拒绝，重新
`pin` 之后同一批记录写入成功（`tests/test_versioning.py` 中
`TestRequiredScenario`）。
