# 库差异对比插件 — 实施计划

目标：对**当前选择的连接 + 库**，把**特定几张系统表的数据**和**表结构**，与一份历史快照对比，
导出可以拿到另一个环境执行的 SQL。插件全程只读，不改任何数据。

支持两个数据库族，按连接的 `type` 自动选（见 §1.1）。

---

## 0. 不变量（写进代码，不允许绕过）

1. **`diff(快照, 实时读)`** —— 永远拿快照和**当前实时读**比，绝不拿两份历史快照互比。
   这是自愈性的唯一来源：读窗口内变过的行，下一轮会被当新增补出去。一旦破例，那批行就永久漏了。

2. **结构差异和数据差异走两条独立通道。** 两边各采各的，生成到不同文件，互不影响。

3. **结构对象只增不删。**
   表 / 列 / 索引 / 外键的 `DROP` 一律不产出，但**必须在报告里列出来**——静默忽略才是危险的。
   （数据行的删除是产出的：那是「备份 + 删 + 插」这套形状的一部分，和结构是两回事。）

4. **列清单是发现的，不是编译进去的；漂移按快照的列比、按当前列存。**
   见 §4.1。任何一份编译进去的清单在某个环境上都会读出不存在的列——那是报错，不是空结果。

5. **快照记录的 `columns` 和它文件里的 hash 必须永远描述同一件事。**
   这是这个插件最容易悄悄出错的地方：对不上，下一趟对比就会把整张表每一行都报成"修改"。

6. **报告和生成文件里的过滤条件，永远是"本次用的那条"**，不是快照记的那条。
   生成的 SQL 里所有的 `WHERE` 和产生它的那次对比必须是同一个条件。

---

## 1. 两条通道的范围（不一样，别搞混）

| 通道 | 范围 | 来源 |
|---|---|---|
| 数据 | **六张系统表**，表清单写死；**列清单每次从库里发现** | `information_schema` |
| 结构 | **当前 schema 的全部表** | `information_schema` / `pg_index` |

结构通道按需支持**排除清单**（正则匹配表名，例如跳过 `*_bak_*` 备份表），默认空。

### 1.1 方言

| | 认哪些 `type` | |
|---|---|---|
| MySQL 族 | `mysql` `mariadb` `doris` `starrocks` | |
| PostgreSQL 族 | `postgres` `kingbase` `gaussdb` `highgo` `vastbase` `uxdb` `yashandb` `greenplum` `redshift` | **按协议族算，不是某一个产品**。金仓、GaussDB 都走 PG 协议，`dbx connections list` 一律报 `postgres` |

认不出的类型**明确报错**，不猜——拿 MySQL 语法去打 MongoDB 只会得到一个莫名其妙的服务器错误。
所有语法差异集中在 `backend/src/dialect.rs`，别的模块写一遍就够。

**实测差异**（`timeuse` 是 MySQL，`fnec_prod` / `eb169` 是 KingbaseES）：

| | MySQL | PostgreSQL |
|---|---|---|
| 标识符引号 | `` `x` `` | `"x"` |
| 当前库 | `DATABASE()` | `current_database()` |
| 结构作用域 | `TABLE_SCHEMA = '库名'` | `table_schema = current_schema()` |
| 表引用 | `` `库`.`表` `` | `"表"`（PG 不能跨库） |
| 条件式 | `IF(c,a,b)` | `CASE WHEN c THEN a ELSE b END` |
| 备份表 | `CREATE TABLE x LIKE y` | `CREATE TABLE x (LIKE y INCLUDING ALL)` |
| 改列类型 | `MODIFY COLUMN` | `ALTER COLUMN ... TYPE` |
| 加索引 | `ALTER TABLE ... ADD KEY` | `CREATE INDEX ... ON ...` |
| 表引擎 | `ENGINE=InnoDB` | 没有这个概念 |
| 索引来源 | `information_schema.STATISTICS` | `pg_index`（标准视图里根本没有索引） |
| 列类型 | `COLUMN_TYPE`（`varchar(64)`） | `udt_name` + 长度/精度拼出来 |
| 库选择器 | 列出所有库 | **只有连接自己那个库**（PG 连接不能跨库） |

数据通道两边几乎一样：`dbx query` 对两族**都把值返回成字符串**，`COUNT(*)`、
`ORDER BY ... LIMIT n OFFSET m`、行构造器 `(a,b) IN ((..),(..))` 全都通用。

**`information_schema` 的列名两边都按小写写**：MySQL 对它的大小写不敏感，PG 会把不加引号的
标识符折叠成小写，所以一条 `WHERE table_schema = ...` 两边都能跑。

### 1.2 名字的折叠

`tables.rs` 里那张表用的拼写（`dsfa_mm_valueAttributes_id`）在两个方言上都要能用：

- MySQL 列名大小写不敏感，原样用
- PG 会把不加引号的标识符折成小写，所以**引用前先折成小写**，否则加了引号就是区分大小写的

折叠只发生在**这一份固定清单**上。从 `information_schema` 读回来的名字（结构通道的表名列名）
是它们真实的拼写，不再折。一个折叠点，避免两份会漂移的清单。

---

## 2. 快照存储格式

不用数据库。**一个快照一个目录，纯文本文件**：

```
<插件数据目录>/snapshots/<连接目录>/<库目录>/<快照id>/
  meta.json
  data/<表名>.jsonl        -- 数据通道，每张表一个
  schema.jsonl             -- 结构通道，全库
```

- `<快照id>` = `20260917-143000`，同一秒重名就加 `-2`
- `<连接目录>` / `<库目录>` = 名字清洗 + `sha256(名字)` 前 8 位
  （连接名里有 `192.168.0.195:6397 | gjj:1` 这种，`:` `|` 在 Windows 上是非法文件名字符）

**快照的标识是「连接 + 库」，两个都是路径的一段。** 同一个连接下的两个库是两段互不相干的
历史：拿 `timeuse` 的快照去比 `timeuse_bak`，算出来的差异会变成一份改写错误数据的 SQL。
分成两级目录之后，两者在列表里和磁盘上都不可能混在一起。库变了要重新打快照，不能拿旧库的比。

（早期版本只有一级目录。第一次列快照时会把还在旧位置、带 `meta.json` 的目录按它自己记的
库名 `rename` 进对应的库目录——同卷重命名是原子的，中途崩最多留在原地。）

### 2.1 `meta.json`

```json
{
  "snapshot_id": "20260920-143000",
  "connection": "timeuse",
  "database": "timeuse",
  "taken_at": "2026-09-20T14:30:00+08:00",
  "plugin_version": "0.1.0",
  "hash_algo_version": 1,
  "session": { "time_zone": "SYSTEM", "sql_mode": "STRICT_TRANS_TABLES,...", "charset": "utf8mb4" },
  "schema_filter": "^(?!test_|t_).*$",
  "data_tables": [
    {
      "table": "dsfa_rm",
      "columns": ["dsfa_rm_id", "ds_create_time", "..."],
      "primary_key": ["dsfa_rm_id"],
      "filter": "ds_active = '1'",
      "row_count": 2276,
      "count_star": 2279,
      "collapsed": 3,
      "collapsed_samples": ["..."],
      "pages": 1,
      "sha256": "<file sha256>",
      "file": "data/dsfa_rm.jsonl"
    }
  ],
  "schema": { "object_count": 18422, "table_count": 467, "sha256": "<file sha256>" }
}
```

- **`columns` 存在快照里**，是**实际参与 hash 的那些列**（发现来的，见 §4.1）。老快照因此能
  自证"我这次 hash 覆盖了哪些列"，对比时可以直接判定怎么处理，而不是算出一堆假差异。
- **`primary_key` 也存**。标识列变了以后 hash 的含义就变了，对比时直接拒绝。
- **`filter` 是这张表这次实际用的条件**（内置条件 + 用户输入），不是用户输入。存它是为了让
  下一次对比能拿它和界面上当前的值比，不一致时先问。
- **`collapsed`**：多个行共用一个标识时，快照每个标识只能留一条，其余的**被丢弃**。丢弃是
  显式记录的，报告里单开一节列出来，也有 `collapsed_samples` 让人认得出是哪些。
- **`session` 只记录，没有任何代码读它。** 时区和 `sql_mode` 在项目层面强制一致，这里纯粹是留个记录。
- `count_star` 是**目标库原始 `COUNT(*)`** 的结果，和实际取到的 `row_count` 一起进报告。
  **不等只警告不中止**——采集期间表在变是正常的，标识折叠也会让它不等。

### 2.2 `data/<表名>.jsonl`

一行一个 JSON 对象，按排序键序：

```json
{"p":["1"],"h":"3f2a1c...c9"}
```

- `p` = **标识列的值**，字符串数组。必须存：被删掉的行在实时读里已经不存在，
  生成 `DELETE ... WHERE pk = ...` 的字面量只能从这里拿。
- `h` = 覆盖 `columns` 的内容 hash，hex。
- map 的 key 在内存里由 `p` 长度前缀拼接派生，**不落盘**——不把有歧义的编码写进文件。

### 2.3 `schema.jsonl`

一行一个结构对象，按 `(表名, 对象类型, 对象名)` 排序，**两个方言产出同一种形状**，
所以"什么算差异"那段逻辑只写了一遍：

```json
{"t":"dsfa_rm","o":"table","n":"","d":{"engine":"InnoDB","collation":"utf8mb4_general_ci"}}
{"t":"dsfa_rm","o":"column","n":"id","d":{"columnType":"int(11)","nullable":false,"default":null,"extra":"","charset":"utf8mb4","collation":"utf8mb4_general_ci","ordinal":1}}
{"t":"dsfa_rm","o":"index","n":"PRIMARY","d":{"primary":true,"unique":true,"type":"BTREE","columns":[{"name":"id","prefix":null}]}}
```

PG 那边 `engine` / `collation`（表级）是 `null`——它没有这两个概念——所以表选项那段分支
在两个方言上都不会误报。

### 2.4 为什么是这个格式

- 访问模式永远是"整个读进 HashMap"再对比，**不需要磁盘索引**，所以不上 SQLite
- sidecar 只依赖 `serde_json`，不引入任何新依赖
- 纯文本：`grep` / `diff` / 肉眼都能查，出问题好定位
- **排序确定 → 文件 sha256 稳定**，可以当"这轮到底变没变"的快速判断和完整性校验
- 不压缩，要压自己 zip

---

## 3. 采集

### 3.1 分页（数据通道）

每张表：

```sql
SELECT <标识列>, <全部列...> FROM <表> ORDER BY <主键列...> LIMIT 10000 OFFSET <n>
```

调 CLI 时 `--limit 10000` 与 SQL 里的 `LIMIT` 严格一致。标识符按方言引用（§1.1）。

- **`ORDER BY` 是硬要求**：不带它时行序无保证，页边界未定义，会漏行也会重行。
  排序列用**数据库自己的主键**（从 `information_schema` / `pg_index` 按序号取），它唯一、
  有索引、且不含 TEXT/BLOB（MySQL 上排 TEXT 会间歇性报 `ERROR 1038 Out of sort memory`）
- 终止条件：某一页返回 < 10000 行
- 表行数恰好是 10000 整数倍时会多查一次空页，正常
- 页内不加锁、不隔离；跨页偏移产生的问题由 §0.1 的自愈兜住
- 每张表额外跑一次 `SELECT COUNT(*)`（**用同一个过滤条件**）记进 `meta.json`

**为什么必须显式带 `--limit`**：`dbx query` 不带 `--limit` 时默认最多返回 10000 行**且不报错**
（`crates/dbx-core/src/db/mysql.rs:4054` → `unwrap_or(MAX_ROWS)`，`query.rs:32` 里 `MAX_ROWS = 10000`；
CLI 的 JSON 输出里没有 `truncated` 字段）。实测：25000 行的查询，不带 `--limit` 只回 10000 行。
页大小和 `--limit` 取同值，正好把这个默认上限消掉。

`OFFSET` 是 O(n²)（跳 9 万行要先扫过 9 万行）。系统表规模下无所谓。
真遇到大表再换 keyset（`WHERE (pk) > (上一页最后的 pk)`）——都有主键，改动很小。

**每次 `dbx query` 都是新连接**，一次快照要开十几个。远程库上任何一个握手卡住都会让整个任务
失败，所以**连接级**错误重试 3 次（间隔 2s / 4s / 6s）。这个列表是实测堆出来的，
每一条都对应一次真实失败：

```
connection timed out
connection pool checkout timed out            ← 服务端连接池没轮到握手就超时
timeout occurred while creating a new object  ← PG 驱动对同一件事的另一种说法
can't connect to mysql server / can't connect to server
communications link failure / lost connection to mysql server
connection reset / connection refused
```

**SQL 错误不重试**——它是确定性的，重试只会浪费时间并掩盖真正的报错。

### 3.2 结构采集（结构通道）

**MySQL**：全走 `information_schema`，不用 `SHOW CREATE TABLE`（版本、顺序、注释漂移会产生大量假差异）

- `TABLES` → 表清单、`ENGINE`、`TABLE_COLLATION`
- `COLUMNS` → 列名、`COLUMN_TYPE`、`IS_NULLABLE`、`COLUMN_DEFAULT`、`EXTRA`、字符集 / 排序规则、`ORDINAL_POSITION`
- `STATISTICS` → 索引名、列序、唯一性、前缀长度
- `KEY_COLUMN_USAGE` + `REFERENTIAL_CONSTRAINTS` → 外键

**PostgreSQL**：标准视图里没有的就从目录表取

- `information_schema.tables` → 表清单
- `information_schema.columns` → 列名、`udt_name`、长度 / 精度、`is_nullable`、`column_default`、`collation_name`、`is_identity`、`ordinal_position`
- `pg_index` + `pg_attribute`（配 `unnest(indkey) WITH ORDINALITY`）→ 索引名、键列（**按键序**）、
  `indisprimary` / `indisunique`、访问方法、部分索引的谓词
- `information_schema.table_constraints` + `key_column_usage` + `constraint_column_usage` +
  `referential_constraints` → 外键

**列类型文本**在 PG 侧是**拼**出来的，不是直接读来的：`udt_name` 是每个服务器上都在用的规范短名，
而 `data_type` 给的是 `timestamp without time zone` 这种既不是人写的、也不适合比较的措辞。
所以按 `udt_name` + 长度/精度拼成 `varchar(36)`、`numeric(10,2)`、`integer`、`timestamp`
这种能直接写进 DDL 的文本。

量级可能上万张表——按库 / schema 过滤后一次取回，不是每表一次。

**不采集**：列 / 表注释、`AUTO_INCREMENT`、分区、视图、触发器、存储过程。

### 3.3 行 hash 与字面量

**编码**（`hash_algo_version = 1`）：每个值 → `[是否 NULL 1B][长度 varint][原始字节]`，
按列顺序拼接后整体 SHA-256。

- 长度前缀**自定界**，没有任何分隔符歧义：`["ab","c"]` 和 `["a","bc"]` 编码不同
- **不含列名、不含列序号、不含类型**。类型进 hash 的话，某列改了类型会让全表每一行都算"修改"
- 列清单本身的变化由快照记录 + §4.1 处理，不由 hash 承担

**字面量生成**从结构通道拿列类型，不从 CLI 的返回值猜：

| 情况 | 写法 |
|---|---|
| SQL NULL | `NULL`（和空串严格区分） |
| 数值列且文本能解析成数字 | 裸字面量 |
| 其他一切 | 单引号 + 按标准规则转义 |

**没有十六进制形式**：这六张表里没有二进制列，所以没有 `X'...'` 这条路。真加了二进制列，
`render_value` 要跟着改——现在会把二进制当文本引号包起来。

---

## 4. 对比

### 4.1 数据通道

先把两边的列清单对齐，这一步决定了后面 hash 怎么算：

| 情况 | 处理 |
|---|---|
| 列清单一致 | 一次 hash，正常比 |
| 本库**多了**列 | 对比按**基准快照的列**算 hash（这些列都还在，两边含义才一致）；**新快照记本库当前的列**，下一趟就对齐了；报告里单开一节说明 |
| 本库**少了**列 | **拒绝对比**。基准快照的 hash 覆盖了一个已经不存在的值，重算不出来；硬比会把每一行都报成"修改" |
| 标识列变了 | **拒绝对比**。那重新定义了"一行是什么"，没什么可调和的 |

**多了列的时候，逐行这么判：**

| 这一行 | 处理 |
|---|---|
| 旧列 hash 不一样 | 正常走修改（备份 + 删 + 插），INSERT 带着新列 |
| 旧列 hash 一样、新列**全是空** | **不动**。表加了一列但每行都是空的，为这个写整张表没有意义 |
| 旧列 hash 一样、新列**有一个有值** | 也走修改，把新列的值推过去。hash 看不到它（那些列本来就在 hash 之外），但目标库没有这个值，不推过去就是漏了 |

「空」= NULL 或长度 0 的字符串。`NOT NULL` 的列存不了 NULL，所以那边"没有值"就长成 `''`。

**只在"多了列"那一趟才算两次 hash**（一次按快照的列给对比用，一次按本库的列给新快照用）。
一致时仍然只算一次——这是常态，不能变慢。

两边都按标识建 map，逐个 key：

```rust
match (old.get(key), live.get(key)) {
    (None, Some(_))              => Insert,   // INSERT
    (Some(_), None)              => Delete,   // 备份 + DELETE
    (Some(o), Some(l)) if o != l => Update,   // 备份 + DELETE + INSERT
    (Some(_), Some(_)) if 新列有值 => Update,   // 同上，见上表
    _                            => 未变
}
```

### 4.2 结构通道

| 差异 | MySQL | PostgreSQL | 落哪个文件 |
|---|---|---|---|
| 表不存在 | `CREATE TABLE` | `CREATE TABLE`（非主键索引另出 `CREATE INDEX`） | auto |
| 列不存在 | `ADD COLUMN` | `ADD COLUMN` | auto |
| 索引 / 唯一键 / 主键 不存在 | `ADD INDEX` / `ADD UNIQUE` / `ADD PRIMARY KEY` | `CREATE [UNIQUE] INDEX` | auto |
| 列类型**放宽** | `MODIFY COLUMN` | `ALTER COLUMN ... TYPE` | auto |
| 列类型收窄 / 转换 | `MODIFY COLUMN` | `ALTER COLUMN ... TYPE` | **review** |
| `ENGINE` / `CHARSET` / `COLLATE` 不同 | `ALTER TABLE ...` | 无此概念 | auto，报告标红 |
| 列被删除 | 不生成 | 不生成 | 只进报告 |
| 索引被删除 | 不生成 | 不生成 | 只进报告 |
| 表被删除 | 不生成 | 不生成 | 只进报告 |
| 注释差异 / `AUTO_INCREMENT` | 不比 | 不比 | — |

**PG 的非主键索引没法写在 `CREATE TABLE` 里面**（标准 SQL 的语法限制），所以新建表时它们是
跟在后面的独立 `CREATE INDEX`。MySQL 相反，是内联在 `CREATE TABLE` 里的——这个差异只在建
新表时出现，别的路径两边一致。

**类型放宽的判定**（`widens`）：只回答一个问题——**会不会丢数据**。会、或者拿不准，就进 review。
拿不准也进 review，是因为两边代价不对等：进 review 的代价是有人看一眼，进 auto 的代价是
静默截断一列生产数据。

判为放宽的只有这几类：

- 同 base 且容量只增不减（`varchar(100)` -> `varchar(1000)`）
- 整数族内向上（`int` -> `bigint`）、文本族内向上（`text` -> `longtext`）、二进制族内向上
- `float` -> `double`（PG 是 `real` -> `double precision`）
- `decimal` / `numeric` 要整数位数和小数位数都不减（`decimal(10,2)` -> `decimal(10,3)` 精度没变
  但少了一位整数位，会溢出，不算放宽）

`signed` / `zerofill` 之类的修饰符必须完全一致：`int(11)` -> `int(11) unsigned` 是换了种列，
每个值都可能要重写，不是放宽。`varchar` -> `text` 虽然确实变大，但会一并失去索引能力，
保守起见也进 review。

改列名在 naive 实现下看起来像「删 + 加」。既然删永远不生成，实际后果是：B 上多出一个空的新列、
旧列带着数据还在。**不丢数据，可以接受**。报告里会点出来，避免误以为迁移过了。

---

## 5. 输出

### 5.1 文件

```
<输出目录>/<连接名>/<库名>/<时间戳>/
  report.md              -- 人看的：每表增/改/删计数、被忽略的差异、警告
  00-precheck.sql        -- 前置校验，不符就中止
  01-schema-auto.sql     -- 建表 / 加列 / 加索引 / 引擎字符集 / 类型放宽
  02-schema-review.sql   -- 类型收窄或转换，人工判断
  03-data.sql            -- 新增 + 修改：备份 → 删除 → 插入（一行一条 INSERT）
  04-delete.sql          -- 对比出来是删除的行：备份 → 删除，用自己的备份表
  manifest.json          -- 本次输入（快照 id、连接、配置 hash、各文件 sha256）
```

**某一类一条语句都没有，那个文件不生成。** 空文件会让人以为「有东西要评审，只是没写进来」，
而且四个文件里两个是空的，就是多开两个文件。`manifest.json` 的 `files` 里只有真正写出来的那些，
所以「某一类没变更」和「这次没跑」能分得清。报告里逐条写明哪个文件没有生成。

**生成的文件不带库名**（MySQL 下也不写 `` `库`.`表` ``）：源和目标环境的库名通常不一样，
把源库名焊进语句里就是错的。操作者自己先选库——这也意味着**执行前必须选库**，
否则就是 `No database selected`。

**03 / 04 的列清单取的是新快照（本库当前），不是基准快照。** 值是按本库形状取的，列清单要是
按基准的来，`zip` 会把新列的值**静默丢掉**；列顺序一旦不同还会整体错位。这个 bug 是写实验室
（§7.2）抓出来的——只读的验收测试看不见它。

### 5.2 备份（在 B 上做）

备份取的是 **B 的实际当前行**，不是快照里的值：

```sql
CREATE TABLE IF NOT EXISTS <表>_bak_20260920_143000 LIKE <表>;
INSERT INTO <表>_bak_20260920_143000
  SELECT * FROM <表> WHERE (pk) IN (...);
DELETE FROM <表> WHERE (pk) IN (...);
```

- 备份表建在同一个库，`<表名>_bak_<时间戳>`，**不自动删除**。原来的
  `_dbx_bak_<时间戳>_<表名>` 把时间戳放前面，一次运行的所有备份按名字挤在一起，
  而不是按表名——但人找备份是按表名找的
- `CREATE TABLE ... LIKE` 建的是空表，**备份的数据只有 pk 列表里那些**，不是全表。
  PG 那边要 `(LIKE ... INCLUDING ALL)`，否则备份表没有主键
- **必须 `INSERT` 在 `DELETE` 之前**，且用同一个 pk 列表
- **这个 pk 列表包含新增和修改的 id**：目标库若已有一行同 id（源库从没有过的），
  不先删掉插入会撞唯一键；先删掉它被备份下来，再被源库的值覆盖
- **按对比结果分文件**：03 管新增和修改，04 管删除。两边的 id 集合不相交，所以先跑哪个都行，
  各自都能单独跑
- **两个文件各有自己的备份表**（`_bak_` / `_delbak_`），不是共用一张：单独跑 04 时，它删掉的
  东西也要能从自己的表里找回来
- **重跑是安全的**，但要整个文件重跑。备份表的 `LIKE` 带着同样的主键，所以重跑时已经备份过的
  那几条会因主键重复而失败——跳过即可，那说明已经备份过了
- **INSERT 一行一条**：哪一条失败就跳过哪一条，其余照跑，不用为了一行回退整个文件
- pk 列表按 500 个一组分块。上限不是个数而是内存：默认 `range_optimizer_max_mem_size` = 8 MB，
  实测（MySQL 8.0.36，32 字符 id）到 **10,000** 个左右 MySQL 放弃索引退回全表扫描并报警告 3170——
  是警告不是错误。500 离那儿有二十倍
- 复合主键用 row constructor：`WHERE (a, b) IN ((1,'x'), (2,'y'))`，两边通用

### 5.3 前置校验

一条语句查完全部表，`UNION ALL` 串起来，每行一个 `verdict`。用 `UNION ALL` 不用 `UNION`：
`UNION` 会顺手合并完全相同的行，而两张空表的两行恰好就是完全相同的。

```sql
SELECT 'dsfa_rm' AS table_name, 2279 AS expected, COUNT(*) AS actual,
       IF(COUNT(*) = 2279, 'ok', 'MISMATCH -- 数量不一致') AS verdict
  FROM `dsfa_rm` WHERE ds_active = '1'
UNION ALL
SELECT 'dsfa_mm' AS table_name, ...
  FROM `dsfa_mm` WHERE ds_active = '1'
...
;
```

- 每一行带着**自己的过滤条件**，和本次对比实际用的那条一致
- `expected` 是**目标库原始 `COUNT(*)`**，所以标识折叠不影响它
- 条件式按方言写（`IF` / `CASE WHEN`）
- 表里的标识折叠、`COUNT(*)` 取不到的注意事项都挪到语句上方——一条语句里没有"某张表旁边"
  可以挂注释了

目的：把「最坏情况 = 静默删错」变成「最坏情况 = 明确中止」。

### 5.4 顺序

`01-schema-auto` 在 `03-data` / `04-delete` 之前（先有列再有值）。
03 和 04 之间没有顺序要求——两边处理的 id 集合不相交。

跨文件不做事务——MySQL 和 PG 的 DDL 都隐式提交，做不到，不假装能做到。

**每条 DDL 前不再挂检测语句。** 执行时失败的跳过、修好后整个文件重跑，就够了；代价是半截
失败后得自己看失败的是哪条，换来的是文件短一半。

**已知未验**：`ADD COLUMN ... NOT NULL` 不带 `DEFAULT` 时，PG 在非空表上会报
`column "..." contains null values` 而失败（PG 11+ 只有带 `DEFAULT` 才走填充的快路径）；
MySQL 则会成功并给已有行填隐式默认值（`''` / `0`）。两个方言的可执行性不一样，而 01 的定位是
"可以无人值守跑完"。**PG 那半句没有实测过**——手上没有可写的 PG 环境，要坐实得在一张草稿表上
试一次。（MySQL 那半句在写实验室里验过：能执行。）

---

## 6. 插件结构

```
manifest.json          workbench contribution: dbx.demo.dbdiff.panel
ui/index.html          连接/库选择、六张表 + 过滤条件、表结构正则、快照列表、报告展示
backend/src/
  main.rs              JSON-RPC 方法表 + 连接 / 库列表
  dialect.rs           **所有方言差异**（引号、当前库、作用域、条件式、备份表、分页、会话探针）
  cli.rs               CLI 定位 + 调用 + 分页 + 连接级重试 + 连接类型缓存
  config.rs            过滤条件、表结构正则、输出目录
  tables.rs            六张表的固定清单（表名、标识、排序键、内置条件）+ 按方言折叠 + 列发现
  snapshot.rs          分页采集、行编码、列发现、后台任务状态
  schema.rs            MySQL 的结构采集 + **两个方言共用**的对比与 DDL 分发
  schema_pg.rs         PostgreSQL 的结构采集与 DDL
  diff.rs              两条通道的对比 + 报告 + SQL 写出
  encode.rs            行编码、hash、引号
  store.rs             快照读写、目录分层、文件 sha256
build.py               编译 + 打包 .dbxp
```

sidecar 方法：`host/info`、`connections/list`、`databases/list`、`config/get`、`config/save`、
`snapshot/list`、`snapshot/inspect`、`snapshot/create`、`snapshot/status`、`snapshot/cancel`、
`snapshot/delete`、`diff/run`、`diff/reveal`。

**全部只读**：不传 `--allow-writes`，任何写操作都到不了 A。

**方言是 sidecar 自己查的**（`cli::connection_kind`，进程内缓存一次 `dbx connections list`），
不从界面传——这样界面不用知道方言存在，直接驱动 sidecar 的测试脚本也能拿到正确的方言。

**后台任务**：单次插件 invoke 被宿主限流 120s，所以采集 / 对比跑在后台线程，起手就返回，
界面轮询 `snapshot/status`。取消是**杀掉正在跑的那个 `dbx` 子进程**，不是只设个标志——
一页数据在慢链路上可能要跑几分钟，只设标志的话点完取消会一直卡到查询返回。

### 6.2 打包：一个平台一个包

包里的 sidecar 是按某一个 target 编的，manifest 的 `entrypoints.backend.executable`
指向 `bin/<target>/<名字>`。DBX 的包布局就是这样（`plugins/README.md` 的 "Package layout"），
manifest schema 里 `executable` 是**单个路径**，没有平台映射表。

所以 `...-windows-x64.dbxp` 和 `...-darwin-arm64.dbxp` 是**同一份源码的两个产物**。
带原生 sidecar 的插件用不了 `universal`——SDK 文档写明那只有"同一个包在每个受支持
target 上都有效"时才用。

- `build.py -t <target>`，默认本机；`--list-targets` 列出来
- manifest 里那个路径**在打包时写**，不是留在源文件里手改——手改正是"Windows 包宣称自己是
  Mac 二进制"这类事故的来源
- 测出来的事实：`aarch64-apple-darwin` 上**这个 crate 和全部依赖都编译通过**，
  只在链接时因缺 macOS SDK 停下。**代码层面 Mac 侧是通的**，但 Mac 包没在真机上跑过
- 装错平台的包安装时不会被拒（宿主只查 manifest 指的文件在不在），启动 sidecar 时才失败

### 6.3 dbx CLI 在哪（macOS 上这是真问题）

插件所有读库都靠 `dbx query` 子进程，所以「找到 dbx CLI」是它的启动前提。
`cli::resolve` 按顺序试：

| # | 来源 | 说明 |
|---|---|---|
| 1 | `DBX_CLI_BIN` | 显式指定，照用 |
| 2 | 配置里的 `cliPath` | 下面全落空时的人工兜底 |
| 3 | npm 全局前缀 | 见下 |
| 4 | PATH 上的**真程序** | 跳过 `target/debug`、`target/release` |
| 5 | PATH 上的 npm shim | 不能直接跑，但它指明了包在哪 |
| 6 | 登录 shell 的 `command -v dbx` | 只在 mac / linux，一个进程一次，10s 超时 |

**为什么 Windows 一直没事、mac 上有事。** Windows 的全局前缀是
`%APPDATA%/npm/node_modules` —— 一个绝对路径，第 3 步就命中。mac 上 Node 多半是版本管理器
装的（nvm / fnm / volta / asdf），全局前缀落在
`~/.nvm/versions/node/<版本>/lib/node_modules` 这类**要列目录才知道**的地方；更要命的是
**从 GUI 启动的进程不继承终端的 PATH**，launchd 只给一条最简 PATH —— 用户在终端里 `dbx`
好好的，插件里就是找不到。DBX 自己给 MCP 找 `node` 用的是同一个办法
（`src-tauri/src/commands/mcp.rs` 的 `user_shell_node_candidate`），这里跟它保持一致。

第 3 步的候选前缀：`~/.npm-global`、`~/.local`、`~/.npm`、yarn 与 bun 的全局目录、
上面那些版本管理器的全局目录（按版本号从新到旧，`v9` 不会盖过 `v20`）、
`NVM_DIR`、`PNPM_HOME`、`NODE_PATH`，以及 `/opt/homebrew`（Apple Silicon Homebrew）、
`/usr/local`（Intel Homebrew 与官网安装包）、`/opt/local`（MacPorts）、`/usr`。
一个候选没命中只花一次 `stat`，所以这里宁可多猜——猜漏了有第 6 步兜着。

第 5 步值得单说：npm 装出来的 `dbx` 是**软链到 `bin/dbx.js` 的脚本**，直接 spawn 要能找
`node`，而 GUI 进程恰恰没有。所以它不能当程序用，但能当**指针**用 —— `realpath` 之后它就是
`.../@dbx-app/cli/bin/dbx.js`，往上两级是包目录，原生二进制在
`<包目录>/node_modules/@dbx-app/cli-<os>-<arch>/bin/dbx`（npm 的嵌套布局），
或者在某个祖先目录的 `node_modules` 下（pnpm 的 store、以及被提升的依赖）。
两个形状都试，`realpath` 先走 —— pnpm 和 `npm link` 都是软链装的，**链接的目标**才指对地方，
所以 pnpm 不需要特殊处理。

找不到时，报错把**查过的每一个位置**列出来，并附上配置文件的位置。mac 上叫用户
「设个环境变量」是不现实的：GUI 应用没有地方设，所以能用的兜底是配置文件里的 `cliPath`，
填了就不再用自动查找，路径不存在直接报错而不是悄悄回退。**界面上没有这一项**——
它是给「全部自动查找都落空」准备的，摆个输入框只会让人以为必须手动填。

（`testenv.py` 是同一套顺序的 Python 版：测试脚本必须驱动**插件会用的那个** CLI，
否则测的不是插件。）

### 6.1 几处默认值

| 项 | 默认 | 理由 |
|---|---|---|
| 表名 / 标识列 / 排序键 / 内置条件 | 写死在 `tables.rs` | 定义的是语义，不能跟着数据漂 |
| 列清单 | **每次采集从 `information_schema` 发现** | 见 §0.4 |
| 快照存放 | 插件数据目录（从 sidecar 的 cwd 反推 `.../<id>/versions/<ver>` → `.../plugin-data/<id>`） | 跟着 DBX 走，便携模式也不丢 |
| 生成文件落哪 | 插件数据目录的 `output`，界面显示完整路径 + 打开 + 复制 | 不弹保存对话框，批量产出更顺 |
| 快照保留 | 全留（体积小），界面可手动删 | 不需要自动清理 |
| `dbx` CLI 路径（`cliPath`，只有配置文件） | 空 = 自动查找（§6.3） | 只有自动查找失败才需要填 |

---

## 7. 验收

三个命令，各自覆盖不同的层：

```bash
python build.py -t <target>                               # 打包，见 §6.2
cargo test --release --manifest-path backend/Cargo.toml   # 单元测试
python acceptance-test.py timeuse                         # 端到端，只读（MySQL）
python acceptance-test.py fnec_prod                       # 同一套检查跑在 PostgreSQL 上
python drift-lab.py                                       # 会写库，见 §7.2
```

测试脚本的路径从 `testenv.py` 取（sidecar 二进制、dbx CLI），按平台解析，所以两个脚本
在 Mac 上能直接跑。

### 7.1 只读验收（`acceptance-test.py`）

**契约：绝不执行任何 SQL。** 同一套检查两族都跑，脚本从连接的 `type` 认出方言，
引号、`CREATE TABLE ... LIKE`、`MODIFY COLUMN`、有没有 `ENGINE` 这些期望值跟着变。
**两条命令都必须是 ALL CHECKS PASSED**，改动才算数。

覆盖：

- 拿自己的快照比自己必须是 **0 差异**，而且这时除了 `00-precheck.sql` 和 `report.md`
  **一个 sql 文件都不生成**
- 通过**改基准快照文件**注入差异，逐类验证方向和落点（这和真实变更的形状一样：
  基准里没有的就是新增，只在基准里的就是被删的）
- **同一次注入里一列放宽、一列收窄**，验证前者落 01、后者落 02
- **列明细漂移**：从基准的记录里拿掉一列（等于"表多了这一列"），验证不拒绝对比、
  漂移被报告、新快照记录本库的列，以及**漂移那一趟写出的快照自比必须 0 差异**
- **03 是备份-删除-插入、一行一条 INSERT**（删除列表包含新增的 id），**04 只删不写、
  用自己那张备份表**
- 过滤条件变化时**不启动任务**而是返回待确认，且报告和生成文件里的过滤条件
  都是**本次用的那条**
- 库是必填参数、快照落在「连接 + 库」两级目录下、点名的库在 `databases/list` 里

### 7.2 写实验室（`drift-lab.py`）

只读验收验不了三件事，所以单独一个脚本，**明确会写**：

1. **真的列漂移**。只读那边是模拟的，模拟不出"旧列 hash 相同、新列有值"这个判定场景
2. **生成的 SQL 到底能不能跑**。这个项目里没有别的东西执行过它
3. **`ADD COLUMN ... NOT NULL` 的真实行为**（见 §5.4）

```bash
python drift-lab.py 127.0.0.1 test_a    # 默认值
```

- **拒绝**对不以 `test_` 开头的库运行（要强行跑得显式 `--force`）
- 清理只按本轮**确切生成**的备份表名，不用 `LIKE` 模式——SQL 里 `_` 是单字符通配符，
  `'%_bak_%'` 会连别人的备份表一起匹配上
- 跑完把库恢复原状

**实测结果**（85 行的 `dsfa_route_version` 加一列、填 5 行、其余留空）：

```
{"inserts": 0, "updates": 5, "deletes": 0, "unchanged": 80}
00-precheck.sql 跑通，六行 verdict 全是 ok
03-data.sql 执行成功，表状态不变
```

### 7.3 单元测试

行编码 / 过滤条件拼接 / 类型放宽判定 / 前置校验的形状 / 漂移的三条规则 /
`ScannedRow` 在无第二次 hash 时回退到第一次。

**界面 JS 没有完整的自动化测试**，但 markdown 渲染器（表格分隔行、按列上色、⚠ 标题）可以
单独在 node 里跑；状态栏那几处逻辑（提示的显示与清除、清除按钮的启用条件）是从 `index.html`
里抠出函数、用桩 DOM 跑的。界面本身要人工点。

---

## 8. 明确不做

视图 / 触发器 / 存储过程 / 事件、分区、三路合并、表或列的删除、注释同步、
`AUTO_INCREMENT` 同步、时区与 `sql_mode` 适配（项目层面已强制一致）、
B 侧连接（插件从不连 B）、快照自动清理、加密、压缩、
**跨方言对比**（拿 MySQL 的快照比 PG 的实时读）。

## 9. 已知风险

- **B 不校验**：脚本执行前 B 是否等于基准快照，除了 `00-precheck.sql` 没有别的保障。
  能连 B 的话，对 B 也打一次快照然后 `diff(B现, A现)`，这个问题彻底消失，而且代码更少。
- **半截失败**：DDL 隐式提交，跑到中间炸了就是半截状态。缓解是「文件里的语句重复执行是安全的」
  加上失败时人工跳过——但**没有自动判断从哪继续**的手段了（检测语句已移除）。
- **`ADD COLUMN ... NOT NULL` 在 PG 上会失败**，而它在 01 文件里。见 §5.4，未实测。
- **PG 的结构作用域写死 `current_schema()`**。表在别的 schema 且不在 `search_path` 上的环境
  需要再加配置。目前两个 PG 环境都是 `public`。
- **PG 的列 `collation` 参与对比**。两个环境的默认排序规则不同的话，每个文本列都会报
  "列定义变化"——是真差异，但可能会吵。
- **漂移那一趟，"旧列变了"和"只有新列有值"两种行在 SQL 里长得一样**。报告里给出了后者的条数，
  但文件里看不出区别。
- **字面量只有一半验证过**：写实验室把生成的语句真的执行了一遍，这证明它们语法正确、能跑通；
  但"写进去再读出来逐列相同"这个往返验证还没做。二进制列目前会被当文本引号包起来
  （这六张表里没有，所以还没暴露）。
- **Mac 包只有编译层面的保证**。CI 在 Apple 的 runner 上编得出三个平台的包，但
  **没有一个包在真机上装过、跑过**。测试脚本和打包脚本都按平台参数化了，
  但**从没在非 Windows 上运行过**。
- **CLI 定位在 mac 上是猜出来的**（§6.3）。第 3 步那张前缀表是在 Windows 上照着
  Node 生态的常见布局写的，只有 `%APPDATA%` 那一条在真机上验过。第 6 步的 shell 探针是
  照着 DBX 自己的做法写的，也没在 mac 上跑过。两条都不中时，用户会看到一张查过的位置清单，
  以及配置文件里那个可以手填的 `cliPath`。
