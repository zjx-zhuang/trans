# Hive to BigQuery SQL 转换检查清单

按 DDL、DML、DQL 分类整理的转换注意事项。

---

## 📋 目录

1. [通用规则](#通用规则)
2. [DDL (数据定义语言)](#ddl-数据定义语言)
3. [DML (数据操作语言)](#dml-数据操作语言)
4. [DQL (数据查询语言)](#dql-数据查询语言)
5. [函数转换](#函数转换)
6. [表引用规则](#表引用规则)
7. [变量处理](#变量处理)

---

## 通用规则

### ⚠️ 变量处理策略（优先阅读）
- **默认行为**：保留调度变量为字符串字面量
- **与 DATE 列比较**：用 `DATE()` 包装变量
  - `WHERE d = '${{zdt.format("yyyy-MM-dd")}}'` → `WHERE d = DATE('${{zdt.format("yyyy-MM-dd")}}')`
- **与 STRING 列比较**：保持原样

### 数据类型转换
| Hive | BigQuery |
|------|----------|
| INT, SMALLINT, TINYINT | INT64 |
| BIGINT | INT64 |
| FLOAT, DOUBLE | FLOAT64 |
| BOOLEAN | BOOL |
| DECIMAL(p,s) | NUMERIC or BIGNUMERIC |
| MAP<K,V> | JSON or STRUCT |

### 类型转换关键点
1. **字符串与数值比较**：必须使用 `SAFE_CAST`
   - `string_col > 0` → `SAFE_CAST(string_col AS INT64) > 0`
   - 常见 ID 列：`masterhotelid`, `cityid`, `country_flag` 等

2. **日期列处理**：
   - ❌ 错误：`WHERE PARSE_DATE('%Y-%m-%d', d) = ...`
   - ✅ 正确：`WHERE d = DATE('2024-01-01')`

3. **NVL/COALESCE 类型匹配**：
   - 数值列：`nvl(star, '')` → `COALESCE(star, 0)`
   - ID 列：`nvl(hotel_id, '')` → `COALESCE(CAST(hotel_id AS STRING), '')`

### 表名格式
- **所有表名必须用反引号包裹**：`` `project.dataset.table` ``
- **包含连字符的项目 ID**：必须用反引号，否则会被解释为减法运算符

### 需要移除的 Hive 特定语法
- `USE database;` → 移除
- `DISTRIBUTE BY col` → 移除
- `CLUSTER BY col` → 移除或改为 `ORDER BY`
- `SORT BY col` → `ORDER BY col`
- `STORED AS format` → 移除
- `ROW FORMAT ...` → 移除
- `SERDE ...` → 移除
- `TBLPROPERTIES (...)` → 移除或改为 `OPTIONS`
- `/*+ HINT */` → 移除

---

## DDL (数据定义语言)

### CREATE TABLE

#### 基本转换
```sql
-- Hive:
CREATE TABLE t (col1 INT, col2 STRING) 
PARTITIONED BY (dt STRING) 
STORED AS PARQUET

-- BigQuery:
CREATE TABLE `t` (col1 INT64, col2 STRING) 
PARTITION BY dt
-- 注意：移除 STORED AS, ROW FORMAT, SERDE, TBLPROPERTIES
```

#### 分区表
- Hive: `PARTITIONED BY (dt STRING)`
- BigQuery: `PARTITION BY dt`（不需要指定类型）

#### 动态表名（必须使用 EXECUTE IMMEDIATE）
```sql
-- ❌ 错误（DDL 不支持通配符）：
CREATE OR REPLACE TABLE `project.dataset.result_*` AS ...

-- ✅ 正确：
DECLARE date_suffix STRING DEFAULT FORMAT_DATE('%Y%m%d', CURRENT_DATE());
EXECUTE IMMEDIATE FORMAT('''
  CREATE OR REPLACE TABLE `project.dataset.result_%s` AS
  SELECT * FROM source_table
''', date_suffix);
```

### ALTER VIEW
```sql
-- Hive:
ALTER VIEW view_name AS SELECT ...

-- BigQuery:
CREATE OR REPLACE VIEW `view_name` AS SELECT ...
```

### 关键限制
1. **DDL 不支持通配符 `*`**：只能用于 SELECT 查询
2. **动态表名必须用 EXECUTE IMMEDIATE**
3. **分区规范必须匹配**：替换已存在的分区表时，必须包含 `PARTITION BY`

---

## DML (数据操作语言)

### INSERT OVERWRITE

#### 基本转换
```sql
-- Hive:
INSERT OVERWRITE TABLE target_table SELECT ...

-- BigQuery:
CREATE OR REPLACE TABLE `target_table` AS SELECT ...
```

#### 分区处理（关键）
**Hive vs BigQuery 分区列处理：**
- Hive: 分区列在 `PARTITION(col=val)` 子句中，**不在 SELECT 列表**
- BigQuery: 分区列**必须在 SELECT 列表末尾**

```sql
-- Hive:
INSERT OVERWRITE TABLE db.target_table PARTITION (d='2024-01-01')
SELECT user_id, order_amount, order_count
FROM source_table;

-- BigQuery:
CREATE OR REPLACE TABLE `project.dataset.target_table` 
PARTITION BY d  -- ← 必须添加分区规范
AS
SELECT 
    user_id,
    order_amount,
    order_count,
    DATE('2024-01-01') AS d  -- ← 分区列必须在 SELECT 末尾
FROM `project.dataset.source_table`;
```

#### 多分区列
```sql
-- Hive:
INSERT OVERWRITE TABLE result PARTITION (country='US', dt='2024-01-01')
SELECT id, name, amount FROM orders;

-- BigQuery:
CREATE OR REPLACE TABLE `project.dataset.result` AS
SELECT 
    id, 
    name, 
    amount,
    'US' AS country,      -- 分区列1
    DATE '2024-01-01' AS dt  -- 分区列2
FROM `project.dataset.orders`;
```

#### 动态分区
```sql
-- Hive:
INSERT OVERWRITE TABLE result PARTITION (dt)
SELECT id, name, amount, order_date AS dt FROM orders;

-- BigQuery:
CREATE OR REPLACE TABLE `project.dataset.result` 
PARTITION BY dt
AS SELECT id, name, amount, order_date AS dt FROM `project.dataset.orders`;
```

### INSERT INTO
- 基本语法相同，但需要确保表名有反引号

### 关键注意事项
1. **分区列必须在 SELECT 列表末尾**
2. **替换分区表时必须包含 `PARTITION BY`**
3. **动态表名必须用 EXECUTE IMMEDIATE**（不能用于 DDL）

---

## DQL (数据查询语言)

### SELECT 基本规则

#### 表引用
- **真实表**：必须添加数据集前缀 `` `project.dataset.table` ``
- **CTE**：不需要前缀
- **子查询别名**：不需要前缀
- **UNNEST 别名**：不需要前缀

#### 识别虚拟表 vs 真实表
1. 扫描整个查询，找出所有 CTE 和子查询定义
2. 任何未定义的名称都是真实表 → 需要前缀

### JOIN 类型转换

#### LEFT ANTI JOIN
```sql
-- Hive:
SELECT t0.* FROM table_a t0
LEFT ANTI JOIN table_b t1 ON t0.id = t1.id

-- BigQuery:
SELECT t0.* FROM `table_a` t0
LEFT JOIN `table_b` t1 ON t0.id = t1.id
WHERE t1.id IS NULL

-- 或使用 NOT EXISTS:
SELECT t0.* FROM `table_a` t0
WHERE NOT EXISTS (
    SELECT 1 FROM `table_b` t1 WHERE t0.id = t1.id
)
```

#### LEFT SEMI JOIN
```sql
-- Hive:
SELECT t0.* FROM table_a t0
LEFT SEMI JOIN table_b t1 ON t0.id = t1.id

-- BigQuery:
SELECT t0.* FROM `table_a` t0
WHERE EXISTS (
    SELECT 1 FROM `table_b` t1 WHERE t0.id = t1.id
)
```

### LATERAL VIEW / EXPLODE

#### 基本 EXPLODE
```sql
-- Hive:
SELECT id, item FROM t LATERAL VIEW explode(items) tmp AS item

-- BigQuery:
SELECT id, item FROM t CROSS JOIN UNNEST(items) AS item
```

#### UNNEST 别名位置（关键）
```sql
-- ❌ 错误（别名在括号内）：
CROSS JOIN UNNEST([...] jt)

-- ✅ 正确（别名在括号后）：
CROSS JOIN UNNEST([...]) AS jt
```

#### json_tuple（不要用 UNNEST）
```sql
-- Hive:
LATERAL VIEW json_tuple(t.json_col, 'name', 'age') jt AS name, age

-- BigQuery（直接提取，不用 UNNEST）：
SELECT 
    JSON_EXTRACT_SCALAR(t.json_col, '$.name') AS name,
    JSON_EXTRACT_SCALAR(t.json_col, '$.age') AS age
FROM `table` t
```

### 窗口函数

#### 基本语法
- 大部分窗口函数语法相同：`ROW_NUMBER()`, `RANK()`, `LAG()`, `LEAD()` 等

#### HAVING 子句限制（关键）
**BigQuery 的 HAVING 只能过滤聚合函数，不能过滤窗口函数结果！**

```sql
-- ❌ 错误：
SELECT *, ROW_NUMBER() OVER(...) AS rk FROM t HAVING rk = 1

-- ✅ 正确：
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER(...) AS rk FROM t
) WHERE rk = 1
```

### GROUP BY

#### GROUPING SETS
```sql
-- Hive 和 BigQuery 语法相同：
GROUP BY GROUPING SETS ((a, b), (a), ())
-- 或
GROUP BY ROLLUP(a, b)
-- 或
GROUP BY CUBE(a, b)
```

#### 非聚合列
- 所有非聚合的 SELECT 列必须在 GROUP BY 中

### UNION ALL
- 每个分支必须有相同的列，且顺序相同
- 保留列别名

### 超长 SQL 处理（200+ 列）
1. **保持列顺序**：不要重新排序
2. **保留列别名**：保持所有 `AS alias` 名称
3. **保留 CAST 表达式**：如 `CAST(x AS DECIMAL(18,4))`
4. **保留注释**：保留 SQL 注释
5. **UNION ALL 一致性**：每个分支列顺序相同

---

## 函数转换

### 日期/时间函数
| Hive | BigQuery |
|------|----------|
| `date_format(date, 'yyyy-MM-dd')` | `FORMAT_DATE('%Y-%m-%d', date)` |
| `datediff(end, start)` | `DATE_DIFF(end, start, DAY)` |
| `date_add(date, n)` | `DATE_ADD(date, INTERVAL n DAY)` |
| `date_add(date, -n)` | `DATE_SUB(date, INTERVAL n DAY)` |
| `add_months(date, n)` | `DATE_ADD(date, INTERVAL n MONTH)` |
| `from_unixtime(ts)` | `TIMESTAMP_SECONDS(CAST(ts AS INT64))` |
| `unix_timestamp()` | `UNIX_SECONDS(CURRENT_TIMESTAMP())` |
| `to_date(ts)` | `DATE(ts)` |
| `year(date)` | `EXTRACT(YEAR FROM date)` |
| `trunc(date, 'MM')` | `DATE_TRUNC(date, MONTH)` |

### 字符串函数
| Hive | BigQuery |
|------|----------|
| `concat_ws(sep, a, b, ...)` | `ARRAY_TO_STRING([a, b, ...], sep)` |
| `instr(str, substr)` | `STRPOS(str, substr)` |
| `locate(substr, str)` | `STRPOS(str, substr)` |
| `get_json_object(json, '$.key')` | `JSON_EXTRACT_SCALAR(json, '$.key')` |
| `json_tuple(json, 'k1', 'k2')` | `JSON_EXTRACT_SCALAR(json, '$.k1'), JSON_EXTRACT_SCALAR(json, '$.k2')` |

### 聚合函数
| Hive | BigQuery |
|------|----------|
| `collect_list(col)` | `ARRAY_AGG(col IGNORE NULLS)` |
| `collect_set(col)` | `ARRAY_AGG(DISTINCT col IGNORE NULLS)` |
| `percentile_approx(col, 0.5)` | `APPROX_QUANTILES(col, 100)[OFFSET(50)]` |

### Map/JSON 函数

#### Map 访问
```sql
-- Hive:
map_col['key']

-- BigQuery（JSON 类型）：
JSON_VALUE(map_col, '$.key')

-- BigQuery（ARRAY<STRUCT> 类型）：
(SELECT value FROM UNNEST(map_col) WHERE key = 'target_key')
```

#### ARRAY<STRUCT<key, value>> 类型
**当 Hive MAP 存储为 `ARRAY<STRUCT<key STRING, value STRING>>` 时：**

```sql
-- ❌ 错误（不能用 JSON_EXTRACT_SCALAR）：
JSON_EXTRACT_SCALAR(map_col, '$.target_key')

-- ✅ 正确（用 UNNEST 子查询）：
(SELECT value FROM UNNEST(map_col) WHERE key = 'target_key')
```

#### to_json
```sql
-- Hive:
udf.to_json(map('key1', v1, 'key2', v2))

-- BigQuery:
TO_JSON(JSON_OBJECT('key1', v1, 'key2', v2))
```

### 数组函数
| Hive | BigQuery |
|------|----------|
| `size(array)` | `ARRAY_LENGTH(array)` |
| `array_contains(arr, val)` | `val IN UNNEST(arr)` |
| `explode(arr)` | `UNNEST(arr)` |
| `posexplode(arr)` | `UNNEST(arr) WITH OFFSET` |

---

## 表引用规则

### 需要数据集前缀（真实表/视图）
- `db_name.table_name` → `` `project.dataset.db_tablename` ``
- 独立表名（未在 WITH 中定义）→ `` `project.dataset.tablename` ``
- 视图（`v_xxx`, `xxx_view`）→ 视为真实表，需要前缀

### 不需要数据集前缀（虚拟表）
- **CTE**：`WITH name AS (...)` 定义的
- **子查询别名**：`(SELECT ...) AS alias`
- **表别名**：`FROM real_table alias` 中的 `alias`
- **UNNEST 别名**：`CROSS JOIN UNNEST(...) AS alias`

### 识别步骤
1. 扫描整个查询，找出所有 CTE 和子查询定义
2. 任何未定义的名称都是真实表 → 需要前缀

### 常见模式
- `exploded_data`, `derived_data`, `tmp_xxx` → 可能是 CTE，检查 WITH 子句
- `v_dim_xxx`, `dim_xxx_df` → 真实视图，需要前缀
- `ods_xxx`, `dw_xxx`, `dwhtl.xxx` → 真实表，需要前缀

---

## 变量处理

### 调度变量（默认保留）
- **保留原样**：`${{zdt.format("yyyy-MM-dd")}}`
- **与 DATE 列比较**：`WHERE d = DATE('${{zdt.format("yyyy-MM-dd")}}')`
- **与 STRING 列比较**：`WHERE str_col = '${{zdt.format("yyyy-MM-dd")}}'`

### 变量映射表（可选转换）
| Hive 变量 | BigQuery 等价 |
|-----------|---------------|
| `${{zdt.format("yyyy-MM-dd")}}` | `CURRENT_DATE()` |
| `${{zdt.addDay(-1).format("yyyy-MM-dd")}}` | `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` |
| `${{zdt.addMonth(-1).format("yyyy-MM")}}` | `FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))` |

### 动态表名

#### SELECT（可用通配符）
```sql
-- Hive:
SELECT * FROM db.table_${{zdt.format("yyyyMMdd")}}

-- BigQuery:
SELECT * FROM `project.dataset.table_*`
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE())
```

#### DDL（必须用 EXECUTE IMMEDIATE）
```sql
-- Hive:
INSERT OVERWRITE TABLE db.result_${{hivevar:date_suffix}} SELECT ...

-- BigQuery:
DECLARE date_suffix STRING DEFAULT FORMAT_DATE('%Y%m%d', CURRENT_DATE());
EXECUTE IMMEDIATE FORMAT('''
  CREATE OR REPLACE TABLE `project.dataset.result_%s` AS
  SELECT * FROM source_table
''', date_suffix);
```

---

## 常见错误修复

### 类型错误
- **String-to-Number**：使用 `SAFE_CAST(string_col AS INT64)`
- **COALESCE 类型不匹配**：数值列用 `0`，ID 列用 `CAST(... AS STRING)`

### 语法错误
- **缺少反引号**：所有表名必须用反引号包裹
- **UNNEST 别名位置**：别名必须在括号后
- **HAVING 窗口函数**：用子查询 + WHERE 替代

### 分区错误
- **分区规范不匹配**：添加 `PARTITION BY` 到 `CREATE OR REPLACE TABLE`
- **分区列位置**：必须在 SELECT 列表末尾

### 表引用错误
- **表未限定**：检查是 CTE 还是真实表
- **虚拟表加前缀**：CTE 和子查询别名不需要前缀

---

## 输出要求

1. **只返回转换后的 BigQuery SQL**
2. **不要解释，不要 markdown 格式，不要代码块**
3. **保持查询结构和逻辑**
4. **确保所有表名正确映射**
5. **保持模板变量不变**（除非明确要求转换）
