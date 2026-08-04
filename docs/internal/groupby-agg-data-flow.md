# GroupBy Aggregation 数据流全解

本文用一个具体例子，逐步展示通用路径和 inline fast path 每一步的数据形态。

---

## 输入数据

```python
df.group_by("city", "year").agg(
    col("revenue").sum(),
    col("revenue").count(),
)
```

原始 RecordBatch（8 行）：

```
row | city       | year | revenue
----|------------|------|--------
 0  | "Beijing"  | 2023 | 100
 1  | "Shanghai" | 2024 | 200
 2  | "Beijing"  | 2023 | 150
 3  | "Shanghai" | 2024 | 300
 4  | "Beijing"  | 2024 | 50
 5  | "Shanghai" | 2023 | 400
 6  | "Beijing"  | 2023 | 75
 7  | "Shanghai" | 2024 | 125
```

期望输出（4 个 group）：

```
city       | year | sum(revenue) | count(revenue)
-----------|------|--------------|---------------
"Beijing"  | 2023 | 325          | 3
"Shanghai" | 2024 | 625          | 3
"Beijing"  | 2024 | 50           | 1
"Shanghai" | 2023 | 400          | 1
```

---

# 路径 A: 通用路径（非 fast path）

## A.1: eval_expression_list(group_by)

求值 group_by 表达式，得到只含 key 列的 RecordBatch：

```
groupby_table:
row | city       | year
----|------------|-----
 0  | "Beijing"  | 2023
 1  | "Shanghai" | 2024
 2  | "Beijing"  | 2023
 3  | "Shanghai" | 2024
 4  | "Beijing"  | 2024
 5  | "Shanghai" | 2023
 6  | "Beijing"  | 2023
 7  | "Shanghai" | 2024
```

## A.2: make_groups() → hash_rows()

对 groupby_table 的每行计算合并 hash（xxHash3，多列组合）：

```
row | city       | year | hash (xxHash3)
----|------------|------|---------------
 0  | "Beijing"  | 2023 | 0x7A3F
 1  | "Shanghai" | 2024 | 0xB1D2
 2  | "Beijing"  | 2023 | 0x7A3F
 3  | "Shanghai" | 2024 | 0xB1D2
 4  | "Beijing"  | 2024 | 0xC4E8
 5  | "Shanghai" | 2023 | 0xD5F1
 6  | "Beijing"  | 2023 | 0x7A3F
 7  | "Shanghai" | 2024 | 0xB1D2
```

## A.3: make_groups() → to_probe_hash_table()

用 `HashMap<IndexHash, SmallVec<[u64; 2]>>` 收集每个 group 的行号列表：

```rust
// HashMap 的状态（遍历结束后）:
{
  IndexHash{idx:0, hash:0x7A3F} → SmallVec[0, 2, 6],   // "Beijing",2023
  IndexHash{idx:1, hash:0xB1D2} → SmallVec[1, 3, 7],   // "Shanghai",2024
  IndexHash{idx:4, hash:0xC4E8} → SmallVec[4],          // "Beijing",2024
  IndexHash{idx:5, hash:0xD5F1} → SmallVec[5],          // "Shanghai",2023
}
```

输出拆分：

```
groupkey_indices = [0, 1, 4, 5]

groupvals_indices = [
  [0, 2, 6],   ← group 0: Beijing+2023 的所有行号
  [1, 3, 7],   ← group 1: Shanghai+2024 的所有行号
  [4],         ← group 2: Beijing+2024 的所有行号
  [5],         ← group 3: Shanghai+2023 的所有行号
]
```

**内存**: 8 个 u64 行号 = 64 bytes + 4 个 SmallVec 头 = ~128 bytes overhead

## A.4: take(groupkey_indices) → groupkeys_table

从 groupby_table 按 indices [0,1,4,5] 取出去重的 key 行：

```
groupkeys_table:
row | city       | year
----|------------|-----
 0  | "Beijing"  | 2023    (原 row 0)
 1  | "Shanghai" | 2024    (原 row 1)
 2  | "Beijing"  | 2024    (原 row 4)
 3  | "Shanghai" | 2023    (原 row 5)
```

## A.5: eval_agg_child("revenue") → 求值 value 列

```
evaluated_series (revenue): [100, 200, 150, 300, 50, 400, 75, 125]
```

（这里 revenue 是直接列引用，所以就是原始列本身。如果是表达式如 `col("price") * col("qty")`，这一步会计算出中间 Series。）

## A.6: series.sum(Some(&groupvals_indices)) → grouped_sum

逐 group 遍历行号列表，随机访问原始数组：

```
Group 0: indices=[0, 2, 6]
  get(0)=100, get(2)=150, get(6)=75 → sum = 325

Group 1: indices=[1, 3, 7]
  get(1)=200, get(3)=300, get(7)=125 → sum = 625

Group 2: indices=[4]
  get(4)=50 → sum = 50

Group 3: indices=[5]
  get(5)=400 → sum = 400

输出 Series: [325, 625, 50, 400]
```

## A.7: series.count(Some(&groupvals_indices)) → grouped_count

同样逐 group 遍历：

```
Group 0: indices=[0, 2, 6] → 3 个非 null → count = 3
Group 1: indices=[1, 3, 7] → 3 个非 null → count = 3
Group 2: indices=[4]       → 1 个非 null → count = 1
Group 3: indices=[5]       → 1 个非 null → count = 1

输出 Series: [3, 3, 1, 1]
```

## A.8: 拼接输出

```
最终 RecordBatch = concat(groupkeys_table, [sum_series, count_series]):

city       | year | revenue | revenue
-----------|------|---------|--------
"Beijing"  | 2023 | 325     | 3
"Shanghai" | 2024 | 625     | 3
"Beijing"  | 2024 | 50      | 1
"Shanghai" | 2023 | 400     | 1
```

---

# 路径 B: Inline Fast Path

## B.1: eval_expression_list(group_by) → groupby_table

和通用路径相同：

```
groupby_table:
row | city       | year
----|------------|-----
 0  | "Beijing"  | 2023
 ...（同上）
```

## B.2: 创建 Accumulator

对每个 agg 表达式创建 typed accumulator：

```rust
accumulators = [
  AggAccumulator::SumI64(SumAccumI64 { accumulators: [], source: revenue_i64_array }),
  AggAccumulator::Count(CountAccum { counts: [], mode: All, nulls: None }),
]
output_names = ["revenue", "revenue"]
```

此时 accumulator 内部数组为空，等分组完成后再 init。

## B.3: 选择分组策略

多列 key，包含 Utf8 → 尝试 `agg_symbolized_path`。

检查门槛：平均字符串 bytes/row：
```
city 列总 bytes = 7*4 + 8*4 = 60   (Beijing=7B × 4行, Shanghai=8B × 4行)
year 列是 Int64，不计入
avg = 60 / 8 = 7.5 bytes/row < 16 → 不满足门槛!
```

门槛不满足 → 返回 None → 落到 `agg_generic_hash_path`。

（如果 city 列是更长的字符串如 "San Francisco" 等，avg > 16 就会走 symbolized path。这里用 generic_hash_path 演示。）

## B.4: agg_generic_hash_path — hash_rows()

```
row | hash (xxHash3)
----|---------------
 0  | 0x7A3F
 1  | 0xB1D2
 2  | 0x7A3F
 3  | 0xB1D2
 4  | 0xC4E8
 5  | 0xD5F1
 6  | 0x7A3F
 7  | 0xB1D2
```

## B.5: agg_generic_hash_path — HashMap probe（单次遍历）

```
HashMap<IndexHash, u32> + comparator:

row 0: hash=0x7A3F → Vacant  → gid=0, groupkey_indices=[0], group_sizes=[1]
row 1: hash=0xB1D2 → Vacant  → gid=1, groupkey_indices=[0,1], group_sizes=[1,1]
row 2: hash=0x7A3F → compare(row2,row0)=true → Occupied → gid=0, group_sizes=[2,1]
row 3: hash=0xB1D2 → compare(row3,row1)=true → Occupied → gid=1, group_sizes=[2,2]
row 4: hash=0xC4E8 → Vacant  → gid=2, groupkey_indices=[0,1,4], group_sizes=[2,2,1]
row 5: hash=0xD5F1 → Vacant  → gid=3, groupkey_indices=[0,1,4,5], group_sizes=[2,2,1,1]
row 6: hash=0x7A3F → compare(row6,row0)=true → Occupied → gid=0, group_sizes=[3,2,1,1]
row 7: hash=0xB1D2 → compare(row7,row1)=true → Occupied → gid=1, group_sizes=[3,3,1,1]
```

输出 `GroupingResult`：

```
groupkey_indices = [0, 1, 4, 5]       // 每个 group 的代表行号
group_ids        = [0, 1, 0, 1, 2, 3, 0, 1]  // 每行属于哪个 group (Vec<u32>)
group_sizes      = [3, 3, 1, 1]       // 每个 group 多少行
```

**内存**: `group_ids` = 8×4 = 32 bytes，远小于通用路径的行号列表。

## B.6: accumulate — Count

CountAccum 尝试 `try_use_group_sizes`：
- mode = All，无 null → 直接用 group_sizes！

```
counts = group_sizes.clone() = [3, 3, 1, 1]   ← O(4)，不用遍历 8 行
```

## B.7: accumulate — SumI64

SumAccumI64 无法用 group_sizes 捷径，走 `update_batch`：

```
init: accumulators = [None, None, None, None]

source (revenue): [100, 200, 150, 300, 50, 400, 75, 125]
group_ids:        [  0,   1,   0,   1,  2,   3,  0,   1]

遍历 (顺序 zip):
  row 0: gid=0, val=100 → acc[0] = Some(100)
  row 1: gid=1, val=200 → acc[1] = Some(200)
  row 2: gid=0, val=150 → acc[0] = Some(100+150) = Some(250)
  row 3: gid=1, val=300 → acc[1] = Some(200+300) = Some(500)
  row 4: gid=2, val=50  → acc[2] = Some(50)
  row 5: gid=3, val=400 → acc[3] = Some(400)
  row 6: gid=0, val=75  → acc[0] = Some(250+75) = Some(325)
  row 7: gid=1, val=125 → acc[1] = Some(500+125) = Some(625)

最终: accumulators = [Some(325), Some(625), Some(50), Some(400)]
```

## B.8: finalize — 转换为 Series

```
SumAccumI64.finalize("revenue"):
  无 None → DataArray::from_field_and_values([325, 625, 50, 400]) → Series<Int64>

CountAccum.finalize("revenue"):
  DataArray::from_vec("revenue", [3, 3, 1, 1]) → Series<UInt64>
```

## B.9: take groupkeys + concat 输出

```
groupkeys_table = groupby_table.take([0, 1, 4, 5]):

row | city       | year
----|------------|-----
 0  | "Beijing"  | 2023
 1  | "Shanghai" | 2024
 2  | "Beijing"  | 2024
 3  | "Shanghai" | 2023

最终输出 = concat(groupkeys_table, [sum_series, count_series]):

city       | year | revenue(sum) | revenue(count)
-----------|------|--------------|---------------
"Beijing"  | 2023 | 325          | 3
"Shanghai" | 2024 | 625          | 3
"Beijing"  | 2024 | 50           | 1
"Shanghai" | 2023 | 400          | 1
```

---

# 两条路径的数据形态对比

## 分组阶段输出

| 维度 | 通用路径 | Inline Fast Path |
|------|---------|-----------------|
| 表示方式 | `Vec<SmallVec<[u64; 2]>>` — 每 group 一个行号列表 | `Vec<u32>` — 每行一个 group id |
| 数据形态 | `[[0,2,6], [1,3,7], [4], [5]]` | `[0, 1, 0, 1, 2, 3, 0, 1]` |
| 内存/行 | 8 bytes (u64 per row) + SmallVec header | 4 bytes (u32 per row) |
| 本例内存 | 8×8 + 4×24 = 160 bytes | 8×4 + 4×8 = 64 bytes |

## 聚合阶段

| 维度 | 通用路径 | Inline Fast Path |
|------|---------|-----------------|
| 算法 | 对每 group 遍历其行号, `get(idx)` 随机访问 | 顺序遍历所有行, `acc[gid] op= val` |
| 访问模式 | **随机读** value 列 (跳着读) | **顺序读** value 列 + **随机写** accumulator |
| cache 友好 | 差 (行号跳跃) | 好 (value 列顺序; accumulator 数组小, 在 L1) |
| 多 agg 时 | 每个 agg 独立遍历 groupvals_indices | 所有 accumulator 共享一次 group_ids 遍历 |

---

# 端到端对比图

```
═══════════════════ 通用路径 ═══════════════════

  原始数据 (8 rows)
       │
       ▼
  ① eval group_by → groupby_table (8 rows, 2 key cols)
       │
       ▼
  ② hash_rows() → hashes: [0x7A3F, 0xB1D2, ...]
       │
       ▼
  ③ probe HashMap → 收集行号列表
       │            groupvals_indices = [[0,2,6],[1,3,7],[4],[5]]
       │            groupkey_indices = [0,1,4,5]
       ▼
  ④ take(groupkey_indices) → groupkeys_table (4 rows)
       │
       ▼
  ⑤ eval_agg_child("revenue") → revenue_series (8 values)
       │
       ├──→ grouped_sum(indices) → 按行号随机访问 → [325, 625, 50, 400]
       │
       └──→ grouped_count(indices) → 按行号随机访问 → [3, 3, 1, 1]
               │
               ▼
  ⑥ concat → 最终 RecordBatch (4 rows, 4 cols)


═══════════════════ Inline Fast Path ═══════════════════

  原始数据 (8 rows)
       │
       ▼
  ① eval group_by → groupby_table (8 rows, 2 key cols)
       │
       ├──→ ② 创建 accumulators (SumI64, Count)
       │        source = revenue 列 (提前求值, 缓存引用)
       ▼
  ③ hash_rows() → hashes: [0x7A3F, 0xB1D2, ...]
       │
       ▼
  ④ 单次 HashMap probe → 不存行号列表, 只输出:
       │   group_ids   = [0,1,0,1,2,3,0,1]  (Vec<u32>, 32 bytes)
       │   group_sizes = [3,3,1,1]
       │   groupkey_indices = [0,1,4,5]
       ▼
  ⑤ accumulate:
       │   Count: counts = group_sizes = [3,3,1,1]  ← O(4)
       │   Sum:   顺序 zip(group_ids, values):
       │          acc[0]+=100, acc[1]+=200, acc[0]+=150, ...
       │          → [325, 625, 50, 400]
       ▼
  ⑥ finalize → Series
       │
       ▼
  ⑦ take(groupkey_indices) → groupkeys_table (4 rows)
       │
       ▼
  ⑧ concat → 最终 RecordBatch (4 rows, 4 cols)
```

---

# 关键差异总结

| 问题 | 通用路径的代价 | Inline 如何解决 |
|------|--------------|----------------|
| 行号列表内存大 | N×8 bytes (u64) | N×4 bytes (u32 group_id) |
| 随机访问 value 列 | 按行号 `get(idx)` 跳跃读取 | 顺序 `zip(group_ids, values)` |
| 多 agg 重复遍历 | 每个 agg 各遍历一遍 indices | 所有 acc 共享一次遍历 (Count 甚至 O(groups)) |
| 动态分发 | Series trait object dispatch | enum match, 编译器可内联 |
| 分组和聚合解耦 | 必须先完成分组, 再开始聚合 | 分组结果直接喂给 accumulate, 无中间态持久化 |

---

# 何时走哪条路

```rust
// src/daft-recordbatch/src/ops/agg.rs
pub fn agg_groupby(&self, to_agg, group_by) {
    // Python UDF → map_groups (完全不同的路径)
    // can_inline_agg? → agg_groupby_inline (本文路径 B)
    // 否则 → 通用路径 (本文路径 A)
}
```

`can_inline_agg` = true 当且仅当：
1. 所有 agg 都是 Count / Sum / Min / Max
2. Sum/Min/Max 的值列 dtype 是 Int8-64 / UInt8-64 / Float32 / Float64

任何不满足的情况（Mean、Percentile、List、Python UDF、Decimal 类型等）→ 走通用路径。
