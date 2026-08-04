# agg_groupby_inline 五条分组路径详解

## 总览

`agg_groupby_inline` 的 Step 3 根据 key 列的类型和数量，选择最快的分组方式：

```mermaid
flowchart TD
    ENTRY["agg_groupby_inline Step 3:\n选择分组方式"]
    ENTRY --> Q1{"group_by 是单列?"}

    Q1 -->|是| Q2{"key 列类型?"}
    Q2 -->|Int8-64 / UInt8-64| PATH1["agg_single_col_int\n(FnvHashMap<原始值, gid>)"]
    Q2 -->|Utf8 / Binary| PATH2["agg_single_col_bytes\n(FnvHashMap<&[u8], gid>)"]
    Q2 -->|其他| PATH5["agg_generic_hash_path\n(hash_rows + comparator)"]

    Q1 -->|否 (多列)| Q3{"总宽度 ≤ 16 bytes\n且无 null\n且都是 fixed-width?"}
    Q3 -->|是| PATH3["agg_packed_key_path\n(多列打包成 u64/u128)"]
    Q3 -->|否| Q4{"有 Utf8/Binary 列?"}
    Q4 -->|是| PATH4["agg_symbolized_path\n(字符串符号化→整数拼接)"]
    Q4 -->|否| PATH5
```

---

## 路径 1: agg_single_col_int（最快）

**适用**: 单列 group key，类型是 Int8/16/32/64 或 UInt8/16/32/64

**算法**: `FnvHashMap<T::Native, u32>` — 直接用原始整数值做 HashMap key

```
输入: key 列 year: [2023, 2024, 2023, 2024, 2023]

HashMap<i64, u32>:
  row 0: entry(2023) → Vacant → gid=0
  row 1: entry(2024) → Vacant → gid=1
  row 2: entry(2023) → Occupied → gid=0
  row 3: entry(2024) → Occupied → gid=1
  row 4: entry(2023) → Occupied → gid=0

group_ids = [0, 0, 1, 1, 0]  (密集数组, 不存行号列表)
→ accumulate(accumulators, group_ids)
```

**为什么最快**: FNV hash 对整数非常快（一次乘法 + 异或）。Key 比较是 `==`，一条指令。零分配。

**代码位置**: `inline_agg.rs` 函数 `agg_single_col_int` (~line 570)

---

## 路径 2: agg_single_col_bytes（次快）

**适用**: 单列 group key，类型是 Utf8（字符串）或 Binary

**算法**: `FnvHashMap<&[u8], u32>` — 借用 Arrow buffer 里的字节切片做 key

```
输入: key 列 city: ["Beijing", "Shanghai", "Beijing"]

HashMap<&[u8], u32>:  (key 是引用，零拷贝)
  row 0: entry(b"Beijing")  → Vacant → gid=0
  row 1: entry(b"Shanghai") → Vacant → gid=1
  row 2: entry(b"Beijing")  → Occupied → gid=0

group_ids = [0, 1, 0]
→ accumulate(accumulators, group_ids)
```

**为什么快**: 借用 Arrow buffer 的切片，不拷贝字符串。FNV 对短字符串高效。

**代码位置**: `inline_agg.rs` 函数 `agg_single_col_bytes` (~line 658)

---

## 路径 3: agg_packed_key_path（多列打包）

**适用**: 多列 group key，满足以下条件：
- 所有列无 null
- 所有列是 fixed-width 类型（int/uint/float/bool）或短字符串（≤15 bytes）
- 所有列拼接总宽度 ≤ 16 bytes

**算法**: 把多列值按固定偏移位拼接成一个 u64 或 u128，然后用 `FnvHashMap<u128, u32>`

```
输入: city: ["B", "S", "B"], year: [2023, 2024, 2023]
      假设 city 打包 2 bytes (len_prefix + 1 char), year 打包 8 bytes → 总 10 bytes → 用 u128

row 0: packed = (0x01_42_00...00) | (2023_as_le_bytes << 16) → 一个 u128
row 1: packed = (0x01_53_00...00) | (2024_as_le_bytes << 16) → 另一个 u128
row 2: packed = 同 row 0

HashMap<u128, u32>:
  entry(row0_packed) → gid=0
  entry(row1_packed) → gid=1
  entry(row2_packed) → 已有 → gid=0

group_ids = [0, 1, 0]
```

**为什么快**: 多列比较变成**单个 u128 的整数 ==**，一条指令完成。

**代码位置**: `inline_agg.rs` 函数 `agg_packed_key_path` (~line 773)

---

## 路径 4: agg_symbolized_path（字符串符号化）

**适用**: 多列 group key，包含 Utf8/Binary 列，但不满足 packed_key 条件（如字符串太长）

**算法**: 
1. 对每列字符串，用 HashMap 把每个唯一值映射为一个 u32 symbol_id
2. 把多列 symbol_id 拼接成一个 u128（每列占 32 bit）
3. 用 `FnvHashMap<u128, u32>` 做分组

```
输入: city: ["Beijing", "Shanghai", "Beijing"], dept: ["eng", "sales", "eng"]

Step 1: 符号化
  city:  "Beijing"→0, "Shanghai"→1   → symbol_ids = [0, 1, 0]
  dept:  "eng"→0, "sales"→1         → symbol_ids = [0, 1, 0]

Step 2: 拼接 symbol_ids 成 composite key
  row 0: (city_sym=0, dept_sym=0) → packed u64 = 0x0000_0000_0000_0000
  row 1: (city_sym=1, dept_sym=1) → packed u64 = 0x0000_0001_0000_0001
  row 2: (city_sym=0, dept_sym=0) → 同 row 0

Step 3: HashMap<u64, u32> 分组
  gid=0, gid=1, gid=0
```

**为什么快**: 长字符串比较变成 u32 整数比较。代价是要先做一遍符号化（额外一个 HashMap），但对后续重复查找有摊销收益。

**代码位置**: `inline_agg.rs` 函数 `agg_symbolized_path` (~line 1212)

---

## 路径 5: agg_generic_hash_path（兜底）

**适用**: 所有其他情况（key 列类型不属于前四种，或多列 key 不满足打包/符号化条件）

**算法**: 和 `to_probe_hash_table`（通用路径的 `make_groups`）类似，但**仍然是单 pass + accumulate**
1. `hash_rows()` → 对多列计算合并 hash（xxHash3）
2. `HashMap<IndexHash, u32>` + comparator probe
3. 得到 `group_ids` → `accumulate(accumulators, group_ids)`

```
输入: 任意多列 key

Step 1: hash_rows() → hashes = [0xA1B2, 0xC3D4, 0xA1B2, ...]

Step 2: probe HashMap
  for row_idx, h in hashes:
    entry = group_table.raw_entry_mut().from_hash(h, |other| {
        (h == other.hash) && comparator(row_idx, other.idx)
    })
    match entry:
      Vacant → 新 group, gid = num_groups++
      Occupied → 已有 group, gid = entry.get()
    group_ids.push(gid)

Step 3: accumulate(accumulators, &group_ids)
```

**为什么是兜底**: 需要 `hash_rows()`（比直接用原始值做 key 多一次 hash 计算），且 comparator 需要逐列比较。但比通用路径（`make_groups` + `grouped_sum`）快，因为仍然是单 pass + 不存行号列表。

**代码位置**: `inline_agg.rs` 函数 `agg_generic_hash_path` (~line 1047)

---

## 五条路径对比表

| 路径 | HashMap key | Hash 算法 | 相等比较 | 适用条件 | 相对速度 |
|------|------------|-----------|---------|---------|---------|
| single_col_int | `i64/u64` 原始值 | FNV (内置) | `==` (1 条指令) | 单列整数 | ★★★★★ |
| single_col_bytes | `&[u8]` 切片引用 | FNV | memcmp | 单列 Utf8/Binary | ★★★★ |
| packed_key | `u64` 或 `u128` | FNV | `==` (1-2 条指令) | 多列 fixed-width, 总 ≤16B, 无 null | ★★★★ |
| symbolized | `u64` 或 `u128` (symbol ids) | FNV | `==` | 多列, 有字符串 | ★★★ |
| generic_hash | `IndexHash {idx, hash}` | xxHash3 + comparator | 逐列比原始值 | 任意 | ★★ |

所有五条都共享同一个输出接口：`group_ids: Vec<u32>` + `accumulate(accumulators, &group_ids)`。

---

## 共同的最后一步: accumulate

不管走哪条路径，最终都调：

```rust
fn accumulate(accumulators: &mut [AggAccumulator], result: &GroupingResult) {
    for acc in accumulators {
        acc.init_groups(num_groups);
        if !acc.try_use_group_sizes(&result.group_sizes) {
            acc.update_batch(&result.group_ids);
        }
    }
}
```

遍历 `group_ids`，对每个 accumulator 用 `group_ids[row]` 做索引更新：

```
Count:  counts[gid] += 1
Sum:    accum[gid] += value[row]
Min:    accum[gid] = min(accum[gid], value[row])
Max:    accum[gid] = max(accum[gid], value[row])
```
