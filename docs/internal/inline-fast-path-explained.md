# Inline Fast Path 详解

## 一句话概括

Inline fast path 是 `RecordBatch::agg_groupby` 中的一条优化路径：当所有聚合表达式都是 Count/Sum/Min/Max 且值列是数值类型时，跳过通用路径（先 `make_groups` 收集行号列表，再对每个组分别调用聚合函数），改为 **单次遍历同时完成分组和聚合**，避免中间的 `Vec<Vec<u64>>` 内存分配。

---

## 为什么需要它

### 通用路径的问题

通用路径 (`agg_groupby` 的原始逻辑) 分两步：

```
Step 1: make_groups()
  输入: key 列 [A, B, A, B, A]
  输出:
    groupkey_indices = [0, 1]          // 每个 unique key 的第一行
    groupvals_indices = [[0,2,4], [1,3]]  // 每个 group 包含哪些行号 ← 大量分配

Step 2: eval_agg_expression(agg, groupvals_indices)
  对每个 group 的行号列表，take 出子集，再做 sum/count/min/max
```

问题出在 `groupvals_indices: Vec<Vec<u64>>`：
- **内存**: 每一行至少 8 bytes（u64）存在某个 group 的 Vec 里。100M 行 = 800MB 只是为了存行号。
- **Cache 不友好**: 先做完所有分组，再回头按行号 take 数据。两次遍历，数据访问模式碎片化。
- **额外 take 开销**: 每个 group 需要 `series.take(indices)` 构造子 Series，再做聚合。

### Inline fast path 的做法

```
单次遍历:
  for row in 0..N:
    gid = hash_lookup(key[row])        // 顺序遍历 key，确定 group id
    accumulator[gid] += value[row]     // 顺序遍历 value，直接 scatter 到 accumulator
```

- **零中间分配**: 不存 `Vec<Vec<u64>>`，只存 `group_ids: Vec<u32>`（4 bytes/row）和少量 accumulator 状态。
- **单次遍历**: key 列和 value 列都只读一遍，顺序访问，cache line 利用率高。
- **直接更新**: accumulator 是 `Vec<i64>` / `Vec<f64>`，按 `gid` 索引直接更新，无需 take。

---

## 何时触发

入口在 `RecordBatch::agg_groupby` (`src/daft-recordbatch/src/ops/agg.rs`)：

```rust
pub fn agg_groupby(&self, to_agg: &[BoundAggExpr], group_by: &[BoundExpr]) -> DaftResult<Self> {
    // ... MapGroups 特殊处理 ...

    // Fast path: inline aggregation for supported agg types
    if can_inline_agg(to_agg, self) {
        return self.agg_groupby_inline(to_agg, group_by);
    }

    // 通用路径: make_groups + eval_agg_expression ...
}
```

`can_inline_agg` 的判断条件：
1. **所有** agg 表达式都是 `Count` / `Sum` / `Min` / `Max`
2. 对于 Sum/Min/Max，值列 dtype 必须是数值类型 (Int8-64, UInt8-64, Float32, Float64)

只要有一个不满足（如 Mean、List、任何 Python UDF），就走通用路径。

---

## 整体架构（四个阶段）

```
┌────────────────────────────────────────────────────────────────┐
│ agg_groupby_inline(to_agg, group_by)                           │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Phase 1: 创建 Accumulator                                     │
│    对每个 AggExpr 创建一个 typed accumulator (enum, 无 vtable)   │
│                                                                │
│  Phase 2: 分组 (Grouping)                                      │
│    根据 key 列数量和类型选择最快的分组策略                         │
│    → 输出: group_ids: Vec<u32>, group_sizes: Vec<u64>           │
│                                                                │
│  Phase 3: 累加 (Accumulation)                                  │
│    对每个 accumulator:                                          │
│      先尝试 O(groups) 捷径 (Count 可用 group_sizes)             │
│      否则 O(rows) scatter loop: acc[gid] op= value[row]        │
│                                                                │
│  Phase 4: 输出构造                                             │
│    take group keys by groupkey_indices                          │
│    finalize 每个 accumulator → Series                           │
│    concat → RecordBatch                                        │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Accumulator 设计

### 为什么用 enum 而不是 trait object

```rust
enum AggAccumulator {
    Count(CountAccum),
    SumI64(SumAccumI64),
    SumF64(SumAccumF64),
    MinI64(MinAccumI64),
    MaxF64(MaxAccumF64),
    // ... 共 25 个 variant，覆盖所有 dtype 组合
}
```

**理由**: hot loop 里每行都要调用 `acc.update_batch()`。如果用 `Box<dyn Accumulator>`，每次调用都经过 vtable 间接跳转，对分支预测不友好。Enum dispatch 编译为 match + 直接函数调用，编译器可以内联整个更新逻辑。

### Accumulator 的三个方法

```rust
impl AggAccumulator {
    fn init_groups(&mut self, n: u32);        // 预分配 n 个 group 的存储
    fn update_batch(&mut self, group_ids: &[u32]); // 单次遍历所有行
    fn finalize(self, name: &str) -> Series;  // 输出结果 Series
}
```

### CountAccum 的特殊优化

Count 在很多情况下不需要逐行遍历：

```rust
fn try_use_group_sizes(&mut self, group_sizes: &[u64]) -> bool {
    match self.mode {
        CountMode::All => { self.counts = group_sizes.to_vec(); true }  // O(groups)
        CountMode::Valid if no_nulls => { self.counts = group_sizes.to_vec(); true }
        CountMode::Null if no_nulls => { /* counts already 0 */ true }
        _ => false  // 有 null 时需要逐行检查
    }
}
```

---

## Phase 2: 分组策略

这是 inline fast path 最关键的优化点。根据 key 列的数量和类型，选择不同的 HashMap 策略：

### 策略 1: `agg_single_col_int` — 单列整数

```rust
FnvHashMap<T::Native, u32>  // T = i64, u32, etc.
```

- Hash: FNV-1a (一次乘法 + 异或)
- 比较: `==` (一条 CPU 指令)
- 无 allocation: key 就是原始值

### 策略 2: `agg_single_col_bytes` — 单列字符串/二进制

```rust
FnvHashMap<&'a str, u32>  // 借用 Arrow buffer，零拷贝
```

- Hash: FNV-1a (逐字节)
- 比较: memcmp
- 零拷贝: key 是 `&str` 引用，指向 Arrow 的 buffer

### 策略 3: `agg_symbolized_path` — 多列且含字符串

```
Step 1: symbolize — 每个字符串列用 HashMap 分配 u32 id
Step 2: 用 symbol id 列替换原始字符串列
Step 3: 在替换后的 RecordBatch 上跑 generic_hash_path
```

好处：后续的 hash 和比较都是固定宽度整数，不需要反复 hash 和 memcmp 长字符串。

门槛：平均字符串长度 ≥ 16 bytes 时才划算（短字符串如 "M"/"F" 直接 hash 就很快，额外做一遍符号化反而慢）。

### 策略 4: `agg_generic_hash_path` — 兜底

```rust
HashMap<IndexHash, u32>  // IndexHash = { idx: u64, hash: u64 }
// 用 xxHash3 计算 hash_rows()，再用 comparator 逐列比较
```

仍然比通用路径快，因为：
- 仍然是单次遍历 + scatter 累加
- 不存 `Vec<Vec<u64>>` 行号列表

---

## Phase 3: Accumulation 的 scatter 模式

所有分组策略输出统一格式：

```rust
struct GroupingResult {
    groupkey_indices: Vec<u64>,  // 每个 group 的代表行号
    group_ids: Vec<u32>,         // 每行属于哪个 group
    group_sizes: Vec<u64>,       // 每个 group 有多少行
}
```

然后 `accumulate()` 函数遍历每个 accumulator：

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

`update_batch` 的内部循环（以 SumI64 为例）：

```rust
// 无 null 的紧凑循环
for (&gid, &val) in group_ids.iter().zip(values.iter()) {
    accs[gid as usize] = Some(match accs[gid as usize] {
        Some(a) => a + val,
        None => val,
    });
}
```

这个循环的内存访问模式：
- `group_ids`: 顺序读
- `values`: 顺序读
- `accs[gid]`: 随机写，但如果 group 数量少（典型场景），accumulator 数组整体在 L1/L2 cache 里

---

## 与通用路径的对比

| 维度 | 通用路径 | Inline Fast Path |
|------|---------|-----------------|
| 遍历次数 | 2 次 (make_groups + take + agg) | 1 次 |
| 中间内存 | `Vec<Vec<u64>>` = 8 bytes/row | `Vec<u32>` = 4 bytes/row |
| accumulator | 通用 Series 操作 (take → sum) | 原始类型数组直接索引 |
| dispatch | trait object / Series 动态类型 | enum match, 可内联 |
| 适用范围 | 任意 AggExpr | 仅 Count/Sum/Min/Max + 数值列 |

---

## 在执行引擎中的位置

```
GroupedAggregateSink (daft-local-execution)
  │
  ├── sink(): 接收 MicroPartition
  │     └── strategy.execute_strategy()
  │           └── input.agg(partial_agg_exprs, group_by)
  │                 └── RecordBatch::agg_groupby()
  │                       └── if can_inline_agg → agg_groupby_inline()  ← HERE
  │
  └── finalize(): 合并分区
        └── concated.agg(final_agg_exprs, final_group_by)
              └── RecordBatch::agg_groupby()
                    └── if can_inline_agg → agg_groupby_inline()  ← HERE
```

Inline fast path 在 partial agg 和 final agg 阶段都会触发（只要满足条件）。

---

## 关键源文件

| 文件 | 职责 |
|------|------|
| `src/daft-recordbatch/src/ops/inline_agg.rs` | 全部 inline fast path 实现 |
| `src/daft-recordbatch/src/ops/agg.rs` | 入口 `agg_groupby()`，决定走 inline 还是通用 |
| `src/daft-recordbatch/src/ops/groups.rs` | 通用路径的 `make_groups()` 实现 |
| `src/daft-local-execution/src/sinks/grouped_aggregate.rs` | 执行引擎层面的 GroupedAggregate sink |

---

## 未来可能的扩展

1. **packed_key_path**: 多列 fixed-width key 打包成 u64/u128，用 FNV 直接做 HashMap key（目前 doc 里有描述，代码尚未实现）
2. **Mean accumulator**: Sum + Count 组合，inline 内部同时维护两个 accumulator
3. **更多类型支持**: Date/Time 等 fixed-width 类型本质上是整数，可以复用 int path
4. **SIMD 向量化**: scatter 循环的 gather/scatter 可以用 AVX-512 加速
