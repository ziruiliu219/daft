# Benchmark 设计文档：Daft hashbrown vs TaperHashMap

## 1. 目标

比较两种 hash table 在 GroupBy Aggregation 场景下的性能：
- **Daft 侧**: hashbrown (Swiss Table) + IdentityHasher + IndexHash + 即时 comparator
- **Taper 侧**: TaperHashMap + SWAR tag + 延迟 batch compare

---

## 2. 模拟的数据

### 2.1 列数据 (`KeyModel`)

模拟 Daft 的 Arrow 列式数组，用 `Vec<i64>` / `Vec<String>` 代替。

```
KeyModel::generate(kind, num_rows, num_groups)

输入:
  kind = "2col_i64"
  num_rows = 1,000,000
  num_groups = 100
  seed = 42 (固定, 保证可重现)

输出:
  col_a: Vec<i64> = [97, 194, 97, 4850, 97, ...]   ← 100万个值, 只有 100 种不同
  col_b: Vec<i64> = [60, 113, 60, 346, 60, ...]    ← 同上
  hashes: Vec<u64> = [0xA1B2.., 0xC3D4.., ...]     ← 预计算的 xxHash3 值
```

四种 key 类型：

| key_kind | 列组成 | comparator 开销 |
|----------|--------|----------------|
| `1col_i64` | 1 列 i64 | 极低 (1次 i64 ==) |
| `2col_i64` | 2 列 i64 | 低 (2次 i64 ==) |
| `4col_i64` | 4 列 i64 | 中 (4次 i64 ==) |
| `2col_i64_string` | 2 列 i64 + 1 列 string(30字节) | 高 (2次 i64 == + string compare) |

### 2.2 Hash 计算

使用和 Daft 完全相同的 xxHash3 (链式 seed)：

```rust
use xxhash_rust::xxh3::xxh3_64_with_seed;

// 模拟 Daft 的 hash_rows():
fn mix_hash2(a: u64, b: u64) -> u64 {
    let h = xxh3_64_with_seed(&a.to_le_bytes(), 0);     // 第一列, seed=0
    xxh3_64_with_seed(&b.to_le_bytes(), h)              // 第二列, seed=第一列 hash
}
```

对应 Daft 源码 (`ops/hash.rs`):
```rust
let mut hash_so_far = cols[0].hash(None)?;
for c in cols.iter().skip(1) {
    hash_so_far = c.hash(Some(&hash_so_far))?;
}
```

### 2.3 Value 列 (聚合用)

```rust
let values: Vec<i64> = (0..num_rows).map(|i| (i % 1000) as i64).collect();
```

用于 `sums[gid] += values[i]`，模拟 SUM 聚合的状态更新。

---

## 3. Daft 侧调用的接口

### 3.1 使用的库

```rust
use hashbrown::{HashMap, hash_map::RawEntryMut};
```

### 3.2 数据结构

```rust
// 和 Daft 完全一致:
struct IndexHash { idx: u64, hash: u64 }

impl Hash for IndexHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);  // IdentityHasher: 不再 rehash
    }
}

let mut table = HashMap::<IndexHash, u32, IdentityBuildHasher>::with_capacity_and_hasher(
    init_cap, Default::default(),
);
```

### 3.3 核心 API 调用

```rust
// 对每行:
let entry = table.raw_entry_mut().from_hash(h, |other| {
    //                              ↑ hashbrown 内部:
    //                                1. h 低位定位 Group
    //                                2. NEON SIMD 比较 ctrl byte (tag)
    //                                3. tag 匹配后调这个闭包:
    (h == other.hash)                    // ← Stage 1b: hash ==
    && keys.compare(i, other.idx as usize)  // ← Stage 2: comparator (立即)
});

match entry {
    RawEntryMut::Occupied(e) => {
        let gid = *e.get();
        sums[gid as usize] += values[i];     // ← 聚合
    }
    RawEntryMut::Vacant(e) => {
        let gid = ngroups; ngroups += 1;
        e.insert_hashed_nocheck(h, IndexHash { idx: i as u64, hash: h }, gid);
        sums.push(0);
    }
}
```

### 3.4 Comparator

```rust
impl KeyModel {
    fn compare(&self, i: usize, j: usize) -> bool {
        match self {
            OneCol { col } => col[i] == col[j],
            TwoCols { a, b } => a[i] == a[j] && b[i] == b[j],
            FourCols { a, b, c, d } => a[i]==a[j] && b[i]==b[j] && c[i]==c[j] && d[i]==d[j],
            TwoColsAndString { a, b, s } => a[i]==a[j] && b[i]==b[j] && s[i]==s[j],
        }
    }
}
```

对应 Daft 的 `build_multi_array_is_equal` 返回的闭包。

---

## 4. Taper 侧调用的接口

### 4.1 使用的库

```rust
use taper_hashmap::taper_hashmap::TaperHashMap;
use taper_hashmap::chunk::SlotValue;
```

### 4.2 数据结构

```rust
let mut map = TaperHashMap::with_capacity(init_cap);
```

### 4.3 核心 API 调用 — Phase 1

```rust
let _update_list = map.emplace_batch(
    hashes,                          // ← 和 Daft 侧相同的 hash 数组
    &mut |row_idx, sv| {             // on_new: 新 group
        let g = ngroups; ngroups += 1;
        write_gid(sv, g);            // 写 group_id 到 6-byte SlotValue
        sums.push(0);
        group_rep_rows.push(row_idx);
        new_entries.push((row_idx, g));
    },
    &mut |row_idx, sv| {             // on_existing: tag+hash 匹配
        let g = read_gid(sv);        // 从 SlotValue 读 group_id
        existing_entries.push((row_idx, g));
    },
);
```

`emplace_batch` 内部做的：
```
for 每行:
  1. tag = (hash >> 16) & 0x7F
  2. chunk = chunks[hash & mask]
  3. SWAR BitMask::match_tag(chunk.tags, tag)  ← tag 过滤
  4. 对匹配的 slot: chunk.keys[slot] == hash?  ← hash ==
  5. 匹配 → on_existing (不做 key compare!)
     不匹配 → 找 empty slot → on_new
```

### 4.4 核心 API 调用 — Phase 2

```rust
// 处理 new group:
for &(idx, g) in &new_entries {
    sums[g as usize] += values[idx];
}

// Deferred full key compare:
for &(idx, tentative_gid) in &existing_entries {
    let rep = group_rep_rows[tentative_gid as usize];
    if keys.compare(idx, rep) {                // ← 和 Daft 相同的 comparator
        sums[tentative_gid as usize] += values[idx];
    } else {
        // hash collision (极少) — 简化处理
        sums[tentative_gid as usize] += values[idx];
    }
}
```

### 4.5 SlotValue 读写

```rust
// 写 group_id (u32) 到 6-byte SlotValue:
fn write_gid(sv: &mut SlotValue, gid: u32) {
    sv.bytes[0..4].copy_from_slice(&gid.to_ne_bytes());
}

// 从 SlotValue 读 group_id:
fn read_gid(sv: &SlotValue) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&sv.bytes[0..4]);
    u32::from_ne_bytes(b)
}
```

---

## 5. 两侧流程对比

```
Daft (每行):
  from_hash(h, closure)
    → [hashbrown 内部] NEON tag match
    → [closure] hash== → comparator(cols[i] vs cols[j]) → 确认
  → Occupied: sums[gid] += val
  → Vacant: insert + sums.push(0)

Taper (分两阶段):
  Phase 1 - emplace_batch (每行):
    → SWAR tag match
    → hash==
    → on_new: write gid, push to new_entries
    → on_existing: read gid, push to existing_entries

  Phase 2 - deferred (只对 existing_entries):
    → comparator(cols[i] vs cols[rep]) → 确认
    → sums[gid] += val
```

---

## 6. 测量范围

| 内容 | 计入时间？ |
|------|-----------|
| 数据生成 (KeyModel::generate) | ✗ 不计 |
| Hash 计算 (xxHash3) | ✗ 不计 (预算好的) |
| HashMap/TaperHashMap 初始化 | ✓ 计入 |
| Probe + Insert (全部行) | ✓ 计入 |
| Comparator (逐列比较) | ✓ 计入 |
| Aggregation (sums[gid] += val) | ✓ 计入 |
| Vec 分配 (new_entries 等) | ✓ 计入 (Taper 的 overhead) |

---

## 7. 三组 Benchmark 的参数

### bench_key_complexity

| 固定 | 变量 |
|------|------|
| rows = 1M | key_kind × num_groups |

```
key_kind ∈ {1col_i64, 2col_i64, 4col_i64, 2col_i64_string}
num_groups ∈ {10, 100, 1000, 10000}
```

### bench_row_scale

| 固定 | 变量 |
|------|------|
| key = 2col_i64, groups = 100 | rows |

```
rows ∈ {100_000, 1_000_000, 10_000_000}
```

### bench_load_factor

| 固定 | 变量 |
|------|------|
| key = 2col_i64, rows = 1M, groups = 1000 | init_cap (控制 load factor) |

```
target_lf ∈ {0.5, 0.7, 0.9}
init_cap = groups / target_lf
```

---

## 8. 公平性控制

| 维度 | 两侧是否相同 |
|------|------------|
| 输入 hash 值 | ✓ 同一个 `&[u64]` |
| 随机 seed | ✓ 固定 seed=42 |
| Comparator 逻辑 | ✓ 同一个 `keys.compare(i, j)` |
| Aggregation | ✓ 同样的 `sums[gid] += values[i]` |
| 初始容量 | ✓ 同一个 `init_cap` |
| Key 数据 | ✓ 同一个 `KeyModel` |

---

## 9. 对应 Daft 源码位置

| Benchmark 中的模拟 | Daft 真实代码 |
|-------------------|-------------|
| `HashMap::<IndexHash, u32, IdentityBuildHasher>` | `src/daft-recordbatch/src/ops/hash.rs` |
| `raw_entry_mut().from_hash(h, closure)` | `src/daft-recordbatch/src/ops/hash.rs:50` |
| `(h == other.hash) && keys.compare(i, j)` | `src/daft-recordbatch/src/ops/hash.rs:51-54` |
| `IdentityHasher / IndexHash` | `src/daft-core/src/utils/identity_hash_set.rs` |
| `keys.compare(i, j)` | `src/daft-core/src/array/ops/arrow/comparison.rs` |
| hash 计算 (xxHash3 链式) | `src/daft-core/src/kernels/hashing.rs` |
| `sums[gid] += values[i]` | `src/daft-recordbatch/src/ops/inline_agg.rs` accumulator |
