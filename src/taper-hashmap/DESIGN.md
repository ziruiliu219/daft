# TaperHashMap 设计文档

## 1. 概述

TaperHashMap 是一个面向 **hash aggregation** 场景的高性能哈希表实现，设计灵感来源于 Taper（一种面向列式数据库的聚合算法）。它针对批量行处理（batch emplace）场景做了深度优化，通过三层渐进过滤和 cache-friendly 的内存布局实现高吞吐。

**核心设计目标：**
- 批量聚合场景下的高吞吐（vectorized aggregation）
- 最小化 cache miss（128B 对齐 chunk）
- 三层渐进过滤减少不必要的 full-key 比较
- 无 SIMD 依赖的跨平台 SWAR 加速

---

## 2. 整体架构

```
┌──────────────────────────────────────────────────────────────────┐
│                         TaperHashMap                              │
│                                                                  │
│  fields:                                                         │
│    chunks: Vec<Chunk>    // power-of-2 数量的 chunk 数组          │
│    mask: usize           // chunks.len() - 1, 用于快速取模        │
│    size: usize           // 已占用 slot 总数                      │
├──────────────────────────────────────────────────────────────────┤
│  对外接口:                                                        │
│    emplace_batch()       // 批量聚合主路径 (Stage 1 only)         │
│    emplace()             // 单行精确插入 (Full 3-stage)           │
│    put() / get()         // 简单 KV API (benchmark 用)           │
│    iter()                // 遍历所有已占用 slot                    │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                      Chunk (128 bytes, aligned)                   │
│                                                                  │
│  ┌─────────┬─────────────────┬────────────────────┬───────────┐ │
│  │ tags    │ keys            │ values             │ _padding  │ │
│  │ [u8;8]  │ [u64;8]         │ [SlotValue;8]      │ [u8;8]    │ │
│  │ 8 bytes │ 64 bytes        │ 48 bytes           │ 8 bytes   │ │
│  └─────────┴─────────────────┴────────────────────┴───────────┘ │
│  Total: 8 + 64 + 48 + 8 = 128 bytes                             │
├──────────────────────────────────────────────────────────────────┤
│  try_emplace(hash, tag) → EmplaceResult                          │
│  try_emplace_with_cmp(hash, tag, key_cmp) → EmplaceResult        │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                 ┌──────────────┴──────────────┐
                 ▼                              ▼
┌────────────────────────────┐  ┌────────────────────────────────┐
│      BitMask (SWAR)        │  │     SlotValue (6 bytes)        │
│                            │  │                                │
│  match_tag(tags, target)   │  │  48-bit 压缩指针               │
│  match_empty(tags)         │  │  → RowContainer 中的某一行     │
│  Iterator<Item=u8>         │  │  set_ptr() / get_ptr()         │
└────────────────────────────┘  └────────────────────────────────┘
```

---

## 3. 模块详解

### 3.1 `SlotValue` — 6 字节压缩指针

**文件:** `src/slot_value.rs`

#### 设计动机

在 x86_64 和 aarch64 架构上，用户态虚拟地址只使用低 48 位。利用这一事实，将指针压缩为 6 字节存储：

- 每个 slot 节省 2 字节
- 8 个 slot 共节省 16 字节
- 使 Chunk 总大小恰好为 128 字节（2 cache line）

#### 数据结构

```rust
#[repr(C)]
pub struct SlotValue {
    pub bytes: [u8; 6],  // 低 48 位，little-endian 存储
}
```

#### 核心操作

| 方法 | 功能 |
|------|------|
| `set_ptr(ptr)` | 取指针低 48 位，little-endian 写入 6 字节 |
| `get_ptr()` | 从 6 字节还原指针，如 bit47=1 则符号扩展高 16 位 |
| `is_empty()` | 判断是否全零（空 slot） |

#### 符号扩展

```rust
// 如果 bit 47 = 1 (内核地址空间)，高 16 位补 1
if addr & (1 << 47) != 0 {
    addr |= 0xFFFF_0000_0000_0000;
}
```

用户态指针 bit47 = 0，所以正常使用时不会触发扩展。保留此逻辑是为了安全性。

---

### 3.2 `BitMask` — SWAR 位并行比较

**文件:** `src/bitmask.rs`

#### 设计动机

在不依赖 SIMD intrinsics 的前提下，利用 SWAR (SIMD Within A Register) 技巧，用一个 u64 寄存器同时比较 8 个 tag 字节。所有平台通用，编译器可进一步向量化。

#### SWAR 原理

将 8 个 1-byte tag 拼成一个 u64，利用经典的 "零字节检测" 算法：

```
match_tag(tags_u64, target):
  step 1: x = tags_u64 XOR broadcast(target)
           → 匹配的字节变为 0x00，不匹配的非零
  
  step 2: result = (x - 0x0101_0101_0101_0101)  // 零字节会下溢
                 & (!x)                          // 确认原始字节为 0
                 & 0x8080_8080_8080_8080          // 提取每字节 MSB
           → 匹配字节的 bit7 = 1
```

**性能：** 3-4 条 ALU 指令完成 8 路并行比较，无分支。

#### 核心操作

| 方法 | 功能 |
|------|------|
| `match_tag(tags_u64, target)` | 返回所有 tag == target 的 slot 位掩码 |
| `match_empty(tags_u64)` | 返回所有空 slot (tag == 0x80) 的位掩码 |
| `any()` | 是否有任何匹配 |
| `lowest()` | 最低匹配的 slot 索引 (0-7) |
| `advance()` | 移除最低匹配，返回新 BitMask |
| `Iterator` | 依次产出所有匹配的 slot 索引 |

#### Tag 编码

- **有效 tag:** 0x00 - 0x7F (7-bit fingerprint, MSB=0)
- **空 slot:** 0x80 (MSB=1, 标记为 empty)

这样 `match_empty` 直接复用 `match_tag(tags, 0x80)` 即可。

---

### 3.3 `Chunk` — 128 字节对齐存储单元

**文件:** `src/chunk.rs`

#### 内存布局

```
Offset  Field       Size    Description
───────────────────────────────────────────────────
0-7     tags[8]     8B      7-bit fingerprint per slot, 0x80=empty
8-71    keys[8]     64B     完整 hash 值 (u64)
72-119  values[8]   48B     压缩指针 (6B × 8)
120-127 _padding    8B      对齐填充
───────────────────────────────────────────────────
Total: 128 bytes = 2 cache lines (64B) 或 1 cache line (128B systems)
```

```rust
#[repr(C, align(128))]
pub struct Chunk {
    pub tags: [u8; 8],
    pub keys: [u64; 8],
    pub values: [SlotValue; 8],
    _padding: [u8; 8],
}
```

**为什么 128 字节对齐？**
- 避免 chunk 跨 cache line 边界，减少 cache miss
- 在 128B cache line 的系统上（如某些 ARM 服务器），一个 chunk 正好一条 cache line
- 在 64B cache line 的 x86 系统上，一个 chunk = 2 条相邻 cache line，prefetcher 效果好

#### 核心方法: `try_emplace`

```rust
pub fn try_emplace(&mut self, hash_val: u64, tag: u8) -> EmplaceResult {
    // 1. SWAR tag 比较 → 找 tag 匹配的 slot
    let tag_matches = BitMask::match_tag(self.tags_u64(), tag);
    
    // 2. 对 tag 匹配的 slot 做 hash == hash 比较
    for slot_idx in tag_matches {
        if self.keys[slot] == hash_val {
            return EmplaceResult::Existing(slot_idx);  // 找到已有 group
        }
    }
    
    // 3. 找 empty slot 插入新 entry
    let empty_mask = BitMask::match_empty(self.tags_u64());
    if empty_mask.any() {
        let slot_idx = empty_mask.lowest();
        self.tags[slot] = tag;
        self.keys[slot] = hash_val;
        return EmplaceResult::New(slot_idx);
    }
    
    // 4. chunk 满
    EmplaceResult::Full
}
```

#### `EmplaceResult` 枚举

```rust
pub enum EmplaceResult {
    New(u8),       // 新 group, 已写入 tag+key, 调用者需写 value
    Existing(u8),  // tag+hash 匹配已有 slot, 可能需要 full-key 验证
    Full,          // chunk 满, 需线性步进到下一个 chunk
}
```

---

### 3.4 `TaperHashMap` — 顶层哈希表

**文件:** `src/taper_hashmap.rs`

#### 参数设计

| 参数 | 值 | 说明 |
|------|------|------|
| Key 类型 | `u64` | 直接使用预计算的 hash 值 |
| KeyScattered | `true` | 输入已是 hash，不再二次 hash |
| Chunk 大小 | 8 slots | 128B 对齐 |
| 最大负载因子 | 0.9 | 超过即触发扩容 |
| Chunk 数量 | power of 2 | 允许位与代替取模 |

#### Hash → 位置映射

```rust
// 从 hash 提取 7-bit tag (fingerprint)
fn extract_tag(hash: u64) -> u8 {
    ((hash >> 16) & 0x7F) as u8
}

// 从 hash 计算 chunk 位置
fn chunk_pos(&self, hash: u64) -> usize {
    (hash as usize) & self.mask  // 等价于 hash % chunks.len()
}
```

**为什么 tag 取 bit[22:16]？**
- 不能和 chunk_pos 使用同一段 bit，否则同一 chunk 内所有 entry 的 tag 都一样，失去过滤能力
- chunk_pos 使用低位 bit，tag 使用中间位，避免重叠

---

## 4. 三层渐进过滤

这是 TaperHashMap 的核心设计思想，通过从廉价到昂贵的三层检查逐步缩小候选集：

```
┌─────────────────────────────────────────────────────────────┐
│  所有输入行                                                   │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Stage 1a: SWAR Tag Match (7-bit fingerprint)               │
│  成本: ~3 ALU 指令 / 8 slots                                 │
│  过滤率: 理论 127/128 ≈ 99.2%                                │
│  → 大多数不匹配行在此被淘汰                                   │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Stage 1b: Hash Value Compare (u64 ==)                      │
│  成本: 1 次 64-bit 整数比较                                   │
│  过滤率: 极高 (u64 碰撞概率 ≈ 1/2^64)                        │
│  → 几乎消除所有 false positive                               │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Stage 2: Full Key Compare (外部回调)                        │
│  成本: 取决于 key 类型 (字符串比较、多列比较等)               │
│  → 最终确认 key 完全相同                                      │
└─────────────────────────────────────────────────────────────┘
```

**在 `emplace_batch` 中：** 只执行 Stage 1a + 1b，返回需要 Stage 2 验证的行号列表 (`update_list`)。调用者批量做 full-key 比较后决定是真匹配还是 hash 碰撞。

**在 `emplace` 中：** 内部完成全部三层，通过 `key_cmp` 闭包执行 Stage 2。

---

## 5. 冲突解决策略

采用 **chunk 级线性探测 (linear probing)**：

```
初始 chunk_idx = hash & mask

loop {
    match chunk.try_emplace(hash, tag) {
        New / Existing → 完成
        Full → chunk_idx = (chunk_idx + 1) & mask  // 步进到下一个 chunk
    }
}
```

**为什么是 chunk 级步进而非 slot 级？**
- 一个 chunk 内的 8 个 slot 共享同一次 cache line 加载
- 步进到下一个 chunk = 下一次 cache line 加载
- slot 级步进会在 chunk 边界产生非对齐访问

**探测终止条件：**
- `get()` 中遇到空 slot 即可终止（说明 key 不存在）
- `put()`/`emplace()` 中遇到空 slot 即插入

---

## 6. 扩容机制

#### 触发条件

```rust
const MAX_LOAD_FACTOR: f64 = 0.9;

// 插入前检查
if (self.size + count) as f64 > self.capacity() as f64 * MAX_LOAD_FACTOR {
    self.expand_to_fit(needed);
}
```

#### Rehash 流程

```rust
fn rehash(&mut self, new_capacity: usize) {
    1. 计算 new_num_chunks = capacity_to_num_chunks(new_capacity)  // 向上取 2 的幂
    2. 分配 new_chunks: Vec<Chunk>
    3. 遍历旧 table 所有已占用 slot:
       - 提取 hash, tag, value
       - 在 new_chunks 中做 try_emplace + 线性探测
       - 写入 value
    4. 替换 self.chunks, 更新 self.mask
}
```

**扩容策略：**
- `expand()`: 2× 容量
- `expand_to_fit(needed)`: 确保 `needed / 0.9` 的容量，至少 2×

---

## 7. 公开 API

### 7.1 `emplace_batch` — 批量聚合主路径

```rust
pub fn emplace_batch(
    &mut self,
    hashes: &[u64],                           // 每行预计算的 hash
    on_new: &mut dyn FnMut(usize, &mut SlotValue),  // 新 group 回调
    on_existing: &mut dyn FnMut(usize, &SlotValue), // 已有 group 回调
) -> Vec<usize>                               // 需要 Stage 2 验证的行号
```

**使用场景：** 列式数据库的 hash aggregation。对一个 batch 的行做分组，快速决定每行是新 group 还是已有 group。返回的 `update_list` 中的行需要调用者做 full-key 比较验证。

### 7.2 `emplace` — 单行精确插入

```rust
pub fn emplace(
    &mut self,
    hash: u64,
    key_cmp: &dyn Fn(&SlotValue) -> bool,      // Stage 2: full key compare
    on_new: &mut dyn FnMut(&mut SlotValue),    // 新 group
    on_match: &mut dyn FnMut(&SlotValue),      // 已有 group (确认匹配)
)
```

**使用场景：** Stage 2 碰撞修复时的 re-emplace，或需要精确 key 比较的单行操作。

### 7.3 `put` / `get` — 简单 KV 接口

```rust
pub fn put(&mut self, key: u64, value: u64)   // 插入/覆盖
pub fn get(&self, key: u64) -> Option<u64>    // 查找
```

**限制：** value 只使用 SlotValue 的 6 字节（48-bit），高 16 位丢失。主要用于 benchmark 和单元测试。

### 7.4 `iter` — 遍历

```rust
pub fn iter(&self) -> impl Iterator<Item = (u64, &SlotValue)>
```

遍历所有已占用 slot，跳过 tag == 0x80 的空 slot。

---

## 8. 典型调用流程

### 场景: Hash Aggregation (列式数据库)

```
Caller (聚合算子)
    │
    ├── 1. 对 batch 中每行计算 hash
    │      hashes = rows.map(|row| hash(row.group_keys))
    │
    ├── 2. 批量 emplace (Stage 1)
    │      update_list = map.emplace_batch(&hashes, on_new, on_existing)
    │      │
    │      │  on_new: 在 RowContainer 分配新行, 把指针写入 SlotValue
    │      │  on_existing: 暂定为已有 group, 记录行号
    │      │
    │      └── 内部: tag(SWAR) → hash(u64==) → New/Existing/Full
    │
    ├── 3. Stage 2 验证 (Full Key Compare)
    │      for row_idx in update_list:
    │          slot_value → get_ptr → RowContainer → 比较完整 key
    │          if 真匹配 → 做聚合更新 (sum/count/etc)
    │          if hash碰撞 → map.emplace(hash, key_cmp, on_new, on_match)
    │
    └── 4. 输出
           map.iter() → 收集所有 group 结果
```

---

## 9. 性能特征

| 特征 | 说明 |
|------|------|
| Cache 友好 | 128B 对齐 chunk, 一次 cache line load 涵盖 tags + keys |
| 分支可预测 | SWAR 无分支比较, 大多数 emplace 命中第一个 chunk |
| 批量友好 | `emplace_batch` 延迟 full-key compare, 减少 pipeline stall |
| 无 SIMD 依赖 | 纯位运算, 所有平台通用 |
| 扩容代价 | Rehash 需复制所有 entry, 但 0.9 负载因子减少扩容频率 |
| 适用场景 | 高基数聚合 (大量不同 group), batch 处理 |

---

## 10. 与经典哈希表的对比

| 特性 | 标准 HashMap | Swiss Table (hashbrown) | TaperHashMap |
|------|-------------|------------------------|--------------|
| 探测单元 | slot | group (16 slots) | chunk (8 slots) |
| 元数据 | 无 (open addressing) 或链表 | 1 byte ctrl per slot | 1 byte tag + 8B hash per slot |
| SIMD 加速 | 无 | SSE2/NEON (16路) | SWAR (8路, 无 SIMD 依赖) |
| 值存储 | inline | inline | 6B 压缩指针 (外部 RowContainer) |
| 批量操作 | 无 | 无 | `emplace_batch` (延迟 full-key cmp) |
| 目标场景 | 通用 | 通用 | Hash aggregation |
| 对齐 | 无要求 | 16 byte group | 128 byte chunk |

---

## 11. 文件结构

```
src/
├── lib.rs            // 模块导出
├── bitmask.rs        // SWAR 位并行比较
├── chunk.rs          // 128B 对齐存储单元
├── slot_value.rs     // 6 字节压缩指针
└── taper_hashmap.rs  // 顶层哈希表 + 所有 API
```

---

## 12. 未来扩展方向

- **Prefetch hint**: 在批量 emplace 时对下一行的目标 chunk 发出 prefetch 指令
- **SIMD 加速**: 在支持的平台上用 SSE2/NEON 替换 SWAR
- **Variable-length key**: 支持非 u64 的 key 类型 (字符串、多列组合)
- **并发**: 支持 lock-free 或分片并发聚合
- **内存池**: SlotValue 指向的 RowContainer 使用 arena 分配减少 malloc 开销
