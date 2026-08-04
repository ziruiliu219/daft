# Hash Table 设计对比：hashbrown (Swiss Table) vs TaperHashTable

## 1. hashbrown / Swiss Table 内存布局

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Hash Table                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Group 0 (16 slots)                                                 │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │ ctrl bytes (16 bytes, 128-bit, SIMD 一次加载)            │       │
│  │ [0x23][0x91][0x80][0x56][0x12][0x80][0xFE][0x37]        │       │
│  │ [0x44][0x80][0x80][0x80][0x80][0x80][0x80][0x80]        │       │
│  │  ↑full  ↑full ↑empty ↑full ↑full ↑empty ↑del  ↑full    │       │
│  └─────────────────────────────────────────────────────────┘       │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │ slots (16 × (Key, Value))                                │       │
│  │ slot0: key=IndexHash{idx:0,hash:0x7A3F} val=SmallVec[0,2,6]    │
│  │ slot1: key=IndexHash{idx:1,hash:0xB1D2} val=SmallVec[1,3,7]    │
│  │ slot2: (empty)                                           │       │
│  │ slot3: key=IndexHash{idx:4,hash:0xC4E8} val=SmallVec[4]        │
│  │ slot4: key=IndexHash{idx:5,hash:0xD5F1} val=SmallVec[5]        │
│  │ ...                                                      │       │
│  └─────────────────────────────────────────────────────────┘       │
│                                                                     │
│  Group 1 (16 slots)                                                 │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │ ctrl bytes [0x80][0x80]...(全 empty)                     │       │
│  └─────────────────────────────────────────────────────────┘       │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │ slots (全空)                                             │       │
│  └─────────────────────────────────────────────────────────┘       │
│  ...                                                                │
└─────────────────────────────────────────────────────────────────────┘

ctrl byte 编码:
  0b0xxx_xxxx = FULL (低7bit = hash的高7bit, 即 fingerprint/tag)
  0b1000_0000 = EMPTY (0x80)
  0b1111_1110 = DELETED (0xFE)

Key: IndexHash { idx: u64, hash: u64 }  ← Daft 存的是行号+hash, 不是原始 key 值
Value: SmallVec<[u64; 2]>               ← 该 group 的所有行号
```

---

## 2. hashbrown Lookup 流程

```
输入: 要查的行 i, 该行 hash = 0x7A3F1B02

Step 1: 计算 h1, h2
  h1 = hash & mask → 定位 Group (假设 Group 0)
  h2 = (hash >> 57) & 0x7F → 0x23 (fingerprint)

Step 2: SIMD 加载 + 比较 ctrl bytes
  ┌──────────────────────────────────────────┐
  │ Group 0 ctrl: [23][91][80][56][12][80][FE][37][44][80]...  │
  │                                          │
  │ SIMD compare 0x23 vs all 16 ctrl:        │
  │ result bitmask = 1000000000000000        │
  │                  ↑ slot0 匹配!            │
  └──────────────────────────────────────────┘

Step 3: 对 bitmask 中的候选 slot 做 key 比较
  candidate: slot0
  调用闭包:
    (*h == other.hash)?      0x7A3F1B02 == 0x7A3F1B02 → true
    && comparator(i, 0)?     逐列比较 row[i] vs row[0] → true
  → 命中! 返回 Occupied

如果 Step 3 不匹配:
  继续 bitmask 中下一个候选
  如果 bitmask 耗尽, 看有没有 EMPTY slot:
    有 → key 不存在, 返回 Vacant
    无 → 跳到 Group 1 (triangular probing), 回到 Step 2
```

---

## 3. TaperHashTable 内存布局

```
┌─────────────────────────────────────────────────────────────────────┐
│                       TaperFlatHashTable                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Chunk 0 (8 slots)                                                  │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │ tags (8 bytes)                                           │       │
│  │ [0x23][0x56][0x00][0x12][0x00][0x00][0x00][0x00]        │       │
│  │  ↑used ↑used ↑empty                                     │       │
│  └─────────────────────────────────────────────────────────┘       │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │ hashes (8 × int64)   ← Stage 1b 用                      │       │
│  │ [0x7A3F1B02][0xC4E89F30][0][0][0][0][0][0]             │       │
│  └─────────────────────────────────────────────────────────┘       │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │ values (8 × 6-byte RowId)  ← 指向 RowContainer          │       │
│  │ [row_ptr_0][row_ptr_1][...][...][...][...][...][...]    │       │
│  └─────────────────────────────────────────────────────────┘       │
│                                                                     │
│  Chunk 1 (8 slots)                                                  │
│  ...                                                                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

                            │
                            │ row_ptr 指向
                            ▼

┌─────────────────────────────────────────────────────────────────────┐
│                        RowContainer                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Row 0 (Group "Beijing" + 2023):                                    │
│  ┌──────────────────────────────────────────────┐                  │
│  │ [null_bitmap][col0: "Beijing" (7B)][col1: 2023 (8B)]│           │
│  └──────────────────────────────────────────────┘                  │
│                                                                     │
│  Row 1 (Group "Shanghai" + 2024):                                   │
│  ┌──────────────────────────────────────────────┐                  │
│  │ [null_bitmap][col0: "Shanghai" (8B)][col1: 2024 (8B)]│          │
│  └──────────────────────────────────────────────┘                  │
│                                                                     │
│  ...                                                                │
└─────────────────────────────────────────────────────────────────────┘

注意: TaperHashTable 的 chunk 里:
  - 没有真正的 key (没有 "Beijing", 2023)
  - 只有 tag (7-bit) + hash (int64) + 指针 (RowId)
  - 真正的 key 存在外部 RowContainer 的 Row 里
```

---

## 4. TaperHashTable Lookup 流程

```
输入: 一批行的 hash 值 (batch emplace)
  workingHashVals = [0x7A3F1B02, 0xB1D2E4A7, 0x7A3F1B02, ...]

对每行:
  hash = 0x7A3F1B02
  tag = (hash >> 16) & 0x7F = 0x23
  chunk_idx = hash & mask → Chunk 0

Step 1a: Tag Match (SIMD)
  ┌──────────────────────────────────────────┐
  │ Chunk 0 tags: [23][56][00][12][00][00][00][00] │
  │                                          │
  │ PHBitMask::MatchTag(tags, 0x23):         │
  │ result bitmask = 10000000                │
  │                  ↑ slot0 匹配!            │
  └──────────────────────────────────────────┘

Step 1b: Hash KeyEquals (int64 ==)
  chunk.hashes[slot0] == 0x7A3F1B02?
  → true!
  → 暂定: 这行属于 group_id = chunk.values[slot0]
  → 加入 workingUpdateIndices (待验证队列)

Step 2: Deferred Full Key Compare (batch 结束后统一做)
  ┌──────────────────────────────────────────┐
  │ GetUnequalsNumWithDecode:                │
  │                                          │
  │ 对 workingUpdateIndices 中每行:           │
  │   从 RowContainer 读出 stored key        │
  │   逐列 typed compare:                    │
  │     input.city[i] == stored.city?        │
  │     input.year[i] == stored.year?        │
  │   全等 → 确认同组                         │
  │   不等 → hash 碰撞, re-Emplace           │
  └──────────────────────────────────────────┘
```

---

## 5. 并排对比图

```
═══ hashbrown (Swiss Table) ═══        ═══ TaperHashTable ═══

  hash(key)                              hash(key)
     │                                      │
     ▼                                      ▼
  h1 → Group 定位                        chunk_idx → Chunk 定位
  h2 → 7-bit fingerprint                tag → 7-bit fingerprint
     │                                      │
     ▼                                      ▼
  ┌─────────────────┐                  ┌─────────────────┐
  │ SIMD: ctrl==h2? │                  │ SIMD: tag match?│
  │ 16 slots 一次   │                  │ 8 slots 一次    │
  └────────┬────────┘                  └────────┬────────┘
           │ candidate                          │ candidate
           ▼                                    ▼
  ┌─────────────────┐                  ┌─────────────────┐
  │ 比较真正的 key   │                  │ hash == hash?   │
  │ (key 在 slot 里) │                  │ (int64, 很快)   │
  └────────┬────────┘                  └────────┬────────┘
           │                                    │
           ▼                                    ▼
        确认/拒绝                         暂定 match
                                         收集到 deferred 队列
                                                │
                                                ▼ (batch 结束后)
                                       ┌─────────────────┐
                                       │ RowContainer    │
                                       │ 逐列比较 full key│
                                       │ (typed compare) │
                                       └────────┬────────┘
                                                │
                                                ▼
                                         确认/碰撞修复
```

---

## 6. 核心差异总结表

| 维度 | hashbrown (Swiss Table) | TaperHashTable |
|------|------------------------|----------------|
| **slot 存什么** | key + value (完整数据) | tag + hash + RowId (不存 key) |
| **key 在哪** | 就在 slot 里 | 外部 RowContainer |
| **tag/fingerprint** | ctrl byte = `(hash>>57)&0x7F` | tag = `(hash>>16)&0x7F` |
| **SIMD 宽度** | 16 slots/group (128-bit) | 8 slots/chunk (64-bit) |
| **碰撞确认** | 立即比较 slot 里的 key | 先比 int64 hash → 延迟批量比 full key |
| **probe 接口** | 逐个 (entry / raw_entry_mut) | batch emplace (一次传一批) |
| **适合场景** | 通用 key-value | 多列复杂 key 的聚合 |
| **新 group 开销** | 写 key+value 到 slot | serialize key 到 RowContainer + 写 tag+hash+RowId |
| **probe 开销** | tag match + key compare (1-2次) | tag match + hash compare (极便宜) |
| **full key compare 频率** | 每次碰撞都做 | 只有 hash 碰撞（极少）才做 |

---

## 7. 为什么 Taper 更快（在 aggregation 场景下）

```
典型 aggregation 场景:
  - 1000 万行
  - 100 个 group
  - probe:insert = 99999:1

hashbrown:
  每次 probe 碰撞都要: comparator(i, j) → 回到 Arrow 数组逐列比较
  约 1000 万次 × (tag miss 过滤后约 5-10% 需要 key compare)
  = ~50-100 万次 full key compare

TaperHashTable:
  每次 probe 只做: hash == hash (int64, 一条指令)
  hash 碰撞率: 约 2^-64 ≈ 0
  full key compare: 接近 0 次!
  只有 100 次 insert 时做 RowContainer store

省掉的: 50-100 万次逐列 key compare
```

这就是为什么 Taper 在低基数多列 group-by 场景下比直接用 Swiss Table 快得多。
