# Tag → Hash Compare 流程对比

## hash 值的拆分

一个 64-bit hash 值被拆成不同部分，用在不同阶段：

```
hash = 0x7A3F1B02_C4E89F30 (64 bit)

       ┌─── 高位 ──────────────────────────── 低位 ───┐
bit:   63        57  56        16  15                 0
       ├──────────┤  ├──────────┤  ├─────────────────┤
       │ 7 bit    │  │ 中间 bit │  │ 低位             │
       └──────────┘  └──────────┘  └─────────────────┘
           │              │               │
           ▼              ▼               ▼
     hashbrown tag    Taper tag     slot/Group 定位
     (hash>>57)&0x7F  (hash>>16)&0x7F   hash & mask
```

---

## Daft (hashbrown) 的流程

```
输入: row i, hash = 0x7A3F1B02C4E89F30

Step 0: 拆 hash
  tag (ctrl byte) = (0x7A3F1B02C4E89F30 >> 57) & 0x7F = 0x3D
  group_idx       = hash & mask → 定位到 Group 2

Step 1: SIMD Tag Match (hashbrown 内部自动做, Daft 看不到)
  ┌───────────────────────────────────────────────────┐
  │ Group 2 的 16 个 ctrl bytes:                       │
  │                                                   │
  │ [3D][12][80][56][3D][80][80][80]                  │
  │ [80][80][80][80][80][80][80][80]                  │
  │  ↑                ↑                               │
  │ slot0            slot4                            │
  │                                                   │
  │ SIMD: 把 0x3D 广播到 128-bit 寄存器                │
  │       和 16 个 ctrl 同时比较                        │
  │                                                   │
  │ 结果 bitmask = 1000100000000000                   │
  │               slot0 ✓  slot4 ✓  其余 ✗            │
  │                                                   │
  │ → 只有 slot0 和 slot4 的 tag 匹配                  │
  │ → 其余 14 个 slot: 跳过, 不进闭包                   │
  └───────────────────────────────────────────────────┘
                    │
                    ▼ (只对 tag 匹配的 slot 调闭包)

Step 2: Hash Compare (Daft 的闭包第一部分)
  ┌───────────────────────────────────────────────────┐
  │ slot0: IndexHash { idx:5, hash:0x7A3F1B02C4E89F30}│
  │                                                   │
  │ *h == other.hash ?                                │
  │ 0x7A3F1B02C4E89F30 == 0x7A3F1B02C4E89F30         │
  │ → true ✓                                          │
  │                                                   │
  │ (如果 false → 尝试 slot4)                          │
  └───────────────────────────────────────────────────┘
                    │
                    ▼ (hash 相等, 进入 comparator)

Step 3: Full Key Compare (Daft 的闭包第二部分, 立即执行)
  ┌───────────────────────────────────────────────────┐
  │ comparator(i, 5):                                  │
  │   city_array[i] == city_array[5]?  → true          │
  │   year_array[i] == year_array[5]?  → true          │
  │ → return true                                      │
  │                                                   │
  │ → Occupied! row i 属于 slot0 代表的 group           │
  └───────────────────────────────────────────────────┘
```

---

## Taper 的流程

```
输入: batch of rows, workingHashVals[i] = 0x7A3F1B02C4E89F30

Step 0: 拆 hash
  tag       = (0x7A3F1B02C4E89F30 >> 16) & 0x7F = 0x1B
  chunk_idx = hash & mask → 定位到 Chunk 3

Step 1: SIMD Tag Match (Taper 显式做)
  ┌───────────────────────────────────────────────────┐
  │ Chunk 3 的 8 个 tags:                              │
  │                                                   │
  │ [1B][45][00][1B][00][00][00][00]                  │
  │  ↑          ↑                                     │
  │ slot0      slot3                                  │
  │                                                   │
  │ PHBitMask::MatchTag(tags, 0x1B):                  │
  │ 结果 bitmask = 10010000                           │
  │              slot0 ✓  slot3 ✓                      │
  │                                                   │
  │ → 只有 slot0 和 slot3 需要继续检查                  │
  └───────────────────────────────────────────────────┘
                    │
                    ▼ (只对 tag 匹配的 slot)

Step 2: Hash Compare (int64 KeyEquals)
  ┌───────────────────────────────────────────────────┐
  │ slot0: chunk.hashes[0] = 0x7A3F1B02C4E89F30      │
  │                                                   │
  │ workingHashVals[i] == chunk.hashes[0] ?           │
  │ 0x7A3F1B02C4E89F30 == 0x7A3F1B02C4E89F30         │
  │ → true ✓                                          │
  │                                                   │
  │ → 暂定匹配! group_id = chunk.group_ids[0]         │
  │ → 加入 workingUpdateIndices 队列                   │
  │ → 不做 full key compare!                           │
  └───────────────────────────────────────────────────┘
                    │
                    │ (batch 里所有行都处理完后...)
                    ▼

Step 3: Deferred Full Key Compare (只对队列里的行, 批量做)
  ┌───────────────────────────────────────────────────┐
  │ GetUnequalsNumWithDecode:                          │
  │                                                   │
  │ 对 workingUpdateIndices 中的 row i:                │
  │   stored = RowContainer[group_id]                  │
  │   input.city[i] == stored.city?  → true            │
  │   input.year[i] == stored.year?  → true            │
  │ → 确认: 真的同组                                    │
  │                                                   │
  │ (如果不等 → hash 碰撞, re-Emplace)                 │
  └───────────────────────────────────────────────────┘
```

---

## 并排对比

```
时间轴 →

Daft:
  ┌──────┐   ┌──────────┐   ┌──────────────────┐
  │ Tag  │──→│ Hash ==  │──→│ comparator(i,j)  │──→ 确认/拒绝
  │ SIMD │   │ u64 ==   │   │ 逐列比 Arrow     │
  │ 内部 │   │          │   │ 立即执行          │
  └──────┘   └──────────┘   └──────────────────┘
  ← hashbrown 内部 →         ← Daft 闭包 →
                              ↑ 每次碰撞都做

Taper:
  ┌──────┐   ┌──────────┐             ┌────────────────────┐
  │ Tag  │──→│ Hash ==  │──→ 暂定 ──→ │ Full Key Compare   │
  │ SIMD │   │ int64 == │   攒队列    │ RowContainer decode │
  │ 显式 │   │          │             │ 批量执行            │
  └──────┘   └──────────┘             └────────────────────┘
  ← Phase 1 (batch emplace) →         ← Phase 2 (deferred) →
                                       ↑ 只有 hash 碰撞才做
                                         (概率 ≈ 0)
```

---

## 关键区别一目了然

| 时刻 | Daft | Taper |
|------|------|-------|
| Tag 匹配后 | 进闭包 → 比 hash → 比 full key → 确认 | 只比 hash → 暂定 → 攒起来 |
| Full key 比较 | **每次** hash 相等都立即做 | **batch 结束后**统一做，且只对碰撞行 |
| 确认延迟 | 0（立即知道结果） | 有延迟（batch 结束才知道） |
| Full key 比较次数 | = hash 碰撞次数 + 真匹配次数 | ≈ 真碰撞次数（极少，接近 0） |

**为什么 Taper 的 full key compare 几乎不执行**: 64-bit hash 碰撞概率约 2^-64。如果 hash 相等，99.999...% 的情况下 key 就是相等的。所以"暂定匹配"几乎永远是对的，Phase 2 验证出不等的概率趋近于 0。
