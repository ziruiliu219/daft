# Daft Hash Aggregation 总体设计流程

## 总览

```mermaid
flowchart TD
    SQL["SELECT city, year, SUM(price)\nFROM orders\nGROUP BY city, year"]
    SQL --> PLANNER

    PLANNER["Planner: 有 GROUP BY?\n→ 选择 GroupedAggregateSink"]
    PLANNER --> SINK

    subgraph SINK["GroupedAggregateSink (外层并行调度)\ngrouped_aggregate.rs"]
        direction TB
        RECV["收到每个 batch"]
        RECV --> STRAT{"策略选择\n(第 1 个 batch 时)\n估算 group 基数"}
        STRAT -->|低基数| AGG_THEN["AggThenPartition\n先 partial agg 再 hash partition"]
        STRAT -->|高基数| PART_THEN["PartitionThenAgg\n先 hash partition 再 partial agg"]
        STRAT -->|UDF| PART_ONLY["PartitionOnly\n只 hash partition"]

        AGG_THEN --> PARTITION["partition_by_hash(keys, N)\nhash(key) % N → 分到 N 个桶"]
        PART_THEN --> PARTITION
        PART_ONLY --> PARTITION

        PARTITION --> STATE["state: 每个桶累积 partial 结果"]
        STATE --> FINAL["finalize:\n每个桶独立做 final agg"]
    end

    FINAL --> INNER

    subgraph INNER["单 RecordBatch 内的 agg_groupby\nops/agg.rs:35"]
        direction TB
        ENTRY["agg_groupby(to_agg, group_by)"]
        ENTRY --> UDF_CHECK{"是 MapGroups UDF?"}
        UDF_CHECK -->|是| MAP_GROUPS["map_groups()\n按组调 Python UDF"]
        UDF_CHECK -->|否| INLINE_CHECK{"can_inline_agg?"}

        INLINE_CHECK -->|true| FAST["Fast Path\nagg_groupby_inline\ninline_agg.rs:1020"]
        INLINE_CHECK -->|false| GENERIC["通用路径"]
    end

    subgraph FAST_DETAIL["Fast Path 细节 (单 Pass)"]
        direction TB
        F1["遍历每行"]
        F2["HashMap.entry(key_value)\n→ 得到 group_id"]
        F3["accumulator[group_id] += value"]
        F4["一次遍历完成\n输出 accumulators"]
        F1 --> F2 --> F3 --> F4
    end

    subgraph GENERIC_DETAIL["通用路径细节 (两 Pass)"]
        direction TB
        G1["eval_expression_list(group_by)\n→ groupby_table"]
        G2{"单列 or 多列?"}
        G3_SINGLE["单列: make_groups()\nHashMap<value, Vec<row_idx>>"]
        G3_MULTI["多列: to_probe_hash_table()\nhash_rows() + comparator"]
        G4["得到:\ngroupkey_indices\ngroupvals_indices"]
        G5["take(groupkey_indices)\n→ 去重 key 表"]
        G6["eval_agg_expression(groups=Some)\n→ grouped_sum/min/max 按索引 fold"]
        G7["拼接 [key列, agg结果列]"]
        G1 --> G2
        G2 -->|单列| G3_SINGLE --> G4
        G2 -->|多列| G3_MULTI --> G4
        G4 --> G5 --> G6 --> G7
    end

    FAST --> FAST_DETAIL
    GENERIC --> GENERIC_DETAIL
```

---

## 三条 Hash 路径的核心区别

```mermaid
flowchart LR
    subgraph PATH1["Fast Path\n(inline_agg.rs)"]
        P1_MAP["FnvHashMap<原始值, group_id>"]
        P1_ACC["accumulator[group_id] += val"]
        P1_MAP --> P1_ACC
    end

    subgraph PATH2["通用 - 单列\n(daft-groupby/arrays.rs)"]
        P2_MAP["HashMap<原始值, Vec<行号>>"]
        P2_FOLD["grouped_sum(行号列表)"]
        P2_MAP --> P2_FOLD
    end

    subgraph PATH3["通用 - 多列\n(ops/hash.rs)"]
        P3_HASH["hash_rows() → u64"]
        P3_MAP["HashMap<IndexHash, Vec<行号>>"]
        P3_CMP["冲突时 comparator 逐列比"]
        P3_FOLD["grouped_sum(行号列表)"]
        P3_HASH --> P3_MAP --> P3_CMP --> P3_FOLD
    end
```

---

## 多列路径 probe 流程（核心设计）

```mermaid
flowchart TD
    ROW["输入: row i, columns=(city, year, price)"]
    ROW --> HASH["hash_rows(): 合并 city+year 的 hash\n→ h = xxHash3(city[i]) combine xxHash3(year[i])\n→ 一个 u64 整数"]

    HASH --> PROBE["probe HashMap: raw_entry_mut().from_hash(h, ...)"]
    PROBE --> SLOT{"slot 状态?"}

    SLOT -->|空 slot| INSERT["Vacant: 插入\nIndexHash{idx:i, hash:h} → [i]\n新建一个 group"]

    SLOT -->|有值| CMP{"比较:\n1. h == slot.hash? (整数比较)"}
    CMP -->|hash 不等| NEXT["继续 probe 下一个 slot"]
    CMP -->|hash 相等| FULL_CMP{"2. comparator(row_i, row_j)\ncity[i]==city[j]?\nyear[i]==year[j]?"}

    FULL_CMP -->|不等 (hash 冲突)| NEXT
    FULL_CMP -->|相等 (同一个 group)| PUSH["Occupied: push row_i\n行号追加到该 group 的 Vec"]

    style INSERT fill:#d4edda
    style PUSH fill:#d4edda
    style NEXT fill:#fff3cd
```

关键：**不序列化**。HashMap key 存的是 `{行号, hash}`，不是序列化后的 bytes。
相等判断回查原始列数据，不做 memcmp。

---

## 与传统序列化 Hash Agg 对比

```mermaid
flowchart LR
    subgraph TRADITIONAL["传统: 序列化 key"]
        T1["多列 key"] --> T2["serialize → blob bytes"]
        T2 --> T3["hash(blob)"]
        T3 --> T4["HashMap<blob, state>"]
        T4 --> T5["probe: memcmp(blob, blob)"]
    end

    subgraph DAFT["Daft: index + comparator"]
        D1["多列 key"] --> D2["hash_rows() → u64\n(不分配 buffer)"]
        D2 --> D3["HashMap<{idx,hash}, Vec<idx>>"]
        D3 --> D4["probe: hash==hash?\n冲突→逐列比原始值"]
    end

    subgraph TAPER["Taper: 两阶段 filter"]
        TA1["多列 key"] --> TA2["hash → i64"]
        TA2 --> TA3["HashMap<i64, RowContainer>"]
        TA3 --> TA4["probe: tag→hash→\n冲突→decode 逐列比"]
    end
```

| 维度 | 传统序列化 | Daft | Taper |
|------|-----------|------|-------|
| Probe 分配内存 | 每行序列化 blob | 零 | 零 |
| HashMap key 大小 | 变长 blob | 16 bytes (idx+hash) | 8 bytes (i64) |
| 相等判断 | memcmp | 逐列比原始值 | tag→hash→decode 比 |
| 适合场景 | 简单通用 | 中小规模数据 | 超高吞吐 |

---

## 代码文件对应

| 层级 | 文件 | 职责 |
|------|------|------|
| 算子调度 | `daft-local-execution/src/sinks/grouped_aggregate.rs` | batch 接收、策略、partition、finalize |
| RecordBatch agg | `daft-recordbatch/src/ops/agg.rs` | `agg_groupby` 入口 + 通用路径 |
| Fast path | `daft-recordbatch/src/ops/inline_agg.rs` | 单 pass hash+accumulate |
| 分组 (单列) | `daft-groupby/src/arrays.rs` | `make_groups` HashMap |
| 分组 (多列) | `daft-recordbatch/src/ops/hash.rs` | `to_probe_hash_table` + comparator |
| 分组 (入口) | `daft-recordbatch/src/ops/groups.rs` | `RecordBatch::make_groups` |
| Hash 计算 | `daft-core/src/kernels/hashing.rs` | xxHash3_64 逐列 hash |
| Per-group 聚合 | `daft-core/src/array/ops/sum.rs` | `grouped_sum` 按索引 fold |
