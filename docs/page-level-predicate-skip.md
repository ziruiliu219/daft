# Page-Level Predicate Skip Optimization

## Summary

Reduce Parquet scan CPU time by skipping pages whose Column Index min/max statistics prove they cannot contain rows matching the query predicate. This avoids decompressing and decoding entire pages of data that would ultimately be filtered out.

## Problem

Current behavior when scanning a Parquet file with a filter (e.g. TPC-H q6):

```
RG with 4 pages (10K rows each):

Phase 1 (predicate column decode):
  Page 0: decompress → decode → 10K values → eval filter → 0% pass → WASTED
  Page 1: decompress → decode → 10K values → eval filter → 30% pass
  Page 2: decompress → decode → 10K values → eval filter → 25% pass
  Page 3: decompress → decode → 10K values → eval filter → 0% pass → WASTED

Total: 40K rows decoded, but only ~5K rows actually pass the filter.
Pages 0 and 3 were fully decompressed for nothing.
```

With CPU at 2400% (24 cores saturated), the bottleneck is decode throughput. Reducing the number of pages decoded directly reduces wall time.

## Solution

Before decoding predicate columns, use the Parquet **Column Index** (page-level min/max statistics) to identify pages that cannot contain matching rows. Mark those pages as `skip` in the `RowSelection` passed to the decoder.

```
Phase 1 with page-level skip:
  Column Index says: Page 0 max < filter min, Page 3 min > filter max
  
  Page 0: skip_records(10K) → NO decompress, NO decode, CPU ≈ 0
  Page 1: decompress → decode → 10K values → eval filter → 30% pass
  Page 2: decompress → decode → 10K values → eval filter → 25% pass
  Page 3: skip_records(10K) → NO decompress, NO decode, CPU ≈ 0

Total: 20K rows decoded (50% reduction)
```

## Implementation Details

### Files Changed

1. **`src/daft-parquet/src/statistics/mod.rs`**
   - Changed `mod column_range` to `pub(crate) mod column_range`
   - Reason: expose `parquet_statistics_to_column_range_statistics` to the reader module

2. **`src/daft-parquet/src/reader/mod.rs`**
   - Added new function `build_page_level_selection`
   - Modified `build_rg_inputs` to use page-level selection before `spawn_col_decoders`
   - Fixed `total_selected` and `refine_selection` to work with the effective selection

### New Function: `build_page_level_selection`

```
fn build_page_level_selection(
    metadata: &ParquetMetaData,
    rg_idx: usize,
    pred_col_indices: &[usize],
    arrow_schema: &ArrowSchema,
    daft_schema: &Schema,
    bound_pred: &BoundExpr,
) -> Option<RowSelection>
```

**Logic:**
1. Read Column Index and Offset Index from Parquet metadata
2. Compute per-page row counts from Offset Index `first_row_index` differences
3. For each page:
   a. For each predicate column, extract typed min/max from ColumnIndexMetaData
   b. Construct a `parquet::file::statistics::Statistics` object
   c. Convert to `ColumnRangeStatistics` via existing `parquet_statistics_to_column_range_statistics`
   d. Build `TableStatistics` and evaluate the bound predicate
   e. If result is `TruthValue::False` → mark page as skip
   f. Otherwise → mark as select
4. Return `None` if no pages were skipped (avoid overhead)

**Supported physical types:** INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY
**Unsupported (graceful fallback):** INT96, FIXED_LEN_BYTE_ARRAY → returns select (conservative)

### Modified: `build_rg_inputs`

Before `spawn_col_decoders` call:
```rust
// NEW: try page-level skip
let page_sel = build_page_level_selection(...);
let effective_sel = combine_selections(base_sel.clone(), page_sel);

// Pass effective_sel (with page skips) to decoders
spawn_col_decoders(..., effective_sel.as_ref(), ...);
```

After decoder loop, when constructing Phase 2's RowSelection:
```rust
// Use effective_sel (not base_sel) as the reference for refine_selection
let selection = refine_selection(&effective_sel, &pred_sel);
```

## Safety / Correctness

| Scenario | Behavior | Correct? |
|----------|----------|----------|
| Column Index not present | `build_page_level_selection` returns `None` → no change | ✅ |
| Page min/max overlaps filter | Page marked as select → decoded normally | ✅ |
| Page min/max outside filter | Page marked as skip → not decoded | ✅ (proven no matching rows) |
| Null page | Conservatively marked as select | ✅ |
| Unsupported type | Conservatively marked as select | ✅ |
| All pages need reading | Returns `None` → zero overhead | ✅ |

The optimization is strictly conservative: only skips pages when statistics **prove** no rows can match. Per-row filter evaluation still runs on all decoded rows.

## Expected Performance Impact

Depends on data ordering and filter selectivity:

| Data Ordering | Filter | Pages Skipped | CPU Savings |
|---------------|--------|---------------|-------------|
| Random (dbgen default) | date range | ~0% | None |
| Sorted by L_SHIPDATE | date range 1yr/7yr | ~70-85% | ~50-60% |
| Sorted by L_SHIPDATE | date range 1yr/7yr + other cols | ~50-70% | ~30-40% |

**To get maximum benefit, the data should be sorted by the primary filter column before writing to Parquet.** This ensures pages have narrow min/max ranges that can be effectively pruned.

## How to Test

1. Build: `.venv/bin/maturin develop --release --uv --skip-install`
2. Generate sorted TPC-H data (optional, for maximum benefit):
   ```python
   import daft
   df = daft.read_parquet("data/tpch/lineitem/*.parquet")
   df = df.sort("L_SHIPDATE")
   df.write_parquet("data/tpch/lineitem_sorted/")
   ```
3. Run TPC-H q6 benchmark and compare wall time

## Future Improvements

1. Apply page skip to Phase 2 data columns (not just Phase 1 pred columns)
2. Use Column Index `boundary_order` to enable binary search for sorted pages
3. Combine page skip with bloom filter checks for equality predicates
