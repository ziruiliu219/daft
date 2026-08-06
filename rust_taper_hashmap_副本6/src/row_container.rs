/// Packed representation of offset, null byte offset and null mask for
/// a column inside a RowContainer.
/// Mirrors C++ `RowColumn` — packs offset/nullByte/nullMask into a single u64.
///
/// Encoding:
///   bits [63:32] = column data offset in row
///   bits [31:8]  = null byte offset in row (derived from bit offset)
///   bits [7:0]   = null mask (1 << bit_within_byte)
#[derive(Clone, Copy)]
pub struct RowColumn {
    packed: u64,
}

impl RowColumn {
    /// Pack offset and null bit offset into a single u64.
    /// Mirrors C++ `RowColumn::PackOffsets`.
    ///
    /// - `offset`: byte offset of this column's data within the row
    /// - `null_bit_offset`: bit offset within the null block (0-based)
    /// - `null_block_start`: byte offset where null block begins in the row
    fn pack(offset: usize, null_bit_offset: usize, null_block_start: usize) -> Self {
        let null_byte = null_block_start + null_bit_offset / 8;
        let null_mask: u8 = 1 << (null_bit_offset & 7);
        let packed = ((offset as u64) << 32) | ((null_byte as u64) << 8) | (null_mask as u64);
        RowColumn { packed }
    }

    /// Column data offset within the row.
    #[inline(always)]
    pub fn offset(&self) -> usize {
        (self.packed >> 32) as usize
    }

    /// Byte offset of the null flag within the row.
    #[inline(always)]
    pub fn null_byte(&self) -> usize {
        ((self.packed >> 8) & 0x00FF_FFFF) as usize
    }

    /// Bit mask for the null flag.
    #[inline(always)]
    pub fn null_mask(&self) -> u8 {
        (self.packed & 0xFF) as u8
    }
}

/// Block-based arena allocator. Each block is a fixed-size Vec<u8> that never
/// moves once allocated, so pointers into it remain stable.
/// Mirrors C++ RowContainer's batch allocation (kBatchSize = 1024).
const BLOCK_ROWS: usize = 1024;

/// Row-oriented container storing group keys + aggregation state.
/// Uses block-based allocation to ensure pointer stability.
///
/// Row layout (matching C++):
///   [key0 data][key1 data]...[keyN data][null_block][agg_state data]
///
/// - Key data is packed at the beginning, no per-column null byte inline.
/// - Null block: ceil((numKeys) / 8) bytes, one bit per key column.
/// - AggState follows null block.
pub struct RowContainer {
    blocks: Vec<Vec<u8>>,
    row_size: usize,
    columns: Vec<RowColumn>,
    null_block_start: usize,
    null_bytes: usize,       // number of bytes in null block
    agg_state_offset: usize,
    num_keys: usize,
    num_rows: usize,
    // Current block state
    current_block_idx: usize,
    current_row_in_block: usize,
}

impl RowContainer {
    /// Create a new RowContainer.
    ///
    /// Layout mirrors C++:
    ///   [key0(size0)][key1(size1)]...[null_block][agg_state(aggSize)]
    ///
    /// - `key_sizes`: byte size of each key column
    /// - `agg_state_size`: total bytes for all aggregation states
    pub fn new(key_sizes: &[usize], agg_state_size: usize) -> Self {
        let num_keys = key_sizes.len();

        // Compute key data offsets (packed sequentially)
        let mut offsets: Vec<usize> = Vec::with_capacity(num_keys);
        let mut cur_offset = 0usize;
        for &size in key_sizes {
            offsets.push(cur_offset);
            cur_offset += size;
        }

        // Null block starts right after all key data
        let null_block_start = cur_offset;
        let null_bytes = (num_keys + 7) / 8; // ceil(numKeys / 8)

        // AggState starts after null block
        let agg_state_offset = null_block_start + null_bytes;
        let row_size = agg_state_offset + agg_state_size;

        // Build RowColumn descriptors (packed, matching C++)
        let columns: Vec<RowColumn> = (0..num_keys)
            .map(|i| RowColumn::pack(offsets[i], i, null_block_start))
            .collect();

        // Allocate first block
        let first_block = vec![0u8; row_size * BLOCK_ROWS];

        RowContainer {
            blocks: vec![first_block],
            row_size,
            columns,
            null_block_start,
            null_bytes,
            agg_state_offset,
            num_keys,
            num_rows: 0,
            current_block_idx: 0,
            current_row_in_block: 0,
        }
    }

    /// Allocate a new zero-initialized row, return pointer to row start.
    /// Pointer is stable — never invalidated by subsequent allocations.
    /// Mirrors C++ `RowContainer::NewRow()`.
    pub fn new_row(&mut self) -> *mut u8 {
        if self.current_row_in_block >= BLOCK_ROWS {
            let new_block = vec![0u8; self.row_size * BLOCK_ROWS];
            self.blocks.push(new_block);
            self.current_block_idx = self.blocks.len() - 1;
            self.current_row_in_block = 0;
        }

        let offset_in_block = self.current_row_in_block * self.row_size;
        self.current_row_in_block += 1;
        self.num_rows += 1;

        unsafe {
            self.blocks[self.current_block_idx].as_mut_ptr().add(offset_in_block)
        }
    }

    /// Reserve capacity for at least `additional` more rows.
    pub fn reserve(&mut self, additional: usize) {
        let rows_in_current = BLOCK_ROWS - self.current_row_in_block;
        if additional > rows_in_current {
            let extra_needed = additional - rows_in_current;
            let blocks_needed = (extra_needed + BLOCK_ROWS - 1) / BLOCK_ROWS;
            self.blocks.reserve(blocks_needed);
        }
    }

    /// Check if a column is null in the given row.
    /// Mirrors C++ `RowContainer::IsNullAt`.
    #[inline(always)]
    pub fn is_null_at(row: *const u8, null_byte: usize, null_mask: u8) -> bool {
        unsafe { *row.add(null_byte) & null_mask != 0 }
    }

    /// Set a column to null in the given row.
    /// Mirrors C++ `RowContainer::SetNullAt`.
    #[inline(always)]
    pub fn set_null_at(row: *mut u8, null_byte: usize, null_mask: u8) {
        unsafe { *row.add(null_byte) |= null_mask; }
    }

    /// Clear a column's null flag in the given row.
    /// Mirrors C++ `RowContainer::ClearNullAt`.
    #[inline(always)]
    pub fn clear_null_at(row: *mut u8, null_byte: usize, null_mask: u8) {
        unsafe { *row.add(null_byte) &= !null_mask; }
    }

    /// Read a fixed-width value from a row at the given offset.
    /// Mirrors C++ `RowContainer::ReadValue<T>`.
    #[inline(always)]
    pub fn read_value<T: Copy>(row: *const u8, offset: usize) -> T {
        unsafe { (row.add(offset) as *const T).read_unaligned() }
    }

    /// Store a fixed-width value into a row at the given offset.
    /// Mirrors C++ `RowContainer::StoreValue<T>`.
    #[inline(always)]
    pub fn store_value<T: Copy>(row: *mut u8, offset: usize, value: T) {
        unsafe { (row.add(offset) as *mut T).write_unaligned(value); }
    }

    /// Get the RowColumn descriptor for a given column index.
    /// Mirrors C++ `RowContainer::ColumnAt`.
    #[inline(always)]
    pub fn column_at(&self, col_idx: usize) -> RowColumn {
        self.columns[col_idx]
    }

    /// Offset where AggState begins in a row.
    #[inline(always)]
    pub fn agg_state_offset(&self) -> usize {
        self.agg_state_offset
    }

    /// Fixed row size.
    #[inline(always)]
    pub fn row_size(&self) -> usize {
        self.row_size
    }

    /// Number of key columns.
    pub fn num_keys(&self) -> usize {
        self.num_keys
    }

    /// Number of rows allocated.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_layout() {
        // 2 keys: i64(8B) + i32(4B), agg: i64(8B)
        let rc = RowContainer::new(&[8, 4], 8);

        // Layout should be: [key0: 0..8][key1: 8..12][null_block: 12..13][agg: 13..21]
        assert_eq!(rc.column_at(0).offset(), 0);
        assert_eq!(rc.column_at(1).offset(), 8);
        assert_eq!(rc.agg_state_offset(), 13); // 12 + ceil(2/8)=1
        assert_eq!(rc.row_size(), 21);         // 13 + 8

        // Null masks: col0 → bit0, col1 → bit1
        assert_eq!(rc.column_at(0).null_byte(), 12);
        assert_eq!(rc.column_at(0).null_mask(), 0x01);
        assert_eq!(rc.column_at(1).null_byte(), 12);
        assert_eq!(rc.column_at(1).null_mask(), 0x02);
    }

    #[test]
    fn test_row_container_basic() {
        let mut rc = RowContainer::new(&[8, 8], 8);

        let row = rc.new_row();
        let col0 = rc.column_at(0);
        let col1 = rc.column_at(1);

        RowContainer::store_value::<i64>(row, col0.offset(), 12345);
        RowContainer::store_value::<i64>(row, col1.offset(), 67890);

        assert_eq!(RowContainer::read_value::<i64>(row, col0.offset()), 12345);
        assert_eq!(RowContainer::read_value::<i64>(row, col1.offset()), 67890);
        assert!(!RowContainer::is_null_at(row, col0.null_byte(), col0.null_mask()));
    }

    #[test]
    fn test_null_operations() {
        let mut rc = RowContainer::new(&[8, 8], 8);
        let row = rc.new_row();
        let col0 = rc.column_at(0);

        // Initially not null (zero-initialized)
        assert!(!RowContainer::is_null_at(row, col0.null_byte(), col0.null_mask()));

        // Set null
        RowContainer::set_null_at(row, col0.null_byte(), col0.null_mask());
        assert!(RowContainer::is_null_at(row, col0.null_byte(), col0.null_mask()));

        // Clear null
        RowContainer::clear_null_at(row, col0.null_byte(), col0.null_mask());
        assert!(!RowContainer::is_null_at(row, col0.null_byte(), col0.null_mask()));
    }

    #[test]
    fn test_many_columns_null_packing() {
        // 10 columns — need 2 null bytes
        let rc = RowContainer::new(&[8; 10], 8);

        // col[7] should be bit 7 of null_byte 0
        assert_eq!(rc.column_at(7).null_mask(), 0x80);

        // col[8] should be bit 0 of null_byte 1
        let null_block_start = 8 * 10; // 80
        assert_eq!(rc.column_at(8).null_byte(), null_block_start + 1);
        assert_eq!(rc.column_at(8).null_mask(), 0x01);

        // col[9] should be bit 1 of null_byte 1
        assert_eq!(rc.column_at(9).null_byte(), null_block_start + 1);
        assert_eq!(rc.column_at(9).null_mask(), 0x02);
    }

    #[test]
    fn test_pointer_stability() {
        let mut rc = RowContainer::new(&[8, 8], 8);
        let mut ptrs: Vec<*mut u8> = Vec::new();
        let col0 = rc.column_at(0);

        // Allocate more than one block's worth
        for i in 0..5000 {
            let row = rc.new_row();
            RowContainer::store_value::<i64>(row, col0.offset(), i as i64);
            ptrs.push(row);
        }

        // Verify all pointers are still valid
        for (i, &ptr) in ptrs.iter().enumerate() {
            assert_eq!(RowContainer::read_value::<i64>(ptr, col0.offset()), i as i64);
        }

        assert_eq!(rc.num_rows(), 5000);
    }
}
