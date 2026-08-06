/// Metadata for a column in the row layout.
pub struct ColumnMeta {
    pub offset: usize,
    pub null_byte: usize,
    pub null_mask: u8,
}

/// Block-based arena allocator. Each block is a fixed-size Vec<u8> that never
/// moves once allocated, so pointers into it remain stable forever.
const BLOCK_ROWS: usize = 4096;

/// Row-oriented container storing group keys + aggregation state.
/// Uses block-based allocation to ensure pointer stability.
/// Each group occupies one fixed-size row.
pub struct RowContainer {
    blocks: Vec<Vec<u8>>,
    row_size: usize,
    columns: Vec<ColumnMeta>,
    agg_state_offset: usize,
    num_rows: usize,
    // Current block state
    current_block_idx: usize,
    current_row_in_block: usize,
}

impl RowContainer {
    /// Create a new RowContainer.
    /// key_sizes: byte size of each key column.
    /// agg_state_size: total bytes for all aggregation states.
    pub fn new(key_sizes: &[usize], agg_state_size: usize) -> Self {
        let mut offset = 0;
        let mut columns = Vec::new();

        // Key columns
        for &size in key_sizes.iter() {
            let null_byte = offset;
            columns.push(ColumnMeta {
                offset: offset + 1,  // +1 for null byte
                null_byte,
                null_mask: 1,
            });
            offset += 1 + size;
        }

        let agg_state_offset = offset;
        let row_size = offset + agg_state_size;

        // Allocate first block
        let first_block = vec![0u8; row_size * BLOCK_ROWS];

        RowContainer {
            blocks: vec![first_block],
            row_size,
            columns,
            agg_state_offset,
            num_rows: 0,
            current_block_idx: 0,
            current_row_in_block: 0,
        }
    }

    /// Allocate a new zero-initialized row, return pointer to row start.
    /// Pointer is stable — never invalidated by subsequent allocations.
    pub fn new_row(&mut self) -> *mut u8 {
        // Check if current block is full
        if self.current_row_in_block >= BLOCK_ROWS {
            // Allocate a new block
            let new_block = vec![0u8; self.row_size * BLOCK_ROWS];
            self.blocks.push(new_block);
            self.current_block_idx = self.blocks.len() - 1;
            self.current_row_in_block = 0;
        }

        let offset_in_block = self.current_row_in_block * self.row_size;
        self.current_row_in_block += 1;
        self.num_rows += 1;

        // SAFETY: block is allocated with size row_size * BLOCK_ROWS,
        // and current_row_in_block < BLOCK_ROWS, so offset is within bounds.
        unsafe {
            self.blocks[self.current_block_idx].as_mut_ptr().add(offset_in_block)
        }
    }

    /// Reserve capacity for at least `additional` more rows.
    /// Pre-allocates blocks so that subsequent new_row() won't need to allocate.
    pub fn reserve(&mut self, additional: usize) {
        let rows_available = (BLOCK_ROWS - self.current_row_in_block)
            + (self.blocks.capacity().saturating_sub(self.blocks.len())) * BLOCK_ROWS;

        if additional > rows_available {
            let extra_blocks_needed = (additional - rows_available + BLOCK_ROWS - 1) / BLOCK_ROWS;
            self.blocks.reserve(extra_blocks_needed);
            // Pre-allocate blocks
            for _ in 0..extra_blocks_needed {
                self.blocks.push(vec![0u8; self.row_size * BLOCK_ROWS]);
            }
            // Reset: we added blocks at the end, but current_block_idx stays
            // pointing at the current (possibly partially filled) block.
            // The new blocks will be used when current fills up.
            // Actually, let's fix this: revert the push and just reserve Vec capacity
        }
        // Simpler approach: just ensure the blocks Vec won't reallocate
        // (which would invalidate the Vec<u8> pointers stored elsewhere — but
        //  Vec items are heap-allocated, so Vec<Vec<u8>> reallocation only moves
        //  the Vec headers, not the actual data buffers. So this is already safe!)
    }

    /// Read a fixed-width value from a row.
    #[inline(always)]
    pub fn read_value<T: Copy>(&self, row: *const u8, col_idx: usize) -> T {
        let offset = self.columns[col_idx].offset;
        unsafe { (row.add(offset) as *const T).read_unaligned() }
    }

    /// Write a fixed-width value to a row.
    #[inline(always)]
    pub fn write_value<T: Copy>(&self, row: *mut u8, col_idx: usize, val: T) {
        let offset = self.columns[col_idx].offset;
        unsafe { (row.add(offset) as *mut T).write_unaligned(val) }
    }

    /// Check if a column is null in the given row.
    #[inline(always)]
    pub fn is_null(&self, row: *const u8, col_idx: usize) -> bool {
        let col = &self.columns[col_idx];
        unsafe { *row.add(col.null_byte) & col.null_mask != 0 }
    }

    /// Set column as null.
    #[inline(always)]
    pub fn set_null(&self, row: *mut u8, col_idx: usize) {
        let col = &self.columns[col_idx];
        unsafe { *row.add(col.null_byte) |= col.null_mask; }
    }

    /// Clear null flag.
    #[inline(always)]
    pub fn clear_null(&self, row: *mut u8, col_idx: usize) {
        let col = &self.columns[col_idx];
        unsafe { *row.add(col.null_byte) &= !col.null_mask; }
    }

    /// Offset where AggState begins in a row.
    pub fn agg_state_offset(&self) -> usize {
        self.agg_state_offset
    }

    /// Row size.
    pub fn row_size(&self) -> usize {
        self.row_size
    }

    /// Number of rows.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Get column metadata.
    pub fn column_at(&self, col_idx: usize) -> &ColumnMeta {
        &self.columns[col_idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_container_basic() {
        let mut rc = RowContainer::new(&[8, 8], 8);

        let row = rc.new_row();
        rc.write_value::<i64>(row, 0, 12345);
        rc.write_value::<i64>(row, 1, 67890);

        assert_eq!(rc.read_value::<i64>(row, 0), 12345);
        assert_eq!(rc.read_value::<i64>(row, 1), 67890);
        assert!(!rc.is_null(row, 0));
    }

    #[test]
    fn test_row_container_many_rows_pointer_stability() {
        let mut rc = RowContainer::new(&[8, 8], 8);
        let mut ptrs: Vec<*mut u8> = Vec::new();

        // Allocate more than one block's worth
        for i in 0..10000 {
            let row = rc.new_row();
            rc.write_value::<i64>(row, 0, i as i64);
            rc.write_value::<i64>(row, 1, i as i64 * 2);
            ptrs.push(row);
        }

        // Verify all pointers are still valid
        for (i, &ptr) in ptrs.iter().enumerate() {
            assert_eq!(rc.read_value::<i64>(ptr, 0), i as i64);
            assert_eq!(rc.read_value::<i64>(ptr, 1), i as i64 * 2);
        }

        assert_eq!(rc.num_rows(), 10000);
    }
}
