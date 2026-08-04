/// Metadata for a column in the row layout.
pub struct ColumnMeta {
    pub offset: usize,
    pub null_byte: usize,
    pub null_mask: u8,
}

/// Row-oriented container storing group keys + aggregation state.
/// Each group occupies one fixed-size row.
pub struct RowContainer {
    pool: Vec<u8>,
    row_size: usize,
    columns: Vec<ColumnMeta>,
    agg_state_offset: usize,
    num_rows: usize,
    next_offset: usize,
}

impl RowContainer {
    /// Create a new RowContainer.
    /// key_sizes: byte size of each key column.
    /// agg_state_size: total bytes for all aggregation states.
    pub fn new(key_sizes: &[usize], agg_state_size: usize) -> Self {
        let mut offset = 0;
        let mut columns = Vec::new();

        // Key columns
        for (i, &size) in key_sizes.iter().enumerate() {
            let null_byte = offset;  // simplified: null bit at start of each column
            columns.push(ColumnMeta {
                offset: offset + 1,  // +1 for null byte
                null_byte,
                null_mask: 1,
            });
            offset += 1 + size;  // 1 byte null + value
        }

        let agg_state_offset = offset;
        let row_size = offset + agg_state_size;

        RowContainer {
            pool: Vec::with_capacity(row_size * 1024),
            row_size,
            columns,
            agg_state_offset,
            num_rows: 0,
            next_offset: 0,
        }
    }

    /// Allocate a new zero-initialized row, return pointer to row start.
    pub fn new_row(&mut self) -> *mut u8 {
        let start = self.next_offset;
        self.pool.resize(start + self.row_size, 0);
        self.next_offset += self.row_size;
        self.num_rows += 1;
        unsafe { self.pool.as_mut_ptr().add(start) }
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
        // 2 key columns: i64 (8B) + i64 (8B), agg state: i64 (8B)
        let mut rc = RowContainer::new(&[8, 8], 8);

        let row = rc.new_row();
        rc.write_value::<i64>(row, 0, 12345);
        rc.write_value::<i64>(row, 1, 67890);

        assert_eq!(rc.read_value::<i64>(row, 0), 12345);
        assert_eq!(rc.read_value::<i64>(row, 1), 67890);
        assert!(!rc.is_null(row, 0));
    }
}
