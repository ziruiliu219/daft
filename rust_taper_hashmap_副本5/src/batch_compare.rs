/// Batch key comparison (Stage 2): compare input values vs RowContainer stored values.
/// Returns number of unequal rows. Unequal row indices are moved to the front of `indices`.

/// Scalar implementation: works on all platforms.
pub fn compare_i64_scalar(
    indices: &mut [u32],
    count: usize,
    input_values: &[i64],
    groups: &[*const u8],
    offset: usize,
) -> usize {
    let mut idx_from = 0;

    for i in 0..count {
        let idx = indices[i] as usize;
        let row = groups[idx];
        let stored: i64 = unsafe { (row.add(offset) as *const i64).read_unaligned() };
        let input = input_values[idx];

        if stored != input {
            indices.swap(i, idx_from);
            idx_from += 1;
        }
    }

    idx_from
}

/// NEON SIMD implementation for aarch64 (compare multiple rows in parallel).
#[cfg(target_arch = "aarch64")]
pub fn compare_i64_neon(
    indices: &mut [u32],
    count: usize,
    input_values: &[i64],
    groups: &[*const u8],
    offset: usize,
) -> usize {
    use std::arch::aarch64::*;

    let mut idx_from = 0;

    // Process 2 rows at a time using NEON 128-bit (2 × i64)
    let mut i = 0;
    while i + 2 <= count {
        let idx0 = indices[i] as usize;
        let idx1 = indices[i + 1] as usize;

        let row0 = groups[idx0];
        let row1 = groups[idx1];

        unsafe {
            let stored0: i64 = (row0.add(offset) as *const i64).read_unaligned();
            let stored1: i64 = (row1.add(offset) as *const i64).read_unaligned();
            let input0 = input_values[idx0];
            let input1 = input_values[idx1];

            let v_stored = vcombine_s64(vcreate_s64(stored0 as u64), vcreate_s64(stored1 as u64));
            let v_input = vcombine_s64(vcreate_s64(input0 as u64), vcreate_s64(input1 as u64));

            // Compare: element-wise equal
            let cmp = vceqq_s64(v_stored, v_input);
            let mask0 = vgetq_lane_u64::<0>(cmp);
            let mask1 = vgetq_lane_u64::<1>(cmp);

            if mask0 == 0 {  // not equal
                indices.swap(i, idx_from);
                idx_from += 1;
            }
            if mask1 == 0 {  // not equal
                indices.swap(i + 1, idx_from);
                idx_from += 1;
            }
        }
        i += 2;
    }

    // Handle remaining row
    while i < count {
        let idx = indices[i] as usize;
        let row = groups[idx];
        let stored: i64 = unsafe { (row.add(offset) as *const i64).read_unaligned() };
        let input = input_values[idx];
        if stored != input {
            indices.swap(i, idx_from);
            idx_from += 1;
        }
        i += 1;
    }

    idx_from
}

/// Dispatch to best available implementation.
pub fn compare_i64(
    indices: &mut [u32],
    count: usize,
    input_values: &[i64],
    groups: &[*const u8],
    offset: usize,
) -> usize {
    #[cfg(target_arch = "aarch64")]
    {
        compare_i64_neon(indices, count, input_values, groups, offset)
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        compare_i64_scalar(indices, count, input_values, groups, offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_scalar_basic() {
        // Simulate: 3 rows in updateList, one has a mismatch
        let mut pool = vec![0u8; 100];
        let offset = 0;

        // Row 0: stored value = 42
        unsafe { *(pool.as_mut_ptr().add(0) as *mut i64) = 42; }
        // Row 1: stored value = 77
        unsafe { *(pool.as_mut_ptr().add(16) as *mut i64) = 77; }
        // Row 2: stored value = 42 (but input will be different)
        unsafe { *(pool.as_mut_ptr().add(32) as *mut i64) = 42; }

        let groups: Vec<*const u8> = vec![
            pool.as_ptr(),
            unsafe { pool.as_ptr().add(16) },
            unsafe { pool.as_ptr().add(32) },
        ];

        let input_values: Vec<i64> = vec![42, 77, 99];  // row2 mismatch!
        let mut indices: Vec<u32> = vec![0, 1, 2];

        let unequals = compare_i64_scalar(&mut indices, 3, &input_values, &groups, offset);

        assert_eq!(unequals, 1);
        assert_eq!(indices[0], 2);  // row 2 is the unequal one
    }
}
