use std::ops::Rem;

use common_error::{DaftError, DaftResult};
use daft_core::datatypes::UInt64Array;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_groupby::IntoGroups;
use rand::SeedableRng;

use crate::RecordBatch;

impl RecordBatch {
    fn partition_by_index(
        &self,
        targets: &UInt64Array,
        num_partitions: usize,
    ) -> DaftResult<Vec<Self>> {
        if self.len() != targets.len() {
            return Err(DaftError::ValueError(format!(
                "Mismatch of length of table and targets, {} vs {}",
                self.len(),
                targets.len()
            )));
        }
        if targets.null_count() != 0 {
            return Err(DaftError::ComputeError(format!(
                "target array can not contain nulls, contains {} nulls",
                targets.null_count()
            )));
        }

        let num_rows = self.len();

        // --- Phase 1: Count rows per partition (one pass over targets) ---
        let mut counts = vec![0usize; num_partitions];
        for &t_idx in targets.values().iter() {
            let p = t_idx as usize;
            if p >= num_partitions {
                return Err(DaftError::ComputeError(format!(
                    "idx in target array is out of bounds, target idx {t_idx} out of {num_partitions} partitions"
                )));
            }
            counts[p] += 1;
        }

        // --- Phase 2: Build a single permutation array ---
        // Compute the starting offset of each partition in the permutation.
        let mut offsets = Vec::with_capacity(num_partitions + 1);
        offsets.push(0usize);
        for &c in &counts {
            offsets.push(offsets.last().unwrap() + c);
        }

        // Scatter source indices into the permutation array at their
        // partition's current write position.
        let mut write_pos = offsets[..num_partitions].to_vec();
        let mut permutation = vec![0u64; num_rows];
        for (s_idx, &t_idx) in targets.values().iter().enumerate() {
            let p = t_idx as usize;
            permutation[write_pos[p]] = s_idx as u64;
            write_pos[p] += 1;
        }

        // --- Phase 3: Single gather + slice ---
        // Perform one `take` per column using the full permutation, then
        // slice into per-partition RecordBatches (zero-copy on the arrow level).
        let perm_indices = UInt64Array::from_vec("idx", permutation);
        let gathered = self.take(&perm_indices)?;

        let result = (0..num_partitions)
            .map(|p| {
                let start = offsets[p];
                let len = counts[p];
                gathered.slice(start, start + len)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(result)
    }

    pub fn partition_by_hash(
        &self,
        exprs: &[BoundExpr],
        num_partitions: usize,
    ) -> DaftResult<Vec<Self>> {
        if num_partitions == 0 {
            return Err(DaftError::ValueError(
                "Can not partition a Table by 0 partitions".to_string(),
            ));
        }

        let targets =
            self.eval_expression_list(exprs)?
                .hash_rows()?
                .rem(&UInt64Array::from_slice(
                    "num_partitions",
                    &[num_partitions as u64],
                ))?;
        self.partition_by_index(&targets, num_partitions)
    }

    pub fn partition_by_random(&self, num_partitions: usize, seed: u64) -> DaftResult<Vec<Self>> {
        if num_partitions == 0 {
            return Err(DaftError::ValueError(
                "Can not partition a Table by 0 partitions".to_string(),
            ));
        }
        use rand::{Rng, distr::Uniform};
        let range = Uniform::try_from(0..num_partitions as u64).unwrap();

        let rng = rand::rngs::StdRng::seed_from_u64(seed);
        let values: Vec<u64> = rng.sample_iter(&range).take(self.len()).collect();
        let targets = UInt64Array::from_vec("idx", values);

        self.partition_by_index(&targets, num_partitions)
    }

    pub fn partition_by_range(
        &self,
        partition_keys: &[BoundExpr],
        boundaries: &Self,
        descending: &[bool],
    ) -> DaftResult<Vec<Self>> {
        if boundaries.is_empty() {
            return Ok(vec![self.clone()]);
        }
        let partition_key_table = self.eval_expression_list(partition_keys)?;
        let targets = boundaries.search_sorted(&partition_key_table, descending)?;
        self.partition_by_index(&targets, boundaries.len() + 1)
    }

    pub fn partition_by_value(
        &self,
        partition_keys: &[BoundExpr],
    ) -> DaftResult<(Vec<Self>, Self)> {
        let partition_key_table = self.eval_expression_list(partition_keys)?;
        let (key_idx, group_idx) = partition_key_table.make_groups()?;
        let key_idx = UInt64Array::from_vec("idx", key_idx);
        let pkeys_per_output_table = partition_key_table.take(&key_idx)?;
        drop(partition_key_table);
        let output_tables = group_idx
            .into_iter()
            .map(|gidx| {
                let gidx = UInt64Array::from_vec("idx", gidx.into_vec());
                self.take(&gidx)
            })
            .collect::<DaftResult<Vec<_>>>()?;
        Ok((output_tables, pkeys_per_output_table))
    }

    pub fn partition_by_value_projected(
        &self,
        partition_keys: &[BoundExpr],
        projection: &[usize],
    ) -> DaftResult<(Vec<Self>, Self)> {
        let partition_key_table = self.eval_expression_list(partition_keys)?;
        let (key_idx, group_idx) = partition_key_table.make_groups()?;
        let key_idx = UInt64Array::from_vec("idx", key_idx);
        let pkeys_per_output_table = partition_key_table.take(&key_idx)?;
        drop(partition_key_table);
        let projected = self.get_columns(projection);
        let output_tables = group_idx
            .into_iter()
            .map(|gidx| {
                let gidx = UInt64Array::from_vec("idx", gidx.into_vec());
                projected.take(&gidx)
            })
            .collect::<DaftResult<Vec<_>>>()?;
        Ok((output_tables, pkeys_per_output_table))
    }
}
