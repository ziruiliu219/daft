//! Hash table microbenchmark: TaperHashMap vs hashbrown (Daft-style)
//!
//! 纯 hash table 层面的性能测试
//!
//! Key Types:
//!   - 1col_i32:  单列 i32 key (4B)
//!   - 1col_i64:  单列 i64 key (8B)
//!   - 2col_i64:  两列 i64 key (16B)
//!   - 4col_i64:  四列 i64 key (32B)
//!
//! 参数:
//!   - HT size: 256, 1024, 4096, 16384
//!   - Load Factor: 0.5, 0.75
//!   - Selectivity: 0.1 ~ 0.9

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use hashbrown::{HashMap, hash_map::RawEntryMut};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use taper_hashmap::chunk::SlotValue;
use taper_hashmap::row_container::RowContainer;
use taper_hashmap::taper_hashmap::TaperHashMap;
use xxhash_rust::xxh3::xxh3_64_with_seed;
use arrow::array::Int64Array as ArrowInt64Array;

// ═══════════════════════════════════════════════════════════════════
// Daft infra
// ═══════════════════════════════════════════════════════════════════

#[derive(Default)]
struct IdentityHasher(u64);
impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 { self.0 }
    fn write(&mut self, _: &[u8]) { unreachable!() }
    fn write_u64(&mut self, i: u64) { self.0 = i; }
}
type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;

#[derive(Eq, PartialEq)]
struct IndexHash { idx: u64, hash: u64 }
impl Hash for IndexHash {
    fn hash<H: Hasher>(&self, state: &mut H) { state.write_u64(self.hash); }
}

// ═══════════════════════════════════════════════════════════════════
// Hash functions
// ═══════════════════════════════════════════════════════════════════

#[inline]
fn hash_i64(val: i64) -> u64 {
    xxh3_64_with_seed(&val.to_le_bytes(), 0)
}

#[inline]
fn hash_i32(val: i32) -> u64 {
    xxh3_64_with_seed(&val.to_le_bytes(), 0)
}

#[inline]
fn hash_combine(seed: u64, val: i64) -> u64 {
    xxh3_64_with_seed(&val.to_le_bytes(), seed)
}

// ═══════════════════════════════════════════════════════════════════
// Key type abstraction
// ═══════════════════════════════════════════════════════════════════

/// Precomputed benchmark data for a given key type configuration.
struct BenchData {
    /// Key columns as i64 Vecs (used by Taper side for RowContainer writes).
    keys: Vec<Vec<i64>>,
    /// Key columns as Arrow Int64Array (used by Daft side for comparator).
    arrow_keys: Vec<ArrowInt64Array>,
    hashes: Vec<u64>,
    values: Vec<i64>,
    num_cols: usize,
    col_size: usize, // bytes per key column in RowContainer
}

/// Generate build+probe data for a given key type configuration.
fn generate_data(
    num_cols: usize,
    col_size: usize, // 4 for i32, 8 for i64
    num_keys: usize,
    num_probe_rows: usize,
    selectivity: f64,
    rng: &mut StdRng,
) -> BenchData {
    // Build keys: each column is deterministic
    let mut build_cols: Vec<Vec<i64>> = Vec::with_capacity(num_cols);
    for c in 0..num_cols {
        let col: Vec<i64> = (0..num_keys)
            .map(|i| i as i64 * (97 + c as i64 * 31) + 1 + c as i64)
            .collect();
        build_cols.push(col);
    }

    // Build hashes
    let build_hashes: Vec<u64> = (0..num_keys)
        .map(|i| {
            let mut h = 0u64;
            for c in 0..num_cols {
                if col_size == 4 {
                    h = if c == 0 {
                        hash_i32(build_cols[c][i] as i32)
                    } else {
                        hash_combine(h, build_cols[c][i])
                    };
                } else {
                    h = if c == 0 {
                        hash_i64(build_cols[c][i])
                    } else {
                        hash_combine(h, build_cols[c][i])
                    };
                }
            }
            h
        })
        .collect();

    let build_values: Vec<i64> = (0..num_keys).map(|i| (i % 1000) as i64).collect();

    // Probe keys
    let num_hits: usize = (num_probe_rows as f64 * selectivity) as usize;
    let num_misses = num_probe_rows - num_hits;

    let mut probe_cols: Vec<Vec<i64>> = vec![Vec::with_capacity(num_probe_rows); num_cols];
    let mut probe_hashes: Vec<u64> = Vec::with_capacity(num_probe_rows);

    // Hits: random from build keys
    for _ in 0..num_hits {
        let idx = rng.random_range(0..num_keys);
        for c in 0..num_cols {
            probe_cols[c].push(build_cols[c][idx]);
        }
        probe_hashes.push(build_hashes[idx]);
    }

    // Misses: guaranteed unique keys not in build set
    let miss_base = (num_keys as i64 + 1) * 200 + 10000;
    for i in 0..num_misses {
        let mut h = 0u64;
        for c in 0..num_cols {
            let v = miss_base + i as i64 * (31 + c as i64 * 7) + c as i64 * 3;
            probe_cols[c].push(v);
            if col_size == 4 {
                h = if c == 0 { hash_i32(v as i32) } else { hash_combine(h, v) };
            } else {
                h = if c == 0 { hash_i64(v) } else { hash_combine(h, v) };
            }
        }
        probe_hashes.push(h);
    }

    // Shuffle probe
    let mut order: Vec<usize> = (0..num_probe_rows).collect();
    for i in (1..num_probe_rows).rev() {
        order.swap(i, rng.random_range(0..=i));
    }
    let probe_cols: Vec<Vec<i64>> = (0..num_cols)
        .map(|c| order.iter().map(|&i| probe_cols[c][i]).collect())
        .collect();
    let probe_hashes: Vec<u64> = order.iter().map(|&i| probe_hashes[i]).collect();
    let probe_values: Vec<i64> = (0..num_probe_rows).map(|i| (i % 1000) as i64).collect();

    // Combine build + probe
    let mut all_cols: Vec<Vec<i64>> = Vec::with_capacity(num_cols);
    for c in 0..num_cols {
        let mut col = build_cols[c].clone();
        col.extend_from_slice(&probe_cols[c]);
        all_cols.push(col);
    }
    let mut all_hashes = build_hashes;
    all_hashes.extend_from_slice(&probe_hashes);
    let mut all_values = build_values;
    all_values.extend_from_slice(&probe_values);

    BenchData {
        keys: all_cols.clone(),
        arrow_keys: all_cols.iter().map(|col| ArrowInt64Array::from(col.clone())).collect(),
        hashes: all_hashes,
        values: all_values,
        num_cols,
        col_size,
    }
}

// ═══════════════════════════════════════════════════════════════════
// Taper runner (generic over num_cols)
// ═══════════════════════════════════════════════════════════════════

#[inline(never)]
fn run_taper(data: &BenchData, ht_size: usize) {
    let key_sizes: Vec<usize> = vec![data.col_size; data.num_cols];
    let mut rc = RowContainer::new(&key_sizes, 8);
    rc.reserve(ht_size + 256);
    let mut map = TaperHashMap::with_capacity(ht_size);
    let agg_offset = rc.agg_state_offset();

    // Precompute column offsets
    let col_offsets: Vec<usize> = (0..data.num_cols)
        .map(|c| rc.column_at(c).offset())
        .collect();

    let num_cols = data.num_cols;
    let col_size = data.col_size;

    // Use emplace_batch_full — matches C++ EmplaceBatchImpl with collision retry + prefetch
    let key_cmp = |i: usize, sv: &SlotValue| -> bool {
        let rp = sv.get_ptr();
        for c in 0..num_cols {
            if col_size == 4 {
                let stored: i32 = unsafe { (rp.add(col_offsets[c]) as *const i32).read_unaligned() };
                if stored != data.keys[c][i] as i32 {
                    return false;
                }
            } else {
                let stored: i64 = unsafe { (rp.add(col_offsets[c]) as *const i64).read_unaligned() };
                if stored != data.keys[c][i] {
                    return false;
                }
            }
        }
        true
    };

    let mut on_init = |i: usize, sv: &mut SlotValue| {
        let row = rc.new_row();
        unsafe {
            for c in 0..num_cols {
                if col_size == 4 {
                    (row.add(col_offsets[c]) as *mut i32).write_unaligned(data.keys[c][i] as i32);
                } else {
                    (row.add(col_offsets[c]) as *mut i64).write_unaligned(data.keys[c][i]);
                }
            }
            (row.add(agg_offset) as *mut i64).write_unaligned(data.values[i]);
        }
        sv.set_ptr(row as *const u8);
    };

    let mut on_update = |_i: usize, sv: &SlotValue, is_new: bool| {
        if !is_new {
            let rp = sv.get_ptr() as *mut u8;
            unsafe { *(rp.add(agg_offset) as *mut i64) += data.values[_i]; }
        }
    };

    map.emplace_batch_full(&data.hashes, &key_cmp, &mut on_init, &mut on_update);

    black_box(rc.num_rows());
}

// ═══════════════════════════════════════════════════════════════════
// Daft runner (generic over num_cols)
// ═══════════════════════════════════════════════════════════════════

#[inline(never)]
fn run_daft(data: &BenchData, ht_size: usize) {
    let mut table = HashMap::<IndexHash, u32, IdentityBuildHasher>::with_capacity_and_hasher(
        ht_size, Default::default(),
    );
    let mut ngroups: u32 = 0;
    let mut sums = Vec::<i64>::with_capacity(ht_size);
    let num_cols = data.num_cols;

    for (i, &h) in data.hashes.iter().enumerate() {
        let entry = table.raw_entry_mut().from_hash(h, |other| {
            if h != other.hash {
                return false;
            }
            let j = other.idx as usize;
            // Use Arrow Int64Array::value() — identical to Daft's comparator
            // which calls arrow array.value(i) == array.value(j)
            for c in 0..num_cols {
                if data.arrow_keys[c].value(i) != data.arrow_keys[c].value(j) {
                    return false;
                }
            }
            true
        });
        match entry {
            RawEntryMut::Occupied(e) => {
                sums[*e.get() as usize] += data.values[i];
            }
            RawEntryMut::Vacant(e) => {
                e.insert_hashed_nocheck(h, IndexHash { idx: i as u64, hash: h }, ngroups);
                ngroups += 1;
                sums.push(data.values[i]);
            }
        }
    }

    black_box(&sums);
}

// ═══════════════════════════════════════════════════════════════════
// Benchmark
// ═══════════════════════════════════════════════════════════════════

fn bench_hashagg(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashagg");
    group.sample_size(20);
    let num_probe_rows = 1_000_000;

    // Key type configurations: (name, num_cols, col_size_bytes)
    let key_types: &[(&str, usize, usize)] = &[
        ("1col_i32", 1, 4),
        ("1col_i64", 1, 8),
        ("2col_i64", 2, 8),
        ("4col_i64", 4, 8),
    ];

    for &(type_name, num_cols, col_size) in key_types {
        for &ht_size in &[256, 1024, 4096, 16384] {
            for &load_factor in &[0.5, 0.75] {
                let num_keys = (ht_size as f64 * load_factor) as usize;

                for &selectivity in &[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9] {
                    let mut rng = StdRng::seed_from_u64(42);

                    let data = generate_data(
                        num_cols, col_size, num_keys,
                        num_probe_rows, selectivity, &mut rng,
                    );

                    let param = format!(
                        "{}_ht={}_lf={:.2}_sel={:.1}",
                        type_name, ht_size, load_factor, selectivity
                    );

                    // ─── Daft ───
                    group.bench_with_input(
                        BenchmarkId::new("daft", &param),
                        &data,
                        |b, d| {
                            b.iter(|| run_daft(black_box(d), ht_size));
                        },
                    );

                    // ─── Taper ───
                    group.bench_with_input(
                        BenchmarkId::new("taper", &param),
                        &data,
                        |b, d| {
                            b.iter(|| run_taper(black_box(d), ht_size));
                        },
                    );
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_hashagg);
criterion_main!(benches);
