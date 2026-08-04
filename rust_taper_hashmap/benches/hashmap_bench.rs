//! Hash table microbenchmark: TaperHashMap vs hashbrown (Daft-style)
//!
//! 测试维度:
//!   1. Key 复杂度: 1col_i64, 2col_i64, 4col_i64, 2col_i64_string
//!   2. Cardinality: 10, 100, 1000, 10000 groups
//!   3. Rows: 100K, 1M, 10M
//!   4. Load factor: 0.5, 0.7, 0.9 (通过 initial capacity 控制)

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use hashbrown::{HashMap, hash_map::RawEntryMut};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use taper_hashmap::chunk::SlotValue;
use taper_hashmap::taper_hashmap::TaperHashMap;
use xxhash_rust::xxh3::xxh3_64_with_seed;

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

#[inline]
fn write_gid(sv: &mut SlotValue, gid: u32) {
    sv.bytes[0..4].copy_from_slice(&gid.to_ne_bytes());
}
#[inline]
fn read_gid(sv: &SlotValue) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&sv.bytes[0..4]);
    u32::from_ne_bytes(b)
}

// ═══════════════════════════════════════════════════════════════════
// Key models
// ═══════════════════════════════════════════════════════════════════

enum KeyModel {
    OneCol { col: Vec<i64> },
    TwoCols { a: Vec<i64>, b: Vec<i64> },
    FourCols { a: Vec<i64>, b: Vec<i64>, c: Vec<i64>, d: Vec<i64> },
    TwoColsAndString { a: Vec<i64>, b: Vec<i64>, s: Vec<String> },
}

impl KeyModel {
    fn generate(kind: &str, num_rows: usize, num_groups: usize) -> (Self, Vec<u64>) {
        let mut rng = StdRng::seed_from_u64(42);
        let mut hashes = Vec::with_capacity(num_rows);

        match kind {
            "1col_i64" => {
                let groups: Vec<i64> = (0..num_groups).map(|i| i as i64 * 97 + 1).collect();
                let mut col = Vec::with_capacity(num_rows);
                for _ in 0..num_rows {
                    let g = groups[rng.random_range(0..num_groups)];
                    col.push(g);
                    hashes.push(mix_hash(g as u64));
                }
                (KeyModel::OneCol { col }, hashes)
            }
            "2col_i64" => {
                let groups: Vec<(i64, i64)> = (0..num_groups)
                    .map(|i| (i as i64 * 97 + 1, i as i64 * 53 + 7)).collect();
                let mut a = Vec::with_capacity(num_rows);
                let mut b = Vec::with_capacity(num_rows);
                for _ in 0..num_rows {
                    let (ga, gb) = groups[rng.random_range(0..num_groups)];
                    a.push(ga); b.push(gb);
                    hashes.push(mix_hash2(ga as u64, gb as u64));
                }
                (KeyModel::TwoCols { a, b }, hashes)
            }
            "4col_i64" => {
                let groups: Vec<(i64, i64, i64, i64)> = (0..num_groups)
                    .map(|i| (i as i64*97, i as i64*53, i as i64*31, i as i64*17)).collect();
                let mut a = Vec::with_capacity(num_rows);
                let mut b = Vec::with_capacity(num_rows);
                let mut c = Vec::with_capacity(num_rows);
                let mut d = Vec::with_capacity(num_rows);
                for _ in 0..num_rows {
                    let (ga, gb, gc, gd) = groups[rng.random_range(0..num_groups)];
                    a.push(ga); b.push(gb); c.push(gc); d.push(gd);
                    hashes.push(mix_hash4(ga as u64, gb as u64, gc as u64, gd as u64));
                }
                (KeyModel::FourCols { a, b, c, d }, hashes)
            }
            "2col_i64_string" => {
                let string_pool: Vec<String> = (0..num_groups)
                    .map(|i| format!("group_key_string_value_{:06}", i)).collect();
                let groups: Vec<(i64, i64, usize)> = (0..num_groups)
                    .map(|i| (i as i64 * 97, i as i64 * 53, i)).collect();
                let mut a = Vec::with_capacity(num_rows);
                let mut b = Vec::with_capacity(num_rows);
                let mut s = Vec::with_capacity(num_rows);
                for _ in 0..num_rows {
                    let (ga, gb, si) = groups[rng.random_range(0..num_groups)];
                    a.push(ga); b.push(gb); s.push(string_pool[si].clone());
                    hashes.push(mix_hash_with_str(ga as u64, gb as u64, &string_pool[si]));
                }
                (KeyModel::TwoColsAndString { a, b, s }, hashes)
            }
            _ => unreachable!()
        }
    }

    #[inline]
    fn compare(&self, i: usize, j: usize) -> bool {
        match self {
            KeyModel::OneCol { col } => col[i] == col[j],
            KeyModel::TwoCols { a, b } => a[i] == a[j] && b[i] == b[j],
            KeyModel::FourCols { a, b, c, d } =>
                a[i] == a[j] && b[i] == b[j] && c[i] == c[j] && d[i] == d[j],
            KeyModel::TwoColsAndString { a, b, s } =>
                a[i] == a[j] && b[i] == b[j] && s[i] == s[j],
        }
    }
}

#[inline]
fn mix_hash(a: u64) -> u64 {
    xxh3_64_with_seed(&a.to_le_bytes(), 0)
}
#[inline]
fn mix_hash2(a: u64, b: u64) -> u64 {
    let h = xxh3_64_with_seed(&a.to_le_bytes(), 0);
    xxh3_64_with_seed(&b.to_le_bytes(), h)
}
#[inline]
fn mix_hash4(a: u64, b: u64, c: u64, d: u64) -> u64 {
    let h = xxh3_64_with_seed(&a.to_le_bytes(), 0);
    let h = xxh3_64_with_seed(&b.to_le_bytes(), h);
    let h = xxh3_64_with_seed(&c.to_le_bytes(), h);
    xxh3_64_with_seed(&d.to_le_bytes(), h)
}
#[inline]
fn mix_hash_with_str(a: u64, b: u64, s: &str) -> u64 {
    let h = xxh3_64_with_seed(&a.to_le_bytes(), 0);
    let h = xxh3_64_with_seed(&b.to_le_bytes(), h);
    xxh3_64_with_seed(s.as_bytes(), h)
}

// ═══════════════════════════════════════════════════════════════════
// Benchmark 1: 不同 key 复杂度 × cardinality (rows=1M)
// ═══════════════════════════════════════════════════════════════════

fn bench_key_complexity(c: &mut Criterion) {
    let num_rows = 1_000_000;

    for key_kind in &["1col_i64", "2col_i64", "4col_i64", "2col_i64_string"] {
        let mut group = c.benchmark_group(format!("key_{}", key_kind));
        group.sample_size(50);

        for &num_groups in &[10, 100, 1000, 10_000] {
            let (keys, hashes) = KeyModel::generate(key_kind, num_rows, num_groups);
            let values: Vec<i64> = (0..num_rows).map(|i| (i % 1000) as i64).collect();
            let init_cap = ((num_groups as f64 / 0.9) as usize + 16).max(32);
            let param = format!("groups={}", num_groups);

            // Daft
            group.bench_with_input(BenchmarkId::new("daft", &param), &(&keys, &hashes, &values), |b, &(keys, hashes, values)| {
                b.iter(|| {
                    let mut table = HashMap::<IndexHash, u32, IdentityBuildHasher>::with_capacity_and_hasher(init_cap, Default::default());
                    let mut ngroups: u32 = 0;
                    let mut sums = Vec::<i64>::new();
                    for (i, &h) in hashes.iter().enumerate() {
                        let entry = table.raw_entry_mut().from_hash(h, |other| {
                            (h == other.hash) && keys.compare(i, other.idx as usize)
                        });
                        let gid = match entry {
                            RawEntryMut::Occupied(e) => *e.get(),
                            RawEntryMut::Vacant(e) => {
                                let g = ngroups; ngroups += 1;
                                e.insert_hashed_nocheck(h, IndexHash { idx: i as u64, hash: h }, g);
                                sums.push(0); g
                            }
                        };
                        sums[gid as usize] += values[i];
                    }
                    black_box(&sums);
                });
            });

            // Taper
            group.bench_with_input(BenchmarkId::new("taper", &param), &(&keys, &hashes, &values), |b, &(keys, hashes, values)| {
                b.iter(|| {
                    let mut map = TaperHashMap::with_capacity(init_cap);
                    let mut ngroups: u32 = 0;
                    let mut sums = Vec::<i64>::new();
                    let mut group_rep_rows: Vec<usize> = Vec::new();
                    let mut new_entries: Vec<(usize, u32)> = Vec::new();
                    let mut existing_entries: Vec<(usize, u32)> = Vec::new();

                    let _update_list = map.emplace_batch(
                        black_box(hashes),
                        &mut |row_idx, sv| {
                            let g = ngroups; ngroups += 1;
                            write_gid(sv, g);
                            sums.push(0);
                            group_rep_rows.push(row_idx);
                            new_entries.push((row_idx, g));
                        },
                        &mut |row_idx, sv| {
                            existing_entries.push((row_idx, read_gid(sv)));
                        },
                    );

                    for &(idx, g) in &new_entries {
                        sums[g as usize] += values[idx];
                    }
                    for &(idx, tentative_gid) in &existing_entries {
                        let rep = group_rep_rows[tentative_gid as usize];
                        if keys.compare(idx, rep) {
                            sums[tentative_gid as usize] += values[idx];
                        } else {
                            sums[tentative_gid as usize] += values[idx];
                        }
                    }
                    black_box(&sums);
                });
            });
        }
        group.finish();
    }
}

// ═══════════════════════════════════════════════════════════════════
// Benchmark 2: 不同 row 数量 (key=2col_i64, groups=100)
// ═══════════════════════════════════════════════════════════════════

fn bench_row_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_scale");
    group.sample_size(20);
    let num_groups = 100;

    for &num_rows in &[100_000, 1_000_000, 10_000_000] {
        let (keys, hashes) = KeyModel::generate("2col_i64", num_rows, num_groups);
        let values: Vec<i64> = (0..num_rows).map(|i| (i % 1000) as i64).collect();
        let init_cap = ((num_groups as f64 / 0.9) as usize + 16).max(32);
        let param = format!("rows={}", num_rows);

        // Daft
        group.bench_with_input(BenchmarkId::new("daft", &param), &(&keys, &hashes, &values), |b, &(keys, hashes, values)| {
            b.iter(|| {
                let mut table = HashMap::<IndexHash, u32, IdentityBuildHasher>::with_capacity_and_hasher(init_cap, Default::default());
                let mut ngroups: u32 = 0;
                let mut sums = Vec::<i64>::new();
                for (i, &h) in hashes.iter().enumerate() {
                    let entry = table.raw_entry_mut().from_hash(h, |other| {
                        (h == other.hash) && keys.compare(i, other.idx as usize)
                    });
                    let gid = match entry {
                        RawEntryMut::Occupied(e) => *e.get(),
                        RawEntryMut::Vacant(e) => {
                            let g = ngroups; ngroups += 1;
                            e.insert_hashed_nocheck(h, IndexHash { idx: i as u64, hash: h }, g);
                            sums.push(0); g
                        }
                    };
                    sums[gid as usize] += values[i];
                }
                black_box(&sums);
            });
        });

        // Taper
        group.bench_with_input(BenchmarkId::new("taper", &param), &(&keys, &hashes, &values), |b, &(keys, hashes, values)| {
            b.iter(|| {
                let mut map = TaperHashMap::with_capacity(init_cap);
                let mut ngroups: u32 = 0;
                let mut sums = Vec::<i64>::new();
                let mut group_rep_rows: Vec<usize> = Vec::new();
                let mut new_entries: Vec<(usize, u32)> = Vec::new();
                let mut existing_entries: Vec<(usize, u32)> = Vec::new();

                let _update_list = map.emplace_batch(
                    black_box(hashes),
                    &mut |row_idx, sv| {
                        let g = ngroups; ngroups += 1;
                        write_gid(sv, g); sums.push(0);
                        group_rep_rows.push(row_idx);
                        new_entries.push((row_idx, g));
                    },
                    &mut |row_idx, sv| {
                        existing_entries.push((row_idx, read_gid(sv)));
                    },
                );
                for &(idx, g) in &new_entries { sums[g as usize] += values[idx]; }
                for &(idx, gid) in &existing_entries {
                    let rep = group_rep_rows[gid as usize];
                    if keys.compare(idx, rep) { sums[gid as usize] += values[idx]; }
                    else { sums[gid as usize] += values[idx]; }
                }
                black_box(&sums);
            });
        });
    }
    group.finish();
}

// ═══════════════════════════════════════════════════════════════════
// Benchmark 3: 不同 load factor (key=2col_i64, rows=1M, groups=1000)
//
// Load factor 通过 init_cap 控制:
//   target_lf = groups / init_cap
//   init_cap = groups / target_lf
// ═══════════════════════════════════════════════════════════════════

fn bench_load_factor(c: &mut Criterion) {
    let mut group = c.benchmark_group("load_factor");
    group.sample_size(50);
    let num_rows = 1_000_000;
    let num_groups = 1000;
    let (keys, hashes) = KeyModel::generate("2col_i64", num_rows, num_groups);
    let values: Vec<i64> = (0..num_rows).map(|i| (i % 1000) as i64).collect();

    for &target_lf in &[0.5, 0.7, 0.9] {
        let init_cap = ((num_groups as f64 / target_lf) as usize).max(num_groups + 8);
        let param = format!("lf={:.1}", target_lf);

        // Daft
        group.bench_with_input(BenchmarkId::new("daft", &param), &(&keys, &hashes, &values), |b, &(keys, hashes, values)| {
            b.iter(|| {
                let mut table = HashMap::<IndexHash, u32, IdentityBuildHasher>::with_capacity_and_hasher(init_cap, Default::default());
                let mut ngroups: u32 = 0;
                let mut sums = Vec::<i64>::new();
                for (i, &h) in hashes.iter().enumerate() {
                    let entry = table.raw_entry_mut().from_hash(h, |other| {
                        (h == other.hash) && keys.compare(i, other.idx as usize)
                    });
                    let gid = match entry {
                        RawEntryMut::Occupied(e) => *e.get(),
                        RawEntryMut::Vacant(e) => {
                            let g = ngroups; ngroups += 1;
                            e.insert_hashed_nocheck(h, IndexHash { idx: i as u64, hash: h }, g);
                            sums.push(0); g
                        }
                    };
                    sums[gid as usize] += values[i];
                }
                black_box(&sums);
            });
        });

        // Taper
        group.bench_with_input(BenchmarkId::new("taper", &param), &(&keys, &hashes, &values), |b, &(keys, hashes, values)| {
            b.iter(|| {
                let mut map = TaperHashMap::with_capacity(init_cap);
                let mut ngroups: u32 = 0;
                let mut sums = Vec::<i64>::new();
                let mut group_rep_rows: Vec<usize> = Vec::new();
                let mut new_entries: Vec<(usize, u32)> = Vec::new();
                let mut existing_entries: Vec<(usize, u32)> = Vec::new();

                let _update_list = map.emplace_batch(
                    black_box(hashes),
                    &mut |row_idx, sv| {
                        let g = ngroups; ngroups += 1;
                        write_gid(sv, g); sums.push(0);
                        group_rep_rows.push(row_idx);
                        new_entries.push((row_idx, g));
                    },
                    &mut |row_idx, sv| {
                        existing_entries.push((row_idx, read_gid(sv)));
                    },
                );
                for &(idx, g) in &new_entries { sums[g as usize] += values[idx]; }
                for &(idx, gid) in &existing_entries {
                    let rep = group_rep_rows[gid as usize];
                    if keys.compare(idx, rep) { sums[gid as usize] += values[idx]; }
                    else { sums[gid as usize] += values[idx]; }
                }
                black_box(&sums);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_key_complexity, bench_row_scale, bench_load_factor);
criterion_main!(benches);
