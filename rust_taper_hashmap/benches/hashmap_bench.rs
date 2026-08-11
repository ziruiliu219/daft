//! Hash table microbenchmark: TaperHashMap vs hashbrown (Daft-style)
//!
//! 纯 hash table 层面的 build + probe 性能测试 (2col_i64 key)
//!
//! 参数:
//!   - HT size: 256, 1024, 4096, 16384 (hash table slot 数)
//!   - Load Factor: 0.5, 0.75 → num_keys = ht_size * lf
//!   - Selectivity: 0.1, 0.3, 0.5, 0.7, 0.9
//!     (probe_hits / num_probe_rows)
//!
//! 流程:
//!   Build: 插入 num_keys 个唯一 key
//!   Probe: 1M 行，selectivity 比例 hit，其余 miss

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use hashbrown::{HashMap, hash_map::RawEntryMut};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use taper_hashmap::batch_compare::compare_i64;
use taper_hashmap::row_container::RowContainer;
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

// ═══════════════════════════════════════════════════════════════════
// Hash function
// ═══════════════════════════════════════════════════════════════════

#[inline]
fn hash2(a: u64, b: u64) -> u64 {
    let h = xxh3_64_with_seed(&a.to_le_bytes(), 0);
    xxh3_64_with_seed(&b.to_le_bytes(), h)
}

// ═══════════════════════════════════════════════════════════════════
// Taper: build + probe
// ═══════════════════════════════════════════════════════════════════

#[inline(never)]
fn run_taper_build_probe(
    build_a: &[i64], build_b: &[i64], build_hashes: &[u64], build_values: &[i64],
    probe_a: &[i64], probe_b: &[i64], probe_hashes: &[u64], probe_values: &[i64],
    ht_size: usize, num_keys: usize, num_misses: usize,
) {
    let mut rc = RowContainer::new(&[8, 8], 8);
    rc.reserve(num_keys + num_misses + 64);
    let mut map = TaperHashMap::with_capacity(ht_size);
    let agg_offset = rc.agg_state_offset();
    let col0_offset = rc.column_at(0).offset;
    let col1_offset = rc.column_at(1).offset;

    // ═══ Build: insert all unique keys ═══
    map.emplace_batch(
        build_hashes,
        &mut |row_idx, sv: &mut taper_hashmap::chunk::SlotValue| {
            let row = rc.new_row();
            unsafe {
                (row.add(col0_offset) as *mut i64).write_unaligned(build_a[row_idx]);
                (row.add(col1_offset) as *mut i64).write_unaligned(build_b[row_idx]);
                (row.add(agg_offset) as *mut i64).write_unaligned(build_values[row_idx]);
            }
            sv.set_ptr(row as *const u8);
        },
        &mut |_row_idx, _sv| {},
    );

    // ═══ Probe: probe_batch → batch compare → aggregate ═══
    let num_probe = probe_hashes.len();
    let mut hit_entries: Vec<(usize, *const u8)> = Vec::with_capacity(num_probe);
    let mut miss_indices: Vec<usize> = Vec::new();

    map.probe_batch(
        probe_hashes,
        &mut |row_idx, sv| { hit_entries.push((row_idx, sv.get_ptr())); },
        &mut |row_idx| { miss_indices.push(row_idx); },
    );

    // Batch key compare on hits
    let num_hits = hit_entries.len();
    if num_hits > 0 {
        let mut indices: Vec<u32> = (0..num_hits as u32).collect();
        let group_ptrs: Vec<*const u8> = hit_entries.iter().map(|&(_, ptr)| ptr).collect();
        let hit_row_indices: Vec<usize> = hit_entries.iter().map(|&(idx, _)| idx).collect();

        // Compare col 0
        let input_a: Vec<i64> = (0..num_hits).map(|i| probe_a[hit_row_indices[i]]).collect();
        let mut mismatches = compare_i64(&mut indices, num_hits, &input_a, &group_ptrs, col0_offset);

        // Compare col 1
        if mismatches < num_hits {
            let input_b: Vec<i64> = (0..num_hits).map(|i| probe_b[hit_row_indices[i]]).collect();
            let m2 = compare_i64(&mut indices[mismatches..], num_hits - mismatches, &input_b, &group_ptrs, col1_offset);
            mismatches += m2;
        }

        // Matched: aggregate
        for i in mismatches..num_hits {
            let pos = indices[i] as usize;
            let row_ptr = group_ptrs[pos] as *mut u8;
            unsafe {
                let agg_ptr = row_ptr.add(agg_offset) as *mut i64;
                *agg_ptr += probe_values[hit_row_indices[pos]];
            }
        }

        // Mismatched (collision): insert as new
        for i in 0..mismatches {
            let pos = indices[i] as usize;
            let row_idx = hit_row_indices[pos];
            let hash = probe_hashes[row_idx];
            let ka = probe_a[row_idx];
            let kb = probe_b[row_idx];
            let val = probe_values[row_idx];
            map.emplace(hash,
                &|sv| {
                    let rp = sv.get_ptr();
                    let sa: i64 = unsafe { (rp.add(col0_offset) as *const i64).read_unaligned() };
                    let sb: i64 = unsafe { (rp.add(col1_offset) as *const i64).read_unaligned() };
                    sa == ka && sb == kb
                },
                &mut |sv| {
                    let row = rc.new_row();
                    unsafe {
                        (row.add(col0_offset) as *mut i64).write_unaligned(ka);
                        (row.add(col1_offset) as *mut i64).write_unaligned(kb);
                        (row.add(agg_offset) as *mut i64).write_unaligned(val);
                    }
                    sv.set_ptr(row as *const u8);
                },
                &mut |sv| {
                    let rp = sv.get_ptr() as *mut u8;
                    unsafe { *(rp.add(agg_offset) as *mut i64) += val; }
                },
            );
        }
    }

    // Misses: insert new groups
    if !miss_indices.is_empty() {
        let miss_hashes: Vec<u64> = miss_indices.iter().map(|&i| probe_hashes[i]).collect();
        map.emplace_batch(
            &miss_hashes,
            &mut |batch_idx, sv: &mut taper_hashmap::chunk::SlotValue| {
                let row_idx = miss_indices[batch_idx];
                let row = rc.new_row();
                unsafe {
                    (row.add(col0_offset) as *mut i64).write_unaligned(probe_a[row_idx]);
                    (row.add(col1_offset) as *mut i64).write_unaligned(probe_b[row_idx]);
                    (row.add(agg_offset) as *mut i64).write_unaligned(probe_values[row_idx]);
                }
                sv.set_ptr(row as *const u8);
            },
            &mut |_batch_idx, _sv| {},
        );
    }

    black_box(rc.num_rows());
}

// ═══════════════════════════════════════════════════════════════════
// Benchmark: HT Size × Load Factor × Selectivity
// ═══════════════════════════════════════════════════════════════════

fn bench_build_probe(c: &mut Criterion) {
    let mut group = c.benchmark_group("ht_probe");
    group.sample_size(30);
    let num_probe_rows = 1_000_000;

    for &ht_size in &[256, 1024, 4096, 16384] {
        for &load_factor in &[0.5, 0.75] {
            let num_keys = (ht_size as f64 * load_factor) as usize;

            for &selectivity in &[0.1, 0.3, 0.5, 0.7, 0.9] {
                let mut rng = StdRng::seed_from_u64(42);

                // Build keys
                let build_a: Vec<i64> = (0..num_keys).map(|i| i as i64 * 97 + 1).collect();
                let build_b: Vec<i64> = (0..num_keys).map(|i| i as i64 * 53 + 7).collect();
                let build_hashes: Vec<u64> = (0..num_keys).map(|i| hash2(build_a[i] as u64, build_b[i] as u64)).collect();
                let build_values: Vec<i64> = (0..num_keys).map(|i| (i % 1000) as i64).collect();

                // Probe keys
                let num_hits = (num_probe_rows as f64 * selectivity) as usize;
                let num_misses = num_probe_rows - num_hits;

                let mut probe_a: Vec<i64> = Vec::with_capacity(num_probe_rows);
                let mut probe_b: Vec<i64> = Vec::with_capacity(num_probe_rows);
                let mut probe_hashes: Vec<u64> = Vec::with_capacity(num_probe_rows);

                // Hits: random from build keys
                for _ in 0..num_hits {
                    let idx = rng.random_range(0..num_keys);
                    probe_a.push(build_a[idx]);
                    probe_b.push(build_b[idx]);
                    probe_hashes.push(build_hashes[idx]);
                }

                // Misses: guaranteed not in build keys
                let miss_base = (num_keys as i64 + 1) * 97 + 10000;
                for i in 0..num_misses {
                    let a = miss_base + i as i64 * 31;
                    let b = miss_base + i as i64 * 17 + 3;
                    probe_a.push(a);
                    probe_b.push(b);
                    probe_hashes.push(hash2(a as u64, b as u64));
                }

                // Shuffle to interleave hits and misses
                let mut order: Vec<usize> = (0..num_probe_rows).collect();
                for i in (1..num_probe_rows).rev() {
                    order.swap(i, rng.random_range(0..=i));
                }
                let probe_a: Vec<i64> = order.iter().map(|&i| probe_a[i]).collect();
                let probe_b: Vec<i64> = order.iter().map(|&i| probe_b[i]).collect();
                let probe_hashes: Vec<u64> = order.iter().map(|&i| probe_hashes[i]).collect();
                let probe_values: Vec<i64> = (0..num_probe_rows).map(|i| (i % 1000) as i64).collect();

                let param = format!("ht={}_lf={:.2}_sel={:.1}", ht_size, load_factor, selectivity);

                // ─── Daft ───
                group.bench_with_input(
                    BenchmarkId::new("daft", &param),
                    &(&build_a, &build_b, &build_hashes, &build_values, &probe_a, &probe_b, &probe_hashes, &probe_values),
                    |b, &(ba, bb, bh, bv, pa, pb, ph, pv)| {
                        b.iter(|| {
                            let mut table = HashMap::<IndexHash, u32, IdentityBuildHasher>::with_capacity_and_hasher(ht_size, Default::default());
                            let mut ngroups: u32 = 0;
                            let mut sums = Vec::<i64>::with_capacity(num_keys);

                            // Build
                            for (i, &h) in bh.iter().enumerate() {
                                let entry = table.raw_entry_mut().from_hash(h, |o| o.hash == h && ba[i] == ba[o.idx as usize] && bb[i] == bb[o.idx as usize]);
                                if let RawEntryMut::Vacant(e) = entry {
                                    e.insert_hashed_nocheck(h, IndexHash { idx: i as u64, hash: h }, ngroups);
                                    ngroups += 1;
                                    sums.push(bv[i]);
                                }
                            }

                            // Probe
                            for (i, &h) in ph.iter().enumerate() {
                                let entry = table.raw_entry_mut().from_hash(h, |o| o.hash == h && pa[i] == ba[o.idx as usize] && pb[i] == bb[o.idx as usize]);
                                match entry {
                                    RawEntryMut::Occupied(e) => { sums[*e.get() as usize] += pv[i]; }
                                    RawEntryMut::Vacant(e) => {
                                        e.insert_hashed_nocheck(h, IndexHash { idx: i as u64, hash: h }, ngroups);
                                        ngroups += 1;
                                        sums.push(pv[i]);
                                    }
                                }
                            }
                            black_box(&sums);
                        });
                    },
                );

                // ─── Taper ───
                group.bench_with_input(
                    BenchmarkId::new("taper", &param),
                    &(&build_a, &build_b, &build_hashes, &build_values, &probe_a, &probe_b, &probe_hashes, &probe_values),
                    |b, &(ba, bb, bh, bv, pa, pb, ph, pv)| {
                        b.iter(|| {
                            run_taper_build_probe(
                                black_box(ba), black_box(bb), black_box(bh), black_box(bv),
                                black_box(pa), black_box(pb), black_box(ph), black_box(pv),
                                ht_size, num_keys, num_misses,
                            );
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_build_probe);
criterion_main!(benches);
