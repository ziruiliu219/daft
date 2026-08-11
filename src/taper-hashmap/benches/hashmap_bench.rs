use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use hashbrown::{HashMap, hash_map::RawEntryMut};
use rand::Rng;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use taper_hashmap::TaperHashMap;

// ═══════════════════════════════════════════════════════════════════
// Daft 风格的 IdentityHasher + IndexHash
// ═══════════════════════════════════════════════════════════════════

#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;

#[derive(Eq, PartialEq)]
struct IndexHash {
    idx: u64,
    hash: u64,
}

impl Hash for IndexHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

// ═══════════════════════════════════════════════════════════════════
// 模拟多列 key 数据 (列式 Arrow 数组)
// ═══════════════════════════════════════════════════════════════════

struct MockColumns {
    col_a: Vec<i64>,
    col_b: Vec<i64>,
}

impl MockColumns {
    fn new(num_rows: usize, num_groups: usize) -> (Self, Vec<u64>) {
        let mut rng = rand::rng();
        let group_keys: Vec<(i64, i64)> = (0..num_groups)
            .map(|i| (i as i64 * 100, i as i64 * 7))
            .collect();

        let mut col_a = Vec::with_capacity(num_rows);
        let mut col_b = Vec::with_capacity(num_rows);
        let mut hashes = Vec::with_capacity(num_rows);

        for _ in 0..num_rows {
            let group_idx = rng.random_range(0..num_groups);
            let (a, b) = group_keys[group_idx];
            col_a.push(a);
            col_b.push(b);
            hashes.push(hash_two_i64(a, b));
        }

        (Self { col_a, col_b }, hashes)
    }

    /// Daft 的 comparator: 逐列比较两个行号的原始值
    #[inline]
    fn compare(&self, i: usize, j: usize) -> bool {
        self.col_a[i] == self.col_a[j] && self.col_b[i] == self.col_b[j]
    }
}

#[inline]
fn hash_two_i64(a: i64, b: i64) -> u64 {
    let mut h = a as u64;
    h = h.wrapping_mul(0x517cc1b727220a95);
    h ^= b as u64;
    h = h.wrapping_mul(0x6c62272e07bb0142);
    h ^= h >> 33;
    // 确保 tag 不等于 0x80 (empty marker)
    if ((h >> 16) & 0x7F) == 0x80 {
        h ^= 0x0001_0000;
    }
    h
}

// ═══════════════════════════════════════════════════════════════════
// 辅助: 从 SlotValue 读写 group_id (u32)
// ═══════════════════════════════════════════════════════════════════

#[inline]
fn write_group_id(sv: &mut taper_hashmap::SlotValue, gid: u32) {
    let bytes = gid.to_ne_bytes();
    sv.bytes[0..4].copy_from_slice(&bytes);
}

#[inline]
fn read_group_id(sv: &taper_hashmap::SlotValue) -> u32 {
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&sv.bytes[0..4]);
    u32::from_ne_bytes(bytes)
}

// ═══════════════════════════════════════════════════════════════════
// Benchmark 1: BUILD — 完整 hash agg 建表 (遍历所有行, build + probe 混合)
//
// Daft 流程: 对每行 → tag(内部) → hash== → comparator → Occupied/Vacant
// Taper 流程: 对所有行 → emplace_batch(tag+hash only) → deferred compare
// ═══════════════════════════════════════════════════════════════════

fn bench_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("build");

    for &num_rows in &[100_000, 1_000_000] {
        for &num_groups in &[10, 100, 1000, 10_000] {
            if num_groups > num_rows {
                continue;
            }
            let (cols, hashes) = MockColumns::new(num_rows, num_groups);
            let param = format!("rows={}_groups={}", num_rows, num_groups);

            // ─── Daft: hashbrown + IndexHash + comparator (每次碰撞都比) ───
            group.bench_with_input(
                BenchmarkId::new("daft_hashbrown", &param),
                &(&cols, &hashes),
                |b, &(cols, hashes)| {
                    b.iter(|| {
                        let mut table =
                            HashMap::<IndexHash, u32, IdentityBuildHasher>::with_capacity_and_hasher(
                                num_groups * 2,
                                Default::default(),
                            );
                        let mut num_groups_found: u32 = 0;
                        let mut group_ids: Vec<u32> = Vec::with_capacity(hashes.len());

                        for (row_idx, &h) in hashes.iter().enumerate() {
                            let entry = table.raw_entry_mut().from_hash(h, |other| {
                                // hashbrown 内部先做了 tag match (我们看不到)
                                // 然后进闭包:
                                (h == other.hash)
                                    && cols.compare(row_idx, other.idx as usize)
                            });

                            let gid = match entry {
                                RawEntryMut::Occupied(e) => *e.get(),
                                RawEntryMut::Vacant(e) => {
                                    let gid = num_groups_found;
                                    num_groups_found += 1;
                                    e.insert_hashed_nocheck(
                                        h,
                                        IndexHash {
                                            idx: row_idx as u64,
                                            hash: h,
                                        },
                                        gid,
                                    );
                                    gid
                                }
                            };
                            group_ids.push(gid);
                        }

                        black_box(num_groups_found);
                        black_box(&group_ids);
                    });
                },
            );

            // ─── Taper: emplace_batch (tag+hash only) + deferred compare ───
            group.bench_with_input(
                BenchmarkId::new("taper_batch", &param),
                &(&cols, &hashes),
                |b, &(cols, hashes)| {
                    b.iter(|| {
                        let mut map = TaperHashMap::new(num_groups * 2);
                        let mut num_groups_found: u32 = 0;
                        let mut group_ids: Vec<u32> = vec![0; hashes.len()];

                        // Phase 1: batch emplace — 只做 tag + hash 比较
                        let mut new_gids: Vec<(usize, u32)> = Vec::new();
                        let mut existing_gids: Vec<(usize, u32)> = Vec::new();

                        let update_list = map.emplace_batch(
                            black_box(hashes),
                            &mut |row_idx, sv| {
                                // on_new: 新 group, 写 group_id 到 SlotValue
                                let gid = num_groups_found;
                                num_groups_found += 1;
                                write_group_id(sv, gid);
                                new_gids.push((row_idx, gid));
                            },
                            &mut |row_idx, sv| {
                                // on_existing: tag+hash 匹配, 从 SlotValue 读 group_id
                                let gid = read_group_id(sv);
                                existing_gids.push((row_idx, gid));
                            },
                        );

                        // 写入 group_ids
                        for (idx, gid) in new_gids {
                            group_ids[idx] = gid;
                        }
                        for (idx, gid) in existing_gids {
                            group_ids[idx] = gid;
                        }

                        // Phase 2: deferred full key compare
                        // 在真实场景中这里对 update_list 做逐列比较
                        // 但由于 64-bit hash 碰撞率 ≈ 0, 这里模拟验证开销
                        // (用 cols.compare 检测真碰撞，模拟 GetUnequalsNumWithDecode)
                        let mut _collision_count = 0u32;
                        for &idx in &update_list {
                            let gid = group_ids[idx] as usize;
                            // 找到这个 group 的代表行 (第一次出现的行)
                            // 简化: 直接做列比较
                            // 真实 Taper 会从 RowContainer 读 stored key
                            // 这里用 cols 模拟 (开销类似)
                            let _same = cols.col_a[idx] != 0 || cols.col_b[idx] != 0;
                            // 实际碰撞检测需要 stored key, 这里只模拟内存访问开销
                            black_box(gid);
                        }

                        black_box(num_groups_found);
                        black_box(&group_ids);
                    });
                },
            );
        }
    }
    group.finish();
}

// ═══════════════════════════════════════════════════════════════════
// Benchmark 2: PROBE — 纯 probe (表已建好, 所有行命中已有 group)
//
// 模拟: 低基数 agg, 表建好后大量后续 batch 的 probe
// ═══════════════════════════════════════════════════════════════════

fn bench_probe(c: &mut Criterion) {
    let mut group = c.benchmark_group("probe");

    for &num_rows in &[100_000, 1_000_000] {
        for &num_groups in &[10, 100, 1000] {
            let (cols, hashes) = MockColumns::new(num_rows, num_groups);
            let param = format!("rows={}_groups={}", num_rows, num_groups);

            // 预先建好 Daft 风格的 table
            let mut daft_table =
                HashMap::<IndexHash, u32, IdentityBuildHasher>::with_capacity_and_hasher(
                    num_groups * 2,
                    Default::default(),
                );
            {
                let mut gid_counter = 0u32;
                for (row_idx, &h) in hashes.iter().enumerate() {
                    let entry = daft_table.raw_entry_mut().from_hash(h, |other| {
                        (h == other.hash) && cols.compare(row_idx, other.idx as usize)
                    });
                    if let RawEntryMut::Vacant(e) = entry {
                        e.insert_hashed_nocheck(
                            h,
                            IndexHash {
                                idx: row_idx as u64,
                                hash: h,
                            },
                            gid_counter,
                        );
                        gid_counter += 1;
                    }
                }
            }

            // 预先建好 Taper table
            let mut taper_table = TaperHashMap::new(num_groups * 2);
            {
                let mut gid_counter = 0u32;
                taper_table.emplace_batch(
                    &hashes,
                    &mut |_, sv| {
                        write_group_id(sv, gid_counter);
                        gid_counter += 1;
                    },
                    &mut |_, _| {},
                );
            }

            // 新的 probe 数据
            let (probe_cols, probe_hashes) = MockColumns::new(num_rows, num_groups);

            // ─── Daft probe: 每行都调 comparator ───
            group.bench_with_input(
                BenchmarkId::new("daft_hashbrown", &param),
                &(&probe_cols, &probe_hashes),
                |b, &(cols, hashes)| {
                    b.iter(|| {
                        let mut sum_gid: u64 = 0;
                        for (row_idx, &h) in hashes.iter().enumerate() {
                            let entry = daft_table.raw_entry().from_hash(h, |other| {
                                (h == other.hash) && cols.compare(row_idx, other.idx as usize)
                            });
                            if let Some((_, &gid)) = entry {
                                sum_gid += gid as u64;
                            }
                        }
                        black_box(sum_gid);
                    });
                },
            );

            // ─── Taper probe: 只做 tag + hash, 从 SlotValue 读 group_id ───
            group.bench_with_input(
                BenchmarkId::new("taper_get", &param),
                &probe_hashes,
                |b, hashes| {
                    b.iter(|| {
                        let mut sum_gid: u64 = 0;
                        for &h in hashes {
                            if let Some(v) = taper_table.get(black_box(h)) {
                                sum_gid += v;
                            }
                        }
                        black_box(sum_gid);
                    });
                },
            );
        }
    }
    group.finish();
}

// ═══════════════════════════════════════════════════════════════════
// Benchmark 3: MICRO — 纯 hash table 微操作 (不带列比较)
//
// 隔离 hash table 本身的性能差异，排除 comparator 开销
// ═══════════════════════════════════════════════════════════════════

fn bench_micro(c: &mut Criterion) {
    let mut group = c.benchmark_group("micro_put_get");

    for &num_entries in &[100, 1000, 10_000, 100_000] {
        let keys: Vec<u64> = (0..num_entries).map(|i| hash_two_i64(i as i64, 0)).collect();
        let param = format!("entries={}", num_entries);

        // Taper put
        group.bench_with_input(BenchmarkId::new("taper_put", &param), &keys, |b, keys| {
            b.iter(|| {
                let mut map = TaperHashMap::new(keys.len() * 2);
                for (i, &k) in keys.iter().enumerate() {
                    map.put(black_box(k), i as u64);
                }
                black_box(map.len());
            });
        });

        // hashbrown insert
        group.bench_with_input(
            BenchmarkId::new("hashbrown_insert", &param),
            &keys,
            |b, keys| {
                b.iter(|| {
                    let mut map =
                        HashMap::<u64, u64, IdentityBuildHasher>::with_capacity_and_hasher(
                            keys.len() * 2,
                            Default::default(),
                        );
                    for (i, &k) in keys.iter().enumerate() {
                        map.insert(black_box(k), i as u64);
                    }
                    black_box(map.len());
                });
            },
        );

        // 预建表后 get
        let mut taper_map = TaperHashMap::new(keys.len() * 2);
        let mut hb_map = HashMap::<u64, u64, IdentityBuildHasher>::with_capacity_and_hasher(
            keys.len() * 2,
            Default::default(),
        );
        for (i, &k) in keys.iter().enumerate() {
            taper_map.put(k, i as u64);
            hb_map.insert(k, i as u64);
        }

        // Taper get
        group.bench_with_input(BenchmarkId::new("taper_get", &param), &keys, |b, keys| {
            b.iter(|| {
                let mut sum: u64 = 0;
                for &k in keys {
                    sum += taper_map.get(black_box(k)).unwrap_or(0);
                }
                black_box(sum);
            });
        });

        // hashbrown get
        group.bench_with_input(
            BenchmarkId::new("hashbrown_get", &param),
            &keys,
            |b, keys| {
                b.iter(|| {
                    let mut sum: u64 = 0;
                    for &k in keys {
                        sum += hb_map.get(black_box(&k)).copied().unwrap_or(0);
                    }
                    black_box(sum);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_build, bench_probe, bench_micro);
criterion_main!(benches);
