use crate::bitmask::BitMask;
use crate::chunk::{Chunk, SlotValue};

const LOAD_FACTOR_THRESHOLD: f64 = 0.9;
const SLOTS_PER_CHUNK: usize = 8;

/// TaperHashMap: chunked open-addressing hash table.
/// Key = u64 (hash value), Value = 6-byte compressed pointer.
///
/// Design mirrors C++ TaperHashTable's single-row emplace pattern:
/// - `emplace` processes ONE row (matching C++ TryEmplaceAtPos + EmplaceImpl loop)
/// - `emplace_batch` is a thin wrapper looping over rows
/// - `probe` / `probe_batch` for read-only lookups
pub struct TaperHashMap {
    chunks: Vec<Chunk>,
    size: usize,
    mask: usize, // chunks.len() - 1 (power of 2)
}

impl TaperHashMap {
    /// Create with default capacity (128 slots = 16 chunks).
    pub fn new() -> Self {
        Self::with_capacity(16)
    }

    /// Create with specified number of chunks (must be power of 2).
    /// This is the primary constructor for benchmark use.
    pub fn with_capacity(num_chunks: usize) -> Self {
        let num_chunks = num_chunks.max(1).next_power_of_two();
        let chunks: Vec<Chunk> = (0..num_chunks).map(|_| Chunk::new()).collect();
        TaperHashMap {
            mask: chunks.len() - 1,
            chunks,
            size: 0,
        }
    }

    /// Create with specified minimum number of slots.
    pub fn with_slot_capacity(min_slots: usize) -> Self {
        let slots_needed = min_slots.max(SLOTS_PER_CHUNK);
        let chunks_needed =
            ((slots_needed + SLOTS_PER_CHUNK - 1) / SLOTS_PER_CHUNK).next_power_of_two();
        Self::with_capacity(chunks_needed)
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn capacity(&self) -> usize {
        self.chunks.len() * SLOTS_PER_CHUNK
    }

    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    #[inline(always)]
    fn should_expand(&self) -> bool {
        self.size as f64 >= self.capacity() as f64 * LOAD_FACTOR_THRESHOLD
    }

    #[inline(always)]
    fn chunk_pos(&self, hash: u64) -> usize {
        (hash as usize) & self.mask
    }

    #[inline(always)]
    fn rehash_pos(&self, collision_batch: usize, pos: usize) -> usize {
        (pos + collision_batch) & self.mask
    }

    // ─── Single-row emplace (matches C++ TryEmplaceAtPos + EmplaceImpl loop) ───

    /// Process ONE row: find existing slot or create new one.
    ///
    /// Mirrors C++ `EmplaceImpl` + `TryEmplaceAtPos`:
    /// 1. Tag match → key compare → `on_update(slot, false)` if match found
    /// 2. Empty slot → write tag + key → `on_init(slot)` → `on_update(slot, true)`
    /// 3. Chunk full → linear probe next chunk
    ///
    /// This is the ONLY public insert/probe API. All batch methods delegate here.
    #[inline]
    pub fn emplace<FKeyCmp, FInit, FUpdate>(
        &mut self,
        hash: u64,
        key_cmp: &FKeyCmp,
        on_init: &mut FInit,
        on_update: &mut FUpdate,
    ) where
        FKeyCmp: Fn(&SlotValue) -> bool,
        FInit: FnMut(&mut SlotValue),
        FUpdate: FnMut(&SlotValue, bool), // (slot_value, is_new)
    {
        if self.should_expand() {
            self.expand();
        }

        let tag_hash = ((hash >> 16) & 0x7F) as u8;
        let mut pos = self.chunk_pos(hash);
        let mut collision_batch = 1usize;

        loop {
            let chunk = &mut self.chunks[pos];
            let tags = chunk.tags_u64();

            // Try tag+key match (probe existing)
            for i in BitMask::match_tag(tags, tag_hash) {
                let slot = i as usize;
                if chunk.keys[slot] == hash && key_cmp(&chunk.values[slot]) {
                    on_update(&chunk.values[slot], false);
                    return;
                }
            }

            // Try empty slot (build new)
            if let Some(i) = BitMask::match_empty(tags).next() {
                let slot = i as usize;
                chunk.tags[slot] = tag_hash;
                chunk.keys[slot] = hash;
                on_init(&mut chunk.values[slot]);
                on_update(&chunk.values[slot], true);
                self.size += 1;
                return;
            }

            // Chunk full → linear probe
            pos = self.rehash_pos(collision_batch, pos);
            collision_batch += 1;
        }
    }

    // ─── Batch emplace (thin wrapper) ───────────────────────────────────────────

    /// Batch emplace: loops over hashes calling `emplace` for each row.
    ///
    /// Callbacks receive `(row_idx, slot_value, is_new)`:
    /// - `on_init(row_idx, &mut SlotValue)`: called for new slots
    /// - `on_update(row_idx, &SlotValue, is_new)`: called for every row
    pub fn emplace_batch<FKeyCmp, FInit, FUpdate>(
        &mut self,
        hashes: &[u64],
        key_cmp: FKeyCmp,
        mut on_init: FInit,
        mut on_update: FUpdate,
    ) where
        FKeyCmp: Fn(usize, &SlotValue) -> bool,
        FInit: FnMut(usize, &mut SlotValue),
        FUpdate: FnMut(usize, &SlotValue, bool),
    {
        for (i, &h) in hashes.iter().enumerate() {
            let mut row_init = |slot: &mut SlotValue| on_init(i, slot);
            let mut row_update = |slot: &SlotValue, is_new: bool| on_update(i, slot, is_new);
            self.emplace(
                h,
                &|slot| key_cmp(i, slot),
                &mut row_init,
                &mut row_update,
            );
        }
    }

    // ─── Expand (rehash) ────────────────────────────────────────────────────────

    /// Expand capacity by 2x and rehash all elements.
    fn expand(&mut self) {
        let new_len = self.chunks.len() * 2;
        let old_chunks = std::mem::replace(
            &mut self.chunks,
            (0..new_len).map(|_| Chunk::new()).collect(),
        );
        self.mask = self.chunks.len() - 1;
        self.size = 0;

        for chunk in &old_chunks {
            for slot_idx in 0..SLOTS_PER_CHUNK {
                if chunk.tags[slot_idx] != 0x80 {
                    let hash = chunk.keys[slot_idx];
                    let value = chunk.values[slot_idx];
                    // During rehash: never matches existing (insert-only)
                    self.emplace(
                        hash,
                        &|_| false,
                        &mut |sv| {
                            *sv = value;
                        },
                        &mut |_, _| {},
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_emplace_new() {
        let mut map = TaperHashMap::new();
        let mut init_called = false;
        let mut update_is_new = None;

        map.emplace(
            42,
            &|_| false, // no existing match
            &mut |_slot| {
                init_called = true;
            },
            &mut |_slot, is_new| {
                update_is_new = Some(is_new);
            },
        );

        assert!(init_called);
        assert_eq!(update_is_new, Some(true));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_single_emplace_existing() {
        let mut map = TaperHashMap::new();

        // Insert first
        map.emplace(42, &|_| false, &mut |slot| slot.bytes = [0xAA; 6], &mut |_, _| {});

        // Now emplace again with same hash — key_cmp matches
        let mut update_is_new = None;
        map.emplace(
            42,
            &|slot: &SlotValue| slot.bytes == [0xAA; 6], // matches existing
            &mut |_slot| panic!("should not init"),
            &mut |_slot, is_new| {
                update_is_new = Some(is_new);
            },
        );

        assert_eq!(update_is_new, Some(false));
        assert_eq!(map.len(), 1); // no new entry
    }

    #[test]
    fn test_single_emplace_hash_collision_different_key() {
        let mut map = TaperHashMap::new();

        // Insert first entry with hash=42
        map.emplace(42, &|_| false, &mut |slot| slot.bytes = [0xAA; 6], &mut |_, _| {});

        // Emplace with same hash but key_cmp rejects → creates new entry
        let mut init_called = false;
        map.emplace(
            42,
            &|slot: &SlotValue| slot.bytes == [0xBB; 6], // won't match [0xAA]
            &mut |slot| {
                slot.bytes = [0xBB; 6];
                init_called = true;
            },
            &mut |_, _| {},
        );

        assert!(init_called);
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_batch_emplace() {
        let mut map = TaperHashMap::new();
        let hashes = vec![42u64, 77, 42, 42, 77, 63];

        let mut new_count = 0;
        let mut existing_count = 0;

        // key_cmp: hash-only comparison (always true if tag+hash matched)
        map.emplace_batch(
            &hashes,
            |_row, _slot| true,
            |_row, _slot| {
                new_count += 1;
            },
            |_row, _slot, is_new| {
                if !is_new {
                    existing_count += 1;
                }
            },
        );

        assert_eq!(new_count, 3); // 42, 77, 63 each create one group
        assert_eq!(existing_count, 3); // rows 2,3,4 match existing
        assert_eq!(map.len(), 3);
    }

    #[test]
    fn test_expand_preserves_entries() {
        // Use a very small map that will trigger expansion
        let mut map = TaperHashMap::with_capacity(2); // 2 chunks = 16 slots
        let total = 14; // will exceed 90% of 16 slots

        for i in 0..total {
            map.emplace(
                i as u64 * 1000003, // spread out hashes
                &|_| false,
                &mut |slot| slot.bytes[0] = i as u8,
                &mut |_, _| {},
            );
        }

        assert_eq!(map.len(), total);

        // Verify all entries are still findable after expansion(s)
        for i in 0..total {
            let mut found = false;
            map.emplace(
                i as u64 * 1000003,
                &|slot: &SlotValue| slot.bytes[0] == i as u8,
                &mut |_| panic!("should not create new entry for {}", i),
                &mut |_, is_new| {
                    assert!(!is_new);
                    found = true;
                },
            );
            assert!(
                found,
                "Entry {} not found after expansion",
                i
            );
        }
    }

    #[test]
    fn test_with_capacity() {
        let map = TaperHashMap::with_capacity(64);
        assert_eq!(map.num_chunks(), 64);
        assert_eq!(map.capacity(), 64 * SLOTS_PER_CHUNK);
    }

    #[test]
    fn test_with_slot_capacity() {
        let map = TaperHashMap::with_slot_capacity(100);
        // 100 slots / 8 = 12.5 → next_power_of_two = 16 chunks
        assert_eq!(map.num_chunks(), 16);
        assert_eq!(map.capacity(), 128);
    }
}
