use crate::chunk::{Chunk, SlotValue};

const LOAD_FACTOR_THRESHOLD: f64 = 0.9;
const SLOTS_PER_CHUNK: usize = 8;

/// TaperHashMap: chunked open-addressing hash table.
/// Key = u64 (hash value), Value = 6-byte compressed pointer.
pub struct TaperHashMap {
    chunks: Vec<Chunk>,
    size: usize,
    mask: usize,  // chunks.len() - 1 (power of 2)
}

impl TaperHashMap {
    /// Create with initial capacity (power of 2 chunks).
    pub fn new() -> Self {
        Self::with_capacity(128)
    }

    /// Create with specified minimum capacity.
    pub fn with_capacity(min_capacity: usize) -> Self {
        let slots_needed = min_capacity.max(SLOTS_PER_CHUNK);
        let chunks_needed = ((slots_needed + SLOTS_PER_CHUNK - 1) / SLOTS_PER_CHUNK).next_power_of_two();
        let chunks: Vec<Chunk> = (0..chunks_needed).map(|_| Chunk::new()).collect();
        TaperHashMap {
            mask: chunks.len() - 1,
            chunks,
            size: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn capacity(&self) -> usize {
        self.chunks.len() * SLOTS_PER_CHUNK
    }

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

    /// Batch emplace: for each hash, find or create a slot.
    /// on_new(row_idx, &mut SlotValue): called when empty slot found (new group).
    /// on_existing(row_idx, &SlotValue): called when hash matches existing slot.
    /// Returns indices that matched existing slots (update_list for Stage 2 verify).
    pub fn emplace_batch(
        &mut self,
        hashes: &[u64],
        on_new: &mut dyn FnMut(usize, &mut SlotValue),
        on_existing: &mut dyn FnMut(usize, &SlotValue),
    ) -> Vec<usize> {
        let mut update_indices: Vec<usize> = Vec::new();

        for (row_idx, &hash) in hashes.iter().enumerate() {
            // Check expand before each insert to prevent infinite loop
            if self.should_expand() {
                self.expand();
            }

            let mut pos = self.chunk_pos(hash);
            let mut collision_batch = 1usize;

            loop {
                let chunk = &mut self.chunks[pos];
                let tag_hash = ((hash >> 16) & 0x7F) as u8;
                let tags = chunk.tags_u64();

                // Try tag+key match
                let mut found = false;
                for i in crate::bitmask::BitMask::match_tag(tags, tag_hash) {
                    let slot = i as usize;
                    if chunk.keys[slot] == hash {
                        // Hash matches existing → update
                        on_existing(row_idx, &chunk.values[slot]);
                        update_indices.push(row_idx);
                        found = true;
                        break;
                    }
                }
                if found { break; }

                // Try empty slot
                for i in crate::bitmask::BitMask::match_empty(tags) {
                    let slot = i as usize;
                    chunk.tags[slot] = tag_hash;
                    chunk.keys[slot] = hash;
                    on_new(row_idx, &mut chunk.values[slot]);
                    self.size += 1;
                    found = true;
                    break;
                }
                if found { break; }

                // Chunk full → linear probe to next chunk
                pos = self.rehash_pos(collision_batch, pos);
                collision_batch += 1;
            }
        }

        update_indices
    }

    /// Read-only batch probe: for each hash, find matching slot and call on_found.
    /// Does NOT insert. Does NOT modify the table.
    pub fn probe_batch(
        &self,
        hashes: &[u64],
        on_found: &mut dyn FnMut(usize, &SlotValue),
        on_not_found: &mut dyn FnMut(usize),
    ) {
        for (row_idx, &hash) in hashes.iter().enumerate() {
            let mut pos = self.chunk_pos(hash);
            let tag_hash = ((hash >> 16) & 0x7F) as u8;
            let mut found = false;

            loop {
                let chunk = &self.chunks[pos];
                let tags = chunk.tags_u64();

                for i in crate::bitmask::BitMask::match_tag(tags, tag_hash) {
                    let slot = i as usize;
                    if chunk.keys[slot] == hash {
                        on_found(row_idx, &chunk.values[slot]);
                        found = true;
                        break;
                    }
                }
                if found { break; }

                // Check for empty → key not present
                if crate::bitmask::BitMask::match_empty(tags).any() {
                    on_not_found(row_idx);
                    break;
                }

                pos = (pos + 1) & self.mask;
            }
        }
    }

    /// Single-row emplace with full key comparison (for collision repair).
    pub fn emplace(
        &mut self,
        hash: u64,
        key_cmp: &dyn Fn(&SlotValue) -> bool,
        on_new: &mut dyn FnMut(&mut SlotValue),
        on_match: &mut dyn FnMut(&SlotValue),
    ) {
        let mut pos = self.chunk_pos(hash);
        let mut collision_batch = 1usize;

        loop {
            let chunk = &mut self.chunks[pos];
            let tag_hash = ((hash >> 16) & 0x7F) as u8;
            let tags = chunk.tags_u64();

            // Check tag matches
            for i in crate::bitmask::BitMask::match_tag(tags, tag_hash) {
                let slot = i as usize;
                if chunk.keys[slot] == hash && key_cmp(&chunk.values[slot]) {
                    on_match(&chunk.values[slot]);
                    return;
                }
            }

            // Empty slot → new
            for i in crate::bitmask::BitMask::match_empty(tags) {
                let slot = i as usize;
                chunk.tags[slot] = tag_hash;
                chunk.keys[slot] = hash;
                on_new(&mut chunk.values[slot]);
                self.size += 1;
                return;
            }

            pos = self.rehash_pos(collision_batch, pos);
            collision_batch += 1;
        }
    }

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
            for slot in 0..SLOTS_PER_CHUNK {
                if chunk.tags[slot] != 0x80 {
                    let hash = chunk.keys[slot];
                    let value = chunk.values[slot];
                    self.emplace(
                        hash,
                        &|_| false,  // never matches existing (insert-only rehash)
                        &mut |sv| { *sv = value; },
                        &mut |_| {},
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
    fn test_basic_insert_and_find() {
        let mut map = TaperHashMap::new();
        let hashes = vec![42u64, 77, 42, 42, 77, 63];

        let mut new_count = 0;
        let update_list = map.emplace_batch(
            &hashes,
            &mut |_row, _slot| { new_count += 1; },
            &mut |_row, _slot| {},
        );

        assert_eq!(new_count, 3);  // 42, 77, 63 each create one group
        assert_eq!(update_list.len(), 3);  // rows 2,3,4 match existing
        assert_eq!(map.len(), 3);
    }

    #[test]
    fn test_collision_repair() {
        let mut map = TaperHashMap::new();

        // Insert hash=42 as first group
        map.emplace_batch(
            &[42],
            &mut |_, slot| { slot.bytes = [0xAA; 6]; },
            &mut |_, _| {},
        );

        // Now do single emplace with full key compare that rejects existing
        let mut created_new = false;
        map.emplace(
            42,
            &|slot| slot.bytes == [0xAA; 6],  // this would match, but let's say key is different
            &mut |slot| { slot.bytes = [0xBB; 6]; created_new = true; },
            &mut |_| {},
        );
        // First slot matches key_cmp, so on_match is called, not on_new
        assert!(!created_new);
    }
}
