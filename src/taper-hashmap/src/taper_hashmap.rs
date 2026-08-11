use crate::bitmask::BitMask;
use crate::chunk::{Chunk, EmplaceResult, CHUNK_SLOTS};
use crate::slot_value::SlotValue;

/// TaperHashMap: Taper 风格的 hash aggregation 专用 hash table。
///
/// 设计要点:
/// - Key 类型固定为 u64 (hash 值), KeyScattered=true (传入的就是 hash, 不再 rehash)
/// - 每个 chunk 8 slot, 128B 对齐
/// - 三层过滤: tag (SWAR) → hash (int64 ==) → full key compare (延迟/外部)
/// - 接口: emplace_batch (批量) / emplace (单行精确)
///
/// 冲突解决: 线性步进到下一个 chunk (不是下一个 slot)
/// 扩容: load factor > 0.9 时 rehash 到 2× 容量
const MAX_LOAD_FACTOR: f64 = 0.9;

pub struct TaperHashMap {
    chunks: Vec<Chunk>,
    mask: usize, // chunks.len() - 1 (power of 2)
    size: usize, // 已占用 slot 数量
}

impl TaperHashMap {
    /// 创建新的 TaperHashMap
    ///
    /// `initial_capacity` 会被向上取整到 8 的倍数 (chunk 对齐),
    /// 然后 chunk 数量取到下一个 2 的幂。
    pub fn new(initial_capacity: usize) -> Self {
        let num_chunks = Self::capacity_to_num_chunks(initial_capacity).max(4);
        let chunks = (0..num_chunks).map(|_| Chunk::new()).collect();
        Self {
            chunks,
            mask: num_chunks - 1,
            size: 0,
        }
    }

    /// 已占用 slot 数量
    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    /// 是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// 总容量 (slot 数)
    #[inline]
    pub fn capacity(&self) -> usize {
        self.chunks.len() * CHUNK_SLOTS
    }

    /// 批量 emplace: 只做 tag + hash(int64) 比较, 不做 full key 比较。
    ///
    /// 对每行:
    ///   - 如果是新 group → 调 `on_new(row_idx, slot_value_ref)`, 让调用者写 RowContainer 指针
    ///   - 如果 tag+hash 匹配已有 slot → 调 `on_existing(row_idx, slot_value_ref)`, 返回行号加入 update list
    ///
    /// 返回 update_list: 需要 Stage 2 验证的 (row_idx, slot_value) 对。
    /// 调用者拿到后做 full key compare, 确认或修复。
    pub fn emplace_batch(
        &mut self,
        hashes: &[u64],
        on_new: &mut dyn FnMut(usize, &mut SlotValue),
        on_existing: &mut dyn FnMut(usize, &SlotValue),
    ) -> Vec<usize> {
        // 预检查是否需要扩容
        let needed = self.size + hashes.len();
        if needed as f64 > self.capacity() as f64 * MAX_LOAD_FACTOR {
            self.expand_to_fit(needed);
        }

        let mut update_list: Vec<usize> = Vec::new();

        for (row_idx, &hash) in hashes.iter().enumerate() {
            let tag = Self::extract_tag(hash);
            let mut chunk_idx = self.chunk_pos(hash);

            loop {
                let chunk = &mut self.chunks[chunk_idx];
                match chunk.try_emplace(hash, tag) {
                    EmplaceResult::New(slot_idx) => {
                        let slot = slot_idx as usize;
                        on_new(row_idx, &mut chunk.values[slot]);
                        self.size += 1;
                        break;
                    }
                    EmplaceResult::Existing(slot_idx) => {
                        let slot = slot_idx as usize;
                        on_existing(row_idx, &chunk.values[slot]);
                        update_list.push(row_idx);
                        break;
                    }
                    EmplaceResult::Full => {
                        // 线性步进到下一个 chunk
                        chunk_idx = (chunk_idx + 1) & self.mask;
                    }
                }
            }
        }

        update_list
    }

    /// 单行精确 emplace: 带 full key 比较。
    /// 用于 Stage 2 碰撞修复时 re-emplace。
    ///
    /// - `key_cmp`: 给定 slot 的 SlotValue, 判断 full key 是否相等
    /// - `on_new`: 新 group, 需要存 key 到 RowContainer
    /// - `on_match`: 已有 group, 确认匹配
    pub fn emplace(
        &mut self,
        hash: u64,
        key_cmp: &dyn Fn(&SlotValue) -> bool,
        on_new: &mut dyn FnMut(&mut SlotValue),
        on_match: &mut dyn FnMut(&SlotValue),
    ) {
        if (self.size + 1) as f64 > self.capacity() as f64 * MAX_LOAD_FACTOR {
            self.expand();
        }

        let tag = Self::extract_tag(hash);
        let mut chunk_idx = self.chunk_pos(hash);

        loop {
            let chunk = &mut self.chunks[chunk_idx];
            let tags_u64 = chunk.tags_u64();

            // Stage 1a: tag match
            let tag_matches = BitMask::match_tag(tags_u64, tag);

            for slot_idx in tag_matches {
                let slot = slot_idx as usize;
                // Stage 1b: hash compare
                if chunk.keys[slot] == hash {
                    // Stage 2: full key compare via SlotValue
                    if key_cmp(&chunk.values[slot]) {
                        on_match(&chunk.values[slot]);
                        return;
                    }
                }
            }

            // 没有 full match → 找 empty slot
            let empty_mask = BitMask::match_empty(tags_u64);
            if empty_mask.any() {
                let slot_idx = empty_mask.lowest() as usize;
                chunk.tags[slot_idx] = tag;
                chunk.keys[slot_idx] = hash;
                on_new(&mut chunk.values[slot_idx]);
                self.size += 1;
                return;
            }

            // chunk 满 → 线性步进
            chunk_idx = (chunk_idx + 1) & self.mask;
        }
    }

    // ─── 简单 put/get API (benchmark 用) ────────────────────

    /// 插入 key-value 对。key 是 u64 hash 值，value 是 u64。
    /// 如果 key 已存在，覆盖 value。
    pub fn put(&mut self, key: u64, value: u64) {
        let tag = Self::extract_tag(key);
        let mut chunk_idx = self.chunk_pos(key);

        // 预检查扩容
        if (self.size + 1) as f64 > self.capacity() as f64 * MAX_LOAD_FACTOR {
            self.expand();
            chunk_idx = self.chunk_pos(key);
        }

        loop {
            let chunk = &mut self.chunks[chunk_idx];
            let tags_u64 = chunk.tags_u64();

            // 查找已有 slot
            let tag_matches = BitMask::match_tag(tags_u64, tag);
            for slot_idx in tag_matches {
                let slot = slot_idx as usize;
                if chunk.keys[slot] == key {
                    // 已有 → 覆盖 value
                    let val_bytes = value.to_ne_bytes();
                    chunk.values[slot].bytes[0..6].copy_from_slice(&val_bytes[0..6]);
                    return;
                }
            }

            // 找 empty slot 插入
            let empty_mask = BitMask::match_empty(tags_u64);
            if empty_mask.any() {
                let slot_idx = empty_mask.lowest() as usize;
                chunk.tags[slot_idx] = tag;
                chunk.keys[slot_idx] = key;
                let val_bytes = value.to_ne_bytes();
                chunk.values[slot_idx].bytes[0..6].copy_from_slice(&val_bytes[0..6]);
                self.size += 1;
                return;
            }

            // chunk 满 → 步进
            chunk_idx = (chunk_idx + 1) & self.mask;
        }
    }

    /// 查找 key，返回 value (如果存在)。key 是 u64 hash 值。
    pub fn get(&self, key: u64) -> Option<u64> {
        let tag = Self::extract_tag(key);
        let mut chunk_idx = self.chunk_pos(key);

        loop {
            let chunk = &self.chunks[chunk_idx];
            let tags_u64 = chunk.tags_u64();

            // tag match
            let tag_matches = BitMask::match_tag(tags_u64, tag);
            for slot_idx in tag_matches {
                let slot = slot_idx as usize;
                if chunk.keys[slot] == key {
                    // 从 6-byte SlotValue 还原 u64 (高 2 byte 为 0)
                    let mut val_bytes = [0u8; 8];
                    val_bytes[0..6].copy_from_slice(&chunk.values[slot].bytes);
                    return Some(u64::from_ne_bytes(val_bytes));
                }
            }

            // 有 empty slot → key 不存在
            let empty_mask = BitMask::match_empty(tags_u64);
            if empty_mask.any() {
                return None;
            }

            // chunk 满且没找到 → 继续步进
            chunk_idx = (chunk_idx + 1) & self.mask;
        }
    }

    // ─── 原有 API ────────────────────────────────────────

    /// 迭代所有已占用的 slot
    pub fn iter(&self) -> impl Iterator<Item = (u64, &SlotValue)> {
        self.chunks.iter().flat_map(|chunk| {
            chunk
                .tags
                .iter()
                .enumerate()
                .filter(|(_, &tag)| tag != crate::chunk::TAG_EMPTY)
                .map(move |(i, _)| (chunk.keys[i], &chunk.values[i]))
        })
    }

    // ─── 内部方法 ────────────────────────────────────────

    /// 从 hash 值提取 7-bit tag
    #[inline]
    fn extract_tag(hash: u64) -> u8 {
        ((hash >> 16) & 0x7F) as u8
    }

    /// 从 hash 值计算 chunk 位置
    #[inline]
    fn chunk_pos(&self, hash: u64) -> usize {
        (hash as usize) & self.mask
    }

    /// 计算所需 chunk 数量 (向上取到 2 的幂)
    fn capacity_to_num_chunks(capacity: usize) -> usize {
        let slots_needed = capacity.max(CHUNK_SLOTS);
        let chunks_needed = (slots_needed + CHUNK_SLOTS - 1) / CHUNK_SLOTS;
        chunks_needed.next_power_of_two()
    }

    /// 扩容到至少能容纳 `needed` 个 slot
    fn expand_to_fit(&mut self, needed: usize) {
        let target_capacity = ((needed as f64 / MAX_LOAD_FACTOR) as usize).max(self.capacity() * 2);
        self.rehash(target_capacity);
    }

    /// 2× 扩容
    fn expand(&mut self) {
        self.rehash(self.capacity() * 2);
    }

    /// Rehash: 分配新 chunk 数组, 重新插入所有已有 entry
    fn rehash(&mut self, new_capacity: usize) {
        let new_num_chunks = Self::capacity_to_num_chunks(new_capacity);
        let new_mask = new_num_chunks - 1;
        let mut new_chunks: Vec<Chunk> = (0..new_num_chunks).map(|_| Chunk::new()).collect();

        // 遍历旧 table, 把所有已占用 slot 重新插入
        for old_chunk in &self.chunks {
            for i in 0..CHUNK_SLOTS {
                if old_chunk.tags[i] == crate::chunk::TAG_EMPTY {
                    continue;
                }
                let hash = old_chunk.keys[i];
                let tag = Self::extract_tag(hash);
                let value = old_chunk.values[i];

                let mut chunk_idx = (hash as usize) & new_mask;
                loop {
                    let chunk = &mut new_chunks[chunk_idx];
                    match chunk.try_emplace(hash, tag) {
                        EmplaceResult::New(slot_idx) => {
                            chunk.values[slot_idx as usize] = value;
                            break;
                        }
                        EmplaceResult::Existing(_) => {
                            // 不应该发生: rehash 时每个 entry 的 hash 应该唯一
                            // (同一个 group 不会出现两次)
                            unreachable!("duplicate entry during rehash");
                        }
                        EmplaceResult::Full => {
                            chunk_idx = (chunk_idx + 1) & new_mask;
                        }
                    }
                }
            }
        }

        self.chunks = new_chunks;
        self.mask = new_mask;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let map = TaperHashMap::new(64);
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
        assert!(map.capacity() >= 64);
    }

    #[test]
    fn test_emplace_batch_basic() {
        let mut map = TaperHashMap::new(64);
        let hashes = vec![0x1111u64, 0x2222, 0x1111, 0x3333, 0x2222];

        let mut new_count = 0;
        let mut existing_count = 0;

        let update_list = map.emplace_batch(
            &hashes,
            &mut |_row_idx, _sv| {
                new_count += 1;
            },
            &mut |_row_idx, _sv| {
                existing_count += 1;
            },
        );

        // 3 unique hashes → 3 new, 2 existing
        assert_eq!(new_count, 3);
        assert_eq!(existing_count, 2);
        assert_eq!(map.len(), 3);
        assert_eq!(update_list.len(), 2); // row 2 and row 4
    }

    #[test]
    fn test_emplace_batch_all_unique() {
        let mut map = TaperHashMap::new(64);
        let hashes: Vec<u64> = (0..20).map(|i| i * 0x10000_0001).collect();

        let mut new_count = 0;
        let update_list = map.emplace_batch(
            &hashes,
            &mut |_, _| new_count += 1,
            &mut |_, _| {},
        );

        assert_eq!(new_count, 20);
        assert_eq!(map.len(), 20);
        assert!(update_list.is_empty());
    }

    #[test]
    fn test_emplace_batch_triggers_expand() {
        let mut map = TaperHashMap::new(8); // 很小的初始容量
        let hashes: Vec<u64> = (0..100).map(|i| i * 0x10000_0001).collect();

        let mut new_count = 0;
        map.emplace_batch(&hashes, &mut |_, _| new_count += 1, &mut |_, _| {});

        assert_eq!(new_count, 100);
        assert_eq!(map.len(), 100);
        assert!(map.capacity() >= 100);
    }

    #[test]
    fn test_emplace_single_new() {
        let mut map = TaperHashMap::new(64);
        let hash = 0xABCD_EF01_2345_6789u64;

        let mut was_new = false;
        map.emplace(
            hash,
            &|_sv| false, // key_cmp: 不匹配任何已有 (因为 table 空)
            &mut |_sv| was_new = true,
            &mut |_sv| {},
        );

        assert!(was_new);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_emplace_single_match() {
        let mut map = TaperHashMap::new(64);
        let hash = 0xABCD_EF01_2345_6789u64;

        // 先通过 emplace_batch 插入
        map.emplace_batch(&[hash], &mut |_, _| {}, &mut |_, _| {});
        assert_eq!(map.len(), 1);

        // 再用 emplace 精确查找
        let mut was_match = false;
        map.emplace(
            hash,
            &|_sv| true, // key_cmp: 匹配
            &mut |_sv| {},
            &mut |_sv| was_match = true,
        );

        assert!(was_match);
        assert_eq!(map.len(), 1); // 没有新增
    }

    #[test]
    fn test_iter() {
        let mut map = TaperHashMap::new(64);
        let hashes = vec![0x1111u64, 0x2222, 0x3333];
        map.emplace_batch(&hashes, &mut |_, _| {}, &mut |_, _| {});

        let collected: Vec<u64> = map.iter().map(|(h, _)| h).collect();
        assert_eq!(collected.len(), 3);
        for h in &hashes {
            assert!(collected.contains(h));
        }
    }

    #[test]
    fn test_high_load_factor() {
        let mut map = TaperHashMap::new(16);
        // 插入很多不同的 hash, 测试扩容和线性探测
        let hashes: Vec<u64> = (0..500).map(|i| i * 7919 + 42).collect();

        map.emplace_batch(&hashes, &mut |_, _| {}, &mut |_, _| {});
        assert_eq!(map.len(), 500);

        // 再次插入相同 hash, 全部应该是 existing
        let mut existing_count = 0;
        let update_list = map.emplace_batch(
            &hashes,
            &mut |_, _| {},
            &mut |_, _| existing_count += 1,
        );

        assert_eq!(existing_count, 500);
        assert_eq!(update_list.len(), 500);
        assert_eq!(map.len(), 500); // 没有新增
    }

    // ─── put/get API tests ───

    #[test]
    fn test_put_get_basic() {
        let mut map = TaperHashMap::new(64);
        map.put(42, 100);
        map.put(99, 200);

        assert_eq!(map.get(42), Some(100));
        assert_eq!(map.get(99), Some(200));
        assert_eq!(map.get(1), None);
    }

    #[test]
    fn test_put_overwrite() {
        let mut map = TaperHashMap::new(64);
        map.put(42, 100);
        assert_eq!(map.get(42), Some(100));

        map.put(42, 999);
        assert_eq!(map.get(42), Some(999));
        assert_eq!(map.len(), 1); // 没有新增
    }

    #[test]
    fn test_put_get_many() {
        let mut map = TaperHashMap::new(16);
        for i in 0..1000u64 {
            map.put(i * 7919 + 1, i);
        }
        assert_eq!(map.len(), 1000);

        for i in 0..1000u64 {
            assert_eq!(map.get(i * 7919 + 1), Some(i));
        }
        assert_eq!(map.get(0xDEAD), None);
    }

    #[test]
    fn test_put_get_max_value() {
        let mut map = TaperHashMap::new(64);
        // SlotValue 只有 6 字节, 所以 value 最大 48-bit
        let max_48bit: u64 = (1u64 << 48) - 1;
        map.put(1, max_48bit);
        assert_eq!(map.get(1), Some(max_48bit));
    }
}
