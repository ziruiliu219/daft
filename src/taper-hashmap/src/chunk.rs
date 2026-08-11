use crate::bitmask::BitMask;
use crate::slot_value::SlotValue;

/// Chunk: 128 字节对齐的存储单元，容纳 8 个 slot。
///
/// 内存布局:
///   byte  0-7:   tags[8]    (1B × 8) — 7-bit fingerprint, 0x80 = empty
///   byte  8-71:  keys[8]    (8B × 8) — hash value (u64)
///   byte 72-119: values[8]  (6B × 8) — 压缩指针 → RowContainer row
///   byte 120-127: padding
///
/// Total: 128 bytes = 2 cache lines (或 1 on 128B line systems)
pub const CHUNK_SLOTS: usize = 8;
pub const TAG_EMPTY: u8 = 0x80;

#[repr(C, align(128))]
pub struct Chunk {
    pub tags: [u8; CHUNK_SLOTS],
    pub keys: [u64; CHUNK_SLOTS],
    pub values: [SlotValue; CHUNK_SLOTS],
    _padding: [u8; 8],
}

impl Chunk {
    /// 创建全空 chunk
    pub fn new() -> Self {
        Self {
            tags: [TAG_EMPTY; CHUNK_SLOTS],
            keys: [0u64; CHUNK_SLOTS],
            values: [SlotValue::EMPTY; CHUNK_SLOTS],
            _padding: [0; 8],
        }
    }

    /// 将 8 个 tag 读为一个 u64 (用于 BitMask SWAR 比较)
    #[inline]
    pub fn tags_u64(&self) -> u64 {
        u64::from_ne_bytes(self.tags)
    }

    /// chunk 已占用 slot 数量
    pub fn occupied_count(&self) -> usize {
        self.tags.iter().filter(|&&t| t != TAG_EMPTY).count()
    }

    /// chunk 是否已满
    #[inline]
    pub fn is_full(&self) -> bool {
        // 如果没有任何 empty slot，则满了
        !BitMask::match_empty(self.tags_u64()).any()
    }

    /// 尝试在本 chunk 内 emplace 一行。
    ///
    /// 流程:
    ///   1. SWAR 比较 tag → 找到 tag 匹配的 slot
    ///   2. 对匹配 slot 做 KeyEquals (hash == hash)
    ///   3. 如果 KeyEquals 命中 → 调 on_update(slot_idx), 返回 EmplaceResult::Existing
    ///   4. 如果没有命中 → 找 empty slot → 写入 tag+key, 调 on_new(slot_idx), 返回 EmplaceResult::New
    ///   5. 如果 chunk 满 → 返回 EmplaceResult::Full
    ///
    /// # Arguments
    /// - `hash_val`: 输入行预计算的 hash 值
    /// - `tag`: `(hash_val >> 16) & 0x7F` 预计算的 tag
    ///
    /// # Returns
    /// - `EmplaceResult::New(slot_idx)` — 新 group, 已写入 tag+key
    /// - `EmplaceResult::Existing(slot_idx)` — 已有 group (tag+hash 匹配)
    /// - `EmplaceResult::Full` — chunk 满，需要步进到下一个 chunk
    pub fn try_emplace(&mut self, hash_val: u64, tag: u8) -> EmplaceResult {
        let tags_u64 = self.tags_u64();

        // Stage 1a: SWAR tag match
        let tag_matches = BitMask::match_tag(tags_u64, tag);

        // Stage 1b: 对 tag 匹配的 slot 做 KeyEquals (hash == hash)
        for slot_idx in tag_matches {
            let slot = slot_idx as usize;
            if self.keys[slot] == hash_val {
                // tag 匹配 + hash 匹配 → 暂定为同一个 group
                return EmplaceResult::Existing(slot_idx);
            }
        }

        // 没有 tag+hash 匹配 → 尝试找 empty slot 插入
        let empty_mask = BitMask::match_empty(tags_u64);
        if empty_mask.any() {
            let slot_idx = empty_mask.lowest();
            let slot = slot_idx as usize;
            self.tags[slot] = tag;
            self.keys[slot] = hash_val;
            return EmplaceResult::New(slot_idx);
        }

        // chunk 满了
        EmplaceResult::Full
    }

    /// 单行精确 emplace: 带自定义 key_cmp 闭包做 full key 比较。
    /// 用于 Stage 2 碰撞修复时的 re-emplace。
    pub fn try_emplace_with_cmp(
        &mut self,
        hash_val: u64,
        tag: u8,
        key_cmp: &dyn Fn(u8) -> bool, // slot_idx → 是否 full key 相等
    ) -> EmplaceResult {
        let tags_u64 = self.tags_u64();
        let tag_matches = BitMask::match_tag(tags_u64, tag);

        for slot_idx in tag_matches {
            let slot = slot_idx as usize;
            if self.keys[slot] == hash_val && key_cmp(slot_idx) {
                return EmplaceResult::Existing(slot_idx);
            }
        }

        let empty_mask = BitMask::match_empty(tags_u64);
        if empty_mask.any() {
            let slot_idx = empty_mask.lowest();
            let slot = slot_idx as usize;
            self.tags[slot] = tag;
            self.keys[slot] = hash_val;
            return EmplaceResult::New(slot_idx);
        }

        EmplaceResult::Full
    }
}

impl Default for Chunk {
    fn default() -> Self {
        Self::new()
    }
}

/// Chunk::try_emplace 的返回值
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmplaceResult {
    /// 新 group: 已写入 tag+key, 需要调用者存 key 到 RowContainer 并设 value
    New(u8),
    /// 已有 group: tag+hash 匹配, 需要后续 Stage 2 验证 (或直接当作 update)
    Existing(u8),
    /// chunk 满: 需要线性步进到下一个 chunk
    Full,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_chunk_is_empty() {
        let chunk = Chunk::new();
        assert_eq!(chunk.occupied_count(), 0);
        assert!(!chunk.is_full());
    }

    #[test]
    fn test_emplace_new() {
        let mut chunk = Chunk::new();
        let hash: u64 = 0x7A3F1B02C4E89F30;
        let tag = ((hash >> 16) & 0x7F) as u8;

        let result = chunk.try_emplace(hash, tag);
        assert_eq!(result, EmplaceResult::New(0));
        assert_eq!(chunk.tags[0], tag);
        assert_eq!(chunk.keys[0], hash);
        assert_eq!(chunk.occupied_count(), 1);
    }

    #[test]
    fn test_emplace_existing() {
        let mut chunk = Chunk::new();
        let hash: u64 = 0x7A3F1B02C4E89F30;
        let tag = ((hash >> 16) & 0x7F) as u8;

        // 第一次: 新插入
        let r1 = chunk.try_emplace(hash, tag);
        assert_eq!(r1, EmplaceResult::New(0));

        // 第二次: 同样的 hash → Existing
        let r2 = chunk.try_emplace(hash, tag);
        assert_eq!(r2, EmplaceResult::Existing(0));
    }

    #[test]
    fn test_emplace_different_hashes() {
        let mut chunk = Chunk::new();

        let hash1: u64 = 0x1111111111111111;
        let tag1 = ((hash1 >> 16) & 0x7F) as u8;
        let hash2: u64 = 0x2222222222222222;
        let tag2 = ((hash2 >> 16) & 0x7F) as u8;

        let r1 = chunk.try_emplace(hash1, tag1);
        assert_eq!(r1, EmplaceResult::New(0));

        let r2 = chunk.try_emplace(hash2, tag2);
        assert_eq!(r2, EmplaceResult::New(1));

        assert_eq!(chunk.occupied_count(), 2);
    }

    #[test]
    fn test_emplace_full_chunk() {
        let mut chunk = Chunk::new();

        // 填满 8 个 slot
        for i in 0..8u64 {
            let hash = i * 0x1000_0000_0000 + 0x0100_0000; // 确保不同的 tag
            let tag = ((hash >> 16) & 0x7F) as u8;
            let result = chunk.try_emplace(hash, tag);
            assert_eq!(result, EmplaceResult::New(i as u8));
        }

        assert!(chunk.is_full());

        // 再插入 → Full
        let hash = 0xDEAD_BEEF_CAFE_BABE;
        let tag = ((hash >> 16) & 0x7F) as u8;
        let result = chunk.try_emplace(hash, tag);
        assert_eq!(result, EmplaceResult::Full);
    }

    #[test]
    fn test_same_tag_different_hash() {
        let mut chunk = Chunk::new();

        // 构造两个 hash: tag 相同但完整 hash 不同
        let hash1: u64 = 0x00_2A_0000_0000_0001; // tag = (hash>>16)&0x7F = 0x2A & 0x7F = 0x2A
        let hash2: u64 = 0x00_2A_0000_0000_0002; // tag = 同上 = 0x2A
        let tag = 0x2A;

        let r1 = chunk.try_emplace(hash1, tag);
        assert_eq!(r1, EmplaceResult::New(0));

        // 同 tag 但不同 hash → 不会匹配 → 新插入
        let r2 = chunk.try_emplace(hash2, tag);
        assert_eq!(r2, EmplaceResult::New(1));
    }
}
