/// BitMask: SWAR (SIMD Within A Register) 位运算实现
///
/// 一次性比较 chunk 内 8 个 tag byte，返回匹配 slot 编号的迭代器。
/// 不依赖 SIMD intrinsics，纯位运算，所有平台通用。

const ONES: u64 = 0x0101_0101_0101_0101;
const HIGH_BITS: u64 = 0x8080_8080_8080_8080;

/// 将一个 u8 广播到 u64 的每个字节
#[inline(always)]
const fn broadcast(byte: u8) -> u64 {
    (byte as u64) * ONES
}

/// BitMask 存储匹配结果，每个匹配 slot 在对应字节的 MSB (bit 7) 为 1。
/// 通过迭代器返回匹配的 slot 索引 (0-7)。
#[derive(Clone, Copy, Debug)]
pub struct BitMask(u64);

impl BitMask {
    /// 空 bitmask
    pub const EMPTY: Self = Self(0);

    /// SWAR tag 比较：在 tags_u64 中找所有等于 target 的字节
    ///
    /// 算法来源: https://graphics.stanford.edu/~seander/bithacks.html#ValueInWord
    ///
    /// 原理:
    ///   1. XOR: 匹配字节变 0x00，不匹配字节非零
    ///   2. (x - 0x01...) & !x & 0x80...: 检测零字节（标准 SWAR 技巧）
    #[inline]
    pub fn match_tag(tags_u64: u64, target: u8) -> Self {
        let x = tags_u64 ^ broadcast(target);
        // 标准零字节检测: 如果某字节为 0x00, 则 (byte-1) 会借位使 MSB=1, 而 !byte=0xFF 的 MSB=1
        let result = x.wrapping_sub(ONES) & !x & HIGH_BITS;
        Self(result)
    }

    /// 找所有 empty slot (tag == 0x80)
    #[inline]
    pub fn match_empty(tags_u64: u64) -> Self {
        // Empty 的 tag 是 0x80。直接检测每个字节的 MSB 是否为 1 且低 7 位为 0。
        // 简单实现: 用 match_tag 匹配 0x80
        Self::match_tag(tags_u64, 0x80)
    }

    /// 是否有任何匹配
    #[inline]
    pub fn any(self) -> bool {
        self.0 != 0
    }

    /// 匹配数量
    #[inline]
    pub fn count(self) -> u32 {
        // 每个匹配贡献 1 个 set bit (在每字节的 bit 7)
        (self.0 >> 7).count_ones() as u32
    }

    /// 最低匹配 slot 的索引 (0-7), 前提: self.any() == true
    #[inline]
    pub fn lowest(self) -> u8 {
        debug_assert!(self.any(), "BitMask::lowest called on empty mask");
        // 找到最低 set bit 的字节位置
        (self.0.trailing_zeros() / 8) as u8
    }

    /// 移除最低匹配，返回新 BitMask
    #[inline]
    pub fn advance(self) -> Self {
        debug_assert!(self.any(), "BitMask::advance called on empty mask");
        // 清除最低的 set bit (在 MSB 位置)
        let lowest_bit = self.0 & self.0.wrapping_neg();
        Self(self.0 ^ lowest_bit)
    }

    /// 获取原始值
    #[inline]
    pub fn raw(self) -> u64 {
        self.0
    }
}

impl Iterator for BitMask {
    type Item = u8;

    #[inline]
    fn next(&mut self) -> Option<u8> {
        if self.0 != 0 {
            let idx = self.lowest();
            *self = self.advance();
            Some(idx)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_tag_no_match() {
        // 所有 slot 都是 empty (0x80)
        let tags = broadcast(0x80);
        let mask = BitMask::match_tag(tags, 0x2A);
        assert!(!mask.any());
        assert_eq!(mask.count(), 0);
    }

    #[test]
    fn test_match_tag_single_match() {
        // slot 0 = 0x2A, 其余 empty
        let mut tags_bytes = [0x80u8; 8];
        tags_bytes[0] = 0x2A;
        let tags = u64::from_ne_bytes(tags_bytes);
        let mask = BitMask::match_tag(tags, 0x2A);
        assert!(mask.any());
        assert_eq!(mask.count(), 1);
        assert_eq!(mask.lowest(), 0);
    }

    #[test]
    fn test_match_tag_multiple_matches() {
        let mut tags_bytes = [0x80u8; 8];
        tags_bytes[0] = 0x3D;
        tags_bytes[3] = 0x3D;
        tags_bytes[7] = 0x3D;
        let tags = u64::from_ne_bytes(tags_bytes);
        let mask = BitMask::match_tag(tags, 0x3D);
        assert_eq!(mask.count(), 3);

        let indices: Vec<u8> = mask.collect();
        assert_eq!(indices, vec![0, 3, 7]);
    }

    #[test]
    fn test_match_empty() {
        let mut tags_bytes = [0x80u8; 8];
        tags_bytes[0] = 0x2A;
        tags_bytes[1] = 0x3F;
        let tags = u64::from_ne_bytes(tags_bytes);
        let mask = BitMask::match_empty(tags);
        assert_eq!(mask.count(), 6); // slot 2-7 are empty
    }

    #[test]
    fn test_iterator() {
        let mut tags_bytes = [0x80u8; 8];
        tags_bytes[1] = 0x42;
        tags_bytes[5] = 0x42;
        let tags = u64::from_ne_bytes(tags_bytes);
        let mask = BitMask::match_tag(tags, 0x42);

        let indices: Vec<u8> = mask.collect();
        assert_eq!(indices, vec![1, 5]);
    }

    #[test]
    fn test_advance() {
        let mut tags_bytes = [0x80u8; 8];
        tags_bytes[2] = 0x11;
        tags_bytes[6] = 0x11;
        let tags = u64::from_ne_bytes(tags_bytes);
        let mut mask = BitMask::match_tag(tags, 0x11);

        assert_eq!(mask.lowest(), 2);
        mask = mask.advance();
        assert_eq!(mask.lowest(), 6);
        mask = mask.advance();
        assert!(!mask.any());
    }
}
