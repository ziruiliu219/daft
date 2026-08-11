/// SlotValue: 6-byte 压缩指针，指向 RowContainer 中的某一行。
///
/// 为什么 6 字节而不是 8 字节:
///   - 节省 chunk 空间 (8 slot × 2B = 16B 节省)
///   - 48-bit 地址空间足够用户态指针 (x86_64/aarch64 都是 48-bit 虚拟地址)
///   - 总 chunk 大小 = 8 + 64 + 48 + 8(pad) = 128 bytes = 2 cache line

#[derive(Clone, Copy)]
#[repr(C)]
pub struct SlotValue {
    pub bytes: [u8; 6],
}

impl SlotValue {
    /// 空值
    pub const EMPTY: Self = Self { bytes: [0; 6] };

    /// 从原始指针设置 (只取低 48 bit)
    #[inline]
    pub fn set_ptr(&mut self, ptr: *const u8) {
        let addr = ptr as u64;
        // 取低 48 bit, 存为 little-endian 6 bytes
        self.bytes[0] = addr as u8;
        self.bytes[1] = (addr >> 8) as u8;
        self.bytes[2] = (addr >> 16) as u8;
        self.bytes[3] = (addr >> 24) as u8;
        self.bytes[4] = (addr >> 32) as u8;
        self.bytes[5] = (addr >> 40) as u8;
    }

    /// 还原为指针 (从低 48 bit 符号扩展)
    #[inline]
    pub fn get_ptr(&self) -> *const u8 {
        let mut addr: u64 = 0;
        addr |= self.bytes[0] as u64;
        addr |= (self.bytes[1] as u64) << 8;
        addr |= (self.bytes[2] as u64) << 16;
        addr |= (self.bytes[3] as u64) << 24;
        addr |= (self.bytes[4] as u64) << 32;
        addr |= (self.bytes[5] as u64) << 40;
        // 符号扩展: 如果 bit 47 = 1, 则高 16 bit 全 1 (内核地址)
        // 用户态地址 bit 47 = 0, 所以这里一般不需要扩展
        // 但为安全起见做一下:
        if addr & (1 << 47) != 0 {
            addr |= 0xFFFF_0000_0000_0000;
        }
        addr as *const u8
    }

    /// 还原为可变指针
    #[inline]
    pub fn get_ptr_mut(&self) -> *mut u8 {
        self.get_ptr() as *mut u8
    }

    /// 是否为空 (全零)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes == [0; 6]
    }
}

impl Default for SlotValue {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl std::fmt::Debug for SlotValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlotValue({:p})", self.get_ptr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let data: Vec<u8> = vec![1, 2, 3, 4, 5];
        let ptr = data.as_ptr();

        let mut sv = SlotValue::EMPTY;
        sv.set_ptr(ptr);
        assert_eq!(sv.get_ptr(), ptr);
    }

    #[test]
    fn test_empty() {
        let sv = SlotValue::EMPTY;
        assert!(sv.is_empty());
        assert_eq!(sv.get_ptr(), std::ptr::null());
    }

    #[test]
    fn test_non_empty() {
        let data = vec![42u8; 16];
        let mut sv = SlotValue::EMPTY;
        sv.set_ptr(data.as_ptr());
        assert!(!sv.is_empty());
    }

    #[test]
    fn test_multiple_pointers() {
        let allocs: Vec<Vec<u8>> = (0..10).map(|i| vec![i; 64]).collect();
        for alloc in &allocs {
            let mut sv = SlotValue::EMPTY;
            sv.set_ptr(alloc.as_ptr());
            assert_eq!(sv.get_ptr(), alloc.as_ptr());
        }
    }
}
