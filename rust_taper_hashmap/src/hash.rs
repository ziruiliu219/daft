/// Hash computation utilities.
/// Uses CRC32 for integers (hardware accelerated on ARM/x86),
/// and a fast byte hash for strings.

/// Hash a 64-bit integer using CRC32 intrinsic if available.
#[inline(always)]
pub fn hash_i64(val: i64) -> u64 {
    #[cfg(target_arch = "aarch64")]
    {
        // ARM CRC32 intrinsic
        unsafe {
            use std::arch::aarch64::__crc32d;
            __crc32d(0xFFFFFFFF, val as u64) as u64
        }
    }
    #[cfg(target_arch = "x86_64")]
    {
        // x86 CRC32 intrinsic
        unsafe {
            use std::arch::x86_64::_mm_crc32_u64;
            _mm_crc32_u64(0xFFFFFFFF, val as u64)
        }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        // Fallback: FxHash-style
        let mut h = val as u64;
        h = h.wrapping_mul(0x517cc1b727220a95);
        h ^= h >> 32;
        h
    }
}

/// Hash a byte slice (for VARCHAR/string types).
#[inline]
pub fn hash_bytes(data: &[u8]) -> u64 {
    // Simple FNV-1a for now; can replace with xxhash3 later
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Mix two hash values (multi-column combine).
/// Same as boost::hash_combine / FastHashMix.
#[inline(always)]
pub fn hash_mix(a: u64, b: u64) -> u64 {
    a ^ (b.wrapping_add(0x9e3779b97f4a7c15).wrapping_add(a << 6).wrapping_add(a >> 2))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_deterministic() {
        assert_eq!(hash_i64(42), hash_i64(42));
        assert_ne!(hash_i64(42), hash_i64(43));
    }

    #[test]
    fn test_hash_bytes_deterministic() {
        assert_eq!(hash_bytes(b"Beijing"), hash_bytes(b"Beijing"));
        assert_ne!(hash_bytes(b"Beijing"), hash_bytes(b"Shanghai"));
    }

    #[test]
    fn test_hash_mix() {
        let h1 = hash_i64(42);
        let h2 = hash_i64(77);
        let mixed = hash_mix(h1, h2);
        assert_ne!(mixed, h1);
        assert_ne!(mixed, h2);
    }
}
