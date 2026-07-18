use crate::error::{Error, Result};

pub struct Bloom {
    bits: Vec<u8>,
    k: u32,
}

impl Bloom {
    pub fn build(keys: &[&[u8]], bits_per_key: usize) -> Bloom {
        let k = ((bits_per_key as f64) * 0.69).round().clamp(1.0, 30.0) as u32;
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let mut bits = vec![0u8; nbytes];
        for key in keys {
            let (h1, h2) = double_hash(key);
            for i in 0..k {
                let bit = (h1.wrapping_add(h2.wrapping_mul(i as u64)) as usize) % (nbytes * 8);
                bits[bit / 8] |= 1 << (bit % 8);
            }
        }
        Bloom { bits, k }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let nbytes = self.bits.len();
        let (h1, h2) = double_hash(key);
        for i in 0..self.k {
            let bit = (h1.wrapping_add(h2.wrapping_mul(i as u64)) as usize) % (nbytes * 8);
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.bits.len());
        out.extend_from_slice(&self.k.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Bloom> {
        if bytes.len() < 12 {
            return Err(Error::Corruption("bloom too short".into()));
        }
        let k = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if !(1..=30).contains(&k) {
            return Err(Error::Corruption("bloom k out of range".into()));
        }
        Ok(Bloom {
            bits: bytes[4..].to_vec(),
            k,
        })
    }
}

fn double_hash(key: &[u8]) -> (u64, u64) {
    let h1 = fnv1a(key, 0xcbf29ce484222325);
    let h2 = fnv1a(key, 0x100000001b3).max(1);
    (h1, h2)
}

fn fnv1a(key: &[u8], seed: u64) -> u64 {
    let mut h = seed;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let keys: Vec<&[u8]> = vec![b"apple", b"banana", b"cherry"];
        let b = Bloom::build(&keys, 10);
        for k in &keys {
            assert!(b.contains(k), "must contain inserted key");
        }
    }

    #[test]
    fn absent_key_usually_rejected() {
        let keys: Vec<&[u8]> = vec![b"apple"];
        let b = Bloom::build(&keys, 10);
        assert!(!b.contains(b"zzzzzzzz-not-present"));
    }

    #[test]
    fn roundtrips_through_bytes() {
        let keys: Vec<&[u8]> = vec![b"x", b"y"];
        let b = Bloom::build(&keys, 10);
        let restored = Bloom::from_bytes(&b.to_bytes()).unwrap();
        assert!(restored.contains(b"x"));
        assert!(restored.contains(b"y"));
    }

    #[test]
    fn from_bytes_rejects_empty_bits() {
        let result = Bloom::from_bytes(&[0, 0, 0, 5]);
        assert!(matches!(result, Err(Error::Corruption(_))));
    }

    #[test]
    fn from_bytes_rejects_out_of_range_k() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1000u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 8]);
        let result = Bloom::from_bytes(&buf);
        assert!(matches!(result, Err(Error::Corruption(_))));
    }
}
