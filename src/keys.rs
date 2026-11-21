use std::hash::{Hash, Hasher};

const ALPHABET: &[u8;63] = b"\00123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const CODES: [u8;256] = {
    let mut codes = [0;256];
    let mut i = 0;
    while i < 10 { codes[b'0' as usize+i] = i as u8+1; i += 1; }
    i = 0;
    while i < 26 {
        codes[b'A' as usize+i] = i as u8+11;
        codes[b'a' as usize+i] = i as u8+37;
        i += 1;
    }
    codes
};

pub fn pack(bytes: &[u8]) -> Option<u64> {
    if bytes.len() > 10 { return None; }
    let mut key = 0u64;
    let mut invalid = false;
    for &byte in bytes {
        let code = CODES[byte as usize];
        invalid |= code == 0;
        key = (key << 6) | u64::from(code);
    }
    (!invalid).then_some(key << ((64-6*bytes.len()) & 63))
}

pub fn unpack(mut key: u64, bytes: &mut [u8;10]) -> usize {
    let mut length = 0;
    while key != 0 {
        bytes[length] = ALPHABET[(key >> 58) as usize];
        key <<= 6;
        length += 1;
    }
    length
}

pub trait Key<'a>: Copy+Ord+Hash+Send+Sync {
    fn parse(bytes: &'a [u8]) -> Option<Self>;
    fn fingerprint(self) -> u64;
    fn append_plain(self, data: &mut Vec<u8>);
}
impl Key<'_> for u64 {
    fn parse(bytes: &[u8]) -> Option<Self> { pack(bytes) }
    fn fingerprint(self) -> u64 {
        let mut hasher = crate::input::FastHasher::default();
        hasher.write_u64(self); hasher.finish()
    }
    fn append_plain(self, data: &mut Vec<u8>) {
        let mut bytes = [0;10];
        let length = unpack(self, &mut bytes);
        data.extend_from_slice(&(length as u32).to_le_bytes());
        data.extend_from_slice(&bytes[..length]);
    }
}
impl<'a> Key<'a> for &'a [u8] {
    fn parse(bytes: &'a [u8]) -> Option<Self> { Some(bytes) }
    fn fingerprint(self) -> u64 { crate::input::hash(self) }
    fn append_plain(self, data: &mut Vec<u8>) {
        data.extend_from_slice(&(self.len() as u32).to_le_bytes());
        data.extend_from_slice(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_byte_uses_the_exact_ascii_code_or_fallback() {
        for byte in 0..=255u8 {
            let code = match byte {
                b'0'..=b'9' => Some(byte-b'0'+1),
                b'A'..=b'Z' => Some(byte-b'A'+11),
                b'a'..=b'z' => Some(byte-b'a'+37),
                _ => None,
            };
            assert_eq!(pack(&[byte]), code.map(|code| u64::from(code)<<58));
        }
    }
    #[test]
    fn packed_order_matches_byte_order_including_prefixes_and_case() {
        assert_eq!(pack(b"u"), Some(57u64<<58));
        let alphabet = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        let mut state = 1u64;
        let mut values = vec![b"".to_vec(), b"a".to_vec(), b"ab".to_vec(), b"abc".to_vec(), b"A".to_vec(), b"0".to_vec(), b"z".to_vec()];
        for _ in 0..10000 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let length = (state%10+1) as usize;
            let mut text = Vec::with_capacity(length);
            for _ in 0..length {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                text.push(alphabet[(state%62) as usize]);
            }
            values.push(text);
        }
        values.sort(); values.dedup();
        let keys: Vec<_> = values.iter().map(|v| pack(v).unwrap()).collect();
        assert!(keys.windows(2).all(|pair| pair[0]<pair[1]));
        for (value,key) in values.iter().zip(keys) {
            let mut bytes = [0;10];
            let length = unpack(key, &mut bytes);
            assert_eq!(&bytes[..length], value);
        }
    }
    #[test]
    fn non_alphanumeric_and_overlong_ids_require_exact_fallback() {
        for text in ["a_b", " u", "u ", "é", "a,b", "abcdefghijk", "a\0"] { assert_eq!(pack(text.as_bytes()), None); }
    }
    #[test]
    fn aligned_packed_keys_mix_into_low_table_index_bits() {
        assert!([b"a",b"b",b"c",b"0",b"z"].iter().any(|text| pack(*text).unwrap().fingerprint() & 1023 != 0));
    }
}
