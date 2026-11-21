use memchr::{memchr, memchr_iter};
use memmap2::{Mmap, MmapOptions};
use std::{collections::HashMap, fs::{self, File}, hash::{BuildHasherDefault, Hasher}, io, path::Path};
use crate::keys::Key;
use crate::profile::{Phase, Span};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub type Map<K, V> = HashMap<K, V, BuildHasherDefault<FastHasher>>;

#[derive(Default)]
pub struct FastHasher(u64);
impl Hasher for FastHasher {
    fn finish(&self) -> u64 {
        // Packed IDs are left-aligned; multiplicative hashing alone preserves zero low bits.
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
    fn write(&mut self, bytes: &[u8]) { self.0 = hash(bytes).wrapping_add(self.0.rotate_left(17)); }
    fn write_u64(&mut self, value: u64) { self.0 = (self.0.rotate_left(5) ^ value).wrapping_mul(0x517cc1b727220a95); }
    fn write_usize(&mut self, value: usize) { self.write_u64(value as u64); }
    fn write_u32(&mut self, value: u32) { self.write_u64(value as u64); }
    fn write_i32(&mut self, value: i32) { self.write_u64(value as u32 as u64); }
}

pub fn hash(bytes: &[u8]) -> u64 {
    let mut h = 0x517cc1b727220a95u64 ^ bytes.len() as u64;
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        h = (h.rotate_left(5) ^ u64::from_le_bytes(chunk.try_into().unwrap())).wrapping_mul(0x517cc1b727220a95);
    }
    let mut tail = 0u64;
    for &b in chunks.remainder() { tail = (tail << 8) | b as u64; }
    h = (h.rotate_left(5) ^ tail).wrapping_mul(0x517cc1b727220a95);
    h ^ (h >> 32)
}

pub fn csv<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut fields = [&[][..]; N];
    let mut start = 0;
    let mut n = 0;
    let mut quotes = 0;
    for end in memchr_iter(b',', line).chain(std::iter::once(line.len())) {
        if n == N { return None; }
        let mut field = &line[start..end];
        if field.first() == Some(&b'"') {
            if field.len() < 2 || field.last() != Some(&b'"') { return None; }
            field = &field[1..field.len()-1];
            quotes += 2;
        }
        fields[n] = field;
        start = end + 1;
        n += 1;
    }
    (n == N && memchr_iter(b'"', line).count() == quotes).then_some(fields)
}

pub fn lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    memchr_iter(b'\n', bytes).chain(std::iter::once(bytes.len())).map(move |end| {
        let line = &bytes[start..end];
        start = end+1;
        line
    })
}

pub fn body(bytes: &[u8]) -> &[u8] {
    memchr(b'\n', bytes).map_or(&[], |p| &bytes[p+1..])
}

pub fn map_file(path: &Path) -> io::Result<Mmap> {
    // The scorer mounts inputs read-only; mappings outlive every borrowed field.
    unsafe { MmapOptions::new().map(&File::open(path)?) }
}

pub fn map_dir(path: &Path) -> io::Result<Vec<Mmap>> {
    let mut files = fs::read_dir(path)?.map(|entry| entry.map(|e| e.path())).collect::<io::Result<Vec<_>>>()?;
    files.retain(|p| p.extension().is_some_and(|e| e == "csv") && p.is_file());
    files.sort_unstable();
    files.iter().map(|p| map_file(p)).collect()
}

pub fn chunks(maps: &[Mmap]) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    for map in maps {
        let mut bytes = body(map);
        while bytes.len() > 2 * 1024 * 1024 {
            let middle = 2 * 1024 * 1024;
            let end = memchr(b'\n', &bytes[middle..]).map_or(bytes.len(), |n| middle+n+1);
            chunks.push(&bytes[..end]);
            bytes = &bytes[end..];
        }
        if !bytes.is_empty() { chunks.push(bytes); }
    }
    chunks
}

fn digits(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() { return None; }
    let mut value = 0i64;
    for &b in bytes {
        if !b.is_ascii_digit() { return None; }
        value = value.checked_mul(10)?.checked_add((b-b'0') as i64)?;
    }
    Some(value)
}

fn civil_days(year: i64, month: i64, day: i64) -> i64 {
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468
}

pub fn timestamp(s: &[u8]) -> Option<i64> {
    if (s.len() != 20 && s.len() != 25) || s[4] != b'-' { return digits(s); }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' { return None; }
    let year = digits(&s[..4])?;
    let month = digits(&s[5..7])?;
    let day = digits(&s[8..10])?;
    let hour = digits(&s[11..13])?;
    let minute = digits(&s[14..16])?;
    let second = digits(&s[17..19])?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month { 2 => 28 + i64::from(leap), 4|6|9|11 => 30, 1|3|5|7|8|10|12 => 31, _ => return None };
    if day < 1 || day > max_day || hour > 23 || minute > 59 || second > 59 { return None; }
    let offset = if s.len() == 20 {
        if s[19] != b'Z' { return None; }
        0
    } else {
        if !matches!(s[19], b'+'|b'-') || s[22] != b':' { return None; }
        let h = digits(&s[20..22])?;
        let m = digits(&s[23..25])?;
        if h > 23 || m > 59 { return None; }
        (h*60+m) * 60 * if s[19] == b'+' { 1 } else { -1 }
    };
    Some((civil_days(year, month, day)*86400 + hour*3600 + minute*60 + second - offset)*1000)
}

pub fn price(s: &[u8]) -> Option<u32> {
    let value = digits(s)?;
    (1..=10_000_000).contains(&value).then_some(value as u32)
}

pub fn window(path: &Path) -> Result<(i64,i64), Box<dyn std::error::Error>> {
    let json = fs::read_to_string(path)?;
    let value = |key: &str| -> Option<i64> {
        let tail = json.split_once(key)?.1.split_once(':')?.1;
        let quoted = tail.split_once('"')?.1.split_once('"')?.0;
        timestamp(quoted.as_bytes())
    };
    Ok((value("\"window_start\"").ok_or("invalid window_start")?, value("\"window_end\"").ok_or("invalid window_end")?))
}

pub trait LinkKey<'a>: Key<'a> {
    fn build(maps: &'a [Mmap], unsupported: &AtomicBool) -> Links<Self> {
        Links::sequential(maps, unsupported)
    }
}
impl<'a> LinkKey<'a> for &'a [u8] {}
impl<'a> LinkKey<'a> for u64 {
    fn build(maps: &'a [Mmap], unsupported: &AtomicBool) -> Links<Self> {
        let (ids,mask) = crate::identity::build(maps, unsupported);
        Links { ids, mask, empty: 0 }
    }
}

pub struct Links<K> { ids: Vec<(K,K)>, mask: usize, empty: K }
impl<K> Links<K> {
    pub fn new<'a>(maps: &'a [Mmap], unsupported: &AtomicBool) -> Self where K: LinkKey<'a> {
        K::build(maps, unsupported)
    }
    fn sequential<'a>(maps: &'a [Mmap], unsupported: &AtomicBool) -> Self where K: Key<'a> {
        let _phase = Span::worker(Phase::Links);
        let rows: usize = maps.iter().map(|map| {
            let bytes = body(map);
            memchr_iter(b'\n', bytes).count() + usize::from(!bytes.is_empty() && bytes.last() != Some(&b'\n'))
        }).sum();
        // Each physical row adds at most two IDs; keep the lookup table at most half full.
        let capacity = (rows*4).max(1).next_power_of_two();
        let mask = capacity-1;
        let empty = K::parse(b"").expect("empty IDs must be representable");
        let mut ids = vec![(empty,empty);capacity];
        let mut parent = vec![0usize;capacity];
        let mut size = vec![0usize;capacity];
        fn root(parent: &mut [usize], mut i: usize) -> usize {
            while parent[i] != i { parent[i] = parent[parent[i]]; i = parent[i]; }
            i
        }
        for map in maps {
            for line in lines(body(map)) {
                let Some([a,b]) = csv::<2>(line) else { continue };
                if a.is_empty() || b.is_empty() { continue; }
                let (Some(a),Some(b)) = (K::parse(a),K::parse(b)) else {
                    unsupported.store(true, Relaxed); continue;
                };
                let mut pair = [0;2];
                for (slot, id) in pair.iter_mut().zip([a,b]) {
                    let mut i = id.fingerprint() as usize & mask;
                    while ids[i].0 != empty && ids[i].0 != id { i = (i+1)&mask; }
                    if ids[i].0 == empty {
                        ids[i] = (id,id); parent[i] = i; size[i] = 1;
                    }
                    *slot = i;
                }
                let mut a = root(&mut parent, pair[0]);
                let mut b = root(&mut parent, pair[1]);
                if a == b { continue; }
                if size[a] < size[b] { std::mem::swap(&mut a, &mut b); }
                parent[b] = a; size[a] += size[b];
                ids[a].1 = ids[a].1.min(ids[b].1);
            }
        }
        for i in 0..capacity {
            if ids[i].0 != empty { let r = root(&mut parent,i); ids[i].1 = ids[r].1; }
        }
        Self { ids, mask, empty }
    }
    #[inline]
    pub fn name<'a>(&self, id: K) -> K where K: Key<'a> {
        let mut slot = id.fingerprint() as usize & self.mask;
        loop {
            let (key,name) = self.ids[slot];
            if key == self.empty { return id; }
            if key == id { return name; }
            slot = (slot+1)&self.mask;
        }
    }
    #[inline]
    pub fn prefetch<'a>(&self, id: K) where K: Key<'a> {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let slot = id.fingerprint() as usize & self.mask;
            // The address is in the allocation; prefetch does not dereference it.
            std::arch::x86_64::_mm_prefetch(self.ids.as_ptr().add(slot).cast(), std::arch::x86_64::_MM_HINT_T0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn mapped(bytes: &[u8]) -> Mmap {
        let mut map = memmap2::MmapMut::map_anon(bytes.len()).unwrap();
        map.copy_from_slice(bytes);
        map.make_read_only().unwrap()
    }
    #[test]
    fn concurrent_links_match_exact_reference_across_thread_counts() {
        let labels: Vec<_> = (0..32768).map(|i| format!("u{i}").into_bytes()).collect();
        let mut edges: Vec<_> = (17..labels.len()).map(|i| (i,i-17)).collect();
        edges.extend((17..labels.len()).step_by(13).map(|i| (i,i-17)));
        let mut state = 9u64;
        for i in (1..edges.len()).rev() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            edges.swap(i,state as usize%(i+1));
        }
        let mut bytes = b"left,right\n".to_vec();
        for (a,b) in edges {
            bytes.extend_from_slice(&labels[a]); bytes.push(b',');
            bytes.extend_from_slice(&labels[b]); bytes.push(b'\n');
        }
        for i in 0..17 { bytes.extend_from_slice(format!("g{i},u{i}\n").as_bytes()); }
        bytes.extend_from_slice(b"Z,ZZ");
        let maps = [mapped(&bytes)];
        let unsupported = AtomicBool::new(false);
        let reference = Links::<&[u8]>::new(&maps,&unsupported);
        for threads in [1,2,8] {
            let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
            let parallel = pool.install(|| Links::<u64>::new(&maps,&unsupported));
            for label in &labels {
                let id = crate::keys::pack(label).unwrap();
                let mut name = [0;10];
                let length = crate::keys::unpack(parallel.name(id),&mut name);
                assert_eq!(&name[..length],reference.name(label));
            }
            let z = crate::keys::pack(b"Z").unwrap();
            assert_eq!(parallel.name(crate::keys::pack(b"ZZ").unwrap()),z);
            let absent = crate::keys::pack(b"unlinked").unwrap();
            assert_eq!(parallel.name(absent),absent);
        }
        assert!(!unsupported.load(Relaxed));
    }
    #[test]
    fn flat_links_keep_exact_identity_through_collisions_and_transitive_unions() {
        #[derive(Clone,Copy,Debug,Eq,PartialEq,Ord,PartialOrd,Hash)]
        struct Collision(u64);
        impl Key<'_> for Collision {
            fn parse(bytes: &[u8]) -> Option<Self> { crate::keys::pack(bytes).map(Self) }
            fn fingerprint(self) -> u64 { 7 }
            fn append_plain(self, bytes: &mut Vec<u8>) { self.0.append_plain(bytes); }
        }
        impl LinkKey<'_> for Collision {}
        let maps = [mapped(b"left,right\nq,a\nb,q\nc,b\nq,q\n\n,x")];
        let unsupported = AtomicBool::new(false);
        let links = Links::<Collision>::new(&maps, &unsupported);
        let a = Collision::parse(b"a").unwrap();
        for id in [b"a",b"b",b"c",b"q"] {
            let id = Collision::parse(id).unwrap();
            links.prefetch(id);
            assert_eq!(links.name(id), a);
        }
        let x = Collision::parse(b"x").unwrap();
        assert_eq!(links.name(x), x);
        assert!(!unsupported.load(Relaxed));
    }
    #[test]
    fn flat_links_preserve_unicode_byte_order_and_packed_fallback() {
        let maps = [mapped(b"left,right\nz,\xc3\xa9\n\xc3\xa9,A\nA,0")];
        let unsupported = AtomicBool::new(false);
        let _ = Links::<u64>::new(&maps, &unsupported);
        assert!(unsupported.load(Relaxed));
        unsupported.store(false, Relaxed);
        let links = Links::<&[u8]>::new(&maps, &unsupported);
        for id in [&b"z"[..], b"\xc3\xa9", b"A", b"0"] {
            assert_eq!(links.name(id), b"0");
        }
        assert_eq!(links.name(b"x"), b"x");
        assert!(!unsupported.load(Relaxed));
    }
    #[test]
    fn csv_preserves_carriage_returns_and_spaces_and_rejects_inner_quotes() {
        assert_eq!(csv::<2>(b"\" a\",b\r"), Some([&b" a"[..], &b"b\r"[..]]));
        assert_eq!(csv::<2>(b"\"\",\"\""), Some([&b""[..], &b""[..]]));
        for bad in [b"a,b,c".as_slice(), b"a,\"b", b"a,b\"", b"a,\"b\"\r", b"a,\"b\"\"", b""] { assert!(csv::<2>(bad).is_none(), "{bad:?}"); }
    }
    #[test]
    fn timestamp_forms_and_exact_rejections() {
        assert_eq!(timestamp(b"1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(timestamp(b"1970-01-01T00:00:00-00:00"), Some(0));
        assert_eq!(timestamp(b"1970-01-01T01:00:00+01:00"), Some(0));
        assert_eq!(timestamp(b"00000000000000000000000000086400000"), Some(DAY));
        assert_eq!(timestamp(b"2028-02-29T00:00:00Z"), Some(1835395200000));
        for bad in ["", "2027-02-29T00:00:00Z", "2028-02-30T00:00:00Z", "2026-01-01T24:00:00Z", "2026-01-01T00:00:60Z", "2026-01-01T00:00:00+24:00", "2026-01-01T00:00:00+05:60", "2026-01-01T00:00:00+0200", "2026-01-01T00:00:00Z\r", "1.0", "-5", "٢٥٠", "999999999999999999999999999"] { assert_eq!(timestamp(bad.as_bytes()), None, "{bad}"); }
    }
    #[test]
    fn prices_are_bounded_ascii_integers() {
        assert_eq!(price(b"0250"), Some(250));
        assert_eq!(price(b"10000000"), Some(10_000_000));
        for bad in ["", "0", "-5", "+5", "1.0", "10000001", "1000\r", "٢٥٠"] { assert_eq!(price(bad.as_bytes()), None); }
    }
}
