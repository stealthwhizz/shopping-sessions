//! Single-pass CSV scanner and timestamp parser. Results are identical to
//! `for line in input::lines(chunk) { input::csv::<N>(line) }` and `input::timestamp`.

/// Calls `f` once per `\n`-separated line of `chunk` (including the final, possibly empty,
/// unterminated one) with the line's `N` fields, quotes stripped, or `None` if malformed.
pub fn for_each_line<'a, const N: usize>(chunk: &'a [u8], f: impl FnMut(Option<[&'a [u8]; N]>)) {
    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("bmi1") && is_x86_feature_detected!("popcnt") {
        // SAFETY: the CPU features `scan_avx2` enables were just detected.
        return unsafe { scan_avx2(chunk, f) };
    }
    scan_portable(chunk, f)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,popcnt")]
fn scan_avx2<'a, const N: usize>(chunk: &'a [u8], f: impl FnMut(Option<[&'a [u8]; N]>)) {
    scan(chunk, f, |b| masks_avx2(b))
}

/// The exact fallback for CPUs without AVX2; public so tests can check it on any machine.
pub fn scan_portable<'a, const N: usize>(chunk: &'a [u8], f: impl FnMut(Option<[&'a [u8]; N]>)) {
    scan(chunk, f, masks_portable)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
fn masks_avx2(block: &[u8; 64]) -> [u64; 3] {
    use std::arch::x86_64::*;
    // SAFETY: `block` is 64 readable bytes; loadu has no alignment requirement.
    let (lo, hi) = unsafe { (_mm256_loadu_si256(block.as_ptr().cast()), _mm256_loadu_si256(block.as_ptr().add(32).cast())) };
    let eq = |c: u8| {
        let c = _mm256_set1_epi8(c as i8);
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, c)) as u32 as u64;
        let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, c)) as u32 as u64;
        lo | hi << 32
    };
    [eq(b'\n'), eq(b','), eq(b'"')]
}

#[inline(always)]
fn masks_portable(block: &[u8; 64]) -> [u64; 3] {
    const LO7: u64 = 0x7f7f_7f7f_7f7f_7f7f;
    let mut m = [0u64; 3];
    for (k, word) in block.chunks_exact(8).enumerate() {
        let x = u64::from_le_bytes(word.try_into().unwrap());
        for (j, c) in [b'\n', b',', b'"'].into_iter().enumerate() {
            // Exact zero-byte test (no false positives), then gather each byte's high bit into 8 bits.
            let t = x ^ (c as u64 * 0x0101_0101_0101_0101);
            let zero = !(((t & LO7) + LO7) | t | LO7);
            m[j] |= ((zero >> 7).wrapping_mul(0x0102_0408_1020_4080) >> 56) << (8 * k);
        }
    }
    m
}

/// Walks newline and comma bits of each 64-byte block in order. Quotes are only counted: a line is
/// valid iff its quote count equals the quotes stripped from wrapped fields, which also rejects a
/// field that starts with a quote but does not end with one.
#[inline(always)]
fn scan<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut(Option<[&'a [u8]; N]>), masks: impl Fn(&[u8; 64]) -> [u64; 3]) {
    let mut fields = [&chunk[..0]; N];
    let mut n = 0; // fields seen in the current line; past N the line is dropped anyway
    let mut stripped = 0; // wrapping quotes removed in the current line
    let mut quotes = 0; // quotes in the current line before the unwalked part of this block
    let mut start = 0; // first byte of the current field
    let mut tail = [0u8; 64];
    let mut base = 0;
    while base < chunk.len() {
        let block: &[u8; 64] = match chunk.get(base..base + 64) {
            Some(b) => b.try_into().unwrap(),
            None => {
                // Zero padding holds no separators, so no bit lands past the chunk.
                tail[..chunk.len() - base].copy_from_slice(&chunk[base..]);
                &tail
            }
        };
        let [nl, comma, mut q] = masks(block);
        let mut seps = nl | comma;
        while seps != 0 {
            let i = seps.trailing_zeros();
            seps &= seps - 1;
            let end = base + i as usize;
            // SAFETY: end < chunk.len() (a separator's position) and start <= end, so every index is
            // in bounds and start + w <= end - w (w = 1 needs end - start >= 2). An empty field reads
            // its separator as both first and last byte, which is not a quote.
            let (first, last) = unsafe { (*chunk.get_unchecked(start), *chunk.get_unchecked(end - (end > start) as usize)) };
            let wrapped = (end - start >= 2) & (first == b'"') & (last == b'"');
            let w = wrapped as usize;
            fields[n.min(N - 1)] = unsafe { chunk.get_unchecked(start + w..end - w) };
            stripped += 2 * w as u32;
            n += 1;
            start = end + 1;
            if nl >> i & 1 != 0 {
                let line_quotes = quotes + (q & ((1u64 << i) - 1)).count_ones();
                q &= u64::MAX << i;
                quotes = 0;
                f((n == N && line_quotes == stripped).then_some(fields));
                (n, stripped) = (0, 0);
            }
        }
        quotes += q.count_ones();
        base += 64;
    }
    let mut last = &chunk[start..];
    if let [b'"', inner @ .., b'"'] = last {
        last = inner;
        stripped += 2;
    }
    fields[n.min(N - 1)] = last;
    f((n + 1 == N && quotes == stripped).then_some(fields));
}

/// Epoch milliseconds of an F1, F2 or F3 timestamp; `None` for anything else.
pub fn timestamp(s: &[u8]) -> Option<i64> {
    match s.len() {
        13 => {
            // Two overlapping 8-byte loads cover all 13 digits; the second keeps only s[8..13].
            let hi = load(s, 0) ^ ZEROS;
            let lo = load(s, 5) ^ ZEROS;
            if !ok(hi, !0, 0) || !ok(lo, !0, 0) { return None; }
            Some((parse8(hi) * 100_000 + parse8(lo & !0xff_ffff)) as i64)
        }
        20 | 25 if s[4] == b'-' => datetime(s),
        _ => timestamp_slow(s),
    }
}

const ZEROS: u64 = 0x3030_3030_3030_3030;

#[inline(always)]
fn load(s: &[u8], at: usize) -> u64 { u64::from_le_bytes(s[at..at + 8].try_into().unwrap()) }

/// `v` is input XOR a template: its `digits` bytes must be 0..=9 and its `seps` bytes 0.
#[inline(always)]
fn ok(v: u64, digits: u64, seps: u64) -> bool {
    // A byte <= 9 has a clear high nibble before and after adding 6. A carry out of an invalid
    // byte can only corrupt bytes above a failure that is detected anyway.
    (v & seps) == 0 && ((v | v.wrapping_add(0x0606_0606_0606_0606 & digits)) & digits & 0xf0f0_f0f0_f0f0_f0f0) == 0
}

/// Value of 8 digit bytes (each 0..=9, first byte most significant).
#[inline(always)]
fn parse8(v: u64) -> u64 {
    let v = v.wrapping_mul(10 << 8 | 1) >> 8;
    let v = (v & 0x00ff_00ff_00ff_00ff).wrapping_mul(100 << 16 | 1) >> 16;
    (v & 0x0000_ffff_0000_ffff).wrapping_mul(10_000 << 32 | 1) >> 32
}

/// Byte i becomes 10 * byte i + byte i+1: two-digit values at each pair's first byte. Every byte
/// must be 0..=9.
#[inline(always)]
fn pairs(v: u64) -> u64 { v * 10 + (v >> 8) }

#[inline(always)]
fn byte(v: u64, i: u32) -> u32 { (v >> (8 * i)) as u8 as u32 }

/// Fixed-layout F1/F2: XOR with a template zeroes separators and maps digits to 0..=9.
#[inline(always)]
fn datetime(s: &[u8]) -> Option<i64> {
    let a = load(s, 0) ^ u64::from_le_bytes(*b"0000-00-");
    let b = load(s, 8) ^ u64::from_le_bytes(*b"00T00:00");
    if !ok(a, 0x00ff_ff00_ffff_ffff, 0xff00_00ff_0000_0000) || !ok(b, 0xffff_00ff_ff00_ffff, 0x0000_ff00_00ff_0000) { return None; }
    let (a, b) = (pairs(a), pairs(b));
    let (year, month) = (byte(a, 0) * 100 + byte(a, 2), byte(a, 5));
    let (day, hour, minute) = (byte(b, 0), byte(b, 3), byte(b, 6));
    let (second, offset) = if s.len() == 20 {
        let c = u64::from(u32::from_le_bytes(s[16..20].try_into().unwrap()) ^ u32::from_le_bytes(*b":00Z"));
        if !ok(c, 0x00ff_ff00, 0xff00_00ff) { return None; }
        (byte(pairs(c), 1), 0)
    } else {
        // s[17..25] = "SS±HH:MM"; the sign byte is masked out before pairing.
        let c = load(s, 17) ^ u64::from_le_bytes(*b"00\x0000:00");
        if s[16] != b':' || !matches!(s[19], b'+' | b'-') || !ok(c, 0xffff_00ff_ff00_ffff, 0x0000_ff00_0000_0000) { return None; }
        let c = pairs(c & !0xff_0000);
        let (h, m) = (byte(c, 3), byte(c, 6));
        if h > 23 || m > 59 { return None; }
        let offset = (h * 60 + m) as i64 * 60;
        (byte(c, 0), if s[19] == b'+' { offset } else { -offset })
    };
    if hour > 23 || minute > 59 || second > 59 { return None; }
    let days = if (TABLE_YEAR..TABLE_YEAR+TABLE_YEARS).contains(&year) && month <= 12 && day <= 31 {
        match DAYS_SINCE_TABLE_YEAR[(((year - TABLE_YEAR) * 13 + month) * 32 + day) as usize] {
            u16::MAX => return None,
            days => days as i64 + TABLE_YEAR_EPOCH_DAY,
        }
    } else {
        epoch_day(year, month, day)?
    };
    Some((days * 86400 + (hour * 3600 + minute * 60 + second) as i64 - offset) * 1000)
}

const DAYS: [u32; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

const fn leap(year: u32) -> bool { year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) }

/// Days from 1970-01-01 to a valid date; `None` for an impossible one.
fn epoch_day(year: u32, month: u32, day: u32) -> Option<i64> {
    const BEFORE: [u32; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    if !(1..=12).contains(&month) { return None; }
    let leap = leap(year);
    if day < 1 || day > DAYS[month as usize] + (leap && month == 2) as u32 { return None; }
    // Days since 0000-01-01 (proleptic Gregorian, year 0 leap) minus those up to 1970-01-01.
    let doy = BEFORE[month as usize] + day - 1 + (leap && month > 2) as u32;
    let days = 365 * year + (year + 3) / 4 - (year + 99) / 100 + (year + 399) / 400 + doy;
    Some(days as i64 - 719_528)
}

/// Calendar lookup replacing leap-year and day-count arithmetic for the years real data uses:
/// entry `((year - TABLE_YEAR) * 13 + month) * 32 + day` is days since TABLE_YEAR-01-01,
/// or `u16::MAX` for an impossible date (month 0, day 0, February 30, ...).
const TABLE_YEAR: u32 = 2000;
const TABLE_YEARS: u32 = 128;
const TABLE_YEAR_EPOCH_DAY: i64 = 10_957;
static DAYS_SINCE_TABLE_YEAR: [u16; (TABLE_YEARS * 13 * 32) as usize] = {
    let mut table = [u16::MAX; (TABLE_YEARS * 13 * 32) as usize];
    let mut days = 0u32;
    let mut y = 0;
    while y < TABLE_YEARS {
        let mut month = 1;
        while month <= 12 {
            let mut day = 1;
            while day <= DAYS[month as usize] + (leap(TABLE_YEAR + y) && month == 2) as u32 {
                table[((y * 13 + month) * 32 + day) as usize] = days as u16;
                days += 1;
                day += 1;
            }
            month += 1;
        }
        y += 1;
    }
    table
};

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

/// The reference parser, kept verbatim for every input the fast paths do not cover.
fn timestamp_slow(s: &[u8]) -> Option<i64> {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scan_matches_line_by_line_csv_across_blocks() {
        let mut state = 3u64;
        let mut next = |n: u64| { state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (state >> 33) % n };
        let pieces: [&[u8]; 9] = [b"a", b"\"q\"", b"\"", b"", b"xyz", b"\"\"", b"\r", b"0123456789abcdef", b" "];
        for round in 0..300 {
            let mut text = Vec::new();
            let lines = next(400);
            for _ in 0..lines {
                let fields = if next(4) == 0 { next(10) } else { 7 };
                for k in 0..fields {
                    if k > 0 { text.push(b','); }
                    text.extend_from_slice(pieces[next(pieces.len() as u64) as usize]);
                }
                if next(50) == 0 { text.extend(std::iter::repeat_n(b',', next(5000) as usize)); }
                if next(30) == 0 { text.extend(std::iter::repeat_n(b'"', next(200) as usize)); }
                text.push(b'\n');
            }
            if round % 2 == 0 { text.extend_from_slice(b"t,\"u\",,,,,z"); }
            let expected: Vec<_> = crate::input::lines(&text).map(crate::input::csv::<7>).collect();
            let mut actual = Vec::new();
            scan::<7>(&text, |f| actual.push(f), masks_portable);
            assert_eq!(actual, expected, "round {round}");
        }
    }
    #[test]
    fn fast_datetime_matches_reference_on_every_calendar_cell() {
        for year in (1995..2135).chain([0, 1970, 9999]) {
            for month in 0..=13 {
                for day in 0..=32 {
                    for (time, zone) in [("00:00:00", "Z"), ("23:59:59", "Z"), ("12:34:56", "+05:30"), ("00:00:00", "-23:59"), ("24:00:00", "Z")] {
                        let s = format!("{year:04}-{month:02}-{day:02}T{time}{zone}");
                        assert_eq!(timestamp(s.as_bytes()), timestamp_slow(s.as_bytes()), "{s}");
                    }
                }
            }
        }
    }
}
