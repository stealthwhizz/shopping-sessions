use crate::input::WEEK;

pub const INVALID: i64 = i64::MIN;

pub trait Layout: Copy + Send + Sync {
    type Data: Copy + Send + Sync;
    type Time: Copy + Ord + Send + Sync;
    fn event(ts: i64, revenue: u32, category: u32, origin: i64) -> Self::Data;
    fn valid(data: Self::Data) -> bool;
    fn time(data: Self::Data) -> Self::Time;
    fn click(ts: i64, origin: i64) -> Self::Time;
    fn units(time: Self::Time) -> i64;
    fn epoch(time: i64, origin: i64) -> i64;
    fn revenue(data: Self::Data) -> u32;
    fn category(data: Self::Data) -> u32;
}

#[derive(Clone, Copy)]
pub struct Compact;
impl Compact {
    pub fn eligible(start: i64, end: i64, categories: usize) -> bool {
        let Some(origin) = start.checked_sub(WEEK) else { return false };
        let Some(span) = end.checked_sub(origin) else { return false };
        categories <= 256 && (0..=i64::from(u32::MAX)).contains(&span)
    }
}
impl Layout for Compact {
    type Data = u64;
    type Time = u32;
    fn event(ts: i64, revenue: u32, category: u32, origin: i64) -> u64 {
        if ts == INVALID { return u64::from(u32::MAX) << 32; }
        let relative = ts-origin;
        debug_assert!((0..i64::from(u32::MAX)).contains(&relative));
        debug_assert!(revenue < 1<<24 && category < 256);
        ((relative as u64) << 32) | (u64::from(category) << 24) | u64::from(revenue)
    }
    fn valid(data: u64) -> bool { Self::time(data) != u32::MAX }
    fn time(data: u64) -> u32 { (data >> 32) as u32 }
    fn click(ts: i64, origin: i64) -> u32 {
        debug_assert!((0..i64::from(u32::MAX)).contains(&(ts-origin)));
        (ts-origin) as u32
    }
    fn units(time: u32) -> i64 { i64::from(time) }
    fn epoch(time: i64, origin: i64) -> i64 { time+origin }
    fn revenue(data: u64) -> u32 { data as u32 & 0xff_ffff }
    fn category(data: u64) -> u32 { (data as u32) >> 24 }
}

#[derive(Clone, Copy)]
pub struct Wide;
#[derive(Clone, Copy)]
pub struct WideData { ts: i64, revenue: u32, category: u32 }
impl Layout for Wide {
    type Data = WideData;
    type Time = i64;
    fn event(ts: i64, revenue: u32, category: u32, _: i64) -> WideData { WideData { ts, revenue, category } }
    fn valid(data: WideData) -> bool { data.ts != INVALID }
    fn time(data: WideData) -> i64 { data.ts }
    fn click(ts: i64, _: i64) -> i64 { ts }
    fn units(time: i64) -> i64 { time }
    fn epoch(time: i64, _: i64) -> i64 { time }
    fn revenue(data: WideData) -> u32 { data.revenue }
    fn category(data: WideData) -> u32 { data.category }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_startup_guard_reserves_invalid_timestamp_and_checks_all_ranges() {
        let limit = i64::from(u32::MAX)-WEEK;
        assert!(Compact::eligible(0, limit, 256));
        assert!(!Compact::eligible(0, limit+1, 256));
        assert!(!Compact::eligible(0, WEEK, 257));
        assert!(!Compact::eligible(i64::MIN, 0, 1));
        assert!(!Compact::eligible(i64::MAX, i64::MIN, 1));
        assert!(!Compact::eligible(0, -WEEK-1, 1));
    }
    #[test]
    fn compact_payload_round_trips_boundaries_and_full_purchase_range() {
        let origin = -62_167_219_200_000;
        for time in [0, 1, WEEK, i64::from(u32::MAX)-1] {
            for revenue in [0, 1, 10_000_000] {
                for category in [0, 1, 255] {
                    let data = Compact::event(origin+time, revenue, category, origin);
                    assert!(Compact::valid(data));
                    assert_eq!(Compact::units(Compact::time(data)), time);
                    assert_eq!(Compact::epoch(time, origin), origin+time);
                    assert_eq!(Compact::revenue(data), revenue);
                    assert_eq!(Compact::category(data), category);
                    assert_eq!(Compact::units(Compact::click(origin+time, origin)), time);
                }
            }
        }
        assert!(!Compact::valid(Compact::event(INVALID, 10_000_000, 255, origin)));
        let wide = Wide::event(INVALID, u32::MAX, u32::MAX, 0);
        assert!(!Wide::valid(wide));
        assert_eq!(Wide::revenue(wide), u32::MAX);
        assert_eq!(Wide::category(wide), u32::MAX);
    }
}
