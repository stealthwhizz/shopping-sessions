//! Compile with `--features phase-profile`; all guards are zero-sized no-ops otherwise.
#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Phase { Startup, Mmap, Links, Catalog, EventParse, Dedup, ClickParse, BucketCopy, EventSort, ClickSort, Grouping, PersonFused, CampaignMerge, Parquet, Teardown, SessionSample, AttributionSample, AggregationSample, ParquetWrite }

pub struct Span {
    #[cfg(feature = "phase-profile")]
    state: Option<(Phase, bool, u64, u64)>,
}
impl Span {
    #[inline]
    pub fn process(phase: Phase) -> Self { Self::new(phase, true, true) }
    #[inline]
    pub fn worker(phase: Phase) -> Self { Self::new(phase, false, true) }
    #[inline]
    pub fn sampled(phase: Phase, active: bool) -> Self { Self::new(phase, false, active) }
    #[inline]
    fn new(phase: Phase, process: bool, active: bool) -> Self {
        #[cfg(feature = "phase-profile")]
        { Self { state: active.then(|| (phase, process, enabled::wall(), enabled::stamp(phase, process))) } }
        #[cfg(not(feature = "phase-profile"))]
        { let _ = (phase, process, active); Self {} }
    }
}
impl Drop for Span {
    #[inline]
    fn drop(&mut self) {
        #[cfg(feature = "phase-profile")]
        if let Some((phase,process,wall,cpu)) = self.state {
            let end_cpu = enabled::stamp(phase, process);
            let end_wall = enabled::wall();
            enabled::record(phase, wall, end_wall, end_cpu-cpu);
        }
    }
}

#[inline]
pub fn sample() -> bool {
    #[cfg(feature = "phase-profile")]
    { enabled::sample() }
    #[cfg(not(feature = "phase-profile"))]
    { false }
}

pub fn report() {
    #[cfg(feature = "phase-profile")]
    enabled::report();
}

#[cfg(feature = "phase-profile")]
mod enabled {
    use super::Phase;
    use std::{cell::Cell, sync::{LazyLock, atomic::{AtomicU64, Ordering::Relaxed}}, time::Instant};
    const COUNT: usize = 19;
    const NAMES: [&str;COUNT] = ["startup", "file_mmap", "links_union_find", "catalogs", "event_parse_validate", "event_dedup_partition", "click_parse_validate_partition", "bucket_copy", "event_sort", "click_sort", "person_group_discovery", "person_fused", "campaign_reduce", "parquet_encode_write", "exit_unmap", "sessionize_sample", "credit_assists_sample", "aggregation_sample", "parquet_write_only"];
    static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);
    struct Record { calls: AtomicU64, wall: AtomicU64, cpu: AtomicU64, first: AtomicU64, last: AtomicU64 }
    impl Record {
        const fn new() -> Self { Self { calls: AtomicU64::new(0), wall: AtomicU64::new(0), cpu: AtomicU64::new(0), first: AtomicU64::new(u64::MAX), last: AtomicU64::new(0) } }
    }
    static RECORDS: [Record;COUNT] = [const { Record::new() };COUNT];
    thread_local! { static SAMPLES: Cell<u64> = const { Cell::new(0) }; }
    pub fn sample() -> bool { SAMPLES.with(|value| { let n = value.get(); value.set(n+1); n & 127 == 0 }) }
    #[repr(C)]
    struct Timespec { sec: i64, nsec: i64 }
    unsafe extern "C" { fn clock_gettime(clock: i32, value: *mut Timespec) -> i32; }
    pub fn cpu(process: bool) -> u64 {
        let mut value = Timespec { sec: 0, nsec: 0 };
        // Linux CLOCK_PROCESS_CPUTIME_ID=2, CLOCK_THREAD_CPUTIME_ID=3.
        let result = unsafe { clock_gettime(if process { 2 } else { 3 }, &mut value) };
        assert_eq!(result, 0);
        value.sec as u64 * 1_000_000_000 + value.nsec as u64
    }
    #[inline]
    pub fn stamp(phase: Phase, process: bool) -> u64 {
        if matches!(phase, Phase::SessionSample|Phase::AttributionSample|Phase::AggregationSample) { ticks() } else { cpu(process) }
    }
    #[inline]
    fn ticks() -> u64 {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_lfence();
            let value = std::arch::x86_64::_rdtsc();
            std::arch::x86_64::_mm_lfence();
            value
        }
        #[cfg(not(target_arch = "x86_64"))]
        { wall() }
    }
    pub fn wall() -> u64 { EPOCH.elapsed().as_nanos() as u64 }
    pub fn record(phase: Phase, first: u64, last: u64, cpu: u64) {
        let record = &RECORDS[phase as usize];
        record.calls.fetch_add(1, Relaxed); record.wall.fetch_add(last-first, Relaxed); record.cpu.fetch_add(cpu, Relaxed);
        record.first.fetch_min(first, Relaxed); record.last.fetch_max(last, Relaxed);
    }
    pub fn report() {
        // Sub-microsecond session work is below syscall-clock resolution. Use the
        // invariant TSC only for sampled relative weights; outer CPU clocks stay exact.
        let frequency_start = Instant::now();
        let ticks_start = ticks();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ticks_per_second = (ticks()-ticks_start) as f64 / frequency_start.elapsed().as_secs_f64();
        let mut overhead_ticks = 0;
        for _ in 0..10000 { let a = ticks(); let b = ticks(); overhead_ticks += b-a; }
        let overhead_ticks = overhead_ticks / 10000;
        eprintln!("PROFILE sample_rate=128 empty_probe_ticks={overhead_ticks} ticks_per_second={ticks_per_second:.0}");
        for (i,record) in RECORDS.iter().enumerate() {
            let calls = record.calls.load(Relaxed);
            if calls == 0 { continue; }
            let cpu = record.cpu.load(Relaxed);
            let work_wall = record.wall.load(Relaxed);
            let envelope = record.last.load(Relaxed)-record.first.load(Relaxed);
            if (Phase::SessionSample as usize..=Phase::AggregationSample as usize).contains(&i) {
                let estimated_work = cpu.saturating_sub(calls*overhead_ticks) as f64 * 128.0 / ticks_per_second;
                eprintln!("PROFILE sampled_phase={} estimated_worker_wall_sum={estimated_work:.6} calls={calls}", NAMES[i]);
                continue;
            }
            let wall = if (i <= Phase::ClickParse as usize && i != Phase::EventParse as usize && i != Phase::Links as usize) || i == Phase::Teardown as usize { work_wall } else { envelope };
            eprintln!("PROFILE phase={} wall={:.6} work_wall={:.6} cpu={:.6} avg_threads={:.3} calls={calls}", NAMES[i], wall as f64/1e9, work_wall as f64/1e9, cpu as f64/1e9, cpu as f64/wall.max(1) as f64);
        }
    }
}
