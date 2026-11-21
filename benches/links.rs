#![allow(dead_code)]
#[path = "../src/input.rs"] mod input;
#[path = "../src/keys.rs"] mod keys;
#[path = "../src/profile.rs"] mod profile;
#[path = "../src/identity.rs"] mod identity;

use keys::Key;
use std::{path::Path, sync::atomic::{AtomicBool, Ordering::Relaxed}, time::Instant};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct Serial(u64);
impl Key<'_> for Serial {
    fn parse(bytes: &[u8]) -> Option<Self> { keys::pack(bytes).map(Self) }
    fn fingerprint(self) -> u64 { self.0.fingerprint() }
    fn append_plain(self, out: &mut Vec<u8>) { self.0.append_plain(out); }
}
impl input::LinkKey<'_> for Serial {}

#[test]
#[ignore = "requires DATA_DIR and benchmarks real link files"]
fn build_links() {
    let data = std::env::var("DATA_DIR").expect("DATA_DIR");
    let threads = std::env::var("NCPU").unwrap_or_else(|_| "8".into()).parse().unwrap();
    let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
    let maps = input::map_dir(&Path::new(&data).join("links")).unwrap();
    let unsupported = AtomicBool::new(false);
    for run in 1..=5 {
        let start = Instant::now();
        let sequential = pool.install(|| input::Links::<Serial>::new(&maps,&unsupported));
        eprintln!("links_impl=sequential run={run} wall={:.6}",start.elapsed().as_secs_f64());
        std::hint::black_box(&sequential);
        drop(sequential);
        let start = Instant::now();
        let parallel = pool.install(|| input::Links::<u64>::new(&maps,&unsupported));
        eprintln!("links_impl=parallel run={run} wall={:.6}",start.elapsed().as_secs_f64());
        std::hint::black_box(&parallel);
        drop(parallel);
    }
    assert!(!unsupported.load(Relaxed));
}
