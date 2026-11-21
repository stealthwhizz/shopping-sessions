mod input;
mod parquet;
mod profile;
mod keys;
mod scan;
mod records;

use input::{DAY, WEEK, Links, Map, body, chunks, csv, lines, map_dir, map_file, price};
use scan::timestamp;
use parquet::Column;
use profile::{Phase, Span};
use keys::Key;
use memmap2::Mmap;
use rayon::prelude::*;
use std::{env, path::Path, sync::atomic::{AtomicBool, Ordering::Relaxed}, time::Instant};

use records::{Compact, Layout, Wide, INVALID};
/// Raw event copies are routed into this many event_id-hash buckets at parse time so each
/// bucket's dedup (about 20K rows at 20M events) works in L2. At most 2^16 (u16 bucket ids).
const DEDUP_BITS: u32 = 10;
const DEDUP_BUCKETS: usize = 1 << DEDUP_BITS;
const _: () = assert!(DEDUP_BITS <= 16 && DEDUP_BUCKETS % DEDUP_TASKS == 0);
/// Dedup tasks, each taking DEDUP_BUCKETS/DEDUP_TASKS consecutive buckets.
const DEDUP_TASKS: usize = 128;
/// Person buckets: 128 keeps a bucket's events (~2.4 MB at 20M) near L2 for the sorts and the
/// largest bucket at ~2% of rows (mostly its whale person), for even scheduling.
const USER_BITS: u32 = 7;
const USER_BUCKETS: usize = 1 << USER_BITS;

struct Catalog<'a> {
    products: Map<&'a [u8], u32>,
    categories: Vec<&'a [u8]>,
    campaign_ids: Map<&'a [u8], u32>,
    campaigns: Vec<(&'a [u8], &'a [u8])>,
}
impl<'a> Catalog<'a> {
    fn new(products: &'a [u8], campaigns: &'a [u8]) -> Self {
        let mut categories = vec![&b"unknown"[..]];
        let mut category_ids = Map::default();
        category_ids.insert(&b"unknown"[..], 0);
        let mut product_map = Map::default();
        for line in lines(body(products)) {
            let Some([product, category]) = csv::<2>(line) else { continue };
            let id = *category_ids.entry(category).or_insert_with(|| {
                let id = categories.len() as u32; categories.push(category); id
            });
            product_map.insert(product, id);
        }
        let mut campaign_ids = Map::default();
        let mut campaign_rows = vec![(&b"organic"[..], &b"organic"[..])];
        for line in lines(body(campaigns)) {
            let Some([id, channel]) = csv::<2>(line) else { continue };
            campaign_ids.insert(id, campaign_rows.len() as u32);
            campaign_rows.push((id, channel));
        }
        Self { products: product_map, categories, campaign_ids, campaigns: campaign_rows }
    }
}

#[derive(Clone, Copy)]
struct RawEvent<K, L: Layout> {
    id: K,
    user: K,
    ingest: i64,
    data: L::Data,
}
#[derive(Clone, Copy)]
struct Event<K, L: Layout> { user: K, data: L::Data }
#[derive(Clone, Copy)]
struct Click<K, L: Layout> { user: K, id: K, ts: L::Time, campaign: u32 }

/// One chunk's raw event copies grouped by dedup bucket: bucket `b` is `rows[starts[b]..starts[b+1]]`.
struct Routed<K, L: Layout> { rows: Vec<RawEvent<K,L>>, starts: Vec<u32> }
impl<K, L: Layout> Routed<K,L> {
    fn bucket(&self, b: usize) -> &[RawEvent<K,L>] { &self.rows[self.starts[b] as usize..self.starts[b+1] as usize] }
}

/// Parses into `scratch` (parse order plus each row's bucket), then counts-then-scatters into
/// an exactly sized bucket-grouped copy. `scratch` is reused across chunks, so the scatter
/// reads cache-hot rows and each routed row is written once.
fn parse_events<'a, K: Key<'a>, L: Layout>(bytes: &'a [u8], catalog: &Catalog<'_>, start: i64, end: i64, origin: i64, unsupported: &AtomicBool,
    scratch: &mut (Vec<RawEvent<K,L>>, Vec<u16>)) -> Routed<K,L> {
    let _phase = Span::worker(Phase::EventParse);
    let (parsed, buckets) = scratch;
    parsed.clear(); buckets.clear();
    let mut starts = vec![0u32; DEDUP_BUCKETS+1];
    scan::for_each_line::<7>(bytes, |f| {
        let Some(f) = f else { return };
        if f[0].is_empty() { return; }
        let (Some(id),Some(user)) = (K::parse(f[0]),K::parse(f[1])) else {
            unsupported.store(true, Relaxed); return;
        };
        let Some(ingest) = timestamp(f[3]) else { return };
        let mut ts = timestamp(f[2]).unwrap_or(INVALID);
        let mut revenue = 0;
        let mut category = 0;
        if f[1].is_empty() || f[5].is_empty() || ts < start || ts >= end {
            ts = INVALID;
        } else {
            match f[4] {
                b"view" | b"cart" => {}
                b"purchase" => {
                    if let Some(p) = price(f[6]) {
                        revenue = p;
                        category = catalog.products.get(f[5]).copied().unwrap_or(0);
                    } else { ts = INVALID; }
                }
                _ => ts = INVALID,
            }
            if ts != INVALID && ingest - ts > DAY { ts = INVALID; }
        }
        // Invalid earliest copies remain in dedup, so a later valid row cannot rescue them.
        let bucket = (id.fingerprint() >> (64-DEDUP_BITS)) as usize;
        starts[bucket+1] += 1;
        parsed.push(RawEvent { id, user, ingest, data: L::event(ts, revenue, category, origin) });
        buckets.push(bucket as u16);
    });
    for b in 0..DEDUP_BUCKETS { starts[b+1] += starts[b]; }
    let mut cursor = starts[..DEDUP_BUCKETS].to_vec();
    let mut rows = Vec::<RawEvent<K,L>>::with_capacity(parsed.len());
    let out = rows.as_mut_ptr();
    for (row, &bucket) in parsed.iter().zip(buckets.iter()) {
        let slot = &mut cursor[bucket as usize];
        // SAFETY: cursors start at the prefix sums of the exact per-bucket counts, so each
        // index in 0..parsed.len() is written exactly once before set_len.
        unsafe { out.add(*slot as usize).write(*row); }
        *slot += 1;
    }
    unsafe { rows.set_len(parsed.len()); }
    Routed { rows, starts }
}

fn parse_clicks<'a, K: Key<'a>, L: Layout>(bytes: &'a [u8], links: &Links<K>, catalog: &Catalog<'_>, start: i64, end: i64, origin: i64, unsupported: &AtomicBool) -> Vec<Click<K,L>> {
    let mut rows = Vec::with_capacity(bytes.len()/45);
    scan::for_each_line::<4>(bytes, |f| {
        let Some([id,user,campaign,ts]) = f else { return };
        if id.is_empty() || user.is_empty() { return; }
        let (Some(id),Some(user)) = (K::parse(id),K::parse(user)) else {
            unsupported.store(true, Relaxed); return;
        };
        let Some(ts) = timestamp(ts).filter(|&ts| ts >= start-WEEK && ts < end) else { return };
        let Some(&campaign) = catalog.campaign_ids.get(campaign) else { return };
        // Click duplicates have identical values: repeating one changes neither the
        // winning click nor the per-campaign sliding-window counts' zero/nonzero state.
        rows.push(Click { user, id, ts: L::click(ts, origin), campaign });
    });
    resolve(&mut rows, links, |row| &mut row.user);
    rows
}

fn resolve<'a, K: Key<'a>, T>(rows: &mut [T], links: &Links<K>, user: impl Fn(&mut T) -> &mut K) {
    for i in 0..rows.len() {
        if i+16 < rows.len() { links.prefetch(*user(&mut rows[i+16])); }
        let id = user(&mut rows[i]);
        *id = links.name(*id);
    }
}

fn partition<T>(rows: Vec<T>, key: impl Fn(&T) -> u64) -> Vec<Vec<T>> {
    let mut counts = [0; USER_BUCKETS];
    for row in &rows { counts[(key(row) >> (64-USER_BITS)) as usize] += 1; }
    let mut buckets: Vec<Vec<T>> = counts.into_iter().map(Vec::with_capacity).collect();
    for row in rows { buckets[(key(&row) >> (64-USER_BITS)) as usize].push(row); }
    buckets
}

fn transpose<T>(parts: Vec<Vec<Vec<T>>>, count: usize) -> Vec<Vec<Vec<T>>> {
    let mut buckets: Vec<Vec<Vec<T>>> = (0..count).map(|_| Vec::with_capacity(parts.len())).collect();
    for part in parts {
        for (bucket, rows) in buckets.iter_mut().zip(part) { bucket.push(rows); }
    }
    buckets
}

struct DedupScratch<K, L: Layout> { rows: Vec<RawEvent<K,L>>, hashes: Vec<u32>, seen: Vec<u64>, repeated: Vec<u64>, suspects: Vec<u32>, table: Vec<u32> }
impl<K, L: Layout> Default for DedupScratch<K,L> {
    fn default() -> Self { Self { rows: Vec::new(), hashes: Vec::new(), seen: Vec::new(), repeated: Vec::new(), suspects: Vec::new(), table: Vec::new() } }
}

/// Dedups each bucket in `buckets` across every chunk's copies. A bucket's rows are gathered
/// into `s` (L2-sized) while a bitmap of 16 bits per row marks hash bits seen more than once.
/// A row whose bit was seen once is the only copy of its event_id and is kept directly; only
/// the suspects (true duplicates plus ~6% bit collisions) go through the exact index table
/// that keeps the copy with the smallest ingest_ts.
fn dedup<'a, K: Key<'a>, L: Layout>(parts: &[Routed<K,L>], buckets: std::ops::Range<usize>, links: &Links<K>, s: &mut DedupScratch<K,L>) -> Vec<Event<K,L>> {
    let count: usize = parts.iter().map(|p| (p.starts[buckets.end]-p.starts[buckets.start]) as usize).sum();
    let mut events = Vec::with_capacity(count);
    let mut emit = |row: &RawEvent<K,L>| if L::valid(row.data) { events.push(Event { user: row.user, data: row.data }) };
    for b in buckets {
        s.rows.clear(); s.hashes.clear(); s.suspects.clear();
        let n: usize = parts.iter().map(|p| p.bucket(b).len()).sum();
        let mask = (n*16).max(64).next_power_of_two()-1;
        s.seen.clear(); s.seen.resize((mask+1)/64, 0);
        s.repeated.clear(); s.repeated.resize((mask+1)/64, 0);
        for part in parts {
            for row in part.bucket(b) {
                let hash = row.id.fingerprint() as u32;
                let bit = hash as usize & mask;
                let (word, flag) = (bit >> 6, 1u64 << (bit & 63));
                s.repeated[word] |= s.seen[word] & flag;
                s.seen[word] |= flag;
                s.rows.push(*row); s.hashes.push(hash);
            }
        }
        for (i, (row, &hash)) in s.rows.iter().zip(&s.hashes).enumerate() {
            let bit = hash as usize & mask;
            if s.repeated[bit >> 6] & (1 << (bit & 63)) == 0 { emit(row); } else { s.suspects.push(i as u32); }
        }
        let capacity = (s.suspects.len()*2).max(2).next_power_of_two();
        s.table.clear(); s.table.resize(capacity, u32::MAX);
        for &i in &s.suspects {
            let (row, hash) = (&s.rows[i as usize], s.hashes[i as usize]);
            // High hash bits: suspects share their low (bitmap) bits with their colliders.
            let mut slot = hash.rotate_left(13) as usize & (capacity-1);
            loop {
                let old = s.table[slot];
                if old == u32::MAX { s.table[slot] = i; break; }
                let kept = &s.rows[old as usize];
                if s.hashes[old as usize] == hash && kept.id == row.id {
                    if row.ingest < kept.ingest { s.table[slot] = i; }
                    break;
                }
                slot = (slot+1) & (capacity-1);
            }
        }
        for &i in &s.table { if i != u32::MAX { emit(&s.rows[i as usize]); } }
    }
    resolve(&mut events, links, |row| &mut row.user);
    events
}

/// Dense per-bucket person index: an open-addressing table of indexes into `users`.
struct Grouper<K> { slots: Vec<u32>, users: Vec<K>, ids: Vec<u32> }
impl<K> Default for Grouper<K> {
    fn default() -> Self { Self { slots: Vec::new(), users: Vec::new(), ids: Vec::new() } }
}

/// `Ok(person index)` of `user`, or `Err(empty slot)` where it would go.
fn find_person<'a, K: Key<'a>>(slots: &[u32], users: &[K], user: K) -> Result<u32, usize> {
    let mask = slots.len()-1;
    let mut slot = user.fingerprint() as usize & mask;
    loop {
        match slots[slot] {
            u32::MAX => return Err(slot),
            i if users[i as usize] == user => return Ok(i),
            _ => slot = (slot+1) & mask,
        }
    }
}

/// Copies `rows` into one vector ordered by person (`ids[i]` is row i's person, u32::MAX drops
/// it) and returns it with each person's start offset (`persons+1` entries).
fn scatter_by_person<'r, T: Copy + 'r>(rows: impl Iterator<Item=&'r T>, ids: &[u32], persons: usize) -> (Vec<T>, Vec<u32>) {
    let mut starts = vec![0u32; persons+1];
    for &i in ids { if i != u32::MAX { starts[i as usize+1] += 1; } }
    for p in 0..persons { starts[p+1] += starts[p]; }
    let total = starts[persons] as usize;
    let mut cursor = starts[..persons].to_vec();
    let mut out = Vec::<T>::with_capacity(total);
    let dst = out.as_mut_ptr();
    for (row, &i) in rows.zip(ids) {
        if i == u32::MAX { continue; }
        let slot = &mut cursor[i as usize];
        // SAFETY: cursors start at the prefix sums of the exact per-person counts, so each index
        // in 0..total is written exactly once before set_len.
        unsafe { dst.add(*slot as usize).write(*row); }
        *slot += 1;
    }
    unsafe { out.set_len(total); }
    (out, starts)
}

/// One bucket's rows grouped by exact person: person p's events are
/// `events[event_starts[p]..event_starts[p+1]]` sorted by ts, and its clicks likewise sorted by
/// ts, then click ID descending (so within equal timestamps the smallest ID is last and wins).
struct Grouped<K, L: Layout> { events: Vec<Event<K,L>>, clicks: Vec<Click<K,L>>, event_starts: Vec<u32>, click_starts: Vec<u32> }

/// Groups without sorting the whole bucket: each distinct user gets a dense index, rows are
/// counted and scattered per person, and only each person's few rows are sorted. Clicks of users
/// with no event in the bucket are dropped; they cannot credit a session.
fn group<'a, K: Key<'a>, L: Layout>(g: &mut Grouper<K>, events: Vec<Vec<Event<K,L>>>, clicks: Vec<Vec<Click<K,L>>>) -> Grouped<K,L> {
    let phase = Span::worker(Phase::Grouping);
    let n: usize = events.iter().map(Vec::len).sum();
    let Grouper { slots, users, ids } = g;
    slots.clear(); slots.resize((n/8).max(64).next_power_of_two(), u32::MAX);
    users.clear(); ids.clear();
    for event in events.iter().flatten() {
        let i = match find_person(slots, users, event.user) {
            Ok(i) => i,
            Err(slot) => {
                let i = users.len() as u32;
                users.push(event.user);
                slots[slot] = i;
                if users.len()*2 > slots.len() {
                    let size = slots.len()*2;
                    slots.clear(); slots.resize(size, u32::MAX);
                    for (i, &user) in users.iter().enumerate() {
                        let slot = find_person(slots, users, user).unwrap_err();
                        slots[slot] = i as u32;
                    }
                }
                i
            }
        };
        ids.push(i);
    }
    ids.extend(clicks.iter().flatten().map(|click| find_person(slots, users, click.user).unwrap_or(u32::MAX)));
    drop(phase);
    let phase = Span::worker(Phase::BucketCopy);
    let event_count = n;
    let (mut grouped_events, event_starts) = scatter_by_person(events.iter().flatten(), &ids[..event_count], users.len());
    let (mut grouped_clicks, click_starts) = scatter_by_person(clicks.iter().flatten(), &ids[event_count..], users.len());
    drop((events, clicks));
    drop(phase);
    let phase = Span::worker(Phase::EventSort);
    for p in event_starts.windows(2) {
        grouped_events[p[0] as usize..p[1] as usize].sort_unstable_by_key(|event| L::time(event.data));
    }
    drop(phase);
    let phase = Span::worker(Phase::ClickSort);
    for p in click_starts.windows(2) {
        grouped_clicks[p[0] as usize..p[1] as usize].sort_unstable_by(|a,b| a.ts.cmp(&b.ts).then_with(|| b.id.cmp(&a.id)));
    }
    drop(phase);
    Grouped { events: grouped_events, clicks: grouped_clicks, event_starts, click_starts }
}

struct UserRow<K> { user: K, day: i32, sessions: i64, events: i64, revenue: i64, category: Option<u32> }
#[derive(Default, Clone, Copy)]
struct CampaignRow { sessions: i64, purchasing: i64, revenue: i64, assists: i64 }
impl CampaignRow {
    fn add(&mut self, other: Self) {
        self.sessions += other.sessions; self.purchasing += other.purchasing;
        self.revenue += other.revenue; self.assists += other.assists;
    }
}
struct Aggregate<K> {
    users: Vec<UserRow<K>>,
    campaigns: Vec<CampaignRow>,
    first_day: i32,
    days: usize,
}
impl<K> Aggregate<K> {
    fn new(campaigns: usize, first_day: i32, days: usize) -> Self {
        Self { users: Vec::new(), campaigns: vec![CampaignRow::default(); campaigns*days], first_day, days }
    }
    fn campaign(&mut self, campaign: u32, day: i32) -> &mut CampaignRow {
        &mut self.campaigns[campaign as usize*self.days + (day-self.first_day) as usize]
    }
}

fn top_category(totals: &[(u32,i64)], names: &[&[u8]]) -> Option<u32> {
    totals.iter().max_by(|a,b| a.1.cmp(&b.1).then_with(|| names[b.0 as usize].cmp(names[a.0 as usize]))).map(|v| v.0)
}

fn person<K: Copy+Ord, L: Layout>(agg: &mut Aggregate<K>, events: &[Event<K,L>], clicks: &[Click<K,L>], categories: &[&[u8]], origin: i64) {
    let mut active = Map::<u32,u32>::default();
    let (mut left, mut right, mut first) = (0,0,0);
    let mut daily: Option<UserRow<K>> = None;
    let mut category_totals = Vec::<(u32,i64)>::new();
    let mut sampled = false;
    while first < events.len() {
        sampled = profile::sample();
        let phase = Span::sampled(Phase::SessionSample, sampled);
        let start = L::units(L::time(events[first].data));
        let day = L::epoch(start, origin).div_euclid(DAY) as i32;
        let mut last = first+1;
        while last < events.len() && L::units(L::time(events[last].data))-L::units(L::time(events[last-1].data)) <= 1_800_000 { last += 1; }
        drop(phase);
        let phase = Span::sampled(Phase::AggregationSample, sampled);
        if daily.as_ref().is_some_and(|row| row.day != day) {
            let mut row = daily.take().unwrap();
            row.category = top_category(&category_totals, categories);
            agg.users.push(row); category_totals.clear();
        }
        let row = daily.get_or_insert_with(|| UserRow { user: events[first].user, day, sessions: 0, events: 0, revenue: 0, category: None });
        let mut revenue = 0i64;
        for event in &events[first..last] {
            let amount = L::revenue(event.data);
            let category = L::category(event.data);
            revenue += i64::from(amount);
            if amount > 0 {
                if let Some(total) = category_totals.iter_mut().find(|v| v.0 == category) { total.1 += i64::from(amount); }
                else { category_totals.push((category, i64::from(amount))); }
            }
        }
        row.sessions += 1; row.events += (last-first) as i64; row.revenue += revenue;
        drop(phase);
        let phase = Span::sampled(Phase::AttributionSample, sampled);
        while right < clicks.len() && L::units(clicks[right].ts) <= start {
            *active.entry(clicks[right].campaign).or_default() += 1;
            right += 1;
        }
        while left < right && L::units(clicks[left].ts) < start-WEEK {
            let campaign = clicks[left].campaign;
            let count = active.get_mut(&campaign).unwrap(); *count -= 1;
            if *count == 0 { active.remove(&campaign); }
            left += 1;
        }
        let campaign = if left < right { clicks[right-1].campaign } else { 0 };
        let credited = agg.campaign(campaign, day);
        credited.sessions += 1; credited.purchasing += i64::from(revenue > 0); credited.revenue += revenue;
        for &campaign in active.keys() { agg.campaign(campaign, day).assists += 1; }
        drop(phase);
        first = last;
    }
    let _phase = Span::sampled(Phase::AggregationSample, sampled);
    if let Some(mut row) = daily {
        row.category = top_category(&category_totals, categories); agg.users.push(row);
    }
}

fn output_users<'a, K: Key<'a>>(path: &Path, part: usize, rows: &[UserRow<K>], catalog: &Catalog<'_>) -> std::io::Result<()> {
    let _phase = Span::worker(Phase::Parquet);
    let mut names = Vec::with_capacity(rows.len()*13);
    for row in rows { row.user.append_plain(&mut names); }
    parquet::write(&path.join("user_daily"), part, rows.len(), vec![
        Column::utf8_plain("user_id", names),
        Column::date("day", rows.iter().map(|r| r.day)),
        Column::integer("sessions", rows.iter().map(|r| r.sessions)),
        Column::integer("events", rows.iter().map(|r| r.events)),
        Column::integer("revenue_cents", rows.iter().map(|r| r.revenue)),
        Column::string("top_category", true, rows.iter().map(|r| r.category.map(|c| catalog.categories[c as usize]))),
    ])
}

fn output_campaigns(path: &Path, totals: Vec<CampaignRow>, first_day: i32, days: usize, catalog: &Catalog<'_>) -> std::io::Result<()> {
    let campaigns: Vec<_> = totals.into_iter().enumerate()
        .filter(|(_,row)| row.sessions != 0 || row.assists != 0)
        .map(|(i,row)| (((i/days) as u32, first_day+(i%days) as i32), row)).collect();
    parquet::write(&path.join("campaign_daily"), 0, campaigns.len(), vec![
        Column::string("campaign_id", false, campaigns.iter().map(|r| Some(catalog.campaigns[r.0.0 as usize].0))),
        Column::string("channel", false, campaigns.iter().map(|r| Some(catalog.campaigns[r.0.0 as usize].1))),
        Column::date("day", campaigns.iter().map(|r| r.0.1)),
        Column::integer("sessions", campaigns.iter().map(|r| r.1.sessions)),
        Column::integer("purchasing_sessions", campaigns.iter().map(|r| r.1.purchasing)),
        Column::integer("revenue_cents", campaigns.iter().map(|r| r.1.revenue)),
        Column::integer("assisted_sessions", campaigns.iter().map(|r| r.1.assists)),
    ])
}

fn pipeline<'a, K: Key<'a>, L: Layout>(link_maps: &'a [Mmap], event_maps: &'a [Mmap], click_maps: &'a [Mmap], catalog: &Catalog<'_>, start: i64, end: i64, out: &Path) -> std::io::Result<bool> {
    let clock = Instant::now();
    let first_day = start.div_euclid(DAY) as i32;
    let days = ((end-1).div_euclid(DAY)-i64::from(first_day)+1).max(0) as usize;
    let origin = start-WEEK;
    let unsupported = AtomicBool::new(false);
    let (links, parts) = rayon::join(
        || {
            let _phase = Span::worker(Phase::Links);
            Links::<K>::new(link_maps, &unsupported)
        },
        || chunks(event_maps).par_iter().map_init(|| (Vec::new(), Vec::new()), |scratch, bytes| {
            parse_events::<K,L>(bytes, catalog, start, end, origin, &unsupported, scratch)
        }).collect::<Vec<_>>(),
    );
    if unsupported.load(Relaxed) { return Ok(false); }
    eprintln!("parse {:.3}s", clock.elapsed().as_secs_f64());
    let phase = Span::process(Phase::Dedup);
    let per_task = DEDUP_BUCKETS/DEDUP_TASKS;
    let events = transpose((0..DEDUP_TASKS).into_par_iter().map_init(DedupScratch::default, |scratch, task| {
        partition(dedup(&parts, task*per_task..(task+1)*per_task, &links, scratch), |event| event.user.fingerprint())
    }).collect(), USER_BUCKETS);
    drop(parts);
    drop(phase);
    eprintln!("dedup {:.3}s", clock.elapsed().as_secs_f64());
    let phase = Span::process(Phase::ClickParse);
    let clicks = transpose(chunks(&click_maps).par_iter().map(|bytes| {
        partition(parse_clicks::<K,L>(bytes, &links, catalog, start, end, origin, &unsupported), |click| click.user.fingerprint())
    }).collect(), USER_BUCKETS);
    drop(phase);
    if unsupported.load(Relaxed) { return Ok(false); }
    // Buckets run largest first (the ones holding whale persons are up to 1.4x the mean), pulled
    // from a shared counter by one worker per thread, so the stage ends on small buckets. Each
    // worker sums campaign totals in its own array instead of one per bucket.
    let mut order: Vec<usize> = (0..events.len()).collect();
    let size = |b: usize| events[b].iter().map(Vec::len).sum::<usize>() + clicks[b].iter().map(Vec::len).sum::<usize>();
    order.sort_by_cached_key(|&b| std::cmp::Reverse(size(b)));
    let tasks: Vec<_> = events.into_iter().zip(clicks).map(|task| std::sync::Mutex::new(Some(task))).collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let campaign_parts = (0..rayon::current_num_threads()).into_par_iter().map(|_| {
        let mut agg = Aggregate::new(catalog.campaigns.len(), first_day, days);
        let mut grouper = Grouper::default();
        while let Some(&part) = order.get(next.fetch_add(1, Relaxed)) {
            let (events, clicks) = tasks[part].lock().unwrap().take().unwrap();
            let grouped = group(&mut grouper, events, clicks);
            let phase = Span::worker(Phase::PersonFused);
            agg.users.clear();
            for p in 0..grouped.event_starts.len()-1 {
                let (events, clicks) = (grouped.event_starts[p] as usize..grouped.event_starts[p+1] as usize, grouped.click_starts[p] as usize..grouped.click_starts[p+1] as usize);
                person(&mut agg, &grouped.events[events], &grouped.clicks[clicks], &catalog.categories, origin);
            }
            drop(phase);
            output_users(out, part, &agg.users, catalog)?;
        }
        Ok(agg.campaigns)
    }).collect::<std::io::Result<Vec<_>>>()?;
    let campaign_totals = campaign_parts.into_par_iter().reduce_with(|mut a, b| {
        let phase = Span::worker(Phase::CampaignMerge);
        for (a,b) in a.iter_mut().zip(b) { a.add(b); }
        drop(phase);
        a
    }).unwrap();
    eprintln!("aggregate {:.3}s", clock.elapsed().as_secs_f64());
    let phase = Span::process(Phase::Parquet);
    output_campaigns(out, campaign_totals, first_day, days, catalog)?;
    drop(phase);
    Ok(true)
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let clock = Instant::now();
    let phase = Span::process(Phase::Startup);
    let args: Vec<_> = env::args_os().skip(1).collect();
    let data = env::var_os("DATA_DIR").or_else(|| args.first().cloned()).ok_or("DATA_DIR or input argument required")?;
    let out = env::var_os("OUTPUT_DIR").or_else(|| args.get(1).cloned()).ok_or("OUTPUT_DIR or output argument required")?;
    let string_mode = match env::var_os("SHOPPING_PATH") {
        None => false,
        Some(value) if value == "packed" => false,
        Some(value) if value == "string" => true,
        _ => return Err("SHOPPING_PATH must be packed or string".into()),
    };
    let wide_mode = match env::var_os("SHOPPING_LAYOUT") {
        None => false,
        Some(value) if value == "compact" => false,
        Some(value) if value == "wide" => true,
        _ => return Err("SHOPPING_LAYOUT must be compact or wide".into()),
    };
    let threads = env::var("NCPU").ok().and_then(|s| s.parse::<usize>().ok()).filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from));
    rayon::ThreadPoolBuilder::new().num_threads(threads).build_global()?;
    drop(phase);
    let phase = Span::process(Phase::Mmap);
    let data = Path::new(&data);
    let (start,end) = input::window(&data.join("manifest.json"))?;
    let link_maps = map_dir(&data.join("links"))?;
    drop(phase);
    let phase = Span::process(Phase::Mmap);
    let products = map_file(&data.join("products.csv"))?;
    let campaigns = map_file(&data.join("campaigns.csv"))?;
    drop(phase);
    let phase = Span::process(Phase::Catalog);
    let catalog = Catalog::new(&products, &campaigns);
    drop(phase);
    let phase = Span::process(Phase::Mmap);
    let event_maps = map_dir(&data.join("events"))?;
    let click_maps = map_dir(&data.join("clicks"))?;
    drop(phase);
    if string_mode {
        pipeline::<&[u8],Wide>(&link_maps, &event_maps, &click_maps, &catalog, start, end, Path::new(&out))?;
        eprintln!("path=string reason=forced layout=wide");
    } else {
        let packed = if !wide_mode && Compact::eligible(start, end, catalog.categories.len()) {
            eprintln!("layout=compact");
            pipeline::<u64,Compact>(&link_maps, &event_maps, &click_maps, &catalog, start, end, Path::new(&out))?
        } else {
            eprintln!("layout=wide");
            pipeline::<u64,Wide>(&link_maps, &event_maps, &click_maps, &catalog, start, end, Path::new(&out))?
        };
        if packed { eprintln!("path=packed"); }
        else {
            eprintln!("path=string reason=unsupported-id layout=wide");
            pipeline::<&[u8],Wide>(&link_maps, &event_maps, &click_maps, &catalog, start, end, Path::new(&out))?;
        }
    }
    eprintln!("total {:.3}s", clock.elapsed().as_secs_f64());
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        if let Some(peak) = status.lines().find(|line| line.starts_with("VmHWM:")) { eprintln!("{peak}"); }
    }
    {
        let phase = Span::process(Phase::Teardown);
        drop(click_maps); drop(event_maps); drop(catalog); drop(campaigns);
        drop(products); drop(link_maps);
        drop(phase);
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() { eprintln!("shopping-sessions: {error}"); std::process::exit(1); }
    profile::report();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_record_widths_reduce_memory_traffic() {
        assert_eq!(std::mem::size_of::<RawEvent<u64,Compact>>(), 32);
        assert_eq!(std::mem::size_of::<Event<u64,Compact>>(), 16);
        assert_eq!(std::mem::size_of::<Click<u64,Compact>>(), 24);
    }
    #[test]
    fn dedup_keeps_invalid_earliest_and_distinguishes_hash_collisions() {
        #[derive(Clone,Copy,Eq,PartialEq,Ord,PartialOrd,Hash)]
        struct Collision<'a>(&'a [u8]);
        impl<'a> Key<'a> for Collision<'a> {
            fn parse(bytes: &'a [u8]) -> Option<Self> { Some(Self(bytes)) }
            fn fingerprint(self) -> u64 { 7 }
            fn append_plain(self, data: &mut Vec<u8>) { self.0.append_plain(data); }
        }
        let make = |id, ingest, ts| RawEvent::<_,Wide> { id: Collision(id), user: Collision(b"u"), ingest, data: Wide::event(ts, 0, 0, 0) };
        let links = Links::new(&[], &AtomicBool::new(false));
        // Fingerprint 7 routes every copy to bucket 0.
        let routed = |rows: Vec<_>| { let mut starts = vec![rows.len() as u32; DEDUP_BUCKETS+1]; starts[0] = 0; Routed { rows, starts } };
        let parts = [routed(vec![make(&b"a"[..], 20, 100), make(&b"b"[..], 30, 200)]), routed(vec![make(&b"a"[..], 10, INVALID)])];
        let result = dedup(&parts, 0..1, &links, &mut DedupScratch::default());
        assert_eq!(result.len(), 1);
        assert_eq!(Wide::time(result[0].data), 200);
    }
    #[test]
    fn dedup_matches_smallest_ingest_reference_across_chunks_and_buckets() {
        let catalog = Catalog::new(b"product_id,category\np,category\n", b"campaign_id,channel\n");
        let (start, end) = (1, 1001);
        let mut state = 7u64;
        let mut text = Vec::new();
        let mut expected = std::collections::BTreeMap::new();
        for _ in 0..20_000 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let id = (state >> 33) % 6000;
            let ingest = ((state >> 20) % 1000) as i64;
            let ts = ingest - ((state >> 50) % 2) as i64;
            text.extend_from_slice(format!("e{id},u{},{ts},{ingest},view,p,\n", ingest % 3).as_bytes());
            let best = expected.entry(id).or_insert((ingest, ts));
            if ingest < best.0 { *best = (ingest, ts); }
        }
        let (mut parts, mut scratch, mut rest) = (Vec::new(), (Vec::new(), Vec::new()), &text[..]);
        while !rest.is_empty() {
            let cut = rest.len().min(30_000);
            let end_of_line = rest[cut..].iter().position(|&b| b == b'\n').map_or(rest.len(), |p| cut+p+1);
            parts.push(parse_events::<u64,Wide>(&rest[..end_of_line], &catalog, start, end, 0, &AtomicBool::new(false), &mut scratch));
            rest = &rest[end_of_line..];
        }
        assert!(parts.len() > 3);
        let links = Links::new(&[], &AtomicBool::new(false));
        let mut kept: Vec<_> = dedup(&parts, 0..DEDUP_BUCKETS, &links, &mut DedupScratch::default()).iter().map(|e| Wide::time(e.data)).collect();
        let mut reference: Vec<_> = expected.values().filter(|&&(_, ts)| ts >= start && ts < end).map(|&(_, ts)| ts).collect();
        kept.sort_unstable(); reference.sort_unstable();
        assert_eq!(kept, reference);
    }
    #[test]
    fn grouping_keeps_exact_persons_time_order_and_click_tie_break() {
        #[derive(Clone,Copy,Debug,Eq,PartialEq,Ord,PartialOrd,Hash)]
        struct Collision(u64);
        impl Key<'_> for Collision {
            fn parse(bytes: &[u8]) -> Option<Self> { crate::keys::pack(bytes).map(Self) }
            fn fingerprint(self) -> u64 { self.0 % 3 }
            fn append_plain(self, bytes: &mut Vec<u8>) { self.0.append_plain(bytes); }
        }
        // 300 users with colliding fingerprints force probing and several table growths.
        let event = |user: u64, ts| Event::<_,Wide> { user: Collision(user), data: Wide::event(ts, 0, 0, 0) };
        let click = |user: u64, id: u64, ts| Click::<_,Wide> { user: Collision(user), id: Collision(id), ts, campaign: id as u32 };
        let mut state = 9u64;
        let mut next = |n: u64| { state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (state >> 33) % n };
        let events: Vec<Vec<_>> = (0..4).map(|_| (0..500).map(|_| event(next(300), next(50) as i64)).collect()).collect();
        let clicks: Vec<Vec<_>> = (0..3).map(|_| (0..400).map(|_| click(next(400), next(20), next(50) as i64)).collect()).collect();
        let mut expected_events: Vec<_> = events.iter().flatten().map(|e| (e.user, Wide::time(e.data))).collect();
        let users: std::collections::BTreeSet<_> = expected_events.iter().map(|e| e.0).collect();
        let mut expected_clicks: Vec<_> = clicks.iter().flatten().filter(|c| users.contains(&c.user)).map(|c| (c.user, c.ts, std::cmp::Reverse(c.id))).collect();
        let mut grouper = Grouper::default();
        let grouped = group(&mut grouper, events, clicks);
        let persons = grouped.event_starts.len()-1;
        assert_eq!(persons, users.len());
        let (mut actual_events, mut actual_clicks) = (Vec::new(), Vec::new());
        for p in 0..persons {
            let rows = &grouped.events[grouped.event_starts[p] as usize..grouped.event_starts[p+1] as usize];
            let person = rows[0].user;
            assert!(rows.windows(2).all(|w| w[0].user == person && w[1].user == person && Wide::time(w[0].data) <= Wide::time(w[1].data)));
            actual_events.extend(rows.iter().map(|e| (e.user, Wide::time(e.data))));
            let rows = &grouped.clicks[grouped.click_starts[p] as usize..grouped.click_starts[p+1] as usize];
            assert!(rows.iter().all(|c| c.user == person));
            assert!(rows.windows(2).all(|w| (w[0].ts, std::cmp::Reverse(w[0].id)) <= (w[1].ts, std::cmp::Reverse(w[1].id))));
            actual_clicks.extend(rows.iter().map(|c| (c.user, c.ts, std::cmp::Reverse(c.id))));
        }
        expected_events.sort_unstable(); actual_events.sort_unstable();
        expected_clicks.sort_unstable(); actual_clicks.sort_unstable();
        assert_eq!(actual_events, expected_events);
        assert_eq!(actual_clicks, expected_clicks);
    }
    #[test]
    fn bad_ingest_drops_only_that_row() {
        let catalog = Catalog::new(b"product_id,category\np,category\n", b"campaign_id,channel\n");
        let rows = parse_events::<&[u8],Wide>(b"bad,u,1,invalid,view,p,\ngood,u,2,1970-01-01T00:00:00Z,view,p,\n", &catalog, 0, DAY, 0, &AtomicBool::new(false), &mut (Vec::new(), Vec::new()));
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].id, b"good");
    }
}
