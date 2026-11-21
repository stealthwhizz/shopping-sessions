mod input;
mod parquet;
mod profile;
mod keys;
mod scan;
mod records;
mod identity;

use input::{DAY, WEEK, LinkKey, Links, Map, body, chunks, csv, lines, map_dir, map_file, price};
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
const USER_BUCKETS: usize = 32;

struct Catalog<'a> {
    products: Products<'a>,
    categories: Vec<&'a [u8]>,
    campaign_ids: Map<&'a [u8], u32>,
    campaigns: Vec<(&'a [u8], &'a [u8])>,
}
impl<'a> Catalog<'a> {
    fn new(products: &'a [u8], campaigns: &'a [u8]) -> Self {
        let mut categories = vec![&b"unknown"[..]];
        let mut category_ids = Map::default();
        category_ids.insert(&b"unknown"[..], 0);
        let mut product_rows = Vec::new();
        for line in lines(body(products)) {
            let Some([product, category]) = csv::<2>(line) else { continue };
            let id = *category_ids.entry(category).or_insert_with(|| {
                let id = categories.len() as u32; categories.push(category); id
            });
            product_rows.push((product, id));
        }
        let mut campaign_ids = Map::default();
        let mut campaign_rows = vec![(&b"organic"[..], &b"organic"[..])];
        for line in lines(body(campaigns)) {
            let Some([id, channel]) = csv::<2>(line) else { continue };
            campaign_ids.insert(id, campaign_rows.len() as u32);
            campaign_rows.push((id, channel));
        }
        Self { products: Products::new(&product_rows), categories, campaign_ids, campaigns: campaign_rows }
    }
}

/// Exact product/category lookup; later dimension rows replace earlier ones.
struct Products<'a> { slots: Vec<(u64, u32)>, other: Map<&'a [u8], u32> }
impl<'a> Products<'a> {
    fn new(rows: &[(&'a [u8], u32)]) -> Self {
        let packable = rows.iter().filter(|row| keys::pack(row.0).is_some_and(|key| key != 0)).count();
        let mut slots = vec![(0u64, 0u32); (packable*2).max(2).next_power_of_two()];
        let mut other = Map::default();
        for &(product, category) in rows {
            // Zero is the empty slot marker, so the empty ID uses the exact byte map.
            match keys::pack(product) {
                Some(key) if key != 0 => {
                    let slot = Self::slot(&slots, key);
                    slots[slot] = (key, category);
                }
                _ => { other.insert(product, category); }
            }
        }
        Self { slots, other }
    }
    fn slot(slots: &[(u64, u32)], key: u64) -> usize {
        let mask = slots.len()-1;
        let mut slot = key.fingerprint() as usize & mask;
        while slots[slot].0 != 0 && slots[slot].0 != key { slot = (slot+1) & mask; }
        slot
    }
    fn get(&self, product: &[u8]) -> u32 {
        match keys::pack(product) {
            Some(key) if key != 0 => self.slots[Self::slot(&self.slots, key)].1,
            _ => self.other.get(product).copied().unwrap_or(0),
        }
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
                        category = catalog.products.get(f[5]);
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


fn sort_events<K: Copy+Ord, L: Layout>(events: &mut [Event<K,L>]) {
    events.sort_unstable_by_key(|event| (event.user, L::time(event.data)));
}

fn flatten<T>(parts: Vec<Vec<T>>) -> Vec<T> {
    let mut all = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for mut part in parts { all.append(&mut part); }
    all
}

fn partition<T>(rows: Vec<T>, key: impl Fn(&T) -> u64) -> Vec<Vec<T>> {
    let mut counts = [0; USER_BUCKETS];
    for row in &rows { counts[(key(row) >> 59) as usize] += 1; }
    let mut buckets: Vec<Vec<T>> = counts.into_iter().map(Vec::with_capacity).collect();
    for row in rows { buckets[(key(&row) >> 59) as usize].push(row); }
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

struct Work { events: std::ops::Range<usize>, clicks: std::ops::Range<usize> }
fn work<K: Copy+Ord, L: Layout>(events: &[Event<K,L>], clicks: &[Click<K,L>]) -> Vec<Work> {
    let mut result = Vec::new();
    let (mut e, mut c) = (0,0);
    while e < events.len() {
        let start = e;
        let person = &events[e];
        while e < events.len() && events[e].user == person.user { e += 1; }
        while c < clicks.len() && clicks[c].user < person.user { c += 1; }
        let first = c;
        while c < clicks.len() && clicks[c].user == person.user { c += 1; }
        result.push(Work { events: start..e, clicks: first..c });
    }
    result
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

fn pipeline<'a, K: LinkKey<'a>, L: Layout>(link_maps: &'a [Mmap], event_maps: &'a [Mmap], click_maps: &'a [Mmap], catalog: &Catalog<'_>, start: i64, end: i64, out: &Path) -> std::io::Result<bool> {
    let clock = Instant::now();
    let first_day = start.div_euclid(DAY) as i32;
    let days = ((end-1).div_euclid(DAY)-i64::from(first_day)+1).max(0) as usize;
    let origin = start-WEEK;
    let unsupported = AtomicBool::new(false);
    let (links, parts) = rayon::join(
        || {
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
    let campaign_parts: Vec<_> = events.into_par_iter().zip(clicks).enumerate().map(|(part, (events, clicks))| {
        let phase = Span::worker(Phase::BucketCopy);
        let mut events = flatten(events);
        let mut clicks = flatten(clicks);
        drop(phase);
        let phase = Span::worker(Phase::EventSort);
        sort_events(&mut events);
        drop(phase);
        let phase = Span::worker(Phase::ClickSort);
        // Within equal timestamps the smallest click ID is last, hence right-1 wins.
        clicks.sort_unstable_by(|a,b| a.user.cmp(&b.user).then(a.ts.cmp(&b.ts)).then_with(|| b.id.cmp(&a.id)));
        drop(phase);
        let phase = Span::worker(Phase::Grouping);
        let groups = work(&events, &clicks);
        drop(phase);
        let mut agg = Aggregate::new(catalog.campaigns.len(), first_day, days);
        let phase = Span::worker(Phase::PersonFused);
        for group in groups {
            person(&mut agg, &events[group.events], &clicks[group.clicks], &catalog.categories, origin);
        }
        drop(phase);
        output_users(out, part, &agg.users, catalog)?;
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
    fn product_lookup_keeps_last_row_exact_ids_and_unknown() {
        let catalog = Catalog::new(b"product_id,category\np1,a\np2,b\np1,c\n\"p 3\",d\n,e\nP1,f\nabcdefghijk,g\n", b"campaign_id,channel\n");
        let name = |product: &[u8]| catalog.categories[catalog.products.get(product) as usize];
        for (product, category) in [(&b"p1"[..], &b"c"[..]), (b"p2", b"b"), (b"p 3", b"d"), (b"", b"e"), (b"P1", b"f"), (b"abcdefghijk", b"g"), (b"p4", b"unknown"), (b"p10", b"unknown")] {
            assert_eq!(name(product), category, "{product:?}");
        }
    }
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
        impl<'a> LinkKey<'a> for Collision<'a> {}
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
    fn event_sort_uses_exact_person_key_before_timestamp() {
        let make = |user, ts| Event::<_,Wide> { user, data: Wide::event(ts, 0, 0, 0) };
        let mut events = vec![make(2u64, 10), make(1, 30), make(1, 20), make(2, 25)];
        sort_events(&mut events);
        assert_eq!(events.iter().map(|event| (event.user,Wide::time(event.data))).collect::<Vec<_>>(), vec![
            (1, 20), (1, 30), (2, 10), (2, 25),
        ]);
    }
    #[test]
    fn bad_ingest_drops_only_that_row() {
        let catalog = Catalog::new(b"product_id,category\np,category\n", b"campaign_id,channel\n");
        let rows = parse_events::<&[u8],Wide>(b"bad,u,1,invalid,view,p,\ngood,u,2,1970-01-01T00:00:00Z,view,p,\n", &catalog, 0, DAY, 0, &AtomicBool::new(false), &mut (Vec::new(), Vec::new()));
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].id, b"good");
    }
}
