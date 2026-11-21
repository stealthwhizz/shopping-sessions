mod input;
mod parquet;
mod profile;
mod keys;

use input::{DAY, WEEK, Links, Map, body, chunks, csv, lines, map_dir, map_file, price, timestamp};
use parquet::Column;
use profile::{Phase, Span};
use keys::Key;
use memmap2::Mmap;
use rayon::prelude::*;
use std::{env, path::Path, sync::atomic::{AtomicBool, Ordering::Relaxed}, time::Instant};

const INVALID: i64 = i64::MIN;
const DEDUP_BUCKETS: usize = 128;
const USER_BUCKETS: usize = 32;

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

struct RawEvent<K> {
    id: K,
    user: K,
    ts: i64,
    ingest: i64,
    revenue: u32,
    category: u32,
}
#[derive(Clone, Copy)]
struct Event<K> { user: K, ts: i64, revenue: u32, category: u32 }
struct Click<K> { user: K, id: K, ts: i64, campaign: u32 }

fn parse_events<'a, K: Key<'a>>(bytes: &'a [u8], catalog: &Catalog<'_>, start: i64, end: i64, unsupported: &AtomicBool) -> Vec<Vec<RawEvent<K>>> {
    let _phase = Span::worker(Phase::EventParse);
    let mut rows: Vec<Vec<_>> = (0..DEDUP_BUCKETS).map(|_| Vec::with_capacity(bytes.len()/70/DEDUP_BUCKETS)).collect();
    for line in lines(bytes) {
        let Some(f) = csv::<7>(line) else { continue };
        if f[0].is_empty() { continue; }
        let (Some(id),Some(user)) = (K::parse(f[0]),K::parse(f[1])) else {
            unsupported.store(true, Relaxed); continue;
        };
        let Some(ingest) = timestamp(f[3]) else { continue };
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
        let id_hash = id.fingerprint();
        rows[(id_hash >> 57) as usize].push(RawEvent { id, user, ts, ingest, revenue, category });
    }
    rows
}

fn parse_clicks<'a, K: Key<'a>>(bytes: &'a [u8], links: &Links<K>, catalog: &Catalog<'_>, start: i64, end: i64, unsupported: &AtomicBool) -> Vec<Click<K>> {
    let mut rows = Vec::with_capacity(bytes.len()/45);
    for line in lines(bytes) {
        let Some([id,user,campaign,ts]) = csv::<4>(line) else { continue };
        if id.is_empty() || user.is_empty() { continue; }
        let (Some(id),Some(user)) = (K::parse(id),K::parse(user)) else {
            unsupported.store(true, Relaxed); continue;
        };
        let Some(ts) = timestamp(ts).filter(|&ts| ts >= start-WEEK && ts < end) else { continue };
        let Some(&campaign) = catalog.campaign_ids.get(campaign) else { continue };
        let user = links.name(user);
        // Click duplicates have identical values: repeating one changes neither the
        // winning click nor the per-campaign sliding-window counts' zero/nonzero state.
        rows.push(Click { user, id, ts, campaign });
    }
    rows
}


fn sort_events<K: Copy+Ord>(events: &mut [Event<K>]) {
    events.sort_unstable_by_key(|event| (event.user, event.ts));
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

fn dedup<'a, K: Key<'a>>(parts: Vec<Vec<RawEvent<K>>>, links: &Links<K>) -> Vec<Event<K>> {
    let count: usize = parts.iter().map(Vec::len).sum();
    let capacity = (count * 2).max(1).next_power_of_two();
    let mut table: Vec<Option<&RawEvent<K>>> = vec![None; capacity];
    for row in parts.iter().flatten() {
        let mut slot = row.id.fingerprint() as usize & (capacity-1);
        loop {
            match table[slot] {
                None => { table[slot] = Some(row); break; }
                Some(old) if old.id == row.id => {
                    if row.ingest < old.ingest { table[slot] = Some(row); }
                    break;
                }
                _ => slot = (slot+1) & (capacity-1),
            }
        }
    }
    let mut events = Vec::with_capacity(count);
    for row in table.into_iter().flatten().filter(|r| r.ts != INVALID) {
        let user = links.name(row.user);
        events.push(Event { user, ts: row.ts, revenue: row.revenue, category: row.category });
    }
    events
}

struct Work { events: std::ops::Range<usize>, clicks: std::ops::Range<usize> }
fn work<K: Copy+Ord>(events: &[Event<K>], clicks: &[Click<K>]) -> Vec<Work> {
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

fn person<K: Copy+Ord>(agg: &mut Aggregate<K>, events: &[Event<K>], clicks: &[Click<K>], categories: &[&[u8]]) {
    let mut active = Map::<u32,u32>::default();
    let (mut left, mut right, mut first) = (0,0,0);
    let mut daily: Option<UserRow<K>> = None;
    let mut category_totals = Vec::<(u32,i64)>::new();
    let mut sampled = false;
    while first < events.len() {
        sampled = profile::sample();
        let phase = Span::sampled(Phase::SessionSample, sampled);
        let start = events[first].ts;
        let day = start.div_euclid(DAY) as i32;
        let mut last = first+1;
        while last < events.len() && events[last].ts-events[last-1].ts <= 1_800_000 { last += 1; }
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
            revenue += event.revenue as i64;
            if event.revenue > 0 {
                if let Some(total) = category_totals.iter_mut().find(|v| v.0 == event.category) { total.1 += event.revenue as i64; }
                else { category_totals.push((event.category, event.revenue as i64)); }
            }
        }
        row.sessions += 1; row.events += (last-first) as i64; row.revenue += revenue;
        drop(phase);
        let phase = Span::sampled(Phase::AttributionSample, sampled);
        while right < clicks.len() && clicks[right].ts <= start {
            *active.entry(clicks[right].campaign).or_default() += 1;
            right += 1;
        }
        while left < right && clicks[left].ts < start-WEEK {
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

fn pipeline<'a, K: Key<'a>>(link_maps: &'a [Mmap], event_maps: &'a [Mmap], click_maps: &'a [Mmap], catalog: &Catalog<'_>, start: i64, end: i64, out: &Path) -> std::io::Result<bool> {
    let clock = Instant::now();
    let first_day = start.div_euclid(DAY) as i32;
    let days = ((end-1).div_euclid(DAY)-i64::from(first_day)+1).max(0) as usize;
    let unsupported = AtomicBool::new(false);
    let (links, parts) = rayon::join(
        || {
            let _phase = Span::worker(Phase::Links);
            Links::<K>::new(link_maps, &unsupported)
        },
        || chunks(event_maps).par_iter().map(|bytes| parse_events::<K>(bytes, catalog, start, end, &unsupported)).collect::<Vec<_>>(),
    );
    if unsupported.load(Relaxed) { return Ok(false); }
    eprintln!("parse {:.3}s", clock.elapsed().as_secs_f64());
    let phase = Span::process(Phase::Dedup);
    let events = transpose(transpose(parts, DEDUP_BUCKETS).into_par_iter().map(|parts| {
        partition(dedup(parts, &links), |event| event.user.fingerprint())
    }).collect(), USER_BUCKETS);
    drop(phase);
    eprintln!("dedup {:.3}s", clock.elapsed().as_secs_f64());
    let phase = Span::process(Phase::ClickParse);
    let clicks = transpose(chunks(&click_maps).par_iter().map(|bytes| {
        partition(parse_clicks::<K>(bytes, &links, catalog, start, end, &unsupported), |click| click.user.fingerprint())
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
            person(&mut agg, &events[group.events], &clicks[group.clicks], &catalog.categories);
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
        pipeline::<&[u8]>(&link_maps, &event_maps, &click_maps, &catalog, start, end, Path::new(&out))?;
        eprintln!("path=string reason=forced");
    } else if pipeline::<u64>(&link_maps, &event_maps, &click_maps, &catalog, start, end, Path::new(&out))? {
        eprintln!("path=packed");
    } else {
        eprintln!("path=string reason=unsupported-id");
        pipeline::<&[u8]>(&link_maps, &event_maps, &click_maps, &catalog, start, end, Path::new(&out))?;
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
    fn dedup_keeps_invalid_earliest_and_distinguishes_hash_collisions() {
        #[derive(Clone,Copy,Eq,PartialEq,Ord,PartialOrd,Hash)]
        struct Collision<'a>(&'a [u8]);
        impl<'a> Key<'a> for Collision<'a> {
            fn parse(bytes: &'a [u8]) -> Option<Self> { Some(Self(bytes)) }
            fn fingerprint(self) -> u64 { 7 }
            fn append_plain(self, data: &mut Vec<u8>) { self.0.append_plain(data); }
        }
        let make = |id, ingest, ts| RawEvent { id: Collision(id), user: Collision(b"u"), ts, ingest, revenue: 0, category: 0 };
        let links = Links::new(&[], &AtomicBool::new(false));
        let result = dedup(vec![vec![make(&b"a"[..], 20, 100), make(&b"b"[..], 30, 200)], vec![make(&b"a"[..], 10, INVALID)]], &links);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].ts, 200);
    }
    #[test]
    fn event_sort_uses_exact_person_key_before_timestamp() {
        let make = |user, ts| Event { user, ts, revenue: 0, category: 0 };
        let mut events = vec![make(2u64, 10), make(1, 30), make(1, 20), make(2, 25)];
        sort_events(&mut events);
        assert_eq!(events.iter().map(|event| (event.user,event.ts)).collect::<Vec<_>>(), vec![
            (1, 20), (1, 30), (2, 10), (2, 25),
        ]);
    }
    #[test]
    fn bad_ingest_drops_only_that_row() {
        let catalog = Catalog::new(b"product_id,category\np,category\n", b"campaign_id,channel\n");
        let rows = parse_events::<&[u8]>(b"bad,u,1,invalid,view,p,\ngood,u,2,1970-01-01T00:00:00Z,view,p,\n", &catalog, 0, DAY, &AtomicBool::new(false));
        let kept: Vec<_> = rows.iter().flatten().collect();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].id, b"good");
    }
}
