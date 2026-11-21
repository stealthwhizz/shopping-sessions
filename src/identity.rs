use crate::{input::{chunks, csv, lines}, keys::{self, Key}, profile::{Phase, Span}};
use memmap2::Mmap;
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};

const BLOCK: usize = 4096;

fn insert(words: &[AtomicU64], mask: usize, id: u64) -> usize {
    let mut slot = id.fingerprint() as usize & mask;
    loop {
        let old = words[slot].load(Relaxed);
        if old == id { return slot; }
        if old == 0 {
            match words[slot].compare_exchange(0, id, Relaxed, Relaxed) {
                Ok(_) => return slot,
                Err(value) if value == id => return slot,
                _ => {}
            }
        }
        slot = (slot+1)&mask;
    }
}

fn root(parent: &[AtomicUsize], mut i: usize) -> usize {
    loop {
        let p = parent[i].load(Relaxed);
        if p == i { return i; }
        let gp = parent[p].load(Relaxed);
        if gp == p { return p; }
        let _ = parent[i].compare_exchange_weak(p, gp, Relaxed, Relaxed);
        i = gp;
    }
}

fn unite(words: &[AtomicU64], parent: &[AtomicUsize], a: usize, b: usize) {
    loop {
        let (mut a, mut b) = (root(parent,a), root(parent,b));
        if a == b { return; }
        if words[a].load(Relaxed) < words[b].load(Relaxed) { std::mem::swap(&mut a,&mut b); }
        // Immutable keys strictly decrease on every hook, so concurrent hooks cannot cycle.
        if parent[a].compare_exchange(a, b, Relaxed, Relaxed).is_ok() { return; }
    }
}

pub(super) fn build(maps: &[Mmap], unsupported: &AtomicBool) -> (Vec<(u64,u64)>, usize) {
    let ranges = { let _phase = Span::worker(Phase::Links); chunks(maps) };
    let parts: Vec<Vec<(u64,u64)>> = ranges.par_iter().map(|bytes| {
        let _phase = Span::worker(Phase::Links);
        lines(bytes).filter_map(|line| {
            let [a,b] = csv::<2>(line)?;
            if a.is_empty() || b.is_empty() { return None; }
            match (keys::pack(a), keys::pack(b)) {
                (Some(a),Some(b)) => Some((a,b)),
                _ => { unsupported.store(true,Relaxed); None }
            }
        }).collect()
    }).collect();
    let mut edges = {
        let _phase = Span::worker(Phase::Links);
        let mut edges = Vec::with_capacity(parts.iter().map(Vec::len).sum());
        for mut part in parts { edges.append(&mut part); }
        edges
    };
    let capacity = (edges.len()*4).max(1).next_power_of_two();
    let mask = capacity-1;
    let (mut words, mut parent) = {
        let _phase = Span::worker(Phase::Links);
        (Box::<[AtomicU64]>::new_uninit_slice(capacity), Box::<[AtomicUsize]>::new_uninit_slice(capacity))
    };
    words.par_chunks_mut(BLOCK).zip(parent.par_chunks_mut(BLOCK)).enumerate().for_each(|(block,(words,parent))| {
        let _phase = Span::worker(Phase::Links);
        for i in 0..words.len() {
            words[i].write(AtomicU64::new(0));
            parent[i].write(AtomicUsize::new(block*BLOCK+i));
        }
    });
    // Every cell was initialized once by disjoint chunks before these blocking loops return.
    let (words,parent) = unsafe { (words.assume_init(),parent.assume_init()) };
    edges.par_chunks_mut(BLOCK).for_each(|edges| {
        let _phase = Span::worker(Phase::Links);
        for (a,b) in edges {
            // Edges hold packed IDs until this phase, then stable table-slot indices.
            *a = insert(&words,mask,*a) as u64;
            *b = insert(&words,mask,*b) as u64;
        }
    });
    // Interning is complete: words never change again. Phase barriers publish all relaxed stores.
    edges.par_chunks(BLOCK).for_each(|edges| {
        let _phase = Span::worker(Phase::Links);
        for &(a,b) in edges { unite(&words,&parent,a as usize,b as usize); }
    });
    let mut ids = { let _phase = Span::worker(Phase::Links); Box::<[(u64,u64)]>::new_uninit_slice(capacity) };
    ids.par_chunks_mut(BLOCK).enumerate().for_each(|(block,ids)| {
        let _phase = Span::worker(Phase::Links);
        for (i,entry) in ids.iter_mut().enumerate() {
            let slot = block*BLOCK+i;
            let id = words[slot].load(Relaxed);
            let name = if id == 0 { 0 } else { words[root(&parent,slot)].load(Relaxed) };
            entry.write((id,name));
        }
    });
    // All output slots, including empty markers, were written before publishing the immutable table.
    let ids = unsafe { ids.assume_init() }.into_vec();
    { let _phase = Span::worker(Phase::Links); drop(edges); drop(parent); drop(words); }
    (ids,mask)
}
