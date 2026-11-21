# Rust entry

`target/release/shopping-sessions` reads `DATA_DIR` / `OUTPUT_DIR`, or its two positional arguments. `NCPU` selects the Rayon worker count. `SHOPPING_PATH=packed` (default) attempts exact order-preserving numeric IDs and automatically retries with exact byte strings if any parsed ID is unsupported; `SHOPPING_PATH=string` forces the byte-string path. No output is written before numeric eligibility is known. The binary needs no scratch files and writes only the two output folders.

The implementation follows `rules.md`: links use union-find with minimum-byte names; events are hash-partitioned by event ID and deduplicated before invalid retained copies are discarded; valid events and clicks are partitioned by canonical person. IDs of up to ten ASCII alphanumeric characters use six-bit codes in ASCII order, zero-padded and left-aligned in a `u64`; all other IDs retain the exact byte-string path. Hashes assign buckets only: equality, canonical names, user ordering, and click-ID ties use exact keys, never hash-only identity. Per-person timestamp ordering drives sessions, attribution, and a sliding seven-day campaign-count window. Identical click copies need not be materialized as a separate dedup table: repeating an identical click cannot change the winning click or whether its campaign's count is zero in the attribution window.

Outputs are deterministic scalar Parquet v1 with PLAIN/uncompressed pages, UTF-8 string annotations, DATE days, RLE definition levels for nullable categories, and losslessly selected INT32/INT64 columns. User rows are written by each of 32 fixed person partitions as it finishes; no global output sort or user-row merge is needed. Campaign totals use dense campaign-by-day arrays. This deliberately narrow writer avoids Arrow, Thrift, compression libraries, and their startup/build costs. It rejects a data page over 2 GiB rather than emitting invalid Parquet.

## Dependencies

Direct, pinned: `memchr 2.7.6`, `memmap2 0.9.9`, `rayon 1.11.0`.

`Cargo.lock` also pins `crossbeam-deque 0.8.8`, `crossbeam-epoch 0.9.21`, `crossbeam-utils 0.8.23`, `either 1.19.0`, `libc 0.2.191`, and `rayon-core 1.13.0`. No runtime Python or third-party shared libraries are needed; the Debian runtime supplies glibc.

The measurements through the first phase breakdown describe the original round-one entry. Current checkpoint measurements and verification are recorded at the end.

## Correctness commands

```sh
PY=../event/shopping/.venv/bin/python
export GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.top_pad=268435456
cargo build --release
cargo test
$PY scripts/run_fixtures.py fixtures -- target/release/shopping-sessions
NCPU=8 DATA_DIR=data/dev OUTPUT_DIR=out-rust target/release/shopping-sessions
$PY scripts/compare.py out-rust data/dev-answer
make fixtures CTX=. IMAGE=shopping-rust
make run-dev check-dev CTX=. IMAGE=shopping-rust
```

The lead added X1 after the original 36 fixtures: current acceptance is **37/37**. It includes long leading-zero numbers, overflow values, leap-day/calendar rejection cases, Unicode names, quote anomalies, and a final line without a newline.

## Benchmark method

The local machine is an i7-12700H. CPU affinity `0-7` selects four P-cores and their hyperthreads; reference jobs were moved to E-cores `12-19`. Measurements are warm-cache, five runs, eight workers. Laptop turbo/thermals still affect absolute results. Container wrapper wall time includes Podman startup/cleanup, unlike the scorer's process-start-to-exit clock. The binary prints its cumulative stage times and Linux `VmHWM`; the latter measures **the container process**, whereas `/usr/bin/time` around Podman measures the client and is not a container RSS measurement.

```sh
generate --seed 2 --events 20000000 --clicks 5000000 --output data/s20m-2
for n in 1 2 3 4 5; do
  /usr/bin/time -v env NCPU=8 DATA_DIR=data/s20m-2 OUTPUT_DIR=out-rust20m \
    taskset -c 0-7 target/release/shopping-sessions
done
CPUS=8 MEM=12g IMAGE=shopping-rust scripts/docker_run.sh data/s20m-2 out-rust20m-container
```

## Optimization ledger

Times below are back-to-back native runs, not comparisons across changing thermal conditions.

| Change | Before → after | Decision |
|---|---|---|
| Person-hash partitions instead of global event/click sorts | Median 4.79 → 3.67 s | Keep; also avoids a global event-vector copy and parallelizes person-group discovery |
| Custom AVX2 single-pass CSV delimiter/quote scanner | Median 3.10 → 3.02 s | Removed; marginal gain did not justify ~130 lines and unsafe SIMD |
| INT32 when the entire column fits, otherwise INT64 | User output ~97 → 70.0 MB; elapsed within noise | Keep for the ~28% output-size reduction, without changing integer values |
| Numeric `(person_hash, timestamp)` event sort, with byte-checked collision fallback | Median 3.20 → 3.09 s; improved all five paired runs | Keep; preserves exact grouping even for deliberate hash collisions |

The initial global-sort implementation took 5.89 s / 4,135,728 KiB on 20M before CPU isolation; that figure is context, not a controlled comparison with the final implementation.

## Round-one native measurements

Five warm-cache runs, `taskset -c 0-7`, `NCPU=8`; RSS is the maximum observed across the five runs.

| Dataset | Best wall | Median wall | Peak RSS |
|---|---:|---:|---:|
| dev, 1M events | 0.11 s | 0.11 s | 216,428 KiB (211 MiB) |
| s20m-2, 20M events | 3.05 s | 3.06 s | 4,308,092 KiB (4.11 GiB) |

The round-one binary passed its six Rust unit tests, all 37 fixtures, and the official dev comparison. Reversing all input part-file names and running with one worker produced byte-identical Parquet to the normal eight-worker run. An argv-only three-worker run was byte-identical too. A header-only dataset produced both zero-row tables with exactly the required columns and a DATE `day`.

The round-one linear 60M memory extrapolation from its measured maximum was **12.33 GiB** (not a 60M measurement). Leave headroom for a different seed's linked-ID and output cardinalities. That implementation did not reach the sub-two-second target; later checkpoints below supersede its performance.

## Round-one container measurements

Scorer restrictions were used: uid 1000, read-only root, no network, 64 MiB `/tmp`, eight-CPU quota, 12 GiB RAM with no extra swap, and the three input/output/scratch mounts. The rootless local cgroup does not delegate `cpuset`, so `--cpuset-cpus` is unavailable. CPU affinity was instead set **inside** the container with its existing `/usr/bin/taskset`; `/proc/self/status` confirmed `Cpus_allowed_list: 0-7`. Setting affinity only on the Docker client does not propagate through this runtime.

Five pinned runs; process wall uses Bash's built-in `time` around `taskset` plus the binary, so it includes destruction/unmapping but not Podman startup. Peak RSS is the binary's `VmHWM`, not the client's RSS.

| Dataset | Best process wall | Median process wall | Median client wall | Peak process RSS |
|---|---:|---:|---:|---:|
| dev | 0.119 s | 0.120 s | 0.22 s | 219,748 KiB |
| s20m-2 | 3.086 s | 3.111 s | 3.22 s | 4,173,660 KiB |

The exact requested wrapper invocation (`CPUS=8 MEM=12g IMAGE=shopping-rust scripts/docker_run.sh data/s20m-2 out-rust20m-container`) also completed: client wall 3.46 s, process peak RSS 4,137,064 KiB. This run was not affinity-pinned.

The current native 20M output matches the independent reference: 2,240,415 user rows and 28,014 campaign rows, zero differing rows or duplicate keys. The pinned container 20M output also matches the native output. Both native and container dev outputs match the official answer.

To reproduce the pinned process measurement, use the wrapper's flags but override only the timing entrypoint:

```sh
DATA=$(realpath data/s20m-2)
mkdir -p out-rust20m-container
OUT=$(realpath out-rust20m-container)
SCRATCH=$(mktemp -d)
chmod 777 "$OUT" "$SCRATCH"
/usr/bin/time -v docker run --rm --platform linux/amd64 --network none --read-only \
  --tmpfs /tmp:rw,size=64m,mode=1777 --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 4096 --cpus 8 --memory 12g --memory-swap 12g --user 1000:1000 \
  -v "$DATA":/data:ro,z -v "$OUT":/output:z -v "$SCRATCH":/scratch:z \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
  -e NCPU=8 -e MEM_GB=12 --entrypoint /bin/bash shopping-rust \
  -c 'TIMEFORMAT="process_wall=%R"; time taskset -c 0-7 /usr/local/bin/shopping-sessions'
rm -rf "$SCRATCH"
```

## Detailed phase profiling

The optional `phase-profile` Cargo feature records process CPU for sequential pipeline stages and per-thread CPU for overlapping bucket stages. All its guards are zero-sized no-ops in the default submission build. The fused inner loop is sampled every 128 sessions using the invariant TSC, with empty-probe overhead calibrated and reported. Syscall CPU clocks are too expensive for these sub-microsecond inner spans; TSC samples are used only for relative work estimates, not asserted to be exact CPU time.

```sh
cargo build --release --features phase-profile --target-dir target/phase-profile
NCPU=8 DATA_DIR=data/s20m-2 OUTPUT_DIR=out-rust20m-profile \
  taskset -c 0-7 target/phase-profile/release/shopping-sessions
```

For parallel bucket stages, `wall` is the envelope from the first bucket entering that stage to the last bucket leaving it; different stages overlap, so **do not sum their wall envelopes**. `cpu` sums their workers' thread CPU, and `avg_threads = cpu / wall` is mean occupancy over that envelope, not the number of workers configured. `work_wall` sums worker residence times. File/mmap timing covers file discovery/open/map syscalls; actual mmap page faults and byte reads are charged to parsing. The inner sessionization, attribution/assists, and aggregation split is approximate; the complete fused-stage envelope and CPU are measured directly.

### 20M phase breakdown

Median of five profile runs on CPUs 0-7, eight workers. Profile-process wall best/median was 3.09/3.12 s; the default, uninstrumented median was 3.06 s. `avg_threads` is CPU seconds divided by the phase's wall interval (eight means full eight-logical-CPU occupancy).

| Phase | Wall / envelope (s) | CPU work (s) | Average busy threads |
|---|---:|---:|---:|
| File discovery/open/mmap | 0.00052 | 0.00055 | 1.03 |
| Links parse + intern + union-find | 0.30185 | 0.30098 | 1.00 |
| Product/campaign catalogs | 0.00243 | 0.00242 | 1.00 |
| Events CSV parse + timestamp/price validation | 0.87828 | 6.93606 | 7.90 |
| Exact event dedup + person partition | 0.51671 | 4.05312 | 7.85 |
| Click parse + validation + person partition | 0.18616 | 1.43162 | 7.71 |
| Bucket-vector concatenation | 0.55077 | 0.39962 | 0.69 |
| Event ordering | 0.60258 | 2.29789 | 3.88 |
| Click ordering | 0.54762 | 0.86268 | 1.55 |
| Person-group discovery | 0.53760 | 0.62048 | 1.13 |
| Fused sessionization + attribution + per-person aggregation | 0.54441 | 0.83122 | 1.55 |
| Campaign partial-map reduction | 0.58540 | 0.18384 | 0.32 |
| Output row sorting | 0.23727 | 1.06040 | 4.44 |
| Parquet column encoding / surrounding output work | 0.10450 | 0.10421 | 1.00 |
| Parquet file/page/footer writes only | 0.03377 | 0.01173 | 0.35 |
| Explicit userspace destruction/unmap | 0.03545 | 0.03506 | 0.99 |

Bucket copy, event/click ordering, discovery, fused work, and partial reduction overlap across the 32 person buckets. Their envelopes are therefore not additive; the CPU-work column is the useful additive comparison. The complete output phase, including sorting, encoding, and writing, is 0.38213 s / 1.18432 CPU s. File-read page faults are included in parse times, not the sub-millisecond mmap calls.

[INFERENCE] Calibrated TSC samples allocate the fused stage approximately as follows. These are relative-weight allocations of its measured wall/CPU, not independently measured sequential stages; rare whale sessions and sub-microsecond probe overhead limit their accuracy.

| Fused subphase | Allocated wall (s) | Allocated CPU (s) | Busy-thread contribution over fused envelope |
|---|---:|---:|---:|
| Session-boundary scan | 0.074 | 0.115 | 0.21 |
| Click credit + seven-day assists + campaign increments | 0.382 | 0.587 | 1.10 |
| Daily event/revenue/category aggregation | 0.084 | 0.130 | 0.24 |

The default binary's measured interval from the logged final pipeline time to process exit was median 0.095 s; the profile explicitly attributes about 0.035 s to userspace destruction/unmap. The remaining kernel/process-exit work was not independently CPU-attributed.

The two output files for this dataset are 70,047,291 and 1,120,867 bytes. The final native executable is 603,936 bytes; the final `shopping-rust` image is **78,555,248 bytes (74.92 MiB)**. A final `--no-cache` build took **18.19 s**, with base images already present and the host's build-only registry mirror injected by its Docker shim. No mirror is encoded in repository files.

Raw phase measurements are retained under `out-rust20m-profile/phase-logs/`; they are generated, gitignored artifacts.

## Round-two submission checkpoint

The retained changes are dense campaign/day counters, partition-local unsorted user Parquet output, and overlapping union-find with event parsing. The kept event copy is resolved to its canonical person after dedup. A malformed `ingest_ts` now skips only that row rather than aborting the process; a regression test covers it.

| Experiment | Native 20M median, CPUs 0-7 | Decision |
|---|---:|---|
| Original round one | 3.06 s | Baseline |
| Dense campaign/day arrays | 3.03 s (3 runs) | Retained |
| Partition-local unsorted output | 2.64 s (3 runs) | Retained |
| Overlap links with parsing | 2.58 s initially; 2.49 s in later paired control runs | Retained |
| Generic 16-byte inline IDs with a borrowed long-ID fallback | 4.59 s vs 2.49 s paired control | Completely reverted |

The exact source/binary for the overlapped version is saved at `/tmp/rust-good-2/`; its copied binary passed 37/37 fixtures, official dev MATCH, and 12/12 crosscheck seeds including the 20M seed. Its binary SHA-256 is `347b177abb7732c3039026d35c9528336aedf4ef53cccc086181a2bcd8aa5e23`. The subsequent clean checkpoint adds only the malformed-ingest regression fix and is saved separately before further experiments.

The inline-ID experiment was intentionally removed rather than hidden behind an unused option. Prefix-scatter and packed-u64 paths have not been introduced at this checkpoint. New experiments must start from a saved `/tmp/rust-good-N/` source plus paired binary, so a successful measured state can always be submitted independently of ongoing edits.

## Round-three packed identity path

Both key representations instantiate one pipeline, rather than maintaining two implementations of the rules. Packed IDs map digits to 1–10, uppercase letters to 11–36, and lowercase letters to 37–62. Zero padding preserves prefix order; unsigned integer comparison is therefore byte-wise order. Unsupported event, click, user or link IDs set a shared flag before any output, and the entire exact byte-string instantiation is used instead. Product and campaign catalog IDs retain their original exact lookup behavior.

The links table is flattened to key-to-canonical-key after union-find. Packed records contain only integer IDs after parsing, and names are unpacked only while encoding output. Raw events are 40 bytes, surviving events 24 bytes, and clicks 32 bytes, versus 64/40/56 bytes in the original entry. Timestamps remain `i64`; no window-length assumption or timestamp narrowing was introduced.

A packed-order/round-trip test covers 10,000 generated strings, prefixes, digits and case; malformed/long/Unicode IDs exercise fallback. Another regression test catches zero low hash bits from left-aligned keys. Multiplicative hashing without final avalanche caused severe clustering (~9.5 s); the finalizer now mixes high bits into every table-index bit.

Native gates passed in both forced modes: 37/37 fixtures, official dev MATCH, and ten Rust unit tests. Five warm packed-path 20M runs on CPUs 0-7 measured **1.69 s best / 1.70 s median**, peak **3,288,296 KiB**. These timings exclude the independent scanner integration.

Checkpoint 4 additionally passed all twelve independent crosscheck seeds in **both** modes, including 20M seed 3. Its pinned-base `shopping-rust-cp4` container passed 37/37 fixtures and dev MATCH; image size was 78,626,415 bytes. The verified source and paired executable are saved at `/tmp/rust-good-4/`.

```sh
SHOPPING_PATH=packed $PY scripts/run_fixtures.py fixtures -- target/release/shopping-sessions
SHOPPING_PATH=string $PY scripts/run_fixtures.py fixtures -- target/release/shopping-sessions
SHOPPING_PATH=packed taskset -c 12-19 nice -n 10 $PY tools/crosscheck.py -- taskset -c 12-19 /tmp/rust-good-4/shopping-sessions
SHOPPING_PATH=string taskset -c 12-19 nice -n 10 $PY tools/crosscheck.py -- taskset -c 12-19 /tmp/rust-good-4/shopping-sessions
```

## Checkpoint 5: packed IDs plus single-pass scanning

`scan.rs` fuses newline/comma/quote scanning with field extraction, dispatching at runtime to AVX2/BMI1/POPCNT or an exact portable implementation. Event and click parsing use it; small catalogs and links retain the existing parser. Fixed-layout timestamp validation uses SWAR with the scalar parser retained for other forms. ScanLab checked equivalence against the checkpoint-three parser with real files, approximately 30M generated CSV lines, and 19M timestamps before integration.

Observed gates: ten Rust unit tests; 37/37 fixtures and official dev MATCH in both packed and forced-string modes; 12/12 independent crosscheck seeds through the packed path; container 37/37 and dev MATCH. Reversed input filenames with one packed worker and three forced-string workers both MATCH the normal eight-worker output. Native and container 20M outputs MATCH the independent reference (2,240,415 user rows, 28,014 campaign rows).

Five paired native 20M runs, alternating immutable checkpoint-four/five executables on CPUs 0-7: checkpoint four **1.69 s best / 1.71 s median**, checkpoint five **1.43 s best / 1.43 s median**. The first unpaired scanner run was cold/noisy (2.01 s); it is not substituted into the paired comparison.

| Dataset / execution | Best process wall | Median process wall | Peak process RSS |
|---|---:|---:|---:|
| dev, native | 0.06 s | 0.07 s | 189,484 KiB |
| 20M seed 2, native | 1.43 s | 1.43 s | 3,312,884 KiB |
| dev, container | 0.068 s | 0.072 s | 189,936 KiB |
| 20M seed 2, container | 1.446 s | 1.451 s | 3,296,540 KiB |

Container timings use the pinned in-container affinity and scorer flags documented above, with image `shopping-rust-cp5`. Median Podman-client wall was 0.18 s for dev and 1.55 s for 20M. The clean-built image is 78,641,776 bytes. A `--no-cache` image build took **18.30 s**, including dependency downloads through the host-injected mirror, with base images already present. The native release compile took 6.19 s with dependencies cached. Both builder and runtime base images are pinned by digest; no registry mirror is encoded in the entry.

The largest native peak across the initial and paired scanner runs was 3,321,644 KiB. A conservative linear extrapolation to 60M is **9.51 GiB**, not a 60M measurement; different seed cardinalities still need headroom. No timestamp narrowing, CPU-native code generation, forced process exit, or new dependency was introduced.

### Checkpoint-five phase profile

Medians of five instrumented 20M runs, CPUs 0-7, eight workers. Wall values for bucket phases are overlapping envelopes, not sequential critical-path costs; busy threads are the median of per-run CPU/wall ratios. Links overlap event parsing, and click parsing overlaps dedup. Actual mmap page faults remain charged to the parsing phases.

| Phase | Wall / envelope (s) | CPU work (s) | Average busy threads |
|---|---:|---:|---:|
| File discovery/open/mmap | 0.000435 | 0.000467 | 1.02 |
| Links parse + union-find + flatten | 0.447385 | 0.444859 | 0.99 |
| Product/campaign catalogs | 0.003281 | 0.003272 | 1.00 |
| Events parse + validate | 0.514446 | 3.621892 | 7.05 |
| Event dedup + person partition | 0.361368 | 2.842972 | 7.87 |
| Click parse + validate + partition | 0.137366 | 1.063957 | 7.74 |
| Bucket-vector concatenation | 0.299829 | 0.182796 | 0.64 |
| Event ordering | 0.314795 | 1.219663 | 3.77 |
| Click ordering | 0.290496 | 0.390659 | 1.32 |
| Person-group discovery | 0.280860 | 0.074573 | 0.26 |
| Fused sessions + attribution + aggregation | 0.290506 | 0.536520 | 1.84 |
| Campaign reduction | 0.004157 | 0.011818 | 2.81 |
| Parquet encoding + writes | 0.284402 | 0.101421 | 0.35 |
| Parquet writes only, included above | 0.282399 | 0.027852 | 0.10 |
| Explicit userspace destruction/unmap | 0.036519 | 0.036352 | 1.00 |

[INFERENCE] Calibrated samples split the fused stage into approximately 0.043 s allocated wall / 0.079 CPU s for session boundaries; 0.168 / 0.311 for credit and assists; and 0.079 / 0.147 for daily aggregation. Their busy-thread contributions over the fused envelope are 0.27, 1.07, and 0.51 respectively. These allocations are relative work estimates, not independently measured non-overlapping phases. Default native exit occurs approximately 0.07 s after the final pipeline log; only the explicit destruction portion is separately attributed.

Raw profile logs: `out-rust-cp5-bench/phase-logs/{1,2,3,4,5}.log` (generated, gitignored). Reproduce with the correctness and pinned-container commands above, changing the image to `shopping-rust-cp5`; the default path is packed with automatic exact fallback.

Verified source and its paired native executable are saved at `/tmp/rust-good-5/`. The scanner module is included unchanged from ScanLab.

## Checkpoint 6: compact event/click records

The packed path now stores event time relative to `window_start - 7 days` in 32 bits, plus 24 revenue bits and eight category bits. Startup checks the complete click-window span with checked arithmetic, reserves `u32::MAX` for an invalid retained copy, and requires at most 256 catalog categories including `unknown`. Wider windows or catalogs select the original `i64`/`u32` representation before parsing. Ingest timestamps remain `i64`. `SHOPPING_LAYOUT=wide` forces this exact wide representation; forced-string and unsupported-ID fallback also use it.

Both layouts instantiate the same rule-processing functions. Packed raw events shrink 40→32 bytes, surviving events 24→16, clicks 32→24. The record-width test first failed at 40 versus 32 bytes, then passed alongside boundary round-trip/startup-guard tests. Revenue totals and Parquet integer selection are unchanged.

Observed: 13 unit tests; 37/37 fixtures and dev MATCH through compact, forced-wide, and forced-string paths; packed crosscheck 12/12 including 20M; container 37/37 and dev MATCH. Image `shopping-rust-cp6` is 78,718,573 bytes. Paired CPU0-7 20M runs gave checkpoint-five best/median **1.41/1.41 s**, compact records **1.32/1.33 s**, with all five pairs faster. Compact peak RSS was **2,997,120 KiB** (linear 60M extrapolation approximately **8.58 GiB**, not measured).

The verified source and paired native executable are saved at `/tmp/rust-good-6/`.

### Rejected ordering experiments after checkpoint six

An in-place 256-way MSD radix sort (exact packed user keys, timestamp sort within one-user groups) passed the property test, fixtures and dev comparison, but paired 20M median was 1.37 s versus scalar sorting's 1.36 s. Branch-light `u128` comparison keys were also neutral/slower (1.36 versus 1.35 s). Both implementations, their unused key hooks, and experiment-only code were completely removed.

The runtime now includes RunLevel's measured allocator setting, `GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.top_pad=268435456`. Native timing commands must export the same value; the container receives it from the Dockerfile. Historical timings above predate this setting.

## Checkpoint 7: branchless exact ID packing

A 256-byte constant lookup table replaces per-character alphanumeric branches. Codes are accumulated six bits at a time and left-aligned once; a combined invalid flag preserves exact fallback for every unsupported byte. An exhaustive 256-single-byte test supplements the existing 10,000-string order/round-trip checks.

Before the allocator setting, five paired 20M runs improved checkpoint-six best/median **1.31/1.32 s** to **1.16/1.17 s**. With the required allocator setting, a separate quiet paired run improved checkpoint six **1.21/1.23 s** to checkpoint seven **1.05/1.06 s**, with peak **3,051,832 KiB**. Concurrent build/reference runs were substantially noisier and are not substituted into this quiet comparison.

Observed gates: 14 unit tests; 37/37 and dev MATCH for compact, wide and string paths; packed crosscheck 12/12 including 20M; container 37/37 and dev MATCH. The image is **78,720,865 bytes**. The source and paired executable are saved at `/tmp/rust-good-7/`. `scan.rs` remains unchanged; its additional equivalence suites are required before any future scanner edit is retained.

### Rejected direct dedup/person-scatter fusion

Emitting each dedup winner directly into its person bucket removed the intermediate event vector and both repartition passes, and passed unit tests, 37 fixtures and dev MATCH. Nevertheless, all five paired 20M runs were neutral/slower: baseline best/median **1.06/1.08 s**, fused **1.08/1.11 s**. The fusion was fully removed. [INFERENCE] Mixing canonical-name lookups and 32 output write streams lost locality; fewer logical passes did not improve the measured critical path.

Caching each exact event ID alongside the dedup-table row pointer also passed 14 tests, fixtures and dev MATCH, but was slower in all but one neutral pair: baseline median **1.10 s**, cached slots **1.15 s**. The doubled numeric slot width was not worth avoiding collision-time row reads, so this experiment was completely removed.

## Checkpoint 8: flat canonical-identity lookup

Links now use an exact open-addressing table throughout union-find and final lookup, rather than constructing the standard hash map twice. A physical-row count bounds capacity at no more than half occupancy. Roots still use union by size, path halving and the exact minimum name. Empty keys are only unused-slot markers; empty link fields remain ignored.

Dedup survivors and validated clicks first retain their raw user IDs, then a separate resolve pass prefetches the lookup slot 16 rows ahead. Numeric `(ID, canonical ID)` pairs are interleaved in 16-byte slots, so one prefetched cache line supplies both. The generic exact string path uses the same algorithm. Collision/transitive-union and Unicode/fallback regression tests raise the test count to 16.

Five three-way paired 20M runs (CPUs 0-7, allocator tunables exported) measured checkpoint seven **1.24 s best / 1.25 s median**, separate-key/value flat arrays **1.15/1.19 s**, and retained interleaved slots **1.14/1.14 s**, peak **3,038,184 KiB**. Absolute clocks were affected by concurrent work; only paired comparisons are used for the decision.

Observed gates for the retained interleaved version: 16 tests; 37/37 fixtures and dev MATCH for compact/wide/string; packed crosscheck 12/12 including 20M; container 37/37 and dev MATCH. Image `shopping-rust-cp8` is **78,717,280 bytes**. The exact source and paired executable are saved at `/tmp/rust-good-8/`.

### Dense-person indexing decision

Counting/scattering by an exact local person index, followed by per-person timestamp sorts, passed 18 tests and all compact/wide/string fixture/dev gates. However, quiet paired medians were **1.02 s versus 1.02 s** for full sorting. Recording indices during counting avoided the second lookup but produced only small/noisy bucket-stage gains and extra buffers. Neither implementation was retained; the temporary module and its obsolete call sites were removed.

## Checkpoint 9: count/scatter routing and exact suspect dedup

RunLevel's measured design is ported onto checkpoint eight. Each parsed chunk reuses a scratch buffer, counts 1024 event-ID hash buckets, then scatters into one exactly sized routed vector with 1025 prefix offsets. There are still 128 dedup tasks, each handling eight buckets. Each bucket is gathered into reusable cache-sized scratch.

Two bitmaps identify hash bits seen more than once. A singleton bit proves that row's ID has no duplicate; repeated bits send every associated row, including invalid copies, through exact ID comparison and smallest-ingest selection. Hash collisions only add suspects and never merge identities. Canonical person resolution remains the separate prefetched flat-table pass from checkpoint eight.

Five paired CPU0-7 20M runs with the required tunables improved checkpoint-eight best/median **0.99/1.01 s** to **0.88/0.89 s**, all five pairs faster. Peak routed-version RSS was **2,793,860 KiB**, approximately **8.00 GiB** if linearly extrapolated to 60M (not a 60M measurement). Adding a 4 MiB malloc mmap threshold was near noise and was not retained.

Observed gates: 17 tests, including the 20K-row multi-chunk smallest-ingest reference test; 37/37 fixtures and dev MATCH compact/wide/string; packed crosscheck 12/12 including 20M; container 37/37 and dev MATCH. Image `shopping-rust-cp9` is **78,723,423 bytes**. Verified source and paired executable: `/tmp/rust-good-9/`.

### Full raw-work overlap decision

Running raw event dedup and raw click parsing alongside links, then resolving/partitioning after the join, passed 17 tests and all compact/wide/string fixture/dev checks. The links work was hidden, but the separate resolve stage cost about 0.11 s. Five paired process medians were **0.94 s versus 0.94 s**, with the overlapped variant slower in three pairs and peak RSS rising to 3,064,048 KiB. It was not retained; scheduling, signatures, logs and experimental profiling changes were restored to checkpoint nine.

## Checkpoint 10: parallel links and lower-instruction parsing

Link lines are parsed/packed in parallel, endpoints are interned with exact atomic CAS, and
union-find hooks the root with the larger immutable packed key under the smaller root.
Path halving and phase barriers preserve exact minimum-byte names; final lookup slots retain
the previous plain `(ID, name)` layout. The exact byte-string path remains sequential.
An independent adversarial review found no defect, including 25,000 amplified random graphs,
500 faithful graphs, multi-chunk inputs, and three detected planted mutants.

RunLevel's calendar table, eight-byte SWAR ID packing and flat product/category lookup are
ported without narrowing accepted input. Other calendar years and unpackable product IDs
retain exact fallbacks. The new calendar and every-byte packing regression tests bring the
unit-test count to 22.

Five paired 20M runs, CPU0-7 with the allocator tunables, improve checkpoint-nine best/median
**0.90/0.92 s** to **0.88/0.89 s**, with every pair faster. Parallel links alone was neutral
end to end; removing the serial links bound lets the parse improvements reduce wall time.
The isolated links benchmark measures sequential **0.2553/0.2608 s** versus parallel
**0.0400/0.0407 s** best/median:

```sh
GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.top_pad=268435456 \
DATA_DIR=data/s20m-2 NCPU=8 taskset -c 0-7 \
cargo test --release --bench links build_links -- --ignored --nocapture
```

A separate five-run native/container series measured the following process/container
start-to-exit times. Container lifetime comes from Podman's StartedAt/FinishedAt, with
`taskset -c 0-7` as the entrypoint, scorer flags, 8 CPUs and 12 GiB. Docker-client median
times, including engine overhead, were 0.147 s for dev and 0.985 s for 20M.

| Dataset | Native best/median | Native peak KiB | Container best/median | Container peak KiB |
|---|---:|---:|---:|---:|
| dev, 1M | 0.0510/0.0529 s | 179,496 | 0.0512/0.0544 s | 179,660 |
| s20m-2, 20M | 0.8683/0.8777 s | 2,814,660 | 0.8729/0.8840 s | 2,821,916 |

Linear 60M peak extrapolation is approximately **8.08 GiB**, not a 60M measurement.
Observed gates: 22 tests; 37/37 fixtures and dev MATCH compact/wide/string; 20M MATCH;
packed crosscheck 12/12; container 37/37 and dev MATCH. Scanner lab: 197 files,
30M generated CSV lines across N=2/4/7, 9.24M calendar cases and 10M mutated timestamps,
all **0 mismatches**. Image `shopping-rust-cp10`: **78,857,568 bytes**; measured
dependency-cached image build **13.65 s** (Rust compile 8.04 s).

## Checkpoint 11: indexed person groups and largest-first buckets

RunLevel's three bucket commits are ported together: 128 person buckets, largest-first
workers with private campaign totals, and exact local person indexing. Rows scatter
directly from dedup parts into person groups, avoiding flatten-then-scatter copies.
Only each person's rows are sorted; clicks from persons without events are discarded.
The former bucket sort/merge helpers and their obsolete test are replaced, not retained.

Five paired 20M runs improve checkpoint-ten best/median **0.88/0.88 s** to **0.79/0.80 s**.
All five pairs improve. The independent five-run resource series is reproducible with:

```sh
python3 benches/measure.py /tmp/rust-good-11/shopping-sessions shopping-rust-cp11
```

| Dataset | Native best/median | Native peak KiB | Container best/median | Container peak KiB |
|---|---:|---:|---:|---:|
| dev, 1M | 0.0478/0.0485 s | 175,976 | 0.0472/0.0502 s | 176,364 |
| s20m-2, 20M | 0.7786/0.7937 s | 2,816,036 | 0.7641/0.7940 s | 2,825,876 |

The helper uses automatic path/layout, leaves outputs under a unique `out-measure-*`
directory, reports process/container lifetimes separately from Docker-client overhead,
and uses the scorer flags with 8 CPUs, CPU0-7 affinity, 12 GiB and allocator tunables.
Linear 60M peak estimate: **8.09 GiB**; 60M was not run.

Observed gates: 22 tests, 37/37 and dev MATCH compact/wide/string, native/container 20M MATCH,
container 37/37 and dev MATCH, packed crosscheck 12/12. A separate native run with one
thread and every dev part-file order reversed also MATCHed, using the two positional
directory arguments. Image `shopping-rust-cp11`: **78,866,782 bytes**; dependency-cached
build **14.55 s** (Rust compile 8.53 s).

Five-run phase-profile medians (wall/CPU seconds) are links+parse envelope **0.319**
(links CPU **0.288**, events CPU **2.214**), dedup **0.1665/1.2747**, clicks
**0.0684/0.5249**, interleaved bucket envelope about **0.179** (sum CPU **1.295**),
and teardown **0.0421/0.0418**. Bucket event-sort CPU is **0.3173**, click-sort
**0.0952**, grouping **0.1407**, scatter **0.1805**, fused processing **0.4655**,
and Parquet **0.0960**. These bucket envelopes overlap and must not be added.

## Checkpoint 12: reusable dense active campaigns

RunLevel's `Active` design replaces the per-person campaign-count hash map with worker-local
counts, positions and an active list. Enter/leave are O(1); assists visit only present
campaigns. Clearing between persons preserves the previous seven-day window semantics.
A new 20K-operation reference test covers counts, swap removals and repeated clears.

The first five paired runs measured checkpoint-eleven/twelve medians **0.80/0.79 s**.
A second seven-pair series measured best/median **0.78/0.79 s** versus **0.77/0.78 s**,
five faster pairs and two ties at the external timer's 10 ms precision, none slower.
Fused processing CPU measured **0.377 s** versus the earlier **0.466 s** profile.
Other phases were slower in that later profile, so only paired wall comparisons decide
the keep; cross-series absolute clocks are not treated as an improvement.

The subsequent independent five-run resource series was noisier/slower overall:

| Dataset | Native best/median | Native peak KiB | Container best/median | Container peak KiB |
|---|---:|---:|---:|---:|
| dev, 1M | 0.0488/0.0531 s | 175,780 | 0.0484/0.0538 s | 177,024 |
| s20m-2, 20M | 0.8761/0.8946 s | 2,812,724 | 0.8709/0.8919 s | 2,826,008 |

Linear 60M peak estimate remains **8.09 GiB**. Observed gates: 23 tests; 37/37 and dev
MATCH compact/wide/string; native/container 20M MATCH; container 37/37 and dev MATCH;
packed crosscheck 12/12; one-thread reversed dev part-file order MATCH.
Image `shopping-rust-cp12`: **78,865,761 bytes**; dependency-cached build **15.38 s**
(Rust compile 9.60 s). The source and paired binary are in `/tmp/rust-good-12/`.

### Rejected click count/scatter routing

Reusing per-worker click scratch, routing into one contiguous vector with prefix offsets,
and borrowing its bucket slices in `group()` passed 24 tests, 37/37 and dev MATCH in all
three modes, and 20M MATCH. Five paired process runs measured **0.74/0.75 s** best/median
for both baseline and candidate: two pairs faster, two slower, one tied. There was no
end-to-end win, so the parser, generic routed wrapper, iterator API and new routing-only
test were fully restored to checkpoint twelve.

## Checkpoint 13: parallel mapped-page release before ordinary unmap

After every pipeline and writer has completed, worker threads issue best-effort
`MADV_DONTNEED` for input mappings, then the normal destructors unmap them. This clears
page-table entries in parallel without skipping teardown or discarding the file cache.
The mappings are shared/read-only; no mapped-byte borrows remain. Independent review
found no lifetime issue and required ignoring advisory errors, which is implemented:
a failed hint cannot turn fully written results into an unsuccessful process exit.

Five rotating-order paired 20M runs improved native median **0.7859→0.7406 s** and
container start-to-exit median **0.7741→0.7479 s**, every pair faster. The measured
external-minus-body cleanup gap fell from roughly **38–43 ms to 12–14 ms**.
Plain parallel `munmap` did not show a consistent cleanup improvement and was not retained.
Only external process/container wall time, not the internal `total` timer, decides this.

After the optional-hint error-handling correction, the separate five-run series measured:

| Dataset | Native best/median | Native peak KiB | Container best/median | Container peak KiB |
|---|---:|---:|---:|---:|
| dev, 1M | 0.0443/0.0458 s | 175,508 | 0.0469/0.0487 s | 177,536 |
| s20m-2, 20M | 0.7443/0.7735 s | 2,804,000 | 0.7528/0.7596 s | 2,829,052 |

60M peak extrapolation: **8.10 GiB**, not a 60M measurement. The benefit may shrink on
filesystems already mapping input through huge pages; correctness does not depend on
the hint succeeding. The 0.30 s local target remains unmet.

Observed corrected-source gates: 23 tests; 37/37 and dev MATCH compact/wide/string;
native/container 20M MATCH; container 37/37 and dev MATCH; crosscheck 12/12;
one-thread reversed dev file order MATCH. Image `shopping-rust-cp13`: **78,876,513 bytes**;
dependency-cached build **15.25 s** (Rust compile 9.45 s). Frozen source and native binary:
`/tmp/rust-good-13/`.

