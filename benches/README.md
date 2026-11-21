# Rust entry

`target/release/shopping-sessions` reads `DATA_DIR` / `OUTPUT_DIR`, or its two positional arguments. `NCPU` selects the Rayon worker count. It needs no scratch files and writes only the two output folders.

The implementation follows `rules.md`: links use union-find with minimum-byte names; mmap-backed CSV fields are borrowed, not allocated per row; events are hash-partitioned by event ID and deduplicated before invalid retained copies are discarded; valid events and clicks are partitioned by canonical person. Per-person timestamp ordering drives sessions, attribution, and a sliding seven-day campaign-count window. Hash collisions are checked against the actual bytes, including the numeric-sort fast path. Identical click copies need not be materialized as a separate dedup table: repeating an identical click cannot change the winning click or whether its campaign's count is zero in the attribution window.

Outputs are deterministic, sorted before writing, scalar Parquet v1 with PLAIN/uncompressed pages, UTF-8 string annotations, DATE days, RLE definition levels for nullable categories, and losslessly selected INT32/INT64 columns. This deliberately narrow writer avoids Arrow, Thrift, compression libraries, and their startup/build costs. It rejects a data page over 2 GiB rather than emitting invalid Parquet.

## Dependencies

Direct, pinned: `memchr 2.7.6`, `memmap2 0.9.9`, `rayon 1.11.0`.

`Cargo.lock` also pins `crossbeam-deque 0.8.8`, `crossbeam-epoch 0.9.21`, `crossbeam-utils 0.8.23`, `either 1.19.0`, `libc 0.2.191`, and `rayon-core 1.13.0`. No runtime Python or third-party shared libraries are needed; the Debian runtime supplies glibc.

## Correctness commands

```sh
PY=../event/shopping/.venv/bin/python
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

## Final native measurements

Five warm-cache runs, `taskset -c 0-7`, `NCPU=8`; RSS is the maximum observed across the five runs.

| Dataset | Best wall | Median wall | Peak RSS |
|---|---:|---:|---:|
| dev, 1M events | 0.11 s | 0.11 s | 216,428 KiB (211 MiB) |
| s20m-2, 20M events | 3.05 s | 3.06 s | 4,308,092 KiB (4.11 GiB) |

The current binary passes all six Rust unit tests, all 37 fixtures, and the official dev comparison. Reversing all input part-file names and running with one worker produces byte-identical Parquet to the normal eight-worker run. An argv-only three-worker run is byte-identical too. A header-only dataset produces both zero-row tables with exactly the required columns and a DATE `day`.

The linear 60M memory extrapolation from the measured maximum is **12.33 GiB** (not a 60M measurement). Leave headroom for a different seed's linked-ID and output cardinalities. The requested sub-two-second target has not been reached on this laptop; correctness and the final-run three-minute ceiling have large measured margins, but a competitive practice score is not established.

## Final container measurements

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
