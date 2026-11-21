#!/usr/bin/env python3
"""
Cross-check a candidate against the DuckDB reference on generated datasets.

    $PY tools/crosscheck.py [--seeds SEED:EVENTS ...] -- CANDIDATE_CMD...
    $PY tools/crosscheck.py find-leap [--start N] [--count K]

For each SEED:EVENTS, data goes to data/seeds/<seed>-<events>/ (generated once, then reused),
the reference and the candidate run natively (DATA_DIR/OUTPUT_DIR as env vars and as the last
two arguments), and scripts/compare.py diffs the outputs. Exits 1 on any DIFFER or failed run.
`find-leap` lists seeds whose 14-day window holds 2028-02-29 (probed with 1000-event datasets).
Run with a Python that has polars and duckdb (the scripts need both).
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SEEDS_DIR = ROOT / "data" / "seeds"
# Per-process so concurrent runs never delete each other's outputs.
OUT_DIR = ROOT / "data" / "crosscheck-out" / f"run-{os.getpid()}"
REFERENCE = [sys.executable, str(ROOT / "reference" / "pipeline" / "pipeline.py")]
LEAP_DAY = "2028-02-29T00:00:00Z"
# 15, 62, 249 hold 2028-02-29 (from `find-leap`); 10-18 are ordinary; 3 is the 20M run.
DEFAULT_SEEDS = ["15:1000000", "62:2000000", "249:1000000",
                 "10:1000000", "11:2000000", "12:1000000", "13:2000000",
                 "14:1000000", "16:2000000", "17:1000000", "18:2000000",
                 "3:20000000"]


def generate(seed, events, dest):
    """Generate into a temp sibling and rename, so a concurrent or killed run never leaves half a dataset."""
    if (dest / "manifest.json").is_file():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix=f".{dest.name}.", dir=dest.parent))
    subprocess.run(["generate", "--seed", str(seed), "--events", str(events), "--clicks", str(events // 4),
                    "--output", str(tmp)], check=True, stdout=subprocess.DEVNULL)
    try:
        tmp.rename(dest)
    except OSError:  # someone else finished it first
        shutil.rmtree(tmp)


def window(data):
    m = json.loads((data / "manifest.json").read_text())
    return m["window_start"], m["window_end"]


def run(cmd, data, out):
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out))
    t = time.perf_counter()
    p = subprocess.run(cmd + [str(data), str(out)], env=env, capture_output=True, text=True)
    return p, time.perf_counter() - t


def find_leap(start, count):
    found = []
    with tempfile.TemporaryDirectory() as tmp:
        seed = start
        while len(found) < count:
            d = Path(tmp) / str(seed)
            subprocess.run(["generate", "--seed", str(seed), "--events", "1000", "--clicks", "250",
                            "--output", str(d)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            ws, we = window(d)
            if ws <= LEAP_DAY < we:  # ISO-8601 Z strings compare like the instants they name
                found.append(seed)
                print(f"seed {seed}: {ws[:10]}..{we[:10]}", flush=True)
            shutil.rmtree(d)
            seed += 1
    return found


def crosscheck(specs, candidate):
    bad = 0
    for spec in specs:
        seed, events = (int(x) for x in spec.split(":"))
        name = f"{seed}-{events}"
        data = SEEDS_DIR / name
        generate(seed, events, data)
        ws, we = window(data)
        ref_out, cand_out = OUT_DIR / name / "ref", OUT_DIR / name / "cand"
        ref, ref_s = run(REFERENCE, data, ref_out)
        cand, cand_s = run(candidate, data, cand_out)
        if ref.returncode or cand.returncode:
            verdict = f"FAILED (ref exit {ref.returncode}, cand exit {cand.returncode})"
            for label, p in (("ref", ref), ("cand", cand)):
                if p.returncode:
                    print(f"  {label} stderr: {p.stderr[-1500:]}", file=sys.stderr)
        else:
            cmp = subprocess.run([sys.executable, str(ROOT / "scripts" / "compare.py"), str(cand_out), str(ref_out)],
                                 capture_output=True, text=True)
            verdict = "MATCH" if cmp.returncode == 0 else "DIFFER"
            if cmp.returncode:
                print(cmp.stdout, file=sys.stderr)
        bad += verdict != "MATCH"
        print(f"seed={seed:<4} events={events:<9} window={ws[:10]}..{we[:10]}  {verdict:<6}  "
              f"ref={ref_s:6.2f}s  cand={cand_s:6.2f}s", flush=True)
    print(f"{len(specs) - bad}/{len(specs)} seeds match")
    return 1 if bad else 0


def main():
    argv = sys.argv[1:]
    if argv[:1] == ["find-leap"]:
        ap = argparse.ArgumentParser(prog="crosscheck.py find-leap")
        ap.add_argument("--start", type=int, default=1)
        ap.add_argument("--count", type=int, default=2)
        a = ap.parse_args(argv[1:])
        find_leap(a.start, a.count)
        return 0
    if "--" not in argv:
        sys.exit(__doc__)
    sep = argv.index("--")
    ap = argparse.ArgumentParser(prog="crosscheck.py")
    ap.add_argument("--seeds", nargs="+", default=DEFAULT_SEEDS, metavar="SEED:EVENTS")
    a = ap.parse_args(argv[:sep])
    candidate = argv[sep + 1:]
    if not candidate:
        sys.exit("no candidate command after --")
    return crosscheck(a.seeds, candidate)


if __name__ == "__main__":
    sys.exit(main())
