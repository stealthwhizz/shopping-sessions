"""Usage: python3 benches/measure.py BINARY IMAGE; requires data/dev and data/s20m-2."""
import datetime
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

root = Path(__file__).resolve().parents[1]
binary, image = str(Path(sys.argv[1]).resolve()), sys.argv[2]
env = os.environ | {
    "GLIBC_TUNABLES": "glibc.malloc.hugetlb=1:glibc.malloc.top_pad=268435456",
    "NCPU": "8",
}
for key in ("SHOPPING_PATH", "SHOPPING_LAYOUT"):
    env.pop(key, None)
outputs = Path(tempfile.mkdtemp(prefix="out-measure-", dir=root))
print(f"output_root={outputs}", flush=True)
with tempfile.TemporaryDirectory() as scratch:
    os.chmod(scratch, 0o777)
    for dataset in ("dev", "s20m-2"):
        result = {kind: [] for kind in ("native", "container")}
        for run in range(1, 6):
            for kind in result:
                out = outputs / f"{kind}-{dataset}"
                out.mkdir(exist_ok=True)
                out.chmod(0o777)
                if kind == "native":
                    child_env = env | {"DATA_DIR": str(root / "data" / dataset), "OUTPUT_DIR": str(out)}
                    command = ["taskset", "-c", "0-7", binary]
                else:
                    child_env = env
                    name = "shopping-measure-" + uuid.uuid4().hex
                    command = [
                        "docker", "run", "--name", name, "--platform", "linux/amd64",
                        "--network", "none", "--read-only", "--tmpfs", "/tmp:rw,size=64m,mode=1777",
                        "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                        "--pids-limit", "4096", "--cpus", "8", "--memory", "12g", "--memory-swap", "12g",
                        "--user", "1000:1000", "-v", f"{root}/data/{dataset}:/data:ro,z",
                        "-v", f"{out}:/output:z", "-v", f"{scratch}:/scratch:z",
                        "-e", "DATA_DIR=/data", "-e", "OUTPUT_DIR=/output", "-e", "SCRATCH_DIR=/scratch",
                        "-e", "HOME=/scratch", "-e", "NCPU=8", "-e", "MEM_GB=12",
                        "--entrypoint", "/usr/bin/taskset", image, "-c", "0-7", "/usr/local/bin/shopping-sessions",
                    ]
                start = time.perf_counter()
                completed = subprocess.run(command, env=child_env, capture_output=True, text=True)
                client_wall = time.perf_counter() - start
                try:
                    if completed.returncode:
                        sys.stderr.write(completed.stderr)
                        completed.check_returncode()
                    match = re.search(r"VmHWM:\s+(\d+) kB", completed.stderr)
                    if match is None:
                        raise RuntimeError(f"Missing VmHWM: {completed.stderr}")
                    rss = int(match.group(1))
                    if kind == "container":
                        state = json.loads(subprocess.check_output(["docker", "inspect", name, "--format", "{{json .State}}"], text=True))
                        started = datetime.datetime.fromisoformat(state["StartedAt"].replace("Z", "+00:00"))
                        finished = datetime.datetime.fromisoformat(state["FinishedAt"].replace("Z", "+00:00"))
                        wall = (finished - started).total_seconds()
                    else:
                        wall = client_wall
                    result[kind].append((wall, rss, client_wall))
                    print(f"MEASURE dataset={dataset} kind={kind} run={run} process_wall={wall:.6f} rss_kib={rss} client_wall={client_wall:.6f}", flush=True)
                finally:
                    if kind == "container":
                        subprocess.run(["docker", "rm", name], stdout=subprocess.DEVNULL, check=False)
        for kind, runs in result.items():
            print(f"SUMMARY dataset={dataset} kind={kind} best={min(r[0] for r in runs):.6f} median={statistics.median(r[0] for r in runs):.6f} peak_rss_kib={max(r[1] for r in runs)} client_median={statistics.median(r[2] for r in runs):.6f}", flush=True)
