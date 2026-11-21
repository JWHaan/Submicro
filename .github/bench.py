"""Same-host container wall times. Trace and compare runs are never timed."""
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time

lab = Path(os.environ["LAB"])
cpus = os.environ["CPUS"]
arms = ["base", "head"]
if os.environ["CODECS"] == "true":
    arms.append("uncompressed")


def run(arm, out, trace=False):
    out.mkdir(mode=0o777)
    out.chmod(0o777)
    # The runner owns these directories, not uid 1000, so it can unlink
    # container-written parts between measurements without sudo.
    for table in ("user_daily", "campaign_daily"):
        (out / table).mkdir()
        (out / table).chmod(0o777)
    with tempfile.TemporaryDirectory(dir=lab) as scratch:
        Path(scratch).chmod(0o777)
        cmd = ["docker", "run", "--rm", "--network=none", "--read-only", "--user=1000:1000",
               "--platform=linux/amd64", "--cap-drop=ALL", "--security-opt=no-new-privileges",
               "--tmpfs=/tmp:rw,size=64m", f"--cpus={cpus}", "--memory=12g", "--memory-swap=12g",
               "-e", f"NCPU={cpus}", "-v", f"{lab}/data:/data:ro", "-v", f"{out}:/output",
               "-v", f"{scratch}:/scratch"]
        if arm == "uncompressed":
            cmd += ["-e", "PARQUET_WRITER=uncompressed"]
        if trace:
            cmd += ["--entrypoint", "/bin/sh", "-e", "PHASE_TIMING=1"]
        cmd += ["bench-base" if arm == "base" else "bench-head"]
        if trace:
            cmd += ["-ec", '/bin/busybox time -v /app/run.sh; cat /sys/fs/cgroup/memory.peak >&2']
        start = time.monotonic_ns()
        p = subprocess.run(cmd, stdout=subprocess.PIPE, timeout=180, check=True)
        elapsed = (time.monotonic_ns() - start) / 1e6
        if p.stdout:
            raise RuntimeError(f"{arm} wrote {len(p.stdout)} bytes to stdout")
        return elapsed


timings = {arm: [] for arm in arms}
for i in range(int(os.environ["RUNS"]) + 1):
    order = arms[i % len(arms):] + arms[:i % len(arms)]
    for arm in order:
        with tempfile.TemporaryDirectory(dir=lab) as temp:
            ms = run(arm, Path(temp) / "out")
        if i:
            timings[arm].append(ms)
            print(f"pair {i} {arm}: {ms:.1f} ms", flush=True)
for arm, samples in timings.items():
    print(f"{arm} median {statistics.median(samples):.1f} ms; raw {samples}", flush=True)
    print(f"phase / resource trace: {arm}", flush=True)
    run(arm, lab / f"trace-{arm}", trace=True)
    if arm != "base":
        subprocess.run([lab / "env/bin/python", lab / "starter-duckdb/scripts/compare.py",
                        lab / "trace-base", lab / f"trace-{arm}"], check=True)
