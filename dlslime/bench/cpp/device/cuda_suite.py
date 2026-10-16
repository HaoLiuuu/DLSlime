#!/usr/bin/env python3
"""Bounded local copy matrix with a CUDA-aware business-load guard."""
import argparse, concurrent.futures, fcntl, hashlib, json, os, random, select, subprocess, threading, time
from pathlib import Path


def cpu():
    v = list(map(int, Path("/proc/stat").read_text().splitlines()[0].split()[1:9]))
    return sum(v), v[3] + v[4]


class Guard:
    def __init__(self, binary, owner_file, telemetry):
        self.owner_file = owner_file
        self.telemetry = telemetry
        self.binary = str(Path(binary).resolve())
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.active = False
        self.data = {
            "ready": False,
            "busy_epoch": 0,
            "reason": "collecting idle baseline",
        }
        self.last_update = time.monotonic()
        self.thread = threading.Thread(target=self.watch, daemon=True)
        self.thread.start()

    def watch(self):
        previous = cpu()
        idle_since = None
        while not self.stop.wait(2):
            try:
                now = cpu()
                busy = 100 * (1 - (now[1] - previous[1]) / max(1, now[0] - previous[0]))
                previous = now
                apps = subprocess.check_output(
                    [
                        "nvidia-smi",
                        "--query-compute-apps=pid,process_name",
                        "--format=csv,noheader,nounits",
                    ],
                    text=True,
                    timeout=10,
                )
                entries = [
                    line.split(",", 1) for line in apps.splitlines() if line.strip()
                ]
                owner_pid = int(json.loads(self.owner_file.read_text())["host_pid"])
                foreign = [line for line in entries if int(line[0]) != owner_pid]
                own = [line for line in entries if int(line[0]) == owner_pid]
                util = [
                    int(x)
                    for x in subprocess.check_output(
                        [
                            "nvidia-smi",
                            "--query-gpu=utilization.gpu",
                            "--format=csv,noheader,nounits",
                        ],
                        text=True,
                        timeout=10,
                    ).splitlines()
                ]
                with self.lock:
                    active = self.active
                reasons = []
                if busy >= 5:
                    reasons.append("host CPU >= 5%")
                if foreign:
                    reasons.append("foreign compute process")
                if len(own) > 1:
                    reasons.append("unexpected duplicate benchmark process")
                # The owned device-0 DMA is expected during measurement. Other
                # GPUs remain guarded. Idle baseline considers every GPU.
                if max(util[1:] if active else util, default=0) >= 5:
                    reasons.append("accelerator utilization >= 5%")
                if reasons:
                    idle_since = None
                elif idle_since is None:
                    idle_since = time.monotonic()
                with self.lock:
                    self.last_update = time.monotonic()
                    self.data = {
                        "ready": idle_since is not None
                        and time.monotonic() - idle_since >= 60,
                        "busy_epoch": self.data["busy_epoch"] + bool(reasons),
                        "cpu_busy_percent": busy,
                        "gpu_util_percent": util,
                        "reason": (
                            ", ".join(reasons) if reasons else "idle or owned benchmark"
                        ),
                        "foreign_compute_processes": len(foreign),
                    }
                    with self.telemetry.open("a") as log:
                        log.write(json.dumps(self.data) + "\n")
            except Exception as e:
                idle_since = None
                with self.lock:
                    self.last_update = time.monotonic()
                    self.data = {
                        "ready": False,
                        "busy_epoch": self.data["busy_epoch"] + 1,
                        "reason": str(e),
                    }

    def status(self):
        with self.lock:
            d = dict(self.data)
            if time.monotonic() - self.last_update > 30:
                d.update(ready=False, reason="stale telemetry")
            return d


def save(path, obj):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=2))
    tmp.replace(path)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--binary", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--cpus", default="2,3")
    p.add_argument("--run-id", required=True)
    args = p.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    with open("/opt/dlslime-dev/transport-benchmark.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        stderr = (out / "worker.stderr").open("x")
        worker = subprocess.Popen(
            [
                "taskset",
                "-c",
                args.cpus,
                args.binary,
                "--serve",
                "--run-id",
                args.run_id,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            text=True,
            bufsize=1,
        )
        save(out / "worker.json", {"container_pid": worker.pid, "run_id": args.run_id})
        guard = Guard(args.binary, out / "owner-host-pid.json", out / "telemetry.jsonl")
        state = out / "status.json"
        cases = [
            (backend, op, n, batch)
            for backend, ops in [("host", ["H2H"]), ("cuda", ["H2D", "D2H", "D2D"])]
            for op in ops
            for n in [1024, 16384, 262144, 1048576, 4194304, 16777216]
            for batch in [1, 4, 16, 64]
        ]
        random.Random(20261016).shuffle(cases)
        root = Path(__file__).resolve().parents[4]
        save(
            out / "metadata.json",
            {
                "rounds": 5,
                "warmup_seconds": 1,
                "measure_seconds": 3,
                "expected_trials": 480,
                "case_order": cases,
                "cpus": args.cpus,
                "device": 0,
                "binary_sha256": hashlib.sha256(
                    Path(args.binary).read_bytes()
                ).hexdigest(),
                "runtime_library_sha256": hashlib.sha256(
                    (
                        Path(args.binary).parent.parent / "lib/lib_slime_device.so"
                    ).read_bytes()
                ).hexdigest(),
                "runtime_sha256": {
                    f.name: hashlib.sha256(f.read_bytes()).hexdigest()
                    for f in (root / "dlslime/dlslime/csrc/device/runtime").glob("*")
                    if f.is_file()
                },
                "source_sha256": {
                    f.name: hashlib.sha256(f.read_bytes()).hexdigest()
                    for f in Path(__file__).parent.glob("*")
                    if f.is_file()
                },
                "completion": "last event waited, every per-span Event retired; one outstanding batch",
                "latency": "CPU per-batch P50/P99/P99.9, includes API event allocation and retirement",
            },
        )
        done = 0
        try:
            with (out / "trials.jsonl").open("x", buffering=1) as raw:
                for backend, op, n, batch in cases:
                    for round_id in range(5):
                        while True:
                            before = guard.status()
                            save(
                                state,
                                {
                                    "state": "waiting_idle",
                                    "valid_trials": done,
                                    "total_trials": 480,
                                    "case": [backend, op, n, batch],
                                    "round": round_id,
                                    "load": before,
                                },
                            )
                            if before["ready"]:
                                break
                            time.sleep(5)
                        save(
                            state,
                            {
                                "state": "running",
                                "valid_trials": done,
                                "total_trials": 480,
                                "case": [backend, op, n, batch],
                                "round": round_id,
                            },
                        )
                        with guard.lock:
                            guard.active = True
                        try:
                            worker.stdin.write(
                                " ".join(
                                    [backend, "0", op, str(n), str(batch), "1", "3"]
                                )
                                + "\n"
                            )
                            worker.stdin.flush()
                            if not select.select([worker.stdout], [], [], 120)[0]:
                                raise TimeoutError("copy worker exceeded 120 seconds")
                            line = worker.stdout.readline()
                            if not line:
                                raise RuntimeError("copy worker exited")
                            value = json.loads(line)
                            if value.get("error"):
                                raise RuntimeError(value["error"])
                            # Keep the owned-work flag until the GPU sampling interval
                            # has expired, avoiding attribution of residual utilization.
                            time.sleep(3)
                            after = guard.status()
                        finally:
                            with guard.lock:
                                guard.active = False
                        value.update(
                            round=round_id,
                            load_before=before,
                            load_after=after,
                            valid=after["ready"]
                            and after["busy_epoch"] == before["busy_epoch"],
                        )
                        raw.write(json.dumps(value) + "\n")
                        raw.flush()
                        os.fsync(raw.fileno())
                        if not value["valid"]:
                            raise RuntimeError(
                                "business load overlapped measurement; results retained for review"
                            )
                        done += 1
            save(
                state, {"state": "complete", "valid_trials": done, "total_trials": 480}
            )
        except BaseException as e:
            save(state, {"state": "failed", "valid_trials": done, "error": str(e)})
            raise
        finally:
            guard.stop.set()
            if worker.poll() is None:
                worker.terminate()
                try:
                    worker.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    worker.wait()
            stderr.close()


if __name__ == "__main__":
    main()
