#!/usr/bin/env python3
import argparse, fcntl, hashlib, importlib.util, json, os, random, select, subprocess, time
from pathlib import Path


def save(path, value):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(value, indent=2))
    temp.replace(path)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--kind", required=True, choices=["metax", "ascend"])
    p.add_argument("--binary", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--interface", required=True)
    p.add_argument("--cpus", default="2,3")
    args = p.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[4]
    suite = Path(__file__).resolve().parents[1] / "rdma/suite.py"
    spec = importlib.util.spec_from_file_location("rdma_suite", suite)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    backend = "maca" if args.kind == "metax" else "ascend"
    cases = [
        (be, op, n, batch)
        for be, ops in [("host", ["H2H"]), (backend, ["H2D", "D2H", "D2D"])]
        for op in ops
        for n in mod.SIZES
        for batch in mod.BATCHES
    ]
    random.Random(20261016).shuffle(cases)
    with open("/opt/dlslime-dev/transport-benchmark.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        monitor = mod.LoadMonitor(args.kind, args.interface, 60)
        err = (out / "worker.stderr").open("x")
        worker = subprocess.Popen(
            ["taskset", "-c", args.cpus, args.binary, "--serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=err,
            text=True,
            bufsize=1,
        )
        save(
            out / "metadata.json",
            {
                "backend": backend,
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
                "orchestrator_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
                "completion": "one outstanding batch, per-span events, last-event wait and complete event retirement",
                "load_policy": "original CPU/GPU thresholds; invalidate and stop for review on overlapping load, including possible owned-GPU telemetry",
            },
        )
        done = 0
        try:
            with (out / "trials.jsonl").open("x", buffering=1) as raw:
                for be, op, n, batch in cases:
                    for round_id in range(5):
                        while True:
                            before = monitor.status(network=True)
                            save(
                                out / "status.json",
                                {
                                    "state": "waiting_idle",
                                    "valid_trials": done,
                                    "total_trials": 480,
                                    "case": [be, op, n, batch],
                                    "round": round_id,
                                    "load": before,
                                },
                            )
                            if before["ready"]:
                                break
                            time.sleep(5)
                        save(
                            out / "status.json",
                            {
                                "state": "running",
                                "valid_trials": done,
                                "total_trials": 480,
                                "case": [be, op, n, batch],
                                "round": round_id,
                            },
                        )
                        worker.stdin.write(
                            " ".join([be, "0", op, str(n), str(batch), "1", "3"]) + "\n"
                        )
                        worker.stdin.flush()
                        if not select.select([worker.stdout], [], [], 120)[0]:
                            raise TimeoutError("copy worker exceeded 120 seconds")
                        line = worker.stdout.readline()
                        if not line:
                            raise RuntimeError(
                                "copy worker exited; inspect retained stderr"
                            )
                        value = json.loads(line)
                        if value.get("error"):
                            raise RuntimeError(value["error"])
                        after = monitor.status()
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
                                "load overlapped measurement; retain record and inspect ownership before proceeding"
                            )
                        done += 1
            save(
                out / "status.json",
                {"state": "complete", "valid_trials": done, "total_trials": 480},
            )
        except BaseException as e:
            save(
                out / "status.json",
                {"state": "failed", "valid_trials": done, "error": str(e)},
            )
            raise
        finally:
            monitor.stop.set()
            if worker.poll() is None:
                worker.terminate()
                try:
                    worker.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    worker.wait()
            err.close()


if __name__ == "__main__":
    main()
