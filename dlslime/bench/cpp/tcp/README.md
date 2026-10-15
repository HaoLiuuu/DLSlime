# TCP DRAM benchmark

This benchmark calls the production `TcpEndpoint`, using standalone Asio 1.36.0. It measures HOST memory only, without accelerator copies, RDMA, vLLM, or SGLang.

## Measurement contract

- Two independent processes, each with an endpoint and its I/O thread. Each initiator has exactly one outstanding batch and one persistent outgoing connection. Batch size counts spans in one assignment. HOST READ batches pipeline ordered span requests on that connection, with a separate legacy header per span. The default window is 32 requests and 8 MiB of requested but not fully received payload; batch size is not connection count or the in-flight window. WRITE/SEND remain serial at the span level.
- Default per-span sizes: 1 KiB, 16 KiB, 256 KiB, 1 MiB, 4 MiB, 16 MiB. Batches: 1, 4, 16, 64. The maximum batch is 1 GiB. Four buffers plus guards require about 4 GiB per process, or 8 GiB for a loopback pair at the largest point.
- Six modes: READ initiated by A, B, or both simultaneously; WRITE initiated by A, B, or both simultaneously. READ initiated by A carries data B to A. WRITE initiated by A carries data A to B.
- Each point has 50 valid repetitions, each with a separate 1-second warmup and at least 3 measured seconds. The final complete batch can extend the interval. Case order uses a fixed random seed; repetitions within a case are contiguous. These are shared-machine repeated measurements, not independent machine samples.
- Source patterns change between warmup and measurement and between repetitions. After every interval, the entire final batch and 64-byte guards are checked. WRITE uses full READ-back outside timing. This verifies final buffer contents, not a checksum of every repeated network batch.
- WRITE Future completion is local socket retirement. A one-byte READ on the same persistent connection after the timed loop fences all preceding writes. `local_gbps` ends at the last local Future; `confirmed_gbps` includes receiver drain/fence time. This fence is once per trial, not per batch, and does not change the production API. WRITE p50/p99 batch latency remains local-completion latency.
- READ completes when the local payload arrives. Duplex peer rates use each peer's actual interval. `aggregate_confirmed_gbps` divides combined payload by the controller wall interval, including control dispatch and statistics return. This is conservative. Both starts are dispatched concurrently; cross-host clocks are not assumed synchronized and there is no exact hardware barrier.
- Rates are decimal Gb/s: bytes times 8 divided by seconds and 1e9. Compare a full-duplex link per direction; do not compare its aggregate with one direction's 25 Gb/s limit. Loopback does not traverse the NIC.
- Buffers are reused throughout each interval: small working sets may fit in cache. Allocation, initialization, validation and setup are outside per-peer timing. NUMA policy is first touch on fixed process CPU sets (default A: 2,3; B: 4,5). No system tuning is performed.

## Build and run

Use the designated container and a development directory outside host bind mounts:

```sh
cmake -S dlslime/bench/cpp/tcp -B /opt/dlslime-dev/build/tcp-bench-release \
  -DCMAKE_BUILD_TYPE=Release -DTCP_SANITIZE=OFF \
  -DDLSLIME_ASIO_ROOT=/opt/dlslime-dev/deps/asio-asio-1-36-0/asio
cmake --build /opt/dlslime-dev/build/tcp-bench-release -j 2
python3 dlslime/bench/cpp/tcp/suite.py --kind h200 \
  --binary /opt/dlslime-dev/build/tcp-bench-release/tcp_dram_bench \
  --interface bond0 --out /opt/dlslime-dev/bench-results/local-h200
```

Use the actual management interface and kind (`metax`, `ascend`, `h200`). The local pair binds to 127.0.0.1. Each topology has 24 size combinations times 6 modes times 50 repetitions = 7,200 successful paired trials, at least eight hours of warmup and timing, plus validation and idle waits. Independent local topologies may run in parallel. Never overlap a local and cross-host test on the same host.

For a correctness smoke, add `--rounds 1 --warmup 0.05 --seconds 0.1 --sizes 1024 262144 16777216 --batches 1 64`. These short rates are not steady-state results.

For cross-host testing, start the peer with `--serve --bind-ip <peer-address>` in its designated container. Securely copy its `peer-private.json` to the controller container; it contains a per-run secret and must not be published. Start the controller with `--remote <private-manifest> --bind-ip <controller-address>`. The peer accepts one authenticated controller and only bounded benchmark commands. Use a trusted test network: the legacy TCP data protocol is not authenticated or encrypted.

## READ pipeline controls

Use `--read-window 1` for the serial READ control, or `--read-window 32 --read-window-bytes 8388608` for the defaults. Limits apply per submitted batch/connection, not across the endpoint. A span larger than the byte budget runs alone, so the budget is a credit limit rather than a strict allocation cap. Requests are refilled as ordered responses finish; responses go directly into the caller's registered HOST buffers. A single-span batch retains the serial path. Device destinations also retain the existing staging path.

Both settings are recorded in metadata, checked on resume, and forwarded to both benchmark endpoints at setup. For a controlled comparison, use the same Release binary, sizes, affinities, duration and repetitions with separate result directories. Observe background load and alternate configuration order where possible. Batch p50/p99 remains the time to receive the entire READ batch; it is not per-span network RTT.

The C++ and Python endpoint API is `set_read_pipeline_limits(max_requests, max_bytes)`. Values are validated (1..4096 requests, 1..1 GiB bytes) and snapshotted when each operation is submitted. Changing the setting affects later operations. The wire protocol and server response order are unchanged; a pipelined client can use an existing sequential server.

## Workload gate and recovery

The runner holds a container-private lock and samples system CPU and accelerator utilization every five seconds. It starts/resumes after 60 continuous seconds with CPU below 5% and every accelerator below 5%. Before each case it also samples the management interface for one second and waits if either direction carries at least 100 Mb/s. Missing or stale telemetry closes the gate. No other processes are stopped or host configuration changed.

A busy sample overlapping a trial makes that attempt invalid; it does not count toward 50. The current trial finishes and the next waits for idle. Sampling can miss brief bursts. Network utilization is checked between cases because the benchmark itself generates network traffic. This gate does not establish exclusive machine access or guarantee zero competing traffic throughout a measurement. Inspect recorded telemetry and variance.

`status.json` reports waiting, running, complete, or failed. `trials.jsonl` retains all attempts with both peers, CPU time, durations, batch latency, verification and telemetry. `metadata.json` records source/transport/binary hashes, affinity and matrix. Identical reruns resume missing case/round IDs. Changed source or binaries are rejected. A timeout, EOF or checksum failure stops the suite; diagnose the error before retrying. Control calls have a 120-second watchdog and transport calls retain PR01 deadlines.

Keep raw output outside Git. Run sanitizer correctness checks separately. The performance suite rejects non-Release or sanitized builds; never treat smoke/debug results as 50-round Release results.
