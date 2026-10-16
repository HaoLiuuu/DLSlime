# RDMA DRAM benchmark

Uses the production RDMAEndpoint, one outstanding batch per initiator and two
QPs. READ measures arrival of all local payloads. WRITE measures retirement of
all WR CQEs; it is not an application destination ACK. Full remote READ-back and
64-byte guards are verified outside the measured interval. Warmup and measured
trials use distinct patterns. Both directions separately and simultaneously use
the same six span sizes and four batch sizes as the TCP benchmark. Default:
one second warmup, three seconds measured, five repetitions (720 paired trials).

Set DLSLIME_BENCH_RDMA_DEVICE and the existing qualified GID explicitly. Use
BUILD_RDMA_BENCH=ON, BUILD_RDMA=ON, Release, no sanitizers. The binary is under
build/bin; suite.py takes --binary, --kind, --interface, --cpus-a/--cpus-b,
--bind-ip, and --out. The peer service accepts --serve and writes a private
peer-private.json; pass its contents through an authorized private channel to
the controller's --remote file. Never publish this file or endpoint metadata.
Each suite holds the transport-benchmark.lock, checks CPU/GPU/network activity,
and invalidates trials overlapping business load. It never stops other jobs.

Local means two independent processes using the selected NIC's same-node RDMA
path. This is not TCP loopback and cannot establish physical 25 Gb/s wire speed.
Cross-node tests use the existing MetaX/Ascend 25 Gb/s interfaces. H200 hardware
results are skipped when the designated container exposes no verbs devices.

P50/P99/P99.9 describe batch completion latency, not per-span network RTT. Samples
are capped at one million per trial and explicitly report truncation. Report
single-direction bandwidth separately from the sum of simultaneous directions.
The harness applies a 120-second command timeout; failure terminates only its own
benchmark process, keeps existing raw data, and is not silently retried.
