# Portable DeviceRuntime

`dlslime::device::DeviceRuntime` is an additive C++ API. The existing signal and
endpoint APIs remain available. Enable `DLSLIME_RUNTIME_CUDA` for the native CUDA
Driver backend (defaults to `USE_CUDA`). The Driver API requires no new CUDA
kernel or runtime-language build. A requested backend that was not compiled is
an error; it never falls back to HOST.

## Memory and completion contract

Allocations carry a process-local identity, backend, logical device, memory kind
and checked range. A BufferView retains its allocation. `query` first resolves
owned ranges and then asks the selected vendor driver; UNKNOWN is never an
assertion that a pointer is HOST memory. External pointer classification does not
adopt ownership. External memory and framework allocator integration are future
Region work, not an implicit lifetime guarantee in this API.

CUDA allocates device and pinned host memory, restores the calling thread's
context after every operation, and retains/releases the primary context without
resetting it. Pageable HOST allocations are also available. Device copies require
pinned host allocations; HOST is synchronous and does not claim pinning or async
DMA support. Copies currently require allocations and streams from the same
runtime instance. Cross-device peer copies are not implied.

`copy` enqueues a copy and records an immutable completion event. The returned
Event retains source, destination and producer dependencies through completion.
Destroying its final incomplete handle waits. It does not cancel a DMA. A Stream
also drains before destroying its native handle. If driver quiescence fails in
a destructor, the process terminates instead of freeing possibly active buffers.
Explicit `wait` and `synchronize` surface driver errors to the caller. A caller
must not mutate source memory or consume destination memory before the event.

Failed native copy, event-record, and stream-wait submissions drain the affected
stream before their event or producer dependencies are released, because a native
call may accept work before reporting an error. The original error is rethrown
when draining succeeds; if draining fails, the process terminates with the
pending resources still retained. Successful submissions remain asynchronous.

Use `consumer.wait(producer_event)` before enqueueing dependent work. This is
device-stream ordering. `event.wait()` is CPU visibility for the completed copy;
neither constitutes transport delivery or a remote application ACK. Backend HBM
registration policy is a separate capability: CUDA requires NIC/provider
qualification. This PR does not change MR ownership or enable a transport's HBM
staging pipeline.

## Validation and performance

`device_runtime_test host` checks ranges, identity, guards, ownership and invalid
arguments. `device_runtime_test cuda` additionally exercises every visible CUDA
device, cross-stream ordering, external CUDA pointers and caller context
preservation. `device_runtime_test cuda stress` runs eight concurrent workers,
each doing 64 allocation/copy/event/release cycles. Run three bounded rounds.
GPU tests are explicit rather than automatic CTest cases so deployment runners
can defer them while business work is active.

`BUILD_DEVICE_BENCH=ON` builds `device_copy_bench`. It measures a single outstanding
batch, with one API event per span and completion/resource retirement included.
Arguments: backend, logical device, H2H/H2D/D2H/D2D, span bytes, batch size, warmup
seconds, measured seconds. Check the entire final batch and 64-byte guards after
both warmup and measurement, using different patterns. Output includes payload
Gb/s and per-batch P50/P99/P99.9 latency, not network RTT or kernel-only time.
Performance matrices use the transport's six sizes and four batch sizes, five
repetitions at one second warmup and three seconds measured. These copy results
are local staging capability measurements, not end-to-end transport performance.
