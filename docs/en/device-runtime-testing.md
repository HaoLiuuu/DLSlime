# DeviceRuntime failure qualification

Native CUDA, MACA, and Ascend tests establish real allocation, pointer, copy, and event capabilities. The CPU fake driver supplies deterministic failure cases for the same common DeviceRuntime implementation; it is not a fallback backend or a hardware-performance model.

The standalone device_runtime_fault_test target compiles common runtime sources with a private test-access macro. Production targets do not define this macro and cannot select the fake through DeviceRuntime::create. Fake DEVICE and pinned allocations are ordinary CPU storage.

Queued work progresses only on event wait or stream synchronization. Tests cover delayed readiness; producer-to-consumer dependencies; callers dropping allocation/event owners; last-event and stream destruction draining work; allocation/stream/event creation failures; copy failure before and after enqueue; event-record failure after enqueue; and successful reuse after a drained failure. The fake rejects freeing memory referenced by queued copies. Child-process tests inject failed event wait and stream synchronization, asserting termination while pending allocations remain retained.

device_runtime_fault_test stress runs eight callers with 64 producer/consumer cycles each. Each cycle verifies all 4096 payload bytes and ends with no live allocations, streams, events, or queued copies. Execute three separate rounds. Run the standalone target under ASan/UBSan as well as Release; native performance remains a separate hardware qualification.

There is no DeviceRuntime cancel operation. Dropping the final event drains it. Request cancellation and transport leases belong to the later transfer/staging layer and must not be claimed as covered by these runtime tests.
