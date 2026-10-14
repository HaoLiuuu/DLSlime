# TCP request lifecycle and HOST regression tests

TCP uses standalone Asio 1.36.0. CMake fetches the checksum-pinned release or accepts `DLSLIME_ASIO_ROOT` pointing to that release's `asio/` directory for offline builds. Boost.Asio is not a fallback. No system package upgrade is required.

Each endpoint owns an I/O thread. It serializes socket operations, connection checkout/return, timers, accept, and shutdown. A pooled connection retains its socket; a request temporarily borrows the connection. A READ/WRITE batch advances all its spans on one connection, avoiding a fixed-width completion mask and one connection per span. Separate requests may use separate connections. Worker sharding, admission limits and the versioned protocol remain later PRs.

An operation future completes once all its asynchronous buffer references have retired. Keep source and destination memory alive until completion, including after `wait_for` returns false. `wait_for` only limits the caller's wait. The operation's `timeout_ms` covers queued I/O, resolution, connection and socket transfer; timeout cancels the connection and then waits for the cancellation callback before publishing `TCP_TIMEOUT`. Negative operation deadlines are rejected. Synchronous legacy CUDA copies are not interruptible by this deadline; asynchronous device staging belongs to the later runtime/pipeline work.

Legacy SEND/WRITE success means local socket I/O completed. It does not acknowledge remote delivery, remote DRAM commit, or HBM visibility. A failed or timed-out WRITE can have partially affected its destination and is not automatically replayed. READ success includes the local receive and existing CUDA copy when enabled. The new protocol will add target responses and stronger visibility contracts.

All local/remote assignment ranges are validated before a batch is submitted. Incoming legacy READ/WRITE addresses must lie within a registered local range. This is bounds checking, not the future Region identity/lease protocol. Callers still own registration lifetime. SEND mismatches and excess payload are drained with bounded scratch space to keep framing aligned; partial payload EOF fails the matched receive. Unknown opcodes and invalid READ/WRITE ranges close the stream.

Shutdown stops admission, closes the acceptor and active sockets, completes queued receives, and drains callbacks before destroying the I/O context. Concurrent callers serialize submission against shutdown. The API is intended for application threads, not the endpoint's I/O thread.

## Build and test

Run entirely inside the selected development container. For an offline Asio release extracted under `/opt/dlslime-dev/deps/`:

```sh
cmake -S dlslime/tests/cpp/tcp -B /opt/dlslime-dev/build/tcp-host \
  -DDLSLIME_ASIO_ROOT=/opt/dlslime-dev/deps/asio-asio-1-36-0/asio \
  -DCMAKE_BUILD_TYPE=Debug -DTCP_SANITIZE=ON
cmake --build /opt/dlslime-dev/build/tcp-host -j 4
ASAN_OPTIONS=detect_stack_use_after_return=1:detect_leaks=1 \
  ctest --test-dir /opt/dlslime-dev/build/tcp-host --output-on-failure
```

The standalone test builds the actual TCP sources, without GPU SDKs or mocked network I/O. The main project also adds the test with `BUILD_TEST=ON` and `BUILD_TCP=ON`; a CPU build can use `BUILD_RDMA=OFF` and `USE_CUDA=OFF`.

Coverage includes a backpressured header with a delayed executor, a deliberately stalled final span at 31/32/33/1000 spans, persistent connection reuse, deadline recovery, partial responses and EOF, mismatched/oversize/empty SEND frames, fragmented headers, invalid ranges/opcodes, batch prevalidation, concurrent registration/read, and shutdown with multiple active requests. Large SEND timeout verifies safe source retirement under backpressure.

For a functional two-host READ/WRITE smoke test, start `tcp_regression --serve <existing-management-IP> <manifest-file>` in one container. The manifest contains the dynamically selected control port. Run `tcp_regression --client <server-IP> <control-port>` in the other. Swap roles for the second direction. The test uses a same-connection READ after WRITE to verify the destination; it is not a bandwidth benchmark or a claim of remote completion for legacy WRITE.

Current cross-host acceptance is MetaX to/from Ascend on the existing 25 Gb/s management network. H200 runs local tests while its cross-host network is unavailable. This PR neither registers Ascend HBM with IBVerbs nor changes host networking, drivers, or vendor environments.
