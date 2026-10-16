# Acknowledged HOST Region TCP protocol (version 2)

## API and completion contract

`TcpRegionEndpoint` is an explicit opt-in endpoint with its own listener. It does
not share a listening port, sniff frames, or silently change the legacy
`TcpEndpoint` ABI/wire/completion behavior. `RegionPeer.protocol` must be 2. Peer
addresses are numeric IPv4/IPv6; metadata exchange is the caller's control plane.
This protocol is for trusted peers on a controlled network; scope/generation is
not authentication or encryption.

Register a fixed `HostBuffer`, or a C++ owned HOST range, in `endpoint.regions()`.
Keep the actual allocation in the supplied owner and do not resize/free it
independently. Python exposes fixed-size `HostBuffer` through the buffer protocol;
the registry retains the C++ allocation after its Python wrapper is collected.
`RegionSpan` names local and remote regions and separate byte offsets/length.
No remote virtual pointer is exposed or accepted on the wire.

| Operation/result                                                                     | Guarantee                                                                                                                                                                                  |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| WRITE success (0)                                                                    | Every span has a matching successful destination ACK, emitted only after the target received the complete HOST payload. All local socket callbacks have also retired.                      |
| READ success (0)                                                                     | Every matching response and its complete payload has arrived in the local destination; all local callbacks retired. READ data flows from the remote source to the initiator.               |
| Error (-1), deadline (-2), cancel/shutdown (-3), rejection (-4), protocol error (-5) | Local socket I/O has quiesced and local Region leases have been released. The remote outcome may be partial or unknown, including a successful write with a lost ACK. No automatic replay. |
| Busy (-6)                                                                            | Endpoint admission limit reached before network I/O or local leases were taken.                                                                                                            |

`wait_for` timing out only ends that wait; it does not cancel the operation.
Call `cancel()` and wait for the terminal Future before treating local buffers as
quiescent. A failed multi-span operation is not transactional and provides no
rollback. Remote errors can race a socket close and surface as I/O errors.

Success is HOST visibility, not HBM visibility or a remote framework callback.
Applications establish producer readiness and avoid conflicting concurrent access.
They convey transfer completion to their consumer through their control plane.
Legacy WRITE still means local completion and must not be compared to this ACK
latency without explicitly distinguishing those semantics.

## Region lifecycle

Each registry has a random 128-bit scope. Its slot ID and generation identify a
single registration; an old descriptor cannot authorize a reused address or a
registration in another endpoint incarnation. The receiver checks its current
registry, never trusts descriptor length/access supplied by a client, and acquires
a request-scoped lease before touching memory. The lease lasts through payload
and response/ACK I/O retirement.

`retire(key)` atomically stops admission and returns a retirement Future. Already
admitted requests remain valid. Later requests, including later spans of an
already submitted batch, are rejected. Drain completes after all admitted leases
retire and the registry releases its allocation owner reference. Only then can
that range be registered again. The key's generation changes. Owners/deleters must
not block the I/O thread or wait on their own retirement. Application-held owners
may keep storage alive longer. Permissions do not lock payload access.

There is no indefinitely cached remote allocation lease and no network release
message whose loss can pin storage forever. Partial requests and idle peers have
a server deadline; timeout closes their connection and drains local callbacks
before releasing the server lease. Closing a requester alone is not a distributed
QUIESCED certificate. The receiver's retirement Future is the authority for
releasing its memory. This HOST foundation does not register verbs MRs or handle
device allocation, multi-PD ownership, or device consumer fences.

## Framing

All fields use explicit big-endian encoding. The header is exactly 72 bytes,
independent of compiler padding and x86_64/aarch64 ABI.

| Offset | Bytes | Field                                                                 |
| ------ | ----- | --------------------------------------------------------------------- |
| 0      | 4     | Magic `DLS2`                                                          |
| 4      | 2     | Version, 2                                                            |
| 6      | 2     | Opcode: READ=1, WRITE=2; response sets bit 15                         |
| 8      | 4     | Status: success=0, stale=1, denied=2, bounds=3, closed=4, malformed=5 |
| 12     | 4     | Reserved, must be zero                                                |
| 16     | 8     | Nonzero request ID, never reused by the endpoint                      |
| 24     | 16    | Registry scope, high then low word                                    |
| 40     | 8     | Region slot ID                                                        |
| 48     | 8     | Generation                                                            |
| 56     | 8     | Region byte offset                                                    |
| 64     | 8     | Payload length                                                        |

READ sends a request header and receives a response header followed by exactly
`length` bytes. WRITE sends a header followed by `length` bytes and receives only
the response header. Responses echo every request identity/range field and must
match the next outstanding request. A nonzero response status has no payload.
Malformed framing closes the connection. Range/access/generation rejection sends
a NACK then closes, without draining an untrusted WRITE body. No later frame on
that connection is parsed after rejection.

## Bounds, pipelining and shutdown

An API batch has 1..4096 spans, each at most 1 GiB. All local ranges are validated
and leased before posting network work. Default per-batch credits are 32 requests
and 8 MiB of unacknowledged/unreceived payload. Both READ and WRITE refill the
window as ordered responses finish. One oversized span is admitted alone.
Each connection runs at most one composed Asio read and one composed write at a
time; sessions own headers and leases through the callbacks. Ordered READ payload
placement preserves overlapping destination-span order.

An endpoint admits at most 64 client batches and 64 server connections by default;
limits are configurable up to 4096. Idle outgoing connections are also capped by
the client limit. Server connections use a 30-second deadline per request,
including partial header/payload/response; idle connections expire too. Reusing
a peer-closed idle lane may return an I/O error; it is discarded and never replayed
implicitly. A subsequent operation uses a fresh lane.

Deadlines start at API submission, including queue/connect time. Shutdown closes
admission, cancels both socket directions, drains callbacks and joins the I/O
thread before closing the local registry. Cancellation uses its own I/O executor
queue and does not need data-window credits or peer cooperation. Future success
or failure never permits connection reuse while an old callback still has a
buffer reference.

## Validation

`tests/cpp/region` covers ownership, permissions, overflow/ranges, stale scope and
generation, same-address reuse, retirement, reentrant deletion, and admission
races. `tests/cpp/tcp` adds raw-peer tests for missing/mismatched ACKs, malformed
headers, partial EOF, deadline, bidirectional cancellation, bounded admission,
window refill, byte credits, overlapping destinations and receiver retirement.
The original 25-case legacy TCP regression remains in the same CTest run.
`region_python_smoke.py` exercises the public bindings. Cross-node correctness
tests must run the same commit in the designated containers; H200 has local
coverage only until the network is connected.
