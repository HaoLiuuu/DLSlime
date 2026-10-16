# RDMA completion and failure semantics

This change repairs the existing RDMA endpoint before adding device runtimes,
registered Region leases, or staged HBM transfers. It keeps the raw-MR API.

## Operation completion

Each logical READ, WRITE, or WRITE_WITH_IMM owns one operation state. A producer
reference prevents early completion while dispatch is still adding slots.
Success requires every slot on every QP to retire successfully. The final slot
uses WRITE_WITH_IMM on each QP; earlier slots use ordinary WRITE. Immediate data
is encoded and decoded in network byte order. Upgrade both peers together;
mixing this implementation with the old host-order immediate encoding is not
supported. QP counts are explicitly limited to 1..32 by the signal mask.

Every WR has a monotonically increasing completion ID and every send WR is
signaled. CQEs never dereference a recyclable assignment slot. Callbacks and MR
pool ownership are captured before posting and retained until all WRs in the
batch retire. This correctness-first policy increases CQ traffic compared with
the previous selective signaling; performance must be measured separately.

An unready operation retains its internal slot order without blocking unrelated
ready operations. Token charges are returned exactly once after batch retirement.
The producer and queue helpers are tested independently of RDMA hardware.

## Posting errors, cancellation, and memory

On a failed post, only the prefix before `bad_wr` is considered submitted. The
unsubmitted suffix retires immediately. The affected QP is destroyed before the
accepted prefix is cancelled, synchronizing DMA and callbacks. Invalid `bad_wr`
values conservatively retain all entries until successful QP destruction. If
destruction fails, accepted work remains pending; the library does not claim
quiescence or release its pool reference. Error CQEs do not expose valid opcode,
flags, or immediate data, so those fields are inspected only after success.

`shutdown()` and `cancelAll()` close the endpoint permanently, stop new
submissions, destroy its QPs, drain callbacks, and then fail remaining operations.
A Future that already completed keeps its original result. New submissions are
rejected. Public shutdown throws if QP destruction fails, allowing a retry while
the caller retains the endpoint and buffers. Destruction of an endpoint that
cannot quiesce terminates the process rather than freeing potentially DMA-visible
state. There is no automatic replay, rollback, reconnect, or operation deadline.

The caller still owns raw registered allocations and must keep them registered
and alive until the operation completes or successful shutdown proves local
quiescence. Keeping the MR pool alive is not an allocation lease and does not make
concurrent deregistration safe. The separate HOST Region API is not automatically
applied to RDMA MRs. A failed WRITE can leave partial remote changes. A sender's
WRITE CQE is not an application-level destination ACK or proof of HBM visibility.
WRITE_WITH_IMM plus the receiver Future provides a DRAM notification boundary;
device consumer-stream visibility belongs to the later runtime/Region work.

## Validation

`rdma_completion_tracker_test` needs no NIC. It covers WR counts 1/2/31/32/33/64/
65/257; all partial-post prefixes; CQEs before post returns; failed QP destruction;
late/duplicate IDs; error CQEs; independent QPs; reentrant retirement; 100 CQ/
destroy races; producer sealing; slot/QP aggregation; 32-bit mask boundaries; and
ready-queue ordering. Run it through CTest, including ASan/UBSan builds.

`rdma_correctness_smoke DEVICE QPS` needs a real RoCE device. It verifies payload
and guards across assignment counts 1/2/31/32/33/65/257/1025, actual RDMA READ,
WRITE readback, WRITE_WITH_IMM, repeated SEND/RECV slot reuse, invalid ranges,
remote-access errors, cancellation, and retained Future results. Use bounded
external process timeouts. Hardware checks must not run as implicit CTest cases.

`rdma_cross_smoke DEVICE ROLE QPS DIRECTORY` is a two-process DRAM test. Roles are
0 and 1. Exchange the private `endpoint.json` as the peer's `peer.json` after
both start, atomically renaming completed files. Each side runs the same 12
size/batch cases for three rounds, simultaneously issuing WRITE plus full READ
verification, native READ of the peer source, and WRITE_WITH_IMM. The private
bootstrap files contain MR addresses/rkeys and must not be published. After both
`done` files appear and both `result.json` files validate, create each `stop`
file so MRs remain alive until both peers finish. Keep an external timeout.

Suggested constrained test settings: `SLIME_MAX_SEND_WR=64`,
`SLIME_MAX_RECV_WR=64`, `SLIME_MAX_IO_FIFO_DEPTH=8`,
`SLIME_MAX_MSG_FIFO_DEPTH=8`, `SLIME_MAX_RD_ATOMIC=4`,
`SLIME_MAX_DEST_RD_ATOMIC=4`, and an explicitly verified `SLIME_GID_INDEX`.
The existing port, route, MTU, and device permissions must already support RoCE.

This stage validates HOST/DRAM only. Ascend HBM must never be passed to the
generic verbs registration path. Device-pointer enforcement, device-owned
Regions, multi-PD MR lifetimes, HBM staging, destination ACKs for RDMA, framework
integration, and throughput qualification are separate planned changes.
