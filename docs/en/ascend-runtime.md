# Ascend staging runtime and HOST-only verbs policy

Enable `DLSLIME_RUNTIME_ASCEND` with `DLSLIME_ASCEND_ROOT` pointing to CANN.
The backend allocates HBM and pinned DRAM, performs H2D/D2H/D2D copies and exposes
immutable completion events and stream dependencies. Device copies require
runtime-owned pinned buffers. Unknown pointer classifications stay UNKNOWN.

ACL cannot portably restore an absent caller context. A runtime therefore owns
one executor thread and one explicitly created context. ACL operations execute
on that thread; GPU copies remain asynchronous. Caller threads and framework
contexts are not rebound. The extra submission hop is included in API performance
measurements. Stream/event/allocation owners drain before releasing resources.
Only this runtime's context is destroyed. The library never resets a device or
calls process-global aclFinalize, since another component can share ACL state.
PCI/NUMA fields remain unknown until an authoritative logical-to-physical mapping
is available; logical device IDs are never formatted into invented PCI addresses.

An Ascend build unconditionally validates every local MR registration before
cache lookup or re-registration. HBM, managed and UNKNOWN pointers are rejected;
there is no memory-kind hint override. Owned allocations also enforce exact
ranges. External ranges must be fully readable/writable CPU mappings and pass
the ACL HOST classification for every VMA segment. Driver-backed mappings are
rejected. This policy initializes independently of staging endpoints, so lack of
a DeviceRuntime instance cannot disable the guard. Pointer arithmetic overflow
is rejected before adding an endpoint offset.

The raw MR API still requires the caller to keep memory alive and avoid concurrent
free/re-map. Validation is not an allocation lease or a solution to those existing
raw-pointer lifetime requirements. Device-address aliases from aclrtHostRegister
or aclrtHostGetDevicePointer are not accepted HOST buffers. Use the original HOST
address, or allocate pinned DRAM through this runtime. Future device Region/MR
leases belong to PR06b.

Ascend transport always stages through DRAM for TCP and generic verbs RDMA. This
backend does not attempt an Ascend HBM ibv_reg_mr call or claim direct HBM RDMA.

CANN pinned HOST memory may use a device-backed CPU mapping. The registration
guard permits this only for a live, exact-range allocation owned by DeviceRuntime
and classified as HostPinned at allocation. Unowned device mappings remain
rejected, including caller-supplied HOST claims. A native test verifies that this
owned pinned DRAM reaches the real verbs provider while HBM never does.

## HOST copy memory and NIC qualification

Pinned HOST capability means that ACL can use the allocation for asynchronous
copies; it does not guarantee host-NIC MR support. On the validated 910B2 / CANN
9.1 / Linux 5.10 / mlx5_0 deployment, ordinary POSIX DRAM registers successfully,
but aclrtMallocHost memory is rejected by ibv_reg_mr with EFAULT. This is recorded
as unsupported provider qualification, not a successful MR test. The runtime
still provides the native pinned-copy allocator. Future RDMA staging must use
separate NIC-registered POSIX DRAM and ACL pinned-copy pools, with a pipelined
CPU copy between them, unless another allocator/provider combination is qualified.
TCP can use the CPU-accessible ACL allocation directly.

Provider registration errors propagate as system_error. A failed replacement
registration preserves the previous MR; raw callers must still quiesce users
before a successful MR growth changes its key. This does not add MR leases.

The Linux 5.10 deployment is not used to try registering ordinary pageable memory
through aclrtHostRegisterV2: its documented kernel restriction requires a separate
qualification. No kernel, driver, or host-network settings are changed.
