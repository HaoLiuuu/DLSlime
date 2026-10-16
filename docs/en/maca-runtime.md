# MACA DeviceRuntime

Enable `DLSLIME_RUNTIME_MACA` with `DLSLIME_MACA_ROOT` pointing to the native MACA
SDK. This backend uses mcruntime directly; it does not impersonate CUDA. It owns
device/pinned-host allocations, streams and events under the common lifecycle
contract, restores the calling thread's device and reports each logical device's
own PCI address and NUMA placement.

DMA-BUF export requires the SDK's `mcMemGetHandleForAddressRangeExt` offset API.
The returned DmaBuf retains the allocation and owns a CLOEXEC descriptor. `fd()`
is borrowed; retain the DmaBuf through deregistration and never close that fd.
Offset is the SDK-returned allocation offset plus the checked BufferView offset.
The descriptor's actual extent must cover the complete requested range. Unknown
size or invalid offset is rejected. Allocation granularity is queried for the
selected logical device and reported as metadata, not used to guess an offset.
No fixed 64 MiB assumption or device-0 substitution is used.

Export capability does not qualify a NIC for direct HBM RDMA. Memory registration
and remote access must separately validate provider/device compatibility and
retain their own MR leases. That integration is part of PR06b.
