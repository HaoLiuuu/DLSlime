#include <fcntl.h>
#include <mcr/maca.h>
#include <mcr/mc_runtime_api.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>

#include "runtime_internal.h"
namespace dlslime::device::detail {
namespace {
void check(mcError_t rc, const char* action)
{
    if (rc != mcSuccess)
        throw std::runtime_error(std::string(action) + ": " + std::to_string(int(rc)) + ": " + mcGetErrorString(rc));
}
class DeviceScope {
    int previous_;

public:
    explicit DeviceScope(int device)
    {
        check(mcGetDevice(&previous_), "mcGetDevice");
        if (device != previous_)
            check(mcSetDevice(device), "mcSetDevice");
    }
    ~DeviceScope()
    {
        cleanup([&] { check(mcSetDevice(previous_), "mcSetDevice(restore)"); });
    }
};
}  // namespace
class MacaDriver final: public Driver {
    DeviceInfo info_;

public:
    explicit MacaDriver(int ordinal)
    {
        info_.backend = Backend::Maca;
        info_.ordinal = ordinal;
        check(mcGetDeviceCount(&info_.count), "mcGetDeviceCount");
        if (ordinal >= info_.count)
            throw std::invalid_argument("MACA device ordinal out of range");
        DeviceScope scope(ordinal);
        mcDevice_t  device;
        check(mcDeviceGet(&device, ordinal), "mcDeviceGet");
        char name[256], pci[32];
        check(mcDeviceGetName(name, sizeof(name), device), "mcDeviceGetName");
        info_.name = name;
        check(mcDeviceGetPCIBusId(pci, sizeof(pci), ordinal), "mcDeviceGetPCIBusId");
        info_.pci_bus_id = pci;
        std::transform(info_.pci_bus_id.begin(), info_.pci_bus_id.end(), info_.pci_bus_id.begin(), [](unsigned char c) {
            return std::tolower(c);
        });
        info_.numa_node = pciNumaNode(info_.pci_bus_id);
        check(mcDriverGetVersion(&info_.driver_version), "mcDriverGetVersion");
    }
    DeviceInfo info() const override
    {
        return info_;
    }
    Capabilities capabilities() const override
    {
        Capabilities c;
        c.device_allocation = c.pinned_host = c.asynchronous_copy = c.stream_events = c.external_pointer_query =
            c.dma_buf_export                                                        = true;
        c.hbm_registration = HbmRegistration::RequiresQualification;
        return c;
    }
    void* allocate(size_t n, MemoryKind k) override
    {
        DeviceScope scope(info_.ordinal);
        void*       p = nullptr;
        if (k == MemoryKind::Device)
            check(mcMalloc(&p, n), "mcMalloc");
        else if (k == MemoryKind::HostPinned)
            check(mcMallocHost(&p, n, mcMallocHostPortable), "mcMallocHost");
        else if (k == MemoryKind::HostPageable) {
            if (posix_memalign(&p, 64, n))
                throw std::bad_alloc();
        }
        else
            throw std::invalid_argument("unsupported MACA allocation kind");
        return p;
    }
    void release(void* p, MemoryKind k) override
    {
        DeviceScope scope(info_.ordinal);
        if (k == MemoryKind::Device)
            check(mcFree(p), "mcFree");
        else if (k == MemoryKind::HostPinned)
            check(mcFreeHost(p), "mcFreeHost");
        else
            std::free(p);
    }
    PointerInfo query(const void* p) const override
    {
        DeviceScope scope(info_.ordinal);
        PointerInfo i;
        i.backend = Backend::Maca;
        mcPointerAttribute_t a{};
        auto                 rc = mcPointerGetAttributes(&a, p);
        if (rc == mcErrorInvalidValue)
            return i;
        check(rc, "mcPointerGetAttributes");
        if (a.isManaged)
            i.kind = MemoryKind::Managed;
        else if (a.type == mcMemoryTypeDevice)
            i.kind = MemoryKind::Device;
        else if (a.type == mcMemoryTypeHost)
            i.kind = MemoryKind::HostPinned;
        else if (a.type == mcMemoryTypeUnregistered)
            i.kind = MemoryKind::HostPageable;
        if (i.kind == MemoryKind::Device || i.kind == MemoryKind::Managed) {
            i.device            = a.device;
            mcDeviceptr_t base  = nullptr;
            size_t        bytes = 0;
            auto rc = mcMemGetAddressRange(&base, &bytes, reinterpret_cast<mcDeviceptr_t>(const_cast<void*>(p)));
            if (rc == mcSuccess) {
                i.base        = reinterpret_cast<uintptr_t>(base);
                i.bytes       = bytes;
                i.offset      = reinterpret_cast<uintptr_t>(p) - i.base;
                i.range_known = true;
            }
            else if (rc != mcErrorInvalidValue)
                check(rc, "mcMemGetAddressRange");
        }
        return i;
    }
    NativeHandle createStream() override
    {
        DeviceScope scope(info_.ordinal);
        mcStream_t  s;
        check(mcStreamCreateWithFlags(&s, mcStreamNonBlocking), "mcStreamCreateWithFlags");
        return reinterpret_cast<NativeHandle>(s);
    }
    void destroyStream(NativeHandle s) override
    {
        DeviceScope scope(info_.ordinal);
        check(mcStreamDestroy(reinterpret_cast<mcStream_t>(s)), "mcStreamDestroy");
    }
    void synchronize(NativeHandle s) override
    {
        DeviceScope scope(info_.ordinal);
        check(mcStreamSynchronize(reinterpret_cast<mcStream_t>(s)), "mcStreamSynchronize");
    }
    NativeHandle createEvent() override
    {
        DeviceScope scope(info_.ordinal);
        mcEvent_t   e;
        check(mcEventCreateWithFlags(&e, mcEventDisableTiming), "mcEventCreateWithFlags");
        return reinterpret_cast<NativeHandle>(e);
    }
    void destroyEvent(NativeHandle e) override
    {
        DeviceScope scope(info_.ordinal);
        check(mcEventDestroy(reinterpret_cast<mcEvent_t>(e)), "mcEventDestroy");
    }
    void record(NativeHandle e, NativeHandle s) override
    {
        DeviceScope scope(info_.ordinal);
        check(mcEventRecord(reinterpret_cast<mcEvent_t>(e), reinterpret_cast<mcStream_t>(s)), "mcEventRecord");
    }
    bool ready(NativeHandle e) override
    {
        DeviceScope scope(info_.ordinal);
        auto        rc = mcEventQuery(reinterpret_cast<mcEvent_t>(e));
        if (rc == mcErrorNotReady)
            return false;
        check(rc, "mcEventQuery");
        return true;
    }
    void wait(NativeHandle e) override
    {
        DeviceScope scope(info_.ordinal);
        check(mcEventSynchronize(reinterpret_cast<mcEvent_t>(e)), "mcEventSynchronize");
    }
    void streamWait(NativeHandle s, NativeHandle e) override
    {
        DeviceScope scope(info_.ordinal);
        check(mcStreamWaitEvent(reinterpret_cast<mcStream_t>(s), reinterpret_cast<mcEvent_t>(e), 0),
              "mcStreamWaitEvent");
    }
    void copy(void* d, MemoryKind dk, const void* s, MemoryKind sk, size_t n, NativeHandle stream) override
    {
        DeviceScope scope(info_.ordinal);
        auto        native = reinterpret_cast<mcStream_t>(stream);
        if (dk != MemoryKind::Device && sk != MemoryKind::Device) {
            check(mcStreamSynchronize(native), "mcStreamSynchronize(host copy)");
            std::memmove(d, s, n);
            return;
        }
        auto kind = dk == MemoryKind::Device ?
                        (sk == MemoryKind::Device ? mcMemcpyDeviceToDevice : mcMemcpyHostToDevice) :
                        mcMemcpyDeviceToHost;
        check(mcMemcpyAsync(d, s, n, kind, native), "mcMemcpyAsync");
    }
    DmaBufExport exportDmaBuf(void* allocation, size_t allocationBytes, size_t viewOffset, size_t bytes) override
    {
        DeviceScope         scope(info_.ordinal);
        mcMemAllocationProp prop{};
        prop.type                 = mcMemAllocationTypePinned;
        prop.location.type        = mcMemLocationTypeDevice;
        prop.location.id          = info_.ordinal;
        prop.requestedHandleTypes = mcMemHandleTypePosixFileDescriptor;
        size_t granularity        = 0;
        check(mcMemGetAllocationGranularity(&granularity, &prop, MC_MEM_ALLOC_GRANULARITY_MINIMUM),
              "mcMemGetAllocationGranularity");
        if (!granularity)
            throw std::runtime_error("MACA export granularity unavailable");
        int      fd     = -1;
        uint64_t offset = 0;
        check(mcMemGetHandleForAddressRangeExt(
                  &fd, allocation, allocationBytes, mcMemHandleTypePosixFileDescriptor, 0, &offset),
              "mcMemGetHandleForAddressRangeExt");
        if (fd < 0)
            throw std::runtime_error("MACA export returned invalid descriptor");
        try {
            if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
                throw std::runtime_error("cannot set DMA-BUF CLOEXEC");
            off_t exported = lseek(fd, 0, SEEK_END);
            if (exported <= 0 || offset > size_t(exported) || viewOffset > size_t(exported) - offset
                || bytes > size_t(exported) - offset - viewOffset)
                throw std::runtime_error("DMA-BUF extent or backend offset does not cover requested range");
            // The SDK returns the actual allocation offset. No region alignment
            // or assumed device-0 granularity is used to invent an import offset.
            return {fd, size_t(offset) + viewOffset, bytes, size_t(exported), granularity};
        }
        catch (...) {
            ::close(fd);
            throw;
        }
    }
};
std::shared_ptr<Driver> makeMaca(int device)
{
    return std::make_shared<MacaDriver>(device);
}
}  // namespace dlslime::device::detail
