#include <cuda.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include "runtime_internal.h"
namespace dlslime::device::detail {
namespace {
void check(CUresult rc, const char* action)
{
    if (rc == CUDA_SUCCESS)
        return;
    const char *name = nullptr, *message = nullptr;
    cuGetErrorName(rc, &name);
    cuGetErrorString(rc, &message);
    throw std::runtime_error(std::string(action) + ": " + (name ? name : "CUDA_ERROR") + " (" + std::to_string(int(rc))
                             + "): " + (message ? message : "unknown"));
}
class ContextScope {
public:
    explicit ContextScope(CUcontext c)
    {
        check(cuCtxPushCurrent(c), "cuCtxPushCurrent");
    }
    ~ContextScope()
    {
        cleanup([] {
            CUcontext popped;
            check(cuCtxPopCurrent(&popped), "cuCtxPopCurrent");
        });
    }
};
}  // namespace
class CudaDriver final: public Driver {
    CUdevice   device_;
    CUcontext  context_;
    DeviceInfo info_;

public:
    explicit CudaDriver(int ordinal)
    {
        check(cuInit(0), "cuInit");
        info_.backend = Backend::Cuda;
        info_.ordinal = ordinal;
        check(cuDeviceGetCount(&info_.count), "cuDeviceGetCount");
        check(cuDeviceGet(&device_, ordinal), "cuDeviceGet");
        char name[256], pci[32];
        check(cuDeviceGetName(name, sizeof(name), device_), "cuDeviceGetName");
        info_.name = name;
        check(cuDeviceGetPCIBusId(pci, sizeof(pci), device_), "cuDeviceGetPCIBusId");
        info_.pci_bus_id = pci;
        std::transform(info_.pci_bus_id.begin(), info_.pci_bus_id.end(), info_.pci_bus_id.begin(), [](unsigned char c) {
            return std::tolower(c);
        });
        info_.numa_node = pciNumaNode(info_.pci_bus_id);
        check(cuDriverGetVersion(&info_.driver_version), "cuDriverGetVersion");
        check(cuDevicePrimaryCtxRetain(&context_, device_), "cuDevicePrimaryCtxRetain");
    }
    ~CudaDriver() override
    {
        cleanup([&] { check(cuDevicePrimaryCtxRelease(device_), "cuDevicePrimaryCtxRelease"); });
    }
    DeviceInfo info() const override
    {
        return info_;
    }
    Capabilities capabilities() const override
    {
        Capabilities c;
        c.device_allocation = c.pinned_host = c.asynchronous_copy = c.stream_events = c.external_pointer_query = true;
        c.hbm_registration = HbmRegistration::RequiresQualification;
        return c;
    }
    void* allocate(size_t n, MemoryKind kind) override
    {
        ContextScope scope(context_);
        void*        p = nullptr;
        if (kind == MemoryKind::Device) {
            CUdeviceptr d;
            check(cuMemAlloc(&d, n), "cuMemAlloc");
            p = reinterpret_cast<void*>(d);
        }
        else if (kind == MemoryKind::HostPinned)
            check(cuMemHostAlloc(&p, n, CU_MEMHOSTALLOC_PORTABLE), "cuMemHostAlloc");
        else if (kind == MemoryKind::HostPageable) {
            if (posix_memalign(&p, 64, n))
                throw std::bad_alloc();
        }
        else
            throw std::invalid_argument("unsupported CUDA allocation kind");
        return p;
    }
    void release(void* p, MemoryKind k) override
    {
        ContextScope scope(context_);
        if (k == MemoryKind::Device)
            check(cuMemFree(reinterpret_cast<CUdeviceptr>(p)), "cuMemFree");
        else if (k == MemoryKind::HostPinned)
            check(cuMemFreeHost(p), "cuMemFreeHost");
        else
            std::free(p);
    }
    PointerInfo query(const void* p) const override
    {
        ContextScope scope(context_);
        PointerInfo  i;
        i.backend            = Backend::Cuda;
        unsigned int type    = 0;
        CUdeviceptr  address = reinterpret_cast<CUdeviceptr>(p);
        auto         rc      = cuPointerGetAttribute(&type, CU_POINTER_ATTRIBUTE_MEMORY_TYPE, address);
        if (rc == CUDA_ERROR_INVALID_VALUE)
            return i;
        check(rc, "cuPointerGetAttribute(memory type)");
        i.kind               = type == CU_MEMORYTYPE_HOST   ? MemoryKind::HostPinned :
                               type == CU_MEMORYTYPE_DEVICE ? MemoryKind::Device :
                                                              MemoryKind::Unknown;
        unsigned int managed = 0;
        check(cuPointerGetAttribute(&managed, CU_POINTER_ATTRIBUTE_IS_MANAGED, address),
              "cuPointerGetAttribute(managed)");
        if (managed)
            i.kind = MemoryKind::Managed;
        if (i.kind == MemoryKind::Device || i.kind == MemoryKind::Managed) {
            check(cuPointerGetAttribute(&i.device, CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL, address),
                  "cuPointerGetAttribute(device)");
            CUdeviceptr base  = 0;
            size_t      bytes = 0;
            auto        range = cuMemGetAddressRange(&base, &bytes, address);
            if (range == CUDA_SUCCESS) {
                i.base        = base;
                i.bytes       = bytes;
                i.offset      = address - base;
                i.range_known = true;
            }
            else if (range != CUDA_ERROR_INVALID_VALUE)
                check(range, "cuMemGetAddressRange");
        }
        return i;
    }
    NativeHandle createStream() override
    {
        ContextScope scope(context_);
        CUstream     s;
        check(cuStreamCreate(&s, CU_STREAM_NON_BLOCKING), "cuStreamCreate");
        return reinterpret_cast<NativeHandle>(s);
    }
    void destroyStream(NativeHandle s) override
    {
        ContextScope scope(context_);
        check(cuStreamDestroy(reinterpret_cast<CUstream>(s)), "cuStreamDestroy");
    }
    void synchronize(NativeHandle s) override
    {
        ContextScope scope(context_);
        check(cuStreamSynchronize(reinterpret_cast<CUstream>(s)), "cuStreamSynchronize");
    }
    NativeHandle createEvent() override
    {
        ContextScope scope(context_);
        CUevent      e;
        check(cuEventCreate(&e, CU_EVENT_DISABLE_TIMING), "cuEventCreate");
        return reinterpret_cast<NativeHandle>(e);
    }
    void destroyEvent(NativeHandle e) override
    {
        ContextScope scope(context_);
        check(cuEventDestroy(reinterpret_cast<CUevent>(e)), "cuEventDestroy");
    }
    void record(NativeHandle e, NativeHandle s) override
    {
        ContextScope scope(context_);
        check(cuEventRecord(reinterpret_cast<CUevent>(e), reinterpret_cast<CUstream>(s)), "cuEventRecord");
    }
    bool ready(NativeHandle e) override
    {
        ContextScope scope(context_);
        auto         rc = cuEventQuery(reinterpret_cast<CUevent>(e));
        if (rc == CUDA_ERROR_NOT_READY)
            return false;
        check(rc, "cuEventQuery");
        return true;
    }
    void wait(NativeHandle e) override
    {
        ContextScope scope(context_);
        check(cuEventSynchronize(reinterpret_cast<CUevent>(e)), "cuEventSynchronize");
    }
    void streamWait(NativeHandle s, NativeHandle e) override
    {
        ContextScope scope(context_);
        check(cuStreamWaitEvent(reinterpret_cast<CUstream>(s), reinterpret_cast<CUevent>(e), 0), "cuStreamWaitEvent");
    }
    void copy(void* d, MemoryKind dk, const void* s, MemoryKind sk, size_t n, NativeHandle stream) override
    {
        ContextScope scope(context_);
        auto         native = reinterpret_cast<CUstream>(stream);
        if (dk == MemoryKind::Device && sk == MemoryKind::Device)
            check(cuMemcpyDtoDAsync(reinterpret_cast<CUdeviceptr>(d), reinterpret_cast<CUdeviceptr>(s), n, native),
                  "cuMemcpyDtoDAsync");
        else if (dk == MemoryKind::Device)
            check(cuMemcpyHtoDAsync(reinterpret_cast<CUdeviceptr>(d), s, n, native), "cuMemcpyHtoDAsync");
        else if (sk == MemoryKind::Device)
            check(cuMemcpyDtoHAsync(d, reinterpret_cast<CUdeviceptr>(s), n, native), "cuMemcpyDtoHAsync");
        else {
            check(cuStreamSynchronize(native), "cuStreamSynchronize(host copy)");
            std::memmove(d, s, n);
        }
    }
};
std::shared_ptr<Driver> makeCuda(int device)
{
    return std::make_shared<CudaDriver>(device);
}
}  // namespace dlslime::device::detail
