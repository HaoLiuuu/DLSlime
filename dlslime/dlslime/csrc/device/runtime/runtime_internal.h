#pragma once
#include <functional>
#include <mutex>
#include <vector>

#include "device_runtime.h"

namespace dlslime::device::detail {
using NativeHandle = uintptr_t;
class Driver {
public:
    virtual ~Driver()                                                                           = default;
    virtual DeviceInfo   info() const                                                           = 0;
    virtual Capabilities capabilities() const                                                   = 0;
    virtual void*        allocate(size_t, MemoryKind)                                           = 0;
    virtual void         release(void*, MemoryKind)                                             = 0;
    virtual PointerInfo  query(const void*) const                                               = 0;
    virtual NativeHandle createStream()                                                         = 0;
    virtual void         destroyStream(NativeHandle)                                            = 0;
    virtual void         synchronize(NativeHandle)                                              = 0;
    virtual NativeHandle createEvent()                                                          = 0;
    virtual void         destroyEvent(NativeHandle)                                             = 0;
    virtual void         record(NativeHandle event, NativeHandle stream)                        = 0;
    virtual bool         ready(NativeHandle)                                                    = 0;
    virtual void         wait(NativeHandle)                                                     = 0;
    virtual void         streamWait(NativeHandle stream, NativeHandle event)                    = 0;
    virtual void         copy(void*, MemoryKind, const void*, MemoryKind, size_t, NativeHandle) = 0;
};
// If quiescence cannot be proved, freeing allocations would risk DMA use-after-free.
// Resource destructors therefore fail closed instead of swallowing driver errors.
template<class F>
void cleanup(F&& f) noexcept
{
    try {
        f();
    }
    catch (...) {
        std::terminate();
    }
}
struct AllocationState {
    std::shared_ptr<Driver> driver;
    void*                   pointer = nullptr;
    PointerInfo             info;
    ~AllocationState()
    {
        if (pointer)
            cleanup([&] { driver->release(pointer, info.kind); });
    }
};
struct EventState {
    std::shared_ptr<Driver>                       driver;
    NativeHandle                                  handle  = 0;
    bool                                          created = false, recorded = false;
    std::vector<std::shared_ptr<AllocationState>> allocations;
    std::vector<std::shared_ptr<EventState>>      dependencies;
    ~EventState()
    {
        if (created)
            cleanup([&] {
                if (recorded)
                    driver->wait(handle);
                driver->destroyEvent(handle);
            });
    }
};
struct StreamState {
    std::shared_ptr<Driver>                  driver;
    NativeHandle                             handle  = 0;
    bool                                     created = false;
    std::mutex                               mutex;
    std::vector<std::shared_ptr<EventState>> dependencies;
    ~StreamState()
    {
        if (!created)
            return;
        cleanup([&] {
            driver->synchronize(handle);
            driver->destroyStream(handle);
        });
    }
};
std::shared_ptr<Driver> makeHost();
std::shared_ptr<Driver> makeCuda(int);
std::shared_ptr<Driver> makeMaca(int);
std::shared_ptr<Driver> makeAscend(int);
int                     pciNumaNode(const std::string& pci);
}  // namespace dlslime::device::detail
