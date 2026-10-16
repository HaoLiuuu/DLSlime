#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include "runtime_internal.h"
namespace dlslime::device::detail {
class HostDriver final: public Driver {
public:
    DeviceInfo info() const override
    {
        DeviceInfo i;
        i.backend = Backend::Host;
        i.name    = "HOST";
        return i;
    }
    Capabilities capabilities() const override
    {
        return {};
    }
    void* allocate(size_t n, MemoryKind kind) override
    {
        if (kind != MemoryKind::HostPageable)
            throw std::invalid_argument("HOST supports pageable allocation only");
        void* p = nullptr;
        if (posix_memalign(&p, 64, n))
            throw std::bad_alloc();
        return p;
    }
    void release(void* p, MemoryKind) override
    {
        std::free(p);
    }
    PointerInfo query(const void*) const override
    {
        return {};
    }
    NativeHandle createStream() override
    {
        return 0;
    }
    void         destroyStream(NativeHandle) override {}
    void         synchronize(NativeHandle) override {}
    NativeHandle createEvent() override
    {
        return 0;
    }
    void destroyEvent(NativeHandle) override {}
    void record(NativeHandle, NativeHandle) override {}
    bool ready(NativeHandle) override
    {
        return true;
    }
    void wait(NativeHandle) override {}
    void streamWait(NativeHandle, NativeHandle) override {}
    void copy(void* d, MemoryKind, const void* s, MemoryKind, size_t n, NativeHandle) override
    {
        std::memmove(d, s, n);
    }
};
std::shared_ptr<Driver> makeHost()
{
    return std::make_shared<HostDriver>();
}
}  // namespace dlslime::device::detail
