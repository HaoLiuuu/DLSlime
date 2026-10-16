#include <acl/acl.h>

#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <future>
#include <thread>
#include <type_traits>

#include "runtime_internal.h"
namespace dlslime::device::detail {
namespace {
void check(aclError rc, const char* action)
{
    if (rc != ACL_SUCCESS)
        throw std::runtime_error(std::string(action) + ": ACL error " + std::to_string(rc));
}
void initializeAcl()
{
    static std::once_flag once;
    std::call_once(once, [] {
        auto rc = aclInit(nullptr);
        if (rc != ACL_ERROR_REPEAT_INITIALIZE)
            check(rc, "aclInit");
    });
    // ACL is process-global and may be shared with a framework. This library
    // never finalizes it or resets a device owned by another component.
}
class Executor {
    mutable std::mutex                        mutex_;
    mutable std::condition_variable           cv_;
    mutable std::deque<std::function<void()>> queue_;
    bool                                      stopping_ = false;
    std::thread                               worker_;

public:
    Executor():
        worker_([this] {
            for (;;) {
                std::function<void()> work;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    cv_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
                    if (queue_.empty() && stopping_)
                        return;
                    work = std::move(queue_.front());
                    queue_.pop_front();
                }
                work();
            }
        })
    {
    }
    ~Executor()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        cv_.notify_one();
        worker_.join();
    }
    template<class F>
    auto invoke(F&& f) const -> std::invoke_result_t<F>
    {
        using Result = std::invoke_result_t<F>;
        auto work    = std::make_shared<std::packaged_task<Result()>>(std::forward<F>(f));
        auto result  = work->get_future();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.emplace_back([work] { (*work)(); });
        }
        cv_.notify_one();
        return result.get();
    }
};
PointerInfo queryPointer(const void* p)
{
    PointerInfo i;
    i.backend = Backend::Ascend;
    aclrtPtrAttributes a{};
    auto               rc = aclrtPointerGetAttributes(p, &a);
    if (rc != ACL_SUCCESS)
        return i;  // Unknown is explicitly rejected by the registration policy.
    switch (a.location.type) {
        case ACL_MEM_LOCATION_TYPE_HOST:
        case ACL_MEM_LOCATION_TYPE_HOST_NUMA:
            i.kind = MemoryKind::HostPinned;
            break;
        case ACL_MEM_LOCATION_TYPE_UNREGISTERED:
            i.kind = MemoryKind::HostPageable;
            break;
        case ACL_MEM_LOCATION_TYPE_DEVICE:
            i.kind   = MemoryKind::Device;
            i.device = static_cast<int>(a.location.id);
            break;
        case ACL_MEM_LOCATION_TYPE_MANAGED:
            i.kind = MemoryKind::Managed;
            break;
        default:
            break;
    }
    if (i.kind == MemoryKind::Device) {
        void*  base = nullptr;
        size_t n    = 0;
        if (aclrtMemGetAddressRange(const_cast<void*>(p), &base, &n) == ACL_SUCCESS) {
            auto address = reinterpret_cast<uintptr_t>(p);
            i.base       = reinterpret_cast<uintptr_t>(base);
            if (address >= i.base && address - i.base < n) {
                i.bytes       = n;
                i.offset      = address - i.base;
                i.range_known = true;
            }
        }
    }
    return i;
}
}  // namespace
PointerInfo queryAscendForRegistration(const void* pointer)
{
    // Independent of DeviceRuntime construction or staging initialization.
    static Executor executor;
    return executor.invoke([=] {
        initializeAcl();
        return queryPointer(pointer);
    });
}
class AscendDriver final: public Driver {
    Executor     executor_;
    aclrtContext context_ = nullptr;
    DeviceInfo   info_;

public:
    explicit AscendDriver(int ordinal)
    {
        executor_.invoke([&] {
            initializeAcl();
            uint32_t count = 0;
            check(aclrtGetDeviceCount(&count), "aclrtGetDeviceCount");
            if (ordinal >= static_cast<int>(count))
                throw std::invalid_argument("Ascend device ordinal out of range");
            info_.backend = Backend::Ascend;
            info_.ordinal = ordinal;
            info_.count   = count;
            int32_t major, minor, patch;
            check(aclrtGetVersion(&major, &minor, &patch), "aclrtGetVersion");
            info_.driver_version = major * 10000 + minor * 100 + patch;
            check(aclrtCreateContext(&context_, ordinal), "aclrtCreateContext");
            try {
                if (auto name = aclrtGetSocName())
                    info_.name = name;
                else
                    info_.name = "Ascend";
            }
            catch (...) {
                cleanup([&] { check(aclrtDestroyContext(context_), "aclrtDestroyContext(construction failure)"); });
                context_ = nullptr;
                throw;
            }
            // No authoritative PCI mapping is provided by this ACL API. Unknown
            // is reported, rather than treating logical IDs as physical BDFs.
        });
    }
    ~AscendDriver() override
    {
        if (context_)
            cleanup([&] { executor_.invoke([&] { check(aclrtDestroyContext(context_), "aclrtDestroyContext"); }); });
    }
    DeviceInfo info() const override
    {
        return info_;
    }
    Capabilities capabilities() const override
    {
        Capabilities c;
        c.device_allocation = c.pinned_host = c.asynchronous_copy = c.stream_events = c.external_pointer_query = true;
        c.hbm_registration = HbmRegistration::Forbidden;
        return c;
    }
    void* allocate(size_t n, MemoryKind k) override
    {
        return executor_.invoke([=] {
            void* p = nullptr;
            if (k == MemoryKind::Device)
                check(aclrtMalloc(&p, n, ACL_MEM_MALLOC_NORMAL_ONLY), "aclrtMalloc");
            else if (k == MemoryKind::HostPinned)
                check(aclrtMallocHost(&p, n), "aclrtMallocHost");
            else if (k == MemoryKind::HostPageable) {
                if (posix_memalign(&p, 64, n))
                    throw std::bad_alloc();
            }
            else
                throw std::invalid_argument("unsupported Ascend allocation kind");
            return p;
        });
    }
    void release(void* p, MemoryKind k) override
    {
        executor_.invoke([=] {
            if (k == MemoryKind::Device)
                check(aclrtFree(p), "aclrtFree");
            else if (k == MemoryKind::HostPinned)
                check(aclrtFreeHost(p), "aclrtFreeHost");
            else
                std::free(p);
        });
    }
    PointerInfo query(const void* p) const override
    {
        return executor_.invoke([=] { return queryPointer(p); });
    }
    NativeHandle createStream() override
    {
        return executor_.invoke([] {
            aclrtStream s;
            check(aclrtCreateStream(&s), "aclrtCreateStream");
            return reinterpret_cast<NativeHandle>(s);
        });
    }
    void destroyStream(NativeHandle s) override
    {
        executor_.invoke([=] { check(aclrtDestroyStream(reinterpret_cast<aclrtStream>(s)), "aclrtDestroyStream"); });
    }
    void synchronize(NativeHandle s) override
    {
        executor_.invoke(
            [=] { check(aclrtSynchronizeStream(reinterpret_cast<aclrtStream>(s)), "aclrtSynchronizeStream"); });
    }
    NativeHandle createEvent() override
    {
        return executor_.invoke([] {
            aclrtEvent e;
            check(aclrtCreateEventWithFlag(&e, ACL_EVENT_SYNC), "aclrtCreateEventWithFlag");
            return reinterpret_cast<NativeHandle>(e);
        });
    }
    void destroyEvent(NativeHandle e) override
    {
        executor_.invoke([=] { check(aclrtDestroyEvent(reinterpret_cast<aclrtEvent>(e)), "aclrtDestroyEvent"); });
    }
    void record(NativeHandle e, NativeHandle s) override
    {
        executor_.invoke([=] {
            check(aclrtRecordEvent(reinterpret_cast<aclrtEvent>(e), reinterpret_cast<aclrtStream>(s)),
                  "aclrtRecordEvent");
        });
    }
    bool ready(NativeHandle e) override
    {
        return executor_.invoke([=] {
            aclrtEventRecordedStatus status;
            check(aclrtQueryEventStatus(reinterpret_cast<aclrtEvent>(e), &status), "aclrtQueryEventStatus");
            return status == ACL_EVENT_RECORDED_STATUS_COMPLETE;
        });
    }
    void wait(NativeHandle e) override
    {
        executor_.invoke(
            [=] { check(aclrtSynchronizeEvent(reinterpret_cast<aclrtEvent>(e)), "aclrtSynchronizeEvent"); });
    }
    void streamWait(NativeHandle s, NativeHandle e) override
    {
        executor_.invoke([=] {
            check(aclrtStreamWaitEvent(reinterpret_cast<aclrtStream>(s), reinterpret_cast<aclrtEvent>(e)),
                  "aclrtStreamWaitEvent");
        });
    }
    void copy(void* d, MemoryKind dk, const void* s, MemoryKind sk, size_t n, NativeHandle stream) override
    {
        executor_.invoke([=] {
            auto native = reinterpret_cast<aclrtStream>(stream);
            if (dk != MemoryKind::Device && sk != MemoryKind::Device) {
                check(aclrtSynchronizeStream(native), "aclrtSynchronizeStream(host copy)");
                std::memmove(d, s, n);
                return;
            }
            auto kind = dk == MemoryKind::Device ?
                            (sk == MemoryKind::Device ? ACL_MEMCPY_DEVICE_TO_DEVICE : ACL_MEMCPY_HOST_TO_DEVICE) :
                            ACL_MEMCPY_DEVICE_TO_HOST;
            check(aclrtMemcpyAsync(d, n, s, n, kind, native), "aclrtMemcpyAsync");
        });
    }
};
std::shared_ptr<Driver> makeAscend(int device)
{
    return std::make_shared<AscendDriver>(device);
}
}  // namespace dlslime::device::detail
