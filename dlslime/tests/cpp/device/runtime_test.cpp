#include <algorithm>
#include <atomic>
#include <cstring>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <vector>

#include "dlslime/csrc/device/runtime/device_runtime.h"
#ifdef DLSLIME_RUNTIME_CUDA_TEST
#include <cuda.h>
#endif
using namespace dlslime::device;
void check(bool b, const char* m)
{
    if (!b)
        throw std::runtime_error(m);
}
template<class F>
void rejected(F&& f)
{
    bool caught = false;
    try {
        f();
    }
    catch (const std::exception&) {
        caught = true;
    }
    check(caught, "expected error");
}
void exercise(const DeviceRuntime& rt, int loops)
{
    auto c    = rt.capabilities();
    auto host = c.pinned_host ? MemoryKind::HostPinned : MemoryKind::HostPageable;
    auto kind = c.device_allocation ? MemoryKind::Device : host;
    for (int iteration = 0; iteration < loops; ++iteration) {
        const size_t n = 65536 + iteration * 8, offset = 64;
        auto         source = rt.allocate(n + 128, host), target = rt.allocate(n + 128, kind),
             result   = rt.allocate(n + 128, host);
        auto producer = rt.createStream(), consumer = rt.createStream();
        std::memset(source.data(), 0xa7, source.size());
        std::memset(result.data(), 0x3c, result.size());
        {
            auto initialized = rt.copy(target.view(0, target.size()), source.view(0, source.size()), producer);
            initialized.wait();
        }
        auto* bytes = static_cast<unsigned char*>(source.data());
        for (size_t i = offset; i < offset + n; ++i)
            bytes[i] = static_cast<unsigned char>(i + iteration);
        auto ready = rt.copy(target.view(offset, n), source.view(offset, n), producer);
        consumer.wait(ready);
        auto  done = rt.copy(result.view(0, result.size()), target.view(0, target.size()), consumer);
        auto  id   = target.info().allocation_id;
        void* ptr  = target.data();
        target     = {};
        source     = {};
        check(rt.query(ptr).allocation_id == id, "pending event must retain allocation owner");
        ready = {};
        done.wait();
        check(done.ready(), "wait did not complete event");
        auto* got = static_cast<unsigned char*>(result.data());
        for (size_t i = 0; i < result.size(); ++i)
            check(got[i] == (i >= offset && i < offset + n ? static_cast<unsigned char>(i + iteration) : 0xa7),
                  "copy/guard mismatch");
        auto barrier = rt.record(consumer);
        consumer.wait(barrier);
        consumer.synchronize();
    }
}
void basic(Backend backend, int ordinal)
{
    auto rt   = DeviceRuntime::create(backend, ordinal);
    auto c    = rt.capabilities();
    auto info = rt.deviceInfo();
    check(info.backend == backend, "wrong backend");
    check(c.device_allocation == (backend != Backend::Host), "device capability mismatch");
    if (backend == Backend::Host) {
        check(!c.asynchronous_copy && !c.pinned_host, "HOST must not pretend pinned/async");
        rejected([&] { rt.allocate(8, MemoryKind::Device); });
    }
    auto a = rt.allocate(4096, MemoryKind::HostPageable);
    auto i = rt.query(static_cast<char*>(a.data()) + 9);
    check(i.kind == MemoryKind::HostPageable && i.owned && i.offset == 9 && i.bytes == 4096
              && i.allocation_id == a.info().allocation_id,
          "owned range classification");
    auto b = rt.allocate(4096, MemoryKind::HostPageable);
    check(b.info().allocation_id != a.info().allocation_id, "allocation identity collision");
    rejected([&] { a.view(4097, 0); });
    rejected([&] { a.view(0, std::numeric_limits<size_t>::max()); });
    rejected([&] { rt.allocate(0, MemoryKind::HostPageable); });
    rejected([&] { rt.allocate(1, MemoryKind::Managed); });
    auto s = rt.createStream();
    rejected([&] { rt.copy(a.view(0, 8), b.view(0, 16), s); });
    auto other = DeviceRuntime::create(Backend::Host);
    auto os    = other.createStream();
    rejected([&] { rt.record(os); });
    rejected([&] { s.wait(other.record(os)); });
    rejected([&] { rt.copy(other.allocate(8, MemoryKind::HostPageable).view(0, 8), a.view(0, 8), s); });
    auto empty = rt.copy(a.view(4096, 0), b.view(4096, 0), s);
    empty.wait();
    if (c.device_allocation) {
        auto d = rt.allocate(4096, MemoryKind::Device);
        rejected([&] { rt.copy(d.view(0, 4096), a.view(0, 4096), s); });
        rejected([&] { rt.copy(d.view(8, 128), d.view(0, 128), s); });
        check(rt.query(d.data()).kind == MemoryKind::Device, "device classified as HOST");
        check(c.hbm_registration != HbmRegistration::NotApplicable, "HBM policy absent");
    }
    exercise(rt, 8);
    std::cout << "PASS " << backendName(backend) << " device=" << ordinal << " pci=" << info.pci_bus_id
              << " numa=" << info.numa_node
              << " range, guards, ownership, producer/consumer ordering, rejected misuse\n";
}
#ifdef DLSLIME_RUNTIME_CUDA_TEST
void cudaExternal()
{
    check(cuInit(0) == CUDA_SUCCESS, "cuInit");
    CUdevice device;
    check(cuDeviceGet(&device, 0) == CUDA_SUCCESS, "device");
    CUcontext ctx;
    check(cuDevicePrimaryCtxRetain(&ctx, device) == CUDA_SUCCESS, "retain");
    check(cuCtxPushCurrent(ctx) == CUDA_SUCCESS, "push");
    CUdeviceptr p;
    check(cuMemAlloc(&p, 4096) == CUDA_SUCCESS, "external allocation");
    {
        auto rt = DeviceRuntime::create(Backend::Cuda);
        auto i  = rt.query(reinterpret_cast<void*>(p + 16));
        check(i.kind == MemoryKind::Device && !i.owned && i.range_known && i.base == p && i.offset == 16
                  && i.device == 0,
              "external CUDA classification");
        auto s = rt.createStream();
        auto e = rt.record(s);
        e.wait();
        CUcontext current;
        check(cuCtxGetCurrent(&current) == CUDA_SUCCESS && current == ctx, "caller context not restored");
    }
    check(cuMemFree(p) == CUDA_SUCCESS, "free");
    CUcontext popped;
    check(cuCtxPopCurrent(&popped) == CUDA_SUCCESS && popped == ctx, "pop");
    check(cuDevicePrimaryCtxRelease(device) == CUDA_SUCCESS, "release");
    std::cout << "PASS external CUDA pointer and caller context preservation\n";
}
#endif
int main(int argc, char** argv)
{
    try {
        Backend backend = Backend::Host;
        if (argc > 1) {
            std::string name = argv[1];
            if (name == "cuda")
                backend = Backend::Cuda;
            else if (name == "maca")
                backend = Backend::Maca;
            else if (name == "ascend")
                backend = Backend::Ascend;
            else if (name != "host")
                throw std::invalid_argument("unknown backend");
        }
        bool stress = argc > 2 && std::string(argv[2]) == "stress";
        if (stress) {
            auto                           rt = DeviceRuntime::create(backend);
            std::vector<std::future<void>> tasks;
            for (int t = 0; t < 8; ++t)
                tasks.push_back(std::async(std::launch::async, [rt] { exercise(rt, 64); }));
            for (auto& f : tasks)
                f.get();
            std::cout << "PASS 8 concurrent workers x 64 allocation/copy/event/release cycles\n";
        }
        else {
            basic(Backend::Host, 0);
            if (backend != Backend::Host) {
                auto rt    = DeviceRuntime::create(backend);
                int  count = rt.deviceInfo().count;
                for (int device = 0; device < count; ++device)
                    basic(backend, device);
#ifdef DLSLIME_RUNTIME_CUDA_TEST
                cudaExternal();
#endif
            }
        }
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FAIL " << e.what() << '\n';
        return 1;
    }
}
