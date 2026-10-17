#include <acl/acl.h>
#include <dlfcn.h>
#include <sys/mman.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <system_error>

#include "dlslime/csrc/device/runtime/device_runtime.h"
#include "dlslime/csrc/engine/rdma/rdma_endpoint.h"
using namespace dlslime::device;
void check(bool v, const char* m)
{
    if (!v)
        throw std::runtime_error(m);
}
void ok(aclError rc)
{
    check(rc == ACL_SUCCESS, "ACL call failed");
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
    check(caught, "expected rejection");
}
int main(int argc, char** argv)
{
    try {
        check(argc == 2, "usage: ascend_runtime_test qualified-rdma-device");
        auto calls    = reinterpret_cast<uint64_t (*)()>(dlsym(RTLD_DEFAULT, "dlslime_probe_calls"));
        auto blocked  = reinterpret_cast<uint64_t (*)()>(dlsym(RTLD_DEFAULT, "dlslime_probe_blocked"));
        auto forbid   = reinterpret_cast<void (*)(void*, size_t)>(dlsym(RTLD_DEFAULT, "dlslime_probe_forbid"));
        auto failNext = reinterpret_cast<void (*)(int)>(dlsym(RTLD_DEFAULT, "dlslime_probe_fail_next"));
        check(calls && blocked && forbid && failNext, "verbs interception probe must be preloaded");
        ok(aclInit(nullptr));
        aclrtContext caller;
        ok(aclrtCreateContext(&caller, 0));
        void* hbm = nullptr;
        ok(aclrtMalloc(&hbm, 65536, ACL_MEM_MALLOC_NORMAL_ONLY));
        // The probe will refuse any unexpected HBM registration before the real
        // provider sees it. The production policy must reject even earlier.
        forbid(hbm, 65536);
        void* host = std::malloc(65536);
        check(host, "malloc");
        {
            auto endpoint = std::make_shared<dlslime::RDMAEndpoint>(argv[1], 1, "RoCE", 2);
            auto before   = calls();
            rejected([&] {
                endpoint->registerOrAccessMemoryRegion("forbidden.hbm", reinterpret_cast<uintptr_t>(hbm), 0, 4096);
            });
            check(calls() == before && blocked() == 0, "HBM reached the verbs registration boundary");
            rejected([&] { endpoint->registerOrAccessMemoryRegion("overflow", UINTPTR_MAX, 64, 4096); });
            auto hole = mmap(nullptr, 4096, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            check(hole != MAP_FAILED, "mmap");
            rejected(
                [&] { endpoint->registerOrAccessMemoryRegion("unknown", reinterpret_cast<uintptr_t>(hole), 0, 4096); });
            munmap(hole, 4096);
            check(calls() == before, "UNKNOWN/overflow reached verbs");
            auto first = endpoint->registerOrAccessMemoryRegion("host", reinterpret_cast<uintptr_t>(host), 0, 4096);
            check(first >= 0 && calls() == before + 1, "HOST registration did not reach real verbs");
            check(endpoint->registerOrAccessMemoryRegion("host", reinterpret_cast<uintptr_t>(host), 0, 4096) == first
                      && calls() == before + 1,
                  "cached HOST registration changed");
            auto retained = endpoint->get_local_pool()->get_mr_fast(first);
            failNext(ENOMEM);
            rejected(
                [&] { endpoint->registerOrAccessMemoryRegion("host", reinterpret_cast<uintptr_t>(host), 0, 8192); });
            check(endpoint->get_local_pool()->get_mr_fast(first) == retained && retained->length == 4096,
                  "failed MR growth invalidated the previous registration");
            failNext(EOPNOTSUPP);
            rejected([&] {
                endpoint->registerOrAccessMemoryRegion(
                    "new.failure", reinterpret_cast<uintptr_t>(host) + 32768, 0, 4096);
            });
            check(endpoint->registerOrAccessMemoryRegion(
                      "new.success", reinterpret_cast<uintptr_t>(host) + 32768, 0, 4096)
                      >= 0,
                  "failed new registration poisoned the cache");
            check(endpoint->registerOrAccessMemoryRegion("host", reinterpret_cast<uintptr_t>(host), 0, 8192) == first,
                  "MR growth recovery failed");
            endpoint->shutdown();
        }
        std::free(host);
        auto rt = DeviceRuntime::create(Backend::Ascend, 0);
        check(rt.capabilities().hbm_registration == HbmRegistration::Forbidden, "HBM policy not enforced");
        auto info = rt.query(static_cast<char*>(hbm) + 16);
        check(info.kind == MemoryKind::Device && !info.owned, "external Ascend HBM classification");
        auto pinned = rt.allocate(4096, MemoryKind::HostPinned);
        validateVerbsRegistration(pinned.data(), pinned.size());
        {
            auto endpoint = std::make_shared<dlslime::RDMAEndpoint>(argv[1], 1, "RoCE", 2);
            auto before   = calls();
            try {
                check(endpoint->registerOrAccessMemoryRegion(
                          "pinned.host", reinterpret_cast<uintptr_t>(pinned.data()), 0, pinned.size())
                          >= 0,
                      "owned pinned HOST registration returned invalid handle");
                std::cout << "QUALIFICATION ACL pinned HOST verbs registration: supported\n";
            }
            catch (const std::system_error& e) {
                // This kernel/provider cannot pin the CANN device-file mapping.
                // It is still HOST memory usable for ACL copies. Report the
                // independent NIC qualification, never pretend registration passed.
                if (e.code().value() != EFAULT)
                    throw;
                std::cout
                    << "QUALIFICATION ACL pinned HOST verbs registration: unavailable, EFAULT; use separate NIC DRAM and ACL pinned-copy pools\n";
            }
            check(calls() == before + 1 && blocked() == 0,
                  "pinned HOST did not reach the guarded provider exactly once");
            endpoint->shutdown();
        }
        auto owned = rt.allocate(4096, MemoryKind::Device);
        rejected([&] { validateVerbsRegistration(owned.data(), owned.size()); });
        aclrtContext after;
        ok(aclrtGetCurrentContext(&after));
        check(after == caller, "caller ACL context was changed");
        check(blocked() == 0, "probe blocked a forbidden call; production guard failed");
        forbid(nullptr, 0);
        ok(aclrtFree(hbm));
        ok(aclrtDestroyContext(caller));
        std::cout
            << "PASS Ascend cold-start HBM/UNKNOWN/overflow rejection before verbs, real pageable HOST registration/cache, provider failure recovery, separate pinned-HOST qualification, external pointer and caller context\n";
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FAIL " << e.what() << '\n';
        return 1;
    }
}
