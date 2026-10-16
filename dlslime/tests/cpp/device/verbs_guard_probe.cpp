#include <dlfcn.h>
#include <infiniband/verbs.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#undef ibv_reg_mr
namespace {
std::atomic<uintptr_t> forbidden{0};
std::atomic<size_t>    forbiddenBytes{0};
std::atomic<uint64_t>  calls{0}, blocked{0};
}  // namespace
extern "C" void dlslime_probe_forbid(void* p, size_t n)
{
    forbidden      = reinterpret_cast<uintptr_t>(p);
    forbiddenBytes = n;
}
extern "C" uint64_t dlslime_probe_calls()
{
    return calls.load();
}
extern "C" uint64_t dlslime_probe_blocked()
{
    return blocked.load();
}
extern "C" ibv_mr* ibv_reg_mr(ibv_pd* pd, void* p, size_t n, int access)
{
    ++calls;
    auto a = reinterpret_cast<uintptr_t>(p), f = forbidden.load();
    auto bytes = forbiddenBytes.load();
    if (f && (a >= f ? a - f < bytes : f - a < n)) {
        ++blocked;
        errno = EACCES;
        return nullptr;
    }
    using Register     = ibv_mr* (*)(ibv_pd*, void*, size_t, int);
    static auto native = reinterpret_cast<Register>(dlsym(RTLD_NEXT, "ibv_reg_mr"));
    if (!native) {
        errno = ENOSYS;
        return nullptr;
    }
    return native(pd, p, n, access);
}
