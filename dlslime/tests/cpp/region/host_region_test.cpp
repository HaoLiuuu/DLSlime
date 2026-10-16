#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "dlslime/csrc/engine/region/host_region.h"
using namespace dlslime::region;
using namespace std::chrono_literals;
#define CHECK(x)                                                                                                       \
    do {                                                                                                               \
        if (!(x))                                                                                                      \
            throw std::runtime_error(#x);                                                                              \
    } while (0)
template<class F>
void rejects(Error expected, F f)
{
    try {
        f();
    }
    catch (const RegionError& e) {
        CHECK(e.code == expected);
        return;
    }
    throw std::runtime_error("Expected RegionError");
}
void permissions_and_bounds()
{
    HostRegionRegistry r, other;
    auto               b = std::make_shared<HostBuffer>(16);
    auto               d = r.register_buffer(b, Access::read);
    auto               l = r.acquire(d.key, 16, 0, Access::read);
    CHECK(l && !l.size());
    rejects(Error::permission, [&] { r.acquire(d.key, 0, 1, Access::write); });
    rejects(Error::bounds, [&] { r.acquire(d.key, UINT64_MAX, 1, Access::read); });
    rejects(Error::bounds, [&] { r.acquire(d.key, 1, UINT64_MAX, Access::read); });
    rejects(Error::stale, [&] { other.acquire(d.key, 0, 1, Access::read); });
    rejects(Error::overlap, [&] { r.register_owned_host(b, static_cast<char*>(b->data()) + 4, 8); });
    rejects(Error::invalid, [&] { r.register_owned_host({}, b->data(), 16); });
    rejects(Error::invalid, [&] { r.register_owned_host(b, reinterpret_cast<void*>(UINTPTR_MAX - 4), 8); });
}
void retire_and_reuse()
{
    HostRegionRegistry r;
    auto               b       = std::make_shared<HostBuffer>(16);
    auto               d       = r.register_buffer(b);
    auto               l       = r.acquire(d.key, 0, 16, Access::write);
    auto               drained = r.retire(d.key);
    auto               again   = r.retire(d.key);
    CHECK(drained.wait_for(0ms) == std::future_status::timeout);
    rejects(Error::stale, [&] { r.acquire(d.key, 0, 1, Access::read); });
    rejects(Error::overlap, [&] { r.register_buffer(b); });
    auto moved = std::move(l);
    CHECK(!l && moved);
    moved.reset();
    CHECK(drained.wait_for(0ms) == std::future_status::ready);
    again.get();
    auto fresh = r.register_buffer(b);
    CHECK(fresh.key.id == d.key.id && fresh.key.generation == d.key.generation + 1);
    rejects(Error::stale, [&] { r.acquire(d.key, 0, 1, Access::read); });
    CHECK(r.acquire(fresh.key, 0, 16, Access::read));
}
void owner_and_destruction()
{
    Lease            lease;
    Retirement       drained;
    std::atomic<int> destroyed{};
    {
        HostRegionRegistry r;
        auto               owner = std::shared_ptr<void>(new char[16], [&](void* p) {
            delete[] static_cast<char*>(p);
            ++destroyed;
        });
        auto               d     = r.register_owned_host(owner, owner.get(), 16);
        owner.reset();
        lease   = r.acquire(d.key, 0, 16, Access::write);
        drained = r.retire(d.key);
        CHECK(!destroyed);  // registry destruction must not invalidate the lease
    }
    static_cast<char*>(lease.data())[0] = 42;
    lease.reset();
    drained.get();
    CHECK(destroyed == 1);
}
void reentrant_deleter()
{
    HostRegionRegistry r;
    std::atomic<int>   destroyed{};
    auto               owner = std::shared_ptr<void>(new char[16], [&](void* p) {
        r.close();
        delete[] static_cast<char*>(p);
        ++destroyed;
    });
    auto               d     = r.register_owned_host(owner, owner.get(), 16);
    owner.reset();
    r.retire(d.key).get();
    CHECK(destroyed == 1);
    rejects(Error::closed, [&] { r.register_buffer(std::make_shared<HostBuffer>(16)); });
}
void admission_race()
{
    for (int round = 0; round < 100; ++round) {
        HostRegionRegistry r;
        auto               b = std::make_shared<HostBuffer>(64);
        auto               d = r.register_buffer(b);
        std::atomic<bool>  ready{}, stop{};
        std::atomic<int>   accepted{};
        std::thread        t([&] {
            ready = true;
            while (!stop) {
                try {
                    auto l = r.acquire(d.key, 0, 64, Access::read);
                    ++accepted;
                }
                catch (const RegionError& e) {
                    CHECK(e.code == Error::stale);
                    break;
                }
            }
        });
        while (!ready)
            std::this_thread::yield();
        auto f = r.retire(d.key);
        stop   = true;
        t.join();
        f.get();
        rejects(Error::stale, [&] { r.acquire(d.key, 0, 1, Access::read); });
    }
}
int main()
{
    permissions_and_bounds();
    retire_and_reuse();
    owner_and_destruction();
    reentrant_deleter();
    admission_race();
    std::cout
        << "PASS: permissions/bounds, retirement/reuse, owner lifetime, reentrant deletion, 100 admission races\n";
}
