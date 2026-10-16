#include <atomic>
#include <future>
#include <iostream>
#include <thread>

#include "dlslime/csrc/engine/rdma/completion_tracker.h"
using dlslime::rdma_detail::CompletionTracker;
#define CHECK(x)                                                                                                       \
    do {                                                                                                               \
        if (!(x))                                                                                                      \
            throw std::runtime_error(#x);                                                                              \
    } while (0)
struct Wr {
    uint64_t wr_id{};
};
void boundary(size_t count)
{
    CompletionTracker  t;
    std::vector<Wr>    w(count);
    int                calls = 0, status = -1;
    auto               owner = std::make_shared<int>(42);
    std::weak_ptr<int> weak  = owner;
    CHECK(t.post(
              1,
              w.data(),
              w.size(),
              [&](int s, int) {
                  ++calls;
                  status = s;
              },
              owner,
              [](auto*, auto**) { return 0; },
              [] { return 0; })
          == 0);
    owner.reset();
    CHECK(!weak.expired());
    // Deliberately complete the last WR first: a final-marker-only implementation
    // would report success too early and release the owner's allocation.
    t.complete(w.back().wr_id, 0);
    CHECK(calls == (count == 1 ? 1 : 0));
    for (size_t i = 0; i + 1 < count; ++i)
        t.complete(w[i].wr_id, 0);
    CHECK(calls == 1 && status == 0 && weak.expired() && t.pending() == 0);
    for (auto wr : w)
        CHECK(!t.complete(wr.wr_id, 0));
    CHECK(calls == 1);
}
void partial(size_t bad_index, bool early)
{
    CompletionTracker t;
    std::vector<Wr>   w(5);
    int               calls = 0, status = 0;
    bool              stopped = false;
    int               ret     = t.post(
        1,
        w.data(),
        w.size(),
        [&](int s, int) {
            CHECK(stopped || bad_index == 0);
            ++calls;
            status = s;
        },
        {},
        [&](Wr* wr, Wr** bad) {
            if (early && bad_index)
                t.complete(wr[0].wr_id, 0);
            *bad = &wr[bad_index];
            return 12;
        },
        [&] {
            CHECK(calls == 0);
            stopped = true;
            return 0;
        });
    CHECK(ret == 12 && calls == 1 && status == 403 && t.pending() == 0);
    for (auto wr : w)
        CHECK(!t.complete(wr.wr_id, 0));
}
void before_post_returns()
{
    CompletionTracker t;
    Wr                w[2];
    int               calls = 0;
    t.post(
        1,
        w,
        2,
        [&](int s, int) {
            CHECK(s == 0);
            ++calls;
        },
        {},
        [&](Wr* wr, Wr**) {
            t.complete(wr[1].wr_id, 0);
            t.complete(wr[0].wr_id, 0);
            CHECK(calls == 0);
            return 0;
        },
        [] { return 0; });
    CHECK(calls == 1);
}
void failed_destroy()
{
    CompletionTracker t;
    Wr                w[3];
    int               calls = 0;
    t.post(
        1,
        w,
        3,
        [&](int s, int) {
            CHECK(s == 403);
            ++calls;
        },
        {},
        [](Wr* wr, Wr** bad) {
            *bad = &wr[2];
            return 12;
        },
        [] { return 5; });
    CHECK(calls == 0 && t.pending() == 2);
    CHECK(!t.quiesce(1, [] { return 5; }));
    CHECK(calls == 0);
    CHECK(t.quiesce(1, [] { return 0; }));
    CHECK(calls == 1 && t.pending() == 0);
}
void errors_and_qps()
{
    CompletionTracker t;
    Wr                a[3], b[2];
    int               ac = 0, bc = 0;
    t.post(
        1,
        a,
        3,
        [&](int s, int) {
            CHECK(s == 403);
            ++ac;
        },
        {},
        [](auto*, auto**) { return 0; },
        [] { return 0; });
    t.post(
        2,
        b,
        2,
        [&](int s, int imm) {
            CHECK(s == 0 && imm == 123);
            ++bc;
        },
        {},
        [](auto*, auto**) { return 0; },
        [] { return 0; });
    t.complete(a[0].wr_id, 403);
    CHECK(ac == 0);
    t.complete(a[2].wr_id, 0);
    CHECK(ac == 0);
    t.quiesce(1, [] { return 0; });
    CHECK(ac == 1 && bc == 0 && t.pending() == 2);
    t.complete(b[1].wr_id, 0, 123);
    t.complete(b[0].wr_id, 0);
    CHECK(bc == 1);
}
void malformed_provider()
{
    CompletionTracker t;
    Wr                w[2];
    int               calls   = 0;
    bool              stopped = false;
    t.post(
        1,
        w,
        2,
        [&](int s, int) {
            CHECK(s == 403 && stopped);
            ++calls;
        },
        {},
        [](auto*, auto**) { return 12; },
        [&] {
            stopped = true;
            return 0;
        });
    CHECK(calls == 1 && t.pending() == 0);
}
void cq_retirement_race()
{
    for (int round = 0; round < 100; ++round) {
        CompletionTracker t;
        Wr                w[32];
        std::atomic<int>  calls{};
        t.post(1, w, 32, [&](int, int) { ++calls; }, {}, [](auto*, auto**) { return 0; }, [] { return 0; });
        std::thread cq([&] {
            for (auto wr : w)
                t.complete(wr.wr_id, 0);
        });
        t.quiesce(1, [] { return 0; });
        cq.join();
        CHECK(calls == 1 && t.pending() == 0);
    }
}
void reentrant_quiesce()
{
    CompletionTracker t;
    Wr                a[2], b[2];
    int               ac = 0, bc = 0;
    t.post(
        1,
        a,
        2,
        [&](int status, int) {
            CHECK(status == 403);
            ++ac;
            t.quiesce(1, [] { return 0; });
        },
        {},
        [](auto*, auto**) { return 0; },
        [] { return 0; });
    t.post(
        1,
        b,
        2,
        [&](int status, int) {
            CHECK(status == 403);
            ++bc;
        },
        {},
        [](auto*, auto**) { return 0; },
        [] { return 0; });
    CHECK(t.quiesce(1, [] { return 0; }));
    CHECK(ac == 1 && bc == 1 && t.pending() == 0);
}
int main()
{
    for (size_t n : {1, 2, 31, 32, 33, 64, 65, 257})
        boundary(n);
    for (size_t bad = 0; bad < 5; ++bad)
        for (bool early : {false, true})
            partial(bad, early);
    before_post_returns();
    failed_destroy();
    errors_and_qps();
    malformed_provider();
    cq_retirement_race();
    reentrant_quiesce();
    std::cout
        << "PASS WR boundaries, partial post prefixes, early CQEs, failed quiesce, errors/multi-QP, unknown bad_wr, reentrant retirement, 100 CQ/destroy races\n";
}
