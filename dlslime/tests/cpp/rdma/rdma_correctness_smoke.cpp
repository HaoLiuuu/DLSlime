#include <cstring>
#include <iostream>
#include <vector>

#include "dlslime/csrc/engine/rdma/rdma_endpoint.h"
#include "dlslime/csrc/engine/rdma/rdma_future.h"
using namespace dlslime;
#define CHECK(x)                                                                                                       \
    do {                                                                                                               \
        if (!(x))                                                                                                      \
            throw std::runtime_error(#x);                                                                              \
    } while (0)
int main(int argc, char** argv)
{
    if (argc < 2) {
        std::cerr << "usage: rdma_correctness_smoke DEVICE [QP_COUNT]\n";
        return 2;
    }
    const size_t               qps = argc > 2 ? std::stoul(argv[2]) : 2;
    auto                       a   = std::make_shared<RDMAEndpoint>(argv[1], 1, "RoCE", qps);
    auto                       b   = std::make_shared<RDMAEndpoint>(argv[1], 1, "RoCE", qps);
    std::vector<unsigned char> source(131074, 255), target(source.size(), 255);
    auto                       local =
        a->registerOrAccessMemoryRegion("source", reinterpret_cast<uintptr_t>(source.data()), 0, source.size());
    auto remote =
        b->registerOrAccessMemoryRegion("target", reinterpret_cast<uintptr_t>(target.data()), 0, target.size());
    CHECK(local >= 0 && remote >= 0);
    a->connect(b->endpointInfo());
    b->connect(a->endpointInfo());
    auto rh = a->registerOrAccessRemoteMemoryRegion("target", b->mrInfo().at("target"));
    auto lh = b->registerOrAccessRemoteMemoryRegion("source", a->mrInfo().at("source"));
    std::cout << "CONNECTED qps=" << qps << std::endl;
    for (size_t count :
         {size_t(1), size_t(2), size_t(31), size_t(32), size_t(33), size_t(65), size_t(257), size_t(1025)}) {
        for (size_t i = 0; i < source.size(); ++i)
            source[i] = static_cast<unsigned char>((i * 17 + count) % 251);
        std::fill(target.begin(), target.end(), 255);
        std::vector<assign_tuple_t> spans;
        for (size_t i = 0; i < count; ++i)
            spans.emplace_back(local, rh, 1 + i * 64, 1 + i * 64, 64);
        std::cout << "WRITE count=" << count << std::endl;
        CHECK(a->write(spans, nullptr)->wait() == 0);
        // A completion on a WRITE sender is not a target application ACK. Use a
        // real READ and local comparison to verify the transferred bytes.
        std::fill(source.begin() + 1, source.begin() + 1 + count * 64, 0);
        CHECK(a->read(spans, nullptr)->wait() == 0);
        for (size_t i = 0; i < count * 64; ++i)
            CHECK(source[1 + i] == static_cast<unsigned char>(((1 + i) * 17 + count) % 251));
        auto notification = b->immRecv();
        std::cout << "WRITE_WITH_IMM count=" << count << std::endl;
        CHECK(a->writeWithImm(spans, 0x10203040, nullptr)->wait() == 0);
        CHECK(notification->wait() == 0 && notification->immData() == 0x10203040);
        CHECK(target.front() == 255 && target.back() == 255);
        std::cout << "PASS count=" << count << std::endl;
    }

    for (int round = 0; round < 20; ++round) {
        std::fill(source.begin(), source.end(), static_cast<unsigned char>(round + 7));
        std::fill(target.begin(), target.end(), 0);
        std::shared_ptr<SendFuture> sf;
        std::shared_ptr<RecvFuture> rf;
        if (round % 2) {
            sf = a->send({reinterpret_cast<uintptr_t>(source.data()), 0, source.size()}, nullptr);
            rf = b->recv({reinterpret_cast<uintptr_t>(target.data()), 0, target.size()}, nullptr);
        }
        else {
            rf = b->recv({reinterpret_cast<uintptr_t>(target.data()), 0, target.size()}, nullptr);
            sf = a->send({reinterpret_cast<uintptr_t>(source.data()), 0, source.size()}, nullptr);
        }
        CHECK(sf->wait() == 0 && rf->wait() == 0);
        CHECK(source == target);
    }
    std::cout << "PASS repeated SEND/RECV" << std::endl;
    for (const auto& invalid : std::vector<std::vector<assign_tuple_t>>{{},
                                                                        {{local, rh, 0, source.size(), 1}},
                                                                        {{local, rh, target.size(), 0, 1}},
                                                                        {{UINT64_MAX, rh, 0, 0, 1}},
                                                                        {{local, rh, UINT64_MAX, 0, 2}}}) {
        bool rejected = false;
        try {
            a->write(invalid, nullptr);
        }
        catch (const std::exception&) {
            rejected = true;
        }
        CHECK(rejected);
    }
    std::cout << "PASS invalid batches rejected before posting" << std::endl;
    auto retained = a->write({assign_tuple_t{local, rh, 0, 0, 64}}, nullptr);
    CHECK(retained->wait() == 0);
    // Fail only this test's QPs. A remote access error must retire all shards
    // without poisoning an already completed Future.
    auto invalid_key                       = b->mrInfo().at("target");
    invalid_key["rkey"]                    = 0;
    auto                        bad_handle = a->registerOrAccessRemoteMemoryRegion("invalid-test-key", invalid_key);
    std::vector<assign_tuple_t> bad_spans;
    for (size_t i = 0; i < 257; ++i)
        bad_spans.emplace_back(local, bad_handle, i * 64, i * 64, 64);
    CHECK(a->write(bad_spans, nullptr)->wait() != 0);
    std::cout << "PASS remote access error retires every shard" << std::endl;
    auto pending = b->immRecv();
    a->shutdown();
    b->shutdown();
    CHECK(retained->wait() == 0);
    CHECK(pending->wait() != 0);
    bool rejected = false;
    try {
        a->write({assign_tuple_t{local, rh, 0, 0, 1}}, nullptr);
    }
    catch (const std::exception&) {
        rejected = true;
    }
    CHECK(rejected);
    std::cout << "PASS RDMA DRAM local correctness" << std::endl;
}
