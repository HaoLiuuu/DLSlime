#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

#include "dlslime/csrc/engine/rdma/rdma_endpoint.h"
#include "dlslime/csrc/engine/rdma/rdma_future.h"
using namespace dlslime;
namespace fs = std::filesystem;
#define CHECK(x)                                                                                                       \
    do {                                                                                                               \
        if (!(x))                                                                                                      \
            throw std::runtime_error(#x);                                                                              \
    } while (0)
void waitFile(const fs::path& path)
{
    auto end = std::chrono::steady_clock::now() + std::chrono::seconds(90);
    while (!fs::exists(path)) {
        if (std::chrono::steady_clock::now() > end)
            throw std::runtime_error("Control file timeout");
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}
unsigned char pattern(size_t index, int role)
{
    return static_cast<unsigned char>((index * 17 + role * 59) % 251);
}
int main(int argc, char** argv)
{
    try {
        CHECK(argc == 5);
        const int role = std::stoi(argv[2]);
        CHECK(role == 0 || role == 1);
        const size_t qps = std::stoul(argv[3]);
        fs::path     dir = argv[4];
        fs::create_directories(dir);
        constexpr size_t           capacity = 16 * 1024 * 1024 + 2;
        std::vector<unsigned char> source(capacity), target(capacity, 255), readback(capacity, 255);
        for (size_t i = 0; i < capacity; ++i)
            source[i] = pattern(i, role);
        auto ep = std::make_shared<RDMAEndpoint>(argv[1], 1, "RoCE", qps);
        int  sh = ep->registerOrAccessMemoryRegion("source", reinterpret_cast<uintptr_t>(source.data()), 0, capacity);
        int  th = ep->registerOrAccessMemoryRegion("target", reinterpret_cast<uintptr_t>(target.data()), 0, capacity);
        int  bh =
            ep->registerOrAccessMemoryRegion("readback", reinterpret_cast<uintptr_t>(readback.data()), 0, capacity);
        CHECK(sh >= 0 && th >= 0 && bh >= 0);
        {
            std::ofstream f(dir / "endpoint.tmp");
            f << ep->endpointInfo().dump();
        }
        fs::permissions(dir / "endpoint.tmp", fs::perms::owner_read | fs::perms::owner_write);
        fs::rename(dir / "endpoint.tmp", dir / "endpoint.json");
        waitFile(dir / "peer.json");
        json peer;
        {
            std::ifstream f(dir / "peer.json");
            f >> peer;
        }
        ep->connect(peer);
        int  remote_source = ep->registerOrAccessRemoteMemoryRegion("source", peer.at("mr_info").at("source"));
        int  remote_target = ep->registerOrAccessRemoteMemoryRegion("target", peer.at("mr_info").at("target"));
        json cases         = json::array();
        const std::vector<std::pair<size_t, size_t>> matrix = {{1, 1},
                                                               {64, 1},
                                                               {64, 2},
                                                               {64, 31},
                                                               {64, 32},
                                                               {64, 33},
                                                               {64, 65},
                                                               {64, 257},
                                                               {64, 1025},
                                                               {4096, 33},
                                                               {1024 * 1024, 1},
                                                               {16 * 1024 * 1024, 1}};
        for (int round = 0; round < 3; ++round)
            for (auto [size, count] : matrix) {
                size_t bytes = size * count;
                CHECK(bytes <= capacity - 2);
                std::vector<assign_tuple_t> writes, verify, reads;
                for (size_t i = 0; i < count; ++i) {
                    size_t off = 1 + i * size;
                    writes.emplace_back(sh, remote_target, off, off, size);
                    verify.emplace_back(bh, remote_target, off, off, size);
                    reads.emplace_back(bh, remote_source, off, off, size);
                }
                CHECK(ep->write(writes, nullptr)->wait() == 0);
                std::fill(readback.begin() + 1, readback.begin() + 1 + bytes, 255);
                CHECK(ep->read(verify, nullptr)->wait() == 0);
                for (size_t i = 1; i <= bytes; ++i)
                    CHECK(readback[i] == pattern(i, role));
                std::fill(readback.begin() + 1, readback.begin() + 1 + bytes, 255);
                CHECK(ep->read(reads, nullptr)->wait() == 0);
                for (size_t i = 1; i <= bytes; ++i)
                    CHECK(readback[i] == pattern(i, 1 - role));
                const int imm = static_cast<int>(0x10203040u + round * matrix.size() + cases.size() % matrix.size());
                auto      notification = ep->immRecv();
                CHECK(ep->writeWithImm(writes, imm, nullptr)->wait() == 0);
                CHECK(notification->wait() == 0 && notification->immData() == imm);
                for (size_t i = 1; i <= bytes; ++i)
                    CHECK(target[i] == pattern(i, 1 - role));
                CHECK(target.front() == 255 && target.back() == 255 && readback.front() == 255
                      && readback.back() == 255);
                cases.push_back({{"round", round},
                                 {"size_bytes", size},
                                 {"batch", count},
                                 {"write_readback", true},
                                 {"read", true},
                                 {"write_with_imm", true}});
                std::cout << "PASS role=" << role << " qps=" << qps << " size=" << size << " batch=" << count
                          << " round=" << round << std::endl;
            }
        {
            std::ofstream f(dir / "result.json");
            f << json{{"role", role}, {"qps", qps}, {"cases", cases}}.dump(2);
        }
        {
            std::ofstream f(dir / "done");
            f << "validated";
        }
        waitFile(dir / "stop");
        ep->shutdown();
        std::cout << "PASS cross-node DRAM READ/WRITE/WRITE_WITH_IMM" << std::endl;
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FAIL " << e.what() << std::endl;
        return 1;
    }
}
