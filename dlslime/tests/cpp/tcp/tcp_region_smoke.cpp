#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/engine/tcp/tcp_region_endpoint.h"
using namespace dlslime::tcp;
using namespace dlslime::region;
using json                = nlohmann::json;
using Clock               = std::chrono::steady_clock;
constexpr size_t capacity = 16 * 1024 * 1024 + 2;
#define CHECK(x)                                                                                                       \
    do {                                                                                                               \
        if (!(x))                                                                                                      \
            throw std::runtime_error(#x);                                                                              \
    } while (0)
void save(const std::string& path, const json& data)
{
    std::ofstream out(path);
    out << data.dump(2) << '\n';
    CHECK(bool(out));
}
json describe(const Descriptor& d)
{
    return {{"scope_hi", d.key.scope_hi},
            {"scope_lo", d.key.scope_lo},
            {"id", d.key.id},
            {"generation", d.key.generation},
            {"length", d.length},
            {"access", uint32_t(d.access)}};
}
Descriptor parse(const json& d)
{
    return {{d.at("scope_hi"), d.at("scope_lo"), d.at("id"), d.at("generation")},
            d.at("length"),
            static_cast<Access>(d.at("access").get<uint32_t>())};
}
int server(const std::string& ip, const std::string& path)
{
    TcpRegionEndpoint endpoint(ip);
    auto              buffer = std::make_shared<HostBuffer>(capacity);
    std::memset(buffer->data(), 255, capacity);
    auto descriptor = endpoint.regions()->register_buffer(buffer);
    auto peer       = endpoint.endpoint_info();
    save(path,
         {{"host", peer.host}, {"port", peer.port}, {"protocol", peer.protocol}, {"region", describe(descriptor)}});
    const auto until = Clock::now() + std::chrono::minutes(5);
    while (!std::filesystem::exists(path + ".stop") && Clock::now() < until)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    endpoint.shutdown();
    auto* bytes = static_cast<unsigned char*>(buffer->data());
    CHECK(bytes[0] == 255 && bytes[capacity - 1] == 255);
    std::cout << "PASS server shutdown and destination guards" << std::endl;
    return 0;
}
int client(const std::string& ip, const std::string& metadata, const std::string& result)
{
    std::ifstream input(metadata);
    json          config;
    input >> config;
    RegionPeer        peer{config.at("host"), config.at("port"), config.at("protocol")};
    auto              remote = parse(config.at("region"));
    TcpRegionEndpoint endpoint(ip);
    auto  source = std::make_shared<HostBuffer>(capacity), destination = std::make_shared<HostBuffer>(capacity);
    auto  src = endpoint.regions()->register_buffer(source), dst = endpoint.regions()->register_buffer(destination);
    auto* in      = static_cast<unsigned char*>(source->data());
    auto* out     = static_cast<unsigned char*>(destination->data());
    json  records = json::array();
    for (size_t size : {size_t(1024), size_t(256 * 1024), size_t(16 * 1024 * 1024)})
        for (size_t count : {size_t(1), size_t(64)}) {
            const size_t total = size * count;
            if (total > capacity - 2)
                continue;
            std::vector<RegionSpan> writes, reads;
            for (size_t i = 0; i < count; ++i) {
                writes.push_back({src.key, remote, 1 + i * size, 1 + i * size, size});
                reads.push_back({dst.key, remote, 1 + i * size, 1 + i * size, size});
            }
            for (int round = 0; round < 5; ++round) {
                for (size_t i = 0; i < total; ++i)
                    in[1 + i] = static_cast<unsigned char>((i * 17 + round * 31 + count) % 251);
                std::memset(out, 255, capacity);
                auto started = Clock::now();
                CHECK(endpoint.write(peer, writes, 10000)->wait() == REGION_SUCCESS);
                const double write_us = std::chrono::duration<double, std::micro>(Clock::now() - started).count();
                started               = Clock::now();
                CHECK(endpoint.read(peer, reads, 10000)->wait() == REGION_SUCCESS);
                const double read_us = std::chrono::duration<double, std::micro>(Clock::now() - started).count();
                CHECK(!std::memcmp(in + 1, out + 1, total) && out[0] == 255 && out[capacity - 1] == 255);
                // Verify both remote allocation guards with actual Region READs.
                CHECK(endpoint
                          .read(peer,
                                {{dst.key, remote, 0, 0, 1}, {dst.key, remote, capacity - 1, capacity - 1, 1}},
                                10000)
                          ->wait()
                      == 0);
                CHECK(out[0] == 255 && out[capacity - 1] == 255);
                for (const auto& [op, us] :
                     std::vector<std::pair<std::string, double>>{{"WRITE", write_us}, {"READ", read_us}})
                    records.push_back({{"operation", op},
                                       {"span_bytes", size},
                                       {"batch", count},
                                       {"repeat", round + 1},
                                       {"latency_us", us},
                                       {"payload_Gbps", double(total) * 8 / us / 1000},
                                       {"verified", true}});
            }
        }
    endpoint.shutdown();
    save(result,
         {{"initiator", ip},
          {"remote", peer.host},
          {"protocol", 2},
          {"performance_qualified", false},
          {"records", records}});
    std::cout << "PASS " << records.size() << " ACK/READ operations; full readback and allocation guards verified"
              << std::endl;
    return 0;
}
int main(int argc, char** argv)
{
    try {
        if (argc == 4 && std::string(argv[1]) == "server")
            return server(argv[2], argv[3]);
        if (argc == 5 && std::string(argv[1]) == "client")
            return client(argv[2], argv[3], argv[4]);
        std::cerr << "usage: tcp_region_smoke server IP METADATA | client IP PEER_METADATA RESULTS\n";
        return 2;
    }
    catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}
