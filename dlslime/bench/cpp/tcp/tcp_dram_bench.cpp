#include <sched.h>
#include <sys/resource.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <thread>
#include <vector>

#include "dlslime/csrc/engine/tcp/tcp_endpoint.h"

using namespace dlslime;
using namespace dlslime::tcp;
using Clock                   = std::chrono::steady_clock;
static constexpr size_t guard = 64;

static void require(bool ok, const char* message)
{
    if (!ok)
        throw std::runtime_error(message);
}
static void wait(const std::shared_ptr<TcpFuture>& f)
{
    // The operation's own deadline retires buffers before this wait returns.
    require(f->wait() == TCP_SUCCESS, "TCP operation failed or timed out");
}
static double seconds(Clock::time_point a, Clock::time_point b)
{
    return std::chrono::duration<double>(b - a).count();
}
static int64_t unix_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}
static double usage(const timeval& t)
{
    return t.tv_sec + t.tv_usec / 1e6;
}
static uint64_t pattern(size_t word, uint64_t seed)
{
    return (word * UINT64_C(0x9e3779b97f4a7c15)) ^ (seed * UINT64_C(0xd6e8feb86659fd93));
}
struct Buffer {
    std::vector<uint64_t> words;
    explicit Buffer(size_t payload): words((payload + 2 * guard) / sizeof(uint64_t)) {}
    unsigned char* data()
    {
        return reinterpret_cast<unsigned char*>(words.data());
    }
    size_t size() const
    {
        return words.size() * sizeof(uint64_t);
    }
    void clear(unsigned char byte)
    {
        std::memset(data(), byte, size());
    }
    void fill(size_t payload, uint64_t seed)
    {
        clear(0xd3);
        for (size_t i = 0; i < payload / sizeof(uint64_t); ++i)
            words[guard / sizeof(uint64_t) + i] = pattern(i, seed);
    }
    void check(size_t payload, uint64_t seed, unsigned char sentinel)
    {
        for (size_t i = 0; i < guard; ++i)
            require(data()[i] == sentinel && data()[guard + payload + i] == sentinel, "guard corruption");
        for (size_t i = 0; i < payload / sizeof(uint64_t); ++i)
            require(words[guard / sizeof(uint64_t) + i] == pattern(i, seed), "payload mismatch");
    }
};
struct Peer {
    size_t                      span_bytes, batch, bytes;
    Buffer                      source, target, destination, verification;
    int32_t                     src, dst, check, remote_src = -1, remote_dst = -1;
    uint64_t                    seed = 0, peer_seed = 0;
    bool                        active = false, write = false;
    bool                        completed = false;
    std::vector<assign_tuple_t> spans;
    // Declared last: socket callbacks retire before registered storage is freed.
    std::shared_ptr<TcpEndpoint> ep;
    Peer(const std::string& ip, size_t size, size_t count):
        span_bytes(size),
        batch(count),
        bytes(size * count),
        source(bytes),
        target(bytes),
        destination(bytes),
        verification(bytes),
        ep(std::make_shared<TcpEndpoint>(ip))
    {
        src = ep->register_memory_region("source", reinterpret_cast<uintptr_t>(source.data()), 0, source.size());
        ep->register_memory_region("target", reinterpret_cast<uintptr_t>(target.data()), 0, target.size());
        dst = ep->register_memory_region(
            "destination", reinterpret_cast<uintptr_t>(destination.data()), 0, destination.size());
        check = ep->register_memory_region(
            "verification", reinterpret_cast<uintptr_t>(verification.data()), 0, verification.size());
        require(src >= 0 && dst >= 0 && check >= 0, "MR registration failed");
    }
    void connect(const json& remote)
    {
        ep->connect(remote);
        require(ep->is_connected(), "connect failed");
        remote_src = ep->register_remote_memory_region("source", remote.at("mr_info").at("source"));
        remote_dst = ep->register_remote_memory_region("target", remote.at("mr_info").at("target"));
    }
    void prepare(const json& cmd)
    {
        seed          = cmd.at("seed").get<uint64_t>();
        peer_seed     = cmd.at("peer_seed").get<uint64_t>();
        active        = cmd.at("active").get<bool>();
        const auto op = cmd.at("operation").get<std::string>();
        require(op == "READ" || op == "WRITE", "invalid operation");
        write = op == "WRITE";
        source.fill(bytes, seed);
        target.clear(0xa5);
        destination.clear(0xcc);
        verification.clear(0xbb);
        spans.clear();
        for (size_t i = 0; i < batch; ++i) {
            const auto offset = guard + i * span_bytes;
            spans.emplace_back(write ? src : dst, write ? remote_dst : remote_src, offset, offset, span_bytes);
        }
        completed = false;
    }
    json run(double duration)
    {
        require(duration > 0 && duration <= 60, "duration out of bounds");
        require(!spans.empty() && remote_src >= 0, "prepare/connect required");
        std::vector<double> latencies;
        latencies.reserve(1000000);
        rusage before{}, after{};
        getrusage(RUSAGE_SELF, &before);
        const auto start_ns  = unix_ns();
        const auto begin     = Clock::now();
        auto       local_end = begin;
        uint64_t   batches   = 0;
        if (active) {
            do {
                const auto start = Clock::now();
                wait(write ? ep->write(spans) : ep->read(spans));
                local_end = Clock::now();
                if (latencies.size() < 1000000)
                    latencies.push_back(seconds(start, local_end) * 1e6);
                ++batches;
            } while (seconds(begin, local_end) < duration);
            if (write) {
                // One outstanding call and one persistent outgoing connection per
                // endpoint. A READ response on that stream follows every WRITE
                // payload. This fences the trial, not each batch, and changes no API.
                wait(ep->read({{check, remote_dst, 0, 0, 1}}));
                require(verification.data()[0] == 0xa5, "WRITE fence mismatch");
            }
        }
        else {
            std::this_thread::sleep_for(std::chrono::duration<double>(duration));
            local_end = Clock::now();
        }
        const auto end    = Clock::now();
        const auto end_ns = unix_ns();
        getrusage(RUSAGE_SELF, &after);
        completed          = true;
        const auto elapsed = seconds(begin, end);
        const auto local_s = seconds(begin, local_end);
        const auto total   = batches * static_cast<uint64_t>(bytes);
        std::sort(latencies.begin(), latencies.end());
        auto quantile = [&](double q) {
            return latencies.empty() ? 0.0 : latencies[static_cast<size_t>(std::ceil(q * latencies.size())) - 1];
        };
        return {{"active", active},
                {"operation", write ? "WRITE" : "READ"},
                {"span_bytes", span_bytes},
                {"batch_size", batch},
                {"batch_bytes", bytes},
                {"batches", batches},
                {"payload_bytes", total},
                {"requested_seconds", duration},
                {"local_seconds", local_s},
                {"confirmed_seconds", elapsed},
                {"write_drain_seconds", seconds(local_end, end)},
                {"local_gbps", total * 8.0 / local_s / 1e9},
                {"confirmed_gbps", total * 8.0 / elapsed / 1e9},
                {"batch_p50_us", quantile(0.50)},
                {"batch_p99_us", quantile(0.99)},
                {"latency_samples", latencies.size()},
                {"latency_truncated", batches > latencies.size()},
                {"batch_latency_semantics", write ? "local_socket_retirement" : "local_payload_complete"},
                {"cpu_user_seconds", usage(after.ru_utime) - usage(before.ru_utime)},
                {"cpu_system_seconds", usage(after.ru_stime) - usage(before.ru_stime)},
                {"max_rss_kib", after.ru_maxrss},
                {"start_unix_ns", start_ns},
                {"end_unix_ns", end_ns}};
    }
    void verify()
    {
        require(completed, "run required before verify");
        if (!active)
            return;
        if (write) {
            // Full readback is outside the measured interval. It also checks both guards.
            wait(ep->read({{check, remote_dst, 0, 0, target.size()}}));
            verification.check(bytes, seed, 0xa5);
        }
        else {
            destination.check(bytes, peer_seed, 0xcc);
        }
        source.check(bytes, seed, 0xd3);
    }
};
int main(int argc, char** argv)
{
    try {
        if (argc == 3 && std::string(argv[1]) == "--cpus") {
            cpu_set_t cpus;
            CPU_ZERO(&cpus);
            std::istringstream input(argv[2]);
            std::string        item;
            while (std::getline(input, item, ',')) {
                const int n = std::stoi(item);
                require(n >= 0 && n < CPU_SETSIZE, "invalid CPU");
                CPU_SET(n, &cpus);
            }
            require(sched_setaffinity(0, sizeof(cpus), &cpus) == 0, "CPU affinity failed");
        }
        else {
            require(argc == 1, "usage: tcp_dram_bench [--cpus comma-separated-CPUs]");
        }
        std::unique_ptr<Peer> peer;
        std::string           line;
        while (std::getline(std::cin, line)) {
            const auto cmd    = json::parse(line);
            const auto action = cmd.at("command").get<std::string>();
            json       out    = {{"ok", true}};
            if (action == "setup") {
                const auto size  = cmd.at("span_bytes").get<size_t>();
                const auto count = cmd.at("batch_size").get<size_t>();
                require(size >= 8 && size % 8 == 0 && size <= (1ULL << 30) && count > 0 && count <= 4096
                            && size <= (1ULL << 30) / count,
                        "size/batch out of bounds");
                peer.reset();
                peer            = std::make_unique<Peer>(cmd.at("bind_ip").get<std::string>(), size, count);
                out["endpoint"] = peer->ep->endpoint_info();
            }
            else if (action == "close") {
                peer.reset();
            }
            else if (action == "exit") {
                peer.reset();
                std::cout << out.dump() << std::endl;
                return 0;
            }
            else {
                require(static_cast<bool>(peer), "setup required");
                if (action == "connect")
                    peer->connect(cmd.at("endpoint"));
                else if (action == "prepare")
                    peer->prepare(cmd);
                else if (action == "run")
                    out["result"] = peer->run(cmd.at("seconds").get<double>());
                else if (action == "verify") {
                    peer->verify();
                    out["verified"] = true;
                }
                else
                    throw std::runtime_error("unknown command");
            }
            std::cout << out.dump() << std::endl;
        }
    }
    catch (const std::exception& error) {
        std::cout << json({{"ok", false}, {"error", error.what()}}).dump() << std::endl;
        return 1;
    }
}
