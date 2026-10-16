#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "dlslime/csrc/device/runtime/device_runtime.h"
using namespace dlslime::device;
using Clock = std::chrono::steady_clock;
void check(bool b, const char* m)
{
    if (!b)
        throw std::runtime_error(m);
}
Backend backend(const std::string& b)
{
    if (b == "host")
        return Backend::Host;
    if (b == "cuda")
        return Backend::Cuda;
    if (b == "maca")
        return Backend::Maca;
    if (b == "ascend")
        return Backend::Ascend;
    throw std::invalid_argument("unknown backend");
}
struct Samples {
    uint64_t            batches = 0;
    double              seconds = 0;
    std::vector<double> latency;
};
int main(int argc, char** argv)
{
    try {
        check(argc == 8, "usage: device_copy_bench backend device operation bytes batch warmup seconds");
        auto        rt = DeviceRuntime::create(backend(argv[1]), std::stoi(argv[2]));
        std::string op = argv[3];
        size_t      n = std::stoull(argv[4]), batch = std::stoull(argv[5]);
        double      warmup = std::stod(argv[6]), seconds = std::stod(argv[7]);
        check(n > 0 && batch > 0 && batch <= 4096 && n <= (1ULL << 30) / batch, "invalid size/batch");
        check(warmup > 0 && warmup <= 60 && seconds > 0 && seconds <= 60, "invalid duration");
        bool device = rt.capabilities().device_allocation;
        check(device ? (op == "H2D" || op == "D2H" || op == "D2D") : op == "H2H", "invalid operation");
        auto   hk       = device ? MemoryKind::HostPinned : MemoryKind::HostPageable;
        size_t total    = n * batch + 128;
        auto   expected = rt.allocate(total, hk), observed = rt.allocate(total, hk);
        auto   source = rt.allocate(total, (op == "D2H" || op == "D2D") ? MemoryKind::Device : hk);
        auto   target = rt.allocate(total, (op == "H2D" || op == "D2D") ? MemoryKind::Device : hk);
        auto   stream = rt.createStream();
        auto   seed   = [&](unsigned salt) {
            auto* p = static_cast<unsigned char*>(expected.data());
            for (size_t i = 0; i < total; ++i)
                p[i] = i < 64 || i >= total - 64 ? 0xa7 : static_cast<unsigned char>((i * 13) ^ (i >> 8) ^ salt);
            rt.copy(source.view(0, total), expected.view(0, total), stream).wait();
            std::memset(observed.data(), 0xa7, total);
            rt.copy(target.view(0, total), observed.view(0, total), stream).wait();
        };
        auto run = [&](double duration) {
            Samples samples;
            samples.latency.reserve(1000000);
            auto start = Clock::now();
            do {
                auto               begin = Clock::now();
                std::vector<Event> events;
                events.reserve(batch);
                for (size_t span = 0; span < batch; ++span)
                    events.push_back(rt.copy(target.view(64 + span * n, n), source.view(64 + span * n, n), stream));
                events.back().wait();
                events.clear();
                auto end = Clock::now();
                ++samples.batches;
                if (samples.latency.size() < 1000000)
                    samples.latency.push_back(std::chrono::duration<double, std::micro>(end - begin).count());
                samples.seconds = std::chrono::duration<double>(end - start).count();
            } while (samples.seconds < duration);
            return samples;
        };
        auto verify = [&] {
            rt.copy(observed.view(0, total), target.view(0, total), stream).wait();
            check(std::memcmp(expected.data(), observed.data(), total) == 0, "payload or guard mismatch");
        };
        seed(17);
        run(warmup);
        verify();
        seed(83);
        auto result = run(seconds);
        verify();
        std::sort(result.latency.begin(), result.latency.end());
        auto q    = [&](double x) { return result.latency[static_cast<size_t>(x * (result.latency.size() - 1))]; };
        auto info = rt.deviceInfo();
        std::cout << "{\"backend\":\"" << backendName(info.backend) << "\",\"device\":" << info.ordinal
                  << ",\"operation\":\"" << op << "\",\"span_bytes\":" << n << ",\"batch_size\":" << batch
                  << ",\"pci_bus_id\":\"" << info.pci_bus_id << "\",\"numa_node\":" << info.numa_node
                  << ",\"driver_version\":" << info.driver_version << ",\"batches\":" << result.batches
                  << ",\"seconds\":" << result.seconds
                  << ",\"payload_gbps\":" << result.batches * n * batch * 8.0 / result.seconds / 1e9
                  << ",\"batch_mean_us\":" << result.seconds * 1e6 / result.batches << ",\"batch_p50_us\":" << q(.5)
                  << ",\"batch_p99_us\":" << q(.99) << ",\"batch_p999_us\":" << q(.999)
                  << ",\"latency_samples\":" << result.latency.size()
                  << ",\"latency_truncated\":" << (result.batches > result.latency.size() ? "true" : "false")
                  << ",\"verified\":true,\"outstanding_batches\":1,\"event_per_span\":true}\n";
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FAIL " << e.what() << '\n';
        return 1;
    }
}
