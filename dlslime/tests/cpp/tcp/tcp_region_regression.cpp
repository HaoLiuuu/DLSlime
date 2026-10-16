#include <poll.h>

#include <asio.hpp>
#include <chrono>
#include <cstring>
#include <future>
#include <iostream>
#include <thread>

#include "dlslime/csrc/engine/tcp/region_wire.h"
#include "dlslime/csrc/engine/tcp/tcp_region_endpoint.h"
using namespace dlslime::tcp;
using namespace dlslime::region;
namespace rw = dlslime::tcp::region_wire;
using namespace std::chrono_literals;
using Socket = asio::ip::tcp::socket;
#define CHECK(x)                                                                                                       \
    do {                                                                                                               \
        if (!(x))                                                                                                      \
            throw std::runtime_error(#x);                                                                              \
    } while (0)
template<class F>
void rejects(F f)
{
    try {
        f();
    }
    catch (const std::exception&) {
        return;
    }
    throw std::runtime_error("Expected rejection");
}
int wait(const std::shared_ptr<RegionFuture>& f)
{
    int32_t s{};
    CHECK(f->wait_for(5000, &s));
    return s;
}
bool readable(Socket& s, int ms = 2000)
{
    pollfd fd{s.native_handle(), POLLIN, 0};
    return ::poll(&fd, 1, ms) > 0;
}
rw::Header header(Socket& s)
{
    CHECK(readable(s));
    rw::Bytes b{};
    asio::read(s, asio::buffer(b));
    return rw::decode(b);
}
void reply(Socket& s, rw::Header h, uint32_t status = 0)
{
    h.opcode |= rw::response;
    h.status = status;
    auto b   = rw::encode(h);
    asio::write(s, asio::buffer(b));
}
struct RawPeer {
    asio::io_context        io;
    asio::ip::tcp::acceptor listener{io, {asio::ip::make_address("127.0.0.1"), 0}};
    std::future<void>       task;
    template<class F>
    RawPeer(F f)
    {
        task = std::async(std::launch::async, [this, f] {
            auto s = listener.accept();
            s.set_option(asio::ip::tcp::no_delay(true));
            f(s);
        });
    }
    RegionPeer info()
    {
        return {"127.0.0.1", listener.local_endpoint().port(), 2};
    }
    void join()
    {
        CHECK(task.wait_for(5s) == std::future_status::ready);
        task.get();
    }
};
Descriptor fake(uint64_t length)
{
    return {{0x1122334455667788, 0x8877665544332211, 0x100000002, 0x300000004}, length, Access::read_write};
}
void encoding()
{
    rw::Header h{rw::write, 0, 0x0102030405060708, fake(19).key, 0x1020304050607080, 0xffeeddccbbaa9988};
    auto       b = rw::encode(h);
    CHECK(b.size() == 72 && b[0] == 'D' && b[3] == '2' && b[4] == 0 && b[5] == 2);
    CHECK(b[16] == 1 && b[23] == 8 && b[64] == 0xff && b[71] == 0x88);
    CHECK(rw::decode(b) == h);
    for (int at : {0, 4, 12, 15}) {
        auto bad = b;
        bad[at] ^= 1;
        rejects([&] { rw::decode(bad); });
    }
}
void roundtrip()
{
    TcpRegionEndpoint a("127.0.0.1"), b("127.0.0.1");
    auto  src = std::make_shared<HostBuffer>(1024 * 1024 + 2), dst = std::make_shared<HostBuffer>(src->size());
    auto  ad = a.regions()->register_buffer(src), bd = b.regions()->register_buffer(dst);
    auto* s = static_cast<unsigned char*>(src->data());
    auto* d = static_cast<unsigned char*>(dst->data());
    for (size_t i = 0; i < src->size(); ++i)
        s[i] = i % 251;
    std::memset(d, 255, dst->size());
    std::vector<RegionSpan> spans;
    for (size_t i = 0; i < 64; ++i)
        spans.push_back({ad.key, bd, 1 + i * 16384, 1 + i * 16384, 16384});
    CHECK(wait(a.write(b.endpoint_info(), spans)) == REGION_SUCCESS);
    CHECK(d[0] == 255 && d[dst->size() - 1] == 255 && !std::memcmp(s + 1, d + 1, 1024 * 1024));
    std::memset(s + 1, 0, 1024 * 1024);
    CHECK(wait(a.read(b.endpoint_info(), spans)) == REGION_SUCCESS);
    CHECK(!std::memcmp(s + 1, d + 1, 1024 * 1024));
    // Persistent lane reuse, zero length, and more than 32 spans.
    spans.push_back({ad.key, bd, src->size(), dst->size(), 0});
    CHECK(wait(a.write(b.endpoint_info(), spans)) == REGION_SUCCESS);
    // Different endpoints simultaneously initiate READ and WRITE on disjoint allocations.
    auto as = std::make_shared<HostBuffer>(4096), bs = std::make_shared<HostBuffer>(4096);
    auto ads = a.regions()->register_buffer(as), bds = b.regions()->register_buffer(bs);
    std::memset(as->data(), 42, 4096);
    std::memset(bs->data(), 0, 4096);
    auto first  = a.write(b.endpoint_info(), {{ads.key, bds, 0, 0, 4096}});
    auto second = b.read(a.endpoint_info(), {{bd.key, ad, 0, 0, 4096}});
    CHECK(wait(first) == 0 && wait(second) == 0);
    CHECK(!std::memcmp(as->data(), bs->data(), 4096));
    a.shutdown();
    b.shutdown();
}
void ack_required(int mode)
{
    std::promise<void> payload;
    std::promise<void> release;
    RawPeer            peer([&](Socket& s) {
        auto h = header(s);
        CHECK(h.opcode == rw::write);
        std::vector<char> data(h.length);
        asio::read(s, asio::buffer(data));
        CHECK(data[0] == 42);
        payload.set_value();
        if (mode == 0) {
            CHECK(release.get_future().wait_for(2s) == std::future_status::ready);
            reply(s, h);
        }
        else if (mode == 1) {
            h.request++;
            reply(s, h);
        }
        else if (mode == 2) {
            CHECK(readable(s, 2000));
            char             c;
            asio::error_code ec;
            s.read_some(asio::buffer(&c, 1), ec);
            CHECK(ec);
        }
        else if (mode == 3) {
            reply(s, h, rw::stale);
        }
    });
    TcpRegionEndpoint  ep("127.0.0.1");
    auto               b = std::make_shared<HostBuffer>(64);
    std::memset(b->data(), 42, 64);
    auto d = ep.regions()->register_buffer(b);
    auto f = ep.write(peer.info(), {{d.key, fake(64), 0, 0, 64}}, mode == 2 ? 150 : 2000);
    CHECK(payload.get_future().wait_for(2s) == std::future_status::ready);
    if (mode == 0) {
        CHECK(!f->wait_for(30));
        release.set_value();
        CHECK(wait(f) == 0);
    }
    else
        CHECK(wait(f)
              == (mode == 1 ? REGION_PROTOCOL_ERROR :
                  mode == 2 ? REGION_TIMEOUT :
                  mode == 3 ? REGION_REJECTED :
                              REGION_IO_ERROR));
    ep.shutdown();
    peer.join();
}
void pipeline(size_t depth, size_t bytes, size_t length)
{
    constexpr size_t count    = 65;
    const size_t     capacity = std::min(depth, std::max(size_t{1}, bytes / length));
    RawPeer          peer([&](Socket& s) {
        std::vector<rw::Header> requests;
        for (size_t i = 0; i < capacity; ++i)
            requests.push_back(header(s));
        CHECK(!readable(s, 20));
        for (size_t i = 0; i < count; ++i) {
            auto h = requests.at(i);
            CHECK(h.opcode == rw::read);
            reply(s, h);
            std::vector<unsigned char> data(length, static_cast<unsigned char>(i));
            asio::write(s, asio::buffer(data.data(), 1));
            asio::write(s, asio::buffer(data.data() + 1, data.size() - 1));
            if (requests.size() < count)
                requests.push_back(header(s));
        }
    });
    RegionOptions    opts;
    opts.window_requests = depth;
    opts.window_bytes    = bytes;
    TcpRegionEndpoint ep("127.0.0.1", 0, opts);
    auto              b = std::make_shared<HostBuffer>(count * length + 2);
    std::memset(b->data(), 255, b->size());
    auto                    d = ep.regions()->register_buffer(b);
    std::vector<RegionSpan> spans;
    for (size_t i = 0; i < count; ++i)
        spans.push_back({d.key, fake(count * length), 1 + i * length, i * length, length});
    CHECK(wait(ep.read(peer.info(), spans)) == 0);
    auto* data = static_cast<unsigned char*>(b->data());
    CHECK(data[0] == 255 && data[b->size() - 1] == 255);
    for (size_t i = 0; i < count; ++i)
        for (size_t j = 0; j < length; ++j)
            CHECK(data[1 + i * length + j] == i);
    ep.shutdown();
    peer.join();
}
void cancellation(int mode)
{
    std::promise<void>        entered, release;
    RawPeer                   peer([&](Socket& s) {
        auto h = header(s);
        entered.set_value();
        if (mode == 3) {
            CHECK(release.get_future().wait_for(2s) == std::future_status::ready);
            reply(s, h);
            asio::write(s, asio::buffer("abc", 3));
            return;
        }
        CHECK(readable(s, 2000));
        char             c;
        asio::error_code ec;
        s.read_some(asio::buffer(&c, 1), ec);
        CHECK(ec);
    });
    TcpRegionEndpoint         ep("127.0.0.1");
    auto                      b    = std::make_shared<HostBuffer>(64);
    std::weak_ptr<HostBuffer> weak = b;
    auto                      d    = ep.regions()->register_buffer(b);
    auto                      f    = ep.read(peer.info(), {{d.key, fake(64), 0, 0, 64}}, mode == 0 ? 150 : 2000);
    CHECK(entered.get_future().wait_for(2s) == std::future_status::ready);
    auto drained = ep.regions()->retire(d.key);
    b.reset();
    CHECK(!weak.expired());
    CHECK(drained.wait_for(0ms) == std::future_status::timeout);
    if (mode == 3)
        release.set_value();
    if (mode == 1)
        f->cancel();
    if (mode == 2)
        ep.shutdown();
    CHECK(wait(f) == (mode == 0 ? REGION_TIMEOUT : mode == 3 ? REGION_IO_ERROR : REGION_CANCELLED));
    CHECK(drained.wait_for(0ms) == std::future_status::ready && weak.expired());
    f->cancel();
    ep.shutdown();
    f->cancel();
    peer.join();
}
void await_admission(TcpRegionEndpoint& ep, const Key& key)
{
    const auto limit = std::chrono::steady_clock::now() + 2s;
    while (ep.regions()->active_leases(key) == 0 && std::chrono::steady_clock::now() < limit)
        std::this_thread::sleep_for(1ms);
    CHECK(ep.regions()->active_leases(key) > 0);
}
void bounded_admission_and_write_cancel()
{
    std::promise<void> entered, release;
    RawPeer            peer([&](Socket& s) {
        CHECK(header(s).opcode == rw::write);
        entered.set_value();
        CHECK(release.get_future().wait_for(3s) == std::future_status::ready);
    });
    RegionOptions      opts;
    opts.max_clients = 1;
    TcpRegionEndpoint         ep("127.0.0.1", 0, opts);
    auto                      b    = std::make_shared<HostBuffer>(16 * 1024 * 1024);
    std::weak_ptr<HostBuffer> weak = b;
    auto                      d    = ep.regions()->register_buffer(b);
    auto                      f    = ep.write(peer.info(), {{d.key, fake(b->size()), 0, 0, b->size()}}, 2000);
    CHECK(entered.get_future().wait_for(2s) == std::future_status::ready);
    CHECK(wait(ep.write(peer.info(), {{d.key, fake(64), 0, 0, 64}})) == REGION_BUSY);
    auto drained = ep.regions()->retire(d.key);
    b.reset();
    CHECK(!weak.expired());
    f->cancel();
    CHECK(wait(f) == REGION_CANCELLED);
    CHECK(drained.wait_for(0ms) == std::future_status::ready && weak.expired());
    release.set_value();
    peer.join();
    ep.shutdown();
}
void overlapping_read_destinations()
{
    TcpRegionEndpoint a("127.0.0.1"), b("127.0.0.1");
    auto              local = std::make_shared<HostBuffer>(6), remote = std::make_shared<HostBuffer>(12);
    std::memcpy(local->data(), "G????G", 6);
    std::memcpy(remote->data(), "AAAABBBBCCCC", 12);
    auto ld = a.regions()->register_buffer(local), rd = b.regions()->register_buffer(remote);
    CHECK(wait(a.read(b.endpoint_info(), {{ld.key, rd, 1, 0, 4}, {ld.key, rd, 1, 4, 4}, {ld.key, rd, 1, 8, 4}})) == 0);
    CHECK(!std::memcmp(local->data(), "GCCCCG", 6));
}
void remote_retirement()
{
    RegionOptions opts;
    opts.server_timeout_ms = 500;
    TcpRegionEndpoint ep("127.0.0.1", 0, opts);
    auto              b = std::make_shared<HostBuffer>(64);
    auto              d = ep.regions()->register_buffer(b);
    asio::io_context  io;
    Socket            s(io);
    auto              peer = ep.endpoint_info();
    s.connect({asio::ip::make_address(peer.host), peer.port});
    rw::Header h{rw::write, 0, 1, d.key, 0, 64};
    auto       bytes = rw::encode(h);
    asio::write(s, asio::buffer(bytes));
    asio::write(s, asio::buffer("X", 1));
    await_admission(ep, d.key);
    auto drained = ep.regions()->retire(d.key);
    CHECK(drained.wait_for(0ms) == std::future_status::timeout);
    std::vector<char> rest(63, 'X');
    asio::write(s, asio::buffer(rest));
    auto ack = header(s);
    CHECK(ack.status == 0 && ack.request == 1);
    CHECK(drained.wait_for(2s) == std::future_status::ready);
    CHECK(static_cast<char*>(b->data())[0] == 'X' && static_cast<char*>(b->data())[63] == 'X');
    auto fresh = ep.regions()->register_buffer(b);
    CHECK(fresh.key.generation != d.key.generation);
    h.opcode  = rw::read;
    h.request = 2;
    bytes     = rw::encode(h);
    asio::write(s, asio::buffer(bytes));
    CHECK(header(s).status == rw::stale);
}
void rejected_requests()
{
    RegionOptions opts;
    opts.server_timeout_ms = 100;
    TcpRegionEndpoint ep("127.0.0.1", 0, opts);
    auto              b = std::make_shared<HostBuffer>(16);
    auto              d = ep.regions()->register_buffer(b, Access::read);
    for (int mode = 0; mode < 5; ++mode) {
        asio::io_context io;
        Socket           s(io);
        auto             peer = ep.endpoint_info();
        s.connect({asio::ip::make_address(peer.host), peer.port});
        rw::Header h{rw::read, 0, 1, d.key, 0, 16};
        if (mode == 0)
            h.opcode = rw::write;
        if (mode == 1)
            h.offset = 1;
        if (mode == 2)
            h.key.generation++;
        auto bytes = rw::encode(h);
        if (mode == 3)
            bytes[4] = 99;
        if (mode == 4) {
            asio::write(s, asio::buffer(bytes.data(), 1));
        }
        else
            asio::write(s, asio::buffer(bytes));
        if (mode < 3)
            CHECK(header(s).status == (mode == 0 ? rw::denied : mode == 1 ? rw::bounds : rw::stale));
        else {
            CHECK(readable(s, 1000));
            char             c;
            asio::error_code ec;
            s.read_some(asio::buffer(&c, 1), ec);
            CHECK(ec);
        }
    }
}
void expired_before_start()
{
    TcpRegionEndpoint a("127.0.0.1"), b("127.0.0.1");
    auto              source = std::make_shared<HostBuffer>(64), target = std::make_shared<HostBuffer>(64);
    std::memset(source->data(), 42, 64);
    auto ad = a.regions()->register_buffer(source), bd = b.regions()->register_buffer(target);
    CHECK(wait(a.write(b.endpoint_info(), {{ad.key, bd, 0, 0, 64}}, 0)) == REGION_TIMEOUT);
    CHECK(a.regions()->active_leases(ad.key) == 0);
    for (size_t i = 0; i < 64; ++i)
        CHECK(static_cast<char*>(target->data())[i] == 0);
}
void all_batch_validation()
{
    TcpRegionEndpoint a("127.0.0.1"), b("127.0.0.1");
    auto              ab = std::make_shared<HostBuffer>(64), bb = std::make_shared<HostBuffer>(64);
    std::memset(ab->data(), 42, 64);
    auto ad = a.regions()->register_buffer(ab), bd = b.regions()->register_buffer(bb);
    rejects([&] { a.write(b.endpoint_info(), {{ad.key, bd, 0, 0, 32}, {ad.key, bd, 63, 0, 2}}); });
    auto drained = a.regions()->retire(ad.key);
    CHECK(drained.wait_for(0ms) == std::future_status::ready);
    for (size_t i = 0; i < 64; ++i)
        CHECK(static_cast<char*>(bb->data())[i] == 0);
    auto old     = b.endpoint_info();
    old.protocol = 1;
    rejects([&] { a.read(old, {{ad.key, bd, 0, 0, 1}}); });
}
void server_timeout_drain()
{
    RegionOptions opts;
    opts.server_timeout_ms = 150;
    TcpRegionEndpoint ep("127.0.0.1", 0, opts);
    auto              b = std::make_shared<HostBuffer>(64);
    auto              d = ep.regions()->register_buffer(b);
    asio::io_context  io;
    Socket            s(io);
    auto              peer = ep.endpoint_info();
    s.connect({asio::ip::make_address(peer.host), peer.port});
    auto bytes = rw::encode({rw::write, 0, 1, d.key, 0, 64});
    asio::write(s, asio::buffer(bytes));
    asio::write(s, asio::buffer("X", 1));
    await_admission(ep, d.key);
    auto drained = ep.regions()->retire(d.key);
    CHECK(drained.wait_for(0ms) == std::future_status::timeout);
    CHECK(drained.wait_for(2s) == std::future_status::ready);
    CHECK(readable(s));
    char             c;
    asio::error_code ec;
    s.read_some(asio::buffer(&c, 1), ec);
    CHECK(ec);
}
int main()
{
#define RUN(f)                                                                                                         \
    do {                                                                                                               \
        f;                                                                                                             \
        std::cout << "PASS " << #f << std::endl;                                                                       \
    } while (0)
    RUN(bounded_admission_and_write_cancel());
    RUN(overlapping_read_destinations());
    RUN(expired_before_start());
    RUN(encoding());
    RUN(roundtrip());
    for (int mode = 0; mode < 5; ++mode)
        RUN(ack_required(mode));
    RUN(pipeline(8, 64, 8));
    RUN(pipeline(8, 16, 8));
    RUN(pipeline(8, 4, 8));
    RUN(pipeline(1, 64, 8));
    for (int mode = 0; mode < 4; ++mode)
        RUN(cancellation(mode));
    RUN(remote_retirement());
    RUN(rejected_requests());
    RUN(all_batch_validation());
    RUN(server_timeout_drain());
}
