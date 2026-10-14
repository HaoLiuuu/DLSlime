#include <endian.h>

#include <array>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iostream>
#include <numeric>
#include <thread>

#include "dlslime/csrc/engine/tcp/tcp_endpoint.h"
using namespace dlslime;
using namespace dlslime::tcp;
using Socket = asio::ip::tcp::socket;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c))                                                                                                      \
            throw std::runtime_error(std::string(__func__) + ": " + #c);                                               \
    } while (0)
static int wait(const std::shared_ptr<TcpFuture>& f)
{
    int32_t value = 999;
    CHECK(f->wait_for(3000, &value));
    return value;
}
static void header(Socket& socket, SessionHeader h)
{
    h.size = htole64(h.size);
    h.addr = htole64(h.addr);
    asio::write(socket, asio::buffer(&h, sizeof(h)));
}
static SessionHeader header(Socket& socket)
{
    SessionHeader h{};
    asio::read(socket, asio::buffer(&h, sizeof(h)));
    h.size = le64toh(h.size);
    h.addr = le64toh(h.addr);
    return h;
}
struct RawPeer {
    asio::io_context        io;
    asio::ip::tcp::acceptor acceptor{io, {asio::ip::make_address("127.0.0.1"), 0}};
    std::future<void>       task;
    template<class F>
    explicit RawPeer(F fn)
    {
        task = std::async(std::launch::async, [this, fn] { fn(*this); });
    }
    Socket accept()
    {
        auto s = acceptor.accept();
        s.set_option(asio::ip::tcp::no_delay(true));
        return s;
    }
    json info()
    {
        return {{"host", "127.0.0.1"}, {"port", acceptor.local_endpoint().port()}};
    }
    void join()
    {
        task.get();
    }
};
static std::shared_ptr<TcpEndpoint> endpoint()
{
    return std::make_shared<TcpEndpoint>("127.0.0.1");
}
static Socket raw_connect(asio::io_context& io, const std::shared_ptr<TcpEndpoint>& ep)
{
    Socket s(io);
    s.connect({asio::ip::make_address("127.0.0.1"), ep->endpoint_info()["port"].get<uint16_t>()});
    s.set_option(asio::ip::tcp::no_delay(true));
    return s;
}
static void state_waiters()
{
    auto    op  = TcpOpState::create();
    auto    f   = std::make_shared<TcpFuture>(op);
    int32_t out = 456;
    CHECK(!f->wait_for(0, &out));
    CHECK(out == 456);
    auto a = std::async(std::launch::async, [f] { return f->wait(); });
    auto b = std::async(std::launch::async, [f] { return f->wait(); });
    op->complete(TCP_FAILED);
    op->complete(TCP_SUCCESS);
    CHECK(a.get() == TCP_FAILED);
    CHECK(b.get() == TCP_FAILED);
}
static void delayed_header()
{
    asio::io_context        io;
    asio::ip::tcp::acceptor acceptor(io, {asio::ip::make_address("127.0.0.1"), 0});
    auto conn = std::make_shared<PooledConnection>(io, "127.0.0.1", acceptor.local_endpoint().port());
    conn->socket.connect(acceptor.local_endpoint());
    auto peer = acceptor.accept();
    conn->socket.set_option(asio::socket_base::send_buffer_size(1024));
    conn->socket.non_blocking(true);
    std::array<char, 16384> fill{};
    size_t                  prefix = 0;
    for (;;) {
        asio::error_code ec;
        auto             n = conn->socket.write_some(asio::buffer(fill), ec);
        prefix += n;
        if (ec == asio::error::would_block || ec == asio::error::try_again)
            break;
        CHECK(!ec);
    }
    conn->socket.non_blocking(false);
    std::vector<char> payload(2 * 1024 * 1024, 'Q');
    bool              done    = false;
    auto              session = std::make_shared<ClientSession>(io, conn, [&](asio::error_code ec) {
        CHECK(!ec);
        done = true;
    });
    session->start({{{payload.size(), 0, OP_SEND}, reinterpret_cast<uintptr_t>(payload.data())}},
                   std::chrono::steady_clock::now() + std::chrono::seconds(5));
    session.reset();
    std::array<char, 32768> stack_churn{};
    stack_churn.fill('X');
    auto reader = std::async(std::launch::async, [&] {
        std::vector<char> discarded(prefix);
        asio::read(peer, asio::buffer(discarded));
        auto h = header(peer);
        CHECK(h.opcode == OP_SEND);
        CHECK(h.size == payload.size());
        std::vector<char> received(payload.size());
        asio::read(peer, asio::buffer(received));
        CHECK(received == payload);
    });
    io.run();
    reader.get();
    CHECK(done);
}
static void batch_last_span(size_t count)
{
    std::promise<void> reached, release;
    auto               proceed = release.get_future();
    RawPeer            peer([&](RawPeer& p) {
        auto socket = p.accept();
        for (size_t i = 0; i < count; ++i) {
            auto h = header(socket);
            CHECK(h.opcode == OP_READ);
            CHECK(h.size == 1);
            CHECK(h.addr == 4096 + i);
            if (i + 1 == count) {
                reached.set_value();
                proceed.wait();
            }
            unsigned char v = static_cast<unsigned char>(i % 251);
            asio::write(socket, asio::buffer(&v, 1));
        }
    });
    auto               client = endpoint();
    client->connect(peer.info());
    CHECK(client->is_connected());
    std::vector<unsigned char> data(count, 255);
    auto local  = client->register_memory_region("local", reinterpret_cast<uintptr_t>(data.data()), 0, count);
    auto remote = client->register_remote_memory_region("remote", {{"addr", 4096}, {"length", count}});
    std::vector<assign_tuple_t> spans;
    for (size_t i = 0; i < count; ++i)
        spans.emplace_back(local, remote, i, i, 1);
    auto future = client->read(spans, 5000);
    reached.get_future().wait();
    int32_t result = 321;
    CHECK(!future->wait_for(20, &result));
    CHECK(data.back() == 255);
    release.set_value();
    CHECK(wait(future) == TCP_SUCCESS);
    for (size_t i = 0; i < count; ++i)
        CHECK(data[i] == i % 251);
    peer.join();
}
static void reuse_and_timeout()
{
    std::atomic<int> accepts{0};
    RawPeer          peer([&](RawPeer& p) {
        auto first = p.accept();
        ++accepts;
        for (int i = 0; i < 2; ++i) {
            auto h = header(first);
            CHECK(h.opcode == OP_READ);
            char v = 'A' + i;
            asio::write(first, asio::buffer(&v, 1));
        }
        (void)header(first);  // Third request stalls until its deadline closes the socket.
        char             v{};
        asio::error_code ec;
        first.read_some(asio::buffer(&v, 1), ec);
        CHECK(ec);
        auto second = p.accept();
        ++accepts;
        (void)header(second);
        v = 'Z';
        asio::write(second, asio::buffer(&v, 1));
    });
    auto             client = endpoint();
    client->connect(peer.info());
    char data = 0;
    auto l    = client->register_memory_region("data", reinterpret_cast<uintptr_t>(&data), 0, 1);
    auto r    = client->register_remote_memory_region("remote", {{"addr", 4096}, {"length", 1}});
    for (int i = 0; i < 2; ++i) {
        CHECK(wait(client->read({{l, r, 0, 0, 1}})) == TCP_SUCCESS);
        CHECK(data == 'A' + i);
    }
    CHECK(accepts == 1);
    CHECK(wait(client->read({{l, r, 0, 0, 1}}, 80)) == TCP_TIMEOUT);
    CHECK(wait(client->read({{l, r, 0, 0, 1}})) == TCP_SUCCESS);
    CHECK(data == 'Z');
    peer.join();
    CHECK(accepts == 2);
}
static void read_eof_and_shutdown(bool shutdown)
{
    std::promise<void> reached;
    RawPeer            peer([&](RawPeer& p) {
        auto socket = p.accept();
        (void)header(socket);
        if (!shutdown) {
            char partial[] = "abc";
            asio::write(socket, asio::buffer(partial, 3));
            return;
        }
        reached.set_value();
        char             byte;
        asio::error_code ec;
        socket.read_some(asio::buffer(&byte, 1), ec);
        CHECK(ec);
    });
    auto               client = endpoint();
    client->connect(peer.info());
    std::array<char, 8> data{};
    auto l = client->register_memory_region("data", reinterpret_cast<uintptr_t>(data.data()), 0, data.size());
    auto r = client->register_remote_memory_region("remote", {{"addr", 4096}, {"length", data.size()}});
    auto f = client->read({{l, r, 0, 0, data.size()}}, 30000);
    if (shutdown) {
        reached.get_future().wait();
        client->shutdown();
        client->shutdown();
    }
    CHECK(wait(f) == (shutdown ? TCP_CLOSED : TCP_FAILED));
    peer.join();
}
static void send_framing()
{
    auto             server = endpoint();
    asio::io_context io;
    auto             socket = raw_connect(io, server);
    char             marker = 'K';
    server->register_memory_region("marker", reinterpret_cast<uintptr_t>(&marker), 0, 1);
    auto barrier = [&] {
        header(socket, {1, reinterpret_cast<uintptr_t>(&marker), OP_READ});
        char value{};
        asio::read(socket, asio::buffer(&value, 1));
        CHECK(value == marker);
    };
    std::vector<char> payload(65537, 'A');
    // No posted recv: discard exactly this frame and preserve the next READ.
    header(socket, {payload.size(), 0, OP_SEND});
    asio::write(socket, asio::buffer(payload));
    barrier();
    std::array<char, 4> dst{};
    auto                recv = server->recv({reinterpret_cast<uintptr_t>(dst.data()), 0, dst.size()}, true);
    header(socket, {payload.size(), 0, OP_SEND});
    asio::write(socket, asio::buffer(payload));
    CHECK(wait(recv) == TCP_FAILED);
    barrier();
    recv = server->recv({reinterpret_cast<uintptr_t>(dst.data()), 0, dst.size()});
    header(socket, {payload.size(), 0, OP_SEND});
    asio::write(socket, asio::buffer(payload));
    CHECK(wait(recv) == TCP_SUCCESS);
    CHECK(dst[0] == 'A' && dst[3] == 'A');
    barrier();
    recv = server->recv({0, 0, 0}, true);
    header(socket, {0, 0, OP_SEND});
    CHECK(wait(recv) == TCP_SUCCESS);
    recv = server->recv({reinterpret_cast<uintptr_t>(dst.data()), 0, dst.size()}, true);
    SessionHeader h{htole64(dst.size()), 0, OP_SEND};
    const auto*   bytes = reinterpret_cast<const char*>(&h);
    for (size_t i = 0; i < sizeof(h); ++i)
        asio::write(socket, asio::buffer(bytes + i, 1));
    for (char value : std::string("test"))
        asio::write(socket, asio::buffer(&value, 1));
    CHECK(wait(recv) == TCP_SUCCESS);
    CHECK(std::string(dst.data(), 4) == "test");
    recv = server->recv({reinterpret_cast<uintptr_t>(dst.data()), 0, dst.size()}, true);
    header(socket, {dst.size(), 0, OP_SEND});
    asio::write(socket, asio::buffer("x", 1));
    socket.close();
    CHECK(wait(recv) == TCP_FAILED);
    recv = server->recv({reinterpret_cast<uintptr_t>(dst.data()), 0, dst.size()});
    server->shutdown();
    CHECK(wait(recv) == TCP_CLOSED);
}
static void invalid_frames()
{
    auto             server = endpoint();
    asio::io_context io;
    for (auto h : {SessionHeader{0, 0, 255}, SessionHeader{8, 1, OP_READ}, SessionHeader{8, 1, OP_WRITE}}) {
        auto socket = raw_connect(io, server);
        header(socket, h);
        char             value;
        asio::error_code ec;
        socket.read_some(asio::buffer(&value, 1), ec);
        CHECK(ec);
    }
}
static void endpoint_transfers()
{
    auto                       client = endpoint(), server = endpoint();
    std::vector<unsigned char> source(8192), target(8192, 0), output(8192);
    for (size_t i = 0; i < source.size(); ++i)
        source[i] = i % 251;
    auto l   = client->register_memory_region("source", reinterpret_cast<uintptr_t>(source.data()), 0, source.size());
    auto out = client->register_memory_region("output", reinterpret_cast<uintptr_t>(output.data()), 0, output.size());
    server->register_memory_region("target", reinterpret_cast<uintptr_t>(target.data()), 0, target.size());
    client->connect(server->endpoint_info());
    auto                        r = client->register_remote_memory_region("target", server->mr_info()["target"]);
    std::vector<assign_tuple_t> spans;
    for (size_t i = 0; i < 1000; ++i)
        spans.emplace_back(l, r, i, i, 1);
    CHECK(wait(client->write(spans)) == TCP_SUCCESS);
    // Same persistent connection: READ is a protocol barrier after legacy WRITE.
    CHECK(wait(client->read({{out, r, 0, 0, 1000}})) == TCP_SUCCESS);
    CHECK(std::equal(source.begin(), source.begin() + 1000, output.begin()));
    bool rejected = false;
    try {
        client->write({{l, r, 1000, 0, 1}, {l, r, target.size(), 0, 1}});
    }
    catch (const std::invalid_argument&) {
        rejected = true;
    }
    CHECK(rejected);
    CHECK(wait(client->read({{out, r, 1000, 0, 1}})) == TCP_SUCCESS);
    CHECK(output[0] == 0);
    CHECK(wait(client->write({{l, r, 0, 0, 1}}, 0)) == TCP_TIMEOUT);
    auto future = client->read({{out, r, 0, 0, output.size()}});
    CHECK(wait(future) == TCP_SUCCESS);
}
static void concurrent_reads()
{
    auto                   client = endpoint(), server = endpoint();
    std::array<char, 4096> source{};
    source.fill('R');
    server->register_memory_region("source", reinterpret_cast<uintptr_t>(source.data()), 0, source.size());
    client->connect(server->endpoint_info());
    const auto remote = client->register_remote_memory_region("source", server->mr_info()["source"]);
    std::vector<std::future<void>> jobs;
    for (int i = 0; i < 8; ++i)
        jobs.push_back(std::async(std::launch::async, [&, i] {
            std::array<char, 4096> local{};
            auto                   l = client->register_memory_region(
                "local" + std::to_string(i), reinterpret_cast<uintptr_t>(local.data()), 0, local.size());
            for (int j = 0; j < 16; ++j) {
                CHECK(wait(client->read({{l, remote, 0, 0, local.size()}})) == TCP_SUCCESS);
                CHECK(local == source);
            }
        }));
    for (auto& j : jobs)
        j.get();
}
static void write_deadline()
{
    std::promise<void> release;
    auto               resume = release.get_future();
    RawPeer            peer([&](RawPeer& p) {
        auto socket = p.accept();
        (void)header(socket);
        resume.wait();
        std::array<char, 65536> discard{};
        asio::error_code        ec;
        while (!ec)
            socket.read_some(asio::buffer(discard), ec);
    });
    auto               ep = endpoint();
    ep->connect(peer.info());
    std::vector<char> data(32 * 1024 * 1024, 'T');
    auto              f = ep->send({reinterpret_cast<uintptr_t>(data.data()), 0, data.size()}, 100);
    CHECK(wait(f) == TCP_TIMEOUT);
    data.clear();
    data.shrink_to_fit();  // Completion must retire the async buffer first.
    release.set_value();
    peer.join();
}
static void cancel_concurrent_reads()
{
    constexpr int      count = 8;
    std::promise<void> reached;
    RawPeer            peer([&](RawPeer& p) {
        std::vector<Socket> sockets;
        for (int i = 0; i < count; ++i) {
            sockets.push_back(p.accept());
            (void)header(sockets.back());
        }
        reached.set_value();
        for (auto& socket : sockets) {
            char             byte;
            asio::error_code ec;
            socket.read_some(asio::buffer(&byte, 1), ec);
            CHECK(ec);
        }
    });
    auto               ep = endpoint();
    ep->connect(peer.info());
    auto                          r = ep->register_remote_memory_region("remote", {{"addr", 4096}, {"length", 8}});
    std::vector<std::future<int>> workers;
    for (int i = 0; i < count; ++i)
        workers.push_back(std::async(std::launch::async, [&, i] {
            std::array<char, 8> buffer{};
            auto                l = ep->register_memory_region(
                "buffer" + std::to_string(i), reinterpret_cast<uintptr_t>(buffer.data()), 0, 8);
            return wait(ep->read({{l, r, 0, 0, 8}}));
        }));
    reached.get_future().wait();
    ep->shutdown();
    for (auto& worker : workers)
        CHECK(worker.get() == TCP_CLOSED);
    peer.join();
}
static json receive_json(Socket& socket)
{
    asio::streambuf buffer;
    asio::read_until(socket, buffer, '\n');
    std::istream in(&buffer);
    std::string  line;
    std::getline(in, line);
    return json::parse(line);
}
static void send_json(Socket& socket, const json& value)
{
    const auto line = value.dump() + "\n";
    asio::write(socket, asio::buffer(line));
}
static void serve(const std::string& ip, const std::string& manifest)
{
    asio::io_context           io;
    asio::ip::tcp::acceptor    acceptor(io, {asio::ip::make_address(ip), 0});
    auto                       ep = std::make_shared<TcpEndpoint>(ip);
    std::vector<unsigned char> source(262144), sink(source.size(), 0);
    for (size_t i = 0; i < source.size(); ++i)
        source[i] = i % 251;
    ep->register_memory_region("source", reinterpret_cast<uintptr_t>(source.data()), 0, source.size());
    ep->register_memory_region("sink", reinterpret_cast<uintptr_t>(sink.data()), 0, sink.size());
    std::ofstream(manifest) << acceptor.local_endpoint().port() << std::endl;
    auto socket = acceptor.accept();
    send_json(socket, ep->endpoint_info());
    auto result = receive_json(socket);
    CHECK(result["passed"].get<bool>());
    ep->shutdown();  // Join the writer before inspecting its destination from this thread.
    CHECK(source == sink);
    send_json(socket, {{"passed", true}});
    std::cout << "cross-host server passed\n";
}
static void client(const std::string& ip, uint16_t port)
{
    asio::io_context io;
    Socket           socket(io);
    socket.connect({asio::ip::make_address(ip), port});
    auto info = receive_json(socket);
    auto ep   = endpoint();
    ep->connect(info);
    std::vector<unsigned char> data(262144), verification(data.size());
    auto l = ep->register_memory_region("data", reinterpret_cast<uintptr_t>(data.data()), 0, data.size());
    auto v =
        ep->register_memory_region("verify", reinterpret_cast<uintptr_t>(verification.data()), 0, verification.size());
    auto source = ep->register_remote_memory_region("source", info["mr_info"]["source"]);
    auto sink   = ep->register_remote_memory_region("sink", info["mr_info"]["sink"]);
    CHECK(wait(ep->read({{l, source, 0, 0, data.size()}})) == TCP_SUCCESS);
    for (size_t i = 0; i < data.size(); ++i)
        CHECK(data[i] == i % 251);
    CHECK(wait(ep->write({{l, sink, 0, 0, data.size()}})) == TCP_SUCCESS);
    CHECK(wait(ep->read({{v, sink, 0, 0, data.size()}})) == TCP_SUCCESS);
    CHECK(data == verification);
    send_json(socket, {{"passed", true}});
    CHECK(receive_json(socket)["passed"].get<bool>());
    std::cout << "cross-host client READ/WRITE passed\n";
}
int main(int argc, char** argv)
{
    try {
        if (argc == 4 && std::string(argv[1]) == "--serve") {
            serve(argv[2], argv[3]);
            return 0;
        }
        if (argc == 4 && std::string(argv[1]) == "--client") {
            client(argv[2], std::stoi(argv[3]));
            return 0;
        }
        auto run = [](const char* name, auto fn) {
            fn();
            std::cout << "PASS " << name << std::endl;
        };
        run("future waiters", state_waiters);
        run("delayed executor and backpressured header", delayed_header);
        for (size_t count : {31, 32, 33, 1000}) {
            batch_last_span(count);
            std::cout << "PASS last span " << count << std::endl;
        }
        run("connection reuse and deadline recovery", reuse_and_timeout);
        run("partial READ then EOF", [] { read_eof_and_shutdown(false); });
        run("shutdown during READ", [] { read_eof_and_shutdown(true); });
        run("SEND framing and receive failures", send_framing);
        run("invalid opcode and registered range", invalid_frames);
        run("WRITE batches and prevalidation", endpoint_transfers);
        run("concurrent readers and registrations", concurrent_reads);
        run("backpressured SEND deadline", write_deadline);
        run("shutdown with concurrent requests", cancel_concurrent_reads);
        return 0;
    }
    catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << std::endl;
        return 1;
    }
}
