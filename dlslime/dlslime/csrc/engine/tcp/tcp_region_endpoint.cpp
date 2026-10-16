#include "tcp_region_endpoint.h"

#include <atomic>
#include <map>
#include <optional>

#include "region_wire.h"
#include "tcp_context.h"

namespace dlslime::tcp {
namespace rw = region_wire;
namespace {
using Socket = asio::ip::tcp::socket;
using Clock  = std::chrono::steady_clock;
Clock::time_point deadline(int64_t ms)
{
    if (ms < 0)
        throw std::invalid_argument("Negative Region TCP deadline");
    auto now     = Clock::now();
    auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::time_point::max() - now).count();
    return ms >= maximum ? Clock::time_point::max() : now + std::chrono::milliseconds(ms);
}
// One read and one write can coexist. Completion is delayed until both socket
// callbacks retire; timer callbacks never reference application payloads.
class Session: public std::enable_shared_from_this<Session> {
public:
    explicit Session(Socket& socket): socket_(socket), timer_(socket.get_executor()) {}
    virtual ~Session() = default;
    void cancel(int32_t status = REGION_CANCELLED)
    {
        if (done_)
            return;
        if (!error_)
            error_ = status;
        asio::error_code ignored;
        socket_.cancel(ignored);
        socket_.close(ignored);
        if (!pending_)
            finish();
    }

protected:
    void arm(Clock::time_point when)
    {
        timer_.expires_at(when);
        timer_.async_wait([self = shared_from_this()](asio::error_code ec) {
            if (!ec)
                self->cancel(REGION_TIMEOUT);
        });
    }
    template<class Start, class Next>
    void io(Start start, Next next)
    {
        ++pending_;
        try {
            start([self = shared_from_this(), next = std::move(next)](asio::error_code ec, auto...) {
                --self->pending_;
                if (ec || self->error_) {
                    self->cancel(self->error_.value_or(REGION_IO_ERROR));
                    return;
                }
                try {
                    next();
                }
                catch (...) {
                    self->cancel(REGION_PROTOCOL_ERROR);
                }
            });
        }
        catch (...) {
            --pending_;
            cancel(REGION_IO_ERROR);
        }
    }
    template<class Buffer, class Next>
    void receive(Buffer buffer, Next next)
    {
        io([&](auto cb) { asio::async_read(socket_, buffer, std::move(cb)); }, std::move(next));
    }
    template<class Buffer, class Next>
    void send(Buffer buffer, Next next)
    {
        io([&](auto cb) { asio::async_write(socket_, buffer, std::move(cb)); }, std::move(next));
    }
    void finish()
    {
        if (done_ || pending_)
            return;
        done_ = true;
        timer_.cancel();
        retired(error_.value_or(REGION_SUCCESS));
    }
    virtual void           retired(int32_t) = 0;
    Socket&                socket_;
    asio::steady_timer     timer_;
    size_t                 pending_{};
    bool                   done_{};
    std::optional<int32_t> error_;
};
struct Transfer {
    rw::Header    header;
    region::Lease lease;
};
class Client final: public Session {
public:
    Client(std::shared_ptr<PooledConnection> connection,
           std::vector<Transfer>             transfers,
           RegionOptions                     options,
           std::function<void(int32_t)>      done):
        Session(connection->socket),
        connection_(std::move(connection)),
        transfers_(std::move(transfers)),
        options_(options),
        complete_(std::move(done))
    {
    }
    void start(const asio::ip::tcp::endpoint& peer, Clock::time_point until)
    {
        arm(until);
        if (socket_.is_open()) {
            pump();
            return;
        }
        io([&](auto cb) { socket_.async_connect(peer, std::move(cb)); },
           [this] {
               socket_.set_option(asio::ip::tcp::no_delay(true));
               pump();
           });
    }

private:
    void pump()
    {
        if (done_ || error_)
            return;
        if (received_ == transfers_.size()) {
            finish();
            return;
        }
        if (!writing_ && issued_ < transfers_.size() && issued_ - received_ < options_.window_requests) {
            auto& t = transfers_[issued_];
            if (issued_ == received_
                || (bytes_ <= options_.window_bytes && t.header.length <= options_.window_bytes - bytes_)) {
                outgoing_ = rw::encode(t.header);
                ++issued_;
                bytes_ += t.header.length;
                writing_ = true;
                std::array<asio::const_buffer, 2> buffers{
                    asio::buffer(outgoing_),
                    asio::buffer(t.lease.data(), t.header.opcode == rw::write ? t.lease.size() : 0)};
                send(buffers, [this] {
                    writing_ = false;
                    pump();
                });
            }
        }
        if (!done_ && !error_ && !reading_ && received_ < issued_) {
            reading_ = true;
            receive(asio::buffer(incoming_), [this] {
                auto header   = rw::decode(incoming_);
                auto expected = transfers_[received_].header;
                expected.opcode |= rw::response;
                expected.status = header.status;
                if (header != expected || header.status > rw::malformed) {
                    cancel(REGION_PROTOCOL_ERROR);
                    return;
                }
                if (header.status != rw::ok) {
                    cancel(REGION_REJECTED);
                    return;
                }
                if (transfers_[received_].header.opcode == rw::read) {
                    auto& lease = transfers_[received_].lease;
                    receive(asio::buffer(lease.data(), lease.size()), [this] { acknowledge(); });
                }
                else
                    acknowledge();
            });
        }
    }
    void acknowledge()
    {
        bytes_ -= transfers_[received_].header.length;
        ++received_;
        reading_ = false;
        pump();
    }
    void retired(int32_t status) override
    {
        // Release every local lease BEFORE publishing Future completion.
        transfers_.clear();
        complete_(status);
    }
    std::shared_ptr<PooledConnection> connection_;
    std::vector<Transfer>             transfers_;
    RegionOptions                     options_;
    std::function<void(int32_t)>      complete_;
    rw::Bytes                         incoming_{}, outgoing_{};
    size_t                            issued_{}, received_{}, bytes_{};
    bool                              writing_{}, reading_{};
};
// The owned socket must be constructed before Session stores its reference.
struct SocketOwner {
    explicit SocketOwner(Socket s): owned(std::move(s)) {}
    Socket owned;
};
class Server final: private SocketOwner, public Session {
public:
    Server(Socket                                      socket,
           std::shared_ptr<region::HostRegionRegistry> registry,
           int64_t                                     timeout,
           std::function<void(Server*)>                done):
        SocketOwner(std::move(socket)),
        Session(owned),
        registry_(std::move(registry)),
        timeout_(timeout),
        complete_(std::move(done))
    {
    }
    void start()
    {
        next();
    }

private:
    void next()
    {
        lease_.reset();
        arm(deadline(timeout_));
        receive(asio::buffer(input_), [this] { request(); });
    }
    void request()
    {
        header_ = rw::decode(input_);
        if ((header_.opcode != rw::read && header_.opcode != rw::write) || header_.status || !header_.request
            || header_.length > rw::max_payload) {
            cancel(REGION_PROTOCOL_ERROR);
            return;
        }
        try {
            lease_ = registry_->acquire(header_.key,
                                        header_.offset,
                                        header_.length,
                                        header_.opcode == rw::read ? region::Access::read : region::Access::write);
        }
        catch (const region::RegionError& error) {
            uint32_t status = rw::stale;
            if (error.code == region::Error::permission)
                status = rw::denied;
            if (error.code == region::Error::bounds)
                status = rw::bounds;
            if (error.code == region::Error::closed)
                status = rw::closed;
            reply(status);
            return;
        }
        if (header_.opcode == rw::write) {
            receive(asio::buffer(lease_.data(), lease_.size()), [this] { reply(rw::ok); });
        }
        else
            reply(rw::ok);
    }
    void reply(uint32_t status)
    {
        const bool payload  = header_.opcode == rw::read && status == rw::ok;
        auto       response = header_;
        response.opcode |= rw::response;
        response.status = status;
        output_         = rw::encode(response);
        std::array<asio::const_buffer, 2> buffers{asio::buffer(output_),
                                                  asio::buffer(lease_.data(), payload ? lease_.size() : 0)};
        // A successful WRITE response is issued only after the entire HOST
        // destination has been received. Keep its remote lease through ACK I/O.
        send(buffers, [this, status] {
            if (status != rw::ok)
                cancel(REGION_REJECTED);
            else
                next();
        });
    }
    void retired(int32_t) override
    {
        lease_.reset();
        complete_(this);
    }
    std::shared_ptr<region::HostRegionRegistry> registry_;
    int64_t                                     timeout_;
    std::function<void(Server*)>                complete_;
    rw::Bytes                                   input_{}, output_{};
    rw::Header                                  header_;
    region::Lease                               lease_;
};
}  // namespace
class RegionEndpointImpl: public std::enable_shared_from_this<RegionEndpointImpl> {
public:
    RegionEndpointImpl(const std::string& ip, uint16_t port, RegionOptions opts):
        options(opts), acceptor(context.io_context())
    {
        if (!opts.window_requests || opts.window_requests > 4096 || !opts.window_bytes
            || opts.window_bytes > rw::max_payload || !opts.max_clients || opts.max_clients > 4096 || !opts.max_servers
            || opts.max_servers > 4096 || opts.server_timeout_ms <= 0)
            throw std::invalid_argument("Invalid Region TCP limits");
        asio::ip::tcp::endpoint address(asio::ip::make_address(ip), port);
        acceptor.open(address.protocol());
        acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor.bind(address);
        acceptor.listen(64);
        peer = {ip, acceptor.local_endpoint().port(), rw::version};
    }
    ~RegionEndpointImpl()
    {
        stop();
    }
    void start()
    {
        asio::post(context.io_context(), [this] { accept(); });
    }
    void accept()
    {
        if (!running)
            return;
        acceptor.async_accept([this](asio::error_code ec, Socket socket) {
            if (!running || ec == asio::error::operation_aborted)
                return;
            if (!ec && servers.size() < options.max_servers) {
                socket.set_option(asio::ip::tcp::no_delay(true), ec);
                if (!ec) {
                    auto s = std::make_shared<Server>(std::move(socket),
                                                      registry,
                                                      options.server_timeout_ms,
                                                      [this](Server* p) { servers.erase(p); });
                    servers.emplace(s.get(), s);
                    s->start();
                }
            }
            accept();
        });
    }
    void stop()
    {
        std::lock_guard shutdown_lock(shutdown_mutex);
        {
            std::lock_guard lock(api_mutex);
            if (!running)
                return;
            running = false;
            asio::post(context.io_context(), [this] {
                asio::error_code ignored;
                acceptor.close(ignored);
                auto c = clients;
                for (auto& [_, s] : c)
                    s->cancel();
                auto ss = servers;
                for (auto& [_, s] : ss)
                    s->cancel();
            });
        }
        context.shutdown();
        registry->close();
    }
    void cancel(const std::shared_ptr<TcpOpState>& op)
    {
        std::lock_guard lock(api_mutex);
        if (running)
            asio::post(context.io_context(), [this, op] {
                auto it = clients.find(op.get());
                if (it != clients.end())
                    it->second->cancel();
            });
    }
    RegionOptions                                  options;
    TcpContext                                     context;
    asio::ip::tcp::acceptor                        acceptor;
    RegionPeer                                     peer;
    std::shared_ptr<region::HostRegionRegistry>    registry = std::make_shared<region::HostRegionRegistry>();
    std::mutex                                     api_mutex, shutdown_mutex;
    std::atomic<bool>                              running{true};
    uint64_t                                       next_request{1};
    size_t                                         admitted{};  // protected by api_mutex, includes queued operations
    std::map<TcpOpState*, std::shared_ptr<Client>> clients;
    std::map<Server*, std::shared_ptr<Server>>     servers;
};
TcpRegionEndpoint::TcpRegionEndpoint(const std::string& ip, uint16_t port, RegionOptions options):
    impl_(std::make_shared<RegionEndpointImpl>(ip, port, options))
{
    impl_->start();
}
TcpRegionEndpoint::~TcpRegionEndpoint()
{
    shutdown();
}
std::shared_ptr<region::HostRegionRegistry> TcpRegionEndpoint::regions() const
{
    return impl_->registry;
}
RegionPeer TcpRegionEndpoint::endpoint_info() const
{
    return impl_->peer;
}
void TcpRegionEndpoint::shutdown()
{
    impl_->stop();
}
std::shared_ptr<RegionFuture>
TcpRegionEndpoint::read(const RegionPeer& peer, const std::vector<RegionSpan>& spans, int64_t ms)
{
    return transfer(peer, spans, ms, rw::read);
}
std::shared_ptr<RegionFuture>
TcpRegionEndpoint::write(const RegionPeer& peer, const std::vector<RegionSpan>& spans, int64_t ms)
{
    return transfer(peer, spans, ms, rw::write);
}
std::shared_ptr<RegionFuture>
TcpRegionEndpoint::transfer(const RegionPeer& peer, const std::vector<RegionSpan>& spans, int64_t ms, uint16_t opcode)
{
    const auto until = deadline(ms);
    if (peer.protocol != rw::version || !peer.port || spans.empty() || spans.size() > 4096)
        throw std::invalid_argument("Region TCP requires protocol 2, a port, and 1..4096 spans");
    const asio::ip::tcp::endpoint address(asio::ip::make_address(peer.host), peer.port);
    auto&                         p = *impl_;
    std::lock_guard               lock(p.api_mutex);
    if (!p.running)
        throw std::runtime_error("Region TCP endpoint is closed");
    auto future     = std::make_shared<RegionFuture>();
    auto op         = TcpOpState::create();
    future->state_  = op;
    future->cancel_ = [weak = std::weak_ptr<RegionEndpointImpl>(impl_), op] {
        if (auto p = weak.lock())
            p->cancel(op);
    };
    if (p.admitted >= p.options.max_clients) {
        op->complete(REGION_BUSY);
        return future;
    }
    if (spans.size() > UINT64_MAX - p.next_request)
        throw std::overflow_error("Region request ids exhausted");
    std::vector<Transfer> transfers;
    transfers.reserve(spans.size());
    for (const auto& span : spans) {
        auto required = opcode == rw::write ? region::Access::write : region::Access::read;
        if (span.length > rw::max_payload || span.remote_offset > span.remote.length
            || span.length > span.remote.length - span.remote_offset)
            throw std::invalid_argument("Remote Region range or payload limit exceeded");
        if ((uint32_t(span.remote.access) & uint32_t(required)) != uint32_t(required))
            throw std::invalid_argument("Remote Region permission denied");
        auto local = p.registry->acquire(span.local,
                                         span.local_offset,
                                         span.length,
                                         opcode == rw::write ? region::Access::read : region::Access::write);
        transfers.push_back(
            {{opcode, 0, p.next_request++, span.remote.key, span.remote_offset, span.length}, std::move(local)});
    }
    // Entire batch validated and leased before any network side effect.
    ++p.admitted;
    try {
        asio::post(p.context.io_context(), [raw = &p, op, address, until, transfers = std::move(transfers)]() mutable {
            auto complete = [raw, op](int32_t status) {
                {
                    std::lock_guard lock(raw->api_mutex);
                    --raw->admitted;
                }
                op->complete(status);
            };
            if (!raw->running) {
                transfers.clear();
                complete(REGION_CANCELLED);
                return;
            }
            try {
                auto conn = raw->context.conn_pool().getConnection(address.address().to_string(), address.port());
                auto s    = std::make_shared<Client>(
                    conn, std::move(transfers), raw->options, [raw, op, conn, complete](int32_t status) {
                        raw->context.conn_pool().returnConnection(conn);
                        raw->context.conn_pool().trimIdleConnections(raw->options.max_clients);
                        raw->clients.erase(op.get());
                        complete(status);
                    });
                raw->clients.emplace(op.get(), s);
                s->start(address, until);
            }
            catch (...) {
                auto it = raw->clients.find(op.get());
                if (it != raw->clients.end())
                    it->second->cancel(REGION_IO_ERROR);
                else {
                    transfers.clear();
                    complete(REGION_IO_ERROR);
                }
            }
        });
    }
    catch (...) {
        --p.admitted;
        throw;
    }
    return future;
}
}  // namespace dlslime::tcp
