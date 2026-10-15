#include "tcp_endpoint.h"

#include <limits>
#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

namespace dlslime::tcp {
namespace {
int32_t status(asio::error_code ec)
{
    if (ec == asio::error::timed_out)
        return TCP_TIMEOUT;
    if (ec == asio::error::operation_aborted)
        return TCP_CLOSED;
    return ec ? TCP_FAILED : TCP_SUCCESS;
}
uintptr_t checked_address(uintptr_t base, uint64_t offset, size_t length)
{
    if (offset > std::numeric_limits<uintptr_t>::max() - base
        || length > std::numeric_limits<uintptr_t>::max() - (base + offset) || (!base && length))
        throw std::invalid_argument("TCP buffer address overflow or null buffer");
    return base + offset;
}
}  // namespace
TcpEndpoint::TcpEndpoint(const std::string& ip, uint16_t port):
    local_host_(ip), own_ctx_(std::make_unique<TcpContext>()), acceptor_(own_ctx_->io_context())
{
    auto endpoint = asio::ip::tcp::endpoint(asio::ip::make_address(ip), port);
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen(64);
    local_port_ = acceptor_.local_endpoint().port();
    asio::post(own_ctx_->io_context(), [this] { do_accept(); });
}
TcpEndpoint::~TcpEndpoint()
{
    shutdown();
}
void TcpEndpoint::do_accept()
{
    if (!running_)
        return;
    acceptor_.async_accept([this](asio::error_code ec, asio::ip::tcp::socket socket) {
        if (!running_ || ec == asio::error::operation_aborted)
            return;
        if (!ec) {
            socket.set_option(asio::ip::tcp::no_delay(true), ec);
            if (!ec) {
                auto session = std::make_shared<ServerSession>(
                    std::move(socket),
                    local_pool_,
                    [this] {
                        if (pending_recvs_.empty())
                            return RecvSlot{};
                        auto slot = std::move(pending_recvs_.front());
                        pending_recvs_.pop_front();
                        return slot;
                    },
                    [this](ServerSession* s) { servers_.erase(s); });
                servers_.emplace(session.get(), session);
                session->start();
            }
        }
        do_accept();
    });
}
json TcpEndpoint::endpoint_info() const
{
    return {{"host", local_host_}, {"port", local_port_}, {"mr_info", local_pool_->mr_info()}};
}
json TcpEndpoint::mr_info() const
{
    return local_pool_->mr_info();
}
void TcpEndpoint::connect(const json& remote)
{
    std::lock_guard<std::mutex> lock(api_mu_);
    if (!running_)
        throw std::runtime_error("TCP endpoint is closed");
    const auto host = remote.at("host").get<std::string>();
    const auto port = remote.at("port").get<int>();
    if (host.empty() || port <= 0 || port > 65535)
        throw std::invalid_argument("Invalid TCP peer");
    auto op = TcpOpState::create();
    submit({}, op, kDefaultTimeoutMs, host, static_cast<uint16_t>(port));
    if (op->wait() != TCP_SUCCESS)
        return;
    if (remote.contains("mr_info"))
        for (const auto& [name, info] : remote["mr_info"].items())
            remote_pool_->register_remote_memory_region(info, name);
    peer_host_ = host;
    peer_port_ = static_cast<uint16_t>(port);
    connected_.store(true, std::memory_order_release);
}
int32_t TcpEndpoint::register_memory_region(const std::string& name, uintptr_t ptr, uintptr_t offset, size_t length)
{
    return local_pool_->register_memory_region(checked_address(ptr, offset, length), length, name);
}
int32_t TcpEndpoint::register_remote_memory_region(const std::string& name, const json& info)
{
    return remote_pool_->register_remote_memory_region(info, name);
}
void TcpEndpoint::set_read_pipeline_limits(size_t max_requests, size_t max_bytes)
{
    if (!max_requests || max_requests > 4096 || !max_bytes || max_bytes > (size_t{1} << 30))
        throw std::invalid_argument("TCP READ pipeline requires 1..4096 requests and 1..1 GiB bytes");
    std::lock_guard<std::mutex> lock(api_mu_);
    if (!running_)
        throw std::runtime_error("TCP endpoint is closed");
    read_limits_ = {max_requests, max_bytes};
}
void TcpEndpoint::submit(std::vector<TcpTransfer>           transfers,
                         const std::shared_ptr<TcpOpState>& op,
                         int64_t                            timeout_ms,
                         const std::string&                 host,
                         uint16_t                           port)
{
    if (timeout_ms < 0)
        throw std::invalid_argument("TCP timeout_ms must be nonnegative");
    const auto now = std::chrono::steady_clock::now();
    const auto max_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(ClientSession::Deadline::max() - now).count();
    const auto deadline =
        timeout_ms >= max_ms ? ClientSession::Deadline::max() : now + std::chrono::milliseconds(timeout_ms);
    asio::post(own_ctx_->io_context(),
               [this, transfers = std::move(transfers), op, host, port, deadline, limits = read_limits_]() mutable {
                   if (!running_) {
                       op->complete(TCP_CLOSED);
                       return;
                   }
                   auto& pool    = own_ctx_->conn_pool();
                   auto  conn    = pool.getConnection(host, port);
                   auto  session = std::make_shared<ClientSession>(
                       own_ctx_->io_context(),
                       conn,
                       [this, conn, op](asio::error_code ec) {
                           own_ctx_->conn_pool().returnConnection(conn);
                           clients_.erase(conn.get());
                           op->complete(status(ec));
                       },
                       limits);
                   clients_.emplace(conn.get(), session);
                   session->start(std::move(transfers), deadline);
               });
}
std::shared_ptr<TcpSendFuture> TcpEndpoint::send(const chunk_tuple_t& chunk, int64_t timeout_ms)
{
    std::lock_guard<std::mutex> lock(api_mu_);
    auto                        ptr    = checked_address(std::get<0>(chunk), std::get<1>(chunk), std::get<2>(chunk));
    auto                        op     = TcpOpState::create();
    auto                        future = std::make_shared<TcpSendFuture>(op);
    if (!running_ || !connected_) {
        op->complete(running_ ? TCP_FAILED : TCP_CLOSED);
        return future;
    }
    submit({{{std::get<2>(chunk), 0, OP_SEND}, ptr}}, op, timeout_ms, peer_host_, peer_port_);
    return future;
}
std::shared_ptr<TcpRecvFuture> TcpEndpoint::recv(const chunk_tuple_t& chunk, bool exact_size)
{
    std::lock_guard<std::mutex> lock(api_mu_);
    auto                        op = TcpOpState::create();
    op->user_buffer                = checked_address(std::get<0>(chunk), std::get<1>(chunk), std::get<2>(chunk));
    op->user_length                = std::get<2>(chunk);
    auto future                    = std::make_shared<TcpRecvFuture>(op);
    if (!running_) {
        op->complete(TCP_CLOSED);
        return future;
    }
    RecvSlot slot{op->user_buffer, op->user_length, op, {}, exact_size};
#ifdef USE_CUDA
    cudaPointerAttributes attr{};
    if (cudaPointerGetAttributes(&attr, reinterpret_cast<void*>(op->user_buffer)) == cudaSuccess
        && attr.type == cudaMemoryTypeDevice) {
        auto buffer    = std::make_shared<std::vector<char>>(op->user_length);
        slot.buffer    = reinterpret_cast<uintptr_t>(buffer->data());
        slot.post_read = [buffer, dst = op->user_buffer](size_t bytes) -> asio::error_code {
            if (cudaMemcpy(reinterpret_cast<void*>(dst), buffer->data(), bytes, cudaMemcpyHostToDevice) != cudaSuccess)
                return asio::error::fault;
            return {};
        };
    }
#endif
    asio::post(own_ctx_->io_context(), [this, slot = std::move(slot)]() mutable {
        if (!running_)
            slot.op_state->complete(TCP_CLOSED);
        else
            pending_recvs_.push_back(std::move(slot));
    });
    return future;
}
std::shared_ptr<TcpReadWriteFuture>
TcpEndpoint::transfer(const std::vector<assign_tuple_t>& assign, int64_t timeout_ms, uint8_t opcode)
{
    std::lock_guard<std::mutex> lock(api_mu_);
    if (assign.empty())
        throw std::invalid_argument("TCP empty assignment");
    std::vector<TcpTransfer> transfers;
    transfers.reserve(assign.size());
    // Validate every span before submitting any I/O, so a late invalid handle
    // cannot throw while earlier spans still reference caller-owned buffers.
    for (const auto& a : assign) {
        const auto lh = std::get<0>(a), rh = std::get<1>(a);
        if (lh > INT32_MAX || rh > INT32_MAX)
            throw std::invalid_argument("TCP invalid MR handle");
        const auto local  = local_pool_->get_mr_fast(static_cast<int32_t>(lh));
        const auto remote = remote_pool_->get_remote_mr_fast(static_cast<int32_t>(rh));
        const auto ro = std::get<2>(a), lo = std::get<3>(a), len = std::get<4>(a);
        if (!local.addr || !remote.addr || lo > local.length || len > local.length - lo || ro > remote.length
            || len > remote.length - ro)
            throw std::invalid_argument("TCP invalid MR range");
        transfers.push_back(
            {{len, checked_address(remote.addr, ro, len), opcode}, checked_address(local.addr, lo, len)});
    }
    auto op     = TcpOpState::create();
    auto future = std::make_shared<TcpReadWriteFuture>(op);
    if (!running_ || !connected_) {
        op->complete(running_ ? TCP_FAILED : TCP_CLOSED);
        return future;
    }
    submit(std::move(transfers), op, timeout_ms, peer_host_, peer_port_);
    return future;
}
std::shared_ptr<TcpReadWriteFuture> TcpEndpoint::read(const std::vector<assign_tuple_t>& a, int64_t timeout_ms)
{
    return transfer(a, timeout_ms, OP_READ);
}
std::shared_ptr<TcpReadWriteFuture> TcpEndpoint::write(const std::vector<assign_tuple_t>& a, int64_t timeout_ms)
{
    return transfer(a, timeout_ms, OP_WRITE);
}
void TcpEndpoint::shutdown()
{
    std::lock_guard<std::mutex> lock(api_mu_);
    if (!running_.exchange(false))
        return;
    connected_.store(false, std::memory_order_release);
    asio::post(own_ctx_->io_context(), [this] {
        asio::error_code ignored;
        acceptor_.close(ignored);
        for (auto& slot : pending_recvs_)
            slot.op_state->complete(TCP_CLOSED);
        pending_recvs_.clear();
        const auto clients = clients_;
        const auto servers = servers_;
        for (auto& [_, client] : clients)
            client->cancel();
        for (auto& [_, server] : servers)
            server->cancel();
    });
    own_ctx_->shutdown();
}
}  // namespace dlslime::tcp
