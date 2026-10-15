#include "tcp_session.h"

#include <endian.h>

#include <algorithm>
#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

namespace dlslime::tcp {
namespace {
bool is_device(const void* ptr)
{
#ifdef USE_CUDA
    cudaPointerAttributes attr{};
    return cudaPointerGetAttributes(&attr, ptr) == cudaSuccess && attr.type == cudaMemoryTypeDevice;
#else
    (void)ptr;
    return false;
#endif
}
bool copy_to_host(void* dst, const void* src, size_t size)
{
#ifdef USE_CUDA
    return cudaMemcpy(dst, src, size, cudaMemcpyDeviceToHost) == cudaSuccess;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}
bool copy_to_device(void* dst, const void* src, size_t size)
{
#ifdef USE_CUDA
    return cudaMemcpy(dst, src, size, cudaMemcpyHostToDevice) == cudaSuccess;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}
}  // namespace

ServerSession::ServerSession(asio::ip::tcp::socket          socket,
                             std::shared_ptr<TcpMemoryPool> pool,
                             RecvMatcher                    matcher,
                             DoneCallback                   done):
    socket_(std::move(socket)),
    local_pool_(std::move(pool)),
    recv_matcher_(std::move(matcher)),
    on_done_(std::move(done))
{
}
void ServerSession::start()
{
    readHeader();
}
void ServerSession::cancel()
{
    asio::error_code ignored;
    socket_.close(ignored);
    // The outstanding callback retires any active receive buffer before completion.
}
void ServerSession::finish()
{
    if (finished_)
        return;
    finished_ = true;
    cancel();
    if (recv_.op_state)
        recv_.op_state->complete(TCP_FAILED);
    recv_ = {};
    if (on_done_)
        on_done_(this);
}
void ServerSession::readHeader()
{
    auto self = shared_from_this();
    asio::async_read(socket_, asio::buffer(&header_, sizeof(header_)), [self](asio::error_code ec, size_t) {
        if (ec) {
            self->finish();
            return;
        }
        self->header_.size = le64toh(self->header_.size);
        self->header_.addr = le64toh(self->header_.addr);
        try {
            self->dispatch();
        }
        catch (...) {
            self->finish();
        }
    });
}
void ServerSession::dispatch()
{
    if (header_.opcode == OP_SEND) {
        receiveSend();
        return;
    }
    if ((header_.opcode != OP_READ && header_.opcode != OP_WRITE)
        || !local_pool_->contains(header_.addr, header_.size)) {
        finish();  // Unknown framing or invalid range must never become another header.
        return;
    }
    if (header_.opcode == OP_READ)
        writeBody(reinterpret_cast<const void*>(header_.addr), header_.size);
    else
        readBody(reinterpret_cast<void*>(header_.addr), header_.size);
}
void ServerSession::receiveSend()
{
    recv_      = recv_matcher_();
    rejected_  = !recv_.op_state || (recv_.exact_size && header_.size != recv_.length);
    copied_    = rejected_ ? 0 : std::min<uint64_t>(header_.size, recv_.length);
    remaining_ = header_.size - copied_;
    if (!copied_) {
        drainSend();
        return;
    }
    auto self = shared_from_this();
    asio::async_read(
        socket_, asio::buffer(reinterpret_cast<void*>(recv_.buffer), copied_), [self](asio::error_code ec, size_t) {
            if (ec)
                self->finishSend(ec);
            else
                self->drainSend();
        });
}
void ServerSession::drainSend()
{
    if (!remaining_) {
        finishSend({});
        return;
    }
    auto       self  = shared_from_this();
    const auto count = std::min<uint64_t>(remaining_, discard_.size());
    asio::async_read(socket_, asio::buffer(discard_.data(), count), [self](asio::error_code ec, size_t n) {
        if (ec) {
            self->finishSend(ec);
            return;
        }
        self->remaining_ -= n;
        self->drainSend();
    });
}
void ServerSession::finishSend(asio::error_code ec)
{
    if (!ec && !rejected_ && recv_.post_read)
        ec = recv_.post_read(copied_);
    if (recv_.op_state) {
        recv_.op_state->bytes_copied = ec || rejected_ ? 0 : copied_;
        recv_.op_state->complete(ec || rejected_ ? TCP_FAILED : TCP_SUCCESS);
    }
    recv_ = {};
    if (ec)
        finish();
    else
        readHeader();
}
void ServerSession::readBody(void* dst, size_t len)
{
    const bool device = is_device(dst);
    void*      target = dst;
    if (device) {
        staging_.resize(len);
        target = staging_.data();
    }
    auto self = shared_from_this();
    asio::async_read(socket_, asio::buffer(target, len), [self, dst, len, device](asio::error_code ec, size_t) {
        if (!ec && device && !copy_to_device(dst, self->staging_.data(), len))
            ec = asio::error::fault;
        if (ec)
            self->finish();
        else
            self->readHeader();
    });
}
void ServerSession::writeBody(const void* src, size_t len)
{
    const void* source = src;
    if (is_device(src)) {
        staging_.resize(len);
        if (!copy_to_host(staging_.data(), src, len)) {
            finish();
            return;
        }
        source = staging_.data();
    }
    auto self = shared_from_this();
    asio::async_write(socket_, asio::buffer(source, len), [self](asio::error_code ec, size_t) {
        if (ec)
            self->finish();
        else
            self->readHeader();
    });
}

ClientSession::ClientSession(asio::io_context&                 io,
                             std::shared_ptr<PooledConnection> conn,
                             DoneCallback                      done,
                             ReadPipelineLimits                limits):
    conn_(std::move(conn)), resolver_(io), timer_(io), on_done_(std::move(done)), read_limits_(limits)
{
}
void ClientSession::start(std::vector<TcpTransfer> transfers, Deadline deadline)
{
    transfers_ = std::move(transfers);
    pipeline_read_ =
        read_limits_.max_requests > 1 && transfers_.size() > 1
        && std::all_of(transfers_.begin(), transfers_.end(), [](const TcpTransfer& transfer) {
               return transfer.header.opcode == OP_READ && !is_device(reinterpret_cast<void*>(transfer.local));
           });
    if (deadline <= std::chrono::steady_clock::now()) {
        finish(asio::error::timed_out);
        return;
    }
    timer_.expires_at(deadline);
    auto self = shared_from_this();
    timer_.async_wait([self](asio::error_code ec) {
        if (!ec && !self->done_)
            self->cancel(asio::error::timed_out);
    });
    if (conn_->socket.is_open())
        next();
    else
        connect();
}
void ClientSession::connect()
{
    auto self = shared_from_this();
    ++pending_;
    try {
        resolver_.async_resolve(
            conn_->host,
            std::to_string(conn_->port),
            [self](asio::error_code ec, asio::ip::tcp::resolver::results_type addresses) {
                if (!self->checked(ec))
                    return;
                ++self->pending_;
                try {
                    asio::async_connect(
                        self->conn_->socket, addresses, [self](asio::error_code error, const asio::ip::tcp::endpoint&) {
                            if (!self->checked(error))
                                return;
                            self->conn_->socket.set_option(asio::ip::tcp::no_delay(true), error);
                            if (error)
                                self->finish(error);
                            else
                                self->next();
                        });
                }
                catch (...) {
                    --self->pending_;
                    self->cancel(asio::error::no_memory);
                }
            });
    }
    catch (...) {
        --pending_;
        cancel(asio::error::no_memory);
    }
}
bool ClientSession::checked(asio::error_code ec)
{
    --pending_;
    if (forced_ || ec) {
        cancel(forced_ ? forced_ : ec);
        return false;
    }
    return true;
}
void ClientSession::next()
{
    if (pipeline_read_) {
        pumpRead();
        return;
    }
    if (index_ == transfers_.size()) {
        finish({});
        return;
    }
    const auto& transfer      = transfers_[index_];
    const auto  len           = transfer.header.size;
    void*       ptr           = reinterpret_cast<void*>(transfer.local);
    device_memory_            = is_device(ptr);
    const auto pending_before = pending_;
    try {
        if (device_memory_) {
            staging_.resize(len);
            if (transfer.header.opcode != OP_READ && !copy_to_host(staging_.data(), ptr, len)) {
                finish(asio::error::fault);
                return;
            }
            ptr = staging_.data();
        }
        wire_header_      = transfer.header;
        wire_header_.size = htole64(wire_header_.size);
        wire_header_.addr = htole64(wire_header_.addr);
        auto self         = shared_from_this();
        ++pending_;
        if (transfer.header.opcode == OP_READ) {
            asio::async_write(conn_->socket,
                              asio::buffer(&wire_header_, sizeof(wire_header_)),
                              [self, ptr, len](asio::error_code ec, size_t) {
                                  if (!self->checked(ec))
                                      return;
                                  ++self->pending_;
                                  try {
                                      asio::async_read(
                                          self->conn_->socket,
                                          asio::buffer(ptr, len),
                                          [self](asio::error_code error, size_t) { self->payloadDone(error); });
                                  }
                                  catch (...) {
                                      --self->pending_;
                                      self->cancel(asio::error::no_memory);
                                  }
                              });
        }
        else {
            std::array<asio::const_buffer, 2> buffers{asio::buffer(&wire_header_, sizeof(wire_header_)),
                                                      asio::buffer(ptr, len)};
            asio::async_write(conn_->socket, buffers, [self](asio::error_code ec, size_t) { self->payloadDone(ec); });
        }
    }
    catch (...) {
        pending_ = pending_before;
        cancel(asio::error::no_memory);
    }
}
void ClientSession::pumpRead()
{
    if (done_ || forced_)
        return;
    if (received_ == transfers_.size()) {
        if (!pending_)
            finish({});
        return;
    }
    // Each direction has exactly one composed operation at a time. Issuing
    // headers and receiving bodies progress independently on the same socket.
    if (!headers_pending_)
        issueReadHeaders();
    if (!done_ && !forced_ && !body_pending_ && received_ < requested_)
        receiveReadBody();
}
void ClientSession::issueReadHeaders()
{
    try {
        read_headers_.clear();
        while (requested_ < transfers_.size() && requested_ - received_ < read_limits_.max_requests) {
            const auto& transfer = transfers_[requested_];
            const auto  bytes    = transfer.header.size;
            if (requested_ != received_
                && (read_bytes_inflight_ > read_limits_.max_bytes
                    || bytes > read_limits_.max_bytes - read_bytes_inflight_))
                break;
            auto wire = transfer.header;
            wire.size = htole64(wire.size);
            wire.addr = htole64(wire.addr);
            read_headers_.push_back(wire);
            ++requested_;
            read_bytes_inflight_ += bytes;
        }
    }
    catch (...) {
        cancel(asio::error::no_memory);
        return;
    }
    if (read_headers_.empty())
        return;
    auto self        = shared_from_this();
    headers_pending_ = true;
    ++pending_;
    try {
        // The vector is session-owned and never changed before this callback.
        asio::async_write(conn_->socket, asio::buffer(read_headers_), [self](asio::error_code ec, size_t) {
            self->headers_pending_ = false;
            if (self->checked(ec))
                self->pumpRead();
        });
    }
    catch (...) {
        headers_pending_ = false;
        --pending_;
        cancel(asio::error::no_memory);
    }
}
void ClientSession::receiveReadBody()
{
    const auto& transfer = transfers_[received_];
    auto        self     = shared_from_this();
    body_pending_        = true;
    ++pending_;
    try {
        // Legacy responses are ordered byte streams. Retiring one entire body
        // before selecting the next buffer also preserves overlapping spans.
        asio::async_read(conn_->socket,
                         asio::buffer(reinterpret_cast<void*>(transfer.local), transfer.header.size),
                         [self](asio::error_code ec, size_t) {
                             self->body_pending_ = false;
                             if (!self->checked(ec))
                                 return;
                             self->read_bytes_inflight_ -= self->transfers_[self->received_].header.size;
                             ++self->received_;
                             self->pumpRead();
                         });
    }
    catch (...) {
        body_pending_ = false;
        --pending_;
        cancel(asio::error::no_memory);
    }
}
void ClientSession::payloadDone(asio::error_code ec)
{
    if (!checked(ec))
        return;
    const auto& transfer = transfers_[index_];
    if (device_memory_ && transfer.header.opcode == OP_READ
        && !copy_to_device(reinterpret_cast<void*>(transfer.local), staging_.data(), transfer.header.size)) {
        finish(asio::error::fault);
        return;
    }
    ++index_;
    next();
}
void ClientSession::cancel(asio::error_code reason)
{
    if (done_)
        return;
    if (!forced_)
        forced_ = reason;
    resolver_.cancel();
    asio::error_code ignored;
    conn_->socket.close(ignored);
    if (!pending_)
        finish(forced_);
}
void ClientSession::finish(asio::error_code ec)
{
    if (done_ || pending_)
        return;
    done_ = true;
    asio::error_code ignored;
    timer_.cancel();
    if (ec)
        conn_->socket.close(ignored);
    if (on_done_)
        on_done_(ec);
}
}  // namespace dlslime::tcp
