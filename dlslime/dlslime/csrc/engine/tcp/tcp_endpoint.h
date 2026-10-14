#pragma once
#include <asio.hpp>
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include "dlslime/csrc/engine/assignment.h"
#include "tcp_context.h"
#include "tcp_future.h"
#include "tcp_session.h"

namespace dlslime {
struct not_implemented: public std::logic_error {
    using std::logic_error::logic_error;
};
namespace tcp {
class TcpEndpoint: public std::enable_shared_from_this<TcpEndpoint> {
public:
    static constexpr int64_t kDefaultTimeoutMs = 30000;
    explicit TcpEndpoint(const std::string& ip = "0.0.0.0", uint16_t port = 0);
    TcpEndpoint(TcpContext&, const std::string& = "0.0.0.0", uint16_t = 0) = delete;
    ~TcpEndpoint();
    TcpEndpoint(const TcpEndpoint&)            = delete;
    TcpEndpoint& operator=(const TcpEndpoint&) = delete;
    json         endpoint_info() const;
    void         connect(const json& remote_endpoint_info);
    void         shutdown();
    int32_t      register_memory_region(const std::string& name, uintptr_t ptr, uintptr_t offset, size_t length);
    int32_t      register_remote_memory_region(const std::string& name, const json& mr_info);
    json         mr_info() const;
    std::shared_ptr<TcpSendFuture>      send(const chunk_tuple_t& chunk, int64_t timeout_ms = kDefaultTimeoutMs);
    std::shared_ptr<TcpRecvFuture>      recv(const chunk_tuple_t& chunk, bool exact_size = false);
    std::shared_ptr<TcpReadWriteFuture> read(const std::vector<assign_tuple_t>& assign,
                                             int64_t                            timeout_ms = kDefaultTimeoutMs);
    // Legacy completion means local socket I/O retired, not remote visibility.
    std::shared_ptr<TcpReadWriteFuture> write(const std::vector<assign_tuple_t>& assign,
                                              int64_t                            timeout_ms = kDefaultTimeoutMs);
    [[noreturn]] void                   write_with_imm(const std::vector<assign_tuple_t>&, uint32_t = 0)
    {
        throw not_implemented("TCP transport does not support write_with_imm; this is RDMA-only.");
    }
    [[noreturn]] void imm_recv()
    {
        throw not_implemented("TCP transport does not support imm_recv; this is RDMA-only.");
    }
    void setId(int64_t id)
    {
        id_.store(id, std::memory_order_relaxed);
    }
    int64_t getId() const
    {
        return id_.load(std::memory_order_relaxed);
    }
    bool is_connected() const
    {
        return connected_.load(std::memory_order_acquire);
    }

private:
    void do_accept();
    void submit(std::vector<TcpTransfer>           transfers,
                const std::shared_ptr<TcpOpState>& op,
                int64_t                            timeout_ms,
                const std::string&                 host,
                uint16_t                           port);
    std::shared_ptr<TcpReadWriteFuture>
                                transfer(const std::vector<assign_tuple_t>& assign, int64_t timeout_ms, uint8_t opcode);
    std::atomic<int64_t>        id_{-1};
    std::string                 local_host_;
    uint16_t                    local_port_{0};
    std::string                 peer_host_;
    uint16_t                    peer_port_{0};
    std::atomic<bool>           connected_{false};
    std::atomic<bool>           running_{true};
    mutable std::mutex          api_mu_;
    std::unique_ptr<TcpContext> own_ctx_;
    asio::ip::tcp::acceptor     acceptor_;
    std::shared_ptr<TcpMemoryPool> local_pool_{std::make_shared<TcpMemoryPool>()};
    std::shared_ptr<TcpMemoryPool> remote_pool_{std::make_shared<TcpMemoryPool>()};
    // These containers and every socket are owned by the I/O thread.
    std::deque<RecvSlot>                                                  pending_recvs_;
    std::unordered_map<ServerSession*, std::shared_ptr<ServerSession>>    servers_;
    std::unordered_map<PooledConnection*, std::shared_ptr<ClientSession>> clients_;
};
}  // namespace tcp
}  // namespace dlslime
