#pragma once
#include <array>
#include <asio.hpp>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "tcp_connection_pool.h"
#include "tcp_header.h"
#include "tcp_memory_pool.h"
#include "tcp_op_state.h"

namespace dlslime::tcp {
struct RecvSlot {
    uintptr_t                               buffer{0};
    size_t                                  length{0};
    std::shared_ptr<TcpOpState>             op_state;
    std::function<asio::error_code(size_t)> post_read;
    bool                                    exact_size{false};
};
class ServerSession: public std::enable_shared_from_this<ServerSession> {
public:
    using RecvMatcher  = std::function<RecvSlot()>;
    using DoneCallback = std::function<void(ServerSession*)>;
    ServerSession(asio::ip::tcp::socket          socket,
                  std::shared_ptr<TcpMemoryPool> pool,
                  RecvMatcher                    matcher,
                  DoneCallback                   done);
    void start();
    void cancel();

private:
    void                           readHeader();
    void                           dispatch();
    void                           readBody(void* dst, size_t len);
    void                           writeBody(const void* src, size_t len);
    void                           receiveSend();
    void                           drainSend();
    void                           finishSend(asio::error_code ec);
    void                           finish();
    asio::ip::tcp::socket          socket_;
    std::shared_ptr<TcpMemoryPool> local_pool_;
    RecvMatcher                    recv_matcher_;
    DoneCallback                   on_done_;
    SessionHeader                  header_{};
    RecvSlot                       recv_;
    std::array<char, 16384>        discard_{};
    uint64_t                       remaining_{0};
    size_t                         copied_{0};
    bool                           rejected_{false};
    bool                           finished_{false};
    std::vector<char>              staging_;
};

struct TcpTransfer {
    SessionHeader header;
    uintptr_t     local;
};
class ClientSession: public std::enable_shared_from_this<ClientSession> {
public:
    using DoneCallback = std::function<void(asio::error_code)>;
    using Deadline     = std::chrono::steady_clock::time_point;
    ClientSession(asio::io_context& io, std::shared_ptr<PooledConnection> conn, DoneCallback done);
    // Empty transfers establish a connection. A batch uses one connection and
    // completes after all spans retire, including the last span beyond 32.
    void start(std::vector<TcpTransfer> transfers, Deadline deadline);
    void cancel(asio::error_code reason = asio::error::operation_aborted);

private:
    void                              connect();
    void                              next();
    void                              payloadDone(asio::error_code ec);
    void                              finish(asio::error_code ec);
    bool                              checked(asio::error_code ec);
    std::shared_ptr<PooledConnection> conn_;
    asio::ip::tcp::resolver           resolver_;
    asio::steady_timer                timer_;
    DoneCallback                      on_done_;
    std::vector<TcpTransfer>          transfers_;
    size_t                            index_{0};
    SessionHeader                     wire_header_{};
    std::vector<char>                 staging_;
    bool                              device_memory_{false};
    bool                              pending_{false};
    bool                              done_{false};
    asio::error_code                  forced_;
};
}  // namespace dlslime::tcp
