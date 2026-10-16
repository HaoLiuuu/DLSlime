#pragma once
#include <asio.hpp>
#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace dlslime::tcp {
struct PooledConnection {
    asio::ip::tcp::socket                 socket;
    std::string                           host;
    uint16_t                              port;
    std::chrono::steady_clock::time_point last_used{std::chrono::steady_clock::now()};
    bool                                  in_use{true};
    PooledConnection(asio::io_context& io, std::string h, uint16_t p): socket(io), host(std::move(h)), port(p) {}
};

// All methods run on the owning io_context thread. A checked-out connection
// remains in the pool and its socket never moves into a per-request session.
class TcpConnectionPool {
public:
    static constexpr std::chrono::seconds kIdleTimeout{300};
    explicit TcpConnectionPool(asio::io_context& io): io_ctx_(io) {}
    std::shared_ptr<PooledConnection> getConnection(const std::string& host, uint16_t port);
    void                              returnConnection(std::shared_ptr<PooledConnection> conn);
    void                              cleanupIdleConnections();
    void                              trimIdleConnections(size_t max_idle);
    void                              clear();

private:
    asio::io_context&                              io_ctx_;
    std::vector<std::shared_ptr<PooledConnection>> pool_;
};
}  // namespace dlslime::tcp
