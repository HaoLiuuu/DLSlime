#include "tcp_connection_pool.h"

#include <algorithm>

namespace dlslime::tcp {
std::shared_ptr<PooledConnection> TcpConnectionPool::getConnection(const std::string& host, uint16_t port)
{
    cleanupIdleConnections();
    for (auto& c : pool_) {
        if (!c->in_use && c->socket.is_open() && c->host == host && c->port == port) {
            c->in_use = true;
            return c;
        }
    }
    // Connecting is asynchronous and covered by ClientSession's deadline.
    auto c = std::make_shared<PooledConnection>(io_ctx_, host, port);
    pool_.push_back(c);
    return c;
}
void TcpConnectionPool::returnConnection(std::shared_ptr<PooledConnection> c)
{
    c->in_use    = false;
    c->last_used = std::chrono::steady_clock::now();
    if (!c->socket.is_open())
        pool_.erase(std::remove(pool_.begin(), pool_.end(), c), pool_.end());
}
void TcpConnectionPool::cleanupIdleConnections()
{
    const auto now = std::chrono::steady_clock::now();
    pool_.erase(std::remove_if(pool_.begin(),
                               pool_.end(),
                               [&](const auto& c) {
                                   if (c->in_use)
                                       return false;
                                   if (c->socket.is_open() && now - c->last_used <= kIdleTimeout)
                                       return false;
                                   asio::error_code ignored;
                                   c->socket.close(ignored);
                                   return true;
                               }),
                pool_.end());
}
void TcpConnectionPool::clear()
{
    for (auto& c : pool_) {
        asio::error_code ignored;
        c->socket.close(ignored);
    }
    pool_.clear();
}
}  // namespace dlslime::tcp
