#pragma once
#include <asio.hpp>
#include <thread>
#include <utility>

#include "tcp_connection_pool.h"

namespace dlslime::tcp {
class TcpContext {
public:
    TcpContext();
    ~TcpContext();
    TcpContext(const TcpContext&)                  = delete;
    TcpContext&       operator=(const TcpContext&) = delete;
    asio::io_context& io_context()
    {
        return io_ctx_;
    }
    TcpConnectionPool& conn_pool()
    {
        return conn_pool_;
    }
    // Caller first cancels every session; shutdown then drains cancellation handlers.
    void shutdown();

private:
    asio::io_context                                           io_ctx_;
    asio::executor_work_guard<asio::io_context::executor_type> work_{asio::make_work_guard(io_ctx_)};
    TcpConnectionPool                                          conn_pool_{io_ctx_};
    std::thread                                                io_thread_;
};
}  // namespace dlslime::tcp
