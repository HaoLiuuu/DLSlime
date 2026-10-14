#include "tcp_context.h"

namespace dlslime::tcp {
TcpContext::TcpContext(): io_thread_([this] { io_ctx_.run(); }) {}
TcpContext::~TcpContext()
{
    shutdown();
}
void TcpContext::shutdown()
{
    if (!io_thread_.joinable())
        return;
    asio::post(io_ctx_, [this] {
        conn_pool_.clear();
        work_.reset();
    });
    io_thread_.join();
}
}  // namespace dlslime::tcp
