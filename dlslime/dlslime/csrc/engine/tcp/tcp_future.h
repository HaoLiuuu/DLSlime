#pragma once
#include <memory>
#include <stdexcept>

#include "dlslime/csrc/device/device_future.h"
#include "tcp_op_state.h"

namespace dlslime::tcp {
class TcpFuture: public DeviceFuture {
public:
    explicit TcpFuture(std::shared_ptr<TcpOpState> op): op_state_(std::move(op))
    {
        if (!op_state_)
            throw std::runtime_error("TcpFuture: null op_state");
    }
    int32_t wait() const override
    {
        return op_state_->wait();
    }
    // A wait timeout does not cancel I/O or release the caller's buffers.
    bool wait_for(int64_t timeout_ms, int32_t* out) const
    {
        return op_state_->wait_for(timeout_ms, out);
    }

protected:
    std::shared_ptr<TcpOpState> op_state_;
};
class TcpSendFuture: public TcpFuture {
public:
    using TcpFuture::TcpFuture;
};
class TcpRecvFuture: public TcpFuture {
public:
    using TcpFuture::TcpFuture;
};
class TcpReadWriteFuture: public TcpFuture {
public:
    using TcpFuture::TcpFuture;
};
}  // namespace dlslime::tcp
