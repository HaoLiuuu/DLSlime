#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>

namespace dlslime::tcp {
enum Status : int32_t {
    TCP_SUCCESS = 0,
    TCP_FAILED  = -1,
    TCP_TIMEOUT = -2,
    TCP_CLOSED  = -3
};

// A TCP future represents the entire operation, independently of its span count.
// Completion is published only after every asynchronous user-buffer reference retires.
struct TcpOpState {
    std::atomic<int32_t> completion_status{TCP_SUCCESS};
    uintptr_t            user_buffer{0};
    size_t               user_length{0};
    size_t               bytes_copied{0};

    static std::shared_ptr<TcpOpState> create()
    {
        return std::make_shared<TcpOpState>();
    }
    void complete(int32_t status)
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (done_)
            return;
        completion_status.store(status, std::memory_order_release);
        done_ = true;
        cv_.notify_all();
    }
    int32_t wait() const
    {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return done_; });
        return completion_status.load(std::memory_order_acquire);
    }
    bool wait_for(int64_t timeout_ms, int32_t* out) const
    {
        std::unique_lock<std::mutex> lock(mu_);
        const auto                   now = std::chrono::steady_clock::now();
        const auto                   remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::time_point::max() - now)
                .count();
        const auto deadline = timeout_ms <= 0         ? now :
                              timeout_ms >= remaining ? std::chrono::steady_clock::time_point::max() :
                                                        now + std::chrono::milliseconds(timeout_ms);
        if (!cv_.wait_until(lock, deadline, [this] { return done_; }))
            return false;
        if (out)
            *out = completion_status.load(std::memory_order_acquire);
        return true;
    }

private:
    mutable std::mutex              mu_;
    mutable std::condition_variable cv_;
    bool                            done_{false};
};
}  // namespace dlslime::tcp
