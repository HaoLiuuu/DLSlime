#pragma once
#include <atomic>
#include <cstdint>
#include <deque>
#include <stdexcept>
#include <unordered_set>
namespace dlslime::rdma_detail {
inline uint32_t completionMask(size_t count)
{
    if (!count || count > 32)
        throw std::invalid_argument("RDMA requires 1..32 QPs");
    return count == 32 ? UINT32_MAX : (uint32_t{1} << count) - 1;
}
// The producer owns the initial reference. It releases it after admitting its
// final slot, so even an immediate CQE cannot publish completion during dispatch.
class RetirementCounter {
public:
    void admit(size_t n)
    {
        remaining_.fetch_add(n, std::memory_order_relaxed);
    }
    bool retire()
    {
        return remaining_.fetch_sub(1, std::memory_order_acq_rel) == 1;
    }

private:
    std::atomic<size_t> remaining_{1};
};
// One bounded pass preserves order within each operation, but an unready
// producer cannot block another operation with ready data and available tokens.
template<class T, class Key, class Ready, class Post>
int progressReadyQueue(std::deque<T*>& queue, Key key, Ready ready, Post post)
{
    std::unordered_set<const void*> blocked;
    const size_t                    candidates = queue.size();
    int                             work       = 0;
    for (size_t i = 0; i < candidates; ++i) {
        T* slot = queue.front();
        queue.pop_front();
        const void* operation = key(slot);
        if (blocked.count(operation) || !ready(slot)) {
            blocked.insert(operation);
            queue.push_back(slot);
            continue;
        }
        ++work;
        post(slot);
    }
    return work;
}
}  // namespace dlslime::rdma_detail
