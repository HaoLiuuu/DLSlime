#pragma once
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace dlslime::rdma_detail {
// Completion IDs never point into a reusable assignment slot. Each posted WR
// is signaled and owns one tracker entry until its CQE or successful QP destroy.
class CompletionTracker {
public:
    using Callback                  = std::function<void(int32_t, int32_t)>;
    static constexpr int32_t failed = 403;
    template<class WR, class Provider, class Destroy>
    int post(uint32_t              qp,
             WR*                   wr,
             size_t                count,
             Callback              callback,
             std::shared_ptr<void> owner,
             Provider              provider,
             Destroy               destroy)
    {
        std::lock_guard lock(mutex_);
        if (!count || count > UINT64_MAX - next_)
            throw std::invalid_argument("Invalid WR count or exhausted completion IDs");
        auto                  batch = std::make_shared<Batch>(count, std::move(callback), std::move(owner));
        std::vector<uint64_t> ids;
        ids.reserve(count);
        try {
            for (size_t i = 0; i < count; ++i) {
                const uint64_t id = next_++;
                tokens_.emplace(id, Token{qp, i, batch});
                ids.push_back(id);
                wr[i].wr_id = id;
            }
        }
        catch (...) {
            for (auto id : ids)
                tokens_.erase(id);
            throw;
        }
        WR* bad    = nullptr;
        int result = provider(wr, &bad);
        if (result) {
            batch->status = failed;
            // Only the prefix before bad_wr could have reached the device.
            // If a broken provider supplies no valid bad_wr, retain all tokens
            // until QP destruction proves that no DMA can still touch memory.
            size_t accepted = count;
            for (size_t i = 0; i < count; ++i)
                if (bad == &wr[i]) {
                    accepted = i;
                    break;
                }
            for (size_t i = accepted; i < count; ++i)
                complete(ids[i], failed, 0);
            quiesce(qp, destroy);
        }
        batch->sealed = true;
        batch->finish();
        return result;
    }
    // False means a late/duplicate/unknown CQE. It can never target a new slot.
    bool complete(uint64_t id, int32_t status, int32_t immediate = 0)
    {
        std::lock_guard lock(mutex_);
        auto            found = tokens_.find(id);
        if (found == tokens_.end())
            return false;
        auto token = std::move(found->second);
        tokens_.erase(found);
        token.batch->arrive(token.index, status, immediate);
        return true;
    }
    template<class Destroy>
    bool quiesce(uint32_t qp, Destroy destroy)
    {
        // Serialize against both posting and CQ callbacks. Once this returns,
        // no old callback for this QP can still reference an endpoint slot.
        std::lock_guard lock(mutex_);
        if (destroy() != 0)
            return false;
        std::vector<uint64_t> ids;
        for (const auto& [id, token] : tokens_)
            if (token.qp == qp)
                ids.push_back(id);
        for (auto id : ids)
            complete(id, failed, 0);
        return true;
    }
    size_t pending() const
    {
        std::lock_guard lock(mutex_);
        return tokens_.size();
    }

private:
    struct Batch {
        Batch(size_t n, Callback cb, std::shared_ptr<void> keep):
            count(n), remaining(n), callback(std::move(cb)), owner(std::move(keep))
        {
        }
        void arrive(size_t index, int32_t error, int32_t imm)
        {
            if (error && !status)
                status = error;
            if (index + 1 == count)
                immediate = imm;
            --remaining;
            finish();
        }
        void finish()
        {
            if (!sealed || remaining || !callback)
                return;
            auto cb   = std::move(callback);
            auto keep = std::move(owner);
            cb(status, immediate);
        }
        size_t                count, remaining;
        int32_t               status{}, immediate{};
        bool                  sealed{};
        Callback              callback;
        std::shared_ptr<void> owner;
    };
    struct Token {
        uint32_t               qp;
        size_t                 index;
        std::shared_ptr<Batch> batch;
    };
    // Recursive because a completion callback may synchronously retire a QP;
    // provider mocks also deliver CQEs before post() returns.
    mutable std::recursive_mutex mutex_;
    std::map<uint64_t, Token>    tokens_;
    uint64_t                     next_{1};
};
}  // namespace dlslime::rdma_detail
