#pragma once
#include <algorithm>
#include <cstring>
#include <deque>
#include <initializer_list>
#include <map>
#include <numeric>
#include <set>
#include <string>

#include "dlslime/csrc/device/runtime/runtime_internal.h"

#ifndef DLSLIME_RUNTIME_TEST_ACCESS
#error "The fake driver is only for the standalone CPU test executable"
#endif
namespace dlslime::device::detail {
struct RuntimeTestAccess {
    static DeviceRuntime create(std::shared_ptr<Driver> driver)
    {
        return DeviceRuntime(std::move(driver));
    }
};
}  // namespace dlslime::device::detail
namespace dlslime::device::test {
using detail::NativeHandle;
enum class Fault {
    None,
    Allocate,
    CreateStream,
    CreateEvent,
    CopyBefore,
    CopyAfter,
    Record,
    RecordAfter,
    StreamWaitBefore,
    StreamWaitAfter,
    Wait,
    Synchronize
};
class InjectedFailure: public std::runtime_error {
public:
    InjectedFailure(): std::runtime_error("injected CPU driver failure") {}
};
// No worker runs automatically: readiness stays false until wait/synchronize
// explicitly advances queued work. Fake DEVICE/PINNED storage is ordinary CPU
// memory, never a public backend or evidence of real accelerator capability.
class FakeDriver final: public detail::Driver {
    struct StreamData {
        std::deque<std::function<void()>> operations;
    };
    struct EventData {
        bool                      recorded = false, done = false;
        size_t                    pending_waiters = 0;
        std::weak_ptr<StreamData> stream;
    };
    mutable std::recursive_mutex                        mutex_;
    std::map<uintptr_t, size_t>                         allocations_;
    std::map<NativeHandle, std::shared_ptr<StreamData>> streams_;
    std::map<NativeHandle, std::shared_ptr<EventData>>  events_;
    std::map<uint64_t, std::pair<uintptr_t, uintptr_t>> pending_;
    std::set<const StreamData*>                         advancing_;
    uint64_t                                            next_ = 1, completed_ = 0, checksum_ = 0;
    std::deque<Fault>                                   faults_;
    uint64_t                                            synchronize_attempts_ = 0;
    void                                                fail(Fault point)
    {
        if (!faults_.empty() && faults_.front() == point) {
            faults_.pop_front();
            throw InjectedFailure();
        }
    }
    void advance(const std::shared_ptr<StreamData>& stream, const std::shared_ptr<EventData>& until = {})
    {
        if (!advancing_.insert(stream.get()).second)
            throw std::runtime_error("cyclic fake stream dependency");
        try {
            while (!stream->operations.empty() && (!until || !until->done)) {
                auto operation = stream->operations.front();
                operation();
                stream->operations.pop_front();
            }
        }
        catch (...) {
            advancing_.erase(stream.get());
            throw;
        }
        advancing_.erase(stream.get());
    }
    void complete(const std::shared_ptr<EventData>& event)
    {
        if (event->done)
            return;
        auto stream = event->stream.lock();
        if (!event->recorded || !stream)
            throw std::runtime_error("unrecorded or orphaned fake event");
        advance(stream, event);
        if (!event->done)
            throw std::runtime_error("event marker was not reached");
    }

public:
    void failNext(Fault fault)
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        faults_ = {fault};
    }
    void failSequence(std::initializer_list<Fault> faults)
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        faults_.assign(faults);
    }
    uint64_t synchronizeAttempts() const
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return synchronize_attempts_;
    }
    size_t allocationCount() const
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return allocations_.size();
    }
    size_t eventCount() const
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return events_.size();
    }
    size_t streamCount() const
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return streams_.size();
    }
    size_t pendingCount() const
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return pending_.size();
    }
    uint64_t completedCopies() const
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return completed_;
    }
    uint64_t lastChecksum() const
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return checksum_;
    }
    DeviceInfo info() const override
    {
        return {Backend::Host, 0, 1, -1, 0, "CPU fault injection (tests only)", ""};
    }
    Capabilities capabilities() const override
    {
        return {true, true, true, true, false, false, HbmRegistration::Forbidden};
    }
    void* allocate(size_t bytes, MemoryKind) override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        fail(Fault::Allocate);
        auto p = std::make_unique<unsigned char[]>(bytes);
        allocations_.emplace(reinterpret_cast<uintptr_t>(p.get()), bytes);
        return p.release();
    }
    void release(void* pointer, MemoryKind) override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        auto                                  base = reinterpret_cast<uintptr_t>(pointer);
        auto                                  size = allocations_.at(base);
        for (const auto& [id, pair] : pending_)
            for (auto p : {pair.first, pair.second})
                if (p >= base && p - base < size)
                    throw std::runtime_error("allocation freed before queued copy retired");
        allocations_.erase(base);
        delete[] static_cast<unsigned char*>(pointer);
    }
    PointerInfo query(const void*) const override
    {
        return {};
    }
    NativeHandle createStream() override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        fail(Fault::CreateStream);
        auto id = next_++;
        streams_.emplace(id, std::make_shared<StreamData>());
        return id;
    }
    void destroyStream(NativeHandle id) override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (!streams_.at(id)->operations.empty())
            throw std::runtime_error("stream destroyed before drain");
        streams_.erase(id);
    }
    void synchronize(NativeHandle id) override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        ++synchronize_attempts_;
        fail(Fault::Synchronize);
        advance(streams_.at(id));
    }
    NativeHandle createEvent() override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        fail(Fault::CreateEvent);
        auto id = next_++;
        events_.emplace(id, std::make_shared<EventData>());
        return id;
    }
    void destroyEvent(NativeHandle id) override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        auto                                  event = events_.at(id);
        if (event->pending_waiters)
            throw std::runtime_error("event destroyed before consumer wait retired");
        if (event->recorded && !event->done)
            throw std::runtime_error("event destroyed before completion");
        events_.erase(id);
    }
    void record(NativeHandle eventId, NativeHandle streamId) override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        fail(Fault::Record);
        auto event  = events_.at(eventId);
        auto stream = streams_.at(streamId);
        if (event->recorded)
            throw std::runtime_error("event re-recorded");
        stream->operations.push_back([event] { event->done = true; });
        event->stream   = stream;
        event->recorded = true;
        fail(Fault::RecordAfter);
    }
    bool ready(NativeHandle id) override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        return events_.at(id)->done;
    }
    void wait(NativeHandle id) override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        fail(Fault::Wait);
        complete(events_.at(id));
    }
    void streamWait(NativeHandle streamId, NativeHandle eventId) override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        fail(Fault::StreamWaitBefore);
        auto event = events_.at(eventId);
        streams_.at(streamId)->operations.push_back([this, event] {
            complete(event);
            --event->pending_waiters;
        });
        ++event->pending_waiters;
        fail(Fault::StreamWaitAfter);
    }
    void copy(void* dst, MemoryKind, const void* src, MemoryKind, size_t bytes, NativeHandle id) override
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        fail(Fault::CopyBefore);
        auto sequence = next_++;
        pending_.emplace(sequence, std::make_pair(reinterpret_cast<uintptr_t>(dst), reinterpret_cast<uintptr_t>(src)));
        try {
            streams_.at(id)->operations.push_back([this, dst, src, bytes, sequence] {
                std::memmove(dst, src, bytes);
                auto* data = static_cast<const unsigned char*>(dst);
                checksum_  = std::accumulate(data, data + bytes, uint64_t{0});
                ++completed_;
                pending_.erase(sequence);
            });
        }
        catch (...) {
            pending_.erase(sequence);
            throw;
        }
        fail(Fault::CopyAfter);
    }
};
}  // namespace dlslime::device::test
