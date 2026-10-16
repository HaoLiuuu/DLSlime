#include "host_region.h"

#include <limits>
#include <mutex>
#include <random>
#include <utility>

namespace dlslime::region {
struct Entry {
    Descriptor            descriptor;
    uintptr_t             address{};
    std::shared_ptr<void> owner;
    size_t                leases{};
    bool                  retiring{}, releasing{};
    std::promise<void>    promise;
    Retirement            drained{promise.get_future().share()};
};
struct RegistryState {
    struct Slot {
        uint64_t               generation{};
        std::shared_ptr<Entry> entry;
    };
    std::mutex        mutex;
    uint64_t          scope_hi{}, scope_lo{};
    bool              closed{};
    std::vector<Slot> slots;
};
namespace {
bool valid_access(Access a)
{
    return a == Access::read || a == Access::write || a == Access::read_write;
}
std::shared_ptr<Entry> lookup(RegistryState& s, const Key& k)
{
    if (k.scope_hi != s.scope_hi || k.scope_lo != s.scope_lo || !k.id || k.id > s.slots.size())
        throw RegionError(Error::stale, "Unknown region scope or id");
    auto e = s.slots[k.id - 1].entry;
    if (!e || e->descriptor.key != k)
        throw RegionError(Error::stale, "Stale region generation");
    return e;
}
void release_if_drained(const std::shared_ptr<RegistryState>& s, const std::shared_ptr<Entry>& e)
{
    std::shared_ptr<void> owner;
    {
        std::lock_guard lock(s->mutex);
        if (!e->retiring || e->leases || e->releasing)
            return;
        e->releasing = true;
        owner        = std::move(e->owner);
    }
    // User deleters may reenter the registry. Never invoke one under its lock.
    owner.reset();
    {
        std::lock_guard lock(s->mutex);
        s->slots[e->descriptor.key.id - 1].entry.reset();
    }
    e->promise.set_value();
}
}  // namespace
Lease::Lease(std::shared_ptr<RegistryState> s, std::shared_ptr<Entry> e, void* p, size_t n):
    state_(std::move(s)), entry_(std::move(e)), data_(p), size_(n)
{
}
Lease::Lease(Lease&& other) noexcept
{
    *this = std::move(other);
}
Lease& Lease::operator=(Lease&& other) noexcept
{
    if (this != &other) {
        reset();
        state_ = std::move(other.state_);
        entry_ = std::move(other.entry_);
        data_  = std::exchange(other.data_, nullptr);
        size_  = std::exchange(other.size_, 0);
    }
    return *this;
}
Lease::~Lease()
{
    reset();
}
void Lease::reset()
{
    if (!entry_)
        return;
    auto e = std::move(entry_);
    auto s = std::move(state_);
    data_  = nullptr;
    size_  = 0;
    {
        std::lock_guard lock(s->mutex);
        --e->leases;
    }
    release_if_drained(s, e);
}
HostRegionRegistry::HostRegionRegistry(): state_(std::make_shared<RegistryState>())
{
    std::random_device rng;
    auto               next = [&] { return (uint64_t(rng()) << 32) ^ uint64_t(rng()); };
    do {
        state_->scope_hi = next();
        state_->scope_lo = next();
    } while (!(state_->scope_hi | state_->scope_lo));
}
HostRegionRegistry::~HostRegionRegistry()
{
    close();
}
Descriptor HostRegionRegistry::register_buffer(const std::shared_ptr<HostBuffer>& buffer, Access access)
{
    if (!buffer)
        throw RegionError(Error::invalid, "Null HOST buffer");
    return register_owned_host(buffer, buffer->data(), buffer->size(), access);
}
Descriptor
HostRegionRegistry::register_owned_host(std::shared_ptr<void> owner, void* address, size_t length, Access access)
{
    const auto start = reinterpret_cast<uintptr_t>(address);
    if (!owner || !start || !length || !valid_access(access) || length > std::numeric_limits<uintptr_t>::max() - start)
        throw RegionError(Error::invalid, "Invalid owned HOST range or access");
    auto e = std::make_shared<Entry>();
    // Keep owner in this outer scope so exceptions cannot destroy it under lock.
    e->owner          = std::move(owner);
    e->address        = start;
    auto&           s = *state_;
    std::lock_guard lock(s.mutex);
    if (s.closed)
        throw RegionError(Error::closed, "Registry is closed");
    size_t slot = s.slots.size();
    for (size_t i = 0; i < s.slots.size(); ++i) {
        const auto& old = s.slots[i];
        if (old.entry) {
            const auto& o = *old.entry;
            if (start < o.address + o.descriptor.length && o.address < start + length)
                throw RegionError(Error::overlap, "Overlapping live or retiring HOST range");
        }
        else if (slot == s.slots.size() && old.generation != UINT64_MAX)
            slot = i;
    }
    if (slot == s.slots.size())
        s.slots.emplace_back();
    auto& target  = s.slots[slot];
    e->descriptor = {{s.scope_hi, s.scope_lo, slot + 1, ++target.generation}, length, access};
    target.entry  = e;
    return e->descriptor;
}
Lease HostRegionRegistry::acquire(const Key& key, uint64_t offset, uint64_t length, Access access)
{
    auto&           s = *state_;
    std::lock_guard lock(s.mutex);
    if (s.closed)
        throw RegionError(Error::closed, "Registry is closed");
    auto e = lookup(s, key);
    if (e->retiring)
        throw RegionError(Error::stale, "Region is retiring");
    if (!valid_access(access) || (uint32_t(e->descriptor.access) & uint32_t(access)) != uint32_t(access))
        throw RegionError(Error::permission, "Region access denied");
    if (offset > e->descriptor.length || length > e->descriptor.length - offset)
        throw RegionError(Error::bounds, "Region range out of bounds");
    ++e->leases;
    return Lease(state_, e, reinterpret_cast<void*>(e->address + offset), static_cast<size_t>(length));
}
size_t HostRegionRegistry::active_leases(const Key& key) const
{
    std::lock_guard lock(state_->mutex);
    return lookup(*state_, key)->leases;
}
Retirement HostRegionRegistry::retire(const Key& key)
{
    std::shared_ptr<Entry> e;
    {
        std::lock_guard lock(state_->mutex);
        e           = lookup(*state_, key);
        e->retiring = true;
    }
    release_if_drained(state_, e);
    return e->drained;
}
std::vector<Retirement> HostRegionRegistry::close()
{
    std::vector<std::shared_ptr<Entry>> entries;
    std::vector<Retirement>             results;
    {
        std::lock_guard lock(state_->mutex);
        state_->closed = true;
        for (auto& slot : state_->slots)
            if (slot.entry) {
                slot.entry->retiring = true;
                entries.push_back(slot.entry);
                results.push_back(slot.entry->drained);
            }
    }
    for (const auto& e : entries)
        release_if_drained(state_, e);
    return results;
}
}  // namespace dlslime::region
