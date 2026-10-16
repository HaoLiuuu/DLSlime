#pragma once

#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <stdexcept>
#include <vector>

namespace dlslime::region {
enum class Access : uint32_t {
    read       = 1,
    write      = 2,
    read_write = 3
};
enum class Error {
    stale,
    permission,
    bounds,
    closed,
    overlap,
    invalid
};
class RegionError: public std::runtime_error {
public:
    RegionError(Error code, const char* message): std::runtime_error(message), code(code) {}
    const Error code;
};
struct Key {
    uint64_t scope_hi{}, scope_lo{}, id{}, generation{};
    bool     operator==(const Key&) const = default;
};
struct Descriptor {
    Key      key;
    uint64_t length{};
    Access   access{Access::read_write};
};
// Fixed-size HOST allocation. It never resizes while registered or in flight.
class HostBuffer {
public:
    explicit HostBuffer(size_t size): size_(size), bytes_(new std::byte[size]{}) {}
    void* data() const
    {
        return bytes_.get();
    }
    size_t size() const
    {
        return size_;
    }

private:
    size_t                       size_;
    std::unique_ptr<std::byte[]> bytes_;
};
struct RegistryState;
struct Entry;
class Lease {
public:
    Lease() = default;
    Lease(Lease&&) noexcept;
    Lease& operator=(Lease&&) noexcept;
    Lease(const Lease&)            = delete;
    Lease& operator=(const Lease&) = delete;
    ~Lease();
    void  reset();
    void* data() const
    {
        return data_;
    }
    size_t size() const
    {
        return size_;
    }
    explicit operator bool() const
    {
        return bool(entry_);
    }

private:
    friend class HostRegionRegistry;
    Lease(std::shared_ptr<RegistryState>, std::shared_ptr<Entry>, void*, size_t);
    std::shared_ptr<RegistryState> state_;
    std::shared_ptr<Entry>         entry_;
    void*                          data_{};
    size_t                         size_{};
};
// Completion means every admitted lease retired and the registry released its
// allocation owner. Other application references may still own the allocation.
using Retirement = std::shared_future<void>;
class HostRegionRegistry {
public:
    HostRegionRegistry();
    ~HostRegionRegistry();
    HostRegionRegistry(const HostRegionRegistry&)            = delete;
    HostRegionRegistry& operator=(const HostRegionRegistry&) = delete;
    Descriptor          register_buffer(const std::shared_ptr<HostBuffer>&, Access = Access::read_write);
    // The caller guarantees a HOST range inside owner and must not resize/free
    // it independently of owner. Device pointers are not supported by this API.
    Descriptor
          register_owned_host(std::shared_ptr<void> owner, void* address, size_t length, Access = Access::read_write);
    Lease acquire(const Key&, uint64_t offset, uint64_t length, Access);
    Retirement              retire(const Key&);
    std::vector<Retirement> close();

private:
    std::shared_ptr<RegistryState> state_;
};
}  // namespace dlslime::region
