#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace dlslime::device {
enum class Backend {
    Host,
    Cuda,
    Maca,
    Ascend
};
enum class MemoryKind {
    Unknown,
    HostPageable,
    HostPinned,
    Device,
    Managed
};
// Runtime capability is not proof that a NIC/provider can register device memory.
enum class HbmRegistration {
    NotApplicable,
    RequiresQualification,
    Forbidden
};
struct Capabilities {
    bool            device_allocation = false, pinned_host = false, asynchronous_copy = false;
    bool            stream_events = false, external_pointer_query = false, dma_buf_export = false;
    HbmRegistration hbm_registration = HbmRegistration::NotApplicable;
};
struct DeviceInfo {
    Backend     backend = Backend::Host;
    int         ordinal = -1, count = 0, numa_node = -1, driver_version = 0;
    std::string name, pci_bus_id;
};
struct PointerInfo {
    MemoryKind kind    = MemoryKind::Unknown;
    Backend    backend = Backend::Host;
    int        device  = -1;
    uintptr_t  base    = 0;
    size_t     bytes = 0, offset = 0;
    uint64_t   allocation_id = 0;
    bool       owned = false, range_known = false;
};
namespace detail {
struct DmaBufState;
struct AllocationState;
struct StreamState;
struct EventState;
class Driver;
}  // namespace detail
class BufferView;
class Allocation {
public:
    Allocation() = default;
    void*       data() const;
    size_t      size() const;
    PointerInfo info() const;
    BufferView  view(size_t offset, size_t bytes) const;
    explicit    operator bool() const
    {
        return bool(state_);
    }

private:
    explicit Allocation(std::shared_ptr<detail::AllocationState> state): state_(std::move(state)) {}
    std::shared_ptr<detail::AllocationState> state_;
    friend class DeviceRuntime;
    friend class BufferView;
};
class BufferView {
public:
    void*  data() const;
    size_t size() const
    {
        return bytes_;
    }

private:
    BufferView(std::shared_ptr<detail::AllocationState> a, size_t o, size_t n):
        allocation_(std::move(a)), offset_(o), bytes_(n)
    {
    }
    std::shared_ptr<detail::AllocationState> allocation_;
    size_t                                   offset_, bytes_;
    friend class Allocation;
    friend class DeviceRuntime;
};
class Event {
public:
    Event() = default;
    bool     ready() const;
    void     wait() const;
    explicit operator bool() const
    {
        return bool(state_);
    }

private:
    explicit Event(std::shared_ptr<detail::EventState> state): state_(std::move(state)) {}
    std::shared_ptr<detail::EventState> state_;
    friend class DeviceRuntime;
    friend class Stream;
};
class Stream {
public:
    Stream() = default;
    void wait(const Event& producer) const;
    void synchronize() const;

private:
    explicit Stream(std::shared_ptr<detail::StreamState> state): state_(std::move(state)) {}
    std::shared_ptr<detail::StreamState> state_;
    friend class DeviceRuntime;
};
class DmaBuf {
  public:
    DmaBuf() = default;
    int fd() const; // Borrowed descriptor. Keep this owner alive; do not close it.
    size_t offset() const;
    size_t size() const;
    size_t exportedSize() const;
    size_t granularity() const;
  private:
    explicit DmaBuf(std::shared_ptr<detail::DmaBufState> state) : state_(std::move(state)) {}
    std::shared_ptr<detail::DmaBufState> state_;
    friend class DeviceRuntime;
};
class DeviceRuntime {
public:
    DmaBuf exportDmaBuf(const BufferView& view) const;
    static DeviceRuntime create(Backend backend, int device = 0);
    DeviceInfo           deviceInfo() const;
    Capabilities         capabilities() const;
    Allocation           allocate(size_t bytes, MemoryKind kind) const;
    PointerInfo          query(const void* pointer) const;
    Stream               createStream() const;
    Event                record(const Stream& stream) const;
    // Retains both allocations until native completion, even if callers release
    // their handles. Dropping the last incomplete Event waits; it does not cancel.
    // Device transfers require pinned host memory. Pageable copies are explicit
    // HOST-only operations, never advertised as asynchronous GPU DMA.
    Event copy(const BufferView& destination, const BufferView& source, const Stream& stream) const;

private:
    explicit DeviceRuntime(std::shared_ptr<detail::Driver> driver): driver_(std::move(driver)) {}
    std::shared_ptr<detail::Driver> driver_;
};
const char* backendName(Backend);
const char* memoryKindName(MemoryKind);
}  // namespace dlslime::device
