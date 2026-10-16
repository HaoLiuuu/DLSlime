#include <atomic>
#include <fstream>
#include <limits>
#include <map>
#include <stdexcept>

#include "runtime_internal.h"

namespace dlslime::device {
namespace {
std::mutex                                                  registryMutex;
std::map<uintptr_t, std::weak_ptr<detail::AllocationState>> registry;
std::atomic<uint64_t>                                       nextIdentity{1};
void                                                        require(bool ok, const char* message)
{
    if (!ok)
        throw std::invalid_argument(message);
}
bool hostMemory(MemoryKind k)
{
    return k == MemoryKind::HostPageable || k == MemoryKind::HostPinned;
}
std::shared_ptr<detail::EventState> eventState(const std::shared_ptr<detail::Driver>& d)
{
    auto e     = std::make_shared<detail::EventState>();
    e->driver  = d;
    e->handle  = d->createEvent();
    e->created = true;
    return e;
}
}  // namespace
const char* backendName(Backend b)
{
    switch (b) {
        case Backend::Host:
            return "HOST";
        case Backend::Cuda:
            return "CUDA";
        case Backend::Maca:
            return "MACA";
        case Backend::Ascend:
            return "ASCEND";
    }
    return "UNKNOWN";
}
const char* memoryKindName(MemoryKind k)
{
    switch (k) {
        case MemoryKind::HostPageable:
            return "HOST_PAGEABLE";
        case MemoryKind::HostPinned:
            return "HOST_PINNED";
        case MemoryKind::Device:
            return "DEVICE";
        case MemoryKind::Managed:
            return "MANAGED";
        default:
            return "UNKNOWN";
    }
}
DeviceRuntime DeviceRuntime::create(Backend b, int device)
{
    if (b == Backend::Host) {
        require(device == 0, "HOST has only logical device 0");
        return DeviceRuntime(detail::makeHost());
    }
    require(device >= 0, "negative device ordinal");
#ifdef DLSLIME_RUNTIME_CUDA
    if (b == Backend::Cuda)
        return DeviceRuntime(detail::makeCuda(device));
#endif
#ifdef DLSLIME_RUNTIME_MACA
    if (b == Backend::Maca)
        return DeviceRuntime(detail::makeMaca(device));
#endif
#ifdef DLSLIME_RUNTIME_ASCEND
    if (b == Backend::Ascend)
        return DeviceRuntime(detail::makeAscend(device));
#endif
    throw std::runtime_error(std::string(backendName(b)) + " runtime not built; no HOST fallback");
}
DeviceInfo DeviceRuntime::deviceInfo() const
{
    return driver_->info();
}
Capabilities DeviceRuntime::capabilities() const
{
    return driver_->capabilities();
}
Allocation DeviceRuntime::allocate(size_t bytes, MemoryKind kind) const
{
    require(bytes && bytes <= size_t(std::numeric_limits<ptrdiff_t>::max()), "invalid allocation size");
    require(kind == MemoryKind::Device || kind == MemoryKind::HostPinned || kind == MemoryKind::HostPageable,
            "unsupported allocation kind");
    auto a    = std::make_shared<detail::AllocationState>();
    a->driver = driver_;
    auto di   = driver_->info();
    a->info   = {kind, di.backend, di.ordinal, 0, bytes, 0, nextIdentity.fetch_add(1), true, true};
    if (!a->info.allocation_id)
        throw std::overflow_error("allocation identity exhausted");
    a->pointer = driver_->allocate(bytes, kind);
    if (!a->pointer)
        throw std::bad_alloc();
    a->info.base = reinterpret_cast<uintptr_t>(a->pointer);
    {
        std::lock_guard<std::mutex> lock(registryMutex);
        for (auto it = registry.begin(); it != registry.end();) {
            if (it->second.expired())
                it = registry.erase(it);
            else
                ++it;
        }
        registry[a->info.base] = a;
    }
    return Allocation(a);
}
PointerInfo DeviceRuntime::query(const void* pointer) const
{
    if (!pointer)
        return {};
    auto address = reinterpret_cast<uintptr_t>(pointer);
    {
        std::lock_guard<std::mutex> lock(registryMutex);
        auto                        it = registry.upper_bound(address);
        if (it != registry.begin()) {
            --it;
            if (auto a = it->second.lock()) {
                if (address - a->info.base < a->info.bytes) {
                    auto info   = a->info;
                    info.offset = address - info.base;
                    return info;
                }
            }
        }
    }
    return driver_->query(pointer);
}
void* Allocation::data() const
{
    return state_ ? state_->pointer : nullptr;
}
size_t Allocation::size() const
{
    return state_ ? state_->info.bytes : 0;
}
PointerInfo Allocation::info() const
{
    return state_ ? state_->info : PointerInfo{};
}
BufferView Allocation::view(size_t offset, size_t bytes) const
{
    require(bool(state_), "empty allocation");
    require(offset <= size() && bytes <= size() - offset, "buffer view out of range");
    return BufferView(state_, offset, bytes);
}
void* BufferView::data() const
{
    return reinterpret_cast<void*>(allocation_->info.base + offset_);
}
Stream DeviceRuntime::createStream() const
{
    // Allocate the state before the native resource so allocation failure leaks nothing.
    auto s     = std::make_shared<detail::StreamState>();
    s->driver  = driver_;
    s->handle  = driver_->createStream();
    s->created = true;
    return Stream(s);
}
bool Event::ready() const
{
    require(bool(state_), "empty event");
    return state_->driver->ready(state_->handle);
}
void Event::wait() const
{
    require(bool(state_), "empty event");
    state_->driver->wait(state_->handle);
}
void Stream::wait(const Event& producer) const
{
    require(bool(state_) && bool(producer.state_), "empty stream or producer event");
    require(state_->driver == producer.state_->driver, "event and stream belong to different runtimes");
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->dependencies.push_back(producer.state_);
    try {
        state_->driver->streamWait(state_->handle, producer.state_->handle);
    }
    catch (...) {
        state_->dependencies.pop_back();
        throw;
    }
}
void Stream::synchronize() const
{
    require(bool(state_), "empty stream");
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->driver->synchronize(state_->handle);
    state_->dependencies.clear();
}
Event DeviceRuntime::record(const Stream& stream) const
{
    require(bool(stream.state_) && stream.state_->driver == driver_, "stream belongs to a different runtime");
    auto                        e = eventState(driver_);
    std::lock_guard<std::mutex> lock(stream.state_->mutex);
    driver_->record(e->handle, stream.state_->handle);
    e->recorded = true;
    e->dependencies.swap(stream.state_->dependencies);
    return Event(e);
}
Event DeviceRuntime::copy(const BufferView& dst, const BufferView& src, const Stream& stream) const
{
    require(bool(stream.state_) && stream.state_->driver == driver_, "stream belongs to a different runtime");
    require(dst.size() == src.size(), "copy lengths differ");
    require(dst.allocation_->driver == driver_ && src.allocation_->driver == driver_,
            "copy allocations belong to a different runtime");
    auto dk = dst.allocation_->info.kind, sk = src.allocation_->info.kind;
    if (!hostMemory(dk) || !hostMemory(sk))
        require(dk != MemoryKind::HostPageable && sk != MemoryKind::HostPageable,
                "device copy requires pinned host memory");
    if (dst.size() && dst.allocation_ == src.allocation_ && (!hostMemory(dk) || !hostMemory(sk))) {
        auto a = dst.offset_, b = src.offset_;
        require(a == b || (a < b ? b - a : a - b) >= dst.size(), "overlapping device copy");
    }
    auto e         = eventState(driver_);
    e->allocations = {dst.allocation_, src.allocation_};
    std::lock_guard<std::mutex> lock(stream.state_->mutex);
    try {
        if (dst.size() && dst.data() != src.data())
            driver_->copy(dst.data(), dk, src.data(), sk, dst.size(), stream.state_->handle);
        driver_->record(e->handle, stream.state_->handle);
        e->recorded = true;
    }
    catch (...) {
        detail::cleanup([&] { driver_->synchronize(stream.state_->handle); });
        throw;
    }
    e->dependencies.swap(stream.state_->dependencies);
    return Event(e);
}
DmaBuf DeviceRuntime::exportDmaBuf(const BufferView& view) const {
    require(view.allocation_->driver==driver_,"DMA-BUF allocation belongs to a different runtime");
    require(view.allocation_->info.kind==MemoryKind::Device && view.size()>0,"DMA-BUF requires a nonempty device range");
    auto state=std::make_shared<detail::DmaBufState>();state->allocation=view.allocation_;
    state->exported=driver_->exportDmaBuf(view.allocation_->pointer,view.allocation_->info.bytes,view.offset_,view.size());
    return DmaBuf(state);
}
int DmaBuf::fd() const { require(bool(state_),"empty DMA-BUF");return state_->exported.fd; }
size_t DmaBuf::offset() const { require(bool(state_),"empty DMA-BUF");return state_->exported.offset; }
size_t DmaBuf::size() const { require(bool(state_),"empty DMA-BUF");return state_->exported.bytes; }
size_t DmaBuf::exportedSize() const { require(bool(state_),"empty DMA-BUF");return state_->exported.exported_bytes; }
size_t DmaBuf::granularity() const { require(bool(state_),"empty DMA-BUF");return state_->exported.granularity; }
namespace detail {
int pciNumaNode(const std::string& pci)
{
    int node = -1;
    if (!pci.empty()) {
        std::ifstream f("/sys/bus/pci/devices/" + pci + "/numa_node");
        f >> node;
    }
    return node;
}
}  // namespace detail
}  // namespace dlslime::device
