#include <fcntl.h>
#include <mcr/maca.h>
#include <mcr/mc_runtime_api.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <stdexcept>

#include "dlslime/csrc/device/runtime/device_runtime.h"
using namespace dlslime::device;
void require(bool v, const char* m)
{
    if (!v)
        throw std::runtime_error(m);
}
void ok(mcError_t v)
{
    require(v == mcSuccess, mcGetErrorString(v));
}
int main()
{
    try {
        int count;
        ok(mcGetDeviceCount(&count));
        for (int device = 0; device < count; ++device) {
            ok(mcSetDevice(device));
            void* native = nullptr;
            ok(mcMalloc(&native, 65536));
            auto rt     = DeviceRuntime::create(Backend::Maca, device);
            int  caller = (device + 1) % count;
            ok(mcSetDevice(caller));
            auto info = rt.query(static_cast<char*>(native) + 16);
            require(info.kind == MemoryKind::Device && !info.owned && info.device == device && info.range_known
                        && info.offset == 16,
                    "external MACA classification");
            auto  allocation = rt.allocate(1 << 20, MemoryKind::Device);
            auto  id         = allocation.info().allocation_id;
            void* ptr        = allocation.data();
            auto  whole      = rt.exportDmaBuf(allocation.view(0, 4096));
            auto  offset     = rt.exportDmaBuf(allocation.view(8192, 4096));
            require(offset.offset() == whole.offset() + 8192,
                    "DMA-BUF offset is not allocation offset plus view offset");
            require(offset.offset() <= offset.exportedSize()
                        && offset.size() <= offset.exportedSize() - offset.offset(),
                    "export extent invalid");
            require((fcntl(offset.fd(), F_GETFD) & FD_CLOEXEC) != 0, "DMA-BUF not CLOEXEC");
            allocation = {};
            require(rt.query(ptr).allocation_id == id, "DMA-BUF did not retain allocation");
            int  fd          = offset.fd();
            auto granularity = offset.granularity();
            offset           = {};
            require(fcntl(fd, F_GETFD) < 0 && errno == EBADF, "DMA-BUF descriptor leaked");
            whole = {};
            int current;
            ok(mcGetDevice(&current));
            require(current == caller, "caller device changed");
            ok(mcSetDevice(device));
            ok(mcFree(native));
            std::cout << "PASS MACA device=" << device
                      << " external pointer, caller device, DMA-BUF nonzero offset/extent/owner/CLOEXEC; granularity="
                      << granularity << "\n";
        }
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FAIL " << e.what() << '\n';
        return 1;
    }
}
