#pragma once
#include <functional>
#include <string>

#include "../region/host_region.h"
#include "tcp_op_state.h"

namespace dlslime::tcp {
// Success: all WRITE destination ACKs or all READ payloads arrived. Failure
// makes local buffers quiescent; the remote outcome can still be partial/unknown.
enum RegionStatus : int32_t {
    REGION_SUCCESS        = 0,
    REGION_IO_ERROR       = -1,
    REGION_TIMEOUT        = -2,
    REGION_CANCELLED      = -3,
    REGION_REJECTED       = -4,
    REGION_PROTOCOL_ERROR = -5,
    REGION_BUSY           = -6
};
struct RegionPeer {
    std::string host;
    uint16_t    port{};
    uint16_t    protocol{2};
};
struct RegionSpan {
    region::Key        local;
    region::Descriptor remote;
    uint64_t           local_offset{}, remote_offset{}, length{};
};
struct RegionOptions {
    size_t  window_requests{32}, window_bytes{8 * 1024 * 1024};
    size_t  max_clients{64}, max_servers{64};
    int64_t server_timeout_ms{30000};
};
class RegionFuture {
public:
    int32_t wait()
    {
        return state_->wait();
    }
    bool wait_for(int64_t timeout_ms, int32_t* status = nullptr)
    {
        return state_->wait_for(timeout_ms, status);
    }
    void cancel()
    {
        cancel_();
    }

private:
    friend class TcpRegionEndpoint;
    std::shared_ptr<TcpOpState> state_;
    std::function<void()>       cancel_;
};
class RegionEndpointImpl;
// Explicit opt-in API and separate listener. Never auto-detect legacy frames.
class TcpRegionEndpoint {
public:
    TcpRegionEndpoint(const std::string& ip, uint16_t port = 0, RegionOptions options = {});
    ~TcpRegionEndpoint();
    TcpRegionEndpoint(const TcpRegionEndpoint&)                                     = delete;
    TcpRegionEndpoint&                          operator=(const TcpRegionEndpoint&) = delete;
    std::shared_ptr<region::HostRegionRegistry> regions() const;
    RegionPeer                                  endpoint_info() const;
    std::shared_ptr<RegionFuture> read(const RegionPeer&, const std::vector<RegionSpan>&, int64_t timeout_ms = 30000);
    std::shared_ptr<RegionFuture> write(const RegionPeer&, const std::vector<RegionSpan>&, int64_t timeout_ms = 30000);
    void                          shutdown();

private:
    std::shared_ptr<RegionFuture>       transfer(const RegionPeer&, const std::vector<RegionSpan>&, int64_t, uint16_t);
    std::shared_ptr<RegionEndpointImpl> impl_;
};
}  // namespace dlslime::tcp
