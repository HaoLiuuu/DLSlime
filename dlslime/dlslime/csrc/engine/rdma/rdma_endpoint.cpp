#include "rdma_endpoint.h"

#include <stdlib.h>
#include <sys/types.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "dlslime/csrc/common/pause.h"
#include "dlslime/csrc/device/device_api.h"
#include "dlslime/csrc/engine/assignment.h"
#include "dlslime/csrc/logging.h"
#include "dlslime/csrc/observability/obs.h"
#include "dlslime/csrc/utils.h"
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_channel.h"
#include "rdma_assignment.h"
#include "rdma_common.h"
#include "rdma_context.h"
#include "rdma_context_pool.h"
#include "rdma_env.h"
#include "rdma_future.h"
#include "rdma_op_state.h"
#include "rdma_utils.h"
#include "rdma_worker.h"
#include "rdma_worker_pool.h"

namespace dlslime {

namespace {

uint32_t qpMask(size_t count)
{
    if (!count || count > 32)
        throw std::invalid_argument("RDMA requires 1..32 QPs");
    return count == 32 ? UINT32_MAX : (uint32_t{1} << count) - 1;
}
void publishTerminal(const std::shared_ptr<EndpointOpState>& op)
{
    if (op->terminal.exchange(true, std::memory_order_acq_rel))
        return;
    if (op->obs_enabled && !op->obs_completed.exchange(true, std::memory_order_acq_rel)) {
        if (op->completion_status.load(std::memory_order_acquire) == RDMAAssign::SUCCESS)
            obs::obs_record_complete(op->obs_nic_id, static_cast<obs::ObsOpIndex>(op->obs_op), op->obs_bytes);
        else
            obs::obs_record_fail(op->obs_nic_id, static_cast<obs::ObsOpIndex>(op->obs_op), op->obs_bytes);
    }
    op->completion_mask.store(op->expected_mask, std::memory_order_release);
    if (op->signal)
        for (size_t i = 0; i < 32; ++i)
            if (op->expected_mask & (uint32_t{1} << i))
                op->signal->set_comm_done(i);
}
void retireShard(const std::shared_ptr<EndpointOpState>& op)
{
    if (op->pending_shards.fetch_sub(1, std::memory_order_acq_rel) == 1)
        publishTerminal(op);
}
void setFailure(const std::shared_ptr<EndpointOpState>& op, int32_t status)
{
    if (status != RDMAAssign::SUCCESS) {
        int32_t expected = RDMAAssign::SUCCESS;
        op->completion_status.compare_exchange_strong(expected, status, std::memory_order_acq_rel);
    }
}

// Build a fresh EndpointOpState for one user submission. The signal is
// unique to this op so the returned future is never coupled to a reused
// slot's signal state.
std::shared_ptr<EndpointOpState>
makeOpState(uint32_t expected_mask, bool bypass_signal, void* stream, bool trace_start = false)
{
    auto op_state           = std::make_shared<EndpointOpState>();
    op_state->signal        = dlslime::device::createSignal(bypass_signal);
    op_state->expected_mask = expected_mask;
    op_state->completion_mask.store(0, std::memory_order_relaxed);
    op_state->completion_status.store(RDMAAssign::SUCCESS, std::memory_order_relaxed);
    op_state->imm_data.store(0, std::memory_order_relaxed);
    if (trace_start) {
        op_state->trace_start_ns.store(monotonic_time_ns(), std::memory_order_relaxed);
    }
    op_state->trace_end_ns.store(0, std::memory_order_relaxed);

    if (op_state->signal) {
        op_state->signal->reset_all();
        op_state->signal->bind_stream(stream);
    }
    return op_state;
}

}  // namespace

// ============================================================
// Constructor & Setup
// ============================================================

RDMAEndpoint::RDMAEndpoint(std::shared_ptr<RDMAMemoryPool> pool, size_t num_qp, std::shared_ptr<RDMAWorker> worker):
    local_pool_(pool), num_qp_(num_qp)
{
    if (!local_pool_) {
        SLIME_ABORT("Memory Pool cannot be null");
    }
    ctx_ = local_pool_->context();
    init(worker);
}

RDMAEndpoint::RDMAEndpoint(std::shared_ptr<RDMAContext> ctx, size_t num_qp, std::shared_ptr<RDMAWorker> worker):
    ctx_(ctx), num_qp_(num_qp)
{
    if (!ctx_) {
        SLIME_ABORT("RDMA Context cannot be null");
    }
    local_pool_ = std::make_shared<RDMAMemoryPool>(ctx);
    init(worker);
}

void RDMAEndpoint::init(std::shared_ptr<RDMAWorker> worker)
{
    qpMask(num_qp_);
    if (SLIME_MAX_SEND_WR <= 0 || SLIME_MAX_RECV_WR <= 0 || !SLIME_MAX_LENGTH_PER_ASSIGNMENT)
        throw std::invalid_argument("Invalid RDMA WR/chunk limits");
    if (not ctx_)
        SLIME_ABORT("No NIC Resources");

    // Observability: register this NIC for counter tracking
    if (obs::obs_enabled()) {
        obs_nic_id_ = obs::obs_register_nic(ctx_->device_name_.c_str());
    }

    // Always create a new Remote Pool (Independent)
    remote_pool_ = std::make_shared<RDMARemoteMemoryPool>();

    worker_ = worker ? worker : GlobalWorkerManager::instance().get_default_worker(socketId(ctx_->device_name_));

    SLIME_LOG_INFO("Initializing RDMA Endpoint. QPs: ", num_qp_, ", Token Bucket: ", SLIME_MAX_SEND_WR);
    SLIME_LOG_INFO("bypass Signal: ", SLIME_BYPASS_DEVICE_SIGNAL);
    if (SLIME_BYPASS_DEVICE_SIGNAL)
        bypass_signal_ = true;

    // Aggregation logic is not supported in V0 Send/Recv mode.
    SLIME_ASSERT(1 == SLIME_AGG_QP_NUM, "cannot aggqp when sendrecv");
    SLIME_ASSERT(64 > SLIME_QP_NUM, "QP NUM must less than 64");

    // ============================================================
    // Initialize meta_pool (per-endpoint, sys buffers, borrows PD from local_pool_)
    // ============================================================
    meta_pool_ = std::make_shared<RDMAMemoryPool>(local_pool_);

    // ============================================================
    // Initialize IO Endpoint Components
    // ============================================================

    io_data_channel_ = std::make_shared<RDMAChannel>(local_pool_, remote_pool_);
    io_data_channel_->init(ctx_, num_qp_, 0);

    for (int i = 0; i < num_qp_; ++i) {
        token_bucket_[i].store(SLIME_MAX_SEND_WR);
    }

    size_t ring_size        = SLIME_MAX_IO_FIFO_DEPTH;
    read_write_buffer_ring_ = createRing("io_rw_ring", ring_size);
    imm_recv_buffer_ring_   = createRing("io_recv_ring", ring_size);
    // jring capacity is size-1 (one slot reserved for the head/tail gap).
    // Double the nominal depth so the free ring can actually hold every
    // slot at once.
    rw_free_ring_ = createRing("io_rw_free_ring", ring_size * 2);

    void* raw_rw_ctx = nullptr;
    if (posix_memalign(&raw_rw_ctx, 64, sizeof(ReadWriteContext) * SLIME_MAX_IO_FIFO_DEPTH) != 0) {
        throw std::runtime_error("Failed to allocate memory for ReadWriteContext pool");
    }
    read_write_ctx_pool_ = static_cast<ReadWriteContext*>(raw_rw_ctx);

    void* raw_recv_ctx = nullptr;
    if (posix_memalign(&raw_recv_ctx, 64, sizeof(ImmRecvContext) * SLIME_MAX_IO_FIFO_DEPTH) != 0) {
        free(raw_rw_ctx);
        throw std::runtime_error("Failed to allocate memory for ImmRecvContext pool");
    }
    imm_recv_ctx_pool_ = static_cast<ImmRecvContext*>(raw_recv_ctx);

    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        new (&read_write_ctx_pool_[i]) ReadWriteContext();
        read_write_ctx_pool_[i].assigns_.resize(num_qp_);
        read_write_ctx_pool_[i].slot_id = i;
        ReadWriteContext* slot_ptr      = &read_write_ctx_pool_[i];
        while (jring_enqueue_burst(rw_free_ring_, (void**)&slot_ptr, 1, nullptr) == 0) {
            // Unreachable on a freshly-created ring sized to DEPTH, but guard
            // anyway so a future resize does not silently drop slots.
            machnet_pause();
        }

        new (&imm_recv_ctx_pool_[i]) ImmRecvContext();
        imm_recv_ctx_pool_[i].signal = dlslime::device::createSignal(false);
        imm_recv_ctx_pool_[i].assigns_.resize(num_qp_);
    }

    void* io_dummy_mem = nullptr;
    if (posix_memalign(&io_dummy_mem, 64, sizeof(int64_t)) != 0) {
        throw std::runtime_error("Failed to allocate io_dummy memory");
    }
    io_dummy_ = (int64_t*)io_dummy_mem;

    io_dummy_handle_ =
        meta_pool_->registerMemoryRegion(reinterpret_cast<uintptr_t>(io_dummy_), sizeof(int64_t), "sys.io_dummy");

    // ============================================================
    // Initialize Msg Endpoint Components
    // ============================================================

    // Allocate dummy buffer for Immediate Data payload or signaling.
    void* msg_dummy_mem = nullptr;
    if (posix_memalign(&msg_dummy_mem, 64, sizeof(int64_t)) != 0)
        throw std::runtime_error("msg_dummy alloc fail");
    msg_dummy_ = (int64_t*)msg_dummy_mem;

    // Allocate context pools aligned to cache lines.
    // Coalesced allocation for Send and Recv Contexts
    size_t send_pool_size = sizeof(SendContext) * SLIME_MAX_MSG_FIFO_DEPTH;
    size_t recv_pool_size = sizeof(RecvContext) * SLIME_MAX_MSG_FIFO_DEPTH;
    size_t total_size     = send_pool_size + recv_pool_size;

    void* raw_pool_ptr = nullptr;
    if (posix_memalign(&raw_pool_ptr, 64, total_size) != 0)
        throw std::runtime_error("context pool alloc fail");

    // Assign pointers
    send_ctx_pool_ = static_cast<SendContext*>(raw_pool_ptr);
    // Recv pool follows Send pool immediately
    recv_ctx_pool_ = reinterpret_cast<RecvContext*>(static_cast<char*>(raw_pool_ptr) + send_pool_size);

    for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
        new (&send_ctx_pool_[i]) SendContext();
        send_ctx_pool_[i].slot_id = i;

        new (&recv_ctx_pool_[i]) RecvContext();
        recv_ctx_pool_[i].slot_id = i;
    }

    // Register Memory Regions (MR) upfront on meta_pool_
    msg_dummy_handle_ =
        meta_pool_->registerMemoryRegion(reinterpret_cast<uintptr_t>(msg_dummy_), sizeof(int64_t), "sys.msg_dummy");

    // Register the single large block
    send_ctx_handle_ =
        meta_pool_->registerMemoryRegion(reinterpret_cast<uintptr_t>(send_ctx_pool_), total_size, "sys.send_ctx");

    // Calculate offset of remote_meta_info_ at runtime
    send_ctx_meta_offset_ = reinterpret_cast<uintptr_t>(&(send_ctx_pool_[0].remote_meta_info_))
                            - reinterpret_cast<uintptr_t>(send_ctx_pool_);

    SLIME_LOG_INFO("Endpoint initialized. Send/Recv Pool Coalesced. Meta Offset: ", send_ctx_meta_offset_);

    meta_channel_     = std::make_unique<RDMAChannel>(local_pool_, remote_pool_);
    msg_data_channel_ = std::make_unique<RDMAChannel>(local_pool_, remote_pool_);

    // Meta channel uses 1 QP (latency sensitive), Data channel uses num_qp_
    meta_channel_->init(ctx_, 1, 256);
    msg_data_channel_->init(ctx_, num_qp_, 0);

    // Initialize Rings. Size is double the depth to handle potential overflow gracefully.
    size_t msg_ring_size = SLIME_MAX_MSG_FIFO_DEPTH * 2;
    send_buffer_ring_    = createRing("send_buf", msg_ring_size);
    recv_buffer_ring_    = createRing("recv_buf", msg_ring_size);

    // Free-slot rings. jring capacity is size-1, so 2*DEPTH is required
    // to actually hold DEPTH slot pointers.
    send_free_ring_ = createRing("send_free", SLIME_MAX_MSG_FIFO_DEPTH * 2);
    recv_free_ring_ = createRing("recv_free", SLIME_MAX_MSG_FIFO_DEPTH * 2);
    for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
        SendContext* s_slot = &send_ctx_pool_[i];
        RecvContext* r_slot = &recv_ctx_pool_[i];
        while (jring_enqueue_burst(send_free_ring_, (void**)&s_slot, 1, nullptr) == 0) {
            machnet_pause();
        }
        while (jring_enqueue_burst(recv_free_ring_, (void**)&r_slot, 1, nullptr) == 0) {
            machnet_pause();
        }
    }

    SLIME_LOG_INFO("RDMA Endpoint Initialization Completed.");
}

RDMAEndpoint::RDMAEndpoint(
    std::string dev_name, int32_t ib_port, std::string link_type, size_t num_qp, std::shared_ptr<RDMAWorker> worker):
    RDMAEndpoint(GlobalContextManager::instance().get_context(dev_name, ib_port, link_type), num_qp, worker)
{
}

RDMAEndpoint::~RDMAEndpoint()
{
    try {
        shutdownImpl(false);
        // 1. Stop worker from calling process() on this endpoint
        connected_.store(false, std::memory_order_release);

        // 2. The tracker has synchronized all callbacks after QP destruction.
        //    Late CQEs contain only retired IDs, never context-slot pointers.
        io_data_channel_.reset();
        meta_channel_.reset();
        msg_data_channel_.reset();

        // 3. Now safe to free context pools and rings
        freeRing(read_write_buffer_ring_);
        freeRing(imm_recv_buffer_ring_);
        freeRing(rw_free_ring_);

        for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
            read_write_ctx_pool_[i].~ReadWriteContext();
            imm_recv_ctx_pool_[i].~ImmRecvContext();
        }
        free(read_write_ctx_pool_);
        free(imm_recv_ctx_pool_);
        free(io_dummy_);

        free(msg_dummy_);

        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            send_ctx_pool_[i].~SendContext();
            recv_ctx_pool_[i].~RecvContext();
        }
        free(send_ctx_pool_);

        freeRing(send_buffer_ring_);
        freeRing(recv_buffer_ring_);
        freeRing(send_free_ring_);
        freeRing(recv_free_ring_);

        SLIME_LOG_INFO("RDMAEndpoint destroyed successfully.");
    }
    catch (const std::exception& e) {
        SLIME_LOG_ERROR("Cannot safely destroy a non-quiescent RDMA endpoint: ", e.what());
        std::terminate();
    }
}

void RDMAEndpoint::connect(const json& remote_endpoint_info)
{
    // Auto-register remote user MRs from exchanged endpoint info.
    // Sort by original handle to guarantee remote handles match source-side handles.
    if (remote_endpoint_info.contains("mr_info") && !remote_endpoint_info["mr_info"].empty()) {
        std::vector<std::pair<std::string, json>> sorted_mrs;
        for (auto& [name, mr_data] : remote_endpoint_info["mr_info"].items()) {
            sorted_mrs.emplace_back(name, mr_data);
        }
        std::sort(sorted_mrs.begin(), sorted_mrs.end(), [](const auto& a, const auto& b) {
            return a.second.value("handle", 0) < b.second.value("handle", 0);
        });
        for (auto& [name, mr_data] : sorted_mrs) {
            registerOrAccessRemoteMemoryRegion(name, mr_data);
        }
    }

    // Connect IO Endpoint
    if (remote_endpoint_info.contains("io_info")) {
        io_data_channel_->connect(remote_endpoint_info["io_info"]["data_channel_info"]);
        // RNR avoidance: keep the HW RQ primed with transport-owned RECVs
        // so WRITE_WITH_IMM from the peer always finds a posted receive.
        postImmRecvWindow();
    }
    else {
        SLIME_LOG_WARN("UnifiedConnect: Missing 'io_info' in remote info");
    }

    // Connect Msg Endpoint
    if (remote_endpoint_info.contains("msg_info")) {
        SLIME_LOG_INFO("Establishing RDMA Connection...");
        meta_channel_->connect(remote_endpoint_info["msg_info"]["meta_channel_info"]);
        msg_data_channel_->connect(remote_endpoint_info["msg_info"]["data_channel_info"]);

        SLIME_LOG_INFO("Connection Established. Pre-posting RECV requests...");

        // Register the single Remote MR for Meta
        auto      meta_info   = remote_endpoint_info["msg_info"]["remote_meta_base"];
        uintptr_t remote_base = meta_info["addr"].get<uintptr_t>();
        size_t    length      = meta_info["length"].get<size_t>();
        uint32_t  rkey        = meta_info["rkey"].get<uint32_t>();

        int32_t remote_meta_handle = remote_pool_->registerRemoteMemoryRegion(remote_base, length, rkey);

        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            recv_ctx_pool_[i].remote_meta_key_ = remote_meta_handle;
        }

        // Metadata notifications are transport-owned. Data receives are
        // posted by recvProcess() with immutable operation callbacks.
        for (int i = 0; i < SLIME_MAX_MSG_FIFO_DEPTH; ++i) {
            SendContext*            send_ctx = &(send_ctx_pool_[i]);
            std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, sizeof(int64_t))};
            send_ctx->meta_recv_assign_.reset(OpCode::RECV, 0, batch, [send_ctx](int32_t status, int32_t imm) {
                send_ctx->meta_arrived_flag_.val.store(1, std::memory_order_release);
            });
            meta_channel_->post_recv_batch(0, &(send_ctx->meta_recv_assign_), meta_pool_);
        }

        // recvProcess() posts data receives before publishing metadata. This
        // preserves RNR safety without duplicate, mutable placeholder WRs.

        SLIME_LOG_INFO("RDMA Contexts Launched.");
    }
    else {
        SLIME_LOG_WARN("UnifiedConnect: Missing 'msg_info' in remote info");
    }

    connected_.store(true, std::memory_order_release);
    worker_->addEndpoint(shared_from_this());
}

json RDMAEndpoint::endpointInfo() const
{
    // IO endpoint info
    json io_info = json{{"data_channel_info", io_data_channel_->channelInfo()}};

    // Msg endpoint info (send_ctx is in meta_pool_)
    auto           base_ptr = reinterpret_cast<uintptr_t>(send_ctx_pool_);
    int32_t        handle   = meta_pool_->get_mr_handle(base_ptr);
    struct ibv_mr* mr       = meta_pool_->get_mr_fast(handle);
    SLIME_ASSERT(mr, "Send Context Pool MR not found");

    json msg_info = json{{"meta_channel_info", meta_channel_->channelInfo()},
                         {"data_channel_info", msg_data_channel_->channelInfo()},
                         {"remote_meta_base", {{"addr", base_ptr}, {"rkey", mr->rkey}, {"length", mr->length}}}};

    return json{{"mr_info", local_pool_->mrInfo()}, {"io_info", io_info}, {"msg_info", msg_info}};
}

json RDMAEndpoint::mrInfo() const
{
    return local_pool_->mrInfo();
}

RDMAEndpoint::SubmissionGuard::SubmissionGuard(RDMAEndpoint& ep): endpoint(ep)
{
    std::lock_guard lock(ep.api_mutex_);
    if (ep.closing_ || !ep.connected_)
        throw std::runtime_error("RDMA endpoint is not connected or is closing");
    ++ep.active_submissions_;
}
RDMAEndpoint::SubmissionGuard::~SubmissionGuard()
{
    std::lock_guard lock(endpoint.api_mutex_);
    if (!--endpoint.active_submissions_)
        endpoint.submissions_done_.notify_all();
}
void RDMAEndpoint::shutdown()
{
    shutdownImpl(true);
}
void RDMAEndpoint::cancelAll()
{
    shutdown();
}
void RDMAEndpoint::shutdownImpl(bool remove_worker)
{
    std::lock_guard shutdown_lock(shutdown_mutex_);
    if (shutdown_complete_)
        return;
    {
        std::unique_lock lock(api_mutex_);
        closing_   = true;
        connected_ = false;
        submissions_done_.wait(lock, [&] { return active_submissions_ == 0; });
    }
    std::lock_guard progress_lock(progress_mutex_);
    // Destroying each QP and retiring its tracker entries also synchronizes
    // with CQ callbacks. Never force-complete a future while DMA is possible.
    if ((io_data_channel_ && io_data_channel_->reset() != 0) || (meta_channel_ && meta_channel_->reset() != 0)
        || (msg_data_channel_ && msg_data_channel_->reset() != 0))
        throw std::runtime_error("RDMA QP destruction failed; endpoint is not quiescent");
    void* entry[2]{};
    while (jring_dequeue_burst(read_write_buffer_ring_, entry, 1, nullptr))
        pending_rw_queue_.push_back(static_cast<ReadWriteContext*>(entry[0]));
    for (auto* ctx : pending_rw_queue_)
        for (size_t q = 0; q < num_qp_; ++q) {
            auto cb = std::move(ctx->assigns_[q].callback_);
            if (cb)
                cb(RDMAAssign::FAILED, 0);
        }
    pending_rw_queue_.clear();
    // The remaining message operations were never posted, or their hardware
    // callbacks have now retired. It is safe to wake their waiters as failed.
    completeCancelledOps();
    if (remove_worker && worker_)
        worker_->removeEndpoint(shared_from_this());
    shutdown_complete_ = true;
}

// ============================================================
// Memory Management
// ============================================================

int32_t RDMAEndpoint::registerOrAccessMemoryRegion(uintptr_t mr_key, uintptr_t ptr, uintptr_t offset, size_t length)
{
    std::string auto_name = "mr_" + std::to_string(mr_key);
    return local_pool_->registerMemoryRegion(ptr + offset, length, auto_name);
}

int32_t
RDMAEndpoint::registerOrAccessMemoryRegion(const std::string& name, uintptr_t ptr, uintptr_t offset, size_t length)
{
    return local_pool_->registerMemoryRegion(ptr + offset, length, name);
}

int32_t RDMAEndpoint::registerOrAccessRemoteMemoryRegion(const std::string& name, json mr_info)
{
    return remote_pool_->registerRemoteMemoryRegion(name, mr_info);
}

// ============================================================
// In-flight tracking (for cancelAll)
// ============================================================

void RDMAEndpoint::registerInFlight(const std::shared_ptr<EndpointOpState>& op_state)
{
    std::lock_guard<SpinLock> guard(in_flight_lock_);
    // Opportunistic compaction: drop expired weak_ptrs so the vector does not
    // grow unboundedly with long-lived endpoints.
    if ((in_flight_ops_.size() & 0x3F) == 0) {
        in_flight_ops_.erase(std::remove_if(in_flight_ops_.begin(),
                                            in_flight_ops_.end(),
                                            [](const std::weak_ptr<EndpointOpState>& w) { return w.expired(); }),
                             in_flight_ops_.end());
    }
    in_flight_ops_.emplace_back(op_state);
}

// ============================================================
// Slot acquire / release
// ============================================================
//
// A slot is owned by exactly one in-flight op at a time. Acquisition
// blocks on the free ring if the pool is exhausted — this is explicit
// back-pressure at the API boundary, NOT the modulo stomping that the
// pre-refactor code used. Release is only called once every per-qp
// callback for the owning op has fired, so the HW can never deliver a
// CQE whose wr_id points into a slot that has been reassigned.

ReadWriteContext* RDMAEndpoint::acquireReadWriteSlot()
{
    // jring may batch up to 2 void*s per memcpy; use a 2-wide landing area
    // so static overflow analysis does not flag the unused upper half.
    void* landing[2] = {nullptr, nullptr};
    while (jring_dequeue_burst(rw_free_ring_, landing, 1, nullptr) == 0) {
        if (closing_)
            throw std::runtime_error("RDMA endpoint closed during slot admission");
        machnet_pause();
    }
    ReadWriteContext* ctx = static_cast<ReadWriteContext*>(landing[0]);
    ctx->token_charged.fill(false);
    ctx->slot_qp_mask.store(0, std::memory_order_relaxed);
    ctx->slot_qp_expected = qpMask(num_qp_);
    return ctx;
}

SendContext* RDMAEndpoint::acquireSendSlot()
{
    void* landing[2] = {nullptr, nullptr};
    while (jring_dequeue_burst(send_free_ring_, landing, 1, nullptr) == 0) {
        if (closing_)
            throw std::runtime_error("RDMA endpoint closed during slot admission");
        machnet_pause();
    }
    SendContext* ctx = static_cast<SendContext*>(landing[0]);
    ctx->slot_qp_mask.store(0, std::memory_order_relaxed);
    ctx->slot_qp_expected = qpMask(num_qp_);
    ctx->state_           = SendContextState::WAIT_GPU_READY;
    // NOTE: do NOT reset meta_arrived_flag_ here. The meta-recv RECV on
    // the meta channel is driven by the peer independently of our own
    // acquire; if the peer's meta write (targeting this slot's
    // remote_meta_info_) has already arrived and fired the flag before
    // the user called send(), clearing the flag would deadlock
    // sendProcess in WAIT_META. The flag is consumed (set to 0) by
    // sendProcess when the state machine advances past WAIT_META, so on
    // acquire it is either already 0 (nothing pending) or 1 (pre-arrived
    // for THIS slot) — both are correct inputs to the state machine.
    return ctx;
}

RecvContext* RDMAEndpoint::acquireRecvSlot()
{
    void* landing[2] = {nullptr, nullptr};
    while (jring_dequeue_burst(recv_free_ring_, landing, 1, nullptr) == 0) {
        if (closing_)
            throw std::runtime_error("RDMA endpoint closed during slot admission");
        machnet_pause();
    }
    RecvContext* ctx = static_cast<RecvContext*>(landing[0]);
    ctx->slot_qp_mask.store(0, std::memory_order_relaxed);
    ctx->slot_qp_expected = qpMask(num_qp_);
    ctx->state_           = RecvContextState::WAIT_GPU_BUF;
    return ctx;
}

void RDMAEndpoint::releaseReadWriteSlot(ReadWriteContext* ctx)
{
    ctx->op_state.reset();
    while (jring_enqueue_burst(rw_free_ring_, (void**)&ctx, 1, nullptr) == 0) {
        machnet_pause();
    }
}

void RDMAEndpoint::releaseSendSlot(SendContext* ctx)
{
    ctx->op_state.reset();
    while (jring_enqueue_burst(send_free_ring_, (void**)&ctx, 1, nullptr) == 0) {
        machnet_pause();
    }
}

void RDMAEndpoint::releaseRecvSlot(RecvContext* ctx)
{
    ctx->op_state.reset();
    while (jring_enqueue_burst(recv_free_ring_, (void**)&ctx, 1, nullptr) == 0) {
        machnet_pause();
    }
}

// ============================================================
// IO Endpoint Helper Methods
// ============================================================

void RDMAEndpoint::dummyReset(ImmRecvContext* ctx)
{
    for (int qpi = 0; qpi < num_qp_; ++qpi) {
        ctx->assigns_[qpi].opcode_ = OpCode::RECV;

        ctx->assigns_[qpi].batch_.resize(1);

        ctx->assigns_[qpi].batch_[0].length        = sizeof(*io_dummy_);
        ctx->assigns_[qpi].batch_[0].mr_key        = (uintptr_t)io_dummy_;
        ctx->assigns_[qpi].batch_[0].remote_mr_key = (uintptr_t)io_dummy_;
        ctx->assigns_[qpi].batch_[0].target_offset = 0;
        ctx->assigns_[qpi].batch_[0].source_offset = 0;

        ctx->assigns_[qpi].callback_ = [this, ctx, qpi](int32_t status, int32_t imm) {
            if (status != RDMAAssign::SUCCESS) {
                int32_t expected = RDMAAssign::SUCCESS;
                ctx->completion_status.compare_exchange_strong(
                    expected, status, std::memory_order_release, std::memory_order_relaxed);
            }
            else if (qpi == 0) {
                ctx->imm_data.store(imm, std::memory_order_release);
            }

            uint32_t old_mask = ctx->finished_qp_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
            uint32_t new_mask = old_mask | (1u << qpi);
            if (new_mask == ctx->expected_mask) {
                enqueueImmRecvCompletion(ctx);
            }
        };

        ctx->assigns_[qpi].is_inline_ = false;

        ctx->assigns_[qpi].imm_data_ = 0;
    }
}

void RDMAEndpoint::postImmRecvSlot(ImmRecvContext* ctx)
{
    ctx->expected_mask = qpMask(num_qp_);
    ctx->finished_qp_mask.store(0, std::memory_order_release);
    ctx->completion_status.store(RDMAAssign::SUCCESS, std::memory_order_release);
    ctx->imm_data.store(0, std::memory_order_release);

    dummyReset(ctx);

    for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
        io_data_channel_->post_recv_batch(qpi, &(ctx->assigns_[qpi]), meta_pool_);
    }
}

void RDMAEndpoint::postImmRecvWindow()
{
    // Fill the IO receive queues independently of user immRecv() calls. This
    // is the RNR-avoidance window: WRITE_WITH_IMM should find a posted receive
    // even if the application has not yet asked for the next message.
    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        ImmRecvContext* ctx = &imm_recv_ctx_pool_[i];
        ctx->slot_id        = i;
        postImmRecvSlot(ctx);
    }
}

void RDMAEndpoint::pushRefill(ImmRecvContext* ctx)
{
    // Lock-free MPSC push: CQ callback threads push completed slots.
    ImmRecvContext* old_head = refill_head_.load(std::memory_order_relaxed);
    do {
        ctx->next_refill_.store(old_head, std::memory_order_relaxed);
    } while (!refill_head_.compare_exchange_weak(old_head, ctx, std::memory_order_release, std::memory_order_relaxed));
}

ImmRecvContext* RDMAEndpoint::popAllRefill()
{
    // Atomically grab the entire refill chain. Only the progress thread calls
    // this, so a single exchange is sufficient (no ABA concern).
    return refill_head_.exchange(nullptr, std::memory_order_acquire);
}

void RDMAEndpoint::completeImmRecvOp(const std::shared_ptr<EndpointOpState>& op_state, const ImmRecvEvent& event)
{
    if (!op_state) {
        return;
    }
    op_state->completion_status.store(event.status, std::memory_order_release);
    op_state->imm_data.store(event.imm_data, std::memory_order_release);
    op_state->terminal.store(true, std::memory_order_release);
    if (SLIME_WITH_TIME_TRACE) {
        op_state->trace_end_ns.store(event.trace_end_ns, std::memory_order_release);
    }
    if (op_state->signal) {
        for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
            op_state->signal->set_comm_done(qpi);
        }
    }
}

void RDMAEndpoint::enqueueImmRecvCompletion(ImmRecvContext* ctx)
{
    // CQ callbacks run on the polling thread. Copy the reusable transport
    // slot's result into a small event, match it to a user op if possible, and
    // let the endpoint progress loop repost the slot outside the callback.
    ImmRecvEvent event{
        ctx->completion_status.load(std::memory_order_acquire),
        ctx->imm_data.load(std::memory_order_acquire),
        SLIME_WITH_TIME_TRACE ? monotonic_time_ns() : 0,
    };

    std::shared_ptr<EndpointOpState> op_state;
    {
        std::lock_guard<SpinLock> guard(imm_recv_match_lock_);
        if (!pending_imm_recv_ops_.empty()) {
            op_state = std::move(pending_imm_recv_ops_.front());
            pending_imm_recv_ops_.pop_front();
        }
        else {
            // Queue all events (SUCCESS, FAILED, FLUSH) so that a future
            // immRecv() caller observes the actual completion status rather
            // than blocking indefinitely.
            completed_imm_recv_events_.push_back(event);
        }
    }

    // Lock-free push to refill stack — outside any lock.
    if (event.status == RDMAAssign::SUCCESS)
        pushRefill(ctx);

    if (op_state) {
        completeImmRecvOp(op_state, event);
    }
}

void RDMAEndpoint::validateOneSided(const std::vector<assign_tuple_t>& spans)
{
    if (spans.empty())
        throw std::invalid_argument("Empty RDMA batch");
    uint64_t total = 0;
    for (const auto& span : spans) {
        auto [local, remote, ro, lo, length] = span;
        if (local > INT32_MAX || remote > INT32_MAX)
            throw std::invalid_argument("Invalid RDMA MR handle");
        auto* mr   = local_pool_->get_mr_fast(static_cast<int32_t>(local));
        auto  peer = remote_pool_->get_remote_mr_fast(static_cast<int32_t>(remote));
        if (!mr || !peer.addr || lo > mr->length || length > mr->length - lo || ro > peer.length
            || length > peer.length - ro || total > UINT64_MAX - length)
            throw std::invalid_argument("Invalid RDMA batch range");
        total += length;
    }
}
int32_t RDMAEndpoint::dispatchTask(OpCode                             opcode,
                                   const std::vector<assign_tuple_t>& spans,
                                   std::shared_ptr<EndpointOpState>   op,
                                   int32_t                            immediate,
                                   void*                              stream)
{
    size_t            request = 0, offset = 0;
    int32_t           last    = -1;
    ReadWriteContext* current = nullptr;
    bool              counted = false;
    op->expected_mask         = qpMask(num_qp_);
    if (op->signal) {
        op->signal->reset_all();
        op->signal->bind_stream(stream);
        op->signal->record_gpu_ready();
    }
    try {
        while (request < spans.size()) {
            if (closing_)
                throw std::runtime_error("RDMA dispatch cancelled");
            current           = acquireReadWriteSlot();
            counted           = false;
            current->op_state = op;
            last              = current->slot_id;
            for (auto& a : current->assigns_) {
                a.batch_.clear();
                a.batch_.reserve(SLIME_MAX_SEND_WR);
                a.callback_ = {};
            }
            size_t count = 0;
            while (request < spans.size() && count < size_t(SLIME_MAX_SEND_WR)) {
                auto [local, remote, ro, lo, length] = spans[request];
                const size_t chunk                   = std::min(length - offset, SLIME_MAX_LENGTH_PER_ASSIGNMENT);
                const size_t per_qp                  = chunk / num_qp_ + (chunk % num_qp_ != 0);
                for (size_t q = 0; q < num_qp_; ++q) {
                    const size_t relative    = q * per_qp;
                    const size_t n           = relative < chunk ? std::min(per_qp, chunk - relative) : 0;
                    const size_t safe_offset = n ? relative : 0;
                    current->assigns_[q].batch_.emplace_back(
                        local, remote, ro + offset + safe_offset, lo + offset + safe_offset, n);
                }
                ++count;
                offset += chunk;
                if (offset >= length) {
                    ++request;
                    offset = 0;
                }
            }
            const bool final_slot = request == spans.size();
            for (size_t q = 0; q < num_qp_; ++q) {
                auto* slot = current;
                auto& a    = slot->assigns_[q];
                // Decide AFTER filling the slot, not on each non-tail span.
                a.opcode_               = opcode == OpCode::WRITE_WITH_IMM && !final_slot ? OpCode::WRITE : opcode;
                a.is_inline_            = false;
                a.imm_data_             = immediate;
                const size_t batch_size = a.batch_.size();
                a.callback_             = [this, slot, q, batch_size, op](int32_t status, int32_t) {
                    setFailure(op, status);
                    if (slot->token_charged[q])
                        token_bucket_[q].fetch_add(batch_size, std::memory_order_release);
                    const auto before = slot->slot_qp_mask.fetch_or(uint32_t{1} << q, std::memory_order_acq_rel);
                    if ((before | (uint32_t{1} << q)) == slot->slot_qp_expected)
                        releaseReadWriteSlot(slot);
                    // All QPs and all slots, plus producer sealing, must retire.
                    retireShard(op);
                };
            }
            op->pending_shards.fetch_add(num_qp_, std::memory_order_relaxed);
            counted = true;
            while (jring_enqueue_burst(read_write_buffer_ring_, reinterpret_cast<void**>(&current), 1, nullptr) == 0) {
                if (closing_)
                    throw std::runtime_error("RDMA enqueue cancelled");
                machnet_pause();
            }
            current = nullptr;
        }
    }
    catch (...) {
        setFailure(op, RDMAAssign::FAILED);
        if (current) {
            if (counted) {
                for (size_t q = 0; q < num_qp_; ++q) {
                    auto cb = std::move(current->assigns_[q].callback_);
                    if (cb)
                        cb(RDMAAssign::FAILED, 0);
                }
            }
            else
                releaseReadWriteSlot(current);
        }
    }
    retireShard(op);  // Seal the producer, including cancellation during dispatch.
    return last;
}

// ============================================================
// Two-Sided Primitives (Message Passing)
// ============================================================
//
// Observability v0 reports semantic submit/completion only for
// one-sided read/write/writeWithImm. Two-sided send/recv and immRecv
// semantic pending are intentionally not reported in v0 — their
// completion paths are not yet integrated with the op_state-level
// once-only accounting. Transport-level post counters
// (post_send_batch / post_recv_batch / post_rc_oneside_batch) in
// rdma_channel.cpp are unaffected.

std::shared_ptr<SendFuture> RDMAEndpoint::send(const chunk_tuple_t& chunk, void* stream_handle)
{
    SubmissionGuard submission(*this);
    auto            data_ptr = std::get<0>(chunk);
    auto            offset   = std::get<1>(chunk);
    auto            length   = std::get<2>(chunk);

    storage_view_t view{data_ptr, offset, length};
    int32_t        handle = local_pool_->registerMemoryRegion(data_ptr, length);
    (void)handle;

    uint32_t target_mask = qpMask(num_qp_);

    // Blocking slot acquire — the free ring guarantees that ctx is not
    // owned by any in-flight op.
    SendContext* s_ctx = acquireSendSlot();

    // Build a fresh op_state for this send — unique signal + completion
    // fields. The future the caller receives can never observe another
    // send's completion.
    auto op_state = makeOpState(target_mask, bypass_signal_, stream_handle, /*trace_start=*/false);

    s_ctx->op_state               = op_state;
    s_ctx->local_meta_info_.view_ = {data_ptr, offset, length};

    if (op_state->signal) {
        op_state->signal->record_gpu_ready();
    }

    registerInFlight(op_state);

    while (jring_enqueue_burst(send_buffer_ring_, (void**)&s_ctx, 1, nullptr) == 0) {
        if (closing_) {
            setFailure(op_state, RDMAAssign::FAILED);
            releaseSendSlot(s_ctx);
            publishTerminal(op_state);
            break;
        }
        cpu_relax();
    }

    return std::make_shared<SendFuture>(op_state);
}

std::shared_ptr<RecvFuture> RDMAEndpoint::recv(const chunk_tuple_t& chunk, void* stream_handle)
{
    SubmissionGuard submission(*this);
    auto            data_ptr = std::get<0>(chunk);
    auto            offset   = std::get<1>(chunk);
    auto            length   = std::get<2>(chunk);

    storage_view_t view{data_ptr, offset, length};
    int32_t        handle = local_pool_->registerMemoryRegion(data_ptr, length);

    uint32_t target_mask = qpMask(num_qp_);

    RecvContext* r_ctx = acquireRecvSlot();

    auto op_state = makeOpState(target_mask, bypass_signal_, stream_handle, /*trace_start=*/false);

    r_ctx->op_state = op_state;
    r_ctx->view_    = {data_ptr, offset, length};

    struct ibv_mr* mr              = local_pool_->get_mr_fast(handle);
    r_ctx->local_meta_info_.r_key_ = mr->rkey;
    r_ctx->local_meta_info_.view_  = {data_ptr, offset, length};

    if (op_state->signal) {
        op_state->signal->record_gpu_ready();
    }

    registerInFlight(op_state);

    while (jring_enqueue_burst(recv_buffer_ring_, (void**)&r_ctx, 1, nullptr) == 0) {
        if (closing_) {
            setFailure(op_state, RDMAAssign::FAILED);
            releaseRecvSlot(r_ctx);
            publishTerminal(op_state);
            break;
        }
        cpu_relax();
    }

    return std::make_shared<RecvFuture>(op_state);
}

// ============================================================
// One-Sided Primitives (RDMA IO)
// ============================================================

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::read(const std::vector<assign_tuple_t>& assign, void* stream)
{
    SubmissionGuard submission(*this);
    validateOneSided(assign);
    auto op_state = makeOpState(qpMask(num_qp_), false, stream, /*trace_start=*/false);

    // Observability: anchor semantic accounting on op_state. The CQ
    // callback uses obs_completed.exchange(true) to record completion
    // exactly once per user op, regardless of num_qp or slot count.
    if (obs::obs_enabled()) {
        uint64_t total_bytes = 0;
        for (const auto& a : assign)
            total_bytes += std::get<4>(a);
        op_state->obs_enabled      = true;
        op_state->obs_bytes        = total_bytes;
        op_state->obs_assign_count = static_cast<uint32_t>(assign.size());
        op_state->obs_op           = static_cast<uint8_t>(obs::OBS_OP_READ);
        op_state->obs_nic_id       = static_cast<uint16_t>(obs_nic_id_);
        obs::obs_record_submit(obs_nic_id_, obs::OBS_OP_READ, op_state->obs_assign_count, total_bytes);
    }

    registerInFlight(op_state);
    dispatchTask(OpCode::READ, assign, op_state, 0, stream);
    return std::make_shared<ReadWriteFuture>(op_state);
}

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::write(const std::vector<assign_tuple_t>& assign, void* stream)
{
    SubmissionGuard submission(*this);
    validateOneSided(assign);
    auto op_state = makeOpState(qpMask(num_qp_), false, stream, /*trace_start=*/false);

    if (obs::obs_enabled()) {
        uint64_t total_bytes = 0;
        for (const auto& a : assign)
            total_bytes += std::get<4>(a);
        op_state->obs_enabled      = true;
        op_state->obs_bytes        = total_bytes;
        op_state->obs_assign_count = static_cast<uint32_t>(assign.size());
        op_state->obs_op           = static_cast<uint8_t>(obs::OBS_OP_WRITE);
        op_state->obs_nic_id       = static_cast<uint16_t>(obs_nic_id_);
        obs::obs_record_submit(obs_nic_id_, obs::OBS_OP_WRITE, op_state->obs_assign_count, total_bytes);
    }

    registerInFlight(op_state);
    dispatchTask(OpCode::WRITE, assign, op_state, 0, stream);
    return std::make_shared<ReadWriteFuture>(op_state);
}

std::shared_ptr<ReadWriteFuture>
RDMAEndpoint::writeWithImm(const std::vector<assign_tuple_t>& assign, int32_t imm_data, void* stream)
{
    SubmissionGuard submission(*this);
    validateOneSided(assign);
    auto op_state = makeOpState(qpMask(num_qp_), false, stream, /*trace_start=*/false);

    if (obs::obs_enabled()) {
        uint64_t total_bytes = 0;
        for (const auto& a : assign)
            total_bytes += std::get<4>(a);
        op_state->obs_enabled      = true;
        op_state->obs_bytes        = total_bytes;
        op_state->obs_assign_count = static_cast<uint32_t>(assign.size());
        op_state->obs_op           = static_cast<uint8_t>(obs::OBS_OP_WRITE_WITH_IMM);
        op_state->obs_nic_id       = static_cast<uint16_t>(obs_nic_id_);
        obs::obs_record_submit(obs_nic_id_, obs::OBS_OP_WRITE_WITH_IMM, op_state->obs_assign_count, total_bytes);
    }

    registerInFlight(op_state);
    dispatchTask(OpCode::WRITE_WITH_IMM, assign, op_state, imm_data, stream);
    return std::make_shared<ReadWriteFuture>(op_state);
}

std::shared_ptr<ImmRecvFuture> RDMAEndpoint::immRecv(void* stream)
{
    SubmissionGuard submission(*this);
    // v0: no semantic submit for immRecv — see note on send/recv above.

    io_recv_slot_id_.fetch_add(1, std::memory_order_relaxed);

    // One user-level receive operation. It may complete immediately from a
    // queued event, or wait in pending_imm_recv_ops_ until a future CQ event
    // arrives. It does not own a hardware receive slot; the transport-owned
    // slots stay permanently posted (RNR-avoidance window).
    auto op_state = makeOpState(qpMask(num_qp_), false, stream, /*trace_start=*/SLIME_WITH_TIME_TRACE != 0);

    std::optional<ImmRecvEvent> ready_event;
    {
        std::lock_guard<SpinLock> guard(imm_recv_match_lock_);
        if (!completed_imm_recv_events_.empty()) {
            ready_event = completed_imm_recv_events_.front();
            completed_imm_recv_events_.pop_front();
        }
        else {
            pending_imm_recv_ops_.push_back(op_state);
        }
    }

    registerInFlight(op_state);

    if (ready_event) {
        completeImmRecvOp(op_state, *ready_event);
    }

    return std::make_shared<ImmRecvFuture>(op_state);
}

// ============================================================
// Progress Engine - IO Operations
// ============================================================

int32_t RDMAEndpoint::readWriteProcess()
{
    int       work  = 0;
    const int count = jring_dequeue_burst(read_write_buffer_ring_, io_burst_buf_, IO_BURST_SIZE, nullptr);
    for (int i = 0; i < count; ++i)
        pending_rw_queue_.push_back(static_cast<ReadWriteContext*>(io_burst_buf_[i]));
    std::unordered_set<EndpointOpState*> blocked;
    // One bounded pass. Skip unrelated unready producers, while retaining the
    // original per-operation slot order and final WRITE_WITH_IMM ordering.
    const size_t candidates = pending_rw_queue_.size();
    for (size_t i = 0; i < candidates; ++i) {
        auto* slot = pending_rw_queue_.front();
        pending_rw_queue_.pop_front();
        auto op    = slot->op_state;
        bool ready = op && op->signal && op->signal->is_gpu_ready() && !blocked.count(op.get());
        for (size_t q = 0; ready && q < num_qp_; ++q)
            ready = token_bucket_[q].load(std::memory_order_acquire)
                    >= static_cast<int32_t>(slot->assigns_[q].batch_.size());
        if (!ready) {
            blocked.insert(op.get());
            pending_rw_queue_.push_back(slot);
            continue;
        }
        ++work;
        for (size_t q = 0; q < num_qp_; ++q) {
            const auto n           = slot->assigns_[q].batch_.size();
            slot->token_charged[q] = true;
            token_bucket_[q].fetch_sub(n, std::memory_order_release);
            io_data_channel_->post_rc_oneside_batch(q, &slot->assigns_[q], local_pool_);
        }
    }
    return work;
}

int32_t RDMAEndpoint::immRecvProcess()
{
    int32_t work_done = 0;

    // Atomically drain the entire lock-free refill stack in one exchange,
    // then batch-repost all completed transport slots without any locking.
    ImmRecvContext* batch = popAllRefill();
    while (batch) {
        ImmRecvContext* next = batch->next_refill_.load(std::memory_order_relaxed);
        batch->next_refill_.store(nullptr, std::memory_order_relaxed);
        postImmRecvSlot(batch);
        batch = next;
        work_done++;
    }

    return work_done;
}

// ============================================================
// Progress Engine - Message Operations
// ============================================================

int32_t RDMAEndpoint::sendProcess()
{
    int work_done = 0;

    int n = jring_dequeue_burst(send_buffer_ring_, send_new_burst_buf_, BURST_SIZE, nullptr);
    if (n > 0) {
        work_done += n;
        for (int i = 0; i < n; ++i) {
            auto* s_ctx = (SendContext*)send_new_burst_buf_[i];
            pending_send_queue_.push_back(s_ctx);
        }
    }

    auto it = pending_send_queue_.begin();

    if (it != pending_send_queue_.end()) {
        SendContext* s_ctx          = *it;
        bool         task_completed = false;

        auto op_state = s_ctx->op_state;
        if (!op_state) {
            pending_send_queue_.pop_front();
            return work_done;
        }

        switch (s_ctx->state_) {
            case SendContextState::WAIT_GPU_READY: {
                if (op_state->signal && op_state->signal->is_gpu_ready()) {
                    s_ctx->state_ = SendContextState::WAIT_META;
                    goto CHECK_META_READY;
                }
                break;
            }

            CHECK_META_READY:
            case SendContextState::WAIT_META: {
                if (s_ctx->meta_arrived_flag_.val.load(std::memory_order_acquire)) {
                    s_ctx->meta_arrived_flag_.val.store(false, std::memory_order_release);

                    std::vector<Assignment> meta_batch{
                        Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, sizeof(int64_t))};
                    s_ctx->meta_recv_assign_.reset(OpCode::RECV, 0, meta_batch, [s_ctx](int32_t status, int32_t imm) {
                        s_ctx->meta_arrived_flag_.val.store(1, std::memory_order_release);
                    });
                    meta_channel_->post_recv_batch(0, &(s_ctx->meta_recv_assign_), meta_pool_);

                    s_ctx->state_ = SendContextState::POST_DATA_SEND;

                    int32_t remote_data_handle =
                        remote_pool_->registerRemoteMemoryRegion(s_ctx->remote_meta_info_.view_.data_ptr,
                                                                 s_ctx->remote_meta_info_.view_.length,
                                                                 s_ctx->remote_meta_info_.r_key_);

                    int32_t local_data_handle = local_pool_->get_mr_handle(s_ctx->local_meta_info_.view_.data_ptr);

                    size_t total_len  = s_ctx->remote_meta_info_.view_.length;
                    size_t chunk_size = (total_len + num_qp_ - 1) / num_qp_;

                    for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                        size_t offset      = qpi * chunk_size;
                        size_t current_len = 0;

                        if (offset < total_len) {
                            current_len = std::min(chunk_size, total_len - offset);
                        }
                        else {
                            current_len = 0;
                            offset      = 0;
                        }

                        // Use handles (not raw ptrs) for post_rc_oneside_batch fast-path lookup
                        Assignment      assign(local_data_handle, remote_data_handle, offset, offset, current_len);
                        AssignmentBatch batch{assign};

                        s_ctx->data_send_assigns_[qpi].reset(
                            OpCode::WRITE_WITH_IMM,
                            qpi,
                            batch,
                            // Close over op_state (not s_ctx) so the future
                            // bound to THIS send sees completion even if the
                            // slot is later reused for a different send. The
                            // s_ctx capture is used only to release the slot
                            // back to the free pool once every qp has fired.
                            [this, s_ctx, op_state, qpi](int32_t stat, int32_t imm_data) {
                                if (stat != RDMAAssign::SUCCESS) {
                                    int32_t expected = RDMAAssign::SUCCESS;
                                    op_state->completion_status.compare_exchange_strong(
                                        expected, stat, std::memory_order_release, std::memory_order_relaxed);
                                }
                                op_state->completion_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);

                                uint32_t mask_before =
                                    s_ctx->slot_qp_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
                                uint32_t mask_after = mask_before | (1u << qpi);
                                if (mask_after == s_ctx->slot_qp_expected) {
                                    releaseSendSlot(s_ctx);
                                    publishTerminal(op_state);
                                }
                            },
                            false);

                        msg_data_channel_->post_rc_oneside_batch(qpi, &(s_ctx->data_send_assigns_[qpi]), local_pool_);
                    }

                    task_completed = true;
                }
                break;
            }

            default:
                break;
        }

        if (task_completed) {
            pending_send_queue_.pop_front();
            work_done++;
        }
    }
    return work_done;
}

int32_t RDMAEndpoint::recvProcess()
{
    int work_done = 0;

    int n = jring_dequeue_burst(recv_buffer_ring_, recv_new_burst_buf_, BURST_SIZE, nullptr);
    if (n > 0) {
        work_done += n;
        for (int i = 0; i < n; ++i) {
            auto* r_ctx   = (RecvContext*)recv_new_burst_buf_[i];
            r_ctx->state_ = RecvContextState::WAIT_GPU_BUF;
            pending_recv_queue_.push_back(r_ctx);
        }
    }

    auto it = pending_recv_queue_.begin();
    if (it != pending_recv_queue_.end()) {
        RecvContext* r_ctx          = *it;
        bool         task_completed = false;

        auto op_state = r_ctx->op_state;
        if (!op_state) {
            pending_recv_queue_.pop_front();
            return work_done;
        }

        switch (r_ctx->state_) {
            case RecvContextState::WAIT_GPU_BUF: {
                if (op_state->signal && op_state->signal->is_gpu_ready()) {
                    r_ctx->state_ = RecvContextState::INIT_SEND_META;
                    goto SEND_META;
                }
                break;
            }

            SEND_META:
            case RecvContextState::INIT_SEND_META: {
                // Post this receive's immutable callbacks before publishing
                // metadata, so the sender cannot reach an empty data RQ.
                for (size_t qpi = 0; qpi < num_qp_; ++qpi) {
                    std::vector<Assignment> batch{Assignment(reinterpret_cast<uintptr_t>(msg_dummy_), 0, 0, 8)};
                    r_ctx->data_recv_assigns_[qpi].reset(
                        OpCode::RECV, qpi, batch, [this, r_ctx, op_state, qpi](int32_t status, int32_t imm) {
                            if (status == 0) {
                                op_state->completion_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
                            }
                            else {
                                int32_t expected = RDMAAssign::SUCCESS;
                                op_state->completion_status.compare_exchange_strong(
                                    expected, status, std::memory_order_release, std::memory_order_relaxed);
                                SLIME_LOG_DEBUG("Data Recv flushed during completion (likely teardown)");
                            }

                            uint32_t mask_before = r_ctx->slot_qp_mask.fetch_or(1u << qpi, std::memory_order_acq_rel);
                            uint32_t mask_after  = mask_before | (1u << qpi);
                            if (mask_after == r_ctx->slot_qp_expected) {
                                releaseRecvSlot(r_ctx);
                                publishTerminal(op_state);
                            }
                        });
                    // Metadata is published only after every receive has been posted.
                    msg_data_channel_->post_recv_batch(qpi, &(r_ctx->data_recv_assigns_[qpi]), meta_pool_);
                }

                int slot = r_ctx->slot_id;

                uintptr_t local_meta_addr = reinterpret_cast<uintptr_t>(&(r_ctx->local_meta_info_));
                uintptr_t send_pool_base  = reinterpret_cast<uintptr_t>(send_ctx_pool_);
                uint64_t  local_offset    = local_meta_addr - send_pool_base;

                uint64_t remote_offset = slot * sizeof(SendContext) + send_ctx_meta_offset_;

                // Use handles (not raw ptrs): send_ctx_handle_ for local, remote_meta_key_ already a handle
                Assignment assign(
                    send_ctx_handle_, r_ctx->remote_meta_key_, remote_offset, local_offset, sizeof(meta_info_t));
                AssignmentBatch assign_batch{assign};

                r_ctx->meta_send_assign_.reset(OpCode::WRITE_WITH_IMM, 0, assign_batch, nullptr, true);
                meta_channel_->post_rc_oneside_batch(0, &(r_ctx->meta_send_assign_), meta_pool_);

                r_ctx->state_  = RecvContextState::WAIT_GPU_BUF;
                task_completed = true;
                break;
            }

            default:
                break;
        }

        if (task_completed) {
            pending_recv_queue_.pop_front();
            work_done++;
        }
    }

    return work_done;
}

// ============================================================
// Main Process Loop
// ============================================================

int32_t RDMAEndpoint::process()
{
    std::lock_guard lock(progress_mutex_);
    if (connected_.load(std::memory_order_acquire)) {
        return readWriteProcess() + immRecvProcess() + sendProcess() + recvProcess();
    }
    return 0;
}

// ============================================================
// Cleanup
// ============================================================

void RDMAEndpoint::completeCancelledOps()
{
    // Drain pending imm-recv user ops: mark failed + wake their signals.
    {
        std::lock_guard<SpinLock> guard(imm_recv_match_lock_);
        for (auto& op_state : pending_imm_recv_ops_) {
            if (op_state) {
                op_state->completion_status.store(RDMAAssign::FAILED, std::memory_order_release);
                if (op_state->signal) {
                    op_state->signal->force_complete();
                }
            }
        }
        pending_imm_recv_ops_.clear();
        completed_imm_recv_events_.clear();
    }

    // Drain the lock-free refill stack.
    popAllRefill();

    // Force-complete every in-flight user op. This includes send / recv /
    // read / write / writeWithImm, so futures waiting on shutdown do not
    // block forever. Stale weak_ptrs are skipped automatically.
    std::vector<std::shared_ptr<EndpointOpState>> live_ops;
    {
        std::lock_guard<SpinLock> guard(in_flight_lock_);
        live_ops.reserve(in_flight_ops_.size());
        for (auto& w : in_flight_ops_) {
            if (auto sp = w.lock()) {
                live_ops.push_back(std::move(sp));
            }
        }
        in_flight_ops_.clear();
    }
    for (auto& op_state : live_ops) {
        if (op_state->terminal.load(std::memory_order_acquire))
            continue;
        op_state->terminal.store(true, std::memory_order_release);
        int32_t expected = RDMAAssign::SUCCESS;
        op_state->completion_status.compare_exchange_strong(
            expected, RDMAAssign::FAILED, std::memory_order_release, std::memory_order_relaxed);
        if (op_state->signal) {
            op_state->signal->force_complete();
        }
        // Observability: record cancellation as failure, exactly once.
        // If the real completion already raced ahead and recorded, this
        // short-circuits via obs_completed.
        if (op_state->obs_enabled && !op_state->obs_completed.exchange(true, std::memory_order_acq_rel)) {
            obs::obs_record_fail(
                op_state->obs_nic_id, static_cast<obs::ObsOpIndex>(op_state->obs_op), op_state->obs_bytes);
        }
    }

    // Also force-complete transport-owned imm-recv slot signals so any
    // still-pending CQ wakeups on shutdown do not spin forever.
    for (int i = 0; i < SLIME_MAX_IO_FIFO_DEPTH; ++i) {
        if (imm_recv_ctx_pool_ && imm_recv_ctx_pool_[i].signal) {
            imm_recv_ctx_pool_[i].signal->force_complete();
        }
    }
}

}  // namespace dlslime
