#include "rdma_channel.h"

#include <arpa/inet.h>

#include <limits>
#include <stdexcept>

#include "dlslime/csrc/observability/obs.h"
#include "memory_pool.h"

namespace dlslime {
int32_t RDMAChannel::init(std::shared_ptr<RDMAContext> ctx, size_t num_qp, int32_t max_inline_data)
{
    reset();

    ctx_ = ctx;

    qp_.resize(num_qp);

    send_wr_pool_.resize(num_qp);
    recv_wr_pool_.resize(num_qp);

    send_sge_pool_.resize(num_qp);
    recv_sge_pool_.resize(num_qp);

    local_rdma_info_.resize(num_qp);
    remote_rdma_info_.resize(num_qp);

    for (int qpi = 0; qpi < num_qp; ++qpi) {
        send_wr_pool_[qpi].resize(SLIME_MAX_SEND_WR);
        recv_wr_pool_[qpi].resize(SLIME_MAX_RECV_WR);

        send_sge_pool_[qpi].resize(SLIME_MAX_SEND_WR);
        recv_sge_pool_[qpi].resize(SLIME_MAX_RECV_WR);

        /* Create Queue Pair (QP) */
        struct ibv_qp_init_attr qp_init_attr = {};
        qp_init_attr.send_cq                 = ctx_->cq_;
        qp_init_attr.recv_cq                 = ctx_->cq_;
        qp_init_attr.qp_type                 = IBV_QPT_RC;  // Reliable Connection

        if (max_inline_data == 0) {
            qp_init_attr.cap.max_send_wr = SLIME_MAX_SEND_WR;
        }
        else {
            SLIME_ASSERT(max_inline_data <= 4096, "inline data need to less than or equal to 4096");
            qp_init_attr.cap.max_send_wr     = 4096;
            qp_init_attr.cap.max_inline_data = max_inline_data;
        }

        qp_init_attr.cap.max_recv_wr  = SLIME_MAX_RECV_WR;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;
        qp_init_attr.sq_sig_all       = false;

        qp_[qpi] = ibv_create_qp(local_pool_->pd_, &qp_init_attr);
        if (!qp_[qpi]) {
            SLIME_LOG_ERROR("[" << ctx_->device_name_ << "] Failed to create QP ", qpi, ": ", strerror(errno));
            return -1;
        }

        /* Modify QP to INIT state */
        struct ibv_qp_attr attr = {};
        attr.qp_state           = IBV_QPS_INIT;
        attr.port_num           = ctx_->ib_port_;
        attr.pkey_index         = 0;
        attr.qp_access_flags    = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE;

        int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

        int ret = ibv_modify_qp(qp_[qpi], &attr, flags);
        if (ret) {
            SLIME_LOG_ERROR("Failed to modify QP to INIT");
        }

        /* Set Packet Sequence Number (PSN) */
        srand48(time(NULL));
        int32_t psn = lrand48() & 0xffffff;

        /* Get GID */
        if (ctx_->gidx_ != -1 && ibv_query_gid(ctx_->ib_ctx_, ctx_->ib_port_, ctx_->gidx_, &ctx_->gid_)) {
            SLIME_LOG_ERROR("[" << ctx_->device_name_ << "] Failed to get GID");
        }

        /* Set Local RDMA Info */
        local_rdma_info_[qpi].gidx = ctx_->gidx_;
        local_rdma_info_[qpi].qpn  = qp_[qpi]->qp_num;
        local_rdma_info_[qpi].psn  = psn;
        local_rdma_info_[qpi].gid  = ctx_->gid_;
        local_rdma_info_[qpi].lid  = ctx_->lid_;
        local_rdma_info_[qpi].mtu  = (uint32_t)ctx_->active_mtu_;
    }
    SLIME_LOG_INFO("RDMA context initialized")
    SLIME_LOG_DEBUG("RDMA context local configuration: ", channelInfo());

    // Observability: register NIC for transport-level post counters
    if (obs::obs_enabled()) {
        obs_nic_id_ = obs::obs_register_nic(ctx_->device_name_.c_str());
    }

    state = RDMAChannelState::Initialized;

    return 0;
}

json RDMAChannel::channelInfo() const
{
    json local_info{};
    for (int qpi = 0; qpi < local_rdma_info_.size(); qpi++)
        local_info[qpi] = local_rdma_info_[qpi].to_json();
    return local_info;
}

int32_t RDMAChannel::connect(json remote_rdma_info_json)
{
    SLIME_ASSERT(state == RDMAChannelState::Initialized, "Not Initialized or already connected");
    SLIME_ASSERT_EQ(local_rdma_info_.size(), remote_rdma_info_json.size(), "Peer must have same QP Size.");

    // construct RDMAEndpoint connection
    for (int qpi = 0; qpi < local_rdma_info_.size(); qpi++) {
        remote_rdma_info_[qpi] = rdma_info_t(remote_rdma_info_json[qpi]);
    }

    modify_qp_to_r2r();
    modify_qp_to_r2s();

    state = RDMAChannelState::Connected;
    if (ibv_req_notify_cq(ctx_->cq_, 0)) {
        SLIME_ABORT("Failed to request notify for CQ");
    }
    SLIME_LOG_INFO("RDMA exchange done");
    return 0;
}

int32_t RDMAChannel::modify_qp_to_r2r()
{
    for (int qpi = 0; qpi < local_rdma_info_.size(); qpi++) {
        int                ret;
        struct ibv_qp_attr attr = {};
        int                flags;
        struct ibv_qp*     qp = qp_[qpi];
        // Modify QP to Ready to Receive (RTR) state
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTR;
        attr.path_mtu =
            (enum ibv_mtu)std::min((uint32_t)remote_rdma_info_[qpi].mtu, (uint32_t)local_rdma_info_[qpi].mtu);

        attr.dest_qp_num           = remote_rdma_info_[qpi].qpn;
        attr.rq_psn                = remote_rdma_info_[qpi].psn;
        attr.max_dest_rd_atomic    = SLIME_MAX_DEST_RD_ATOMIC;
        attr.min_rnr_timer         = 0x12;
        attr.ah_attr.dlid          = remote_rdma_info_[qpi].lid;
        attr.ah_attr.sl            = SLIME_SERVICE_LEVEL;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num      = ctx_->ib_port_;

        attr.ah_attr.is_global = 0;
        attr.ah_attr.dlid      = 0;

        if (local_rdma_info_[qpi].gidx == -1) {
            // IB
            attr.ah_attr.dlid = remote_rdma_info_[qpi].lid;
        }
        else {
            // RoCE v2
            attr.ah_attr.is_global         = 1;
            attr.ah_attr.grh.dgid          = remote_rdma_info_[qpi].gid;
            attr.ah_attr.grh.sgid_index    = local_rdma_info_[qpi].gidx;
            attr.ah_attr.grh.hop_limit     = 1;
            attr.ah_attr.grh.flow_label    = 0;
            attr.ah_attr.grh.traffic_class = 0;
        }

        flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC
                | IBV_QP_MIN_RNR_TIMER;

        ret = ibv_modify_qp(qp, &attr, flags);
        if (ret) {
            SLIME_ABORT("Failed to modify QP to RTR: reason: " << strerror(ret));
        }
    }
    return 0;
}

int32_t RDMAChannel::modify_qp_to_r2s()
{
    for (int qpi = 0; qpi < local_rdma_info_.size(); qpi++) {
        int                ret;
        struct ibv_qp_attr attr = {};
        int                flags;
        struct ibv_qp*     qp = qp_[qpi];
        // Modify QP to RTS state
        memset(&attr, 0, sizeof(attr));
        attr.qp_state      = IBV_QPS_RTS;
        attr.timeout       = 14;
        attr.retry_cnt     = 7;
        attr.rnr_retry     = 7;
        attr.sq_psn        = local_rdma_info_[qpi].psn;
        attr.max_rd_atomic = SLIME_MAX_RD_ATOMIC;

        flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN
                | IBV_QP_MAX_QP_RD_ATOMIC;

        ret = ibv_modify_qp(qp, &attr, flags);
        if (ret) {
            SLIME_ABORT("Failed to modify QP to RTS");
        }
    }
    return 0;
}

int32_t RDMAChannel::destroy_qp(size_t qpi)
{
    if (!qp_[qpi])
        return 0;
    auto*     qp     = qp_[qpi];
    const int result = ibv_destroy_qp(qp);
    if (!result)
        qp_[qpi] = nullptr;
    return result;
}
int32_t RDMAChannel::reset()
{
    int status = 0;
    for (size_t i = 0; i < qp_.size(); ++i) {
        if (!qp_[i])
            continue;
        const auto number = qp_[i]->qp_num;
        if (!ctx_->completions_.quiesce(number, [&] { return destroy_qp(i); }))
            status = -1;
    }
    state = RDMAChannelState::Destroyed;
    return status;
}
int64_t RDMAChannel::post_send_batch(int qpi, RDMAAssign* assign, std::shared_ptr<RDMAMemoryPool> pool)
{
    return post_batch(qpi, assign, std::move(pool), false, false);
}
int64_t RDMAChannel::post_recv_batch(int qpi, RDMAAssign* assign, std::shared_ptr<RDMAMemoryPool> pool)
{
    return post_batch(qpi, assign, std::move(pool), true, false);
}
int64_t RDMAChannel::post_rc_oneside_batch(int qpi, RDMAAssign* assign, std::shared_ptr<RDMAMemoryPool> pool)
{
    return post_batch(qpi, assign, std::move(pool), false, true);
}
int64_t RDMAChannel::post_batch(
    int qpi, RDMAAssign* assign, std::shared_ptr<RDMAMemoryPool> pool, bool receiving, bool one_sided)
{
    // Snapshot the callback before posting. A CQE may recycle the assignment
    // slot before this function returns; no code below accesses it afterward.
    auto          callback    = std::move(assign->callback_);
    const auto    opcode      = assign->opcode_;
    const bool    inline_data = assign->is_inline_;
    const int32_t immediate   = assign->imm_data_;
    const auto    batch       = assign->batch_;
    const size_t  count       = batch.size();
    const auto    obs_opcode  = receiving ? obs::OBS_OP_RECV : obs::OPCODE_TO_OBS[static_cast<uint8_t>(opcode)];
    try {
        if (qpi < 0 || size_t(qpi) >= qp_.size() || !qp_[qpi] || !count
            || count > size_t(receiving ? SLIME_MAX_RECV_WR : SLIME_MAX_SEND_WR))
            throw std::invalid_argument("Invalid RDMA QP or batch size");
        std::vector<ibv_sge>     sge(count);
        std::vector<ibv_send_wr> sends(receiving ? 0 : count);
        std::vector<ibv_recv_wr> receives(receiving ? count : 0);
        uint64_t                 total_bytes = 0;
        for (size_t i = 0; i < count; ++i) {
            const auto& a = batch[i];
            auto* mr = a.mr_key < 1000000 ? pool->get_mr_fast(static_cast<int32_t>(a.mr_key)) : pool->get_mr(a.mr_key);
            if (!mr || a.source_offset > mr->length || a.length > mr->length - a.source_offset || a.length > UINT32_MAX
                || a.source_offset > UINTPTR_MAX - reinterpret_cast<uintptr_t>(mr->addr))
                throw std::invalid_argument("Invalid local RDMA range");
            sge[i].addr   = reinterpret_cast<uintptr_t>(mr->addr) + a.source_offset;
            sge[i].length = static_cast<uint32_t>(a.length);
            sge[i].lkey   = mr->lkey;
            total_bytes += a.length;
            if (receiving) {
                receives[i].sg_list = &sge[i];
                receives[i].num_sge = 1;
                receives[i].next    = i + 1 < count ? &receives[i + 1] : nullptr;
            }
            else {
                auto& wr  = sends[i];
                wr.opcode = ASSIGN_OP_2_IBV_WR_OP.at(opcode);
                if (i + 1 < count) {
                    if (wr.opcode == IBV_WR_RDMA_WRITE_WITH_IMM)
                        wr.opcode = IBV_WR_RDMA_WRITE;
                    if (wr.opcode == IBV_WR_SEND_WITH_IMM)
                        wr.opcode = IBV_WR_SEND;
                }
                wr.sg_list    = &sge[i];
                wr.num_sge    = 1;
                wr.send_flags = IBV_SEND_SIGNALED;
                if (inline_data && wr.opcode != IBV_WR_RDMA_READ)
                    wr.send_flags |= IBV_SEND_INLINE;
                wr.imm_data = htonl(static_cast<uint32_t>(immediate));
                wr.next     = i + 1 < count ? &sends[i + 1] : nullptr;
                if (one_sided) {
                    auto remote = remote_pool_->get_remote_mr_fast(static_cast<int32_t>(a.remote_mr_key));
                    if (!remote.addr || a.target_offset > remote.length || a.length > remote.length - a.target_offset
                        || a.target_offset > UINTPTR_MAX - remote.addr)
                        throw std::invalid_argument("Invalid remote RDMA range");
                    wr.wr.rdma.remote_addr = remote.addr + a.target_offset;
                    wr.wr.rdma.rkey        = remote.rkey;
                }
            }
        }
        auto*      qp     = qp_[qpi];
        const auto number = qp->qp_num;
        int        result;
        if (receiving) {
            result = ctx_->completions_.post(
                number,
                receives.data(),
                count,
                callback,
                pool,
                [qp](ibv_recv_wr* wr, ibv_recv_wr** bad) { return ibv_post_recv(qp, wr, bad); },
                [&] { return destroy_qp(qpi); });
        }
        else {
            result = ctx_->completions_.post(
                number,
                sends.data(),
                count,
                callback,
                pool,
                [qp](ibv_send_wr* wr, ibv_send_wr** bad) { return ibv_post_send(qp, wr, bad); },
                [&] { return destroy_qp(qpi); });
        }
        if (result) {
            obs::obs_record_post_failure(obs_nic_id_, obs_opcode);
            return -1;
        }
        if (obs::obs_enabled())
            obs::obs_record_post_batch(obs_nic_id_, obs_opcode, static_cast<uint32_t>(count), total_bytes);
        return 0;
    }
    catch (const std::exception& error) {
        SLIME_LOG_ERROR("RDMA batch validation/allocation failed: ", error.what());
        obs::obs_record_post_failure(obs_nic_id_, obs_opcode);
        if (callback)
            callback(RDMAAssign::FAILED, 0);
        return -1;
    }
}
}  // namespace dlslime
