#include <iostream>

#include "rdma_adapter.h"
#include "rdma_channel.h"
#include "rdma_buffer.h"

using namespace std;

namespace rdma{


string RDMA_MESSAGE(int code){
  switch(code){
    case RDMA_MESSAGE_ACK:
      return "RDMA_MESSAGE_ACK";
      break;
    case RDMA_MESSAGE_BUFFER_IDLE:
      return "RDMA_MESSAGE_BUFFER_IDLE";
      break;
    case RDMA_MESSAGE_BUFFER_REQUEST:
      return "RDMA_MESSAGE_BUFFER_REQUEST";
      break;
    case RDMA_MESSAGE_BUFFER_RESPONSE:
      return "RDMA_MESSAGE_BUFFER_RESPONSE";
      break;
    case RDMA_MESSAGE_DATA_REQUEST:
      return "RDMA_MESSAGE_DATA_REQUEST";
      break;
    case RDMA_MESSAGE_DATA_WRITE:
      return "RDMA_MESSAGE_DATA_WRITE";
      break;
    default:
      return "unknown op code";
      break;
  }
}

string RDMA_OPCODE(int op){
  switch(op){
    case IBV_WC_RECV_RDMA_WITH_IMM:
      return "IBV_WC_RECV_RDMA_WITH_IMM";
      break;
    case IBV_WC_RDMA_WRITE:
      return "IBV_WC_RDMA_WRITE";
      break;
    default:
      return "unknown op code";
      break;
  }
  return "";
}



ibv_context* open_default_device() {
  ibv_device** dev_list;
  ibv_device* ib_dev;
  ibv_context* context;
  int num_devices;
  dev_list = ibv_get_device_list(&num_devices);
  CHECK(dev_list) << "No InfiniBand device found";
  LOG_DEBUG << "\033[;36mibv_get_device_list\033[0m devices num:" << num_devices;
  for (int i=0; i<num_devices; i++){
    ib_dev = dev_list[i];
    CHECK(ib_dev) << "No InfiniBand device found";
    
    LOG_DEBUG << "device " << i;
    LOG_DEBUG << "  ib_dev.transport_type:" << ib_dev->transport_type;
    LOG_DEBUG << "  ib_dev.dev_name:" << ib_dev->dev_name;
    LOG_DEBUG << "  ib_dev.dev_path:" << ib_dev->dev_path;
    LOG_DEBUG << "  ib_dev.ibdev_path:" << ib_dev->ibdev_path;
    LOG_DEBUG << "  ibv_get_device_name:" << ibv_get_device_name(ib_dev);
    LOG_DEBUG << "  ibv_get_device_guid:" << ibv_get_device_guid(ib_dev);
    LOG_DEBUG << "  ibv_node_type_str:" << ibv_node_type_str(ib_dev->node_type);
    
    context = ibv_open_device(ib_dev);
    CHECK(context) << "Open context failed for " << ibv_get_device_name(ib_dev);
    ibv_device_attr device_attr;
    ibv_query_device(context, &device_attr);
    
    LOG_DEBUG << "\033[;36mibv_open_device\033[0m, get context.";
    LOG_DEBUG << "ibv_query_device, get device_attr.";
    LOG_DEBUG << "  ibv_device_attr.fw_ver:" << device_attr.fw_ver;
    LOG_DEBUG << "  ibv_device_attr.node_guid:" << device_attr.node_guid;
    LOG_DEBUG << "  ibv_device_attr.sys_image_guid:" << device_attr.sys_image_guid;
    LOG_DEBUG << "  ibv_device_attr.max_mr_size:" << device_attr.max_mr_size;
    LOG_DEBUG << "  ibv_device_attr.page_size_cap:" << device_attr.page_size_cap;
    LOG_DEBUG << "  ibv_device_attr.vendor_id:" << device_attr.vendor_id;
    LOG_DEBUG << "  ibv_device_attr.vendor_part_id:" << device_attr.vendor_part_id;
    LOG_DEBUG << "  ibv_device_attr.hw_ver:" << device_attr.hw_ver;
    LOG_DEBUG << "  ibv_device_attr.max_qp:" << device_attr.max_qp;
    LOG_DEBUG << "  ibv_device_attr.max_qp_wr:" << device_attr.max_qp_wr;
    LOG_DEBUG << "  ibv_device_attr.device_cap_flags:" << device_attr.device_cap_flags;
    LOG_DEBUG << "  ibv_device_attr.max_sge:" << device_attr.max_sge;
    LOG_DEBUG << "  ibv_device_attr.max_sge_rd:" << device_attr.max_sge_rd;
    LOG_DEBUG << "  ibv_device_attr.max_cq:" << device_attr.max_cq;
    LOG_DEBUG << "  ibv_device_attr.max_cqe:" << device_attr.max_cqe;
    LOG_DEBUG << "  ibv_device_attr.max_mr:" << device_attr.max_mr;
    LOG_DEBUG << "  ibv_device_attr.max_pd:" << device_attr.max_pd;
    LOG_DEBUG << "  ibv_device_attr.max_qp_rd_atom:" << device_attr.max_qp_rd_atom;
    LOG_DEBUG << "  ibv_device_attr.max_ee_rd_atom:" << device_attr.max_ee_rd_atom;
    LOG_DEBUG << "  ibv_device_attr.max_res_rd_atom:" << device_attr.max_res_rd_atom;
    LOG_DEBUG << "  ibv_device_attr.max_qp_init_rd_atom:" << device_attr.max_qp_init_rd_atom;
    LOG_DEBUG << "  ibv_device_attr.max_ee_init_rd_atom:" << device_attr.max_ee_init_rd_atom;
    LOG_DEBUG << "  ibv_device_attr.atomic_cap:" << device_attr.atomic_cap;
    LOG_DEBUG << "  ibv_device_attr.max_ee:" << device_attr.max_ee;
    LOG_DEBUG << "  ibv_device_attr.max_rdd:" << device_attr.max_rdd;
    LOG_DEBUG << "  ibv_device_attr.max_mw:" << device_attr.max_mw;
    LOG_DEBUG << "  ibv_device_attr.max_raw_ipv6_qp:" << device_attr.max_raw_ipv6_qp;
    LOG_DEBUG << "  ibv_device_attr.max_raw_ethy_qp:" << device_attr.max_raw_ethy_qp;
    LOG_DEBUG << "  ibv_device_attr.max_mcast_grp:" << device_attr.max_mcast_grp;
    LOG_DEBUG << "  ibv_device_attr.max_mcast_qp_attach:" << device_attr.max_mcast_qp_attach;
    LOG_DEBUG << "  ibv_device_attr.max_total_mcast_qp_attach:" << device_attr.max_total_mcast_qp_attach;
    LOG_DEBUG << "  ibv_device_attr.max_ah:" << device_attr.max_ah;
    LOG_DEBUG << "  ibv_device_attr.max_fmr:" << device_attr.max_fmr;
    LOG_DEBUG << "  ibv_device_attr.max_map_per_fmr:" << device_attr.max_map_per_fmr;
    LOG_DEBUG << "  ibv_device_attr.max_srq:" << device_attr.max_srq;
    LOG_DEBUG << "  ibv_device_attr.max_srq_wr:" << device_attr.max_srq_wr;
    LOG_DEBUG << "  ibv_device_attr.max_srq_sge:" << device_attr.max_srq_sge;
    LOG_DEBUG << "  ibv_device_attr.max_pkeys:" << device_attr.max_pkeys;
    LOG_DEBUG << "  ibv_device_attr.local_ca_ack_delay:" << (int)device_attr.local_ca_ack_delay;
    LOG_DEBUG << "  ibv_device_attr.phys_port_cnt:" << (int)device_attr.phys_port_cnt;
    return context;
  }
  return context;
}

ibv_pd* alloc_protection_domain(ibv_context* context) {
  ibv_pd* pd = ibv_alloc_pd(context);
  LOG_DEBUG << "\033[;36mibv_alloc_pd\033[0m by context.(Protection Domain)";
  CHECK(pd) << "Failed to allocate protection domain";
  return pd;
}


RdmaAdapter::RdmaAdapter()
      : context_(open_default_device()),pd_(alloc_protection_domain(context_)){
  event_channel_ = ibv_create_comp_channel(context_);
  LOG_DEBUG << "\033[;36mibv_create_comp_channel\033[0m by context. (Completion Channel)(CC is a mechanism for the user to receive notifications when a new CQE arrive CQ)";
	CHECK(event_channel_) << "Failed to create completion channel";
  cq_ = ibv_create_cq(context_, MAX_CONCURRENT_WRITES * 2, NULL, event_channel_, 0);
  LOG_DEBUG << "\033[;36mibv_create_cq\033[0m by (context,completion channel). (channel is used to specify a CC, for notice)";
	CHECK(cq_) << "Failed to create completion queue";
	CHECK(!ibv_req_notify_cq(cq_, 0)) << "Failed to request CQ notification";
  LOG_DEBUG << "\033[;36mibv_req_notify_cq\033[0m. (when a CQE come to CQ, a completion event will be sent to CC)";
  thread_ = new Thread([this] {Process_CQ(); }); 
  LOG_DEBUG << "thread begin";
}

RdmaAdapter::~RdmaAdapter() {
  CHECK(!ibv_destroy_cq(cq_)) << "Failed to destroy CQ";
  CHECK(!ibv_destroy_comp_channel(event_channel_)) << "Failed to destroy channel";
  CHECK(!ibv_dealloc_pd(pd_)) << "Failed to deallocate PD";
  CHECK(!ibv_close_device(context_)) << "Failed to release context";
  delete thread_;
}

void RdmaAdapter::Process_CQ() {
  while (true) {
    LOG_DEBUG << "While begin.";

    ibv_cq* cq;
    void* cq_context;
    CHECK(!ibv_get_cq_event(event_channel_, &cq, &cq_context));
    CHECK(cq == cq_);
    ibv_ack_cq_events(cq, 1);
    CHECK(!ibv_req_notify_cq(cq_, 0));
    int ne = ibv_poll_cq(cq_, MAX_CONCURRENT_WRITES * 2,static_cast<ibv_wc*>(wc_));
    LOG_DEBUG << "ne:" << ne;
    if (ne < 0){
      LOG_WARNING << "Process_CQ:ne < 0";
    } 
    for (int i = 0; i < ne; ++i) {
       CHECK(wc_[i].status == IBV_WC_SUCCESS) << "Failed status \n"
                                              << ibv_wc_status_str(wc_[i].status)
                                              << " " << wc_[i].status << " "
                                              << static_cast<int>(wc_[i].wr_id)
                                              << " "<< wc_[i].vendor_err; 
      LOG_DEBUG << "OP:\033[;36m" << RDMA_OPCODE(wc_[i].opcode).c_str() << "\033[0m";
      //接收端收到信息
      if (wc_[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM) { 
          RdmaChannel* rc = reinterpret_cast<RdmaChannel*>(wc_[i].wr_id);
          rc->Recv();
          uint32_t imm_data = wc_[i].imm_data;
          RdmaBuffer* rb = rc->FindBuffer(imm_data);
          LOG_DEBUG << "RDMA Buffer name:" << rb->name_.c_str();
          RdmaMessage rm;
          RdmaMessage::ParseMessage(rm, rb->buffer_);

        LOG_DEBUG << "RDMA Message type:\033[;36m" << RDMA_MESSAGE(rm.type_).c_str() << "\033[0m, name:" << rm.name_.c_str();
        if (rm.type_ == RDMA_MESSAGE_ACK) { 
          // 对端消息接收buffer空闲，发送下一条”消息“
          rb = rc->tx_message_buffer_;
          rb->SetBufferStatus(remote, idle);
          rb->SendNextItem();
        } else if (rm.type_ == RDMA_MESSAGE_DATA_REQUEST) {
          //接收端申请发送数据
          RdmaBuffer* ab = rc->tx_ack_buffer_;
          ab->SendNextItem();

          rc->SetRecvReady();
          // "tx_data_buffer"
          RdmaBuffer* tb = rc->FindOrCreateBuffer(rm.name_);
          tb->SendNextItem();
        } else if (rm.type_ == RDMA_MESSAGE_BUFFER_IDLE) { 
          // 对端数据接收buffer空闲，发送下一条”数据“
          RdmaBuffer* ab = rc->tx_ack_buffer_;
          ab->SendNextItem();
          // find buffer
          RdmaBuffer* tb = rc->FindBuffer(rm.name_);
          tb->SetBufferStatus(remote, idle);
          tb->SendNextItem();
        } else if (rm.type_ == RDMA_MESSAGE_BUFFER_REQUEST) {
          // 当前注册buffer比要传输的数据小，重新注册本地buffer并发送新buffer信息到对端
          // 同时请求对端重新注册buffer
          RdmaBuffer* ab = rc->tx_ack_buffer_;
          ab->SendNextItem();
          // 重新注册本地buffer "rx_data_buffer_"
          RdmaBuffer* tb = rc->FindOrCreateBuffer(rm.name_);
          RemoteMR rmr;
          rmr.remote_addr = rm.remote_addr_;
          rmr.rkey = rm.rkey_;
          tb->SetRemoteMR(rmr);
          tb->CreateCPUBuffer(rm.buffer_size_);
          // 
          string key = "tx_data_buffer";
          RdmaMessage br;
          br.type_ = RDMA_MESSAGE_BUFFER_RESPONSE;
          br.name_size_ = key.size();
          br.name_ = key;
          br.buffer_size_ = rm.buffer_size_;
          br.remote_addr_ = reinterpret_cast<uint64_t>(tb->buffer_);
          br.rkey_ = tb->self_->rkey;
          string message = RdmaMessage::CreateMessage(br);
          RdmaBuffer* mb = rc->tx_message_buffer_;
          LOG_DEBUG << "Message(name:tx_data_buffer,type:RDMA_MESSAGE_BUFFER_RESPONSE) push to tx_message_buffer.";
          mb->EnqueueItem(message);
          mb->SendNextItem();
        } else if (rm.type_ == RDMA_MESSAGE_BUFFER_RESPONSE) {
          RdmaBuffer* ab = rc->tx_ack_buffer_;
          ab->SendNextItem();
          // "tx_data_buffer_"
          RdmaBuffer* tb = rc->FindBuffer(rm.name_);
          CHECK(rm.buffer_size_ == tb->size_) 
                  << "rm.buffer_size = " << rm.buffer_size_
                  << "tb->size_ = " << tb->size_ 
                  << "rm.name_ = " << rm.name_; 
          RemoteMR rmr;
          rmr.remote_addr = rm.remote_addr_;
          rmr.rkey = rm.rkey_;
          tb->SetRemoteMR(rmr);
          tb->SetBufferStatus(local, idle);
          tb->SetBufferStatus(remote, idle);
          tb->SendNextItem();
        } else if (rm.type_ == RDMA_MESSAGE_DATA_WRITE) {
          //发送端发送数据到本地，本地调用接收回调函数。
          rc->RunRecvCallback();
          // create message
      	  string key = "tx_data_buffer";
          RdmaMessage br;
      	  br.type_ = RDMA_MESSAGE_BUFFER_IDLE;
      	  br.name_size_ = key.size();
      	  br.name_ = key;
      	  string message = RdmaMessage::CreateMessage(br);
      	  RdmaBuffer* tb = rc->tx_message_buffer_;
          LOG_DEBUG << "Message(name:tx_data_buffer,type:RDMA_MESSAGE_BUFFER_IDLE) push to tx_message_buffer.";
      	  tb->EnqueueItem(message); 
      	  tb->SendNextItem();
      	}
       //发送端收到消息发送完成信息
      } else if (wc_[i].opcode == IBV_WC_RDMA_WRITE) { 
          //设置本地”*“buffer空闲，发送下一个消息或数据
          RdmaBuffer* rb = reinterpret_cast<RdmaBuffer*>(wc_[i].wr_id);
          rb->SetBufferStatus(local, idle);
          LOG_DEBUG << "RDMA Buffer name:" << rb->name_.c_str() << ", set local to idle.";
          RdmaMessage rm;
          RdmaMessage::ParseMessage(rm, rb->buffer_);
          LOG_DEBUG << "RDMA Message type:\033[;36m" << RDMA_MESSAGE(rm.type_).c_str() << "\033[0m";
          if (rm.type_ != RDMA_MESSAGE_ACK) {
            rb->SendNextItem();
          }
      }
    }
  }
}

}
