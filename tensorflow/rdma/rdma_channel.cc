#include <unistd.h>
#include <random>
#include <string>
#include <iostream>
#include <functional>
#include "rdma_channel.h"
#include "rdma_buffer.h"
using namespace std;

namespace rdma{

typedef unsigned long long uint64;

std::mt19937_64* InitRng() {
  std::random_device device("/dev/urandom");
  return new std::mt19937_64(device());
}

uint64 New64() {
  static std::mt19937_64* rng = InitRng();
  return (*rng)();
}

RdmaChannel::RdmaChannel(const RdmaAdapter* adapter, const string remote_name)
            :adapter_(adapter),
      	     remote_name_(remote_name),
             connected_(false),
             isReady_(false)
	{
		LOG_DEBUG << "RdmaChannel Construct";
		//创建QP
		{
			struct ibv_qp_init_attr attr;
			memset(&attr,0,sizeof(ibv_qp_init_attr));
			attr.send_cq = adapter_->cq_;
			attr.recv_cq = adapter_->cq_;
			attr.cap.max_send_wr = RdmaAdapter::MAX_CONCURRENT_WRITES;
			attr.cap.max_recv_wr = RdmaAdapter::MAX_CONCURRENT_WRITES;
			attr.cap.max_send_sge = 1;
			attr.cap.max_recv_sge = 1;
			attr.qp_type = IBV_QPT_RC;

			qp_ = ibv_create_qp(adapter_->pd_, &attr);
		    CHECK(qp_) << "Failed to create queue pair";	

		    LOG_DEBUG << "\033[;36mibv_create_qp\033[0m by (pd,attr)";
		}
	    //初始化QP
		{
			struct ibv_qp_attr attr;
			memset(&attr, 0, sizeof(ibv_qp_attr));
			attr.qp_state = IBV_QPS_INIT;
			attr.pkey_index = 0;
			attr.port_num = 1; //这里默认使用第一个物理口
			attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;

			int mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
		    CHECK(!ibv_modify_qp(qp_, &attr, mask)) << "Failed to set QP to INIT";	
		    LOG_DEBUG << "\033[;36mibv_modify_qp\033[0m by (qp_,attr,mask)";
		}
		//获取本地IB口地址信息用于建立RDMA连接
		{
			struct ibv_port_attr attr;
			ibv_query_port(adapter_->context_, (uint8_t) 1, &attr); 
		    CHECK(!ibv_query_port(adapter_->context_, (uint8_t) 1, &attr)) << "Query port";	
			self_.lid = attr.lid;
			self_.qpn = qp_->qp_num;
			self_.psn = static_cast<uint32_t>(New64()) & 0xffffff;			
		    LOG_DEBUG << "\033[;36mibv_query_port\033[0m by (context_,port_num(1(first))), self_.lid:" << self_.lid;
		}

			//初始化buffer
		{
    		const string buffer_names[] = {"tx_message_buffer", "rx_message_buffer", "tx_ack_buffer", "rx_ack_buffer"};
			tx_message_buffer_ = new RdmaMessageBuffer(this, buffer_names[0]);
			rx_message_buffer_ = new RdmaMessageBuffer(this, buffer_names[1]);
			tx_ack_buffer_ = new RdmaAckBuffer(this, buffer_names[2]);
			rx_ack_buffer_ = new RdmaAckBuffer(this, buffer_names[3]);
			message_buffers_.reserve(kNumMessageBuffers);
			message_buffers_.push_back(tx_message_buffer_);
			message_buffers_.push_back(rx_message_buffer_);
			message_buffers_.push_back(tx_ack_buffer_);
			message_buffers_.push_back(rx_ack_buffer_);
			// 
			tx_message_buffer_->CreateCPUBuffer(RdmaMessage::kRdmaMessageBufferSize);
			rx_message_buffer_->CreateCPUBuffer(RdmaMessage::kRdmaMessageBufferSize);
			tx_ack_buffer_->CreateCPUBuffer(RdmaMessage::kRdmaAckBufferSize);
			rx_ack_buffer_->CreateCPUBuffer(RdmaMessage::kRdmaAckBufferSize);
			//
			for (int i = 0; i < kNumMessageBuffers; i++) {
				uint32_t index = str_hash(buffer_names[i]);
				LOG_DEBUG << "Self Buffer Name:" << buffer_names[i].c_str() << ", Hash Index:" << index;
				buffer_table_.insert({index, message_buffers_[i]});
				buffer_index_name_table_.insert({index, buffer_names[i]});
				buffer_name_index_table_.insert({buffer_names[i], index});
    		}
			//"初始化" Recv Queue 
			for (int i = 0; i < 100; i++) {
				Recv();
			}
		}
	}

RdmaChannel::~RdmaChannel(){
	CHECK(!ibv_destroy_qp(qp_)) << "Failed to destroy QP";
	delete tx_message_buffer_;
	delete rx_message_buffer_;
	delete tx_ack_buffer_;
	delete rx_ack_buffer_;
}

void RdmaChannel::SetRemoteAddress(RdmaAddress ra) {
      mux_.lock();
      remote_.lid = ra.lid;
      remote_.qpn = ra.qpn;
      remote_.psn = ra.psn;
      mux_.unlock();
}

void RdmaChannel::Recv() {
	struct ibv_recv_wr wr;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = (uint64_t) this; //将当前RdmaChanel对象指针放入Recv Queue,用于回调指定连接的函数
	struct ibv_recv_wr* bad_wr;
	CHECK(!ibv_post_recv(qp_, &wr, &bad_wr)) << "Failed to post recv";
}

uint32_t RdmaChannel::LookupBufferIndex(const string& buffer_name){
	LOG_DEBUG << "Find Lookup Buffer Index by Name:" << buffer_name.c_str();
	mux_.lock();
	BufferNameIndexTable::iterator iter = buffer_name_index_table_.find(buffer_name);
	CHECK(iter != buffer_name_index_table_.end());
	mux_.unlock();
	return iter->second;
}

RdmaBuffer* RdmaChannel::FindBuffer(const uint32_t index) {
	LOG_DEBUG << "Find Buffer by Index:" << index;
	mux_.lock();
	BufferTable::iterator iter = buffer_table_.find(index);
	CHECK(iter != buffer_table_.end());
	mux_.unlock();
	return iter->second;
}

RdmaBuffer* RdmaChannel::FindBuffer(const string& name) {
	LOG_DEBUG << "Find Buffer by Name:" << name.c_str();
	uint32_t index = LookupBufferIndex(name);
	return FindBuffer(index);
}

RdmaBuffer* RdmaChannel::FindOrCreateBuffer(const string& name) {
	RdmaBuffer* rb;
	mux_.lock();
	// find index
	BufferNameIndexTable::iterator iter = buffer_name_index_table_.find(name);
	if (iter != buffer_name_index_table_.end()) {
		uint32_t index = iter->second;
		// find buffer
		BufferTable::iterator iter = buffer_table_.find(index);
		CHECK(iter != buffer_table_.end());
		rb = iter->second;
		LOG_DEBUG << "Find or Create Buffer by Name:" << name.c_str() << ". Found.";
	} else {
		uint32_t index = str_hash(name);
		rb = new RdmaDataBuffer(this, name);
		buffer_name_index_table_.insert({name, index});
		buffer_index_name_table_.insert({index, name});
		buffer_table_.insert({index, rb});
		LOG_DEBUG << "Find or Create Buffer by Name:" << name.c_str() << ". Created.";
	}
	mux_.unlock();
	return rb;
}
//接收端Receive触发
void RdmaChannel::RegisterRecvCallback(std::function<void(void*,int)> recv_done) {
	//todo: lock
	recvcallback_ = recv_done;
	//todo: lock
}

//DATA_WRITE触发
void RdmaChannel::RunRecvCallback() {
    RdmaBuffer* rb = FindBuffer(str_hash("rx_data_buffer"));
    RdmaMessage rm;
	mux_.lock();
	CHECK(rb->size_ >= RdmaMessage::kMessageTotalBytes) << "RunRecvCallback ERROR; Wrong size !";
    RdmaMessage::ParseMessage(rm, rb->buffer_);
    CHECK(rm.type_ == RDMA_MESSAGE_DATA_WRITE) << "RunRecvCallback ERROR; Wrong Message !";

    void* val = malloc(rm.data_bytes_);
	void* input = static_cast<void*>(static_cast<char*>(rb->buffer_) + RdmaMessage::kDataBufferStartIndex);

	CHECK(rm.data_bytes_ + RdmaMessage::kDataBufferStartIndex <= rb->size_) << "RunRecvCallback ERROR; buffer too small !";

    //取出数据
    memcpy(val,input,rm.data_bytes_);
 	mux_.unlock();
	//触发接收回调
	recvcallback_(val,rm.data_bytes_);
}



void RdmaChannel::RecvRequestData(){
  	RdmaBuffer* rb = tx_message_buffer_;
  	RdmaMessage rm;
  	string key = "tx_data_buffer";
  	rm.type_ = RDMA_MESSAGE_DATA_REQUEST;
  	rm.name_size_ = key.size();
  	rm.name_ = key;
  	string message = RdmaMessage::CreateMessage(rm);
  	rb->EnqueueItem(message);
  	LOG_DEBUG << "Message(name:tx_data_buffer,type:RDMA_MESSAGE_DATA_REQUEST) push to tx_message_buffer Queue";
  	rb->SendNextItem();
}

void RdmaChannel::EnqueueData(Item* item){
	mux_.lock();
	queue_.push(item);
	mux_.unlock();
}

void RdmaChannel::InnerSend(const void* data,int size){
	void* val = malloc(size);
	memcpy(val,data,size);

	auto item = new Item;
	item->data = val;
	item->size = size;
	EnqueueData(item);
	LOG_DEBUG << "Is Remote Recv Ready? " << IsRecvReady();
	if(IsRecvReady()){
    	RdmaBuffer* tb = FindOrCreateBuffer("tx_data_buffer");
    	tb->SendNextItem();
  	}
}

void RdmaChannel::Connect(RdmaAddress& remoteAddr) {
	mux_.lock();
	if (!connected_) {
		struct ibv_qp_attr attr;
		memset(&attr, 0, sizeof(ibv_qp_attr));

		attr.qp_state = IBV_QPS_RTR;
		attr.path_mtu = IBV_MTU_4096;
		attr.dest_qp_num = remoteAddr.qpn;
		attr.rq_psn = remoteAddr.psn;
		attr.max_dest_rd_atomic = 1;
		attr.min_rnr_timer = 12;
		attr.ah_attr.is_global = 0;
		attr.ah_attr.dlid = remoteAddr.lid;
		attr.ah_attr.sl = 0;
		attr.ah_attr.src_path_bits = 0;
		attr.ah_attr.port_num = 1;

		LOG_DEBUG << "\033[;36mibv_modify_qp\033[0m by (qp_,attr,mask) RTR";
		int r;
		CHECK(!(r = ibv_modify_qp(qp_, &attr,
			IBV_QP_STATE |
			IBV_QP_AV |
			IBV_QP_PATH_MTU |
			IBV_QP_DEST_QPN |
			IBV_QP_RQ_PSN |
			IBV_QP_MAX_DEST_RD_ATOMIC |
			IBV_QP_MIN_RNR_TIMER))) << "QP to Ready to Receive " << r;
  
		memset(&attr, 0, sizeof(ibv_qp_attr));
		attr.qp_state = IBV_QPS_RTS;
		attr.sq_psn = self_.psn;
		attr.timeout = 14;
		attr.retry_cnt = 7;
		attr.rnr_retry = 7; /* infinite */
		attr.max_rd_atomic = 1;

		LOG_DEBUG << "\033[;36mibv_modify_qp\033[0m by (qp_,attr,mask) RTS";
		CHECK(!(r = ibv_modify_qp(qp_, &attr,
			IBV_QP_STATE |
			IBV_QP_TIMEOUT |
			IBV_QP_RETRY_CNT |
			IBV_QP_RNR_RETRY |
			IBV_QP_SQ_PSN |
			IBV_QP_MAX_QP_RD_ATOMIC))) << "QP to Ready to Send " << r;
		    
		connected_ = true;
	} else {
		LOG_INFO << "channel already connected";
	}
	mux_.unlock();	
}
}
