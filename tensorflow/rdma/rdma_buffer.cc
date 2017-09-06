#include <string>
#include <iostream>
#include "rdma.h"
#include "rdma_buffer.h"

using namespace std;

namespace rdma{

RdmaBuffer::RdmaBuffer(RdmaChannel* channel,string name)
	   :channel_(channel),name_(name) {}

RdmaBuffer::~RdmaBuffer(){
	CHECK(!ibv_dereg_mr(self_)) << "ibv_dereg_mr failed";
	FreeBuffer();
}

void RdmaBuffer::FreeBuffer(){
	if ((buffer_ != nullptr)) {
		free(buffer_);
	}
}

void RdmaBuffer::CreateCPUBuffer(size_t size){
	CHECK(size > 0);	
	mux_.lock();
	if(local_status_ != none){
		CHECK(!ibv_dereg_mr(self_)) << "ibv_dereg_mr failed";
    LOG_DEBUG << "\033[;36mibv_dereg_mr\033[0m";
		FreeBuffer();
	}
	size_ = size;
	buffer_ = malloc(size_);
	self_ = ibv_reg_mr(channel_->adapter_->pd_,buffer_,size_,IBV_ACCESS_LOCAL_WRITE |IBV_ACCESS_REMOTE_WRITE);
  LOG_DEBUG << "\033[;36mibv_reg_mr\033[0m by (pd,buffer:" << buffer_ << ",size:" << size << ",mask)";
  CHECK(self_) << "Failed to register memory region";	
	local_status_ = idle;
	mux_.unlock();
}

void RdmaBuffer::SetRemoteMR(RemoteMR rmr) {
	mux_.lock();
	remote_.remote_addr = rmr.remote_addr;
	remote_.rkey = rmr.rkey;
	remote_status_ = idle;
	mux_.unlock();
}

void RdmaBuffer::EnqueueItem(std::string item){
	mux_.lock();
	queue_.push(item);
	mux_.unlock();
}

void RdmaBuffer::Write(uint32_t imm_data, size_t buffer_size) {
  
	struct ibv_sge list;
	list.addr = (uint64_t) buffer_;
	list.length = buffer_size;
	list.lkey = self_->lkey;

	struct ibv_send_wr wr;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = (uint64_t) this; 
	wr.sg_list = &list;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.imm_data = imm_data; 
	wr.wr.rdma.remote_addr = (uint64_t) remote_.remote_addr;
	wr.wr.rdma.rkey = remote_.rkey;

	struct ibv_send_wr *bad_wr;
	CHECK(!ibv_post_send(channel_->qp_, &wr, &bad_wr)) << "Failed to post send";
  LOG_DEBUG << "\033[;36mibv_post_send\033[0m by (qp,wr), imm_data:" << wr.imm_data << " wr_id: " << wr.wr_id;
}

RdmaAckBuffer::RdmaAckBuffer(RdmaChannel* channel, string name)
           : RdmaBuffer(channel, name) {}
    
RdmaMessageBuffer::RdmaMessageBuffer(RdmaChannel* channel, string name)
           : RdmaBuffer(channel, name) {}

RdmaDataBuffer::RdmaDataBuffer(RdmaChannel* channel, string name)
           : RdmaBuffer(channel, name) {}

void RdmaAckBuffer::SendNextItem() {
  uint32_t imm_data = channel_->LookupBufferIndex("rx_ack_buffer");
  RdmaMessage rm;
  rm.name_ = "rx_ack_buffer";
  rm.type_ = RDMA_MESSAGE_ACK;
  rm.name_size_ = rm.name_.size();
  string message = RdmaMessage::CreateMessage(rm);
  memcpy(buffer_, message.data(), message.size());
  LOG_DEBUG << "AckBuffer: Message(name:rx_ack_buffer,type:RDMA_MESSAGE_ACK) Write to remote rx_ack_buffer(imm_data:" << imm_data << ")";
  Write(imm_data, message.size());
}

void RdmaMessageBuffer::SendNextItem() {
  mux_.lock();
  uint32_t imm_data = channel_->LookupBufferIndex("rx_message_buffer");
  if (!queue_.empty() && (local_status_ == idle) && (remote_status_ == idle)) {
    local_status_ = busy;
    remote_status_= busy;    
    string message = queue_.front();
    queue_.pop();
    mux_.unlock();
    memcpy(buffer_, message.data(), message.size());
    LOG_DEBUG << "MessageBuffer: Pop a Message from Queue, Write to rx_message_buffer(imm_data:" << imm_data << ")";
    Write(imm_data, message.size());
  } else{
    LOG_DEBUG << "MessageBuffer: Nothing to Send or Buffer is not Idle. (imm_data:" << imm_data << ")";
    mux_.unlock();
  }
}

void RdmaDataBuffer::SendNextItem() {
  LOG_DEBUG << "DataBuffer: Is Empty? " << channel_->IsDataQueueEmpty();
  if (!channel_->IsDataQueueEmpty()) {
      size_t buffer_size = RdmaMessage::kMessageTotalBytes;

      auto item = channel_->GetDataFromQueue();
      buffer_size += item->size;
      RdmaMessage rm;
      string key = "rx_data_buffer";
      rm.name_size_ = key.size();
      rm.name_ = key;
      rm.data_bytes_ = item->size;
      rm.buffer_size_ = buffer_size;
      mux_.lock();
      if (local_status_ == none || (buffer_size > size_ && local_status_ == idle && remote_status_ == idle)) {
	       mux_.unlock();
        //当前注册存储区过小，重新注册
        CreateCPUBuffer(buffer_size);
        //需要重新注册存储区，放回发送队列，等待重传 
        channel_->EnqueueData(item);
        //请求对端注册相同大小的存储区用于接收，并将本地刚注册的存储区信息发送给对端，并等待对端的RESPONSE消息开始数据传输
        rm.type_ = RDMA_MESSAGE_BUFFER_REQUEST;
        rm.remote_addr_ = reinterpret_cast<uint64_t>(buffer_);
        rm.rkey_ = self_->rkey;
        string message = RdmaMessage::CreateMessage(rm);
        LOG_DEBUG << "DataBuffer: Buffer size is not enough.";
        LOG_DEBUG << "DataBuffer: Message(name:" << key << ",type:RDMA_MESSAGE_BUFFER_REQUEST) push to tx_message_buffer Queue";
        channel_->tx_message_buffer_->EnqueueItem(message);
        channel_->tx_message_buffer_->SendNextItem();      
      } else if((local_status_ == idle) && (remote_status_ == idle)) {
        // 设置标志位busy并开始发送数据
        local_status_ = busy;
        remote_status_ = busy;
        mux_.unlock();
        uint32_t imm_data = str_hash(key);
        rm.type_ = RDMA_MESSAGE_DATA_WRITE;
        string message = RdmaMessage::CreateMessage(rm);
        //填入消息
        memcpy(buffer_, message.data(), message.size());
        //填入数据
        void* output = static_cast<void*>(static_cast<char*>(buffer_) + RdmaMessage::kDataBufferStartIndex);
        CHECK(item->size + RdmaMessage::kDataBufferStartIndex <=  size_);
        //将data序列化放入buffer_
        memcpy(output, item->data, item->size);
        delete item;
        LOG_DEBUG << "DataBuffer: Message(name:" << key << ",type:RDMA_MESSAGE_DATA_WRITE) Write to remote " << key << "(imm_data:" << imm_data << ")";
        LOG_DEBUG << "DataBuffer: Total Size:" << buffer_size;
        Write(imm_data, buffer_size);

      }else{
	       mux_.unlock();
          LOG_DEBUG << "DataBuffer: Pop a Data to Queue";
          channel_->EnqueueData(item);
      } 
  }
}

string RdmaMessage::CreateMessage(const RdmaMessage& rm) {

  // type|name_size|name|buffer_size|remote_addr|rkey|data_bytes
  //   1B|    2B   | 512|    8B     |       8B  | 4B |    8B    
 
  // Rdma 消息类型
  // ACK:             type|name_size|buffer_name
  // DATA_REQUEST:    type|name_size|data_name
  // DATA_WRITE:      type|name_size|data_name|data_bytes
  // BUFFER_IDLE:     type|name_size|buffer_name
  // BUFFER_REQUEST:  type|name_size|buffer_name|buffer_size|remote_addr|rkey
  // BUFFER_RESPONSE: type|name_size|buffer_name|buffer_size|remote_addr|rkey
	
  //消息大小固定，未使用的位依然会被传输，但不解析。
  char message[kMessageTotalBytes];
  // 消息类型: RDMA_MESSAGE_ACK、RDMA_MESSAGE_BUFFER_IDLE...
  message[kTypeStartIndex] = static_cast<char>(rm.type_) & 0xff;
  // 消息名称大小:"tx_message_buffer", "rx_message_buffer", "tx_ack_buffer", "rx_ack_buffer" ,...
  memcpy(&message[kNameSizeStartIndex], &rm.name_size_, sizeof(rm.name_size_));
  // 消息名称: 即消息传递所在的buffer类型或实际数据类型
  memcpy(&message[kNameStartIndex], rm.name_.data(), rm.name_.size());
  // 交换RDMAbuffer信息:buffer_size, remote_addr, rkey
  if ((rm.type_ == RDMA_MESSAGE_BUFFER_REQUEST) || (rm.type_ == RDMA_MESSAGE_BUFFER_RESPONSE)) {
    memcpy(&message[kBufferSizeStartIndex], &rm.buffer_size_, sizeof(rm.buffer_size_));
    memcpy(&message[kRemoteAddrStartIndex], &rm.remote_addr_, sizeof(rm.remote_addr_));
    memcpy(&message[kRkeyStartIndex], &rm.rkey_, sizeof(rm.rkey_)); 
  }
  // 实际数据大小: data_bytes
  if (rm.type_ == RDMA_MESSAGE_DATA_WRITE) {
    memcpy(&message[kDataBytesStartIndex], &rm.data_bytes_, sizeof(rm.data_bytes_));
  }
  return string(message, kMessageTotalBytes);
} 

void RdmaMessage::ParseMessage(RdmaMessage& rm, void* buffer) {
  char* message = static_cast<char*>(buffer);
  // 消息类型
  rm.type_ = static_cast<RdmaMessageType>(message[kTypeStartIndex]);
  // 消息名称大小
  memcpy(&rm.name_size_, &message[kNameSizeStartIndex], sizeof(rm.name_size_));
  // 消息名称
  rm.name_ = string(&message[kNameStartIndex], rm.name_size_);
  // 交换RDMAbuffer信息: buffer_size, remote_addr, rkey
  if ((rm.type_ == RDMA_MESSAGE_BUFFER_REQUEST) || (rm.type_ == RDMA_MESSAGE_BUFFER_RESPONSE)) {
    memcpy(&rm.buffer_size_, &message[kBufferSizeStartIndex], sizeof(rm.buffer_size_));
    memcpy(&rm.remote_addr_, &message[kRemoteAddrStartIndex], sizeof(rm.remote_addr_));
    memcpy(&rm.rkey_, &message[kRkeyStartIndex], sizeof(rm.rkey_));
  }
  // 实际数据大小 
  if (rm.type_ == RDMA_MESSAGE_DATA_WRITE) {
    memcpy(&rm.data_bytes_, &message[kDataBytesStartIndex], sizeof(rm.data_bytes_));
  }
}

}
