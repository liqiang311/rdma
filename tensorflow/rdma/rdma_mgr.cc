#include "rdma_mgr.h"
#include "rdma_buffer.h"
namespace rdma{

RdmaMgr::RdmaMgr(){
	
  rdma_adapter_ = new RdmaAdapter();
}

RdmaMgr::~RdmaMgr() {
  for (const auto& p : channel_table_) delete p.second;
  channel_table_.clear();
  delete rdma_adapter_;
}

uint32_t RdmaMgr::SetupChannels(int sock,string remote_name){

  LOG_DEBUG << "Start Setup Channels. Socket:" << sock << ", Remote Name:" << remote_name.c_str();
  auto rc_ = new RdmaChannel(rdma_adapter_,remote_name);
  auto hash_name = str_hash(remote_name);
  channel_table_.insert({hash_name, rc_});
  LOG_DEBUG << "Created Channel. Remote Name:" << remote_name.c_str() << ", hash_name:" << hash_name << ", rc:" << rc_;
  //发送本地rx_message_buffer_地址信息
  RemoteMR l_msg{};
  l_msg.remote_addr = reinterpret_cast<uint64_t>(rc_->rx_message_buffer_->buffer_);
  l_msg.rkey = rc_->rx_message_buffer_->self_->rkey;
  tcp_write(sock, &l_msg, sizeof(l_msg));
  //接收对端rx_message_buffer_地址信息
  RemoteMR r_msg{};
  tcp_read(sock, &r_msg, sizeof(r_msg));
  rc_->tx_message_buffer_->SetRemoteMR(r_msg);

  //发送本地rx_ack_buffer_地址信息
  RemoteMR l_ack{};
  l_ack.remote_addr = reinterpret_cast<uint64_t>(rc_->rx_ack_buffer_->buffer_);
  l_ack.rkey = rc_->rx_ack_buffer_->self_->rkey;
  tcp_write(sock, &l_ack, sizeof(l_ack));

  //接收对端rx_message_buffer_地址信息
  RemoteMR r_ack{};
  tcp_read(sock, &r_ack, sizeof(r_ack));
  rc_->tx_ack_buffer_->SetRemoteMR(r_ack);

  //交换设备及QP信息: lid、qpn、psn
  RdmaAddress ra{};
  ra.lid = rc_->self_.lid;
  ra.qpn = rc_->self_.qpn; 
  ra.psn = rc_->self_.psn;
  tcp_write(sock, &ra, sizeof(ra)); 
  tcp_read(sock, &ra, sizeof(ra)); 

  rc_->SetRemoteAddress(ra);
  rc_->Connect(ra);
  return hash_name;
}

RdmaChannel* RdmaMgr::FindChannel(uint32_t key) {
  ChannelTable::iterator iter = channel_table_.find(key);
  CHECK(iter != channel_table_.end());
 return iter->second;
}

void RdmaMgr::CloseChannel(uint32_t key){
  ChannelTable::iterator iter = channel_table_.find(key);
  if(iter != channel_table_.end()){
	delete iter->second;
	channel_table_.erase(key);
  }
  
}

RdmaMgr* RdmaMgr::GetInstance() {  
  if(mgr_ == nullptr)  
      mgr_ = new RdmaMgr();  
  return mgr_;  
}  

RdmaMgr* RdmaMgr::mgr_ = nullptr;

}
