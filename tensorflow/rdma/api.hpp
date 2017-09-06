#include "rdma_mgr.h"

using namespace std;

namespace rdma {

int Listen(int port){
  sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = INADDR_ANY;

  auto sock = tcp_socket();
  tcp_bind(sock, addr);
  listen(sock, SOMAXCONN);
  LOG_DEBUG << "Start listening. Port:" << port << ", Socket:" << sock;
  return sock;
}

uint32_t Accept(int sock){
  auto mgr = RdmaMgr::GetInstance();
  sockaddr_in inAddr;
  LOG_DEBUG << "Waiting for accept. Socket:" << sock;
  auto acced = tcp_accept(sock, inAddr);
  LOG_DEBUG << "Accepted. Acced:" << acced;
  auto rc = mgr->SetupChannels(acced,string(inet_ntoa(inAddr.sin_addr))+":"+to_string(ntohs(inAddr.sin_port)));
  LOG_DEBUG << "Set Channel Completed. RDMA Channel:" << rc;
  return rc;
}

uint32_t Connect(const char* remote_ip,int port){
  auto mgr = RdmaMgr::GetInstance();
  sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, remote_ip, &addr.sin_addr);

  auto sock = tcp_socket();
  tcp_connect(sock, addr);
  LOG_DEBUG << "Connect to " << remote_ip << ":" << port << ". Socket:" << sock;
  auto rc = mgr->SetupChannels(sock,string(inet_ntoa(addr.sin_addr))+":"+to_string(ntohs(addr.sin_port)));
  LOG_DEBUG << "Set Channel Completed. RDMA Channel:" << rc;
  return rc;
}

//异步发送: 
void Send(uint32_t rc,const void* data,int size){
  LOG_DEBUG << "Start Sending. RC:" << rc << ", Data:" << (char*)data << ", Size:" << size;
  auto mgr = RdmaMgr::GetInstance();
  auto rc_ = mgr->FindChannel(rc);
  rc_->InnerSend(data, size);
}

//异步接收: 
void Receive(uint32_t rc,std::function<void(void*,int)> recv_done){
  auto mgr = RdmaMgr::GetInstance();
  auto rc_ = mgr->FindChannel(rc);
  rc_->RegisterRecvCallback(recv_done);
  rc_->RecvRequestData();
}

void Close(uint32_t rc){
  auto mgr = RdmaMgr::GetInstance();
  mgr->CloseChannel(rc);
}

}
