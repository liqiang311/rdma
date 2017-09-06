#ifndef _RDMA_MGR_H_
#define _RDMA_MGR_H_
#include <string>
#include "../tcp/tcpWrapper.h"
#include "rdma_channel.h"
#include "rdma_adapter.h"

using namespace std;

namespace rdma{

class RdmaMgr {

 public:

  explicit RdmaMgr();

  ~RdmaMgr();
  
  uint32_t SetupChannels(int sock,string remote_name);

  RdmaChannel* FindChannel(uint32_t key);
  void CloseChannel(uint32_t);
  static RdmaMgr* GetInstance();

 public:

  RdmaAdapter* rdma_adapter_;
  
  typedef std::unordered_map<uint32_t, RdmaChannel*> ChannelTable;
  ChannelTable channel_table_;

  std::hash<std::string> str_hash;

  static RdmaMgr* mgr_;

};

}

#endif
