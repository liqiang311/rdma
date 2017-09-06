#ifndef _RDMA_ADAPTER_H_
#define _RDMA_ADAPTER_H_

#include <thread>
#include "rdma.h"

namespace rdma {

class Thread {
 public:
  Thread(std::function<void()> fn)
      : thread_(fn) {}
  ~Thread() { thread_.join(); }

 private:
  std::thread thread_;
};

class RdmaAdapter {

 public:
  RdmaAdapter();
  ~RdmaAdapter();

void Process_CQ();
 public:
  static const int MAX_CONCURRENT_WRITES = 1000;
  
  ibv_context* context_;
  
  ibv_pd* pd_;
  
  ibv_comp_channel* event_channel_;
  
  ibv_cq* cq_;

  ibv_wc wc_[MAX_CONCURRENT_WRITES * 2];

  Thread* thread_;
};
	

}

#endif
