#ifndef _RDMA_BUFFER_H_
#define _RDMA_BUFFER_H_
#include <mutex>
#include <queue>
#include <string>
#include <string.h>
#include "rdma.h"
#include "rdma_channel.h"

using namespace std;

namespace rdma {

class RdmaBuffer {

 public:

  explicit RdmaBuffer(RdmaChannel* channel,string name);
  virtual ~RdmaBuffer();

  void FreeBuffer();
  void EnqueueItem(std::string Item);
  virtual void SendNextItem(){};
  void CreateCPUBuffer(size_t size);
  void SetRemoteMR(RemoteMR rmi);
  void Write(uint32_t imm_data, size_t buffer_size);

  inline void SetBufferStatus(Location loc, BufferStatus status){
    if (loc == local) {
      local_status_ = status;
    } else {
      remote_status_ = status;
    }
  }
 public:
  RdmaChannel* channel_;
  void* buffer_;
  size_t size_ ;
  const std::string name_;
  std::mutex mux_;
  ibv_mr* self_;
  RemoteMR remote_;
  std::queue <std::string> queue_ ;
  BufferStatus local_status_ = none;
  BufferStatus remote_status_ = none;
  std::hash<std::string> str_hash;
};

class RdmaAckBuffer : public RdmaBuffer {
 public:
  explicit RdmaAckBuffer(RdmaChannel* channel, string name);
  virtual ~RdmaAckBuffer() override {}
  void SendNextItem() override;
};

class RdmaMessageBuffer : public RdmaBuffer {
 public:
  explicit RdmaMessageBuffer(RdmaChannel* channel, string name);
  virtual ~RdmaMessageBuffer() override {}
  void SendNextItem() override;
};

class RdmaDataBuffer : public RdmaBuffer {
 public:
  explicit RdmaDataBuffer(RdmaChannel* channel, string name);
  virtual ~RdmaDataBuffer() override {}
  void SendNextItem() override;
};

}

#endif
