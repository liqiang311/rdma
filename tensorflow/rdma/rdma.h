#ifndef _RDMA_H_
#define _RDMA_H_
#include <string>
#include <functional>
#include "infiniband/verbs.h"
#include "log.h"

using namespace std;

namespace rdma{

typedef std::function<void(const string&)> DoneCallback;

struct RdmaAddress {
      uint32_t lid;
      uint32_t qpn;
      uint32_t psn;
};

struct RemoteMR{
    uint64_t remote_addr;
    uint32_t rkey;
};

enum BufferStatus {none, idle, busy};
enum Location {local, remote};
enum RdmaMessageType {RDMA_MESSAGE_ACK, 
                      RDMA_MESSAGE_BUFFER_IDLE,
                      RDMA_MESSAGE_BUFFER_REQUEST,
                      RDMA_MESSAGE_BUFFER_RESPONSE,
                      RDMA_MESSAGE_DATA_REQUEST,
                      RDMA_MESSAGE_DATA_WRITE};


struct RdmaMessage {
  RdmaMessageType type_;
  uint16_t name_size_;
  string name_;
  uint64_t buffer_size_;
  uint64_t remote_addr_;
  uint32_t rkey_;
  size_t data_bytes_;
  
// type|name_size|name|buffer_size|remote_addr|rkey|data_bytes|data_buffer
//   1B|    2B   | 512|    8B     |       8B  | 4B |    8B    |...

  static const size_t kNameCapacity = 512;
  static const size_t kTypeStartIndex = 0;
  static const size_t kNameSizeStartIndex = kTypeStartIndex + sizeof(type_);
  static const size_t kNameStartIndex = kNameSizeStartIndex + sizeof(name_size_);
  static const size_t kBufferSizeStartIndex = kNameStartIndex + kNameCapacity;
  static const size_t kRemoteAddrStartIndex = kBufferSizeStartIndex + sizeof(buffer_size_);
  static const size_t kRkeyStartIndex = kRemoteAddrStartIndex + sizeof(remote_addr_);
  static const size_t kDataBytesStartIndex = kRkeyStartIndex + sizeof(rkey_);
  static const size_t kDataBufferStartIndex = kDataBytesStartIndex + sizeof(data_bytes_);
  static const size_t kMessageTotalBytes = kDataBufferStartIndex;
  static const size_t kRdmaMessageBufferSize = kMessageTotalBytes;
  static const size_t kRdmaAckBufferSize = kMessageTotalBytes;

  static string CreateMessage(const RdmaMessage & rm);
  static void ParseMessage(RdmaMessage& rm, void* buffer);
};

struct Item {
  void* data;
  int size;
  ~Item() {
      if(data){ free(data); data = NULL;}
  }
};


#define LOG_DEBUG   LogMessage(__FILE__, __LINE__, DEBUG)
#define LOG_INFO    LogMessage(__FILE__, __LINE__, INFO)
#define LOG_WARNING LogMessage(__FILE__, __LINE__, WARNING)
#define LOG_ERROR   LogMessage(__FILE__, __LINE__, ERROR)
#define LOG_FATAL   LogMessage(__FILE__, __LINE__, FATAL)

#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define CHECK(condition)  if (PREDICT_FALSE(!(condition)))  LOG_FATAL << "Check failed: " #condition " "

}

#endif
