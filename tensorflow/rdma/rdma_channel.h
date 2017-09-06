#ifndef _RDMA_CHANNEL_H_
#define _RDMA_CHANNEL_H_
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include "rdma_adapter.h"

using namespace std;

namespace rdma {

class RdmaBuffer;

class RdmaChannel{

public:

	explicit RdmaChannel(const RdmaAdapter* adapter, const string remote_name);
	~RdmaChannel();
	void RecvRequestData();
	void InnerSend(const void* data,int size);
	void Connect(RdmaAddress& remoteAddr);
	void Recv();
	RdmaBuffer* FindBuffer(const uint32_t index);
	RdmaBuffer* FindBuffer(const string& name);
	RdmaBuffer* FindOrCreateBuffer(const string& name);
	uint32_t LookupBufferIndex (const string& buffer_name);
	void SetRemoteAddress(RdmaAddress ra);

	void RegisterRecvCallback(std::function<void(void*,int)> recv_run);
	void RunRecvCallback();
	void EnqueueData(Item* data);
	inline bool IsDataQueueEmpty() {return queue_.empty();}
	inline void SetRecvReady(){	if(!isReady_) isReady_ = true;}
	inline bool IsRecvReady(){return isReady_;}
	inline Item* GetDataFromQueue(){
	    mux_.lock();
	    Item*  data = queue_.front();
	    queue_.pop();
	    mux_.unlock();
	    return data;
	}
	static const int kNumMessageBuffers = 4;
public:

	const RdmaAdapter* adapter_;
	ibv_qp* qp_;
	RdmaAddress self_;
	RdmaAddress remote_;
	string remote_name_;
	bool connected_;
	bool remote_set_;
	std::mutex mux_;
	std::function<void(void*,int)> recvcallback_;

	typedef std::unordered_map<unsigned int, RdmaBuffer*> BufferTable;
	BufferTable buffer_table_;

	typedef std::unordered_map<uint32_t, string> BufferIndexNameTable;
	BufferIndexNameTable buffer_index_name_table_;

	typedef std::unordered_map<string, uint32_t> BufferNameIndexTable;
	BufferNameIndexTable buffer_name_index_table_;

	//待发送或待接收数据表
	std::queue <Item*>  queue_;
	bool isReady_;

	RdmaBuffer* tx_message_buffer_;
	RdmaBuffer* rx_message_buffer_;
	RdmaBuffer* tx_ack_buffer_;
	RdmaBuffer* rx_ack_buffer_;
	std::vector<RdmaBuffer*> message_buffers_;

	std::hash<std::string> str_hash;
	};

}

#endif
