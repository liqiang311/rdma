#include <iostream>
#include <unistd.h>
#include <stdio.h>
#include <chrono>
#include <thread>
#include "rdma/api.hpp"

using namespace std;
using namespace rdma;
using namespace api;
using namespace chrono;


void run(RDMAMessageBuffer* rdma){
	while(1){
	        auto ping = rdma->receive();
		//printf("data:%s,size:%d\n",ping.data(),int(ping.size()));
	}
}

void HandleConn(RDMAMessageBuffer* rdma) {
	thread receiver (run,rdma);

	//auto Data4 = array<uint8_t, 6>{"hello"};
    //  	rdma->send(Data4.data(), Data4.size());
	//rdma->send(Data4.data(), Data4.size());
	//rdma->send(Data4.data(), Data4.size());
	//rdma->send(Data4.data(), Data4.size());
	receiver.join();

	Close(rdma);
}

int main(int argc, char **argv) {
    if (argc < 2 || (argv[1][0] == 'c' && argc < 2)) {
        cout << "Usage: " << argv[0] << " <client / server>" << endl;
        return -1;
    }
    const auto isClient = argv[1][0] == 'c';

    if (isClient) {

	RDMAMessageBuffer* rdma = Connect("11.11.11.36",3003);	

	//thread receiver (run,rdma);
		auto start = system_clock::now();
	int send_num = 100000;
	while(send_num--){
        string data = "Hello,World!"+to_string(send_num);
        rdma->send(reinterpret_cast<const uint8_t*>(data.data()), data.size()+1);
        //rdma->send(Data2.data(), Data2.size());
        //rdma->send(Data2.data(), Data2.size());
        //rdma->send(Data2.data(), Data2.size());
    }
    auto end   = system_clock::now();
		auto duration = duration_cast<microseconds>(end - start);
		cout << "Send comsume " << double(duration.count()) * microseconds::period::num / microseconds::period::den 
			<< "s" << endl;
        //receiver.join();
    	Close(rdma);

    } else {

	int sock = Listen(3003);
	while(1){
		RDMAMessageBuffer* rdma = Accept(sock);
			
		thread conn (HandleConn,rdma);
		conn.detach();
	}
    }


    return 0;
}

