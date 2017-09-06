#include <thread>
#include <iostream>
#include <functional>
#include <chrono>
#include "rdma/api.hpp"

using namespace std;
using namespace rdma;
using namespace chrono;

void callback(void* data,int size){
	static int count = 0;
	cout << "count:" << count++ << " callback:"<< (char*)data<< endl;
}

void HandleConn(uint32_t rc){
	Receive(rc,callback);
}

int main(int argc, char **argv) {
    if (argc < 2 || (argv[1][0] == 'c' && argc < 2)) {
        cout << "Usage: " << argv[0] << " <client / server>" << endl;
        return -1;
    }
    const auto isClient = argv[1][0] == 'c';
    
    setenv("RDMA_COMMON_MIN_LOG_LEVEL","0",1);

    if (isClient) {
		auto rc = Connect("127.0.0.1",3005);
		//Receive(rc,callback);
		auto start = system_clock::now();
		int send_num = 100;
		while(send_num--)
		{
			string s_hello = "Hello,World!";// + to_string(send_num);
			Send(rc,s_hello.data(),s_hello.size()+1);
		}
		auto end   = system_clock::now();
		auto duration = duration_cast<microseconds>(end - start);
		cout << "Send comsume " << double(duration.count()) * microseconds::period::num / microseconds::period::den 
			<< "s" << endl;
		while(1);
    } else {
		auto sock = Listen(3005);
		while(1){
			auto rc = Accept(sock);
			thread conn (HandleConn,rc);
			conn.detach();	
		}
    }


    return 0;
}
