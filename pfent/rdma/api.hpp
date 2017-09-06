#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdio.h>
#include <thread>
#include "../tcp/tcpWrapper.h"
#include "RDMAMessageBuffer.h"

using namespace std;
using namespace rdma;

namespace api{

#define BUFFERSIZE  1024*16 // 16K


int Listen(int port){

        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;

        auto sock = tcp_socket();
        tcp_bind(sock, addr);
        listen(sock, SOMAXCONN);
	
	return sock;
}

RDMAMessageBuffer* Accept(int sock){

       	sockaddr_in inAddr;
        auto acced = tcp_accept(sock, inAddr);
     	auto rdma = new RDMAMessageBuffer(BUFFERSIZE, acced);

	return rdma;
}

RDMAMessageBuffer* Connect(const char* ip,int port){

        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, ip, &addr.sin_addr);

        auto sock = tcp_socket();
        tcp_connect(sock, addr);

        RDMAMessageBuffer* rdma= new RDMAMessageBuffer(BUFFERSIZE, sock);
	close(sock);

	return rdma;
}

void Close(RDMAMessageBuffer* rdma){

	//close(acced);
	delete(rdma);
}

}
