cmake_minimum_required(VERSION 3.0)
project(rdma_sockets)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pedantic -Wall -Wextra -Wnon-virtual-dtor -Wold-style-cast -fshow-column -pipe -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ggdb3 -O0 -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -flto -DNDEBUG")

set(SOURCE_FILES
        rdma/CompletionQueuePair.cpp
        rdma/MemoryRegion.cpp
        rdma/Network.cpp
        rdma/QueuePair.cpp
        rdma/ReceiveQueue.cpp
        rdma/WorkRequest.cpp
        rdma/RDMAMessageBuffer.cpp
        tcp/tcpWrapper.cpp
        )

include_directories(..)

add_executable(pingpong pingpong.cpp ${SOURCE_FILES})
target_link_libraries(pingpong ibverbs pthread)


add_library(rdma SHARED ${SOURCE_FILES})
target_link_libraries(rdma ibverbs)
