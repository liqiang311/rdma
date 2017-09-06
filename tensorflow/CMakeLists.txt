cmake_minimum_required(VERSION 3.0)
project(rdma_sockets)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}" )

set(SOURCE_FILES
        rdma/rdma_adapter.cc
        rdma/rdma_buffer.cc
        rdma/rdma_channel.cc
        rdma/rdma_mgr.cc
	rdma/log.cc
        tcp/tcpWrapper.cpp
        )

include_directories(..)

add_executable(pingpong pingpong.cc ${SOURCE_FILES})
target_link_libraries(pingpong ibverbs pthread)

add_library(rdma SHARED ${SOURCE_FILES})
target_link_libraries(rdma ibverbs)
