编译:

	一、启动容器并挂载common库代码

		docker run -it \
		-v /home/RDMA/common:/common \
		ubuntu:rdma

	二、运行编译脚本

		run_in_container$ cd /common/bin
		run_in_container$ ./build


运行(在有IB设备环境):

	一、启动容器并挂载IB设备文件及相关库文件

		docker run -it \	
		--privileged \
		--network=host \
		-v /etc/libibverbs.d:/etc/libibverbs.d:ro \
		-v /usr/lib/libibverbs:/usr/lib/libibverbs:ro \
		-v /usr/lib/librxe-rdmav2.so:/usr/lib/librxe-rdmav2.so:ro \
		-v /sys/class/infiniband_verbs:/sys/class/infiniband_verbs:ro \
		-v /usr/lib/x86_64-linux-gnu/libnuma.so.1:/usr/lib/x86_64-linux-gnu/libnuma.so.1:ro \
		-v /root/RDMA/common:/common \
		ubuntu:rdma

	二、运行编译后的可执行程序

		run_in_container$ cd /common/bin
		run_in_container$ ./pingpong
