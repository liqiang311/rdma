# 主机中运行

```
cd rdma-tf/bin
chmod +x build clean
./clean
./build
./pingpong server &
./pingpong client &
pkill -9 pingpong
```