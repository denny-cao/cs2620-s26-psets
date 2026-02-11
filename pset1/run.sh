cd build; cmake --build .; cd ..; killall rpcg-server; build/rpcg-server& sleep 0.5; build/rpcg-client; sleep 0.1;
