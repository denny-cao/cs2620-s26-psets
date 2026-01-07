# CS 2620 Spring 2026 Problem Set 1: High-Performance RPC

## Build instructions for Mac Homebrew

```
brew install grpc cmake xxhash
cmake -B build
(cd build; cmake --build .)
```

## Running instructions

```
(killall rpcg-server; build/rpcg-server&; sleep 0.5; build/rpcg-client; sleep 0.1)
```
