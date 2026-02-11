# CS 2620 Spring 2026 Problem Set 1: High-Performance RPC

## Build instructions for Mac Homebrew

```
brew install rpclib cmake xxhash
cmake -B build
cmake --build build
```

If `cmake -B build` fails with message about a cache problem, try `cmake
--fresh -B build`; or, as a last resort, `rm -rf build; cmake -B build`.
**Do not put any source files in the `build` directory.**

## Running instructions

```
(killall rpcg-server; build/rpcg-server& sleep 0.5; build/rpcg-client; sleep 0.1)
```
