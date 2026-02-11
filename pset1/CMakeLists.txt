cmake_minimum_required(VERSION 3.16)
project(rpcgame CXX)
add_subdirectory(third_party/rpclib)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Find required packages
find_package(Threads REQUIRED)

# Find xxhash
find_path(XXHASH_INCLUDE_DIR xxhash.h HINTS /opt/homebrew/include)
find_library(XXHASH_LIBRARY xxhash HINTS /opt/homebrew/lib)

if(NOT XXHASH_INCLUDE_DIR OR NOT XXHASH_LIBRARY)
    message(FATAL_ERROR "xxhash not found. Install with: brew install xxhash")
endif()


# Server executable
add_executable(rpcg-server
    rpcg-server.cc
    serverstub.cc
)
target_include_directories(rpcg-server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${XXHASH_INCLUDE_DIR}
)
target_link_libraries(rpcg-server PRIVATE
    ${XXHASH_LIBRARY}
    Threads::Threads
    rpc
)

# Client executable
add_executable(rpcg-client
    rpcg-client.cc
    clientstub.cc
)
target_include_directories(rpcg-client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${XXHASH_INCLUDE_DIR}
)
target_link_libraries(rpcg-client PRIVATE
    ${XXHASH_LIBRARY}
    Threads::Threads
    rpc
)
