cmake_minimum_required(VERSION 3.16)
project(rpcgame CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Find required packages
find_package(Threads REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)

# Find xxhash
find_path(XXHASH_INCLUDE_DIR xxhash.h HINTS /opt/homebrew/include)
find_library(XXHASH_LIBRARY xxhash HINTS /opt/homebrew/lib)

if(NOT XXHASH_INCLUDE_DIR OR NOT XXHASH_LIBRARY)
    message(FATAL_ERROR "xxhash not found. Install with: brew install xxhash")
endif()

# Get the grpc_cpp_plugin location
get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin LOCATION)

# Proto file
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/rpcgame.proto")
set(PROTO_SRC_DIR "${CMAKE_CURRENT_BINARY_DIR}")

# Generate protobuf and grpc sources
set(PROTO_SRCS "${PROTO_SRC_DIR}/rpcgame.pb.cc")
set(PROTO_HDRS "${PROTO_SRC_DIR}/rpcgame.pb.h")
set(GRPC_SRCS "${PROTO_SRC_DIR}/rpcgame.grpc.pb.cc")
set(GRPC_HDRS "${PROTO_SRC_DIR}/rpcgame.grpc.pb.h")

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND protobuf::protoc
    ARGS --grpc_out=${PROTO_SRC_DIR}
         --cpp_out=${PROTO_SRC_DIR}
         -I${CMAKE_CURRENT_SOURCE_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
         ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating protobuf and gRPC sources from rpcgame.proto"
)

# Server executable
add_executable(rpcg-server
    rpcg-server.cc
    serverstub.cc
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_include_directories(rpcg-server PRIVATE
    ${PROTO_SRC_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${XXHASH_INCLUDE_DIR}
)
target_link_libraries(rpcg-server PRIVATE
    gRPC::grpc++
    gRPC::grpc++_reflection
    protobuf::libprotobuf
    ${XXHASH_LIBRARY}
    Threads::Threads
)

# Client executable
add_executable(rpcg-client
    rpcg-client.cc
    clientstub.cc
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_include_directories(rpcg-client PRIVATE
    ${PROTO_SRC_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${XXHASH_INCLUDE_DIR}
)
target_link_libraries(rpcg-client PRIVATE
    gRPC::grpc++
    protobuf::libprotobuf
    ${XXHASH_LIBRARY}
    Threads::Threads
)
