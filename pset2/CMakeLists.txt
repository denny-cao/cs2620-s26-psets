cmake_minimum_required(VERSION 3.16)
project(cotamer CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(-g -O3)
add_link_options(-g -O3)

add_executable(ping
    ping.cc
    cotamer.cc
)

add_executable(ctconsensus
    ctconsensus.cc
    cotamer.cc
)
