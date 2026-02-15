cmake_minimum_required(VERSION 3.16)
project(cotamer CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_compile_options(-g -O3 -W -Wall)
add_link_options(-g -O3 -W -Wall)

add_executable(ping
    ping.cc
    detail/cotamer.cc
    detail/utils.cc
)

add_executable(ctconsensus
    ctconsensus.cc
    detail/cotamer.cc
    detail/utils.cc
)

add_executable(ctstubborn
    ctstubborn.cc
    detail/cotamer.cc
    detail/utils.cc
)
