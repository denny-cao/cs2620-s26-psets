#include <rpc/server.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "rpcgame.hh"

static std::unique_ptr<rpc::server> server;

void server_start(std::string address) {
    size_t colon = address.find(':');
    int port = std::stoi(address.substr(colon + 1));

    server = std::make_unique<rpc::server>(port);

    // Single-try (optional: keep for debugging; client can stop using it)
    server->bind("try", [](uint64_t serial, const std::string& name, uint64_t count) -> uint64_t {
        return server_process_try(serial, name.data(), name.size(), count);
    });

    // Batched try: list of (serial, name, count) -> list of values
    server->bind("try_batch",
                 [](const std::vector<std::tuple<uint64_t, std::string, uint64_t>>& items)
                     -> std::vector<uint64_t> {
                     std::vector<uint64_t> out;
                     out.reserve(items.size());
                     for (const auto& it : items) {
                         uint64_t serial = std::get<0>(it);
                         const std::string& name = std::get<1>(it);
                         uint64_t count = std::get<2>(it);
                         out.push_back(server_process_try(serial, name.data(), name.size(), count));
                     }
                     return out;
                 });

    server->bind("done", [&]() -> std::tuple<std::string, std::string> {
        auto out = std::make_tuple(client_checksum(), server_checksum());
        std::thread([&]{
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            server->stop();
        }).detach();
        return out;
    });

    std::cout << "Server listening on " << address << "\n";
    server->run();
    std::cout << "Server exiting\n";
}
