#include <rpc/server.h>
#include <chrono>
#include <memory>
#include <thread>
#include <tuple>
#include <iostream>
#include "rpcgame.hh"

static std::unique_ptr<rpc::server> server;

void server_start(std::string address) {
    size_t colon = address.find(':');
    int port = std::stoi(address.substr(colon + 1));

    server = std::make_unique<rpc::server>(port);

    server->bind("try", [](uint64_t serial, const std::string& name, uint64_t count) -> uint64_t {
        return server_process_try(serial, name.data(), name.size(), count);
    });

    server->bind("done", [&]() -> std::tuple<std::string, std::string> {
        auto out = std::make_tuple(client_checksum(), server_checksum());
        std::thread([&]{
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            server->stop(); // end run()
        }).detach();
        return out;
    });

    std::cout << "Server listening on " << address << "\n";
    server->run(); // blocks until stop()
    std::cout << "Server exiting\n";
}
