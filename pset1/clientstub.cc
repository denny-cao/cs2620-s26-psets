#include <rpc/client.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

#include "rpcgame.hh"

static size_t WINDOW_SIZE = 512; // TODO: tune this parameter

class RPCGameClient {
public:
    RPCGameClient(const std::string& host, int port)
        : _client(host, port) {
        // Prevent hanging forever on broken connections
        _client.set_timeout(10000); // ms
    }

    void send_try(const char* name, size_t name_len, uint64_t count) {
        // if window full, process until we have space
        while (_in_flight >= _window_size) { process_one_response(); }

        const uint64_t serial = _serial;
        auto fut = _client.async_call("try", serial, std::string(name, name_len), count);
        _pending_calls.emplace(serial, std::move(fut));
        ++_serial;
        ++_in_flight;
    }

    void finish() {
        // Drain outstanding Try RPCs (delivering in order)
        wait_for_all_pending_rpcs();

        // Call done() and retrieve checksums from server
        auto tup = _client.call("done").as<std::tuple<std::string, std::string>>();
        const std::string& server_client_checksum = std::get<0>(tup);
        const std::string& server_server_checksum = std::get<1>(tup);

        // parse response
        const std::string& my_client_checksum = client_checksum(),
            my_server_checksum = server_checksum();
        bool ok = my_client_checksum == server_client_checksum 
            && my_server_checksum == server_server_checksum;
        std::cout << "client checksums: "
            << my_client_checksum << "/" << server_client_checksum
            << "\nserver checksums: "
            << my_server_checksum << "/" << server_server_checksum
            << "\nmatch: " << (ok ? "true\n" : "false\n");
    }

private:
    rpc::client _client;

    uint64_t _serial = 1;
    size_t _window_size = WINDOW_SIZE;
    size_t _in_flight = 0;

    std::unordered_map<uint64_t, std::future<clmdep_msgpack::object_handle>> _pending_calls;

    // buffer out-of-order completions so we can deliver in serial order
    std::unordered_map<uint64_t, uint64_t> _pending_responses;
    uint64_t _next_response_serial = 1;

    void process_one_response() {
        if (_pending_calls.empty()) return;

        // complete the next expected serial if present
        auto it = _pending_calls.find(_next_response_serial);

        // else try to find any ready future to avoid blocking on a slow one
        if (it == _pending_calls.end()) {
            for (auto jt = _pending_calls.begin(); jt != _pending_calls.end(); ++jt) {
                if (jt->second.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                    it = jt;
                    break;
                }
            }
        }

        // if none ready, block on some in-flight call (arbitrary)
        if (it == _pending_calls.end()) {
            it = _pending_calls.begin();
        }

        const uint64_t serial = it->first;
        std::future<clmdep_msgpack::object_handle>& fut = it->second;

        // wait for completion and decode uint64_t from msgpack object
        clmdep_msgpack::object_handle oh = fut.get();
        uint64_t value = oh.get().as<uint64_t>();

        // remove from in-flight tracking
        _pending_calls.erase(it);
        --_in_flight;

        // Deliver in serial order to preserve checksum semantics
        if (serial == _next_response_serial) {
            client_recv_try_response(value);
            ++_next_response_serial;
        } else {
            _pending_responses.emplace(serial, value);
        }

        // Drain any buffered responses that are now deliverable
        while (true) {
            auto jt = _pending_responses.find(_next_response_serial);
            if (jt == _pending_responses.end()) break;
            client_recv_try_response(jt->second);
            _pending_responses.erase(jt);
            ++_next_response_serial;
        }
    }

    void wait_for_all_pending_rpcs() {
        while (_in_flight > 0) { process_one_response(); }
    }
};

static std::unique_ptr<RPCGameClient> client;

void client_connect(std::string address) {
    // address format is "host:port"
    size_t colon = address.find(':');
    if (colon == std::string::npos) {
        std::cerr << "client_connect: bad address (expected host:port): " << address << "\n";
        std::exit(1);
    }

    std::string host = address.substr(0, colon);
    int port = std::stoi(address.substr(colon + 1));

    client = std::make_unique<RPCGameClient>(host, port);
}

void client_send_try(const char* name, size_t name_len, uint64_t count) {
    client->send_try(name, name_len, count);
}

void client_finish() {
    client->finish();
}