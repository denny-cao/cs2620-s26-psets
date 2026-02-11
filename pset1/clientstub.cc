#include <rpc/client.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rpcgame.hh"

// TODO: tune parameters
static size_t WINDOW_SIZE = 4096;
static size_t BATCH_SIZE = 2048;

class RPCGameClient {
public:
    RPCGameClient(const std::string& host, int port)
        : _client(host, port) {
        // Prevent hanging forever on broken connections
        _client.set_timeout(10000); // ms
    }

    void send_try(const char* name, size_t name_len, uint64_t count) {
        // if window full, process until we have space
        while (_in_flight_tries >= WINDOW_SIZE) { process_one_batch_response(); }

        _batch_buf.emplace_back(_serial, std::string(name, name_len), count);

        // send batch once full
        if (_batch_buf.size() >= BATCH_SIZE) { flush_batch(); }
        ++_serial;
    }

    void finish() {
        // flush remaining tries in buffer
        if (!_batch_buf.empty()) { flush_batch(); }

        // drain outstanding batches 
        while (_in_flight_batches > 0) { process_one_batch_response(); }

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
    using TryItem = std::tuple<uint64_t, std::string, uint64_t>; // (serial, name, count)

    struct Batch {
        std::vector<uint64_t> serials; // serials of tries in this batch (for response processing)
        size_t n = 0;                  // number of tries in batch
    };

    rpc::client _client;

    uint64_t _serial = 1;

    std::vector<TryItem> _batch_buf;   // buffer of tries waiting to be sent in a batch

    uint64_t _next_batch_id = 1;   // id of next batch to be sent
    std::unordered_map<uint64_t, std::future<clmdep_msgpack::object_handle>> _batch_futures;
    std::unordered_map<uint64_t, Batch> _batches; // batch metadata for in-flight batches

    // Delivery ordering
    std::unordered_map<uint64_t, uint64_t> _pending_responses; // serial -> value
    uint64_t _next_response_serial = 1;

    // Counts for windowing
    size_t _in_flight_batches = 0;
    size_t _in_flight_tries = 0; // total tries represented by all in-flight batches

    void flush_batch() {
        if (_batch_buf.empty()) return;

        // Build meta (serial list) so we can map response vector back to serials
        Batch meta;
        meta.n = _batch_buf.size();
        meta.serials.reserve(meta.n);
        for (const auto& item : _batch_buf) {
            meta.serials.push_back(std::get<0>(item));
        }

        const uint64_t batch_id = _next_batch_id++;

        // Fire async batch RPC
        auto fut = _client.async_call("try_batch", _batch_buf);

        _batch_futures.emplace(batch_id, std::move(fut));
        _batches.emplace(batch_id, std::move(meta));

        _in_flight_batches += 1;
        _in_flight_tries += _batch_buf.size();

        _batch_buf.clear();
    }

    void process_one_batch_response() {
        if (_batch_futures.empty()) { return; }

        // Prefer to wait on a batch that might enable in-order delivery.
        // (Heuristic: batch containing _next_response_serial)
        uint64_t chosen_id = 0;
        for (const auto& [bid, meta] : _batches) {
            // meta.serials is sorted increasing because serials increase monotonically
            if (!meta.serials.empty() &&
                meta.serials.front() <= _next_response_serial &&
                _next_response_serial <= meta.serials.back()) {
                chosen_id = bid;
                break;
            }
        }

        // Otherwise: pick any ready batch
        if (chosen_id == 0) {
            for (const auto& [bid, fut] : _batch_futures) {
                if (fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                    chosen_id = bid;
                    break;
                }
            }
        }

        // Otherwise: block on some batch
        if (chosen_id == 0) {
            chosen_id = _batch_futures.begin()->first;
        }

        auto fit = _batch_futures.find(chosen_id);
        auto mit = _batches.find(chosen_id);

        // Get returned vector<uint64_t>
        clmdep_msgpack::object_handle resp = fit->second.get();
        std::vector<uint64_t> values = resp.as<std::vector<uint64_t>>();

        Batch& meta = mit->second;
        if (values.size() != meta.n) {
            std::cerr << "try_batch returned wrong size: expected " << meta.n
                      << " got " << values.size() << "\n";
            std::exit(1);
        }

        // Insert responses into pending map keyed by serial.
        for (size_t i = 0; i < meta.n; ++i) {
            _pending_responses.emplace(meta.serials[i], values[i]);
        }

        // Remove this batch from tracking
        _in_flight_batches -= 1;
        _in_flight_tries -= meta.n;

        _batch_futures.erase(fit);
        _batches.erase(mit);

        // Deliver in serial order
        while (true) {
            auto it = _pending_responses.find(_next_response_serial);
            if (it == _pending_responses.end()) break;
            client_recv_try_response(it->second);
            _pending_responses.erase(it);
            ++_next_response_serial;
        }
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