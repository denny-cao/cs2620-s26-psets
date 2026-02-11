#include <grpcpp/grpcpp.h>
#include <memory>
#include <queue>
#include <unordered_map>
#include "rpcgame.grpc.pb.h"
#include "rpcgame.hh"

static size_t WINDOW_SIZE = 20; // TODO: tune this parameter

/** 
 * AsyncCall stores all per-RPC state required by gRPC's async unary API 
 */
struct AsyncCall {
    TryRequest request;
    TryResponse response;
    grpc::ClientContext context;
    grpc::Status status;
    std::unique_ptr<grpc::ClientAsyncResponseReader<TryResponse>> response_reader;
    uint64_t serial; // local ordering key (match req -> resp)
};

class RPCGameClient {
public:
    RPCGameClient(std::shared_ptr<grpc::Channel> channel)
        : _stub(RPCGame::NewStub(channel)) {
    }

    /**
     * Send a Try RPC asynchronously, subject to window size.
     * Blocks until there is space in the window.
     * Otherwise, enqueues an RPC and returns immediately.
     */
    void send_try(const char* name, size_t name_len, uint64_t count) {
        // if window full, process until we have space
        while (_in_flight >= _window_size) { process_one_response(); }

        // ready space for response
        AsyncCall* call = new AsyncCall;
        call->request.set_serial(std::to_string(_serial));
        call->request.set_name(std::string(name, name_len));
        call->request.set_count(std::to_string(count));
        call->serial = _serial;
        ++_serial;

        // send request, get response
        call->response_reader = _stub->AsyncTry(&call->context, call->request, &_cq);
        call->response_reader->Finish(&call->response, &call->status, (void*)call);

        ++_in_flight;
    }

    /**
     * Finish protocol by draining outstanding Try RPCs and sending Done RPC, then print checksums and whether they match.
     */
    void finish() {
        // Process remaining RPCs
        wait_for_all_pending_rpcs();

        // ready space for response
        DoneResponse response;

        // send request, get response
        grpc::ClientContext context;
        grpc::Status status = _stub->Done(&context, DoneRequest(), &response);
        if (!status.ok()) {
            std::cerr << status.error_code() << ": " << status.error_message()
                << "\n";
            exit(1);
        }

        // parse response
        const std::string& my_client_checksum = client_checksum(),
            my_server_checksum = server_checksum();
        bool ok = my_client_checksum == response.client_checksum()
            && my_server_checksum == response.server_checksum();
        std::cout << "client checksums: "
            << my_client_checksum << "/" << response.client_checksum()
            << "\nserver checksums: "
            << my_server_checksum << "/" << response.server_checksum()
            << "\nmatch: " << (ok ? "true\n" : "false\n");
        
        _cq.Shutdown(); // shut down completion queue to end process_one_response loop
    }

private: 
    std::unique_ptr<RPCGame::Stub> _stub;                      // gRPC stub for making RPC calls
    grpc::CompletionQueue _cq;                                 // for processing responses
    uint64_t _serial = 1;                                      // serial number for next request
    size_t _window_size = WINDOW_SIZE;                         // allows at most _window_size requests to be in-flight at once
    size_t _in_flight = 0;                                     // number of requests sent but not yet received responses for

    std::unordered_map<uint64_t, uint64_t> _pending_responses; // serial -> value
    uint64_t _next_response_serial = 1;                        // serial number of next response to inform client about

    /**
     * Block until one async Try RPC completes, then process results.
     */
    void process_one_response() {
        void* tag;
        bool ok;

        // Wait for one RPC to complete
        // Next() blocks until an RPC completes
        if (!_cq.Next(&tag, &ok)) {
            std::cerr << "Completion queue shutdown\n";
            return;
        }

        if (!ok) {
            std::cerr << "RPC failed\n";
            return;
        }

        AsyncCall* call = static_cast<AsyncCall*>(tag);

        if (!call->status.ok()) {
            std::cerr << call->status.error_code() << ": " << call->status.error_message() << "\n";
            exit(1);
        }

        // parse response. serial and val
        const std::string& valuestr = call->response.value();
        uint64_t value = from_str_chars<uint64_t>(valuestr);

        // if response is for next expected serial, inform client immediately
        if (call->serial == _next_response_serial) {
            client_recv_try_response(value);    
            ++_next_response_serial;   
        }
        // otherwise, store response for later
        else { _pending_responses[call->serial] = value; }

        // check if we can now inform client about any pending responses
        while (_pending_responses.count(_next_response_serial)) {
            client_recv_try_response(_pending_responses[_next_response_serial]);
            _pending_responses.erase(_next_response_serial);
            ++_next_response_serial;
        }

        // Clean up
        delete call;
        --_in_flight;
    }

    /**
     * Drain outstanding Try RPCs
     */
    void wait_for_all_pending_rpcs() {
        // Wait until all in-flight requests have received responses
        while (_in_flight > 0) { process_one_response(); }
    }
};

static std::unique_ptr<RPCGameClient> client;

void client_connect(std::string address) {
    // Request a compressed channel
    grpc::ChannelArguments args;
    // args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);
    client = std::make_unique<RPCGameClient>(
        grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args)
    );
}

void client_send_try(const char* name, size_t name_len, uint64_t count) {
    client->send_try(name, name_len, count);
}

void client_finish() {
    client->finish();
}