#include <grpcpp/grpcpp.h>

#include "rpcgame.grpc.pb.h"
#include "rpcgame.hh"

class RPCGameClient {
public:
    RPCGameClient(std::shared_ptr<grpc::Channel> channel)
        : _stub(RPCGame::NewStub(channel)) {
    }

    void send_try(const char* name, size_t name_len, uint64_t count) {
        // construct request
        TryRequest request;
        request.set_serial(std::to_string(_serial));
        request.set_name(std::string(name, name_len));
        request.set_count(std::to_string(count));
        ++_serial;

        // ready space for response
        TryResponse response;

        // send request, get response
        grpc::ClientContext context;
        grpc::Status status = _stub->Try(&context, request, &response);
        if (!status.ok()) {
            std::cerr << status.error_code() << ": " << status.error_message()
                << "\n";
            exit(1);
        }

        // parse response
        std::string valuestr = response.value();
        uint64_t value = from_str_chars<uint64_t>(valuestr);

        // inform client of response
        client_recv_try_response(value);
    }

    void finish() {
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
        std::string my_client_checksum = client_checksum(),
            my_server_checksum = server_checksum();
        bool ok = my_client_checksum == response.client_checksum()
            && my_server_checksum == response.server_checksum();
        std::cout << "client checksums: "
            << my_client_checksum << "/" << response.client_checksum()
            << "\nserver checksums: "
            << my_server_checksum << "/" << response.server_checksum()
            << "\nmatch: " << (ok ? "true\n" : "false\n");
    }

private:
    std::unique_ptr<RPCGame::Stub> _stub;
    uint64_t _serial = 1;
};


static std::unique_ptr<RPCGameClient> client;


void client_connect(std::string address) {
    // Request a compressed channel
    grpc::ChannelArguments args;
    args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);
    client = std::make_unique<RPCGameClient>(
        grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args));
}

void client_send_try(const char* name, size_t name_len, uint64_t count) {
    client->send_try(name, name_len, count);
}

void client_finish() {
    client->finish();
}
