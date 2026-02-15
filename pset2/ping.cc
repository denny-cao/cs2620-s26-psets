#include "cotamer.hh"
#include "netsim.hh"
#include <print>

namespace cot = cotamer;
using namespace std::chrono_literals;
using namespace netsim;


cot::task<> ping_server(int id, channel<int>& out, port<int>& in) {
    int received = 0;
    if (id == 0) {
        std::print("{}: server {} sends initial ping\n", cot::now(), id);
        co_await out.send(0);
    }
    while (received < 5) {
        int msg = co_await in.receive();
        ++received;
        std::print("{}: server {} received {}, sends {}\n", cot::now(), id, msg, msg + 1);
        co_await out.send(msg + 1);
    }
}


// Program entry point

static struct option options[] = {
    { "seed", required_argument, nullptr, 'S' },
    { "verbose", no_argument, nullptr, 'V' },
    { nullptr, 0, nullptr, 0 }
};

int main(int argc, char* argv[]) {
    network<int> net;

    // Read program options: `-V` prints more information about messages.
    auto shortopts = short_options_for(options);
    int ch;
    while ((ch = getopt_long(argc, argv, shortopts.c_str(), options, nullptr)) != -1) {
        if (ch == 'S') {
            net.randomness().seed(from_str_chars<unsigned long>(optarg));
        } else if (ch == 'V') {
            net.set_verbose(true);
        } else {
            std::print(std::cerr, "Unknown option\n");
            return 1;
        }
    }

    // Start the ping server coroutines and run until they’re done
    ping_server(0, net.link(0, 1), net.input(0)).detach();
    ping_server(1, net.link(1, 0), net.input(1)).detach();
    cot::loop();
}
