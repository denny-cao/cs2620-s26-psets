#include "cotamer.hh"
#include "channel.hh"
#include <print>

namespace cot = cotamer;
using namespace std::chrono_literals;
using namespace simulator;


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


int main() {
    network<int> net;

    ping_server(0, net.link(0, 1), net.input(0)).detach();
    ping_server(1, net.link(1, 0), net.input(1)).detach();

    cot::loop();
}
