#pragma once
#include "cotamer.hh"
#include <map>
#include <print>

namespace simulator {
namespace cot = cotamer;
using namespace std::chrono_literals;
template <typename T> struct port;


template <typename T>
struct channel {
    using message_type = T;

    channel(int from, port<T>& to_port)
        : from_(from), to_port_(to_port) {
    }
    channel(const channel<T>&) = delete;
    channel(channel<T>&&) = delete;
    channel<T>& operator=(const channel<T>&) = delete;
    channel<T>& operator=(channel<T>&&) = delete;


    // send a message on this channel
    cot::task<> send(message_type m);


private:
    int from_;
    port<T>& to_port_;
    cot::clock::duration link_delay_ = 20ms;
    cot::clock::duration send_delay_ = 1ms;

    cot::task<> send_after(cot::clock::duration, message_type);
};


template <typename T>
struct port {
    using message_type = T;

    port(int id)
        : id_(id) {
    }
    ~port() {
        // wake up any `receive` coroutines. (They must not *run* —
        // the port they're listnening on has been deleted.)
        receiver_event_.trigger();
    }
    port(const port<T>&) = delete;
    port(port<T>&&) = delete;
    port<T>& operator=(const port<T>&) = delete;
    port<T>& operator=(port<T>&&) = delete;


    // receive a message on this port
    cot::task<T> receive();


private:
    friend struct channel<T>;

    int id_;
    std::deque<message_type> messageq_;
    cot::event receiver_event_;
};


template <typename T>
struct network {
    using channel_type = channel<T>;
    using port_type = port<T>;

    network() = default;
    network(const network<T>&) = delete;
    network(network<T>&&) = delete;
    network<T>& operator=(const network<T>&) = delete;
    network<T>& operator=(network<T>&&) = delete;


    // return the channel from `src` to `dst`
    channel_type& link(int src, int dst);

    // return the input interface for receiving messages to `id`
    port_type& input(int id);


private:
    std::map<std::pair<int, int>, channel_type> links_;
    std::map<int, port_type> ports_;
};


template <typename T>
cot::task<> channel<T>::send(message_type m) {
    std::print("{}: {} → {}: \"{}\"\n", cot::now(), from_, to_port_.id_, m);

    // after `link_delay_`, place the message in the receiver’s queue
    send_after(link_delay_, std::move(m)).detach();

    // sending a message takes time
    co_await cot::after(send_delay_);
}

template <typename T>
cot::task<> channel<T>::send_after(cot::clock::duration delay, message_type m) {
    co_await cot::after(delay);

    // record this message in the receiver’s port
    to_port_.messageq_.emplace_back(std::move(m));

    // wake up a blocked receiver
    to_port_.receiver_event_.trigger();
}

template <typename T>
cot::task<T> port<T>::receive() {
    // sleep until there’s a message
    while (messageq_.empty()) {
        co_await (receiver_event_ = cot::event());
    }

    auto m = std::move(messageq_.front());
    messageq_.pop_front();

    std::print("{}: \"{}\" → {}\n", cot::now(), m, id_);

    co_return m;
}

template <typename T>
channel<T>& network<T>::link(int from, int to) {
    auto [it, inserted] = links_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(std::make_pair(from, to)),
        std::forward_as_tuple(from, input(to))
    );
    return it->second;
}

template <typename T>
port<T>& network<T>::input(int id) {
    auto [it, inserted] = ports_.emplace(id, id);
    return it->second;
}

}
