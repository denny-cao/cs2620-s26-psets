#include "cotamer.hh"
#include "channel.hh"
#include "util.hh"
#include "ctconsensus_msgs.hh"
#include <list>
#include <print>
#include <getopt.h>

namespace cot = cotamer;
using namespace std::chrono_literals;
using namespace simulator;

static std::mt19937_64 randomness = randomly_seeded<std::mt19937_64>();


namespace ctconsensus {

// The message definitions for Chandra-Toueg consensus are in
// `ctconsensus_msgs.hh`.

// the ID of the distinguished server that receive final decisions
constexpr int nancy_id = -1;

struct server {
    server(int id, int N, network<message>& net, std::string color)
        : id_(id), N_(N), net_(net), my_port_(net.input(id_)), color_(color) {
    }

    cot::task<> consensus();

private:
    // set at initialization
    int id_;
    int N_;
    network<message>& net_;
    port<message>& my_port_;
    std::string color_;

    // initial values
    uint64_t round_ = 1;
    uint64_t color_round_ = 0;


    cot::event failure_detector(int leader);
    cot::task<message> receive(message_type mt);
    cot::task<> decide(std::string color);
};


// Return a failure detector for the server with ID `leader`.
// The handout failure detector is a simple timeout.

cot::event server::failure_detector(int leader) {
    (void) leader;
    return cot::after(100ms);
}


// Receive a message of a specific type for the current round.
// Ignore other messages, except for DECIDE messages.

cot::task<message> server::receive(message_type mt) {
    while (true) {
        auto m = co_await my_port_.receive();

        // DECIDE messages get special processing;
        // decide() always throws consensus_achieved
        if (m.type == m_decide) {
            co_await decide(m.color);
            __builtin_unreachable();
        }

        // ignore unwanted messages
        if (m.type != mt || m.round != round_) {
            continue;
        }

        // return wanted message
        co_return m;
    }
}


// `decide(color)` broadcasts DECIDE to all other servers and Nancy,
// then terminates this server’s consensus algorithm via exception.

struct consensus_achieved : public std::runtime_error {
    consensus_achieved(std::string color)
        : std::runtime_error("Consensus achieved: " + color) {
    }
};

cot::task<> server::decide(std::string color) {
    auto decide = decide_message(color);
    // send DECIDE to everyone else
    for (int j = 0; j != N_; ++j) {
        if (j != id_) {
            co_await net_.link(id_, j).send(decide);
        }
    }
    // send DECIDE to Nancy
    co_await net_.link(id_, nancy_id).send(decide);
    // throw an exception to terminate this coroutine
    // and any coroutine waiting on it
    throw consensus_achieved(color);
}


// The main Chandra-Toueg consensus algorithm!

cot::task<> server::consensus() {
    while (true) {
        // Compute leader of current round
        int leader = round_ % N_;

        // Phase 1: Send PREPARE to leader
        co_await net_.link(id_, leader).send(
            prepare_message(round_, color_, color_round_)
        );

        // Phase 2: Leader receives PREPAREs; waits for
        // more than N/2; tracks latest color & round
        if (id_ == leader) {
            int received_prepare = 0;
            while (received_prepare <= N_ / 2) {
                auto m = co_await receive(m_prepare);
                // maybe update color
                if (m.color_round > color_round_) {
                    color_ = m.color;
                    color_round_ = m.color_round;
                }
                ++received_prepare;
            }
        }

        // Phase 3: Leader sends PROPOSE to all
        if (id_ == leader) {
            auto propose = propose_message(round_, color_);
            for (int j = 0; j != N_; ++j) {
                co_await net_.link(id_, j).send(propose);
            }
        }

        // Phase 4: Wait for either leader failure
        // or PROPOSE from leader
        std::optional<message> maybe_propose =
            co_await cot::attempt(receive(m_propose),
                                  failure_detector(leader));
        if (maybe_propose) {
            color_ = maybe_propose->color;
        }
        co_await net_.link(id_, leader).send(
            ack_message(round_, (bool) maybe_propose)
        );

        // Phase 5: Leader waits for ACKs
        if (id_ == leader) {
            int success = 0, total = 0;
            while (total <= N_ / 2) {
                auto m = co_await receive(m_ack);
                if (m.ack) {
                    ++success;
                }
                ++total;
            }

            if (success > N_ / 2) {
                // A majority acknowledged our color! Time to decide.
                co_await decide(color_);
            }
        }

        // Phase 6: Advance the round and continue
        ++round_;

        co_await cot::after(10ms);  // brief delay before next round
    }
}


// Nancy is a distinguished observer that collects DECIDE messages
// and validates that (1) all servers agree on the same color, and
// (2) the consensus color is valid for this initialization.
cot::task<> nancy(port<message>& my_port, int N, bool have_red, bool have_blue) {
    int received = 0;
    std::string consensus_color;

    // Wait until we get at least N/2 messages, and then a little longer
    // (in case more messages come in).
    cot::event stopper;
    while (!stopper.triggered()) {
        auto m = co_await cot::attempt(my_port.receive(), stopper);

        if (!m) { // timeout
            break;
        }

        if (m->type != m_decide
            || (m->color != "red" && m->color != "blue")) {
            std::print("*** ERROR! *** Nancy received unexpected \"{}\"\n", *m);
            goto done;
        }

        if ((m->color == "red" && !have_red)
            || (m->color == "blue" && !have_blue)
            || (!consensus_color.empty() && m->color != consensus_color)) {
            std::print("*** CONSENSUS ERROR! *** Nancy received \"{}\"\n", *m);
            goto done;
        }

        consensus_color = m->color;

        ++received;
        // We've got more than N/2 messages! Set `stopper` to go off after 10s.
        if (received > N/2 && stopper.empty()) {
            stopper = cot::after(10s);
        }
    }

    if (received <= N/2) {
        std::print("*** ERROR! *** Nancy terminated before consensus\n");
        goto done;
    }

    std::print("*** CONSENSUS ACHIEVED *** {} x \"{}\"\n", received, consensus_color);

done:
    // cancel all outstanding tasks and end the event loop
    cot::clear();
    co_return;
}

} // namespace ctconsensus



// The main driver

int main(int argc, char* argv[]) {
    // Read program options: `-n N` sets the number of servers,
    // and `-S SEED` sets the desired random seed
    int N = 3;
    int ch;
    while ((ch = getopt(argc, argv, "n:S:")) != -1) {
        if (ch == 'n') {
            N = from_str_chars<int>(optarg);
            if (N <= 2) {
                throw std::invalid_argument("`-n` must be 3 or more");
            }
        } else if (ch == 'S') {
            auto seed = from_str_chars<unsigned long>(optarg);
            randomness = std::mt19937_64(seed);
        }
    }

    // start N servers, each with a random color
    network<ctconsensus::message> net;
    std::list<ctconsensus::server> servers;
    bool have_red = false, have_blue = false;
    for (int i = 0; i != N; ++i) {
        bool is_red = coin_flip(randomness);
        if (is_red) {
            have_red = true;
        } else {
            have_blue = true;
        }
        servers.emplace_back(i, N, net, is_red ? "red" : "blue");
        servers.back().consensus().detach();
    }

    // start Nancy, who collects DECIDE messages and validates them
    ctconsensus::nancy(net.input(ctconsensus::nancy_id), N, have_red, have_blue).detach();

    cot::loop();
}
