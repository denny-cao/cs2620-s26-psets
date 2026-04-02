#include "lockseq_model.hh"
#include "pancydb.hh"
#include "netsim.hh"
#include <map>
#include <set>
#include <variant>

namespace cot = cotamer;
using namespace std::chrono_literals;

// testinfo
//    Holds configuration information about this test.

struct testinfo {
    random_source randomness;
    double loss = 0.0;
    bool verbose = false;
    bool print_db = false;
    size_t nreplicas = 3;
    size_t initial_leader = 0;

    template <typename T>
    void configure_port(netsim::port<T>& port) {
        port.set_verbose(verbose);
    }
    template <typename T>
    void configure_channel(netsim::channel<T>& chan) {
        chan.set_loss(loss);
        chan.set_verbose(verbose);
    }
    template <typename T>
    void configure_quiet_channel(netsim::channel<T>& chan) {
        chan.set_loss(loss);
    }
};


// pt_paxos_replica, pt_paxos_instance
//    Manage a test of a Paxos-based Pancy service.
//    Initialization is more complicated than in the simpler settings;
//    we have a type, `pt_paxos_replica`, that represents a single replica,
//    and another, `pt_paxos_instance`, that constructs the replica set.

struct pt_paxos_instance;

struct paxos_propose {
    size_t leader;
    uint64_t slot;
    pancy::request req;
};

struct paxos_ack {
    size_t replica;
    uint64_t slot;
};

struct paxos_decide {
    size_t leader;
    uint64_t slot;
    pancy::request req;
};

using paxos_message = std::variant<
    paxos_propose,
    paxos_ack,
    paxos_decide
>;

namespace std {
template <typename CharT>
struct formatter<paxos_propose, CharT> : formatter<const char*, CharT> {
    using parent = formatter<const char*, CharT>;
    template <typename FormatContext>
    auto format(const paxos_propose& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "PROPOSE(slot={}, serial={})", m.slot, pancy::message_serial(m.req));
    }
};

template <typename CharT>
struct formatter<paxos_ack, CharT> : formatter<const char*, CharT> {
    using parent = formatter<const char*, CharT>;
    template <typename FormatContext>
    auto format(const paxos_ack& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "ACK(slot={}, from=R{})", m.slot, m.replica);
    }
};

template <typename CharT>
struct formatter<paxos_decide, CharT> : formatter<const char*, CharT> {
    using parent = formatter<const char*, CharT>;
    template <typename FormatContext>
    auto format(const paxos_decide& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "DECIDE(slot={}, serial={})", m.slot, pancy::message_serial(m.req));
    }
};
}

namespace netsim {
template <>
struct message_traits<paxos_message> {
    static auto print_transform(const paxos_message& m) {
        return std::visit([](const auto& msg) {
            return std::format("{}", msg);
        }, m);
    }
};
}

struct pt_paxos_replica {
    size_t index_;           // index of this replica in the replica set
    size_t nreplicas_;       // number of replicas
    size_t quorum_size_ = nreplicas_ / 2 + 1; // number of ACKs needed for quorum
    size_t leader_index_;    // this replica’s idea of the current leader
    netsim::port<pancy::request> from_clients_;   // port for client messages
    netsim::port<paxos_message> from_replicas_;   // port for inter-replica messages
    netsim::channel<pancy::response> to_clients_; // channel for client responses
    // channels for inter-replica messages:
    std::vector<std::unique_ptr<netsim::channel<paxos_message>>> to_replicas_;
    pancy::pancydb db_;      // our copy of the database
    uint64_t next_slot_ = 1; // next slot leader will assign (Each client req appends one entry to AV)
    uint64_t next_decide_slot_ = 1; // length of decided and applied prefix 
    std::map<uint64_t, pancy::request> decided_reqs_; // decided but not yet applied slots 

    pt_paxos_replica(size_t index, size_t nreplicas, random_source&);
    void initialize(pt_paxos_instance&);

    cot::task<> run();
};

struct pt_paxos_instance {
    testinfo& tester;
    client_model& clients;
    std::vector<std::unique_ptr<pt_paxos_replica>> replicas;
    // ...plus anything you want to add

    pt_paxos_instance(testinfo&, client_model&);
};


// Configuration and initialization

pt_paxos_replica::pt_paxos_replica(size_t index, size_t nreplicas, random_source& randomness)
    : index_(index),
      nreplicas_(nreplicas),
      from_clients_(randomness, std::format("R{}", index_)),
      from_replicas_(randomness, std::format("R{}/r", index_)),
      to_clients_(randomness, from_clients_.id()),
      to_replicas_(nreplicas) {
    for (size_t s = 0UL; s != nreplicas_; ++s) {
        to_replicas_[s].reset(new netsim::channel<paxos_message>(
            randomness, from_clients_.id()
        ));
    }
}

void pt_paxos_replica::initialize(pt_paxos_instance& inst) {
    leader_index_ = inst.tester.initial_leader;
    inst.clients.connect_replica(index_, from_clients_, to_clients_);
    inst.tester.configure_port(from_clients_);
    inst.tester.configure_port(from_replicas_);
    inst.tester.configure_channel(to_clients_);
    inst.tester.configure_quiet_channel(inst.clients.request_channel(index_));
    for (size_t s = 0UL; s != nreplicas_; ++s) {
        to_replicas_[s]->connect(inst.replicas[s]->from_replicas_);
        inst.tester.configure_channel(*to_replicas_[s]);
    }
}

pt_paxos_instance::pt_paxos_instance(testinfo& tester, client_model& clients)
    : tester(tester), clients(clients), replicas(tester.nreplicas) {
    for (size_t s = 0UL; s != tester.nreplicas; ++s) {
        replicas[s].reset(new pt_paxos_replica(s, tester.nreplicas, tester.randomness));
    }
    for (size_t s = 0UL; s != tester.nreplicas; ++s) {
        replicas[s]->initialize(*this);
    }
}



// ********** PANCY SERVICE CODE **********

cot::task<> pt_paxos_replica::run() {
    // Leader: Track proposals awaiting a quorum of ACKs
    struct slot_info {
        pancy::request req;
        size_t acks = 0; // number of ACKs received so far; QUORUM
    };
    std::map<uint64_t, slot_info> pending_; // slots prop but not yet decided


    while (true) {
        // receive from clients or replicas
        auto result = co_await cot::first(from_clients_.receive(), from_replicas_.receive());

        // PROPOSE PHASE (LEADER)
        // Skip PROBE/PREP because leader_index_ fixed; never change. STABLE LEADER!
        if (auto* req_p = std::get_if<pancy::request>(&result)) {
            // if not leader, redirect
            if (index_ != leader_index_) {
                co_await to_clients_.send(pancy::redirection_response{
                    pancy::response_header(*req_p, pancy::errc::redirect), leader_index_
                });
                continue;
            }

            // av += 1. assign next slot, record req in pending
            uint64_t slot = next_slot_++;
            pending_[slot] = slot_info{*req_p, 0};

            // PROPOSE(pr, V) to all replicas
            paxos_propose prop{index_, slot, *req_p};
            for (size_t i = 0; i < nreplicas_; ++i) {
                co_await to_replicas_[i]->send(prop);
            }
        }
        
        else {
            auto& msg = std::get<paxos_message>(result);

            // PROPOSE PHASE (FOLLOWERS)
            if (auto* prop_p = std::get_if<paxos_propose>(&msg)) {
                // ACK(ar, |AV|) 
                // slot number is |AV|!!!! prefix we ack
                co_await to_replicas_[prop_p->leader]->send(paxos_ack{index_, prop_p->slot});
            }

            // ACK PHASE (LEADER)
            else if (auto* ack_p = std::get_if<paxos_ack>(&msg)) {
                // Update pending_ with received ACK
                auto it = pending_.find(ack_p->slot);
                if (it == pending_.end()) {continue; } // Stale ACK

                // Check quorum
                if (++it->second.acks == quorum_size_) {
                    // DECIDE(r, min wk)
                    paxos_decide dec{index_, ack_p->slot, it->second.req};
                    for (size_t i = 0; i < nreplicas_; ++i) {
                        co_await to_replicas_[i]->send(dec);
                    }
                }
            }

            // DECIDE PHASE (ALL)
            else if (auto* dec_p = std::get_if<paxos_decide>(&msg)) {
                decided_reqs_[dec_p->slot] = dec_p->req;

                // apply all decided but not yet applied reqs in order
                while (true) {
                    auto it = decided_reqs_.find(next_decide_slot_);
                    if (it == decided_reqs_.end()) { break; } // No more decided reqs to apply

                    // apply req to db, respond to client, remove from pending_ and decided_reqs_
                    pancy::response resp = db_.process_req(it->second);
                    if (index_ == leader_index_) {
                        co_await to_clients_.send(resp);
                    }


                    pending_.erase(it->first);
                    decided_reqs_.erase(it);
                    ++next_decide_slot_;
                }
            }
        }
    }
}

// ******** end Pancy service code ********



// Test functions

cot::task<> clear_after(cot::duration d) {
    co_await cot::after(d);
    cot::clear();
}

bool try_one_seed(testinfo& tester, unsigned long seed) {
    cot::reset();   // clear old events and coroutines
    tester.randomness.seed(seed);

    // Create client generator and test instance
    lockseq_model clients(tester.nreplicas, tester.randomness);
    pt_paxos_instance inst(tester, clients);

    // Start coroutines
    clients.start();
    std::vector<cot::task<>> tasks;
    for (size_t s = 0UL; s != tester.nreplicas; ++s) {
        tasks.push_back(inst.replicas[s]->run());
    }
    cot::task<> timeout_task = clear_after(100s);

    // Wait for `timeout_task`
    cot::loop();

    // Check database
    std::print("{} lock, {} write, {} clear, {} unlock\n",
               clients.lock_complete, clients.write_complete,
               clients.clear_complete, clients.unlock_complete);
    pancy::pancydb& db = inst.replicas[tester.initial_leader]->db_;
    if (auto problem = clients.check(db)) {
        std::print(std::clog, "*** FAILURE on seed {} at key {}\n", seed, *problem);
        db.print_near(*problem, std::clog);
        return false;
    } else if (tester.print_db) {
        db.print(std::cout);
    }
    return true;
}


// Argument parsing

static struct option options[] = {
    { "count", required_argument, nullptr, 'n' },
    { "seed", required_argument, nullptr, 'S' },
    { "random-seeds", required_argument, nullptr, 'R' },
    { "loss", required_argument, nullptr, 'l' },
    { "verbose", no_argument, nullptr, 'V' },
    { "print-db", no_argument, nullptr, 'p' },
    { "quiet", no_argument, nullptr, 'q' },
    { nullptr, 0, nullptr, 0 }
};

int main(int argc, char* argv[]) {
    testinfo tester;

    std::optional<unsigned long> first_seed;
    unsigned long seed_count = 1;

    auto shortopts = short_options_for(options);
    int ch;
    while ((ch = getopt_long(argc, argv, shortopts.c_str(), options, nullptr)) != -1) {
        if (ch == 'S') {
            first_seed = from_str_chars<unsigned long>(optarg);
        } else if (ch == 'R') {
            seed_count = from_str_chars<unsigned long>(optarg);
        } else if (ch == 'l') {
            tester.loss = from_str_chars<double>(optarg);
        } else if (ch == 'n') {
            tester.nreplicas = from_str_chars<size_t>(optarg);
        } else if (ch == 'V') {
            tester.verbose = true;
        } else if (ch == 'p') {
            tester.print_db = true;
        } else {
            std::print(std::cerr, "Unknown option\n");
            return 1;
        }
    }

    bool ok;
    if (first_seed) {
        ok = try_one_seed(tester, *first_seed);
    } else {
        std::mt19937_64 seed_generator = randomly_seeded<std::mt19937_64>();
        for (unsigned long i = 0; i != seed_count; ++i) {
            if (i > 0 && i % 1000 == 0) {
                std::print(std::cerr, ".");
            }
            unsigned long seed = seed_generator();
            ok = try_one_seed(tester, seed);
            if (!ok) {
                break;
            }
        }
        if (ok && seed_count >= 1000) {
            std::print(std::cerr, "\n");
        }
    }
    return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
