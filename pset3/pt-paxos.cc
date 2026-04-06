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
    template <typename T>
    void configure_replica_channel(netsim::channel<T>& chan) {
        chan.set_loss(loss); 
        chan.set_verbose(verbose);
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
    uint64_t next; // follower next_decide_slot_ PURPOSE: ACK up to slot, so leader can truncate pending_ and resend PROPOSEs with correct slot numbers
};

struct paxos_decide {
    size_t leader;
    uint64_t slot;
    pancy::request req;
};

struct paxos_probe {
    size_t candidate; // the replica that thinks it might be leader
    uint64_t round; // new round num
};

struct paxos_prepare {
    size_t replica;
    uint64_t round;
    uint64_t ack_round;
    uint64_t next_decide_slot;
};

using paxos_message = std::variant<
    paxos_propose,
    paxos_ack,
    paxos_decide,
    paxos_probe,    
    paxos_prepare
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

template <typename CharT>
struct formatter<paxos_probe, CharT> : formatter<const char*, CharT> {
    using parent = formatter<const char*, CharT>;
    template <typename FormatContext>
    auto format(const paxos_probe& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "PROBE(round={}, from=R{})", m.round, m.candidate);
    }
};

template <typename CharT>
struct formatter<paxos_prepare, CharT> : formatter<const char*, CharT> {
    using parent = formatter<const char*, CharT>;
    template <typename FormatContext>
    auto format(const paxos_prepare& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "PREPARE(round={}, from=R{})", m.round, m.replica);
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

    uint64_t round_ = 0; // pr_s
    uint64_t ack_round_ = 0; // ar_s

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
        inst.tester.configure_replica_channel(*to_replicas_[s]);
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
    // Leader state: Track proposals awaiting a quorum of ACKs
    struct slot_info {
        pancy::request req;
        std::set<size_t> acked_by; // replicas that have ACKed 
        std::set<size_t> decided_by; // replicas that have DECIDEd
    };
    std::map<uint64_t, slot_info> pending_; // slots prop but not yet decided

    // Election state
    struct prepare_response {
        size_t replica;
        uint64_t ack_round;
        uint64_t next_decide_slot;
    };
    std::vector<prepare_response> prepare_responses_; // responses to our PREPAREs
    uint64_t election_round_ = 0; // round trying to win

    // FD
    cot::system_time_point last_leader_msg_ = cot::now();
    constexpr cot::duration LEADER_TIMEOUT = 5s;

    while (true) {
        // receive from clients or replicas
        // NOTE: 200 FOR RETRANSMITION, LEADER_TIMEOUT FOR ELECTION
        auto result = co_await cot::first(from_clients_.receive(), from_replicas_.receive(), cot::after(200ms));

        /* 
            TIMEOUT
            Leader: Retransmit unACKed PROPOSEs and unDECIDED slots
            Non-leader: Check FD: If no response from leader, start PROBE to elect self as leader
        */
        if (std::holds_alternative<std::monostate>(result)) {
            // Leader
            if (index_ == leader_index_) {
                for (auto& [slot, info]: pending_) {
                    if (info.acked_by.size() < quorum_size_) {
                        // Retransmit PROPOSE(pr, V) to all replicas that haven’t ACKed yet
                        paxos_propose prop{index_, slot, info.req};
                        for (size_t i = 0; i < nreplicas_; ++i) {
                            if (!info.acked_by.count(i)) {
                                co_await to_replicas_[i]->send(prop);
                            }
                        }
                    }
                    else {
                        // Retransmit DECIDE(r, min wk) to all replicas that haven’t DECIDEd yet
                        paxos_decide dec{index_, slot, info.req};
                        for (size_t i = 0; i < nreplicas_; ++i) {
                            if (!info.decided_by.count(i)) {
                                co_await to_replicas_[i]->send(dec);   
                            }
                        }
                    }
                }
            }
            // Non-leader
            else {
                if (cot::now() - last_leader_msg_ > LEADER_TIMEOUT) {
                    /*
                        PROBE(r) to all replicas w/r > pr_s
                        unique ruonds w/replica index as lower bits
                    */
                   uint64_t new_round = next_decide_slot_ * nreplicas_ + index_; // unique round number higher than any pr_s
                   if (new_round <= round_) {
                        // someone else alr used higher round; go higher
                        new_round = (round_ / nreplicas_ + 1) * nreplicas_ + index_;
                   }
                   round_ = new_round;
                   election_round_ = new_round;
                   prepare_responses_.clear();

                   // PROBE(r) to all replicas
                   paxos_probe probe{index_, round_};
                   for (size_t i = 0; i < nreplicas_; ++i) {
                        co_await to_replicas_[i]->send(probe);
                   }
                }
            }
            continue;
        }
        
        /*
            Leader
            Client request -> PROPOSE  
        */
        if (auto* req_p = std::get_if<pancy::request>(&result)) {
            // if not leader, redirect
            if (index_ != leader_index_) {
                co_await to_clients_.send(pancy::redirection_response{
                    pancy::response_header(*req_p, pancy::errc::redirect), leader_index_
                });
                continue;
            }

            // don't accept req if election
            if (election_round_ != 0) { continue; }

            // av += 1. assign next slot, record req in pending
            uint64_t slot = next_slot_++;
            pending_[slot] = slot_info{*req_p, {}, {}};

            // PROPOSE(pr, V) to all replicas
            paxos_propose prop{index_, slot, *req_p};
            for (size_t i = 0; i < nreplicas_; ++i) {
                co_await to_replicas_[i]->send(prop);
            }
        }
        
        else {
            auto& msg = std::get<paxos_message>(result);

            /*
                Non-leader
                PROBE received -> PREPARE
                send next_decide_slot_ instead of full AV
                new leader uses to find furthest-ahead rep to sync from
            */
            if (auto* prope_p = std::get_if<paxos_probe>(&msg)) {
                // ignore if old r
                if (prope_p->round < round_) { continue; }

                // update pr_s and abandon curr leader
                round_ = prope_p->round;
                ack_round_ = 0; // reset ar for new round
                leader_index_ = prope_p->candidate;
                last_leader_msg_ = cot::now();

                // PREPARE(ar, r, AV) to all replicas
                paxos_prepare prep{index_, round_, ack_round_, next_decide_slot_};
                co_await to_replicas_[prope_p->candidate]->send(prep);
            }
            /*
                New leader/candidate
                PREPARE received -> PROPOSE
                Find replica w/max next_decide_slot_ as starting point.
                Apply slots missing before proceeding
            */
            else if (auto* prep_p = std::get_if<paxos_prepare>(&msg)) {
                if (prep_p->round != election_round_) { continue; } // stale PREPARE from old election
                if (prep_p->round < round_) {
                    // usurped => abandon election
                    election_round_ = 0;
                    continue;
                }

                prepare_responses_.push_back({prep_p->replica, prep_p->ack_round, prep_p->next_decide_slot});

                if (prepare_responses_.size() == quorum_size_) {
                    // quorum of PREPAREs received => now leader

                    // find max next_decide_slot_ among responses to determine where to sync from
                    uint64_t max_decided = next_decide_slot_;
                    for (const auto& resp: prepare_responses_) {
                        max_decided = std::max(max_decided, resp.next_decide_slot);
                    }

                    // start accepting req from next_slot_ = max_decided
                    // any slot between next_decide_slot_ and max_decided missing filled via PROPOSE retransmits from new round
                    leader_index_ = index_;
                    next_slot_ = max_decided + 1;
                    ack_round_ = round_; 
                    election_round_ = 0; // end election
                    pending_.clear(); // old round pending_ is stale
                    last_leader_msg_ = cot::now();
                }
            }
            
            /*
                Non-leader
                PROPOSE received -> ACK
            */
            else if (auto* prop_p = std::get_if<paxos_propose>(&msg)) {
                if (prop_p->leader != leader_index_) { continue; } // ignore if not from current leader

                ack_round_ = round_;
                paxos_ack ack{index_, prop_p->slot, next_decide_slot_}; 
                co_await to_replicas_[prop_p->leader]->send(ack);
            }

            /*
                Leader
                ACK received -> DECIDE 
            */

            else if (auto* ack_p = std::get_if<paxos_ack>(&msg)) {
                //update decided_by for slots follower has ACKed for
                for (auto& [slot, info]: pending_) {
                    if (ack_p->next > slot) {
                        info.decided_by.insert(ack_p->replica);
                    }
                }

                // Update pending_ with received ACK
                auto it = pending_.find(ack_p->slot);
                if (it == pending_.end()) {continue; } // Stale ACK

                // Check quorum
                it->second.acked_by.insert(ack_p->replica);
                if (it->second.acked_by.size() == quorum_size_) {
                    // DECIDE(r, min wk)
                    paxos_decide dec{index_, ack_p->slot, it->second.req};
                    for (size_t i = 0; i < nreplicas_; ++i) {
                        co_await to_replicas_[i]->send(dec);
                    }
                }

                // TRUNCATE SLOTS 
                // erase front of pending_ while all nreplicas have ACKed and DECIDEd
                while (!pending_.empty()) {
                    auto& [slot, info] = *pending_.begin();
                    if (info.acked_by.size() == nreplicas_ && info.decided_by.size() == nreplicas_) { pending_.erase(pending_.begin()); } 
                    else { break;}
                }
            }

            /*
                ALL
                DECIDE received -> apply req, respond to client
            */
            else if (auto* dec_p = std::get_if<paxos_decide>(&msg)) {
                last_leader_msg_ = cot::now();

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

                    // mark leader as decided for this slot (for truncation)
                    auto pending_it = pending_.find(it->first);
                    if (pending_it != pending_.end()) {
                        pending_it->second.decided_by.insert(index_);
                    }

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

// Fail replica i
cot::task<> fail_replica_after(pt_paxos_instance& inst, size_t i, cot::duration d) {
    co_await cot::after(d);
    for (size_t j = 0; j < inst.replicas.size(); ++j) {
        inst.replicas[i]->to_replicas_[j]->set_loss(1.0); // drop outgoing
    }
    inst.replicas[i]->to_clients_.set_loss(1.0); // drop incoming
}

// Fail and recover
cot::task<> fail_then_recover(pt_paxos_instance& inst, size_t i, cot::duration fail_at, cot::duration recover_at, double original_loss) {
    co_await cot::after(fail_at);
    for (size_t j = 0; j < inst.replicas.size(); ++j) { inst.replicas[i]->to_replicas_[j]->set_loss(1.0); } // drop outgoing 
    inst.replicas[i]->to_clients_.set_loss(1.0); // drop incoming

    co_await cot::after(recover_at - fail_at);
    for (size_t j = 0; j < inst.replicas.size(); ++j) { inst.replicas[i]->to_replicas_[j]->set_loss(original_loss); } // restore outgoing
    inst.replicas[i]->to_clients_.set_loss(original_loss); // restore incoming
}

// Split brain
// replicas can talk to clients but not each other
cot::task<> split_brain_after(pt_paxos_instance& inst, cot::duration d) {
    co_await cot::after(d);
    for (size_t i = 0; i < inst.replicas.size(); ++i) {
        for (size_t j = 0; j < inst.replicas.size(); ++j) {
            if (i != j) { inst.replicas[i]->to_replicas_[j]->set_loss(1.0); } // drop inter-replica messages
        }
    }
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
    // failure scenarios
    tasks.push_back(fail_replica_after(inst, 0, 10s));

    // Wait for `timeout_task`
    cot::loop();

    pancy::pancydb& db = inst.replicas[tester.initial_leader]->db_;

    for (size_t s = 0; s < tester.nreplicas; ++s) {
        if (s == tester.initial_leader) continue;
        if (auto key = db.diff(inst.replicas[s]->db_, 20)) {
            std::print(std::clog, "*** FAILURE on seed {} at replica {} key {}\n", seed, s, *key);
            inst.replicas[s]->db_.print_near(*key, std::clog);
            return false;
        }
    }

    // Check database
    std::print("{} lock, {} write, {} clear, {} unlock\n",
               clients.lock_complete, clients.write_complete,
               clients.clear_complete, clients.unlock_complete);
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
