#include "lockseq_model.hh"
#include "pancydb.hh"
#include "netsim.hh"
#include <map>
#include <set>
#include <variant>
#include <vector>
#include <optional>

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

// From l: PROBE(r)
// r: round number
// |DV|: length of decided value sequence (TRUNCATION)
struct paxos_probe {
    size_t l;
    uint64_t r;
    uint64_t dv_len;
};

// From s: PREPARE(pr, ar, AV)
// r: round number
// ar: ack round
// AV: ack value sequence
struct paxos_prepare {
    size_t s;
    uint64_t r;
    uint64_t ar;
    std::vector<pancy::request> AV;
};

// From s: PROPOSE(r, V)
// r: round number
// V: proposed value sequence
struct paxos_propose {
    size_t s;
    uint64_t r;
    std::vector<pancy::request> AV;
};

// From s: ACK(r)
// r: round number
// w: ack length
// |DV|: length of decided value sequence at s
struct paxos_ack {
    size_t s;
    uint64_t r;
    uint64_t w;
    uint64_t dv_len;
};

// From l: DECIDE(r)
// r: round number
// min wk: prefix understood by quorum
struct paxos_decide {
    size_t s;
    uint64_t r;
    uint64_t min_w;
};

// From s -> Nancy: DECIDE(DV)
// DV: decided value sequence
struct paxos_decide_nancy {
    size_t s;
    std::vector<pancy::request> V;
};

using paxos_message = std::variant<
    paxos_probe,
    paxos_prepare,
    paxos_propose,
    paxos_ack,
    paxos_decide,
    paxos_decide_nancy
>;

namespace std {
template <typename CharT>
struct formatter<paxos_probe, CharT> : formatter<const char*, CharT> {
    template <typename FormatContext>
    auto format(const paxos_probe& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "PROBE(r={}, dv_len={}, from=R{})", m.r, m.dv_len, m.l);
    }
};

template <typename CharT>
struct formatter<paxos_prepare, CharT> : formatter<const char*, CharT> {
    template <typename FormatContext>
    auto format(const paxos_prepare& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "PREPARE(r={}, ar={}, |AV|={}, from=R{})", m.r, m.ar, m.AV.size(), m.s);
    }
};

template <typename CharT>
struct formatter<paxos_propose, CharT> : formatter<const char*, CharT> {
    template <typename FormatContext>
    auto format(const paxos_propose& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "PROPOSE(r={}, |AV|={}, from=R{})", m.r, m.AV.size(), m.s);
    }
};

template <typename CharT>
struct formatter<paxos_ack, CharT> : formatter<const char*, CharT> {
    template <typename FormatContext>
    auto format(const paxos_ack& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "ACK(r={}, w={}, dv_len={}, from=R{})", m.r, m.w, m.dv_len, m.s);
    }
};

template <typename CharT>
struct formatter<paxos_decide, CharT> : formatter<const char*, CharT> {
    template <typename FormatContext>
    auto format(const paxos_decide& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "DECIDE(r={}, min_w={}, from=R{})", m.r, m.min_w, m.s);
    }
};

template <typename CharT>
struct formatter<paxos_decide_nancy, CharT> : formatter<const char*, CharT> {
    template <typename FormatContext>
    auto format(const paxos_decide_nancy& m, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "DECIDE_NANCY(|V|={}, from=R{})", m.V.size(), m.s);
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
    size_t quorum_size_;     // number of ACKs needed for quorum
    size_t leader_index_;    // this replica's idea of the current leader
    netsim::port<pancy::request> from_clients_;   // port for client messages
    netsim::port<paxos_message> from_replicas_;   // port for inter-replica messages
    netsim::channel<pancy::response> to_clients_; // channel for client responses
    // channels for inter-replica messages:
    std::vector<std::unique_ptr<netsim::channel<paxos_message>>> to_replicas_;
    pancy::pancydb db_;      // our copy of the database

    // Multi-Paxos state
    std::vector<pancy::request> IV_; // initial value sequence
    uint64_t r_ = 1;                 // phase 1: stable leader round
    uint64_t pr_ = 0;                // probe round
    uint64_t ar_ = 0;                // ack round
    std::vector<pancy::request> AV_; // ack value sequence
    uint64_t decided_len_ = 0;       // |DV| length of decided prefix of AV_

    // Outstanding client requests / responses by slot.
    std::map<uint64_t, pancy::request> slot_to_request_;
    std::map<uint64_t, pancy::response> slot_to_response_;
    std::set<uint64_t> pending_client_slots_;

    struct proposal_state {
        bool active = false;
        uint64_t r = 0;
        uint64_t proposed_len = 0;
        std::set<size_t> ackers;
    } proposal_;

    pt_paxos_replica(size_t index, size_t nreplicas, random_source&);
    void initialize(pt_paxos_instance&);

    void check_invariants() const;
    void apply_up_to(uint64_t w);
    cot::task<> broadcast_replicas(const paxos_message& m);
    cot::task<> maybe_decide_current_proposal();
    cot::task<> maybe_start_proposal();
    cot::task<> handle_client_request(const pancy::request& req);
    cot::task<> handle_propose(const paxos_propose& prop);
    cot::task<> handle_ack(const paxos_ack& ack);
    cot::task<> handle_decide(const paxos_decide& decide);
    cot::task<> handle_replica_message(const paxos_message& msg);
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
      quorum_size_(nreplicas / 2 + 1),
      leader_index_(0),
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

void pt_paxos_replica::check_invariants() const {
    // Accept a same-round PROPOSE => AV_ does not shrink
    assert(pr_ >= ar_); 
    assert(AV_.size() >= decided_len_);
}

void pt_paxos_replica::apply_up_to(uint64_t w) {
    w = std::min<uint64_t>(w, AV_.size());
    while (decided_len_ < w) {
        uint64_t slot = decided_len_;
        pancy::response resp = db_.process_req(AV_[slot]);
        slot_to_response_[slot] = resp;
        ++decided_len_;
    }
}

cot::task<> pt_paxos_replica::broadcast_replicas(const paxos_message& m) {
    for (size_t i = 0; i != nreplicas_; ++i) {
        co_await to_replicas_[i]->send(m);
    }
}

cot::task<> pt_paxos_replica::maybe_decide_current_proposal() {
    if (!proposal_.active) { co_return; }
    if (proposal_.ackers.size() < quorum_size_) { co_return; }

    uint64_t min_w = proposal_.proposed_len;
    paxos_decide decide{index_, proposal_.r, min_w};
    proposal_.active = false;
    co_await broadcast_replicas(decide);
}

cot::task<> pt_paxos_replica::maybe_start_proposal() {
    if (index_ != leader_index_) { co_return; }
    if (proposal_.active) { co_return; }
    if (AV_.size() >= IV_.size()) { co_return; }

    // Phase 1: extend by one queued client request at a time
    uint64_t slot = AV_.size();
    AV_.push_back(IV_[slot]);
    slot_to_request_[slot] = IV_[slot];
    pending_client_slots_.insert(slot);

    proposal_.active = true;
    proposal_.r = r_;
    proposal_.proposed_len = AV_.size();
    proposal_.ackers.clear();
    proposal_.ackers.insert(index_); // leader counts itself

    paxos_propose prop{index_, r_, AV_};
    co_await broadcast_replicas(prop);
    co_await maybe_decide_current_proposal();
}

cot::task<> pt_paxos_replica::handle_client_request(const pancy::request& req) {
    if (index_ != leader_index_) {
        co_await to_clients_.send(pancy::redirection_response{
            pancy::response_header(req, pancy::errc::redirect), leader_index_
        });
        co_return;
    }

    // queue the client request in the leader's initial value sequence
    IV_.push_back(req);
    check_invariants();

    co_await maybe_start_proposal();
}

cot::task<> pt_paxos_replica::handle_propose(const paxos_propose& prop) {
    if (prop.r < pr_) {
        co_return;
    }
    if (prop.r == ar_ && prop.AV.size() < AV_.size()) {
        co_return;
    }

    pr_ = prop.r;
    ar_ = prop.r;
    AV_ = prop.AV;

    check_invariants();

    paxos_ack ack{index_, ar_, static_cast<uint64_t>(AV_.size()), decided_len_};
    co_await to_replicas_[prop.s]->send(ack);
}

cot::task<> pt_paxos_replica::handle_ack(const paxos_ack& ack) {
    if (index_ != leader_index_) { co_return; }
    if (!proposal_.active) { co_return; }
    if (ack.r != proposal_.r) { co_return; }
    if (ack.w < proposal_.proposed_len) { co_return; }

    proposal_.ackers.insert(ack.s);
    check_invariants();
    co_await maybe_decide_current_proposal();
}

cot::task<> pt_paxos_replica::handle_decide(const paxos_decide& decide) {
    if (decide.r > ar_) { co_return; }

    uint64_t old_decided = decided_len_;
    uint64_t w = std::min<uint64_t>(decide.min_w, AV_.size());
    apply_up_to(w);

    check_invariants();

    if (index_ == leader_index_) {
        for (uint64_t slot = old_decided; slot < decided_len_; ++slot) {
            if (pending_client_slots_.contains(slot)) {
                co_await to_clients_.send(slot_to_response_.at(slot));
                pending_client_slots_.erase(slot);
            }
        }
        co_await maybe_start_proposal();
    }
}

cot::task<> pt_paxos_replica::handle_replica_message(const paxos_message& msg) {
    if (auto* prop = std::get_if<paxos_propose>(&msg)) { co_await handle_propose(*prop); } 
    else if (auto* ack = std::get_if<paxos_ack>(&msg)) { co_await handle_ack(*ack); } 
    else if (auto* decide = std::get_if<paxos_decide>(&msg)) { co_await handle_decide(*decide); }
    co_return;
}
cot::task<> pt_paxos_replica::run() {
    while (true) {
        auto result = co_await cot::first(from_clients_.receive(), from_replicas_.receive());
        if (auto* req = std::get_if<pancy::request>(&result)) { co_await handle_client_request(*req); } 
        else if (auto* msg = std::get_if<paxos_message>(&result)) { co_await handle_replica_message(*msg); }
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
