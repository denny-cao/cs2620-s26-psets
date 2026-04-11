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
};

struct pt_paxos_instance;

// -----------------------------------------------------------------------------
// Multi-Paxos messages
// -----------------------------------------------------------------------------

// SEND_l->j (PROBE, r)
// |DV_l| metadata: tell followers how much is definitely decided => safe truncation point
struct mp_probe { 
    size_t leader; 
    uint64_t r; 
    uint64_t dv; 
};

// SEND_l->j (PREPARE, pr, ar, base, applied, AV)
// base: absolute slot number of AV's start
// applied: how much of the log the follower has already applied/decided to compute truncation point
// AV: suffix beginning at base with length |AV|
struct mp_prepare {
    size_t server; 
    uint64_t pr; 
    uint64_t ar; 
    uint64_t base; 
    uint64_t applied; 
    std::vector<pancy::request> AV; 
};

// SEND_l->j (PROPOSE, pr, base, AV)
// base: absolute slot number of AV's start
// AV: suffix beginning at base with length |AV|  
struct mp_propose {
    size_t leader; 
    uint64_t pr; 
    uint64_t base; 
    std::vector<pancy::request> AV; 
};

// SEND_s->l (ACK, ar, w, applied)
// applied: absolute length of log already applied/decided by follower (TRUNCATION)
struct mp_ack { 
    size_t server; 
    uint64_t ar; 
    uint64_t w; 
    uint64_t applied; 
};

// SEND(DECIDE, r, w, trim)
// trim: absolute slot number before which all replicas can safely truncate their logs
struct mp_decide  { 
    size_t leader; 
    uint64_t r;  
    uint64_t w; 
    uint64_t trim; 
};

using paxos_message = std::variant<mp_probe, mp_prepare, mp_propose, mp_ack, mp_decide>;

// -----------------------------------------------------------------------------
// Formatting for messages
// -----------------------------------------------------------------------------
namespace std {
    template <typename CharT> struct formatter<mp_probe, CharT>   : formatter<const char*, CharT> {
        template <typename Ctx> 
        auto format(const mp_probe& m,   Ctx& ctx) const { 
            return std::format_to(ctx.out(), "PROBE(r={}, dv={}, from=R{})",           
                                  m.r, m.dv, m.leader); 
        } 
    };

    template <typename CharT> struct formatter<mp_prepare, CharT> : formatter<const char*, CharT> {
        template <typename Ctx> 
        auto format(const mp_prepare& m, Ctx& ctx) const { 
            return std::format_to(ctx.out(), "PREPARE(pr={}, ar={}, base={}, applied={}, |AV|={}, from=R{})", 
                                  m.pr, m.ar, m.base, m.applied, m.AV.size(), m.server); 
        } 
    };

    template <typename CharT> struct formatter<mp_propose, CharT> : formatter<const char*, CharT> {
        template <typename Ctx> 
        auto format(const mp_propose& m, Ctx& ctx) const { 
            return std::format_to(ctx.out(), "PROPOSE(pr={}, base={}, |AV|={}, from=R{})",  
                                  m.pr, m.base, m.AV.size(), m.leader); 
        } 
    };

    template <typename CharT> struct formatter<mp_ack, CharT>     : formatter<const char*, CharT> {
        template <typename Ctx> 
        auto format(const mp_ack& m,     Ctx& ctx) const { 
            return std::format_to(ctx.out(), "ACK(ar={}, w={}, applied={}, from=R{})",          
                                  m.ar, m.w, m.applied, m.server); 
        } 
    };

    template <typename CharT> struct formatter<mp_decide, CharT>  : formatter<const char*, CharT> {
        template <typename Ctx> 
        auto format(const mp_decide& m,  Ctx& ctx) const { 
            return std::format_to(ctx.out(), "DECIDE(r={}, w={}, trim={}, from=R{})",         
                                  m.r, m.w, m.trim, m.leader); 
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
    size_t index_;
    size_t nreplicas_;
    size_t quorum_size_;
    size_t leader_index_;

    netsim::port<pancy::request> from_clients_;
    netsim::port<paxos_message> from_replicas_;
    netsim::channel<pancy::response> to_clients_;
    std::vector<std::unique_ptr<netsim::channel<paxos_message>>> to_replicas_;

    pancy::pancydb db_;

    // --- Multi-Paxos protocol state ---

    std::vector<pancy::request> IVs_;   // all requests not yet truncated by the leader (INITIAL VALUES WAITING)
    uint64_t iv_base_ = 0;              // absolute slot of IVs_[0]
    uint64_t prs_ = 0;
    uint64_t ars_ = 0;
    std::vector<pancy::request> AVs_;   // ACK suffix beginning at log_start_ (CHANGED TO SUFFIX FOR TRUNCATION)
    uint64_t log_start_ = 0;            // absolute slot of AVs_[0]
    uint64_t decided_len_ = 0;          // absolute num of decided/applied slots |DV|

    // --- Leader state ---
    struct leader_round_state {
        uint64_t r = 0;                 // current leader round
        bool active = false;            // whether we believe we're active leader in this round

        struct prepare_info {
            uint64_t ar;                // follower's ack round
            uint64_t base;              // where follower's AV suffix begins
            uint64_t applied;           // follower's applied/decided prefix length
            std::vector<pancy::request> AV;
        };

        // PREPARE messages collected during round
        std::map<size_t, prepare_info> prepare_q;

        // latest acked absolute lengths from replicas in current round
        // leader can decide the min over a quorum
        std::map<size_t, uint64_t> ack_q;

        // latest known applied lengths from replicas
        // used for truncation / garbage collection
        std::map<size_t, uint64_t> applied_q;

        // curr proposal value suffix for this round
        std::vector<pancy::request> proposed_AV;
        uint64_t proposed_base = 0;

        // highest absolute decided length known by the leader in this round
        uint64_t decided_w = 0;

        // highest safe trim point known by the leader
        uint64_t trim_w = 0;
    } leader_;

    // --- Client Dedup ---
    std::map<uint64_t, pancy::response> committed_response_by_serial_; // if client retries exact same req, leader can answer immediately via map
    uint64_t client_requests_seen_ = 0;
    uint64_t dedup_hits_ = 0;
    uint64_t consensus_inserts_ = 0;

    // --- Timing ---
    // last time we heard from leader
    cot::system_time_point last_leader_msg_ = cot::now();

    // failure detector timeout for starting a new probe
    static constexpr cot::duration LEADER_TIMEOUT_ = 5s;

    // periodic timer for retransmission
    static constexpr cot::duration RETRANSMIT_PERIOD_ = 200ms;

    // --- Invariant tracking ---
    // DEBUGGING
    uint64_t prev_decided_len_ = 0;
    uint64_t prev_ars_ = 0;
    uint64_t prev_av_abs_len_same_round_ = 0;

    pt_paxos_replica(size_t index, size_t nreplicas, random_source& randomness);
    void initialize(pt_paxos_instance&);

    void check_invariants();
    void check_value_message_invariant(uint64_t base, const std::vector<pancy::request>& V, const char* msg_type);

    // check if current round is usurped by a higher round (i.e. if leader should step down)
    bool current_round_usurped() const { return leader_.active && prs_ > leader_.r; }

    // absolute length of curr ACK log
    uint64_t av_abs_len() const { return log_start_ + AVs_.size(); }

    // absolute length of curr leader proposal
    uint64_t leader_proposed_abs_len() const { return leader_.proposed_base + leader_.proposed_AV.size(); }

    cot::task<> broadcast(const paxos_message& m);
    cot::task<> apply_decisions_up_to(uint64_t limit);
    cot::task<> decide();
    cot::task<> trim();
    cot::task<> send_extension_propose();
    cot::task<> probe();

    void truncate_prefix(uint64_t new_start);
    void local_truncate(uint64_t trim_to);
    static std::vector<pancy::request> suffix_from(uint64_t base, const std::vector<pancy::request>& seq, uint64_t from);
    static bool is_extension(uint64_t old_base, const std::vector<pancy::request>& old_seq,
                             uint64_t new_base, const std::vector<pancy::request>& new_seq);

    static std::tuple<uint64_t, uint64_t, std::vector<pancy::request>>
    best_prepare(const std::map<size_t, leader_round_state::prepare_info>& q);

    cot::task<> handle_timeout();
    cot::task<> handle_client_request(const pancy::request& req);
    cot::task<> handle_probe(const mp_probe& m);
    cot::task<> handle_prepare(const mp_prepare& m);
    cot::task<> handle_propose(const mp_propose& m);
    cot::task<> handle_ack(const mp_ack& m);
    cot::task<> handle_decide(const mp_decide& m);

    cot::task<> run();
};

struct pt_paxos_instance {
    testinfo& tester;
    client_model& clients;
    std::vector<std::unique_ptr<pt_paxos_replica>> replicas;

    pt_paxos_instance(testinfo&, client_model&);
};

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

static bool equal_request(const pancy::request& a, const pancy::request& b) {
    // treat req as equal if their serialized form matches
    // EXTENSIONS
    return pancy::message_serial(a) == pancy::message_serial(b);
}

static uint64_t request_serial_key(const pancy::request& req) {
    // get serial
    return pancy::message_serial(req);
}

std::vector<pancy::request> pt_paxos_replica::suffix_from(uint64_t base, const std::vector<pancy::request>& seq, uint64_t from) {
    // given seq starts at absolute slot base, return suffix starting at absolute slot from
    if (from <= base) { return seq; } // whole seq is suffix if from is at or before base
    else if (from >= base + seq.size()) { return {}; } // empty suffix if from is past the end of seq
    return std::vector<pancy::request>(seq.begin() + (from - base), seq.end()); // otherwise return suffix starting at from
}

bool pt_paxos_replica::is_extension(uint64_t old_base, const std::vector<pancy::request>& old_seq,
                                    uint64_t new_base, const std::vector<pancy::request>& new_seq) {
    // true if (new_base, new_seq) contains (old_base, old_seq) as a prefix over the overlapping absolute slots
    // same round prop => extension

    if (new_base > old_base) { return false; }
    else if (new_base + new_seq.size() < old_base + old_seq.size()) { return false; }
    for (uint64_t abs = old_base; abs < old_base + old_seq.size(); ++abs) {
        const auto& old_req = old_seq[abs - old_base];
        const auto& new_req = new_seq[abs - new_base];
        if (!equal_request(old_req, new_req)) { return false; }
    }
    return true;
}

static std::vector<pancy::request> extend_with_initial_values(
        uint64_t base,
        const std::vector<pancy::request>& base_seq,
        uint64_t iv_base,
        const std::vector<pancy::request>& IVs) {
    // starting from some chosen base sequence, append any local initial values that continue after it.
    // leader chooses V by taking best known ACK seq and appending >= 0 elements of IV seq

    std::vector<pancy::request> V = base_seq;
    uint64_t next_abs = base + V.size();
    uint64_t iv_end = iv_base + IVs.size();
    for (uint64_t abs = next_abs; abs < iv_end; ++abs) { V.push_back(IVs[abs - iv_base]); }
    return V;
}

// -----------------------------------------------------------------------------
// Constructor and Initializer
// -----------------------------------------------------------------------------

pt_paxos_replica::pt_paxos_replica(size_t index, size_t nreplicas, random_source& randomness)
    : index_(index), 
      nreplicas_(nreplicas), 
      quorum_size_(nreplicas / 2 + 1), 
      leader_index_(0),
      from_clients_(randomness, std::format("R{}", index_)),
      from_replicas_(randomness, std::format("R{}/r", index_)),
      to_clients_(randomness, from_clients_.id()),
      to_replicas_(nreplicas) {
    for (size_t s = 0; s != nreplicas_; ++s) {
        to_replicas_[s] = std::make_unique<netsim::channel<paxos_message>>(randomness, from_clients_.id());
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
    for (size_t s = 0; s != tester.nreplicas; ++s) {
        replicas[s] = std::make_unique<pt_paxos_replica>(s, tester.nreplicas, tester.randomness);
    }
    for (size_t s = 0; s != tester.nreplicas; ++s) {
        replicas[s]->initialize(*this);
    }
}

// -----------------------------------------------------------------------------
// Invariant Helpers
// -----------------------------------------------------------------------------

void pt_paxos_replica::check_invariants() {
    // prs >= ars
    if (prs_ < ars_) { throw std::runtime_error(std::format("R{} invariant: prs_ ({}) < ars_ ({})", 
                                                            index_, prs_, ars_)); }

    // |AV| >= |DV|
    if (av_abs_len() < decided_len_) { throw std::runtime_error(std::format("R{} invariant: |AV_abs| ({}) < decided_len_ ({})",
                                                                            index_, av_abs_len(), decided_len_)); }

    // decided length should never go backwards.
    if (decided_len_ < prev_decided_len_) { throw std::runtime_error(std::format("R{} invariant: decided_len_ decreased {} -> {}", 
                                                                                 index_, prev_decided_len_, decided_len_)); }

    // Because AVs_ starts at log_start_, the start cannot move beyond what has already been decided locally
    if (log_start_ > decided_len_) { throw std::runtime_error(std::format("R{} invariant: log_start_ ({}) > decided_len_ ({})", 
                                                                          index_, log_start_, decided_len_)); }

    prev_decided_len_ = decided_len_;

    // Same-round monotonicity:
    // within a fixed ars_, the acknowledged absolute length should not shrink
    uint64_t cur_abs_len = av_abs_len();
    if (ars_ == prev_ars_) {
        if (cur_abs_len < prev_av_abs_len_same_round_) {throw std::runtime_error(std::format("R{} invariant: |AV_abs| decreased within round {} ({} -> {})",
                                                                                             index_, ars_, prev_av_abs_len_same_round_, cur_abs_len)); }
    } 
    else { prev_ars_ = ars_; }
    prev_av_abs_len_same_round_ = cur_abs_len;
}

void pt_paxos_replica::check_value_message_invariant(uint64_t base, const std::vector<pancy::request>& V, const char* msg_type) {
    // length of value sequence in any non-ignored PREPARE/PROPOSE >= |DV|
    if (base + V.size() < decided_len_) {
        throw std::runtime_error(std::format("R{} invariant: non-ignored {} has base+|V|={} < decided_len_={}",
                                            index_, msg_type, base + V.size(), decided_len_));
    }
}

// -----------------------------------------------------------------------------
// Protocol Helpers
// -----------------------------------------------------------------------------

cot::task<> pt_paxos_replica::broadcast(const paxos_message& m) {
    // Send the same Paxos message to every replica, including self
    for (size_t i = 0; i != nreplicas_; ++i) { co_await to_replicas_[i]->send(m); }
}

void pt_paxos_replica::truncate_prefix(uint64_t new_start) {
    // drop acknowledged/applied prefix up to absolute slot new_start
    if (new_start <= log_start_) { return; } // nothing to drop
    uint64_t drop = std::min<uint64_t>(new_start - log_start_, AVs_.size());
    AVs_.erase(AVs_.begin(), AVs_.begin() + drop);
    log_start_ += drop;

    // also discard init vals before truncation point
    uint64_t iv_drop_to = std::min<uint64_t>(new_start, iv_base_ + IVs_.size());
    if (iv_drop_to > iv_base_) {
        uint64_t iv_drop = iv_drop_to - iv_base_;
        IVs_.erase(IVs_.begin(), IVs_.begin() + iv_drop);
        iv_base_ = iv_drop_to;
    }
}

void pt_paxos_replica::local_truncate(uint64_t trim_to) {
    // only truncate decided/applied entries, never beyond decided_len_
    uint64_t target = std::min<uint64_t>(trim_to, decided_len_);
    truncate_prefix(target);
}

cot::task<> pt_paxos_replica::apply_decisions_up_to(uint64_t limit) {
    // apply all decided slots up to limit to the local state machine
    // DVs <- AVs[0..w'-1]
    // SEND to Nancy / apply to state machine
    uint64_t wprime = std::min<uint64_t>(limit, av_abs_len());
    while (decided_len_ < wprime) {
        const pancy::request& req = AVs_[decided_len_ - log_start_];
        pancy::response resp = db_.process_req(req);
    
        // DEDUP: Cache committed resp
        uint64_t serial = request_serial_key(req);
        committed_response_by_serial_[serial] = resp;

        // only the current leader_index_ responds to clients
        if (index_ == leader_index_) { co_await to_clients_.send(resp); }

        ++decided_len_;
    }
}

cot::task<> pt_paxos_replica::trim() {
    // leader-side garbage collection:
    // once leader knows every replica has applied up through some point,
    // it can instruct replicas to truncate that prefix
    if (!leader_.active || current_round_usurped()) {
        leader_.active = false; // step down if we lost leadership
        co_return;
    }
    if (leader_.applied_q.size() < nreplicas_) { co_return; } // not heard from everyone yet

    uint64_t min_applied = UINT64_MAX;
    for (const auto& [server, applied] : leader_.applied_q) {
        (void) server;
        min_applied = std::min(min_applied, applied);
    }
    min_applied = std::min<uint64_t>(min_applied, leader_.decided_w);

    if (min_applied > leader_.trim_w) {
        // we can trim up to min_applied
        leader_.trim_w = min_applied;
        local_truncate(min_applied);
        co_await broadcast(mp_decide{index_, leader_.r, leader_.decided_w, leader_.trim_w});
    }
}

cot::task<> pt_paxos_replica::decide() {
    // Leader decides the longest prefix known by a quorum
    // w = min(w_k)
    // DECIDE(r, w)
    if (!leader_.active || current_round_usurped()) {
        leader_.active = false; // step down if we lost leadership
        co_return;
    }
    if (leader_.ack_q.size() < quorum_size_) { co_return; }

    uint64_t min_w = UINT64_MAX;
    for (const auto& [server, w] : leader_.ack_q) {
        (void) server;
        min_w = std::min(min_w, w);
    }

    if (min_w > leader_.decided_w) {
        // we can advance our decided prefix up to min_w
        leader_.decided_w = min_w;
        leader_.trim_w = std::min(leader_.trim_w, leader_.decided_w);
        co_await broadcast(mp_decide{index_, leader_.r, min_w, leader_.trim_w});
    }
    co_await trim();
}

cot::task<> pt_paxos_replica::send_extension_propose() {
    // extend the current proposal in the same round by appending more IVs.
    if (!leader_.active || current_round_usurped()) { co_return; }

    std::vector<pancy::request> next = extend_with_initial_values(leader_.proposed_base, leader_.proposed_AV, iv_base_, IVs_);
    if (next.size() <= leader_.proposed_AV.size()) { co_return; }

    leader_.proposed_AV = next;
    leader_index_ = index_;
    check_invariants();
    co_await broadcast(mp_propose{index_, leader_.r, leader_.proposed_base, leader_.proposed_AV});
}

cot::task<> pt_paxos_replica::probe() {
    // start a fresh leader round

    uint64_t new_r = prs_ / nreplicas_ + 1;
    new_r = new_r * nreplicas_ + index_;
    if (new_r <= prs_) { new_r = prs_ + nreplicas_; } 

    prs_ = new_r;
    leader_index_ = index_;
    leader_ = leader_round_state{};
    leader_.r = new_r;
    leader_.active = true;

    // leader counts its own PREPARE immediately
    leader_.prepare_q[index_] = {ars_, log_start_, decided_len_, AVs_};
    leader_.applied_q[index_] = decided_len_;
    last_leader_msg_ = cot::now();
    check_invariants();

    // Send PROBE(r, dv)
    co_await broadcast(mp_probe{index_, new_r, decided_len_});
}

std::tuple<uint64_t, uint64_t, std::vector<pancy::request>>
pt_paxos_replica::best_prepare(const std::map<size_t, leader_round_state::prepare_info>& q) {
    // Choose the prepare record with highest ar, breaking ties by longest ACK val seq
    uint64_t best_ar = 0;
    uint64_t best_base = 0;
    std::vector<pancy::request> best_AV;
    for (const auto& [server, info] : q) {
        (void) server;
        if (info.ar > best_ar || (info.ar == best_ar && info.base + info.AV.size() > best_base + best_AV.size())) {
            best_ar = info.ar;
            best_base = info.base;
            best_AV = info.AV;
        }
    }
    return {best_ar, best_base, best_AV};
}

// -----------------------------------------------------------------------------
// Per-message Handlers
// -----------------------------------------------------------------------------

cot::task<> pt_paxos_replica::handle_timeout() {
    // LEADER: retransmit PROPOSE / DECIDE / maybe PROBE
    // NON-LEADER: start new PROBE round
    if (index_ == leader_index_ && leader_.active && !current_round_usurped()) {
        if (!leader_.proposed_AV.empty()) {
            co_await broadcast(mp_propose{index_, leader_.r, leader_.proposed_base, leader_.proposed_AV});
        }
        if (leader_.decided_w > 0 || leader_.trim_w > 0) {
            co_await broadcast(mp_decide{index_, leader_.r, leader_.decided_w, leader_.trim_w});
        }
        if (leader_.prepare_q.size() < quorum_size_) {  
            co_await broadcast(mp_probe{index_, leader_.r, decided_len_});
        }
    } 
    else if (cot::now() - last_leader_msg_ > LEADER_TIMEOUT_) {
        co_await probe();
    }
}

cot::task<> pt_paxos_replica::handle_client_request(const pancy::request& req) {
    ++client_requests_seen_;

    // redirect if not leader
    if (index_ != leader_index_) {
        co_await to_clients_.send(pancy::redirection_response{
            pancy::response_header(req, pancy::errc::redirect), leader_index_});
        co_return;
    }

    // DEDUP: Short-circuit
    uint64_t serial = request_serial_key(req);
    auto it = committed_response_by_serial_.find(serial);
    if (it != committed_response_by_serial_.end()) {
        ++dedup_hits_;
        co_await to_clients_.send(it->second);
        co_return;
    }

    IVs_.push_back(req);
    ++consensus_inserts_;
    check_invariants();

    // if think leader and no active round, start
    if (!leader_.active) { co_await probe(); }

    // Once PREPARE quorum is achieved: 
    // either send the first proposal of the round
    // or extend the existing proposal in the same round
    if (leader_.active && leader_.prepare_q.size() >= quorum_size_) {
        if (leader_.proposed_AV.empty()) {
            auto [best_ar, best_base, best_AV] = best_prepare(leader_.prepare_q);
            (void) best_ar;
            leader_.proposed_base = best_base;
            leader_.proposed_AV = extend_with_initial_values(best_base, best_AV, iv_base_, IVs_);
            check_invariants();
            co_await broadcast(mp_propose{index_, leader_.r, leader_.proposed_base, leader_.proposed_AV});
        } 
        else { co_await send_extension_propose(); }
    }
}

cot::task<> pt_paxos_replica::handle_probe(const mp_probe& m) {
    // Follower receives PROBE
    // if r < prs ignore
    // else prs <- r and send PREPARE(prs, ars, AV)
    if (m.r < prs_) { co_return; } 

    prs_ = m.r;
    leader_index_ = m.leader;
    last_leader_msg_ = cot::now();

    // TRUNCATION: if leader says dv slots are definitely decided, trim
    // local_truncate(m.dv);
    check_invariants();

    co_await to_replicas_[m.leader]->send(mp_prepare{index_, prs_, ars_, log_start_, decided_len_, AVs_});
}

cot::task<> pt_paxos_replica::handle_prepare(const mp_prepare& m) {
    // Leader receives PREPARE responses for its current round
    if (!leader_.active || m.pr != leader_.r) { co_return; }
    if (current_round_usurped()) {
        leader_.active = false;
        co_return;
    }

    check_value_message_invariant(m.base, m.AV, "PREPARE");
    leader_.prepare_q[m.server] = {m.ar, m.base, m.applied, m.AV};
    leader_.applied_q[m.server] = m.applied;
    check_invariants();

    if (leader_.prepare_q.size() < quorum_size_) { co_return; }

    // once leader has a quorum, compute best prior accepted sequence
    auto [best_ar, best_base, best_AV] = best_prepare(leader_.prepare_q);
    (void) best_ar;

    // extend w/leader's pending client reqs
    std::vector<pancy::request> V = extend_with_initial_values(best_base, best_AV, iv_base_, IVs_);

    if (leader_.proposed_AV.empty()) {
        // first proposal in this round
        leader_.proposed_base = best_base;
        leader_.proposed_AV = V;
        check_invariants();
        co_await broadcast(mp_propose{index_, leader_.r, leader_.proposed_base, leader_.proposed_AV});
        co_return;
    }

    // if a proposal already exists in this round, extend
    if (!is_extension(leader_.proposed_base, leader_.proposed_AV, best_base, V)) {
        throw std::runtime_error(std::format(
            "R{} invariant: recomputed proposal is not an extension of prior proposal in round {}",
            index_, leader_.r));
    }

    // if this recomputed value is longer, send the extension
    if (best_base + V.size() > leader_proposed_abs_len()) {
        leader_.proposed_base = best_base;
        leader_.proposed_AV = V;
        check_invariants();
        co_await broadcast(mp_propose{index_, leader_.r, leader_.proposed_base, leader_.proposed_AV});
    }
}

cot::task<> pt_paxos_replica::handle_propose(const mp_propose& m) {
    // Follower receives PROPOSE(r, V)
    uint64_t new_abs_len = m.base + m.AV.size();

    // old round => ignore
    if (m.pr < prs_) { co_return; }

    // same round but shorter than what already have => ignore
    if (m.pr == ars_ && new_abs_len < av_abs_len()) { co_return; }

    // Same round proposals must be extensions, never conflicting rewrites
    if (m.pr == ars_ && !is_extension(log_start_, AVs_, m.base, m.AV)) {
        throw std::runtime_error(std::format(
            "R{} invariant: same-round PROPOSE is not an extension (current base={}, |AV|={}, new base={}, |AV|={})",
            index_, log_start_, AVs_.size(), m.base, m.AV.size()));
    }

    check_value_message_invariant(m.base, m.AV, "PROPOSE");

    // accept the proposal
    prs_ = m.pr;
    ars_ = m.pr;
    log_start_ = m.base;
    AVs_ = m.AV;
    leader_index_ = m.leader;
    last_leader_msg_ = cot::now();

    // cal_truncate(decided_len_);
    check_invariants();

    // ACK back with current absolute acknowledged length
    co_await to_replicas_[m.leader]->send(
        mp_ack{index_, ars_, av_abs_len(), decided_len_});
}

cot::task<> pt_paxos_replica::handle_ack(const mp_ack& m) {
    // Leader receives ACK for current round
    if (!leader_.active || m.ar != leader_.r) { co_return; }

    // Follower cannot ACK beyond what leader proposed
    if (m.w > leader_proposed_abs_len()) {
        throw std::runtime_error(std::format(
            "R{} invariant: ACK from R{} has w={} > proposed abs |AV|={}",
            index_, m.server, m.w, leader_proposed_abs_len()));
    }

    leader_.ack_q[m.server] = m.w;
    leader_.applied_q[m.server] = m.applied;
    check_invariants();
    co_await decide();
    check_invariants();
}

cot::task<> pt_paxos_replica::handle_decide(const mp_decide& m) {
    // Receive DECIDE(r, w)
    if (m.r < ars_) { co_return; }

    uint64_t wprime = std::min<uint64_t>(m.w, av_abs_len());
    if (wprime > decided_len_) {
        last_leader_msg_ = cot::now();
        leader_index_ = m.leader;
        co_await apply_decisions_up_to(wprime);
    }

    local_truncate(m.trim);
    check_invariants();
}

// ********** PANCY SERVICE CODE **********

cot::task<> pt_paxos_replica::run() {
    while (true) {
        auto result = co_await cot::first(from_clients_.receive(), from_replicas_.receive(), cot::after(RETRANSMIT_PERIOD_));

        if (std::holds_alternative<std::monostate>(result)) {
            co_await handle_timeout();
            continue;
        }

        if (auto* req = std::get_if<pancy::request>(&result)) {
            co_await handle_client_request(*req);
            continue;
        }

        auto& msg = std::get<paxos_message>(result);
        if (auto* m = std::get_if<mp_probe>(&msg)) { co_await handle_probe(*m); }
        else if (auto* m = std::get_if<mp_prepare>(&msg)) { co_await handle_prepare(*m); }
        else if (auto* m = std::get_if<mp_propose>(&msg)) { co_await handle_propose(*m); }
        else if (auto* m = std::get_if<mp_ack>(&msg)) { co_await handle_ack(*m); }
        else if (auto* m = std::get_if<mp_decide>(&msg)) { co_await handle_decide(*m); }
    }
}

// ******** end Pancy service code ********

// -----------------------------------------------------------------------------
// Test infrastructure (unchanged)
// -----------------------------------------------------------------------------

cot::task<> clear_after(cot::duration d) { 
    co_await cot::after(d); 
    cot::clear(); 
}

cot::task<> fail_replica_after(pt_paxos_instance& inst, size_t i, cot::duration d) {
    // Simulate failure by dropping all traffic to/from replica i.
    co_await cot::after(d);
    for (size_t j = 0; j < inst.replicas.size(); ++j) {
        inst.replicas[i]->to_replicas_[j]->set_loss(1.0);
    }
    inst.replicas[i]->to_clients_.set_loss(1.0);
    for (size_t j = 0; j < inst.replicas.size(); ++j) {
        if (j != i) {
            inst.replicas[j]->to_replicas_[i]->set_loss(1.0);
        }
    }
}

cot::task<> fail_then_recover(pt_paxos_instance& inst, size_t i,
        cot::duration fail_at, cot::duration recover_at, double original_loss) {
    co_await cot::after(fail_at);
    for (size_t j = 0; j < inst.replicas.size(); ++j) {
        inst.replicas[i]->to_replicas_[j]->set_loss(1.0);
    }
    inst.replicas[i]->to_clients_.set_loss(1.0);
    for (size_t j = 0; j < inst.replicas.size(); ++j) {
        if (j != i) {
            inst.replicas[j]->to_replicas_[i]->set_loss(1.0);
        }
    }

    co_await cot::after(recover_at - fail_at);
    for (size_t j = 0; j < inst.replicas.size(); ++j) {
        inst.replicas[i]->to_replicas_[j]->set_loss(original_loss);
    }
    inst.replicas[i]->to_clients_.set_loss(original_loss);
    for (size_t j = 0; j < inst.replicas.size(); ++j) {
        if (j != i) {
            inst.replicas[j]->to_replicas_[i]->set_loss(original_loss);
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

    tasks.push_back(fail_then_recover(inst, 0, 30s, 60s, tester.loss));
    cot::task<> timeout_task = clear_after(100s);

    // Wait for `timeout_task`
    cot::loop();

    size_t ref = 0;
    for (size_t s = 1; s < tester.nreplicas; ++s) {
        if (inst.replicas[s]->decided_len_ > inst.replicas[ref]->decided_len_) {
            ref = s;
        }
    }
    pancy::pancydb& db = inst.replicas[ref]->db_;

    // Check state convergence across replicas
    for (size_t s = 0; s < tester.nreplicas; ++s) {
        if (s == ref) { continue; }
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

    // DEDUP STATS
    for (size_t s = 0; s < tester.nreplicas; ++s) {
        auto& r = *inst.replicas[s];
        std::print("R{}: seen={}, dedup_hits={}, consensus_inserts={}\n",
                   s, r.client_requests_seen_, r.dedup_hits_, r.consensus_inserts_);
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
