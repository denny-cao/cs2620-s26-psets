#pragma once
#include "detail/circular_int.hh"

namespace cotamer {
namespace detail {

template <int arity = 4>
struct driver_timers {
    driver_timers() = default;
    driver_timers(const driver_timers&) = delete;
    driver_timers(driver_timers&&) = delete;
    driver_timers& operator=(const driver_timers&) = delete;
    driver_timers& operator=(driver_timers&&) = delete;
    inline ~driver_timers();

    inline bool empty() const;
    inline clock::time_point top_time() const;
    inline void cull();
    void emplace(clock::time_point t, event_handle&& ev);
    inline void pop_trigger();
    void clear();

  private:
    struct trec {
        clock::time_point when;
        circular_int<unsigned> order;
        event_handle ev;

        inline bool operator<(const trec &x) const noexcept;
    };

    trec* ts_ = nullptr;
    unsigned sz_ = 0;
    unsigned rand_ = 8173;
    unsigned tcap_ = 0;
    unsigned order_ = 0;

    static inline unsigned heap_parent(unsigned i);
    static inline unsigned heap_first_child(unsigned i);
    inline unsigned heap_last_child(unsigned i) const;
    void hard_cull(unsigned pos);
    void expand();
};


template <int arity>
inline driver_timers<arity>::~driver_timers() {
    std::destroy_n(ts_, sz_);
    std::allocator<trec> alloc;
    alloc.deallocate(ts_, tcap_);
}

template <int arity>
inline bool driver_timers<arity>::empty() const {
    return sz_ == 0;
}

template <int arity>
inline clock::time_point driver_timers<arity>::top_time() const {
    assert(sz_ != 0);
    return ts_[0].when;
}

template <int arity>
inline void driver_timers<arity>::cull() {
    while (sz_ != 0 && ts_[0].ev->empty()) {
        hard_cull(0);
    }
}

template <int arity>
inline bool driver_timers<arity>::trec::operator<(const trec &x) const noexcept {
    auto cmp = when <=> x.when;
    return cmp < 0 || (cmp == 0 && order < x.order);
}

template <int arity>
inline unsigned driver_timers<arity>::heap_parent(unsigned i) {
    return (i - (arity == 2)) / arity;
}

template <int arity>
inline unsigned driver_timers<arity>::heap_first_child(unsigned i) {
    return i * arity + (arity == 2 || i == 0);
}

template <int arity>
inline unsigned driver_timers<arity>::heap_last_child(unsigned i) const {
    unsigned p = i * arity + arity + (arity == 2);
    return p < sz_ ? p : sz_;
}

template <int arity>
inline void driver_timers<arity>::pop_trigger() {
    ts_[0].ev->trigger();
    hard_cull(0);
}

template <int arity>
void driver_timers<arity>::clear() {
    for (unsigned i = 0; i != sz_; ++i) {
        ts_[i].ev->trigger();
    }
    std::destroy_n(ts_, sz_);
    sz_ = 0;
}

template <int arity>
void driver_timers<arity>::expand() {
    unsigned ncap = (tcap_ ? (tcap_ * 4) + 3 : 31);
    std::allocator<trec> alloc;
    trec* nts = alloc.allocate(ncap);
    if (ts_) {
        std::uninitialized_move_n(ts_, sz_, nts);
        std::destroy_n(ts_, sz_);
        alloc.deallocate(ts_, tcap_);
    }
    ts_ = nts;
    tcap_ = ncap;
}

template <int arity>
void driver_timers<arity>::emplace(clock::time_point when, event_handle&& ev) {
    using std::swap;

    // Append new trec
    unsigned pos = sz_;
    if (pos == tcap_) {
        expand();
    }
    std::construct_at(ts_ + pos, when, ++order_, std::move(ev));
    ++sz_;

    // Swap trec to proper position in heap
    while (pos != 0) {
        unsigned p = heap_parent(pos);
        if (!(ts_[pos] < ts_[p])) {
            break;
        }
        swap(ts_[pos], ts_[p]);
        pos = p;
    }

    // If heap is largish, check to see if a random element is empty.
    // If it's empty, remove it, and look for another empty element.
    // This should help keep the timer heap small even if we set many
    // more timers than get a chance to fire.
    while (sz_ >= 32) {
        pos = rand_ % sz_;
        rand_ = rand_ * 1664525 + 1013904223U; // Numerical Recipes LCG
        if (!ts_[pos].ev->empty()) {
            break;
        }
        hard_cull(pos);
    }
}

template <int arity>
void driver_timers<arity>::hard_cull(unsigned pos) {
    using std::swap;
    assert(sz_ != 0);

    --sz_;
    if (pos == sz_) {
        std::destroy_at(ts_ + pos);
        return;
    }
    swap(ts_[sz_], ts_[pos]);
    std::destroy_at(ts_ + sz_);

    if (pos == 0 || !(ts_[pos] < ts_[heap_parent(pos)])) {
        while (true) {
            unsigned smallest = pos;
            for (unsigned t = heap_first_child(pos);
                 t < heap_last_child(pos);
                 ++t) {
                if (ts_[t] < ts_[smallest]) {
                    smallest = t;
                }
            }
            if (smallest == pos) {
                break;
            }
            swap(ts_[pos], ts_[smallest]);
            pos = smallest;
        }
    } else {
        do {
            unsigned p = heap_parent(pos);
            swap(ts_[pos], ts_[p]);
            pos = p;
        } while (pos && ts_[pos] < ts_[heap_parent(pos)]);
    }
}

}
}
