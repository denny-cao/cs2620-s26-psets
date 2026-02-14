#pragma once
#include <atomic>
#include <cassert>
#include <chrono>
#include <coroutine>
#include <deque>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <queue>
#include <utility>
#include <variant>
#include <vector>
#include "circular_int.hh"
#include "small_vector.hh"

namespace cotamer {
namespace detail {
struct event_body;
template <typename T> struct task_promise;
template <typename T> struct task_awaiter;
template <typename T> struct task_event_awaiter;
struct interest_event_awaiter;

class event_handle {
public:
    event_handle() = default;
    explicit inline event_handle(event_body*) noexcept;
    inline event_handle(const event_handle&) noexcept;
    inline event_handle(event_handle&&) noexcept;
    inline event_handle& operator=(const event_handle&);
    inline event_handle& operator=(event_handle&&) noexcept;
    inline event_handle& operator=(std::nullptr_t);
    inline void swap(event_handle&) noexcept;
    inline ~event_handle();

    explicit operator bool() const { return eb_ != nullptr; }
    event_body* get() const { return eb_; }
    event_body& operator*() const { return *eb_; }
    event_body* operator->() const { return eb_; }

private:
    event_body* eb_ = nullptr;
};

inline void swap(event_handle& a, event_handle& b) noexcept { a.swap(b); }
}


class event {
    detail::event_handle ep_;

public:
    inline event();
    inline event(detail::event_handle ev);
    explicit inline event(nullptr_t);
    ~event() = default;
    event(const event&) = default;
    event(event&&) = default;
    event& operator=(const event&) = default;
    event& operator=(event&&) = default;

    inline void trigger();
    inline bool triggered() const;

    inline bool empty() const;

    inline const detail::event_handle& handle() const&;
    inline detail::event_handle&& handle() &&;
};


template <typename T = void>
class task {
public:
    using promise_type = detail::task_promise<T>;
    using handle_type = std::coroutine_handle<promise_type>;

    explicit inline task(handle_type handle) noexcept;
    inline task(task&& x) noexcept;
    inline task& operator=(task&& x) noexcept;
    task(const task&) = delete;
    task& operator=(const task&) = delete;
    inline ~task();

    inline void detach();
    inline event completion();
    inline bool done();
    inline void start();

    detail::task_awaiter<T> operator co_await() const noexcept;

private:
    friend class detail::task_promise<T>;
    handle_type handle_;
};


struct interest {};
struct interest_event {};
struct clearing_error {};


template <typename... Es> inline event any(Es&&... es);
template <typename... Es> inline event all(Es&&... es);

template <typename T, typename... Es>
task<std::optional<T>> attempt(task<T> t, Es&&... es);
template <typename T, typename... Es>
task<std::optional<T>> attempt(task<std::optional<T>> t, Es&&... es);
template <typename... Es>
task<std::optional<bool>> attempt(task<void> t, Es&&... es);


inline event asap();

using clock = std::chrono::system_clock;
inline clock::time_point now();
inline event after(clock::duration);
inline event at(clock::time_point);


namespace detail {

constexpr uintptr_t t_quorum = uintptr_t(1);
constexpr uint32_t f_want_interest = 1;
constexpr uint32_t f_interest = 2;
constexpr uint32_t f_quorum = 4;

inline void throw_if_clearing();

template <typename T>
struct task_final_awaiter {
    bool await_ready() noexcept { return false; }
    inline std::coroutine_handle<> await_suspend(std::coroutine_handle<task_promise<T>> self) noexcept;
    void await_resume() noexcept { }
};

template <typename T>
struct task_promise {
    inline task<T> get_return_object() noexcept;
    std::suspend_never initial_suspend() noexcept { return {}; }
    auto final_suspend() noexcept { return task_final_awaiter<T>{}; }
    template <typename Aw>
    Aw&& await_transform(Aw&& aw) noexcept { return std::forward<Aw>(aw); }
    task_event_awaiter<T> await_transform(event ev);
    inline task_event_awaiter<T> await_transform(interest);
    inline interest_event_awaiter await_transform(interest_event);
    void return_value(T value) { result_.template emplace<1>(std::move(value)); }
    void unhandled_exception() noexcept { result_.template emplace<2>(std::current_exception()); }
    T result() {
        if (result_.index() == 2) {
            std::rethrow_exception(std::move(std::get<2>(result_)));
        }
        return std::move(std::get<1>(result_));
    }
    inline event_handle& make_interest();
    bool owned_ = true;
    bool has_interest_ = false;
    std::coroutine_handle<> continuation_;
    event_handle completion_;
    event_handle interest_;
    std::variant<std::monostate, T, std::exception_ptr> result_;
};

template <>
struct task_promise<void> {
    inline task<void> get_return_object() noexcept;
    std::suspend_never initial_suspend() noexcept { return {}; }
    auto final_suspend() noexcept { return task_final_awaiter<void>{}; }
    template <typename Aw>
    Aw&& await_transform(Aw&& aw) noexcept { return std::forward<Aw>(aw); }
    task_event_awaiter<void> await_transform(event ev);
    inline task_event_awaiter<void> await_transform(interest);
    inline interest_event_awaiter await_transform(interest_event);
    void return_void() noexcept { }
    void unhandled_exception() noexcept { exception_ = std::current_exception(); }
    void result() {
        if (exception_) {
            std::rethrow_exception(std::move(exception_));
        }
    }
    inline event_handle& make_interest();
    bool owned_ = true;
    bool has_interest_ = false;
    std::coroutine_handle<> continuation_;
    event_handle completion_;
    event_handle interest_;
    std::exception_ptr exception_;
};

template <typename T>
inline task<T> task_promise<T>::get_return_object() noexcept {
    return task<T>{std::coroutine_handle<task_promise<T>>::from_promise(*this)};
}

inline task<void> task_promise<void>::get_return_object() noexcept {
    return task<void>{std::coroutine_handle<task_promise<void>>::from_promise(*this)};
}

template <typename T>
inline std::coroutine_handle<> task_final_awaiter<T>::await_suspend(
    std::coroutine_handle<task_promise<T>> self
) noexcept {
    auto& promise = self.promise();
    if (promise.completion_) {
        promise.completion_->trigger();
    }
    if (promise.continuation_) {
        return std::exchange(promise.continuation_, nullptr);
    }
    if (!promise.owned_) {
        self.destroy();
    }
    return std::noop_coroutine();
}


template <typename T>
struct task_awaiter {
    std::coroutine_handle<task_promise<T>> self_;

    bool await_ready() noexcept {
        return self_.done();
    }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept {
        self_.promise().continuation_ = awaiting;
        if (self_.promise().interest_) {
            self_.promise().interest_->trigger();
        }
        return std::noop_coroutine();
    }
    T await_resume() {
        return self_.promise().result();
    }
};


struct event_body {
    std::atomic<uint32_t> refcount_ = 1;
    uint32_t flags_;
    small_vector<uintptr_t, 3> vs_;

    event_body(uint32_t flags = 0) : flags_(flags) { }
    event_body(const event_body&) = delete;
    event_body(event_body&&) = delete;
    event_body& operator=(const event_body&) = delete;
    event_body& operator=(event_body&&) = delete;
    ~event_body() {
        if (!vs_.empty()) {
            trigger();
        }
    }

    bool empty() const {
        return vs_.empty();
    }
    bool triggered() const {
        return vs_.empty_capacity();
    }

    void add(uintptr_t vx) {
        assert(vx && !triggered());
        vs_.push_back(vx);
    }

    void remove(uintptr_t vx) {
        for (auto it = vs_.begin(); it != vs_.end(); ) {
            if (*it == vx) {
                *it = vs_.back();
                vs_.pop_back();
            } else {
                ++it;
            }
        }
    }

    inline void trigger();

    inline void apply_interest(event_handle& interest);
};


inline event_handle::event_handle(event_body* eb) noexcept
    : eb_(eb) {
}

inline event_handle::event_handle(const event_handle& x) noexcept
    : eb_(x.eb_) {
    if (eb_) {
        eb_->refcount_.fetch_add(1, std::memory_order_relaxed);
    }
}

inline event_handle::event_handle(event_handle&& x) noexcept
    : eb_(std::exchange(x.eb_, nullptr)) {
}

inline event_handle& event_handle::operator=(std::nullptr_t) {
    event_handle tmp;
    std::swap(eb_, tmp.eb_);
    return *this;
}

inline event_handle::~event_handle() {
    if (eb_ && eb_->refcount_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete eb_;
    }
}

inline event_handle& event_handle::operator=(const event_handle& x) {
    if (this != &x) {
        event_handle tmp(x);
        std::swap(eb_, tmp.eb_);
    }
    return *this;
}

inline event_handle& event_handle::operator=(event_handle&& x) noexcept {
    if (this != &x) {
        event_handle tmp(std::move(x));
        std::swap(eb_, tmp.eb_);
    }
    return *this;
}

inline void event_handle::swap(event_handle& x) noexcept {
    auto tmp = eb_;
    eb_ = x.eb_;
    x.eb_ = tmp;
}


template <typename T>
inline event make_event(T resumable) {
    co_await resumable;
}

inline event& make_event(event& ev) {
    return ev;
}

inline event&& make_event(event&& ev) {
    return std::move(ev);
}

inline event make_event(interest);


struct quorum_event_body {
    template<typename... Es>
    quorum_event_body(size_t quorum, Es&&... es)
        : quorum_(quorum) {
        (add(make_event(std::forward<Es>(es)).handle()), ...);
    }

    uintptr_t self_vx() const {
        return reinterpret_cast<uintptr_t>(this) | t_quorum;
    }

    void add(event_handle eh) {
        if (!eh || eh->triggered()) {
            ++triggered_;
            return;
        }
        eh->add(self_vx());
        if (eh->flags_ & f_want_interest) {
            flags_ |= f_want_interest;
        }
        es_.push_back(std::move(eh));
    }

    event result_event() {
        assert(!result_ptr_);
        if (triggered_ >= quorum_) {
            trigger_and_destroy(false);
            return event(nullptr);
        }
        event_handle eh(new event_body(flags_ | f_quorum));
        eh->add(self_vx());
        result_ptr_ = eh.get();
        return event(std::move(eh));
    }

    void mark_trigger(event_body* e) {
        for (auto it = es_.begin(); it != es_.end(); ) {
            if (it->get() == e) {
                ++triggered_;
                *it = std::move(es_.back());
                es_.pop_back();
            } else {
                ++it;
            }
        }
        if (e == result_ptr_ || triggered_ >= quorum_) {
            trigger_and_destroy(e == result_ptr_);
        }
    }

    void trigger_and_destroy(bool result) {
        for (auto it = es_.begin(); it != es_.end(); ++it) {
            (*it)->remove(self_vx());
        }
        es_.clear();
        if (result_ptr_ && !result) {
            result_ptr_->remove(self_vx());
            result_ptr_->trigger();
        }
        delete this;
    }

    inline void apply_interest(event_handle& interest);

    small_vector<event_handle, 3> es_;
    event_body* result_ptr_ = nullptr;
    uint32_t triggered_ = 0;
    uint32_t quorum_;
    uint32_t flags_ = 0;
};

inline event make_event(interest) {
    auto q = new detail::quorum_event_body(1);
    q->flags_ |= f_interest | f_want_interest;
    return q->result_event();
}


template <typename T>
struct task_event_awaiter {
    event event_;
    uintptr_t coroutine_;

    ~task_event_awaiter() {
        if (coroutine_) {
            event_.handle()->remove(coroutine_);
        }
    }

    bool await_ready() noexcept {
        return event_.triggered();
    }
    void await_suspend(std::coroutine_handle<task_promise<T>> awaiting) noexcept {
        if (event_.handle()->flags_ & f_want_interest) {
            event_.handle()->apply_interest(awaiting.promise().make_interest());
        }
        coroutine_ = reinterpret_cast<uintptr_t>(awaiting.address());
        event_.handle()->add(coroutine_);
    }
    inline void await_resume();
};

template <typename T>
inline task_event_awaiter<T> task_promise<T>::await_transform(event ev) {
    return task_event_awaiter<T>{std::move(ev)};
}

inline task_event_awaiter<void> task_promise<void>::await_transform(event ev) {
    return task_event_awaiter<void>{std::move(ev)};
}

template <typename T>
inline event_handle& task_promise<T>::make_interest() {
    if (!has_interest_) {
        interest_ = event_handle(new event_body);
        has_interest_ = true;
    }
    return interest_;
}

inline event_handle& task_promise<void>::make_interest() {
    if (!has_interest_) {
        interest_ = event_handle(new event_body);
        has_interest_ = true;
    }
    return interest_;
}

template <typename T>
inline task_event_awaiter<T> task_promise<T>::await_transform(interest) {
    return task_event_awaiter<T>(make_interest());
}

inline task_event_awaiter<void> task_promise<void>::await_transform(interest) {
    return task_event_awaiter<void>(make_interest());
}

struct interest_event_awaiter {
    event_handle handle_;
    bool await_ready() noexcept { return true; }
    void await_suspend(std::coroutine_handle<>) noexcept { }
    event await_resume() { return event(std::move(handle_)); }
};

template <typename T>
inline interest_event_awaiter task_promise<T>::await_transform(interest_event) {
    return interest_event_awaiter{make_interest()};
}

inline interest_event_awaiter task_promise<void>::await_transform(interest_event) {
    return interest_event_awaiter{make_interest()};
}

inline void event_body::apply_interest(event_handle& interest) {
    assert(flags_ & f_quorum);
    flags_ &= ~f_want_interest;
    if (triggered()) {
        return;
    }
    assert(!vs_.empty() && (vs_.front() & t_quorum));
    reinterpret_cast<quorum_event_body*>(vs_.front() & ~t_quorum)->apply_interest(interest);
}

inline void quorum_event_body::apply_interest(event_handle& interest) {
    flags_ &= ~f_want_interest;
    if (flags_ & f_interest) {
        add(event_handle(interest));
        if (triggered_ >= quorum_) {
            trigger_and_destroy(false);
        }
    } else {
        small_vector<event_handle, 3> children;
        // make a local copy of children who want interest, because applying
        // interest to them might come back and delete this
        for (auto it = es_.begin(); it != es_.end(); ++it) {
            if ((*it)->flags_ & f_want_interest) {
                children.push_back(*it);
            }
        }
        for (auto it = children.begin(); it != children.end(); ++it) {
            (*it)->apply_interest(interest);
        }
    }
}

#include "cotamer_timers.hh"
}

struct driver {
    driver();
    ~driver();
    driver(const driver&) = delete;
    driver(driver&&) = delete;
    driver& operator=(const driver&) = delete;
    driver& operator=(driver&&) = delete;

    inline clock::time_point now();
    inline void step_time();

    inline void asap(event);
    inline event asap();
    inline void at(clock::time_point t, event);
    inline event at(clock::time_point t);
    inline void after(clock::duration d, event);
    inline event after(clock::duration d);

    void loop();
    void clear();
    bool clearing() const { return clearing_; }

    static std::unique_ptr<driver> main;

private:
    friend class detail::event_body;

    bool clearing_ = false;
    std::deque<std::coroutine_handle<>> ready_;
    std::deque<event> asap_;
    detail::driver_timers<> timed_;
    clock::time_point now_;
};

namespace detail {
template <typename T>
inline void task_event_awaiter<T>::await_resume() {
    coroutine_ = 0;
    // This code helps recover memory when clearing a driver (for instance,
    // if a test exits early). The clearing process triggers all
    // outstanding events, unblocking their waiting coroutines -- but those,
    // in turn, might have other coroutines waiting for their results.
    // We destroy the whole chain by forcing the event-unblocked coroutines
    // to throw an exception.
    if (driver::main->clearing()) {
        throw clearing_error{};
    }
}

inline void event_body::trigger() {
    small_vector<quorum_event_body*, 2> qe;
    for (auto it = vs_.begin(); it != vs_.end(); ++it) {
        if (*it & t_quorum) {
            qe.push_back(reinterpret_cast<quorum_event_body*>(*it & ~t_quorum));
        } else {
            driver::main->ready_.push_back(std::coroutine_handle<>::from_address(reinterpret_cast<void*>(*it)));
        }
    }
    vs_.clear_capacity();
    for (auto it = qe.begin(); it != qe.end(); ++it) {
        (*it)->mark_trigger(this);
    }
}
}


// event methods

inline event::event()
    : ep_(new detail::event_body()) {
}

inline event::event(detail::event_handle ev)
    : ep_(std::move(ev)) {
}

inline event::event(std::nullptr_t) {
}

inline bool event::empty() const {
    return !ep_ || ep_->empty();
}

inline bool event::triggered() const {
    return !ep_ || ep_->triggered();
}

inline void event::trigger() {
    if (ep_) {
        ep_->trigger();
    }
}

inline const detail::event_handle& event::handle() const& {
    return ep_;
}

inline detail::event_handle&& event::handle() && {
    return std::move(ep_);
}


// task methods

template <typename T>
inline task<T>::task(handle_type handle) noexcept
    : handle_(handle) {
}

template <typename T>
inline task<T>::task(task&& x) noexcept
    : handle_(std::exchange(x.handle_, nullptr)) {
}

template <typename T>
inline task<T>& task<T>::operator=(task&& x) noexcept {
    if (this != &x) {
        if (handle_) {
            handle_.destroy();
        }
        handle_ = std::exchange(x.handle_, nullptr);
    }
    return *this;
}

template <typename T>
inline task<T>::~task() {
    if (handle_) {
        handle_.destroy();
    }
}

template <typename T>
inline bool task<T>::done() {
    return handle_ && handle_.done();
}

template <typename T>
inline event task<T>::completion() {
    if (done()) {
        return event(nullptr);
    }
    auto& p = handle_.promise();
    if (!p.completion_) {
        p.completion_ = detail::event_handle(new detail::event_body());
    }
    return event(p.completion_);
}

template <typename T>
inline void task<T>::start() {
    if (done()) {
        return;
    }
    auto& p = handle_.promise();
    if (p.interest_) {
        p.interest_->trigger();
    } else {
        p.has_interest_ = true;
    }
}

template <typename T>
inline void task<T>::detach() {
    if (!handle_) {
        return;
    }
    handle_.promise().owned_ = false;
    if (handle_.done()) {
        handle_.destroy();
    }
    handle_ = nullptr;
}

template <typename T>
inline detail::task_awaiter<T> task<T>::operator co_await() const noexcept {
    return detail::task_awaiter<T>{handle_};
}


// driver methods

inline clock::time_point driver::now() {
    return now_;
}

inline void driver::step_time() {
    now_ += clock::duration{1};
}

inline void driver::asap(event e) {
    asap_.push_back(std::move(e));
}

inline event driver::asap() {
    event e;
    asap(e);
    return e;
}

inline void driver::at(clock::time_point t, event e) {
    timed_.emplace(t, std::move(e).handle());
}

inline event driver::at(clock::time_point t) {
    if (t <= now_) {
        return event(nullptr);
    }
    event e;
    at(t, e);
    return e;
}

inline void driver::after(clock::duration d, event e) {
    at(now() + d, std::move(e));
}

inline event driver::after(clock::duration d) {
    return at(now() + d);
}


// time functions

inline clock::time_point now() {
    return driver::main->now();
}

inline void step_time() {
    driver::main->step_time();
}


// basic events

inline event asap() {
    return driver::main->asap();
}

inline event at(clock::time_point t) {
    return driver::main->at(t);
}

inline event after(clock::duration d) {
    return driver::main->after(d);
}


// event adapter functions

inline event any() {
    return event(nullptr);
}

inline event any(event e) {
    return e;
}

template <typename E0, typename... Es>
inline event any(E0 e0, Es&&... es) {
    auto q = new detail::quorum_event_body(1, std::forward<E0>(e0), std::forward<Es>(es)...);
    return q->result_event();
}

inline event all(event e) {
    return e;
}

template <typename... Es>
inline event all(Es&&... es) {
    auto q = new detail::quorum_event_body(sizeof...(Es), std::forward<Es>(es)...);
    return q->result_event();
}

template <typename T, typename... Es>
task<std::optional<T>> attempt(task<T> t, Es&&... es) {
    if (!t.done()) {
        t.start();
        co_await any(t.completion(), std::forward<Es>(es)...);
    }
    if (t.done()) {
        co_return co_await t;
    }
    co_return std::nullopt;
}

template <typename T, typename... Es>
task<std::optional<T>> attempt(task<std::optional<T>> t, Es&&... es) {
    if (!t.done()) {
        t.start();
        co_await any(t.completion(), std::forward<Es>(es)...);
    }
    if (t.done()) {
        co_return co_await t;
    }
    co_return std::nullopt;
}

template <typename... Es>
task<std::optional<std::monostate>> attempt(task<void> t, Es&&... es) {
    if (!t.done()) {
        t.start();
        co_await any(t.completion(), std::forward<Es>(es)...);
    }
    if (t.done()) {
        co_await t;
        co_return std::monostate{};
    }
    co_return std::nullopt;
}


// main driver functions

void reset();

inline void clear() {
    driver::main->clear();
}

inline void loop() {
    driver::main->loop();
}

}
