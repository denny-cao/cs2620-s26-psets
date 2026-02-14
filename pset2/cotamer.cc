#include "cotamer.hh"
#include <memory>

namespace cotamer {

std::unique_ptr<driver> driver::main{new driver};

driver::driver()
    : now_(std::chrono::system_clock::from_time_t(1634070069)) {
}

driver::~driver() {
    clearing_ = true;
    while (!ready_.empty()) {
        auto ch = ready_.front();
        ready_.pop_front();
        ch();
    }
}

void driver::loop() {
    bool again = true;
    while (again) {
        again = false;

        while (!asap_.empty()) {
            asap_.front().trigger();
            asap_.pop_front();
            again = true;
        }

        while (!ready_.empty()) {
            auto ch = ready_.front();
            ready_.pop_front();
            ch();
            if (!clearing_) {
                now_ += clock::duration{1};
            }
            again = true;
        }

        // update time
        timed_.cull();
        if (asap_.empty() && !timed_.empty()) {
            now_ = timed_.top_time();
        }

        while (!timed_.empty() && timed_.top_time() <= now_) {
            timed_.pop_trigger();
            again = true;
        }
    }
    clearing_ = false;
}

void driver::clear() {
    clearing_ = true;
    while (!asap_.empty()) {
        asap_.front().trigger();
        asap_.pop_front();
    }
    timed_.clear();
}

void reset() {
    driver::main.reset(new driver);
}

}
