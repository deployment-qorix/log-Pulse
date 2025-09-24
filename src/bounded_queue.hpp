/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_002, QCSIDM_SRS_004, QCSIDM_SRS_006, QCSIDM_SRS_011, QCSIDM_SRS_012, QCSIDM_SRS_013, QCSIDM_SRS_014, QCSIDM_SRS_015, QCSIDM_SRS_016, QCSIDM_SRS_017, QCSIDM_SRS_019, QCSIDM_SRS_020, QCSIDM_SRS_021, QCSIDM_SRS_023, QCSIDM_SRS_027, QCSIDM_SRS_078, QCSIDM_SRS_102 */
/* ==================================================== */

#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>

// A thread-safe bounded queue with close/notify support
template<typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity)
        : capacity_(capacity), closed_(false) {}

    // Disable copying
    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    // Push item, blocks if full. Returns false if queue is closed.
    bool push(T item) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_full_.wait(lk, [&]{ return q_.size() < capacity_ || closed_; });
        if (closed_) return false;
        q_.push(std::move(item));
        cv_empty_.notify_one();
        return true;
    }

    // Pop item, blocks if empty. Returns false if closed and empty.
    bool pop(T &out) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_empty_.wait(lk, [&]{ return !q_.empty() || closed_; });
        if (q_.empty()) return false; // closed and empty
        out = std::move(q_.front());
        q_.pop();
        cv_full_.notify_one();
        return true;
    }

    // Close queue: no more pushes, wake all waiters
    void notify_all() {
        std::lock_guard<std::mutex> lk(mu_);
        closed_ = true;
        cv_empty_.notify_all();
        cv_full_.notify_all();
    }

private:
    size_t capacity_;
    std::queue<T> q_;
    std::mutex mu_;
    std::condition_variable cv_empty_;
    std::condition_variable cv_full_;
    bool closed_;
};
