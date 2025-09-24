/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_051, QCSIDM_SRS_052, QCSIDM_SRS_053, QCSIDM_SRS_054, QCSIDM_SRS_055, QCSIDM_SRS_057, QCSIDM_SRS_058, QCSIDM_SRS_059, QCSIDM_SRS_062, QCSIDM_SRS_066, QCSIDM_SRS_073, QCSIDM_SRS_082, QCSIDM_SRS_084, QCSIDM_SRS_103, QCSIDM_SRS_112, QCSIDM_SRS_113 */
/* ==================================================== */

#include "sliding_window.hpp"
#include <algorithm>

SlidingWindow::SlidingWindow(size_t window_seconds)
    : buckets(window_seconds), window_size(window_seconds) {
    start_second = static_cast<size_t>(std::time(nullptr));
    for (auto &b : buckets) b.store(0);
}

void SlidingWindow::add_event(std::time_t sec, uint64_t count) {
    std::lock_guard<std::mutex> lk(mu);
    size_t s = static_cast<size_t>(sec);
    if (s < start_second) {
        // Too old, ignore
        return;
    }
    size_t delta = s - start_second;
    if (delta >= window_size) {
        // advance window
        size_t new_start = s - (window_size - 1);
        for (size_t t = start_second; t < new_start; ++t) {
            size_t idx = (t - start_second) % window_size;
            buckets[idx].store(0);
        }
        start_second = new_start;
        delta = s - start_second;
    }
    size_t idx = delta % window_size;
    buckets[idx].fetch_add(count, std::memory_order_relaxed);
}

double SlidingWindow::rate_over_seconds(size_t last_n) {
    if (last_n == 0) return 0.0;
    std::lock_guard<std::mutex> lk(mu);
    size_t now = static_cast<size_t>(std::time(nullptr));
    uint64_t sum = 0;
    for (size_t i = 0; i < last_n && i < window_size; i++) {
        size_t sec = now - i;
        if (sec < start_second) break;
        size_t idx = (sec - start_second) % window_size;
        sum += buckets[idx].load();
    }
    return static_cast<double>(sum) / static_cast<double>(last_n);
}

void SlidingWindow::reset() {
    std::lock_guard<std::mutex> lk(mu);
    for (auto &b : buckets) b.store(0);
    start_second = static_cast<size_t>(std::time(nullptr));
}
