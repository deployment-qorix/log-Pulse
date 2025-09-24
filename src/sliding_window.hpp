/* ==================================================== */
/* REQ-MAPPING: implements QCSIDM_SRS_006, QCSIDM_SRS_016, QCSIDM_SRS_021, QCSIDM_SRS_024, QCSIDM_SRS_034, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_051, QCSIDM_SRS_052, QCSIDM_SRS_053, QCSIDM_SRS_054, QCSIDM_SRS_055, QCSIDM_SRS_056, QCSIDM_SRS_057, QCSIDM_SRS_058, QCSIDM_SRS_059, QCSIDM_SRS_062, QCSIDM_SRS_073, QCSIDM_SRS_082, QCSIDM_SRS_084, QCSIDM_SRS_085, QCSIDM_SRS_094, QCSIDM_SRS_103, QCSIDM_SRS_112 */
/* ==================================================== */

#pragma once
#include <vector>
#include <atomic>
#include <mutex>
#include <ctime>

// SlidingWindow: fixed-size ring buffer of per-second counters
class SlidingWindow {
    std::vector<std::atomic<uint64_t>> buckets;
    size_t window_size;           // number of seconds tracked
    size_t start_second;          // epoch second corresponding to buckets[0]
    std::mutex mu;                // protects window advancement
public:
    SlidingWindow(size_t window_seconds = 300);

    // Increment count for given epoch second
    void add_event(std::time_t sec, uint64_t count = 1);

    // Compute average rate (events/sec) over last N seconds
    double rate_over_seconds(size_t last_n);

    // Reset all buckets
    void reset();
};
