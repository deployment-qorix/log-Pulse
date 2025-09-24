/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_001, QCSIDM_SRS_002, QCSIDM_SRS_004, QCSIDM_SRS_005, QCSIDM_SRS_006, QCSIDM_SRS_007, QCSIDM_SRS_008, QCSIDM_SRS_009, QCSIDM_SRS_011, QCSIDM_SRS_012, QCSIDM_SRS_013, QCSIDM_SRS_014, QCSIDM_SRS_015, QCSIDM_SRS_021, QCSIDM_SRS_022, QCSIDM_SRS_023, QCSIDM_SRS_024, QCSIDM_SRS_025, QCSIDM_SRS_026, QCSIDM_SRS_027, QCSIDM_SRS_030, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_034, QCSIDM_SRS_036, QCSIDM_SRS_037, QCSIDM_SRS_040, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_045, QCSIDM_SRS_050, QCSIDM_SRS_051, QCSIDM_SRS_052, QCSIDM_SRS_053, QCSIDM_SRS_055, QCSIDM_SRS_056, QCSIDM_SRS_059, QCSIDM_SRS_062, QCSIDM_SRS_063, QCSIDM_SRS_066, QCSIDM_SRS_067, QCSIDM_SRS_070, QCSIDM_SRS_072, QCSIDM_SRS_073, QCSIDM_SRS_074, QCSIDM_SRS_076, QCSIDM_SRS_078, QCSIDM_SRS_081, QCSIDM_SRS_083, QCSIDM_SRS_084, QCSIDM_SRS_085, QCSIDM_SRS_087, QCSIDM_SRS_088, QCSIDM_SRS_089, QCSIDM_SRS_090, QCSIDM_SRS_097, QCSIDM_SRS_099, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_105, QCSIDM_SRS_106, QCSIDM_SRS_109, QCSIDM_SRS_112, QCSIDM_SRS_113, QCSIDM_SRS_116 */
/* ==================================================== */

// tests/test_worker_pool.cpp
// Tests WorkerPool parsing+aggregation pipeline by pushing simulated log lines

#include <iostream>
#include <thread>
#include <chrono>
#include <string>
#include <vector>
#include <cassert>

#include "../src/bounded_queue.hpp"
#include "../src/aggregator.hpp"
#include "../src/worker_pool.hpp" // expects WorkerPool(workers, queue, aggregator) style ctor
// test harness: provide test-side definition for the global terminate flag expected by some headers


int main() {
    using namespace std::chrono_literals;

    BoundedQueue<std::string> bq(1024);
    Aggregator agg(10 /*window_seconds*/, 4 /*ip shards*/, 2 /*endpoint shards*/, false /*cms*/, 0,0, 0,0);
    size_t workers = 2;

    // start worker pool which consumes strings from queue and updates aggregator
    WorkerPool wp(workers, bq, agg);

    // Push a few simple common-log lines (worker pool should parse them)
    std::vector<std::string> lines = {
        R"(127.0.0.1 - - [24/Sep/2025:10:00:00 +0530] "GET /a HTTP/1.1" 200 100)",
        R"(127.0.0.1 - - [24/Sep/2025:10:00:01 +0530] "GET /a HTTP/1.1" 200 100)",
        R"(10.0.0.2 - - [24/Sep/2025:10:00:02 +0530] "POST /login HTTP/1.1" 401 50)"
    };

    for (auto &ln : lines) {
        bq.push(std::move(ln)); // push expects rvalue, move the string
    }

    // notify end of input
    bq.notify_all();

    // Wait for workers to process the lines up to 2s (polling)
    const auto deadline = std::chrono::steady_clock::now() + 2000ms;
    while (std::chrono::steady_clock::now() < deadline) {
        if (agg.get_total() >= 3) break;
        std::this_thread::sleep_for(10ms);
    }

    // we expect 3 total lines
    uint64_t tot = agg.get_total();
    if (tot < 3) {
        std::cerr << "worker_pool: expected total>=3 got " << tot << "\n";
        return 2;
    }

    auto status = agg.snapshot_status_counts();
    if (status[200] < 2 || status[401] < 1) {
        std::cerr << "worker_pool: status counts unexpected\n";
        return 3;
    }

    std::cout << "test_worker_pool: OK\n";
    return 0;
}
