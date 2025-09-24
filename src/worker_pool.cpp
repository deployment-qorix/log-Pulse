/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_001, QCSIDM_SRS_002, QCSIDM_SRS_003, QCSIDM_SRS_004, QCSIDM_SRS_005, QCSIDM_SRS_006, QCSIDM_SRS_008, QCSIDM_SRS_011, QCSIDM_SRS_012, QCSIDM_SRS_013, QCSIDM_SRS_015, QCSIDM_SRS_017, QCSIDM_SRS_018, QCSIDM_SRS_021, QCSIDM_SRS_022, QCSIDM_SRS_023, QCSIDM_SRS_024, QCSIDM_SRS_025, QCSIDM_SRS_026, QCSIDM_SRS_027, QCSIDM_SRS_030, QCSIDM_SRS_031, QCSIDM_SRS_034, QCSIDM_SRS_036, QCSIDM_SRS_037, QCSIDM_SRS_038, QCSIDM_SRS_040, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_050, QCSIDM_SRS_051, QCSIDM_SRS_053, QCSIDM_SRS_055, QCSIDM_SRS_056, QCSIDM_SRS_057, QCSIDM_SRS_059, QCSIDM_SRS_062, QCSIDM_SRS_063, QCSIDM_SRS_070, QCSIDM_SRS_073, QCSIDM_SRS_076, QCSIDM_SRS_078, QCSIDM_SRS_091, QCSIDM_SRS_092, QCSIDM_SRS_094, QCSIDM_SRS_099, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_105, QCSIDM_SRS_106, QCSIDM_SRS_112 */
/* ==================================================== */

// src/worker_pool.cpp
// Worker pool implementation that consumes lines from a bounded queue,
// parses them and updates the aggregator. All worker threads are wrapped
// in try/catch and report exceptions via safe_log().

#include "worker_pool.hpp"
#include "util_log.hpp"
#include "global_ctl.hpp"
#include "parser.hpp"      // parse_common_log(const std::string&) -> std::optional<ParsedLine>
#include <thread>
#include <vector>
#include <optional>
#include <chrono>
#include <exception>
#include <iostream>

WorkerPool::WorkerPool(size_t num_workers, BoundedQueue<std::string> &queue_, Aggregator &aggregator_)
    : queue(queue_), aggregator(aggregator_)
{
    size_t n = (num_workers == 0) ? 1 : num_workers;
    workers.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        workers.emplace_back([this, i]() {
            try {
                std::string line;
                while (!g_terminate.load()) {
                    // Assume queue.pop(line) blocks until an item is available or returns false on shutdown.
                    // If your BoundedQueue API differs, replace this call accordingly.
                    bool ok = queue.pop(line);
                    if (!ok) {
                        if (g_terminate.load()) break;
                        std::this_thread::sleep_for(std::chrono::milliseconds(50));
                        continue;
                    }

                    // parse the line
                    try {
                        auto parsed = parse_common_log(line);
                        if (parsed.has_value()) {
                            ParsedLine p = parsed.value();
                            if (p.ts_received == 0) p.ts_received = static_cast<std::time_t>(std::time(nullptr));
                            aggregator.add_parsed(p);
                        } else {
                            aggregator.add_parse_error();
                        }
                    } catch (const std::exception &pex) {
                        safe_log(std::string("Exception while parsing line in worker: ") + pex.what());
                        aggregator.add_parse_error();
                    } catch (...) {
                        safe_log("Unknown exception while parsing line in worker");
                        aggregator.add_parse_error();
                    }
                }
            } catch (const std::exception &ex) {
                safe_log(std::string("Unhandled exception in worker thread: ") + ex.what());
                g_terminate.store(true);
            } catch (...) {
                safe_log("Unhandled unknown exception in worker thread");
                g_terminate.store(true);
            }
        });
    }
}

WorkerPool::~WorkerPool() {
    // signal shutdown
    g_terminate.store(true);
    try {
        queue.notify_all();
    } catch (...) {
        // ignore notify errors during shutdown
    }

    for (auto &t : workers) {
        if (t.joinable()) {
            try {
                t.join();
            } catch (const std::exception &ex) {
                safe_log(std::string("Exception joining worker thread: ") + ex.what());
            } catch (...) {
                safe_log("Unknown exception joining worker thread");
            }
        }
    }
}
