/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_002, QCSIDM_SRS_004, QCSIDM_SRS_008, QCSIDM_SRS_021, QCSIDM_SRS_023, QCSIDM_SRS_024, QCSIDM_SRS_026, QCSIDM_SRS_027, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_036, QCSIDM_SRS_037, QCSIDM_SRS_040, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_045, QCSIDM_SRS_051, QCSIDM_SRS_052, QCSIDM_SRS_053, QCSIDM_SRS_055, QCSIDM_SRS_059, QCSIDM_SRS_062, QCSIDM_SRS_063, QCSIDM_SRS_064, QCSIDM_SRS_066, QCSIDM_SRS_070, QCSIDM_SRS_071, QCSIDM_SRS_072, QCSIDM_SRS_073, QCSIDM_SRS_074, QCSIDM_SRS_082, QCSIDM_SRS_084, QCSIDM_SRS_085, QCSIDM_SRS_089, QCSIDM_SRS_094, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_105, QCSIDM_SRS_109, QCSIDM_SRS_113 */
/* ==================================================== */

// tests/test_aggregator.cpp
#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include "../src/aggregator.hpp" // adjust if your header path is different

int main() {
    using namespace std::chrono_literals;

    // Construct aggregator with a 5-second window for quick tests.
    Aggregator agg(5 /*window_seconds*/, 4 /*ip shards*/, 2 /*endpoint shards*/, false /*no CMS*/, 0,0, 0, 0);

    // Simulate a few parsed lines
    std::time_t now = std::time(nullptr);
    ParsedLine p1{"1.2.3.4","GET","/a",200, now};
    ParsedLine p2{"1.2.3.4","GET","/a",200, now};
    ParsedLine p3{"5.6.7.8","POST","/login",401, now};
    ParsedLine p4{"9.9.9.9","GET","/a",200, now - 4}; // within window if window >=5

    agg.add_parsed(p1);
    agg.add_parsed(p2);
    agg.add_parsed(p3);
    agg.add_parsed(p4);

    if (agg.get_total() != 4) {
        std::cerr << "total lines expected 4 got " << agg.get_total() << "\n";
        return 1;
    }
    auto stat = agg.snapshot_status_counts();
    if (stat[200] != 3 || stat[401] != 1) {
        std::cerr << "status counts mismatch\n";
        return 2;
    }

    auto top_ips = agg.top_k_ips(3);
    if (top_ips.empty() || top_ips[0].first != "1.2.3.4" || top_ips[0].second < 2) {
        std::cerr << "top_ips mismatch\n";
        return 3;
    }

    // rate over last 5 seconds should be > 0
    double rate = agg.rate_over_seconds(5);
    if (rate <= 0.0) {
        std::cerr << "expected positive rate got " << rate << "\n";
        return 4;
    }

    // Test approximate top-k path if MG present (skipped here since disabled)

    std::cout << "test_aggregator: OK\n";
    return 0;
}
