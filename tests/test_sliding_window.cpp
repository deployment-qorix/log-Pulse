/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_021, QCSIDM_SRS_023, QCSIDM_SRS_031, QCSIDM_SRS_036, QCSIDM_SRS_040, QCSIDM_SRS_051, QCSIDM_SRS_052, QCSIDM_SRS_053, QCSIDM_SRS_055, QCSIDM_SRS_057, QCSIDM_SRS_058, QCSIDM_SRS_059, QCSIDM_SRS_062, QCSIDM_SRS_073, QCSIDM_SRS_082, QCSIDM_SRS_084, QCSIDM_SRS_102, QCSIDM_SRS_103 */
/* ==================================================== */

#include <iostream>
#include <chrono>
#include <thread>
#include <cassert>
#include "../src/aggregator.hpp"

int main() {
    using namespace std::chrono_literals;

    // small window for test
    Aggregator agg(3 /*window seconds*/, 4, 2, false, 0,0, 0,0);

    std::time_t now = std::time(nullptr);
    ParsedLine p1{"1.1.1.1","GET","/t",200, now};
    ParsedLine p2{"2.2.2.2","GET","/t",200, now - 1};
    ParsedLine p3{"3.3.3.3","GET","/t",200, now - 2};

    agg.add_parsed(p1);
    agg.add_parsed(p2);
    agg.add_parsed(p3);

    double rate3 = agg.rate_over_seconds(3);
    if (rate3 <= 0.0) {
        std::cerr << "sliding_window: expected positive rate got " << rate3 << "\n";
        return 2;
    }

    // add older event outside window and ensure rate over 1 second is computed
    ParsedLine p_old{"4.4.4.4","GET","/t",200, now - 10};
    agg.add_parsed(p_old);
    double rate1 = agg.rate_over_seconds(1);
    if (rate1 < 0.0) {
        std::cerr << "sliding_window: rate negative\n";
        return 3;
    }

    std::cout << "test_sliding_window: OK\n";
    return 0;
}