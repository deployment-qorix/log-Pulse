/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_002, QCSIDM_SRS_003, QCSIDM_SRS_005, QCSIDM_SRS_009, QCSIDM_SRS_021, QCSIDM_SRS_024, QCSIDM_SRS_031, QCSIDM_SRS_034, QCSIDM_SRS_036, QCSIDM_SRS_040, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_045, QCSIDM_SRS_047, QCSIDM_SRS_051, QCSIDM_SRS_062, QCSIDM_SRS_063, QCSIDM_SRS_064, QCSIDM_SRS_066, QCSIDM_SRS_070, QCSIDM_SRS_072, QCSIDM_SRS_074, QCSIDM_SRS_089, QCSIDM_SRS_092, QCSIDM_SRS_093, QCSIDM_SRS_094, QCSIDM_SRS_096, QCSIDM_SRS_097, QCSIDM_SRS_099, QCSIDM_SRS_100, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_104, QCSIDM_SRS_105, QCSIDM_SRS_106, QCSIDM_SRS_109, QCSIDM_SRS_110, QCSIDM_SRS_111, QCSIDM_SRS_113, QCSIDM_SRS_119, QCSIDM_SRS_120 */
/* ==================================================== */

// tests/test_checkpoint.cpp
// Test Aggregator checkpoint dump/load

#include <iostream>
#include <cstdio>
#include <cassert>
#include <string>
#include <filesystem>

#include "../src/aggregator.hpp"

int main() {
    namespace fs = std::filesystem;
    std::string tmp = "test_checkpoint.chk";

    // remove existing
    if (fs::exists(tmp)) fs::remove(tmp);

    // build aggregator and add stuff
    Aggregator a1(10, 4, 2, true, 1<<8, 3, 0, 0);
    std::time_t now = std::time(nullptr);
    ParsedLine p1{"1.1.1.1","GET","/ok",200, now};
    ParsedLine p2{"2.2.2.2","GET","/bad",500, now};
    a1.add_parsed(p1);
    a1.add_parsed(p2);
    a1.add_parse_error();

    bool w = a1.dump_checkpoint(tmp);
    if (!w) {
        std::cerr << "checkpoint: dump failed\n";
        return 2;
    }

    Aggregator a2(10,4,2,true,1<<8,3,0,0);
    bool r = a2.load_checkpoint(tmp);
    if (!r) {
        std::cerr << "checkpoint: load failed\n";
        return 3;
    }

    if (a2.get_total() != a1.get_total()) {
        std::cerr << "checkpoint: total mismatch\n";
        return 4;
    }
    if (a2.get_errors() != a1.get_errors()) {
        std::cerr << "checkpoint: errors mismatch\n";
        return 5;
    }
    auto s1 = a1.snapshot_status_counts();
    auto s2 = a2.snapshot_status_counts();
    if (s1 != s2) {
        std::cerr << "checkpoint: status snapshot mismatch\n";
        return 6;
    }

    // cleanup
    if (fs::exists(tmp)) fs::remove(tmp);

    std::cout << "test_checkpoint: OK\n";
    return 0;
}
