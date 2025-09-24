/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_026, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_036, QCSIDM_SRS_037, QCSIDM_SRS_038, QCSIDM_SRS_040, QCSIDM_SRS_045, QCSIDM_SRS_051, QCSIDM_SRS_067, QCSIDM_SRS_070, QCSIDM_SRS_072, QCSIDM_SRS_084, QCSIDM_SRS_085, QCSIDM_SRS_087, QCSIDM_SRS_089, QCSIDM_SRS_096, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_110, QCSIDM_SRS_116 */
/* ==================================================== */

// tests/test_parser.cpp
#include <iostream>
#include <string>
#include <cassert>
#include <optional>
#include "../src/parser.hpp" // adjust path if your parser header location differs

int main() {
    using namespace std;

    // sample common log line (Apache common)
    string line = R"(127.0.0.1 - - [24/Sep/2025:10:00:00 +0530] "GET /index.html HTTP/1.1" 200 1234)";
    optional<ParsedLine> p = parse_common_log(line);
    if(!p) {
        cerr << "parse_common_log failed to parse valid line\n";
        return 2;
    }
    if(p->ip != "127.0.0.1") {
        cerr << "expected ip 127.0.0.1 got: " << p->ip << "\n";
        return 3;
    }
    if(p->endpoint != "/index.html") {
        cerr << "expected endpoint /index.html got: " << p->endpoint << "\n";
        return 4;
    }
    if(p->status != 200) {
        cerr << "expected status 200 got: " << p->status << "\n";
        return 5;
    }

    // malformed line -> should return empty optional
    string bad = "this is not a log line";
    optional<ParsedLine> p2 = parse_common_log(bad);
    if(p2) {
        cerr << "parse_common_log should have failed on bad line\n";
        return 6;
    }

    cout << "test_parser: OK\n";
    return 0;
}
