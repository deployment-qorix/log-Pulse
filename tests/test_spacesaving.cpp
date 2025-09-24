/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_026, QCSIDM_SRS_051, QCSIDM_SRS_062, QCSIDM_SRS_064, QCSIDM_SRS_066, QCSIDM_SRS_070, QCSIDM_SRS_072, QCSIDM_SRS_074, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_109 */
/* ==================================================== */

// tests/test_spacesaving.cpp
// Simple unit test for SpaceSaving implementation.

#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include "../src/space_saving.hpp" // adjust path if needed

int main() {
    using namespace std;

    // capacity 3: keep top-3 approx items
    SpaceSaving ss(3);

    // Feed items: A x100, B x60, C x30, D x10, E x5
    for (int i=0;i<100;i++) ss.update("A", 1);
    for (int i=0;i<60;i++)  ss.update("B", 1);
    for (int i=0;i<30;i++)  ss.update("C", 1);
    for (int i=0;i<10;i++)  ss.update("D", 1);
    for (int i=0;i<5;i++)   ss.update("E", 1);

    auto top3 = ss.topk(3);

    if (top3.size() == 0) {
        std::cerr << "SpaceSaving::topk returned empty\n";
        return 2;
    }

    // Expect A and B definitely in top3 (approx algorithm should keep heavy hitters)
    bool sawA=false, sawB=false;
    for (auto &p : top3) {
        if (p.first == "A") sawA = true;
        if (p.first == "B") sawB = true;
    }
    if (!sawA) {
        std::cerr << "Expected A in top3 but not found\n";
        return 3;
    }
    if (!sawB) {
        std::cerr << "Expected B in top3 but not found\n";
        return 4;
    }

    // The counts returned are estimates; ensure they are reasonably in-range
    for (auto &p : top3) {
        if (p.second == 0) {
            std::cerr << "Expected nonzero estimate for " << p.first << "\n";
            return 5;
        }
    }

    std::cout << "test_spacesaving: OK\n";
    return 0;
}
