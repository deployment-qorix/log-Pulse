/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_004, QCSIDM_SRS_006, QCSIDM_SRS_011, QCSIDM_SRS_012, QCSIDM_SRS_013, QCSIDM_SRS_014, QCSIDM_SRS_015, QCSIDM_SRS_021, QCSIDM_SRS_023, QCSIDM_SRS_027, QCSIDM_SRS_102 */
/* ==================================================== */

#include <iostream>
#include <thread>
#include <vector>
#include <cassert>

#include "../src/bounded_queue.hpp"

int main() {
    BoundedQueue<int> q(10);
    std::vector<int> got;
    bool consumer_done = false;

    std::thread consumer([&]{
        int v;
        while (q.pop(v)) {
            got.push_back(v);
        }
        consumer_done = true;
    });

    for (int i = 0; i < 10; i++) {
        if (!q.push(i)) {
            std::cerr << "bounded_queue: push failed unexpectedly\n";
            return 1;
        }
    }

    // Close queue, let consumer exit
    q.notify_all();
    consumer.join();

    if (!consumer_done) {
        std::cerr << "bounded_queue: consumer did not exit\n";
        return 2;
    }

    if (got.size() != 10) {
        std::cerr << "bounded_queue: expected 10 items, got " << got.size() << "\n";
        return 3;
    }

    std::cout << "test_bounded_queue: OK\n";
    return 0;
}
