/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_043, QCSIDM_SRS_044, QCSIDM_SRS_045, QCSIDM_SRS_053, QCSIDM_SRS_062, QCSIDM_SRS_064, QCSIDM_SRS_066, QCSIDM_SRS_094 */
/* ==================================================== */

#pragma once
#include <unordered_map>
#include <string>
#include <vector>
#include <mutex>
#include <cstdint>

class MisraGries {
public:
    // capacity: maximum number of counters stored (approx-top-k)
    explicit MisraGries(size_t capacity = 1024);

    // add one occurrence of key
    void add(const std::string &key, uint64_t cnt = 1);

    // estimate count for a key (may be <= true count)
    uint64_t estimate(const std::string &key) const;

    // get snapshot of counters as vector<pair<key,count>>
    std::vector<std::pair<std::string,uint64_t>> get_heavy_hitters() const;

    // clear / reset
    void reset();

private:
    size_t capacity_;
    mutable std::mutex mu_;
    std::unordered_map<std::string,uint64_t> counters_;
};
