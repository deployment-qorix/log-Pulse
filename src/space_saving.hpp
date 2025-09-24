/* ==================================================== */
/* REQ-MAPPING: implements QCSIDM_SRS_004, QCSIDM_SRS_006, QCSIDM_SRS_011, QCSIDM_SRS_012, QCSIDM_SRS_013, QCSIDM_SRS_019, QCSIDM_SRS_020, QCSIDM_SRS_021, QCSIDM_SRS_022, QCSIDM_SRS_023, QCSIDM_SRS_026, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_051, QCSIDM_SRS_062, QCSIDM_SRS_064, QCSIDM_SRS_066, QCSIDM_SRS_068, QCSIDM_SRS_072, QCSIDM_SRS_074, QCSIDM_SRS_082, QCSIDM_SRS_101, QCSIDM_SRS_112, QCSIDM_SRS_113 */
/* ==================================================== */

#pragma once
#include <string>
#include <vector>
#include <cstdint>

/// SpaceSaving: memory-bounded approximate heavy-hitters (Space-Saving / Metwally)
/// - Capacity defines the maximum number of tracked keys.
/// - update(key, cnt) processes an occurrence (or multiple).
/// - topk(k) returns the top-k items (approximate counts), sorted desc by count.
///
/// Thread-safe: all public methods lock an internal mutex.
class SpaceSaving {
public:
    explicit SpaceSaving(size_t capacity = 1024);

    // Process an occurrence of `key`. cnt defaults to 1 for one occurrence.
    void update(const std::string &key, uint64_t cnt = 1);

    // Return up to k items as (key, count), sorted by descending count.
    std::vector<std::pair<std::string, uint64_t>> topk(size_t k) const;

    // Return the approximate count for a key (0 if not present).
    uint64_t estimate(const std::string &key) const;

    // Reset internal state.
    void reset();

    // Returns capacity
    size_t get_capacity() const;

private:
    // hide implementation details in cpp
    struct Impl;
    Impl *impl_;
};
