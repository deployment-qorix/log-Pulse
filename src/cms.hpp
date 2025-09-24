/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_053, QCSIDM_SRS_064, QCSIDM_SRS_066, QCSIDM_SRS_119 */
/* ==================================================== */

// src/cms.hpp
#pragma once
#include <vector>
#include <string>
#include <cstdint>

class CountMinSketch {
public:
    CountMinSketch(size_t width = 1<<16, size_t depth = 4, uint64_t seed = 0xC0FFEE);

    void add(const std::string &key, uint64_t x = 1);
    uint64_t estimate(const std::string &key) const;
    void reset();
        // accessors for persistence
    size_t get_width() const { return width; }
    size_t get_depth() const { return depth; }
    uint64_t get_seed0() const { return seeds.empty() ? 0 : seeds[0]; } // we store derived seeds; this returns seed0 for metadata
    uint64_t get_counter(size_t row, size_t idx) const { return table[row][idx]; }
    void set_counter(size_t row, size_t idx, uint64_t v) { table[row][idx] = v; }


private:
    size_t width;
    size_t depth;
    std::vector<std::vector<uint64_t>> table;
    std::vector<uint64_t> seeds;

    uint64_t hash64(const std::string &s, uint64_t seed) const;
};
