/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_006, QCSIDM_SRS_021, QCSIDM_SRS_030, QCSIDM_SRS_031, QCSIDM_SRS_037, QCSIDM_SRS_047, QCSIDM_SRS_048, QCSIDM_SRS_051, QCSIDM_SRS_062, QCSIDM_SRS_064, QCSIDM_SRS_066, QCSIDM_SRS_070, QCSIDM_SRS_072, QCSIDM_SRS_073, QCSIDM_SRS_074, QCSIDM_SRS_082, QCSIDM_SRS_094, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_105, QCSIDM_SRS_110, QCSIDM_SRS_112, QCSIDM_SRS_113, QCSIDM_SRS_119, QCSIDM_SRS_120 */
/* ==================================================== */

// src/cms.cpp
#include "cms.hpp"
#include <functional>
#include <cstring>

// splitmix64 to derive seeds
static uint64_t splitmix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

CountMinSketch::CountMinSketch(size_t w, size_t d, uint64_t seed_)
    : width(w), depth(d), table(d, std::vector<uint64_t>(w, 0)), seeds(d)
{
    uint64_t s = seed_;
    for(size_t i=0;i<d;i++){
        s = splitmix64(s + i + 0x9e3779b97f4a7c15ULL);
        seeds[i] = s;
    }
}

uint64_t CountMinSketch::hash64(const std::string &s, uint64_t seed) const {
    // FNV-1a 64-bit variant seeded
    uint64_t h = 14695981039346656037ULL ^ seed;
    for(unsigned char c : s) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    // final mix
    h ^= (h >> 33);
    h *= 0xff51afd7ed558ccdULL;
    h ^= (h >> 33);
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= (h >> 33);
    return h;
}

void CountMinSketch::add(const std::string &key, uint64_t x){
    for(size_t r=0;r<depth;r++){
        uint64_t h = hash64(key, seeds[r]);
        size_t idx = static_cast<size_t>(h % width);
        table[r][idx] += x;
    }
}

uint64_t CountMinSketch::estimate(const std::string &key) const {
    uint64_t best = UINT64_MAX;
    for(size_t r=0;r<depth;r++){
        uint64_t h = hash64(key, seeds[r]);
        size_t idx = static_cast<size_t>(h % width);
        uint64_t v = table[r][idx];
        if(v < best) best = v;
    }
    if(best == UINT64_MAX) return 0;
    return best;
}

void CountMinSketch::reset(){
    for(size_t r=0;r<depth;r++){
        std::fill(table[r].begin(), table[r].end(), 0);
    }
}
