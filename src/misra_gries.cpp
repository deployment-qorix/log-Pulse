/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_002, QCSIDM_SRS_014, QCSIDM_SRS_018, QCSIDM_SRS_031, QCSIDM_SRS_051, QCSIDM_SRS_053, QCSIDM_SRS_055, QCSIDM_SRS_064, QCSIDM_SRS_066, QCSIDM_SRS_073, QCSIDM_SRS_094, QCSIDM_SRS_113 */
/* ==================================================== */

#include "misra_gries.hpp"
#include <algorithm>

MisraGries::MisraGries(size_t capacity)
    : capacity_(capacity)
{
    if(capacity_ == 0) capacity_ = 1;
}

void MisraGries::add(const std::string &key, uint64_t cnt){
    std::lock_guard<std::mutex> lk(mu_);
    // fast path: existing key
    auto it = counters_.find(key);
    if(it != counters_.end()){
        it->second += cnt;
        return;
    }

    // if room, insert
    if(counters_.size() < capacity_){
        counters_.emplace(key, cnt);
        return;
    }

    // otherwise, decrement all by cnt (Misra-Gries generalized)
    // If cnt is large, repeated decrements might be heavy; but we do a single pass.
    // subtract cnt from all entries; remove non-positive.
    std::vector<std::string> to_erase;
    for(auto &p : counters_){
        if(p.second <= cnt){
            to_erase.push_back(p.first);
        } else {
            p.second -= cnt;
        }
    }
    for(const auto &k : to_erase) counters_.erase(k);
    // If after decrements there is room, insert key with remaining cnt (common MG variants do not insert after decrement;
    // we follow the common variant and only insert if there's room).
    if(counters_.size() < capacity_){
        counters_.emplace(key, cnt);
    }
}

uint64_t MisraGries::estimate(const std::string &key) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = counters_.find(key);
    if(it == counters_.end()) return 0;
    return it->second;
}

std::vector<std::pair<std::string,uint64_t>> MisraGries::get_heavy_hitters() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::pair<std::string,uint64_t>> out;
    out.reserve(counters_.size());
    for(const auto &p : counters_) out.emplace_back(p.first, p.second);
    // sort descending by count
    std::sort(out.begin(), out.end(), [](auto &a, auto &b){ return a.second > b.second; });
    return out;
}

void MisraGries::reset(){
    std::lock_guard<std::mutex> lk(mu_);
    counters_.clear();
}
