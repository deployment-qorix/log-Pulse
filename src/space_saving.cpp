/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_012, QCSIDM_SRS_013, QCSIDM_SRS_014, QCSIDM_SRS_016, QCSIDM_SRS_018, QCSIDM_SRS_019, QCSIDM_SRS_020, QCSIDM_SRS_026, QCSIDM_SRS_027, QCSIDM_SRS_032, QCSIDM_SRS_043, QCSIDM_SRS_044, QCSIDM_SRS_045, QCSIDM_SRS_051, QCSIDM_SRS_053, QCSIDM_SRS_055, QCSIDM_SRS_057, QCSIDM_SRS_058, QCSIDM_SRS_059, QCSIDM_SRS_062, QCSIDM_SRS_066, QCSIDM_SRS_070, QCSIDM_SRS_071, QCSIDM_SRS_072, QCSIDM_SRS_073, QCSIDM_SRS_074, QCSIDM_SRS_081, QCSIDM_SRS_082, QCSIDM_SRS_094, QCSIDM_SRS_095, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_113 */
/* ==================================================== */

#include "space_saving.hpp"
#include <unordered_map>
#include <mutex>
#include <queue>
#include <utility>
#include <algorithm>
#include <stdexcept>

struct SpaceSaving::Impl {
    Impl(size_t cap) : capacity(cap) {
        if (capacity == 0) capacity = 1;
    }

    struct Entry {
        uint64_t count;
        uint64_t error; // guaranteed error bound (for classic SS algorithm)
    };

    // map key -> Entry
    std::unordered_map<std::string, Entry> table;

    // min-heap entries: pair<count, key>. We use a min-heap by count.
    // For lazy updates we may push duplicate keys with larger counts; when popping we validate.
    using HeapItem = std::pair<uint64_t, std::string>;
    struct Cmp {
        bool operator()(const HeapItem &a, const HeapItem &b) const {
            // min-heap: smallest count has highest priority => return a.count > b.count for priority_queue
            return a.first > b.first;
        }
    };
    std::priority_queue<HeapItem, std::vector<HeapItem>, Cmp> minheap;

    size_t capacity;
    mutable std::mutex mu;
};

SpaceSaving::SpaceSaving(size_t capacity) : impl_(nullptr) {
    impl_ = new Impl(capacity);
}

void SpaceSaving::update(const std::string &key, uint64_t cnt) {
    if (cnt == 0) return;
    Impl &I = *impl_;
    std::lock_guard<std::mutex> lk(I.mu);

    auto it = I.table.find(key);
    if (it != I.table.end()) {
        // existing key: increment count and push lazy heap record
        it->second.count += cnt;
        I.minheap.emplace(it->second.count, it->first);
        return;
    }

    // not present
    if (I.table.size() < I.capacity) {
        // has room: insert with count=cnt, error=0
        I.table.emplace(key, Impl::Entry{cnt, 0});
        I.minheap.emplace(cnt, key);
        return;
    }

    // table full: find current minimum by popping lazy heap until top matches current table value
    while (!I.minheap.empty()) {
        auto top = I.minheap.top();
        auto mit = I.table.find(top.second);
        if (mit == I.table.end()) {
            // stale entry for removed key
            I.minheap.pop();
            continue;
        }
        // if top.first doesn't match actual current count for that key, it's stale
        if (top.first != mit->second.count) {
            I.minheap.pop();
            continue;
        }
        // found true min
        break;
    }

    if (I.minheap.empty()) {
        // This shouldn't normally happen (table full but no valid heap entries).
        // Fallback: find minimum by scanning table.
        uint64_t minc = UINT64_MAX;
        std::string minkey;
        for (auto &p : I.table) {
            if (p.second.count < minc) {
                minc = p.second.count;
                minkey = p.first;
            }
        }
        if (minkey.empty()) {
            // extreme degenerate case: just insert by evicting an arbitrary element
            auto it_any = I.table.begin();
            if (it_any != I.table.end()) {
                I.table.erase(it_any);
            }
            I.table.emplace(key, Impl::Entry{cnt, 0});
            I.minheap.emplace(cnt, key);
            return;
        } else {
            // evict minkey
            uint64_t minc = I.table[minkey].count;
            I.table.erase(minkey);
            // insert new key with count = minc + cnt, error = minc
            uint64_t newcount = minc + cnt;
            I.table.emplace(key, Impl::Entry{newcount, minc});
            I.minheap.emplace(newcount, key);
            return;
        }
    }

    // normal path: get top of heap as min
    auto top = I.minheap.top(); I.minheap.pop();
    auto mit = I.table.find(top.second);
    if (mit == I.table.end() || mit->second.count != top.first) {
        // If stale (should be rare due to prior cleaning loop), fallback to scanning min
        uint64_t minc = UINT64_MAX;
        std::string minkey;
        for (auto &p : I.table) {
            if (p.second.count < minc) {
                minc = p.second.count;
                minkey = p.first;
            }
        }
        if (!minkey.empty()) {
            uint64_t minc2 = I.table[minkey].count;
            I.table.erase(minkey);
            uint64_t newcount = minc2 + cnt;
            I.table.emplace(key, Impl::Entry{newcount, minc2});
            I.minheap.emplace(newcount, key);
        } else {
            // no entries found; insert directly
            I.table.emplace(key, Impl::Entry{cnt, 0});
            I.minheap.emplace(cnt, key);
        }
        return;
    }

    // `top` is the true minimum: evict it
    uint64_t min_count = mit->second.count;
    // erase the evicted key
    I.table.erase(mit);
    // insert new key with count = min_count + cnt, error = min_count
    uint64_t newcount = min_count + cnt;
    I.table.emplace(key, Impl::Entry{newcount, min_count});
    I.minheap.emplace(newcount, key);
}

std::vector<std::pair<std::string, uint64_t>> SpaceSaving::topk(size_t k) const {
    Impl &I = *impl_;
    std::lock_guard<std::mutex> lk(I.mu);

    std::vector<std::pair<std::string,uint64_t>> vec;
    vec.reserve(I.table.size());
    for (const auto &p : I.table) {
        vec.emplace_back(p.first, p.second.count);
    }
    if (k == 0 || vec.size() <= k) {
        std::sort(vec.begin(), vec.end(), [](auto &a, auto &b){ return a.second > b.second; });
        return vec;
    }
    // partial sort for top-k
    std::nth_element(vec.begin(), vec.begin() + k, vec.end(),
                     [](auto &a, auto &b){ return a.second > b.second; });
    vec.resize(k);
    std::sort(vec.begin(), vec.end(), [](auto &a, auto &b){ return a.second > b.second; });
    return vec;
}

uint64_t SpaceSaving::estimate(const std::string &key) const {
    Impl &I = *impl_;
    std::lock_guard<std::mutex> lk(I.mu);
    auto it = I.table.find(key);
    if (it == I.table.end()) return 0;
    return it->second.count;
}

void SpaceSaving::reset() {
    Impl &I = *impl_;
    std::lock_guard<std::mutex> lk(I.mu);
    while (!I.minheap.empty()) I.minheap.pop();
    I.table.clear();
}

size_t SpaceSaving::get_capacity() const {
    return impl_->capacity;
}
