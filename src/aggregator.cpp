/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_001, QCSIDM_SRS_002, QCSIDM_SRS_003, QCSIDM_SRS_004, QCSIDM_SRS_005, QCSIDM_SRS_006, QCSIDM_SRS_007, QCSIDM_SRS_008, QCSIDM_SRS_009, QCSIDM_SRS_010, QCSIDM_SRS_011, QCSIDM_SRS_013, QCSIDM_SRS_014, QCSIDM_SRS_016, QCSIDM_SRS_017, QCSIDM_SRS_018, QCSIDM_SRS_019, QCSIDM_SRS_020, QCSIDM_SRS_021, QCSIDM_SRS_022, QCSIDM_SRS_023, QCSIDM_SRS_024, QCSIDM_SRS_025, QCSIDM_SRS_026, QCSIDM_SRS_027, QCSIDM_SRS_028, QCSIDM_SRS_029, QCSIDM_SRS_030, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_034, QCSIDM_SRS_035, QCSIDM_SRS_036, QCSIDM_SRS_037, QCSIDM_SRS_038, QCSIDM_SRS_039, QCSIDM_SRS_040, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_043, QCSIDM_SRS_044, QCSIDM_SRS_045, QCSIDM_SRS_046, QCSIDM_SRS_047, QCSIDM_SRS_048, QCSIDM_SRS_049, QCSIDM_SRS_050, QCSIDM_SRS_051, QCSIDM_SRS_052, QCSIDM_SRS_053, QCSIDM_SRS_054, QCSIDM_SRS_055, QCSIDM_SRS_056, QCSIDM_SRS_057, QCSIDM_SRS_058, QCSIDM_SRS_059, QCSIDM_SRS_060, QCSIDM_SRS_061, QCSIDM_SRS_062, QCSIDM_SRS_063, QCSIDM_SRS_064, QCSIDM_SRS_065, QCSIDM_SRS_066, QCSIDM_SRS_067, QCSIDM_SRS_068, QCSIDM_SRS_069, QCSIDM_SRS_070, QCSIDM_SRS_071, QCSIDM_SRS_072, QCSIDM_SRS_073, QCSIDM_SRS_074, QCSIDM_SRS_075, QCSIDM_SRS_076, QCSIDM_SRS_077, QCSIDM_SRS_078, QCSIDM_SRS_080, QCSIDM_SRS_081, QCSIDM_SRS_082, QCSIDM_SRS_084, QCSIDM_SRS_085, QCSIDM_SRS_087, QCSIDM_SRS_088, QCSIDM_SRS_089, QCSIDM_SRS_090, QCSIDM_SRS_091, QCSIDM_SRS_092, QCSIDM_SRS_093, QCSIDM_SRS_094, QCSIDM_SRS_095, QCSIDM_SRS_096, QCSIDM_SRS_097, QCSIDM_SRS_098, QCSIDM_SRS_099, QCSIDM_SRS_100, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_104, QCSIDM_SRS_105, QCSIDM_SRS_106, QCSIDM_SRS_107, QCSIDM_SRS_108, QCSIDM_SRS_109, QCSIDM_SRS_110, QCSIDM_SRS_111, QCSIDM_SRS_112, QCSIDM_SRS_113, QCSIDM_SRS_114, QCSIDM_SRS_115, QCSIDM_SRS_116, QCSIDM_SRS_117, QCSIDM_SRS_118, QCSIDM_SRS_119, QCSIDM_SRS_120 */
/* ==================================================== */

#include "aggregator.hpp"
#include "util_log.hpp"
#include <algorithm>
#include <fstream>
#include <cstdio>   // std::remove, std::rename
#include <cstdint>
#include <filesystem>
#include <system_error>
#include <functional>
#include <stdexcept>
#include <limits>
#include <iostream>

static const uint32_t CHECK_MAGIC = 0x4C505353; // 'LPSS'
static const uint32_t CHECK_VERSION = 3;       // includes CMS persistence

// defensive limits
static const size_t MAX_MERGED_ENTRIES = 10'000'000; // arbitrary high cap — adjust as needed
static const size_t WARN_MERGED_ENTRIES = 100'000;   // log if we merge more than this
static const size_t MAX_TOPK = 10'000;              // cap top-k responses

// hash helper
static inline size_t hash_string(const std::string &s) {
    return std::hash<std::string>{}(s);
}
static inline size_t shard_index_for_key(const std::string &key, size_t shard_count) {
    return (shard_count == 0) ? 0 : (hash_string(key) % shard_count);
}

// Constructor
// Fail-safe aggregator ctor — minimal allocations, no large vector reserve at ctor time
// Fail-safe aggregator ctor — constructs vectors without relocating non-movable elements.
Aggregator::Aggregator(size_t window_seconds,
                       size_t ip_shards_count,
                       size_t endpoint_shards_count,
                       bool cms_enable,
                       size_t cms_width,
                       size_t cms_depth,
                       size_t mg_k_ips,
                       size_t mg_k_endpoints,
                       size_t ss_ip_cap,
                       size_t ss_ep_cap)
{
    safe_log(std::string("Aggregator ctor (safe): window_seconds=") + std::to_string(window_seconds)
             + " ip_shards_req=" + std::to_string(ip_shards_count)
             + " endpoint_shards_req=" + std::to_string(endpoint_shards_count));

    // Start conservative: cap requested values to sane maxima to avoid huge allocations.
    // You can tune these caps later.
    const size_t MAX_SHARDS = 256;
    const size_t MAX_WINDOW = 24 * 3600; // 1 day cap

    size_t ip_shards_use = ip_shards_count ? std::min(ip_shards_count, MAX_SHARDS) : 1;
    size_t ep_shards_use = endpoint_shards_count ? std::min(endpoint_shards_count, MAX_SHARDS) : 1;
    size_t window_use = window_seconds ? std::min(window_seconds, MAX_WINDOW) : 300;

    // Assign shard counts
    ip_shard_count = ip_shards_use;
    endpoint_shard_count = ep_shards_use;
    window_s = window_use;

    // Construct vectors with final sizes in one shot (no moves/copies of non-movable elements)
    try {
        ip_shard_mus = std::vector<std::shared_mutex>(ip_shard_count);
        ip_shards = std::vector<std::unordered_map<std::string,uint64_t>>(ip_shard_count);
    } catch(...) {
        // fallback to minimal safe 1-shard
        safe_log("Aggregator ctor: ip shard allocation failed; falling back to 1 shard");
        ip_shard_count = 1;
        ip_shard_mus = std::vector<std::shared_mutex>(1);
        ip_shards = std::vector<std::unordered_map<std::string,uint64_t>>(1);
    }

    try {
        endpoint_shard_mus = std::vector<std::shared_mutex>(endpoint_shard_count);
        endpoint_shards = std::vector<std::unordered_map<std::string,uint64_t>>(endpoint_shard_count);
    } catch(...) {
        safe_log("Aggregator ctor: endpoint shard allocation failed; falling back to 1 shard");
        endpoint_shard_count = 1;
        endpoint_shard_mus = std::vector<std::shared_mutex>(1);
        endpoint_shards = std::vector<std::unordered_map<std::string,uint64_t>>(1);
    }

    // Build the window bucket vector *in one shot* to avoid moving atomics.
    try {
        window = std::vector<std::atomic<uint64_t>>(window_s);
        for (size_t i = 0; i < window_s; ++i) window[i].store(0);
    } catch(...) {
        safe_log("Aggregator ctor: window allocation failed; falling back to window 300");
        window_s = 300;
        window = std::vector<std::atomic<uint64_t>>(window_s);
        for (size_t i = 0; i < window_s; ++i) window[i].store(0);
    }

    // Try to create optional heavy structures; on failure leave them nullptr.
    try { if (cms_enable) ip_cms = std::make_unique<CountMinSketch>(cms_width, cms_depth, 0xA5A5A5A5ULL); } catch(...) { ip_cms.reset(); }
    try { if (mg_k_ips > 0) mg_ips = std::make_unique<MisraGries>(mg_k_ips); } catch(...) { mg_ips.reset(); }
    try { if (mg_k_endpoints > 0) mg_endpoints = std::make_unique<MisraGries>(mg_k_endpoints); } catch(...) { mg_endpoints.reset(); }
    try { if (ss_ip_cap > 0) ss_ips = std::make_unique<SpaceSaving>(ss_ip_cap); } catch(...) { ss_ips.reset(); }
    try { if (ss_ep_cap > 0) ss_endpoints = std::make_unique<SpaceSaving>(ss_ep_cap); } catch(...) { ss_endpoints.reset(); }

    safe_log(std::string("Aggregator ctor completed: ip_shards=") + std::to_string(ip_shard_count)
             + ", endpoint_shards=" + std::to_string(endpoint_shard_count)
             + ", window_s=" + std::to_string(window_s));
}

// add parsed line: update shards, cms, mg, status, window
void Aggregator::add_parsed(const ParsedLine &p){
    total_lines.fetch_add(1, std::memory_order_relaxed);

    // ip shard update
    try {
        size_t idx = shard_index_for_key(p.ip, ip_shard_count ? ip_shard_count : 1);
        std::unique_lock<std::shared_mutex> lk(ip_shard_mus[idx]);
        ip_shards[idx][p.ip] += 1;
    } catch (const std::exception &e) {
        safe_log(std::string("add_parsed: ip shard update failed: ") + e.what());
    } catch (...) {
        safe_log("add_parsed: ip shard update unknown failure");
    }

    // CMS
    try {
        if(ip_cms) ip_cms->add(p.ip, 1);
    } catch(...) {}

    // endpoint shard update
    try {
        size_t eidx = shard_index_for_key(p.endpoint, endpoint_shard_count ? endpoint_shard_count : 1);
        std::unique_lock<std::shared_mutex> lk(endpoint_shard_mus[eidx]);
        endpoint_shards[eidx][p.endpoint] += 1;
    } catch (const std::exception &e) {
        safe_log(std::string("add_parsed: endpoint shard update failed: ") + e.what());
    } catch (...) {
        safe_log("add_parsed: endpoint shard update unknown failure");
    }

    // Misra–Gries (approx top-k)
    try {
        if(mg_ips) mg_ips->add(p.ip, 1);
        if(mg_endpoints) mg_endpoints->add(p.endpoint, 1);
    } catch(...) {}

    // SpaceSaving update
    try {
        if(ss_ips) ss_ips->update(p.ip, 1);
        if(ss_endpoints) ss_endpoints->update(p.endpoint, 1);
    } catch(...) {}

    // status counts
    try {
        std::unique_lock<std::shared_mutex> lk(status_mu);
        status_counts[p.status] += 1;
    } catch(...) {}

    // sliding window (per-second bucket)
    try {
        std::lock_guard<std::mutex> lk(window_mu);
        size_t wi = static_cast<size_t>(p.ts_received % window_s);
        window[wi].fetch_add(1, std::memory_order_relaxed);
    } catch(...) {}
}

void Aggregator::add_parse_error(){ parse_errors.fetch_add(1); }
uint64_t Aggregator::get_total() const noexcept { return total_lines.load(); }
uint64_t Aggregator::get_errors() const noexcept { return parse_errors.load(); }

double Aggregator::rate_over_seconds(size_t last_n) noexcept {
    if(last_n == 0) return 0.0;
    try {
        std::lock_guard<std::mutex> lk(window_mu);
        size_t now = static_cast<size_t>(std::time(nullptr));
        uint64_t sum = 0;
        for(size_t i=0;i<last_n && i<window_s;i++){
            size_t sec = now - i;
            size_t idx = sec % window_s;
            sum += window[idx].load();
        }
        return double(sum) / double(last_n);
    } catch(...) {
        safe_log("rate_over_seconds: exception computing rate");
        return 0.0;
    }
}

// merge ip shards then compute top-K exact
std::vector<std::pair<std::string,uint64_t>> Aggregator::top_k_ips(size_t K) noexcept {
    try {
        if (K == 0) K = 10;
        if (K > MAX_TOPK) K = MAX_TOPK;

        std::unordered_map<std::string,uint64_t> merged;
        merged.reserve(1024);

        size_t total_entries = 0;
        for(size_t s=0; s < ip_shard_count; ++s){
            std::shared_lock<std::shared_mutex> lk(ip_shard_mus[s]);
            for(const auto &p : ip_shards[s]) {
                merged[p.first] += p.second;
                ++total_entries;
                if (merged.size() > MAX_MERGED_ENTRIES) {
                    safe_log("top_k_ips: merged entries exceed MAX_MERGED_ENTRIES; trimming and aborting heavy merge");
                    // early return limited result
                    return {};
                }
            }
        }

        if (total_entries > WARN_MERGED_ENTRIES) {
            safe_log(std::string("top_k_ips: warning merged total entries=") + std::to_string(total_entries));
        }

        std::vector<std::pair<std::string,uint64_t>> vec;
        try {
            vec.reserve(merged.size());
        } catch (const std::length_error &le) {
            safe_log(std::string("top_k_ips: reserve failed: ") + le.what());
            // fall back to default small container
            vec.clear();
        }
        for(auto &p : merged) vec.emplace_back(p.first, p.second);

        if(vec.size() <= K){
            std::sort(vec.begin(), vec.end(), [](auto &a, auto &b){ return a.second > b.second; });
            return vec;
        }
        std::nth_element(vec.begin(), vec.begin()+K, vec.end(), [](auto &a, auto &b){ return a.second > b.second; });
        vec.resize(K);
        std::sort(vec.begin(), vec.end(), [](auto &a, auto &b){ return a.second > b.second; });
        return vec;
    } catch(const std::exception &e) {
        safe_log(std::string("top_k_ips: exception: ") + e.what());
        return {};
    } catch(...) {
        safe_log("top_k_ips: unknown exception");
        return {};
    }
}

// merge endpoint shards then compute top-K exact
std::vector<std::pair<std::string,uint64_t>> Aggregator::top_k_endpoints(size_t K) noexcept {
    try {
        if (K == 0) K = 10;
        if (K > MAX_TOPK) K = MAX_TOPK;

        std::unordered_map<std::string,uint64_t> merged;
        merged.reserve(1024);

        size_t total_entries = 0;
        for(size_t s=0; s < endpoint_shard_count; ++s){
            std::shared_lock<std::shared_mutex> lk(endpoint_shard_mus[s]);
            for(const auto &p : endpoint_shards[s]) {
                merged[p.first] += p.second;
                ++total_entries;
                if (merged.size() > MAX_MERGED_ENTRIES) {
                    safe_log("top_k_endpoints: merged entries exceed MAX_MERGED_ENTRIES; trimming and aborting heavy merge");
                    return {};
                }
            }
        }

        if (total_entries > WARN_MERGED_ENTRIES) {
            safe_log(std::string("top_k_endpoints: warning merged total entries=") + std::to_string(total_entries));
        }

        std::vector<std::pair<std::string,uint64_t>> vec;
        try {
            vec.reserve(merged.size());
        } catch(...) { vec.clear(); }
        for(auto &p : merged) vec.emplace_back(p.first, p.second);

        if(vec.size() <= K){
            std::sort(vec.begin(), vec.end(), [](auto &a, auto &b){ return a.second > b.second; });
            return vec;
        }
        std::nth_element(vec.begin(), vec.begin()+K, vec.end(), [](auto &a, auto &b){ return a.second > b.second; });
        vec.resize(K);
        std::sort(vec.begin(), vec.end(), [](auto &a, auto &b){ return a.second > b.second; });
        return vec;
    } catch(...) {
        safe_log("top_k_endpoints: unknown exception");
        return {};
    }
}

// Space-Saving and MG / approximate
std::vector<std::pair<std::string,uint64_t>> Aggregator::top_k_ips_ss(size_t K) const noexcept {
    try {
        if (!ss_ips) return {};
        if (K == 0) K = 10;
        if (K > MAX_TOPK) K = MAX_TOPK;
        auto v = ss_ips->topk(K);
        if (v.size() > K) v.resize(K);
        return v;
    } catch(...) {
        safe_log("top_k_ips_ss: exception");
        return {};
    }
}
std::vector<std::pair<std::string,uint64_t>> Aggregator::top_k_endpoints_ss(size_t K) const noexcept {
    try {
        if (!ss_endpoints) return {};
        if (K == 0) K = 10;
        if (K > MAX_TOPK) K = MAX_TOPK;
        auto v = ss_endpoints->topk(K);
        if (v.size() > K) v.resize(K);
        return v;
    } catch(...) {
        safe_log("top_k_endpoints_ss: exception");
        return {};
    }
}

std::vector<std::pair<std::string,uint64_t>> Aggregator::approx_top_k_ips(size_t K) const noexcept {
    if(!mg_ips) return {};
    auto v = mg_ips->get_heavy_hitters();
    if(v.size() > K) v.resize(K);
    return v;
}
std::vector<std::pair<std::string,uint64_t>> Aggregator::approx_top_k_endpoints(size_t K) const noexcept {
    if(!mg_endpoints) return {};
    auto v = mg_endpoints->get_heavy_hitters();
    if(v.size() > K) v.resize(K);
    return v;
}

std::unordered_map<int,uint64_t> Aggregator::snapshot_status_counts() const noexcept {
    std::unordered_map<int,uint64_t> out;
    try {
        std::shared_lock<std::shared_mutex> lk(status_mu);
        out.reserve(status_counts.size());
        for(const auto &p : status_counts) out[p.first] = p.second;
    } catch(...) {
        safe_log("snapshot_status_counts: exception — returning empty map");
    }
    return out;
}

std::unordered_map<std::string,uint64_t> Aggregator::snapshot_endpoint_counts() const noexcept {
    std::unordered_map<std::string,uint64_t> merged;
    try {
        merged.reserve(1024);
        size_t total_entries = 0;
        for(size_t s=0;s<endpoint_shard_count;++s){
            std::shared_lock<std::shared_mutex> lk(endpoint_shard_mus[s]);
            for(const auto &p : endpoint_shards[s]) {
                merged[p.first] += p.second;
                ++total_entries;
                if (merged.size() > MAX_MERGED_ENTRIES) {
                    safe_log("snapshot_endpoint_counts: merged entries exceed limit; aborting and returning partial snapshot");
                    return merged;
                }
            }
        }
        if (total_entries > WARN_MERGED_ENTRIES) {
            safe_log(std::string("snapshot_endpoint_counts: merged total entries=") + std::to_string(total_entries));
        }
    } catch(...) {
        safe_log("snapshot_endpoint_counts: exception");
    }
    return merged;
}

std::unordered_map<std::string,uint64_t> Aggregator::snapshot_ip_counts() const noexcept {
    std::unordered_map<std::string,uint64_t> merged;
    try {
        merged.reserve(1024);
        size_t total_entries = 0;
        for(size_t s=0;s<ip_shard_count;++s){
            std::shared_lock<std::shared_mutex> lk(ip_shard_mus[s]);
            for(const auto &p : ip_shards[s]) {
                merged[p.first] += p.second;
                ++total_entries;
                if (merged.size() > MAX_MERGED_ENTRIES) {
                    safe_log("snapshot_ip_counts: merged entries exceed limit; aborting and returning partial snapshot");
                    return merged;
                }
            }
        }
        if (total_entries > WARN_MERGED_ENTRIES) {
            safe_log(std::string("snapshot_ip_counts: merged total entries=") + std::to_string(total_entries));
        }
    } catch(...) {
        safe_log("snapshot_ip_counts: exception");
    }
    return merged;
}

// ---------------- Checkpointing (v3 w/ optional CMS) ----------------

static bool write_u32(std::ofstream &o, uint32_t v){ o.write(reinterpret_cast<const char*>(&v), sizeof(v)); return !!o; }
static bool write_u64(std::ofstream &o, uint64_t v){ o.write(reinterpret_cast<const char*>(&v), sizeof(v)); return !!o; }
static bool write_u8(std::ofstream &o, uint8_t v){ o.write(reinterpret_cast<const char*>(&v), sizeof(v)); return !!o; }
static bool write_str_u32(std::ofstream &o, const std::string &s){
    uint32_t L = static_cast<uint32_t>(s.size());
    if(!write_u32(o, L)) return false;
    o.write(s.data(), L);
    return !!o;
}

bool Aggregator::dump_checkpoint(const std::string &path) const {
    std::string tmp = path + ".tmp";
    std::ofstream o(tmp, std::ios::binary | std::ios::trunc);
    if(!o) return false;

    if(!write_u32(o, CHECK_MAGIC)) return false;
    if(!write_u32(o, CHECK_VERSION)) return false;

    uint64_t tot = total_lines.load();
    uint64_t errs = parse_errors.load();
    if(!write_u64(o, tot)) return false;
    if(!write_u64(o, errs)) return false;

    // merged ip map
    auto merged_ips = snapshot_ip_counts();
    if(!write_u64(o, static_cast<uint64_t>(merged_ips.size()))) return false;
    for(const auto &p : merged_ips){
        if(!write_str_u32(o, p.first)) return false;
        if(!write_u64(o, p.second)) return false;
    }

    // merged endpoints
    auto merged_eps = snapshot_endpoint_counts();
    if(!write_u64(o, static_cast<uint64_t>(merged_eps.size()))) return false;
    for(const auto &p : merged_eps){
        if(!write_str_u32(o, p.first)) return false;
        if(!write_u64(o, p.second)) return false;
    }

    // status
    auto st = snapshot_status_counts();
    if(!write_u64(o, static_cast<uint64_t>(st.size()))) return false;
    for(const auto &p : st){
        int32_t s = static_cast<int32_t>(p.first);
        o.write(reinterpret_cast<const char*>(&s), sizeof(s));
        if(!o) return false;
        if(!write_u64(o, p.second)) return false;
    }

    // window
    {
        std::lock_guard<std::mutex> lk(window_mu);
        if(!write_u64(o, static_cast<uint64_t>(window.size()))) return false;
        for(const auto &b : window){
            uint64_t v = b.load();
            if(!write_u64(o, v)) return false;
        }
    }

    // CMS presence & data (v3)
    uint8_t cms_present = (ip_cms ? 1 : 0);
    if(!write_u8(o, cms_present)) return false;
    if(cms_present){
        size_t w = ip_cms->get_width();
        size_t d = ip_cms->get_depth();
        uint64_t seed = ip_cms->get_seed0();

        if(!write_u64(o, static_cast<uint64_t>(w))) return false;
        if(!write_u64(o, static_cast<uint64_t>(d))) return false;
        if(!write_u64(o, seed)) return false;

        for(size_t r=0;r<d;r++){
            for(size_t j=0;j<w;j++){
                uint64_t v = ip_cms->get_counter(r, j);
                if(!write_u64(o, v)) return false;
            }
        }
    }

    o.close();

    // atomic replace
    std::error_code ec;
    std::remove(path.c_str());
    if(std::rename(tmp.c_str(), path.c_str()) != 0){
        try {
            std::filesystem::rename(std::filesystem::path(tmp), std::filesystem::path(path));
        } catch(...) {
            std::remove(tmp.c_str());
            return false;
        }
    }
    return true;
}

// read helpers
static bool read_u32(std::ifstream &i, uint32_t &out){
    i.read(reinterpret_cast<char*>(&out), sizeof(out));
    return !!i;
}
static bool read_u64(std::ifstream &i, uint64_t &out){
    i.read(reinterpret_cast<char*>(&out), sizeof(out));
    return !!i;
}
static bool read_u8(std::ifstream &i, uint8_t &out){
    i.read(reinterpret_cast<char*>(&out), sizeof(out));
    return !!i;
}
static bool read_str_u32(std::ifstream &i, std::string &out){
    uint32_t L;
    if(!read_u32(i, L)) return false;
    out.resize(L);
    i.read(&out[0], L);
    return !!i;
}

bool Aggregator::load_checkpoint(const std::string &path){
    std::ifstream in(path, std::ios::binary);
    if(!in) return false;

    uint32_t magic=0, ver=0;
    if(!read_u32(in, magic)) return false;
    if(magic != CHECK_MAGIC) return false;
    if(!read_u32(in, ver)) return false;
    if(ver < 1) return false;

    uint64_t tot=0, errs=0;
    if(!read_u64(in, tot)) return false;
    if(!read_u64(in, errs)) return false;

    // ip entries (merged)
    uint64_t nip=0;
    if(!read_u64(in, nip)) return false;
    std::vector<std::pair<std::string,uint64_t>> ip_entries;
    ip_entries.reserve(static_cast<size_t>(nip));
    for(uint64_t i=0;i<nip;i++){
        std::string key;
        if(!read_str_u32(in, key)) return false;
        uint64_t val=0;
        if(!read_u64(in, val)) return false;
        ip_entries.emplace_back(std::move(key), val);
    }

    // endpoint_counts (v>=2)
    std::unordered_map<std::string,uint64_t> ep_snapshot;
    if(ver >= 2){
        uint64_t nep=0;
        if(!read_u64(in, nep)) return false;
        for(uint64_t k=0;k<nep;k++){
            std::string key;
            if(!read_str_u32(in, key)) return false;
            uint64_t val=0;
            if(!read_u64(in, val)) return false;
            ep_snapshot.emplace(std::move(key), val);
        }
    }

    // status_counts
    uint64_t nstatus=0;
    if(!read_u64(in, nstatus)) return false;
    std::unordered_map<int,uint64_t> status_snapshot;
    for(uint64_t k=0;k<nstatus;k++){
        int32_t st;
        in.read(reinterpret_cast<char*>(&st), sizeof(st));
        if(!in) return false;
        uint64_t val=0;
        if(!read_u64(in, val)) return false;
        status_snapshot.emplace(static_cast<int>(st), val);
    }

    // window
    uint64_t wsize=0;
    if(!read_u64(in, wsize)) return false;
    std::vector<uint64_t> buckets;
    buckets.reserve(static_cast<size_t>(wsize));
    for(uint64_t i=0;i<wsize;i++){
        uint64_t v=0;
        if(!read_u64(in, v)) return false;
        buckets.push_back(v);
    }

    // CMS (v3+)
    bool file_has_cms = false;
    size_t cms_w = 0, cms_d = 0;
    uint64_t cms_seed = 0;
    std::vector<uint64_t> cms_table;
    if(ver >= 3){
        uint8_t cms_present = 0;
        if(!read_u8(in, cms_present)) return false;
        if(cms_present){
            file_has_cms = true;
            uint64_t w=0,d=0,s=0;
            if(!read_u64(in, w)) return false;
            if(!read_u64(in, d)) return false;
            if(!read_u64(in, s)) return false;
            cms_w = static_cast<size_t>(w);
            cms_d = static_cast<size_t>(d);
            cms_seed = s;
            try {
                cms_table.resize(cms_w * cms_d);
            } catch(...) { return false; }
            for(size_t r=0;r<cms_d;r++){
                for(size_t j=0;j<cms_w;j++){
                    uint64_t v=0;
                    if(!read_u64(in, v)) return false;
                    cms_table[r * cms_w + j] = v;
                }
            }
        }
    }

    // install basic counters
    total_lines.store(tot);
    parse_errors.store(errs);

    // clear and populate ip shards
    for(size_t s=0; s < ip_shard_count; ++s){
        std::unique_lock<std::shared_mutex> lk(ip_shard_mus[s]);
        ip_shards[s].clear();
    }
    for(auto &kv : ip_entries){
        size_t sh = shard_index_for_key(kv.first, ip_shard_count ? ip_shard_count : 1);
        std::unique_lock<std::shared_mutex> lk(ip_shard_mus[sh]);
        ip_shards[sh][kv.first] = kv.second;
    }

    // distribute endpoints into endpoint shards
    for(size_t s=0; s < endpoint_shard_count; ++s){
        std::unique_lock<std::shared_mutex> lk(endpoint_shard_mus[s]);
        endpoint_shards[s].clear();
    }
    for(auto &kv : ep_snapshot){
        size_t sh = shard_index_for_key(kv.first, endpoint_shard_count ? endpoint_shard_count : 1);
        std::unique_lock<std::shared_mutex> lk(endpoint_shard_mus[sh]);
        endpoint_shards[sh][kv.first] = kv.second;
    }

    {
        std::unique_lock<std::shared_mutex> lk(status_mu);
        status_counts.swap(status_snapshot);
    }

    {
        std::lock_guard<std::mutex> lk(window_mu);
        if(buckets.size() == window.size()){
            for(size_t i=0;i<window.size();++i) window[i].store(buckets[i]);
        } else {
            for(auto &b : window) b.store(0);
            size_t copy_n = std::min(window.size(), buckets.size());
            for(size_t i=0;i<copy_n;i++) window[i].store(buckets[i]);
        }
    }

    // install CMS if file had it
    if(file_has_cms){
        if(ip_cms && ip_cms->get_width() == cms_w && ip_cms->get_depth() == cms_d && ip_cms->get_seed0() == cms_seed){
            for(size_t r=0;r<cms_d;r++){
                for(size_t j=0;j<cms_w;j++){
                    uint64_t v = cms_table[r * cms_w + j];
                    ip_cms->set_counter(r, j, v);
                }
            }
        } else {
            try {
                ip_cms = std::make_unique<CountMinSketch>(cms_w, cms_d, cms_seed);
                for(size_t r=0;r<cms_d;r++){
                    for(size_t j=0;j<cms_w;j++){
                        uint64_t v = cms_table[r * cms_w + j];
                        ip_cms->set_counter(r, j, v);
                    }
                }
            } catch(...) {
                ip_cms.reset();
            }
        }
    }

    return true;
}

// CMS approximate count
uint64_t Aggregator::approx_count_ip(const std::string &ip) const noexcept {
    try {
        if(!ip_cms) return 0;
        return ip_cms->estimate(ip);
    } catch(...) {
        return 0;
    }
}
