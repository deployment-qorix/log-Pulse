/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_001, QCSIDM_SRS_002, QCSIDM_SRS_004, QCSIDM_SRS_006, QCSIDM_SRS_021, QCSIDM_SRS_023, QCSIDM_SRS_024, QCSIDM_SRS_026, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_034, QCSIDM_SRS_036, QCSIDM_SRS_040, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_043, QCSIDM_SRS_044, QCSIDM_SRS_045, QCSIDM_SRS_047, QCSIDM_SRS_049, QCSIDM_SRS_050, QCSIDM_SRS_051, QCSIDM_SRS_052, QCSIDM_SRS_053, QCSIDM_SRS_054, QCSIDM_SRS_055, QCSIDM_SRS_056, QCSIDM_SRS_057, QCSIDM_SRS_058, QCSIDM_SRS_059, QCSIDM_SRS_060, QCSIDM_SRS_062, QCSIDM_SRS_063, QCSIDM_SRS_064, QCSIDM_SRS_066, QCSIDM_SRS_072, QCSIDM_SRS_073, QCSIDM_SRS_076, QCSIDM_SRS_078, QCSIDM_SRS_082, QCSIDM_SRS_084, QCSIDM_SRS_085, QCSIDM_SRS_087, QCSIDM_SRS_089, QCSIDM_SRS_090, QCSIDM_SRS_092, QCSIDM_SRS_093, QCSIDM_SRS_094, QCSIDM_SRS_096, QCSIDM_SRS_098, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_105, QCSIDM_SRS_106, QCSIDM_SRS_108, QCSIDM_SRS_112, QCSIDM_SRS_113, QCSIDM_SRS_116, QCSIDM_SRS_119 */
/* ==================================================== */

#pragma once

#include <atomic>
#include <unordered_map>
#include <shared_mutex>
#include <string>
#include <vector>
#include <ctime>
#include <mutex>
#include <memory>
#include <cstdint>

#include "cms.hpp"
#include "misra_gries.hpp"
#include "space_saving.hpp"

struct ParsedLine {
    std::string ip;
    std::string method;
    std::string endpoint;
    int status;
    std::time_t ts_received;
};

class Aggregator {
    // basic counters
    std::atomic<uint64_t> total_lines{0}, parse_errors{0};

    // sharded ip counts to reduce contention
    size_t ip_shard_count;
    mutable std::vector<std::shared_mutex> ip_shard_mus;
    std::vector<std::unordered_map<std::string,uint64_t>> ip_shards;

    // sharded endpoint counts
    size_t endpoint_shard_count;
    mutable std::vector<std::shared_mutex> endpoint_shard_mus;
    std::vector<std::unordered_map<std::string,uint64_t>> endpoint_shards;

    // status map (single shard)
    mutable std::shared_mutex status_mu;
    std::unordered_map<int,uint64_t> status_counts;

    // sliding window (per-second buckets)
    std::vector<std::atomic<uint64_t>> window;
    size_t window_s;
    mutable std::mutex window_mu;

    // optional Count-Min Sketch for approximate ip counts
    std::unique_ptr<CountMinSketch> ip_cms; // nullptr if disabled

    // optional Misra-Gries heavy hitters for ip and endpoints
    std::unique_ptr<MisraGries> mg_ips;          // approximate top-k for IPs
    std::unique_ptr<MisraGries> mg_endpoints;    // approximate top-k for endpoints

    // optional Space-Saving instances (sharded or global)
    std::unique_ptr<SpaceSaving> ss_ips;
    std::unique_ptr<SpaceSaving> ss_endpoints;

public:
    // constructor:
    // window_seconds, ip_shards_count, endpoint_shards_count,
    // cms_enable, cms_width, cms_depth,
    // mg_k_ips, mg_k_endpoints,
    // ss_ip_cap, ss_ep_cap (0 disables)
    explicit Aggregator(size_t window_seconds = 300,
                        size_t ip_shards_count = 16,
                        size_t endpoint_shards_count = 8,
                        bool cms_enable = true,
                        size_t cms_width = 1<<14,
                        size_t cms_depth = 4,
                        size_t mg_k_ips = 0,
                        size_t mg_k_endpoints = 0,
                        size_t ss_ip_cap = 0,
                        size_t ss_ep_cap = 0);

    // ingest / counters
    void add_parsed(const ParsedLine &p);
    void add_parse_error();

    // reads
    uint64_t get_total() const noexcept;
    uint64_t get_errors() const noexcept;
    double rate_over_seconds(size_t last_n) noexcept; // compute lines/sec over last_n seconds

    // top-k helpers (exact)
    std::vector<std::pair<std::string,uint64_t>> top_k_ips(size_t K) noexcept;
    std::vector<std::pair<std::string,uint64_t>> top_k_endpoints(size_t K) noexcept;

    // Space-Saving top-k (if enabled)
    std::vector<std::pair<std::string,uint64_t>> top_k_ips_ss(size_t K) const noexcept;
    std::vector<std::pair<std::string,uint64_t>> top_k_endpoints_ss(size_t K) const noexcept;

    // approximate top-k (MG-backed)
    std::vector<std::pair<std::string,uint64_t>> approx_top_k_ips(size_t K) const noexcept;
    std::vector<std::pair<std::string,uint64_t>> approx_top_k_endpoints(size_t K) const noexcept;

    // snapshots
    std::unordered_map<int,uint64_t> snapshot_status_counts() const noexcept;
    std::unordered_map<std::string,uint64_t> snapshot_ip_counts() const noexcept; // merged
    std::unordered_map<std::string,uint64_t> snapshot_endpoint_counts() const noexcept;

    // checkpoint persistence (unchanged)
    bool dump_checkpoint(const std::string &path) const;
    bool load_checkpoint(const std::string &path);

    // approximate count via CMS
    uint64_t approx_count_ip(const std::string &ip) const noexcept;
};
