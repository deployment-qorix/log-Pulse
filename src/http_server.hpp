/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_002, QCSIDM_SRS_011, QCSIDM_SRS_021, QCSIDM_SRS_023, QCSIDM_SRS_025, QCSIDM_SRS_026, QCSIDM_SRS_027, QCSIDM_SRS_029, QCSIDM_SRS_036, QCSIDM_SRS_040, QCSIDM_SRS_051, QCSIDM_SRS_053, QCSIDM_SRS_055, QCSIDM_SRS_067, QCSIDM_SRS_073, QCSIDM_SRS_081, QCSIDM_SRS_083, QCSIDM_SRS_084, QCSIDM_SRS_087, QCSIDM_SRS_088, QCSIDM_SRS_089, QCSIDM_SRS_090, QCSIDM_SRS_099, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_116 */
/* ==================================================== */

#pragma once
#include <string>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include "aggregator.hpp"

class HttpServer {
public:
    HttpServer(const std::string &bind_addr,
               uint16_t port,
               Aggregator &agg_ref,
               unsigned cache_ttl_seconds = 1,
               const std::string &auth_expected = "");
    ~HttpServer();

    bool start();
    void stop();

    // Rebuild cached metrics body immediately (safe to call from main)
    void rebuild_cache_now();

private:
    void accept_loop();
    void handle_connection(int sock_fd);

    std::string bind_addr_;
    uint16_t port_;
    Aggregator &agg_;
    std::chrono::seconds cache_ttl_;
    std::string auth_expected_header_;

    int listen_sock_;
    std::thread worker_thread_;
    std::atomic<bool> running_;
    std::mutex lifecycle_mu_;

    // cache
    std::mutex cache_mu_;
    std::string cached_body_;
    std::chrono::steady_clock::time_point cached_at_;

    // background refresher (optional)
    std::thread refresher_thread_;
    std::atomic<bool> refresher_running_{false};
};
