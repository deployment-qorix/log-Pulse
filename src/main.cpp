/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_001, QCSIDM_SRS_002, QCSIDM_SRS_003, QCSIDM_SRS_004, QCSIDM_SRS_005, QCSIDM_SRS_006, QCSIDM_SRS_007, QCSIDM_SRS_008, QCSIDM_SRS_009, QCSIDM_SRS_010, QCSIDM_SRS_011, QCSIDM_SRS_012, QCSIDM_SRS_013, QCSIDM_SRS_014, QCSIDM_SRS_015, QCSIDM_SRS_016, QCSIDM_SRS_017, QCSIDM_SRS_018, QCSIDM_SRS_019, QCSIDM_SRS_020, QCSIDM_SRS_021, QCSIDM_SRS_022, QCSIDM_SRS_023, QCSIDM_SRS_024, QCSIDM_SRS_025, QCSIDM_SRS_026, QCSIDM_SRS_027, QCSIDM_SRS_028, QCSIDM_SRS_029, QCSIDM_SRS_030, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_034, QCSIDM_SRS_036, QCSIDM_SRS_038, QCSIDM_SRS_040, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_044, QCSIDM_SRS_045, QCSIDM_SRS_047, QCSIDM_SRS_048, QCSIDM_SRS_049, QCSIDM_SRS_051, QCSIDM_SRS_052, QCSIDM_SRS_053, QCSIDM_SRS_054, QCSIDM_SRS_055, QCSIDM_SRS_056, QCSIDM_SRS_057, QCSIDM_SRS_059, QCSIDM_SRS_060, QCSIDM_SRS_062, QCSIDM_SRS_064, QCSIDM_SRS_065, QCSIDM_SRS_066, QCSIDM_SRS_067, QCSIDM_SRS_069, QCSIDM_SRS_070, QCSIDM_SRS_071, QCSIDM_SRS_072, QCSIDM_SRS_073, QCSIDM_SRS_075, QCSIDM_SRS_076, QCSIDM_SRS_078, QCSIDM_SRS_081, QCSIDM_SRS_082, QCSIDM_SRS_083, QCSIDM_SRS_084, QCSIDM_SRS_085, QCSIDM_SRS_087, QCSIDM_SRS_088, QCSIDM_SRS_089, QCSIDM_SRS_090, QCSIDM_SRS_091, QCSIDM_SRS_092, QCSIDM_SRS_093, QCSIDM_SRS_094, QCSIDM_SRS_095, QCSIDM_SRS_096, QCSIDM_SRS_097, QCSIDM_SRS_098, QCSIDM_SRS_099, QCSIDM_SRS_100, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_104, QCSIDM_SRS_105, QCSIDM_SRS_106, QCSIDM_SRS_107, QCSIDM_SRS_108, QCSIDM_SRS_109, QCSIDM_SRS_110, QCSIDM_SRS_111, QCSIDM_SRS_112, QCSIDM_SRS_113, QCSIDM_SRS_114, QCSIDM_SRS_115, QCSIDM_SRS_116, QCSIDM_SRS_117, QCSIDM_SRS_119, QCSIDM_SRS_120 */
/* ==================================================== */

// src/main.cpp
// Instrumented startup for LogPulse — defensive, logs each step so we can pinpoint issues.

#include "bounded_queue.hpp"
#include "worker_pool.hpp"
#include "aggregator.hpp"
#include "http_server.hpp"
#include "util_log.hpp"

// CLI is implemented in cli.cpp; declare it so main can call it.
// Do NOT include "cli.cpp" here — that's what caused duplicate symbol errors.
extern void run_cli(Aggregator &agg, std::atomic<bool> &terminate_flag);

#include <iostream>
#include <thread>
#include <csignal>
#include <fstream>
#include <filesystem>
#include <chrono>
#include <exception>
#include <memory>
#include <sstream>
#include <stdexcept>

namespace fs = std::filesystem;

// Global termination flag and checkpoint path (single-definition here)
std::atomic<bool> g_terminate{false};
std::string g_checkpoint_path;

// SIGINT handler
void handle_sigint(int) {
    g_terminate.store(true);
    safe_log("SIGINT received — shutting down.");
}

// helper: base64 (used only if CLI/HTTP basic auth requested)
static std::string base64_encode(const std::string &in) {
    static const char *tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((in.size() + 2) / 3) * 4);
    size_t i = 0;
    while (i + 3 <= in.size()) {
        unsigned a = static_cast<unsigned char>(in[i++]);
        unsigned b = static_cast<unsigned char>(in[i++]);
        unsigned c = static_cast<unsigned char>(in[i++]);
        unsigned x = (a << 16) | (b << 8) | c;
        out.push_back(tbl[(x >> 18) & 0x3F]);
        out.push_back(tbl[(x >> 12) & 0x3F]);
        out.push_back(tbl[(x >> 6) & 0x3F]);
        out.push_back(tbl[x & 0x3F]);
    }
    size_t rem = in.size() - i;
    if (rem == 1) {
        unsigned a = static_cast<unsigned char>(in[i++]);
        unsigned x = a << 16;
        out.push_back(tbl[(x >> 18) & 0x3F]);
        out.push_back(tbl[(x >> 12) & 0x3F]);
        out.push_back('=');
        out.push_back('=');
    } else if (rem == 2) {
        unsigned a = static_cast<unsigned char>(in[i++]);
        unsigned b = static_cast<unsigned char>(in[i++]);
        unsigned x = (a << 16) | (b << 8);
        out.push_back(tbl[(x >> 18) & 0x3F]);
        out.push_back(tbl[(x >> 12) & 0x3F]);
        out.push_back(tbl[(x >> 6) & 0x3F]);
        out.push_back('=');
    }
    return out;
}

// Producer for file with --follow support (tail -f style). Defensive and logs exceptions.
static void producer_read_file_loop(const std::string &path, bool follow, BoundedQueue<std::string> &bq) {
    try {
        std::error_code ec;
        while (!g_terminate.load()) {
            if (!fs::exists(path, ec)) {
                if (!follow) {
                    safe_log(std::string("producer: Failed to open ") + path);
                    return;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                continue;
            }
            break;
        }
        if (g_terminate.load()) return;

        std::ifstream in;
        auto open_file = [&](void)->bool {
            if (in.is_open()) in.close();
            in.open(path, std::ios::in);
            if (!in) return false;
            return true;
        };

        if (!open_file()) {
            safe_log(std::string("producer: Failed to open ") + path);
            return;
        }

        uintmax_t last_size = 0;
        std::error_code ec2;
        if (fs::exists(path, ec2)) last_size = fs::file_size(path, ec2);

        std::string line;
        // drain existing
        while (!g_terminate.load() && std::getline(in, line)) {
            bq.push(std::move(line));
        }

        if (!follow) {
            bq.notify_all();
            return;
        }

        // follow loop
        while (!g_terminate.load()) {
            bool any = false;
            while (!g_terminate.load() && std::getline(in, line)) {
                any = true;
                bq.push(std::move(line));
            }
            if (g_terminate.load()) break;
            if (any) continue;

            uintmax_t cur_size = 0;
            std::error_code ec3;
            if (fs::exists(path, ec3)) cur_size = fs::file_size(path, ec3);
            else cur_size = 0;

            if (cur_size < last_size || !in.good()) {
                // rotated/truncated
                in.close();
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                if (open_file()) {
                    last_size = fs::file_size(path, ec2);
                    while (!g_terminate.load() && std::getline(in, line)) {
                        bq.push(std::move(line));
                    }
                    continue;
                } else {
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    continue;
                }
            }

            last_size = cur_size;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            if (in.eof()) in.clear();
        }

        bq.notify_all();
    } catch (const std::exception &ex) {
        safe_log(std::string("producer_read_file_loop: exception: ") + ex.what());
        g_terminate.store(true);
        bq.notify_all();
    } catch (...) {
        safe_log("producer_read_file_loop: unknown exception");
        g_terminate.store(true);
        bq.notify_all();
    }
}

// Producer for stdin
static void producer_read_stdin_loop(BoundedQueue<std::string> &bq) {
    try {
        std::string line;
        while (!g_terminate.load() && std::getline(std::cin, line)) {
            bq.push(std::move(line));
        }
        bq.notify_all();
    } catch (const std::exception &ex) {
        safe_log(std::string("producer_read_stdin_loop: exception: ") + ex.what());
        g_terminate.store(true);
        bq.notify_all();
    } catch (...) {
        safe_log("producer_read_stdin_loop: unknown exception");
        g_terminate.store(true);
        bq.notify_all();
    }
}

int main(int argc, char** argv) {
    std::signal(SIGINT, handle_sigint);

    // defaults
    std::string file;
    bool follow = false;
    size_t workers = std::thread::hardware_concurrency();
    if (workers == 0) workers = 4;
    size_t qcap = 1<<16;

    bool http_enable = false;
    int http_port = 8080;
    std::string http_user, http_pass;
    unsigned http_cache_ttl = 1;

    size_t ss_ip_cap = 0, ss_ep_cap = 0;
    bool checkpoint_enable = false;

    // parse args (simple)
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--file" && i+1 < argc) file = argv[++i];
        else if (a == "--follow") follow = true;
        else if (a == "--workers" && i+1 < argc) workers = std::stoul(argv[++i]);
        else if (a == "--qcap" && i+1 < argc) qcap = std::stoul(argv[++i]);
        else if (a == "--http-enable") http_enable = true;
        else if (a == "--http-port" && i+1 < argc) http_port = std::stoi(argv[++i]);
        else if (a == "--http-user" && i+1 < argc) http_user = argv[++i];
        else if (a == "--http-pass" && i+1 < argc) http_pass = argv[++i];
        else if (a == "--http-cache-ttl" && i+1 < argc) http_cache_ttl = static_cast<unsigned>(std::stoul(argv[++i]));
        else if (a == "--checkpoint-path" && i+1 < argc) { g_checkpoint_path = argv[++i]; }
        else if (a == "--checkpoint-enable") checkpoint_enable = true;
        else if (a == "--ss-ip-cap" && i+1 < argc) ss_ip_cap = std::stoul(argv[++i]);
        else if (a == "--ss-ep-cap" && i+1 < argc) ss_ep_cap = std::stoul(argv[++i]);
    }

    // startup log
    {
        std::ostringstream os;
        os << "Starting LogPulse; file=" << (file.empty() ? "<stdin>" : file)
           << " follow=" << (follow ? "true" : "false")
           << " workers=" << workers
           << " qcap=" << qcap
           << " http_enable=" << (http_enable ? "true" : "false")
           << " http_port=" << http_port;
        safe_log(os.str());
    }

    BoundedQueue<std::string> bq(qcap);

    // instrumented construction of Aggregator
    std::unique_ptr<Aggregator> agg_ptr;
    try {
        safe_log("STEP: constructing Aggregator");
        agg_ptr = std::make_unique<Aggregator>(
            300,  // window_seconds
            16,   // ip_shards_count
            8,    // endpoint_shards_count
            true, // cms_enable
            1<<14,// cms_width
            4,    // cms_depth
            1024, // mg_k_ips
            1024, // mg_k_endpoints
            ss_ip_cap,
            ss_ep_cap
        );
        safe_log("OK: Aggregator constructed");
    } catch (const std::length_error &le) {
        safe_log(std::string("Aggregator construction length_error: ") + le.what());
        return 1;
    } catch (const std::bad_alloc &ba) {
        safe_log(std::string("Aggregator construction bad_alloc: ") + ba.what());
        return 1;
    } catch (const std::exception &e) {
        safe_log(std::string("Aggregator construction exception: ") + e.what());
        return 1;
    } catch (...) {
        safe_log("Aggregator construction unknown exception");
        return 1;
    }
    Aggregator &agg = *agg_ptr;

    // optional checkpoint loading (guarded)
    if (!g_checkpoint_path.empty() && checkpoint_enable) {
        try {
            safe_log(std::string("Attempting checkpoint load from ") + g_checkpoint_path);
            if (agg.load_checkpoint(g_checkpoint_path)) safe_log(std::string("Loaded checkpoint from ") + g_checkpoint_path);
            else safe_log(std::string("No checkpoint loaded or failed to load: ") + g_checkpoint_path);
        } catch (const std::exception &e) {
            safe_log(std::string("Checkpoint load exception: ") + e.what());
        } catch (...) {
            safe_log("Checkpoint load unknown exception");
        }
    }

    // start producer thread
    std::thread prod;
    try {
        safe_log("STEP: starting producer thread");
        if (file.empty()) {
            prod = std::thread([&bq](){ producer_read_stdin_loop(bq); });
        } else {
            prod = std::thread([&]{ producer_read_file_loop(file, follow, bq); });
        }
        safe_log("OK: producer thread started");
    } catch (const std::exception &e) {
        safe_log(std::string("Producer thread exception: ") + e.what());
        g_terminate.store(true);
    } catch (...) {
        safe_log("Producer thread unknown exception");
        g_terminate.store(true);
    }

    // create worker pool
    std::unique_ptr<WorkerPool> wp_ptr;
    try {
        safe_log("STEP: constructing WorkerPool");
        wp_ptr = std::make_unique<WorkerPool>(workers, bq, agg);
        safe_log("OK: WorkerPool constructed");
    } catch (const std::length_error &le) {
        safe_log(std::string("WorkerPool construction length_error: ") + le.what());
        g_terminate.store(true);
    } catch (const std::bad_alloc &ba) {
        safe_log(std::string("WorkerPool construction bad_alloc: ") + ba.what());
        g_terminate.store(true);
    } catch (const std::exception &e) {
        safe_log(std::string("WorkerPool construction exception: ") + e.what());
        g_terminate.store(true);
    } catch (...) {
        safe_log("WorkerPool construction unknown exception");
        g_terminate.store(true);
    }

    // start HTTP server if requested (guarded)
    std::unique_ptr<HttpServer> http_srv;
    if (http_enable) {
        try {
            safe_log("STEP: creating HttpServer (cache warm skipped)");
            std::string auth_expected;
            if (!http_user.empty() || !http_pass.empty()) {
                auth_expected = std::string("Basic ") + base64_encode(http_user + ":" + http_pass);
            }
            http_srv = std::make_unique<HttpServer>("", static_cast<uint16_t>(http_port), agg, http_cache_ttl, auth_expected);
            if (!http_srv->start()) {
                safe_log("HttpServer failed to start");
                http_srv.reset();
            } else {
                safe_log("OK: HttpServer started");
            }
        } catch (const std::exception &e) {
            safe_log(std::string("HttpServer exception: ") + e.what());
            http_srv.reset();
        } catch (...) {
            safe_log("HttpServer unknown exception");
            http_srv.reset();
        }
    }

    // run CLI (interactive) if reading from file, otherwise block until termination
    try {
        if (!file.empty()) {
            safe_log("STEP: running CLI (interactive)");
            run_cli(agg, g_terminate);
            safe_log("OK: CLI exited");
        } else {
            safe_log("No file specified — running until terminated");
            while (!g_terminate.load()) std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    } catch (const std::exception &e) {
        safe_log(std::string("CLI/loop exception: ") + e.what());
        g_terminate.store(true);
    } catch (...) {
        safe_log("CLI/loop unknown exception");
        g_terminate.store(true);
    }

    // shutdown sequence
    safe_log("Shutdown: setting terminate flag");
    g_terminate.store(true);
    bq.notify_all();

    if (http_srv) {
        try { http_srv->stop(); } catch(...) {}
        http_srv.reset();
    }

    if (prod.joinable()) {
        try { prod.join(); } catch(...) { safe_log("producer join failed"); }
    }

    wp_ptr.reset(); // WorkerPool destructor should join workers

    // optional checkpoint write
    if (!g_checkpoint_path.empty() && checkpoint_enable) {
        try {
            if (agg.dump_checkpoint(g_checkpoint_path)) safe_log(std::string("Checkpoint written to ") + g_checkpoint_path);
            else safe_log(std::string("Failed to write checkpoint to ") + g_checkpoint_path);
        } catch (const std::exception &e) {
            safe_log(std::string("Checkpoint write exception: ") + e.what());
        } catch (...) {
            safe_log("Checkpoint write unknown exception");
        }
    }

    safe_log("LogPulse shutting down normally.");
    return 0;
}
