/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_001, QCSIDM_SRS_002, QCSIDM_SRS_003, QCSIDM_SRS_006, QCSIDM_SRS_008, QCSIDM_SRS_021, QCSIDM_SRS_023, QCSIDM_SRS_024, QCSIDM_SRS_026, QCSIDM_SRS_029, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_034, QCSIDM_SRS_036, QCSIDM_SRS_039, QCSIDM_SRS_040, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_047, QCSIDM_SRS_051, QCSIDM_SRS_053, QCSIDM_SRS_055, QCSIDM_SRS_059, QCSIDM_SRS_062, QCSIDM_SRS_065, QCSIDM_SRS_067, QCSIDM_SRS_071, QCSIDM_SRS_072, QCSIDM_SRS_073, QCSIDM_SRS_074, QCSIDM_SRS_080, QCSIDM_SRS_081, QCSIDM_SRS_082, QCSIDM_SRS_084, QCSIDM_SRS_085, QCSIDM_SRS_087, QCSIDM_SRS_088, QCSIDM_SRS_089, QCSIDM_SRS_090, QCSIDM_SRS_091, QCSIDM_SRS_092, QCSIDM_SRS_093, QCSIDM_SRS_094, QCSIDM_SRS_096, QCSIDM_SRS_099, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_105, QCSIDM_SRS_106, QCSIDM_SRS_108, QCSIDM_SRS_110, QCSIDM_SRS_119 */
/* ==================================================== */

#include "aggregator.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <atomic>
#include <string>

// global checkpoint path set in main.cpp (extern)
extern std::string g_checkpoint_path;

// CLI loop function
void run_cli(Aggregator &agg, std::atomic<bool> &terminate_flag) {
    std::string cmd;
    std::cout << "LogPulse CLI ready. Commands: STATS | TOP ips K | TOP endpoints K | TOPSS ips K | TOPSS endpoints K | RATE N | APPROX ip X | APPROX_TOP ips K | APPROX_TOP endpoints K | CHECKPOINT [path] | QUIT\n> " << std::flush;

    while (!terminate_flag.load() && std::getline(std::cin, cmd)) {
        if (cmd.empty()) {
            std::cout << "> " << std::flush;
            continue;
        }

        std::stringstream ss(cmd);
        std::string tok;
        ss >> tok;

        if (tok == "STATS") {
            std::cout << "total_lines: " << agg.get_total()
                      << "  parse_errors: " << agg.get_errors() << "\n";
            auto s = agg.snapshot_status_counts();
            std::cout << "status_counts: ";
            for (auto &p : s) std::cout << p.first << ":" << p.second << " ";
            std::cout << "\n";
        }
        else if (tok == "TOP") {
            std::string what;
            ss >> what;
            if (what == "ips") {
                size_t K = 10;
                ss >> K;
                auto t = agg.top_k_ips(K);
                std::cout << "TOP " << K << " IPs:\n";
                for (auto &p : t) std::cout << p.first << " " << p.second << "\n";
            } else if (what == "endpoints") {
                size_t K = 10;
                ss >> K;
                auto t = agg.top_k_endpoints(K);
                std::cout << "TOP " << K << " endpoints:\n";
                for (auto &p : t) std::cout << p.first << " " << p.second << "\n";
            } else {
                std::cout << "Unknown TOP target. Supported: ips, endpoints\n";
            }
        }
        else if (tok == "TOPSS") {
            std::string what;
            ss >> what;
            if (what == "ips") {
                size_t K = 10;
                ss >> K;
                auto t = agg.top_k_ips_ss(K);
                std::cout << "TOPSS " << K << " IPs (SpaceSaving):\n";
                for (auto &p : t) std::cout << p.first << " ~" << p.second << "\n";
            } else if (what == "endpoints") {
                size_t K = 10;
                ss >> K;
                auto t = agg.top_k_endpoints_ss(K);
                std::cout << "TOPSS " << K << " endpoints (SpaceSaving):\n";
                for (auto &p : t) std::cout << p.first << " ~" << p.second << "\n";
            } else {
                std::cout << "Unknown TOPSS target. Supported: ips, endpoints\n";
            }
        }
        else if (tok == "RATE") {
            size_t N = 60;
            ss >> N;
            double r = agg.rate_over_seconds(N);
            std::cout << std::fixed << std::setprecision(2)
                      << "rate (lines/sec over last " << N << "s): " << r << "\n";
        }
        else if (tok == "APPROX") {
            std::string what;
            ss >> what;
            if (what == "ip") {
                std::string ip;
                if (ss >> ip) {
                    uint64_t est = agg.approx_count_ip(ip);
                    if (est == 0) {
                        std::cout << "CMS disabled or zero estimate for " << ip << "\n";
                    } else {
                        std::cout << "approx_count(" << ip << ") ≈ " << est << "\n";
                    }
                } else {
                    std::cout << "Usage: APPROX ip <ip-address>\n";
                }
            } else {
                std::cout << "Unknown APPROX target. Supported: ip\n";
            }
        }
        else if (tok == "APPROX_TOP") {
            std::string what;
            ss >> what;
            if(what == "ips"){
                size_t K = 10;
                ss >> K;
                auto v = agg.approx_top_k_ips(K);
                std::cout << "APPROX TOP " << K << " IPs (MG):\n";
                for(auto &p : v) std::cout << p.first << " " << p.second << "\n";
            } else if(what == "endpoints"){
                size_t K = 10;
                ss >> K;
                auto v = agg.approx_top_k_endpoints(K);
                std::cout << "APPROX TOP " << K << " endpoints (MG):\n";
                for(auto &p : v) std::cout << p.first << " " << p.second << "\n";
            } else {
                std::cout << "Usage: APPROX_TOP ips|endpoints [K]\n";
            }
        }
        else if (tok == "CHECKPOINT") {
            std::string path;
            if (ss >> path) {
                bool ok = agg.dump_checkpoint(path);
                if (ok) std::cout << "Checkpoint written to " << path << "\n";
                else std::cout << "Checkpoint failed for " << path << "\n";
            } else {
                if (!g_checkpoint_path.empty()) {
                    bool ok = agg.dump_checkpoint(g_checkpoint_path);
                    if (ok) std::cout << "Checkpoint written to " << g_checkpoint_path << "\n";
                    else std::cout << "Checkpoint failed for " << g_checkpoint_path << "\n";
                } else {
                    std::cout << "No checkpoint path configured. Use: CHECKPOINT <path> or start with --checkpoint-path.\n";
                }
            }
        }
        else if (tok == "QUIT" || tok == "EXIT") {
            terminate_flag.store(true);
            break;
        }
        else {
            std::cout << "Unknown command\n";
        }

        std::cout << "> " << std::flush;
    }

    terminate_flag.store(true);
}
