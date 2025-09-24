/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_001, QCSIDM_SRS_002, QCSIDM_SRS_003, QCSIDM_SRS_004, QCSIDM_SRS_005, QCSIDM_SRS_006, QCSIDM_SRS_008, QCSIDM_SRS_011, QCSIDM_SRS_013, QCSIDM_SRS_014, QCSIDM_SRS_015, QCSIDM_SRS_016, QCSIDM_SRS_017, QCSIDM_SRS_019, QCSIDM_SRS_021, QCSIDM_SRS_023, QCSIDM_SRS_024, QCSIDM_SRS_025, QCSIDM_SRS_026, QCSIDM_SRS_027, QCSIDM_SRS_028, QCSIDM_SRS_029, QCSIDM_SRS_030, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_034, QCSIDM_SRS_036, QCSIDM_SRS_037, QCSIDM_SRS_038, QCSIDM_SRS_039, QCSIDM_SRS_040, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_043, QCSIDM_SRS_044, QCSIDM_SRS_045, QCSIDM_SRS_048, QCSIDM_SRS_050, QCSIDM_SRS_051, QCSIDM_SRS_052, QCSIDM_SRS_053, QCSIDM_SRS_054, QCSIDM_SRS_055, QCSIDM_SRS_056, QCSIDM_SRS_057, QCSIDM_SRS_058, QCSIDM_SRS_059, QCSIDM_SRS_061, QCSIDM_SRS_062, QCSIDM_SRS_063, QCSIDM_SRS_064, QCSIDM_SRS_065, QCSIDM_SRS_066, QCSIDM_SRS_067, QCSIDM_SRS_069, QCSIDM_SRS_071, QCSIDM_SRS_072, QCSIDM_SRS_073, QCSIDM_SRS_074, QCSIDM_SRS_075, QCSIDM_SRS_077, QCSIDM_SRS_078, QCSIDM_SRS_080, QCSIDM_SRS_081, QCSIDM_SRS_082, QCSIDM_SRS_083, QCSIDM_SRS_084, QCSIDM_SRS_085, QCSIDM_SRS_086, QCSIDM_SRS_087, QCSIDM_SRS_088, QCSIDM_SRS_089, QCSIDM_SRS_090, QCSIDM_SRS_091, QCSIDM_SRS_093, QCSIDM_SRS_094, QCSIDM_SRS_095, QCSIDM_SRS_096, QCSIDM_SRS_098, QCSIDM_SRS_099, QCSIDM_SRS_101, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_104, QCSIDM_SRS_105, QCSIDM_SRS_106, QCSIDM_SRS_108, QCSIDM_SRS_109, QCSIDM_SRS_111, QCSIDM_SRS_112, QCSIDM_SRS_113, QCSIDM_SRS_114, QCSIDM_SRS_116, QCSIDM_SRS_117, QCSIDM_SRS_119 */
/* ==================================================== */

// src/http_server.cpp
// Lightweight single-threaded HTTP server for telemetry endpoints.
// Provides caching (TTL) and optional Basic Auth protection.
//
// Portable: uses winsock2 on Windows and BSD sockets on POSIX.

#include "http_server.hpp"
#include "util_log.hpp"
#include "aggregator.hpp"

#include <thread>
#include <atomic>
#include <mutex>
#include <string>
#include <sstream>
#include <chrono>
#include <vector>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <cstdlib>

#if defined(_WIN32)
  // Avoid windows defining min/max macros that break std::min/std::max
  #ifndef NOMINMAX
    #define NOMINMAX
  #endif
  #define WIN32_LEAN_AND_MEAN
  #include <winsock2.h>
  #include <ws2tcpip.h>
  using socklen_t = int;
  static const int INVALID_SOCKET_FD = INVALID_SOCKET;
  #define HEADER_EQ(a,b) (_stricmp((a),(b))==0)
#else
  #include <unistd.h>
  #include <sys/socket.h>
  #include <netinet/in.h>
  #include <netdb.h>
  #include <arpa/inet.h>
  using SOCKET = int;
  #define closesocket close
  static const int INVALID_SOCKET_FD = -1;
  #define HEADER_EQ(a,b) (strcasecmp((a),(b))==0)
#endif

// use a unique alias to avoid name collision with C's clock_t
using steady_clock_t = std::chrono::steady_clock;

static inline int clamp_int(int v, int lo, int hi) {
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

static std::string url_decode(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i=0;i<s.size();++i) {
        char c = s[i];
        if (c == '+') out.push_back(' ');
        else if (c == '%' && i+2 < s.size()) {
            std::string hex2 = s.substr(i+1,2);
            char dec = static_cast<char>(std::strtol(hex2.c_str(), nullptr, 16));
            out.push_back(dec);
            i += 2;
        } else out.push_back(c);
    }
    return out;
}

static std::string get_query_param(const std::string &q, const std::string &key) {
    if (q.empty()) return {};
    size_t pos = 0;
    while (pos < q.size()) {
        size_t amp = q.find('&', pos);
        std::string pair = q.substr(pos, (amp==std::string::npos ? std::string::npos : amp-pos));
        size_t eq = pair.find('=');
        if (eq != std::string::npos) {
            std::string k = url_decode(pair.substr(0, eq));
            std::string v = url_decode(pair.substr(eq+1));
            if (k == key) return v;
        } else {
            std::string k = url_decode(pair);
            if (k == key) return {};
        }
        if (amp==std::string::npos) break;
        pos = amp + 1;
    }
    return {};
}

static std::string http_response(int code, const std::string &body, const std::string &ct="text/plain; charset=utf-8") {
    std::ostringstream os;
    os << "HTTP/1.1 " << code << " OK\r\n"
       << "Content-Length: " << body.size() << "\r\n"
       << "Content-Type: " << ct << "\r\n"
       << "Connection: close\r\n"
       << "\r\n"
       << body;
    return os.str();
}

static std::string http_401() {
    std::string body = "401 Unauthorized";
    std::ostringstream os;
    os << "HTTP/1.1 401 Unauthorized\r\n"
       << "Content-Length: " << body.size() << "\r\n"
       << "WWW-Authenticate: Basic realm=\"metrics\"\r\n"
       << "Connection: close\r\n"
       << "Content-Type: text/plain\r\n"
       << "\r\n"
       << body;
    return os.str();
}

// ---------- HttpServer implementation ----------

HttpServer::HttpServer(const std::string &bind_addr,
                       uint16_t port,
                       Aggregator &agg_ref,
                       unsigned cache_ttl_seconds,
                       const std::string &auth_expected)
    : bind_addr_(bind_addr),
      port_(port),
      agg_(agg_ref),
      cache_ttl_(std::chrono::seconds(cache_ttl_seconds)),
      auth_expected_header_(auth_expected),
      listen_sock_(INVALID_SOCKET_FD),
      running_(false),
      refresher_running_(false)
{
#if defined(_WIN32)
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2,2), &wsa) != 0) {
        safe_log("HttpServer: WSAStartup failed");
    }
#endif
    cached_body_.clear();
    cached_at_ = steady_clock_t::time_point{}; // epoch, expired
}

HttpServer::~HttpServer() {
    stop();
#if defined(_WIN32)
    WSACleanup();
#endif
}

bool HttpServer::start() {
    std::lock_guard<std::mutex> lk(lifecycle_mu_);
    if (running_) return true;

    listen_sock_ = static_cast<int>(socket(AF_INET, SOCK_STREAM, 0));
    if (listen_sock_ == INVALID_SOCKET_FD) {
        safe_log("HttpServer: socket() failed");
        return false;
    }

    int opt = 1;
    setsockopt(listen_sock_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char*>(&opt), sizeof(opt));

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    addr.sin_addr.s_addr = (bind_addr_.empty() ? INADDR_ANY : inet_addr(bind_addr_.c_str()));

    if (bind(listen_sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        safe_log("HttpServer: bind() failed");
        closesocket(listen_sock_);
        listen_sock_ = INVALID_SOCKET_FD;
        return false;
    }

    if (listen(listen_sock_, 16) < 0) {
        safe_log("HttpServer: listen() failed");
        closesocket(listen_sock_);
        listen_sock_ = INVALID_SOCKET_FD;
        return false;
    }

    running_ = true;
    worker_thread_ = std::thread([this]{ this->accept_loop(); });

    // optional refresher warm-up thread
    if (cache_ttl_ > std::chrono::seconds(0)) {
        refresher_running_.store(true);
        refresher_thread_ = std::thread([this]{
            while (refresher_running_.load()) {
                std::this_thread::sleep_for(cache_ttl_);
                try { this->rebuild_cache_now(); } catch(...) { /* swallow */ }
            }
        });
    }

    safe_log(std::string("HttpServer started on port ") + std::to_string(port_));
    return true;
}

void HttpServer::stop() {
    {
        std::lock_guard<std::mutex> lk(lifecycle_mu_);
        if (!running_) return;
        running_ = false;
    }

    refresher_running_.store(false);
    if (refresher_thread_.joinable()) refresher_thread_.join();

    if (listen_sock_ != INVALID_SOCKET_FD) {
        closesocket(listen_sock_);
        listen_sock_ = INVALID_SOCKET_FD;
    }

    if (worker_thread_.joinable()) worker_thread_.join();

    safe_log("HttpServer stopped");
}

void HttpServer::accept_loop() {
    while (running_) {
        sockaddr_in client;
        socklen_t clen = sizeof(client);
        int cli_sock = static_cast<int>(accept(listen_sock_, reinterpret_cast<sockaddr*>(&client), &clen));
        if (!running_) break;
        if (cli_sock == INVALID_SOCKET_FD) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }
        std::thread(&HttpServer::handle_connection, this, cli_sock).detach();
    }
}

// read until "\r\n\r\n" or error. returns true if we read something.
static bool recv_request(int sock, std::string &out_req) {
    out_req.clear();
    char buf[4096];
    int total = 0;
    while (true) {
        int r = static_cast<int>(recv(sock, buf, sizeof(buf), 0));
        if (r <= 0) return (r==0 && !out_req.empty());
        out_req.append(buf, buf + r);
        total += r;
        if (out_req.find("\r\n\r\n") != std::string::npos) break;
        if (total > 64*1024) return false;
    }
    return true;
}

void HttpServer::handle_connection(int sock_fd) {
    std::string req;
    bool ok = recv_request(sock_fd, req);
    if (!ok) { closesocket(sock_fd); return; }

    std::istringstream rs(req);
    std::string method, fullpath, proto;
    rs >> method >> fullpath >> proto;

    // parse headers (only Authorization needed)
    std::string line;
    std::string auth_hdr;
    std::getline(rs, line); // finish request line remainder
    while (std::getline(rs, line)) {
        if (line == "\r" || line.empty()) break;
        size_t c = line.find(':');
        if (c != std::string::npos) {
            std::string hn = line.substr(0, c);
            std::string hv = line.substr(c+1);
            // trim leading whitespace
            size_t p = 0; while (p < hv.size() && std::isspace((unsigned char)hv[p])) ++p;
            hv = hv.substr(p);
            if (!hv.empty() && hv.back() == '\r') hv.pop_back();
            if (HEADER_EQ(hn.c_str(), "Authorization")) auth_hdr = hv;
        }
    }

    // Basic Auth if configured
    if (!auth_expected_header_.empty()) {
        if (auth_hdr.empty() || auth_hdr != auth_expected_header_) {
            std::string resp = http_401();
            send(sock_fd, resp.c_str(), static_cast<int>(resp.size()), 0);
            closesocket(sock_fd);
            return;
        }
    }

    if (method != "GET") {
        std::string resp = http_response(405, "Only GET supported\n");
        send(sock_fd, resp.c_str(), static_cast<int>(resp.size()), 0);
        closesocket(sock_fd);
        return;
    }

    // split path & query
    std::string path = fullpath;
    std::string query;
    size_t qpos = fullpath.find('?');
    if (qpos != std::string::npos) {
        path = fullpath.substr(0, qpos);
        query = fullpath.substr(qpos + 1);
    }

    if (path == "/metrics" || path == "/stats") {
        int topk = 10;
        int window = 60;
        std::string s_topk = get_query_param(query, "topk");
        if (!s_topk.empty()) {
            topk = clamp_int(std::atoi(s_topk.c_str()), 1, 1000);
        }
        std::string s_window = get_query_param(query, "window");
        if (!s_window.empty()) {
            window = clamp_int(std::atoi(s_window.c_str()), 1, 3600);
        }

        // attempt to serve cached body
        std::string body;
        bool used_cache = false;
        {
            std::lock_guard<std::mutex> lk(cache_mu_);
            auto now = steady_clock_t::now();
            if (!cached_body_.empty() && (now - cached_at_) < cache_ttl_) {
                body = cached_body_;
                used_cache = true;
            }
        }

        if (!used_cache) {
            try {
                uint64_t total = agg_.get_total();
                uint64_t errs = agg_.get_errors();
                auto status = agg_.snapshot_status_counts();
                auto top_ips = agg_.top_k_ips(static_cast<size_t>(topk));
                auto top_eps = agg_.top_k_endpoints(static_cast<size_t>(topk));
                double rate = agg_.rate_over_seconds(static_cast<size_t>(window));

                std::ostringstream out;
                if (path == "/metrics") {
                    out << "# HELP logpulse_total_lines Total parsed lines\n";
                    out << "logpulse_total_lines " << total << "\n";
                    out << "# HELP logpulse_parse_errors Parse errors\n";
                    out << "logpulse_parse_errors " << errs << "\n";
                    out << "# HELP logpulse_lines_per_second Lines/sec\n";
                    out << "logpulse_lines_per_second " << std::fixed << rate << "\n";
                    out << "# HELP logpulse_status_count Status counts\n";
                    out << "# TYPE logpulse_status_count gauge\n";
                    for (auto &p : status) out << "logpulse_status_count{code=\"" << p.first << "\"} " << p.second << "\n";
                    for (size_t i = 0; i < top_ips.size(); ++i)
                        out << "logpulse_top_ip{rank=\"" << (i+1) << "\",ip=\"" << top_ips[i].first << "\"} " << top_ips[i].second << "\n";
                    for (size_t i = 0; i < top_eps.size(); ++i)
                        out << "logpulse_top_endpoint{rank=\"" << (i+1) << "\",endpoint=\"" << top_eps[i].first << "\"} " << top_eps[i].second << "\n";
                } else {
                    out << "{\n";
                    out << "  \"total_lines\": " << total << ",\n";
                    out << "  \"parse_errors\": " << errs << ",\n";
                    out << "  \"rate_lps\": " << std::fixed << rate << ",\n";
                    out << "  \"status_counts\": {";
                    bool first = true;
                    for (auto &p : status) {
                        if (!first) out << ", ";
                        out << "\"" << p.first << "\":" << p.second;
                        first = false;
                    }
                    out << "},\n";
                    out << "  \"top_ips\": [";
                    for (size_t i=0;i<top_ips.size();++i) {
                        if (i) out << ", ";
                        out << "{\"ip\":\"" << top_ips[i].first << "\",\"count\":" << top_ips[i].second << "}";
                    }
                    out << "],\n";
                    out << "  \"top_endpoints\": [";
                    for (size_t i=0;i<top_eps.size();++i) {
                        if (i) out << ", ";
                        out << "{\"endpoint\":\"" << top_eps[i].first << "\",\"count\":" << top_eps[i].second << "}";
                    }
                    out << "]\n";
                    out << "}\n";
                }

                body = out.str();

                // store in cache
                {
                    std::lock_guard<std::mutex> lk(cache_mu_);
                    cached_body_ = body;
                    cached_at_ = steady_clock_t::now();
                }
            } catch (const std::exception &e) {
                std::string err = std::string("error building metrics: ") + e.what();
                std::string resp = http_response(500, err);
                send(sock_fd, resp.c_str(), static_cast<int>(resp.size()), 0);
                closesocket(sock_fd);
                return;
            } catch (...) {
                std::string err = "error building metrics";
                std::string resp = http_response(500, err);
                send(sock_fd, resp.c_str(), static_cast<int>(resp.size()), 0);
                closesocket(sock_fd);
                return;
            }
        }

        std::string ct = (path == "/metrics") ? "text/plain; version=0.0.4" : "application/json";
        std::string resp = http_response(200, body, ct);
        send(sock_fd, resp.c_str(), static_cast<int>(resp.size()), 0);
    } else {
        std::string resp = http_response(404, "not found\n");
        send(sock_fd, resp.c_str(), static_cast<int>(resp.size()), 0);
    }

    closesocket(sock_fd);
}

// Build & store cached metrics snapshot. Never throws outward.
void HttpServer::rebuild_cache_now() {
    try {
        int topk = 10;
        int window = 60;
        uint64_t total = agg_.get_total();
        uint64_t errs = agg_.get_errors();
        auto status = agg_.snapshot_status_counts();
        auto top_ips = agg_.top_k_ips(static_cast<size_t>(topk));
        auto top_eps = agg_.top_k_endpoints(static_cast<size_t>(topk));
        double rate = agg_.rate_over_seconds(static_cast<size_t>(window));

        std::ostringstream out;
        out << "# HELP logpulse_total_lines Total parsed lines\n";
        out << "logpulse_total_lines " << total << "\n";
        out << "logpulse_parse_errors " << errs << "\n";
        out << "logpulse_lines_per_second " << std::fixed << rate << "\n";
        for (auto &p : status) out << "logpulse_status_count{code=\"" << p.first << "\"} " << p.second << "\n";
        for (size_t i = 0; i < top_ips.size(); ++i)
            out << "logpulse_top_ip{rank=\"" << (i+1) << "\",ip=\"" << top_ips[i].first << "\"} " << top_ips[i].second << "\n";

        std::lock_guard<std::mutex> lk(cache_mu_);
        cached_body_ = out.str();
        cached_at_ = steady_clock_t::now();
    } catch (const std::exception &e) {
        safe_log(std::string("HttpServer::rebuild_cache_now exception: ") + e.what());
        std::lock_guard<std::mutex> lk(cache_mu_);
        cached_body_.clear();
        cached_at_ = steady_clock_t::time_point{};
    } catch (...) {
        safe_log("HttpServer::rebuild_cache_now unknown exception");
        std::lock_guard<std::mutex> lk(cache_mu_);
        cached_body_.clear();
        cached_at_ = steady_clock_t::time_point{};
    }
}
