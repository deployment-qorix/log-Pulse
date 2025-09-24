/* ==================================================== */
/* REQ-MAPPING: implements QCSIDM_SRS_120 */
/* ==================================================== */

#include "util_log.hpp"
#include <mutex>
#include <fstream>
#include <iostream>

static std::mutex g_log_mu_internal;

void safe_log(const std::string &s) {
    std::lock_guard<std::mutex> lk(g_log_mu_internal);
    // write to stderr so console shows messages too
    std::cerr << s << std::endl;
    // append to a rotating/append-only file
    std::ofstream f("logpulse.err.log", std::ios::app);
    if (f) f << s << std::endl;
}
