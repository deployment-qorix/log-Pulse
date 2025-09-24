/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_004, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_036, QCSIDM_SRS_041, QCSIDM_SRS_042, QCSIDM_SRS_070, QCSIDM_SRS_076, QCSIDM_SRS_077, QCSIDM_SRS_093, QCSIDM_SRS_094, QCSIDM_SRS_096, QCSIDM_SRS_097, QCSIDM_SRS_098, QCSIDM_SRS_102, QCSIDM_SRS_103, QCSIDM_SRS_109, QCSIDM_SRS_119 */
/* ==================================================== */

// tests/test_globals.cpp
// Single-definition of globals required by library code when linking tests.

#include <atomic>

// Provide g_terminate used by some headers (defined in main.cpp for the real binary).
std::atomic<bool> g_terminate{false};

// Optional: a checkpoint path global used by CLI code (empty for tests)
#include <string>
std::string g_checkpoint_path;
