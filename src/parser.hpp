/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_031, QCSIDM_SRS_036, QCSIDM_SRS_040, QCSIDM_SRS_102 */
/* ==================================================== */

#pragma once
#include <string>
#include <optional>
#include "aggregator.hpp"


std::optional<ParsedLine> parse_common_log(const std::string &line);