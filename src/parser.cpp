/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_026, QCSIDM_SRS_031, QCSIDM_SRS_032, QCSIDM_SRS_033, QCSIDM_SRS_036, QCSIDM_SRS_038, QCSIDM_SRS_040, QCSIDM_SRS_051, QCSIDM_SRS_056, QCSIDM_SRS_072, QCSIDM_SRS_102, QCSIDM_SRS_116 */
/* ==================================================== */

#include "parser.hpp"
#include <regex>


std::optional<ParsedLine> parse_common_log(const std::string &line){
static const std::regex r(R"(^([^\s]+)\s+[^\s]+\s+[^\s]+\s+\[[^\]]+\]\s+"(\S+)\s+([^"\s]+)\s+\S+"\s+(\d{3}))");
std::smatch m;
if(std::regex_search(line, m, r)){
ParsedLine p;
p.ip = m[1].str();
p.method = m[2].str();
p.endpoint = m[3].str();
p.status = std::stoi(m[4].str());
p.ts_received = std::time(nullptr);
return p;
}
return std::nullopt;
}