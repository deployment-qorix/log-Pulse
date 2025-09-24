/* ==================================================== */
/*  REQ-MAPPING: implements QCSIDM_SRS_004, QCSIDM_SRS_006, QCSIDM_SRS_011, QCSIDM_SRS_012, QCSIDM_SRS_013, QCSIDM_SRS_021, QCSIDM_SRS_022, QCSIDM_SRS_023, QCSIDM_SRS_027, QCSIDM_SRS_030, QCSIDM_SRS_101, QCSIDM_SRS_102 */
/* ==================================================== */

#pragma once
#include "bounded_queue.hpp"
#include "aggregator.hpp"
#include <vector>
#include <thread>


class WorkerPool {
std::vector<std::thread> workers;
BoundedQueue<std::string> &queue;
Aggregator &aggregator;
public:
WorkerPool(size_t n, BoundedQueue<std::string> &q, Aggregator &agg);
~WorkerPool();
};