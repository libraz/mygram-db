/**
 * @file statistics_service.cpp
 * @brief Implementation of StatisticsService
 */

#include "server/statistics_service.h"

namespace mygramdb::server {

void StatisticsService::UpdateServerStatistics(ServerStats& stats, const AggregatedMetrics& metrics) {
  // Update memory usage statistics
  stats.UpdateMemoryUsage(metrics.total_memory);
}

}  // namespace mygramdb::server
