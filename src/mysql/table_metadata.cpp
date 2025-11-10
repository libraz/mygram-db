/**
 * @file table_metadata.cpp
 * @brief Table metadata cache implementation
 */

#include "mysql/table_metadata.h"

#ifdef USE_MYSQL

namespace mygramdb {
namespace mysql {

void TableMetadataCache::Add(uint64_t table_id, const TableMetadata& metadata) {
  cache_[table_id] = metadata;
}

const TableMetadata* TableMetadataCache::Get(uint64_t table_id) const {
  auto it = cache_.find(table_id);
  if (it != cache_.end()) {
    return &it->second;
  }
  return nullptr;
}

void TableMetadataCache::Remove(uint64_t table_id) {
  cache_.erase(table_id);
}

void TableMetadataCache::Clear() {
  cache_.clear();
}

}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
