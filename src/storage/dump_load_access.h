/**
 * @file dump_load_access.h
 * @brief Shared atomic table replacement helpers for dump load paths
 */

#pragma once

#include <algorithm>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "index/index.h"
#include "storage/document_store.h"

namespace mygramdb::storage {

struct DumpLoadAccess {
  struct LoadedTableReplacement {
    std::string table_name;
    index::Index* target_index = nullptr;
    index::Index* loaded_index = nullptr;
    DocumentStore* target_doc_store = nullptr;
    DocumentStore* loaded_doc_store = nullptr;
  };

  static void ReplaceLoadedTables(std::vector<LoadedTableReplacement> replacements) {
    std::sort(replacements.begin(), replacements.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.table_name < rhs.table_name; });

    std::vector<std::unique_lock<std::shared_mutex>> target_locks;
    target_locks.reserve(replacements.size() * 2);
    for (const auto& replacement : replacements) {
      target_locks.emplace_back(replacement.target_index->postings_mutex_);
      target_locks.emplace_back(replacement.target_doc_store->mutex_);
    }

    for (const auto& replacement : replacements) {
      replacement.target_index->term_postings_ = std::move(replacement.loaded_index->term_postings_);
      replacement.target_index->load_generation_.fetch_add(1, std::memory_order_acq_rel);

      replacement.target_doc_store->doc_id_to_pk_ = std::move(replacement.loaded_doc_store->doc_id_to_pk_);
      replacement.target_doc_store->pk_to_doc_id_ = std::move(replacement.loaded_doc_store->pk_to_doc_id_);
      replacement.target_doc_store->doc_filters_ = std::move(replacement.loaded_doc_store->doc_filters_);
      replacement.target_doc_store->doc_texts_ = std::move(replacement.loaded_doc_store->doc_texts_);
      replacement.target_doc_store->filter_index_ = std::move(replacement.loaded_doc_store->filter_index_);
      replacement.target_doc_store->next_doc_id_ = replacement.loaded_doc_store->next_doc_id_;
      replacement.target_doc_store->primary_key_doc_id_order_valid_ =
          replacement.loaded_doc_store->primary_key_doc_id_order_valid_;
      replacement.target_doc_store->last_numeric_primary_key_ = replacement.loaded_doc_store->last_numeric_primary_key_;
    }
  }
};

}  // namespace mygramdb::storage
