/**
 * @file filter_index.cpp
 * @brief Bitmap-based filter index implementation
 */

#include "storage/filter_index.h"

#include <cstring>
#include <mutex>

#include "utils/endian_utils.h"

namespace mygramdb::storage {

FilterIndex::~FilterIndex() {
  Clear();
}

void FilterIndex::AddDocToBitmapsLocked(DocId doc_id, const FilterMap& filters) {
  for (const auto& [column, value] : filters) {
    // Skip NULL values (monostate)
    if (std::holds_alternative<std::monostate>(value)) {
      continue;
    }
    std::string key = SerializeFilterValue(value);
    auto& column_map = eq_bitmaps_[column];
    auto it = column_map.find(key);
    if (it == column_map.end()) {
      roaring_bitmap_t* bm = roaring_bitmap_create();
      roaring_bitmap_add(bm, doc_id);
      column_map[std::move(key)] = bm;
    } else {
      roaring_bitmap_add(it->second, doc_id);
    }
  }
}

void FilterIndex::AddDocument(DocId doc_id, const FilterMap& filters) {
  std::unique_lock lock(mutex_);
  AddDocToBitmapsLocked(doc_id, filters);
}

void FilterIndex::RemoveDocFromBitmapsLocked(DocId doc_id, const FilterMap& filters) {
  for (const auto& [column, value] : filters) {
    if (std::holds_alternative<std::monostate>(value)) {
      continue;
    }
    std::string key = SerializeFilterValue(value);
    auto col_it = eq_bitmaps_.find(column);
    if (col_it != eq_bitmaps_.end()) {
      auto val_it = col_it->second.find(key);
      if (val_it != col_it->second.end()) {
        roaring_bitmap_remove(val_it->second, doc_id);
        if (roaring_bitmap_is_empty(val_it->second)) {
          roaring_bitmap_free(val_it->second);
          col_it->second.erase(val_it);
        }
      }
    }
  }
}

void FilterIndex::UpdateDocument(DocId doc_id, const FilterMap& old_filters, const FilterMap& new_filters) {
  std::unique_lock lock(mutex_);
  RemoveDocFromBitmapsLocked(doc_id, old_filters);
  AddDocToBitmapsLocked(doc_id, new_filters);
}

void FilterIndex::RemoveDocument(DocId doc_id, const FilterMap& filters) {
  std::unique_lock lock(mutex_);
  RemoveDocFromBitmapsLocked(doc_id, filters);
}

RoaringBitmapPtr FilterIndex::GetEqBitmap(const std::string& column, const std::string& serialized_value) const {
  std::shared_lock lock(mutex_);
  auto col_it = eq_bitmaps_.find(column);
  if (col_it == eq_bitmaps_.end()) {
    return RoaringBitmapPtr(nullptr, roaring_bitmap_free);
  }
  auto val_it = col_it->second.find(serialized_value);
  if (val_it == col_it->second.end()) {
    return RoaringBitmapPtr(nullptr, roaring_bitmap_free);
  }
  return RoaringBitmapPtr(roaring_bitmap_copy(val_it->second), roaring_bitmap_free);
}

void FilterIndex::Clear() {
  std::unique_lock lock(mutex_);
  for (auto& [column, value_map] : eq_bitmaps_) {
    for (auto& [key, bm] : value_map) {
      roaring_bitmap_free(bm);
    }
  }
  eq_bitmaps_.clear();
}

size_t FilterIndex::MemoryUsage() const {
  std::shared_lock lock(mutex_);
  size_t total = 0;
  for (const auto& [column, value_map] : eq_bitmaps_) {
    total += column.size() + column.capacity();
    for (const auto& [key, bm] : value_map) {
      total += key.size() + key.capacity();
      total += roaring_bitmap_portable_size_in_bytes(bm);
    }
  }
  return total;
}

std::string FilterIndex::SerializeFilterValue(const FilterValue& value) {
  return std::visit(
      [](const auto& val) -> std::string {
        using T = std::decay_t<decltype(val)>;

        if constexpr (std::is_same_v<T, std::monostate>) {
          return {'\x00'};  // NULL tag
        } else if constexpr (std::is_same_v<T, bool>) {
          std::string result(2, '\0');
          result[0] = '\x01';  // bool tag
          result[1] = val ? '\x01' : '\x00';
          return result;
        } else if constexpr (std::is_same_v<T, std::string>) {
          std::string result;
          result.reserve(1 + val.size());
          result += '\x0B';  // string tag
          result += val;
          return result;
        } else if constexpr (std::is_same_v<T, double>) {
          std::string result(1 + sizeof(double), '\0');
          result[0] = '\x0C';  // double tag
          double le_val = mygram::utils::ToLittleEndianDouble(val);
          std::memcpy(&result[1], &le_val, sizeof(double));
          return result;
        } else if constexpr (std::is_same_v<T, TimeValue>) {
          std::string result(1 + sizeof(int64_t), '\0');
          result[0] = '\x0A';  // TimeValue tag
          int64_t le_val = mygram::utils::ToLittleEndian(val.seconds);
          std::memcpy(&result[1], &le_val, sizeof(int64_t));
          return result;
        } else if constexpr (std::is_same_v<T, int8_t>) {
          std::string result(1 + sizeof(int8_t), '\0');
          result[0] = '\x02';
          result[1] = static_cast<char>(val);
          return result;
        } else if constexpr (std::is_same_v<T, uint8_t>) {
          std::string result(1 + sizeof(uint8_t), '\0');
          result[0] = '\x03';
          result[1] = static_cast<char>(val);
          return result;
        } else if constexpr (std::is_same_v<T, int16_t>) {
          std::string result(1 + sizeof(int16_t), '\0');
          result[0] = '\x04';
          int16_t le_val = mygram::utils::ToLittleEndian(val);
          std::memcpy(&result[1], &le_val, sizeof(int16_t));
          return result;
        } else if constexpr (std::is_same_v<T, uint16_t>) {
          std::string result(1 + sizeof(uint16_t), '\0');
          result[0] = '\x05';
          uint16_t le_val = mygram::utils::ToLittleEndian(val);
          std::memcpy(&result[1], &le_val, sizeof(uint16_t));
          return result;
        } else if constexpr (std::is_same_v<T, int32_t>) {
          std::string result(1 + sizeof(int32_t), '\0');
          result[0] = '\x06';
          int32_t le_val = mygram::utils::ToLittleEndian(val);
          std::memcpy(&result[1], &le_val, sizeof(int32_t));
          return result;
        } else if constexpr (std::is_same_v<T, uint32_t>) {
          std::string result(1 + sizeof(uint32_t), '\0');
          result[0] = '\x07';
          uint32_t le_val = mygram::utils::ToLittleEndian(val);
          std::memcpy(&result[1], &le_val, sizeof(uint32_t));
          return result;
        } else if constexpr (std::is_same_v<T, int64_t>) {
          std::string result(1 + sizeof(int64_t), '\0');
          result[0] = '\x08';
          int64_t le_val = mygram::utils::ToLittleEndian(val);
          std::memcpy(&result[1], &le_val, sizeof(int64_t));
          return result;
        } else if constexpr (std::is_same_v<T, uint64_t>) {
          std::string result(1 + sizeof(uint64_t), '\0');
          result[0] = '\x09';
          uint64_t le_val = mygram::utils::ToLittleEndian(val);
          std::memcpy(&result[1], &le_val, sizeof(uint64_t));
          return result;
        }
      },
      value);
}

}  // namespace mygramdb::storage
