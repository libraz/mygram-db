/**
 * @file filter_index.cpp
 * @brief Bitmap-based filter index implementation
 */

#include "storage/filter_index.h"

#include <algorithm>
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

std::vector<std::pair<std::string, uint64_t>> FilterIndex::GetColumnValueCounts(
    const std::string& column) const {
  std::shared_lock lock(mutex_);
  auto col_it = eq_bitmaps_.find(column);
  if (col_it == eq_bitmaps_.end()) {
    return {};
  }

  std::vector<std::pair<std::string, uint64_t>> result;
  result.reserve(col_it->second.size());

  for (const auto& [serialized_value, bitmap] : col_it->second) {
    uint64_t count = roaring_bitmap_get_cardinality(bitmap);
    if (count > 0) {
      result.emplace_back(serialized_value, count);
    }
  }

  // Sort by count descending
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; });

  return result;
}

std::vector<std::pair<std::string, uint64_t>> FilterIndex::GetColumnValueCountsFiltered(
    const std::string& column, const roaring_bitmap_t* filter_bitmap) const {
  if (filter_bitmap == nullptr) {
    return GetColumnValueCounts(column);
  }

  std::shared_lock lock(mutex_);
  auto col_it = eq_bitmaps_.find(column);
  if (col_it == eq_bitmaps_.end()) {
    return {};
  }

  std::vector<std::pair<std::string, uint64_t>> result;
  result.reserve(col_it->second.size());

  for (const auto& [serialized_value, bitmap] : col_it->second) {
    // Use roaring_bitmap_and_cardinality for efficient counting without materializing intersection
    uint64_t count = roaring_bitmap_and_cardinality(bitmap, filter_bitmap);
    if (count > 0) {
      result.emplace_back(serialized_value, count);
    }
  }

  // Sort by count descending
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; });

  return result;
}

std::string FilterIndex::DeserializeToDisplayString(const std::string& serialized) {
  if (serialized.empty()) {
    return "NULL";
  }

  uint8_t tag = static_cast<uint8_t>(serialized[0]);
  const char* data = serialized.data() + 1;
  size_t data_len = serialized.size() - 1;

  switch (tag) {
    case 0x00:  // NULL
      return "NULL";
    case 0x01:  // bool
      return (data_len > 0 && data[0] != '\0') ? "true" : "false";
    case 0x02: {  // int8_t
      if (data_len < sizeof(int8_t)) return "";
      return std::to_string(static_cast<int8_t>(data[0]));
    }
    case 0x03: {  // uint8_t
      if (data_len < sizeof(uint8_t)) return "";
      return std::to_string(static_cast<uint8_t>(data[0]));
    }
    case 0x04: {  // int16_t
      if (data_len < sizeof(int16_t)) return "";
      int16_t val = 0;
      std::memcpy(&val, data, sizeof(int16_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case 0x05: {  // uint16_t
      if (data_len < sizeof(uint16_t)) return "";
      uint16_t val = 0;
      std::memcpy(&val, data, sizeof(uint16_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case 0x06: {  // int32_t
      if (data_len < sizeof(int32_t)) return "";
      int32_t val = 0;
      std::memcpy(&val, data, sizeof(int32_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case 0x07: {  // uint32_t
      if (data_len < sizeof(uint32_t)) return "";
      uint32_t val = 0;
      std::memcpy(&val, data, sizeof(uint32_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case 0x08: {  // int64_t
      if (data_len < sizeof(int64_t)) return "";
      int64_t val = 0;
      std::memcpy(&val, data, sizeof(int64_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case 0x09: {  // uint64_t
      if (data_len < sizeof(uint64_t)) return "";
      uint64_t val = 0;
      std::memcpy(&val, data, sizeof(uint64_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case 0x0A: {  // TimeValue (int64_t seconds)
      if (data_len < sizeof(int64_t)) return "";
      int64_t val = 0;
      std::memcpy(&val, data, sizeof(int64_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case 0x0B:  // string
      return std::string(data, data_len);
    case 0x0C: {  // double
      if (data_len < sizeof(double)) return "";
      double val = 0.0;
      std::memcpy(&val, data, sizeof(double));
      val = mygram::utils::FromLittleEndianDouble(val);
      // Remove trailing zeros for cleaner display
      std::string str = std::to_string(val);
      size_t dot_pos = str.find('.');
      if (dot_pos != std::string::npos) {
        size_t last_nonzero = str.find_last_not_of('0');
        if (last_nonzero == dot_pos) {
          str.erase(dot_pos);  // Remove trailing ".000000"
        } else {
          str.erase(last_nonzero + 1);  // Keep at least one decimal
        }
      }
      return str;
    }
    default:
      return "";
  }
}

}  // namespace mygramdb::storage
