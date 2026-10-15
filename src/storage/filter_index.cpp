/**
 * @file filter_index.cpp
 * @brief Bitmap-based filter index implementation
 */

#include "storage/filter_index.h"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cstring>
#include <exception>
#include <mutex>
#include <new>
#include <utility>
#include <vector>

#include "storage/document_store_internal.h"
#include "utils/datetime_converter.h"
#include "utils/endian_utils.h"
#include "utils/fd_guard.h"
#include "utils/numeric_parse.h"
#include "utils/string_utils.h"

namespace mygramdb::storage {

namespace {

using ::roaring_array_t;
using roaring::internal::array_container_t;
using roaring::internal::bitset_container_t;
using roaring::internal::container_t;

enum class BitmapEditKind : uint8_t { kAdd, kRemove };

struct BitmapEdit {
  std::string column;
  std::string key;
  BitmapEditKind kind = BitmapEditKind::kAdd;
  int32_t container_index = -1;
  bool new_bitmap = false;
  bool placeholder_inserted = false;
  bool bitmap_column_inserted = false;
  mygram::utils::RoaringBitmapPtr staged_bitmap;
  std::unique_ptr<array_container_t, decltype(&roaring::internal::array_container_free)> staged_array{
      nullptr, roaring::internal::array_container_free};
  std::unique_ptr<bitset_container_t, decltype(&roaring::internal::bitset_container_free)> staged_bitset{
      nullptr, roaring::internal::bitset_container_free};
  bool insert_container = false;
  bool erase_container = false;
  bool shrink_bitset = false;
};

struct ColumnDelta {
  std::string column;
  int delta = 0;
  bool count_placeholder_inserted = false;
};

mygram::utils::Expected<void, mygram::utils::Error> StorageWriteFailure(const char* message) {
  return mygram::utils::MakeUnexpected(
      mygram::utils::MakeError(mygram::utils::ErrorCode::kStorageWriteError, message, ""));
}

int32_t FindContainerIndex(const roaring_array_t& array, uint16_t key) {
  return roaring::internal::ra_get_index(&array, key);
}

bool ValidateBitmapStructure(const roaring_bitmap_t* bitmap) {
  if (bitmap == nullptr) {
    return false;
  }
  const roaring_array_t& array = bitmap->high_low_container;
  if (array.flags != 0 || array.size < 0 || array.size > 65536 || array.allocation_size < array.size ||
      array.allocation_size > 65536) {
    return false;
  }
  if (array.allocation_size > 0 &&
      (array.containers == nullptr || array.keys == nullptr || array.typecodes == nullptr)) {
    return false;
  }
  return true;
}

bool ValidateContainerAt(const roaring_array_t& array, int32_t index) {
  if (index < 0 || index >= array.size || array.containers[index] == nullptr) {
    return false;
  }
  switch (array.typecodes[index]) {
    case ARRAY_CONTAINER_TYPE: {
      const auto* container = static_cast<const array_container_t*>(array.containers[index]);
      if (container->cardinality <= 0 || container->cardinality > roaring::internal::DEFAULT_MAX_SIZE ||
          container->capacity < container->cardinality || container->capacity > roaring::internal::DEFAULT_MAX_SIZE ||
          (container->capacity > 0 && container->array == nullptr)) {
        return false;
      }
      return true;
    }
    case BITSET_CONTAINER_TYPE: {
      const auto* container = static_cast<const bitset_container_t*>(array.containers[index]);
      return container->cardinality > 0 && container->cardinality <= (1 << 16) && container->words != nullptr;
    }
    default:
      return false;
  }
}

bool BitmapContains(const roaring_bitmap_t* bitmap, uint32_t value) {
  const roaring_array_t& array = bitmap->high_low_container;
  const int32_t index = FindContainerIndex(array, static_cast<uint16_t>(value >> 16));
  if (index < 0) {
    return false;
  }
  const uint16_t low = static_cast<uint16_t>(value);
  if (array.typecodes[index] == ARRAY_CONTAINER_TYPE) {
    return roaring::internal::array_container_contains(static_cast<const array_container_t*>(array.containers[index]),
                                                       low);
  }
  return roaring::internal::bitset_container_get(static_cast<const bitset_container_t*>(array.containers[index]), low);
}

int32_t ArrayGrowthCapacity(int32_t cardinality) {
  int32_t capacity = std::max<int32_t>(4, cardinality + cardinality / 2);
  return std::min<int32_t>(capacity, roaring::internal::DEFAULT_MAX_SIZE);
}

void CopyArrayWithAddedValue(const array_container_t& source, uint16_t value, array_container_t* destination) {
  const int32_t insertion =
      static_cast<int32_t>(std::lower_bound(source.array, source.array + source.cardinality, value) - source.array);
  std::copy(source.array, source.array + insertion, destination->array);
  destination->array[insertion] = value;
  std::copy(source.array + insertion, source.array + source.cardinality, destination->array + insertion + 1);
  destination->cardinality = source.cardinality + 1;
}

void CopyBitsetToArrayWithoutValue(const bitset_container_t& source, uint16_t value, array_container_t* destination) {
  int32_t position = 0;
  for (uint32_t low = 0; low < (1U << 16); ++low) {
    if (low != value && roaring::internal::bitset_container_get(&source, static_cast<uint16_t>(low))) {
      destination->array[position++] = static_cast<uint16_t>(low);
    }
  }
  destination->cardinality = position;
}

void CopyArrayToBitsetWithAddedValue(const array_container_t& source, uint16_t value, bitset_container_t* destination) {
  for (int32_t index = 0; index < source.cardinality; ++index) {
    roaring::internal::bitset_container_add(destination, source.array[index]);
  }
  roaring::internal::bitset_container_add(destination, value);
}

}  // namespace

Expected<void, Error> FilterIndex::ApplyFilterChangesLocked(DocId doc_id, const FilterMap* old_filters,
                                                            const FilterMap* new_filters) {
  try {
    std::vector<BitmapEdit> edits;
    std::vector<ColumnDelta> columns;
    const auto add_edit = [&edits](std::string_view column, std::string key, BitmapEditKind kind) {
      BitmapEdit edit;
      edit.column.assign(column);
      edit.key = std::move(key);
      edit.kind = kind;
      edits.push_back(std::move(edit));
    };

    if (old_filters != nullptr) {
      for (const auto& [column, old_value] : *old_filters) {
        const FilterValue* new_value = nullptr;
        if (new_filters != nullptr) {
          const auto new_iter = new_filters->find(column);
          if (new_iter != new_filters->end()) {
            new_value = &new_iter->second;
          }
        }
        const bool has_new = new_value != nullptr;
        columns.push_back({column, has_new ? 0 : -1});
        const bool old_is_null = std::holds_alternative<std::monostate>(old_value);
        const bool new_is_null = has_new && std::holds_alternative<std::monostate>(*new_value);
        if (old_is_null && new_is_null) {
          continue;
        }
        std::string old_key;
        if (!old_is_null) {
          old_key = SerializeFilterValue(old_value);
        }
        std::string new_key;
        if (has_new && !new_is_null) {
          new_key = SerializeFilterValue(*new_value);
        }
        if (has_new && old_key == new_key) {
          continue;
        }
        if (!old_is_null) {
          add_edit(column, std::move(old_key), BitmapEditKind::kRemove);
        }
        if (has_new && !new_is_null) {
          add_edit(column, std::move(new_key), BitmapEditKind::kAdd);
        }
      }
    }
    if (new_filters != nullptr) {
      for (const auto& [column, new_value] : *new_filters) {
        if (old_filters != nullptr && old_filters->contains(column)) {
          continue;
        }
        columns.push_back({column, 1});
        if (!std::holds_alternative<std::monostate>(new_value)) {
          add_edit(column, SerializeFilterValue(new_value), BitmapEditKind::kAdd);
        }
      }
    }

    if (edits.empty() && columns.empty()) {
      return {};
    }
    if (edits.empty() &&
        std::all_of(columns.begin(), columns.end(), [](const ColumnDelta& column) { return column.delta == 0; })) {
      return {};
    }

    const uint32_t raw_doc_id = static_cast<uint32_t>(doc_id);
    size_t new_bitmap_entries = 0;
    size_t new_count_columns = 0;
    for (const auto& column : columns) {
      const auto count_iter = column_ref_counts_.find(column.column);
      if (column.delta < 0 && (count_iter == column_ref_counts_.end() || count_iter->second == 0)) {
        return StorageWriteFailure("Filter index column count is inconsistent");
      }
      if (column.delta > 0 && count_iter == column_ref_counts_.end()) {
        ++new_count_columns;
      }
    }

    // Validate every old membership and prepare all new memberships without changing bitmap contents.
    for (auto& edit : edits) {
      const auto column_iter = eq_bitmaps_.find(edit.column);
      const mygram::utils::RoaringBitmapPtr* existing_bitmap = nullptr;
      if (column_iter != eq_bitmaps_.end()) {
        const auto value_iter = column_iter->second.find(edit.key);
        if (value_iter != column_iter->second.end()) {
          existing_bitmap = &value_iter->second;
        }
      }
      const bool has_bitmap = existing_bitmap != nullptr;
      if (has_bitmap && (!*existing_bitmap || !ValidateBitmapStructure(existing_bitmap->get()))) {
        return StorageWriteFailure("Filter index bitmap structure is inconsistent");
      }
      if (edit.kind == BitmapEditKind::kRemove) {
        if (!has_bitmap) {
          return StorageWriteFailure("Filter index removal membership is inconsistent");
        }
      }
      if (has_bitmap) {
        const int32_t index =
            FindContainerIndex(existing_bitmap->get()->high_low_container, static_cast<uint16_t>(raw_doc_id >> 16));
        edit.container_index = index;
        if (index >= 0 && !ValidateContainerAt(existing_bitmap->get()->high_low_container, index)) {
          return StorageWriteFailure("Filter index bitmap container is inconsistent");
        }
        const bool contains = index >= 0 && BitmapContains(existing_bitmap->get(), raw_doc_id);
        if (edit.kind == BitmapEditKind::kRemove && !contains) {
          return StorageWriteFailure("Filter index removal membership is inconsistent");
        }
        if (edit.kind == BitmapEditKind::kAdd && contains) {
          return StorageWriteFailure("Filter index addition membership is inconsistent");
        }
      } else if (edit.kind == BitmapEditKind::kRemove) {
        return StorageWriteFailure("Filter index removal membership is inconsistent");
      } else {
        edit.new_bitmap = true;
        ++new_bitmap_entries;
      }
    }

    // Reserve map storage before inserting temporary entries. No map iterators are retained across reserves.
    eq_bitmaps_.reserve(eq_bitmaps_.size() + new_bitmap_entries);
    column_ref_counts_.reserve(column_ref_counts_.size() + new_count_columns);

    mygram::utils::ScopeGuard rollback_placeholders([&] {
      for (auto& edit : edits) {
        if (!edit.placeholder_inserted) {
          continue;
        }
        auto column_iter = eq_bitmaps_.find(edit.column);
        if (column_iter != eq_bitmaps_.end()) {
          auto value_iter = column_iter->second.find(edit.key);
          if (value_iter != column_iter->second.end() && value_iter->second == nullptr) {
            column_iter->second.erase(value_iter);
          }
        }
      }
      for (auto& column : columns) {
        if (column.count_placeholder_inserted) {
          auto count_iter = column_ref_counts_.find(column.column);
          if (count_iter != column_ref_counts_.end() && count_iter->second == 0) {
            column_ref_counts_.erase(count_iter);
          }
        }
      }
      for (auto& edit : edits) {
        if (edit.bitmap_column_inserted) {
          auto bitmap_iter = eq_bitmaps_.find(edit.column);
          if (bitmap_iter != eq_bitmaps_.end() && bitmap_iter->second.empty()) {
            eq_bitmaps_.erase(bitmap_iter);
          }
        }
      }
    });

    for (auto& column : columns) {
      if (column.delta > 0 && column_ref_counts_.find(column.column) == column_ref_counts_.end()) {
        std::string map_key = column.column;
        column_ref_counts_.try_emplace(std::move(map_key), 0);
        column.count_placeholder_inserted = true;
      }
    }
    for (auto& edit : edits) {
      if (!edit.new_bitmap || eq_bitmaps_.find(edit.column) != eq_bitmaps_.end()) {
        continue;
      }
      std::string map_key = edit.column;
      eq_bitmaps_.try_emplace(std::move(map_key));
      edit.bitmap_column_inserted = true;
    }
    for (const auto& edit : edits) {
      if (edit.kind == BitmapEditKind::kAdd && edit.new_bitmap) {
        auto column_iter = eq_bitmaps_.find(edit.column);
        if (column_iter == eq_bitmaps_.end()) {
          return StorageWriteFailure("Filter index bitmap column allocation failed");
        }
        // FilterMap contains one value per column, so this update adds at most one new key here.
        column_iter->second.reserve(column_iter->second.size() + 1);
      }
    }
    for (auto& edit : edits) {
      if (edit.kind != BitmapEditKind::kAdd || !edit.new_bitmap) {
        continue;
      }
      auto column_iter = eq_bitmaps_.find(edit.column);
      std::string map_key = edit.key;
      auto [value_iter, inserted] = column_iter->second.try_emplace(std::move(map_key), nullptr);
      static_cast<void>(value_iter);
      if (inserted) {
        edit.placeholder_inserted = true;
      }
    }

    // Stage the Roaring allocations and per-container edits. The only existing bitmap storage
    // change during preparation is a capacity reserve for a future high-key insertion.
    for (auto& edit : edits) {
      if (edit.kind != BitmapEditKind::kAdd) {
        continue;
      }
      const uint16_t high = static_cast<uint16_t>(raw_doc_id >> 16);
      const uint16_t low = static_cast<uint16_t>(raw_doc_id);
      if (edit.new_bitmap) {
        edit.staged_bitmap.reset(roaring_bitmap_create_with_capacity(1));
        if (!edit.staged_bitmap) {
          return StorageWriteFailure("Failed to allocate filter bitmap");
        }
        edit.staged_array.reset(roaring::internal::array_container_create_given_capacity(1));
        if (!edit.staged_array) {
          return StorageWriteFailure("Failed to allocate filter bitmap container");
        }
        edit.staged_array->array[0] = low;
        edit.staged_array->cardinality = 1;
        roaring::internal::ra_insert_new_key_value_at(&edit.staged_bitmap->high_low_container, 0, high,
                                                      static_cast<container_t*>(edit.staged_array.get()),
                                                      ARRAY_CONTAINER_TYPE);
        edit.staged_array.release();
        continue;
      }

      auto column_iter = eq_bitmaps_.find(edit.column);
      auto value_iter = column_iter->second.find(edit.key);
      roaring_bitmap_t* bitmap = value_iter->second.get();
      roaring_array_t& array = bitmap->high_low_container;
      const int32_t index = FindContainerIndex(array, high);
      edit.container_index = index;
      if (index < 0) {
        edit.staged_array.reset(roaring::internal::array_container_create_given_capacity(1));
        if (!edit.staged_array) {
          return StorageWriteFailure("Failed to allocate filter bitmap container");
        }
        edit.staged_array->array[0] = low;
        edit.staged_array->cardinality = 1;
        if (!roaring::internal::extend_array(&array, 1)) {
          return StorageWriteFailure("Failed to grow filter bitmap container index");
        }
        edit.insert_container = true;
        continue;
      }

      if (array.typecodes[index] == ARRAY_CONTAINER_TYPE) {
        const auto* source = static_cast<const array_container_t*>(array.containers[index]);
        if (source->cardinality < roaring::internal::DEFAULT_MAX_SIZE) {
          if (source->cardinality >= source->capacity) {
            edit.staged_array.reset(
                roaring::internal::array_container_create_given_capacity(ArrayGrowthCapacity(source->cardinality)));
            if (!edit.staged_array) {
              return StorageWriteFailure("Failed to grow filter bitmap array container");
            }
            CopyArrayWithAddedValue(*source, low, edit.staged_array.get());
          }
        } else {
          edit.staged_bitset.reset(roaring::internal::bitset_container_create());
          if (!edit.staged_bitset) {
            return StorageWriteFailure("Failed to promote filter bitmap to bitset container");
          }
          CopyArrayToBitsetWithAddedValue(*source, low, edit.staged_bitset.get());
        }
      }
    }

    // Bitset-to-array conversion is optional. If it cannot be allocated, commit the removal in-place.
    for (auto& edit : edits) {
      if (edit.kind != BitmapEditKind::kRemove) {
        continue;
      }
      const uint16_t high = static_cast<uint16_t>(raw_doc_id >> 16);
      const uint16_t low = static_cast<uint16_t>(raw_doc_id);
      auto column_iter = eq_bitmaps_.find(edit.column);
      auto value_iter = column_iter->second.find(edit.key);
      roaring_array_t& array = value_iter->second->high_low_container;
      const int32_t index = FindContainerIndex(array, high);
      edit.container_index = index;
      const int32_t cardinality = array.typecodes[index] == ARRAY_CONTAINER_TYPE
                                      ? static_cast<array_container_t*>(array.containers[index])->cardinality
                                      : static_cast<bitset_container_t*>(array.containers[index])->cardinality;
      edit.erase_container = cardinality == 1;
      if (array.typecodes[index] == BITSET_CONTAINER_TYPE && cardinality == roaring::internal::DEFAULT_MAX_SIZE + 1) {
        edit.staged_array.reset(
            roaring::internal::array_container_create_given_capacity(roaring::internal::DEFAULT_MAX_SIZE));
        if (edit.staged_array) {
          CopyBitsetToArrayWithoutValue(*static_cast<const bitset_container_t*>(array.containers[index]), low,
                                        edit.staged_array.get());
          edit.shrink_bitset = true;
        }
      }
    }

    // All required allocations and structural checks succeeded. Commit is allocation-free.
    for (auto& edit : edits) {
      if (edit.kind != BitmapEditKind::kRemove) {
        continue;
      }
      auto column_iter = eq_bitmaps_.find(edit.column);
      auto value_iter = column_iter->second.find(edit.key);
      roaring_bitmap_t* bitmap = value_iter->second.get();
      roaring_array_t& array = bitmap->high_low_container;
      const uint16_t low = static_cast<uint16_t>(raw_doc_id);
      if (edit.erase_container) {
        roaring::internal::ra_remove_at_index_and_free(&array, edit.container_index);
      } else if (edit.shrink_bitset) {
        roaring::internal::container_t* old_container = array.containers[edit.container_index];
        const uint8_t old_type = array.typecodes[edit.container_index];
        array.containers[edit.container_index] = static_cast<container_t*>(edit.staged_array.release());
        array.typecodes[edit.container_index] = ARRAY_CONTAINER_TYPE;
        roaring::internal::container_free(old_container, old_type);
      } else if (array.typecodes[edit.container_index] == ARRAY_CONTAINER_TYPE) {
        roaring::internal::array_container_remove(
            static_cast<array_container_t*>(array.containers[edit.container_index]), low);
      } else {
        roaring::internal::bitset_container_remove(
            static_cast<bitset_container_t*>(array.containers[edit.container_index]), low);
      }
      if (roaring_bitmap_is_empty(bitmap)) {
        column_iter->second.erase(value_iter);
        if (column_iter->second.empty()) {
          eq_bitmaps_.erase(column_iter);
        }
      }
    }
    for (auto& edit : edits) {
      if (edit.kind != BitmapEditKind::kAdd) {
        continue;
      }
      if (edit.new_bitmap) {
        auto column_iter = eq_bitmaps_.find(edit.column);
        auto value_iter = column_iter->second.find(edit.key);
        value_iter->second = std::move(edit.staged_bitmap);
        continue;
      }
      auto column_iter = eq_bitmaps_.find(edit.column);
      auto value_iter = column_iter->second.find(edit.key);
      roaring_array_t& array = value_iter->second->high_low_container;
      const uint16_t high = static_cast<uint16_t>(raw_doc_id >> 16);
      const uint16_t low = static_cast<uint16_t>(raw_doc_id);
      if (edit.insert_container) {
        const int32_t insertion = -edit.container_index - 1;
        roaring::internal::ra_insert_new_key_value_at(
            &array, insertion, high, static_cast<container_t*>(edit.staged_array.release()), ARRAY_CONTAINER_TYPE);
      } else if (edit.staged_array) {
        container_t* old_container = array.containers[edit.container_index];
        array.containers[edit.container_index] = static_cast<container_t*>(edit.staged_array.release());
        roaring::internal::container_free(old_container, ARRAY_CONTAINER_TYPE);
      } else if (edit.staged_bitset) {
        container_t* old_container = array.containers[edit.container_index];
        array.containers[edit.container_index] = static_cast<container_t*>(edit.staged_bitset.release());
        array.typecodes[edit.container_index] = BITSET_CONTAINER_TYPE;
        roaring::internal::container_free(old_container, ARRAY_CONTAINER_TYPE);
      } else if (array.typecodes[edit.container_index] == ARRAY_CONTAINER_TYPE) {
        roaring::internal::array_container_add(static_cast<array_container_t*>(array.containers[edit.container_index]),
                                               low);
      } else {
        roaring::internal::bitset_container_add(
            static_cast<bitset_container_t*>(array.containers[edit.container_index]), low);
      }
    }

    for (const auto& column : columns) {
      auto count_iter = column_ref_counts_.find(column.column);
      if (column.delta > 0) {
        ++count_iter->second;
      } else if (column.delta < 0 && --count_iter->second == 0) {
        column_ref_counts_.erase(count_iter);
      }
    }
    rollback_placeholders.Release();
    return {};
  } catch (const std::bad_alloc&) {
    return StorageWriteFailure("Failed to prepare filter index update");
  }
}

Expected<void, Error> FilterIndex::AddDocument(DocId doc_id, const FilterMap& filters) {
  std::unique_lock lock(mutex_);
  return ApplyFilterChangesLocked(doc_id, nullptr, &filters);
}

Expected<void, Error> FilterIndex::UpdateDocument(DocId doc_id, const FilterMap& old_filters,
                                                  const FilterMap& new_filters) {
  std::unique_lock lock(mutex_);
  return ApplyFilterChangesLocked(doc_id, &old_filters, &new_filters);
}

FilterIndex::SerializedValueProbe FilterIndex::MakeSerializedValueProbe(const FilterValue& value,
                                                                        SerializedValueStorage& storage) {
  return std::visit(
      [&storage](const auto& val) -> SerializedValueProbe {
        using T = std::decay_t<decltype(val)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          return {internal::kTypeIndexMonostate, {}};
        } else if constexpr (std::is_same_v<T, bool>) {
          storage[0] = val ? '\x01' : '\x00';
          return {internal::kTypeIndexBool, {storage.data(), 1}};
        } else if constexpr (std::is_same_v<T, std::string>) {
          return {internal::kTypeIndexString, val};
        } else if constexpr (std::is_same_v<T, double>) {
          const double little_endian = mygram::utils::ToLittleEndianDouble(val);
          std::memcpy(storage.data(), &little_endian, sizeof(little_endian));
          return {internal::kTypeIndexDouble, {storage.data(), sizeof(little_endian)}};
        } else if constexpr (std::is_same_v<T, TimeValue>) {
          const int64_t little_endian = mygram::utils::ToLittleEndian(val.seconds);
          std::memcpy(storage.data(), &little_endian, sizeof(little_endian));
          return {internal::kTypeIndexTimeValue, {storage.data(), sizeof(little_endian)}};
        } else if constexpr (std::is_same_v<T, int8_t>) {
          storage[0] = static_cast<char>(val);
          return {internal::kTypeIndexInt8, {storage.data(), sizeof(val)}};
        } else if constexpr (std::is_same_v<T, uint8_t>) {
          storage[0] = static_cast<char>(val);
          return {internal::kTypeIndexUInt8, {storage.data(), sizeof(val)}};
        } else if constexpr (std::is_same_v<T, int16_t>) {
          const int16_t little_endian = mygram::utils::ToLittleEndian(val);
          std::memcpy(storage.data(), &little_endian, sizeof(little_endian));
          return {internal::kTypeIndexInt16, {storage.data(), sizeof(little_endian)}};
        } else if constexpr (std::is_same_v<T, uint16_t>) {
          const uint16_t little_endian = mygram::utils::ToLittleEndian(val);
          std::memcpy(storage.data(), &little_endian, sizeof(little_endian));
          return {internal::kTypeIndexUInt16, {storage.data(), sizeof(little_endian)}};
        } else if constexpr (std::is_same_v<T, int32_t>) {
          const int32_t little_endian = mygram::utils::ToLittleEndian(val);
          std::memcpy(storage.data(), &little_endian, sizeof(little_endian));
          return {internal::kTypeIndexInt32, {storage.data(), sizeof(little_endian)}};
        } else if constexpr (std::is_same_v<T, uint32_t>) {
          const uint32_t little_endian = mygram::utils::ToLittleEndian(val);
          std::memcpy(storage.data(), &little_endian, sizeof(little_endian));
          return {internal::kTypeIndexUInt32, {storage.data(), sizeof(little_endian)}};
        } else if constexpr (std::is_same_v<T, int64_t>) {
          const int64_t little_endian = mygram::utils::ToLittleEndian(val);
          std::memcpy(storage.data(), &little_endian, sizeof(little_endian));
          return {internal::kTypeIndexInt64, {storage.data(), sizeof(little_endian)}};
        } else if constexpr (std::is_same_v<T, uint64_t>) {
          const uint64_t little_endian = mygram::utils::ToLittleEndian(val);
          std::memcpy(storage.data(), &little_endian, sizeof(little_endian));
          return {internal::kTypeIndexUInt64, {storage.data(), sizeof(little_endian)}};
        }
      },
      value);
}

void FilterIndex::RemoveDocument(DocId doc_id, const FilterMap& filters) {
  std::unique_lock lock(mutex_);

  const auto invariant_failure = [] {
    assert(false && "FilterIndex removal invariant violated");
    std::terminate();
  };
  const uint32_t raw_doc_id = static_cast<uint32_t>(doc_id);

  // Validate every membership and reference count before changing any bitmap.
  for (const auto& [column, value] : filters) {
    const auto count_iter = column_ref_counts_.find(column);
    if (count_iter == column_ref_counts_.end() || count_iter->second == 0) {
      invariant_failure();
    }
    if (std::holds_alternative<std::monostate>(value)) {
      continue;
    }

    auto column_iter = eq_bitmaps_.find(column);
    if (column_iter == eq_bitmaps_.end()) {
      invariant_failure();
    }
    SerializedValueStorage storage{};
    const SerializedValueProbe probe = MakeSerializedValueProbe(value, storage);
    const auto value_iter = column_iter->second.find(probe);
    if (value_iter == column_iter->second.end() || !value_iter->second ||
        !ValidateBitmapStructure(value_iter->second.get())) {
      invariant_failure();
    }
    const int32_t index =
        FindContainerIndex(value_iter->second->high_low_container, static_cast<uint16_t>(raw_doc_id >> 16));
    if (index < 0 || !ValidateContainerAt(value_iter->second->high_low_container, index) ||
        !BitmapContains(value_iter->second.get(), raw_doc_id)) {
      invariant_failure();
    }
  }

  // Commit uses only in-place bitmap operations and map erases. A shrink allocation
  // is optional: if it fails, remove the id from the existing bitset container.
  for (const auto& [column, value] : filters) {
    if (!std::holds_alternative<std::monostate>(value)) {
      auto column_iter = eq_bitmaps_.find(column);
      SerializedValueStorage storage{};
      const SerializedValueProbe probe = MakeSerializedValueProbe(value, storage);
      auto value_iter = column_iter->second.find(probe);
      roaring_bitmap_t* bitmap = value_iter->second.get();
      roaring_array_t& array = bitmap->high_low_container;
      const uint16_t high = static_cast<uint16_t>(raw_doc_id >> 16);
      const uint16_t low = static_cast<uint16_t>(raw_doc_id);
      const int32_t index = FindContainerIndex(array, high);
      const bool is_array = array.typecodes[index] == ARRAY_CONTAINER_TYPE;
      const int32_t cardinality = is_array ? static_cast<array_container_t*>(array.containers[index])->cardinality
                                           : static_cast<bitset_container_t*>(array.containers[index])->cardinality;

      if (cardinality == 1) {
        roaring::internal::ra_remove_at_index_and_free(&array, index);
      } else if (!is_array && cardinality == roaring::internal::DEFAULT_MAX_SIZE + 1) {
        std::unique_ptr<array_container_t, decltype(&roaring::internal::array_container_free)> staged_array(
            roaring::internal::array_container_create_given_capacity(roaring::internal::DEFAULT_MAX_SIZE),
            roaring::internal::array_container_free);
        if (staged_array) {
          CopyBitsetToArrayWithoutValue(*static_cast<const bitset_container_t*>(array.containers[index]), low,
                                        staged_array.get());
          container_t* old_container = array.containers[index];
          array.containers[index] = static_cast<container_t*>(staged_array.release());
          array.typecodes[index] = ARRAY_CONTAINER_TYPE;
          roaring::internal::container_free(old_container, BITSET_CONTAINER_TYPE);
        } else {
          roaring::internal::bitset_container_remove(static_cast<bitset_container_t*>(array.containers[index]), low);
        }
      } else if (is_array) {
        roaring::internal::array_container_remove(static_cast<array_container_t*>(array.containers[index]), low);
      } else {
        roaring::internal::bitset_container_remove(static_cast<bitset_container_t*>(array.containers[index]), low);
      }

      if (roaring_bitmap_is_empty(bitmap)) {
        column_iter->second.erase(value_iter);
        if (column_iter->second.empty()) {
          eq_bitmaps_.erase(column_iter);
        }
      }
    }

    auto count_iter = column_ref_counts_.find(column);
    if (--count_iter->second == 0) {
      column_ref_counts_.erase(count_iter);
    }
  }
}

bool FilterIndex::CollectEqDocIds(std::string_view column, std::string_view serialized_value,
                                  std::vector<DocId>& doc_ids) const {
  doc_ids.clear();
  std::shared_lock lock(mutex_);
  auto column_iter = eq_bitmaps_.find(column);
  if (column_iter == eq_bitmaps_.end()) {
    return false;
  }
  auto value_iter = column_iter->second.find(serialized_value);
  if (value_iter == column_iter->second.end()) {
    return false;
  }
  doc_ids.resize(roaring_bitmap_get_cardinality(value_iter->second.get()));
  roaring_bitmap_to_uint32_array(value_iter->second.get(), doc_ids.data());
  return true;
}

bool FilterIndex::OrEqBitmapInto(std::string_view column, std::string_view serialized_value,
                                 roaring_bitmap_t* destination) const {
  if (destination == nullptr) {
    return false;
  }
  std::shared_lock lock(mutex_);
  auto column_iter = eq_bitmaps_.find(column);
  if (column_iter == eq_bitmaps_.end()) {
    return false;
  }
  auto value_iter = column_iter->second.find(serialized_value);
  if (value_iter == column_iter->second.end()) {
    return false;
  }
  roaring_bitmap_or_inplace(destination, value_iter->second.get());
  return true;
}

bool FilterIndex::HasColumn(std::string_view column) const {
  std::shared_lock lock(mutex_);
  return column_ref_counts_.contains(column);
}

std::optional<std::string> FilterIndex::ResolveColumnName(std::string_view column) const {
  std::shared_lock lock(mutex_);
  auto exact_iter = column_ref_counts_.find(column);
  if (exact_iter != column_ref_counts_.end()) {
    return exact_iter->first;
  }

  std::optional<std::string> resolved;
  for (const auto& [stored_column, count] : column_ref_counts_) {
    (void)count;
    if (stored_column.size() != column.size()) {
      continue;
    }
    const bool matches = mygram::utils::EqualsAsciiCaseInsensitive(stored_column, column);
    if (!matches) {
      continue;
    }
    if (resolved.has_value()) {
      return std::nullopt;
    }
    resolved = stored_column;
  }
  return resolved;
}

void FilterIndex::Clear() {
  std::unique_lock lock(mutex_);
  eq_bitmaps_.clear();
  column_ref_counts_.clear();
}

size_t FilterIndex::MemoryUsage() const {
  std::shared_lock lock(mutex_);
  size_t total = 0;
  for (const auto& [column, value_map] : eq_bitmaps_) {
    total += column.size() + column.capacity();
    for (const auto& [key, bm] : value_map) {
      total += key.size() + key.capacity();
      total += roaring_bitmap_portable_size_in_bytes(bm.get());
    }
  }
  for (const auto& [column, count] : column_ref_counts_) {
    (void)count;
    total += sizeof(decltype(column_ref_counts_)::value_type) + column.capacity();
  }
  return total;
}

std::string FilterIndex::SerializeFilterValue(const FilterValue& value) {
  // Keep the serialized tag and bytes shared with the allocation-free removal probe.
  SerializedValueStorage storage{};
  const SerializedValueProbe probe = MakeSerializedValueProbe(value, storage);
  std::string serialized;
  serialized.reserve(1 + probe.payload.size());
  serialized.push_back(static_cast<char>(probe.tag));
  if (!probe.payload.empty()) {
    serialized.append(probe.payload.data(), probe.payload.size());
  }
  return serialized;
}

std::vector<std::pair<std::string, uint64_t>> FilterIndex::GetColumnValueCounts(const std::string& column) const {
  std::vector<std::pair<std::string, uint64_t>> result;
  {
    std::shared_lock lock(mutex_);
    auto col_it = eq_bitmaps_.find(column);
    if (col_it == eq_bitmaps_.end()) {
      return {};
    }

    result.reserve(col_it->second.size());

    for (const auto& [serialized_value, bitmap] : col_it->second) {
      uint64_t count = roaring_bitmap_get_cardinality(bitmap.get());
      if (count > 0) {
        result.emplace_back(serialized_value, count);
      }
    }
  }  // Lock released

  // Count descending, then serialized value ascending: the hash-map iteration order must not leak into pages
  std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
    return a.second != b.second ? a.second > b.second : a.first < b.first;
  });

  return result;
}

std::vector<std::pair<std::string, uint64_t>> FilterIndex::GetColumnValueCountsFiltered(
    const std::string& column, const roaring_bitmap_t* filter_bitmap) const {
  if (filter_bitmap == nullptr) {
    return GetColumnValueCounts(column);
  }

  // Hold shared lock for the entire function to prevent use-after-free:
  // a concurrent RemoveDocument could free a bitmap between collection and use.
  std::shared_lock lock(mutex_);

  auto col_it = eq_bitmaps_.find(column);
  if (col_it == eq_bitmaps_.end()) {
    return {};
  }

  std::vector<std::pair<std::string, uint64_t>> result;
  result.reserve(col_it->second.size());
  for (const auto& [serialized_value, bitmap] : col_it->second) {
    uint64_t count = roaring_bitmap_and_cardinality(bitmap.get(), filter_bitmap);
    if (count > 0) {
      result.emplace_back(serialized_value, count);
    }
  }

  // Count descending, then serialized value ascending: the hash-map iteration order must not leak into pages
  std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
    return a.second != b.second ? a.second > b.second : a.first < b.first;
  });

  return result;
}

std::string FilterIndex::DeserializeToDisplayString(const std::string& serialized) {
  if (serialized.empty()) {
    return "NULL";
  }

  uint8_t tag = static_cast<uint8_t>(serialized[0]);
  const char* data = serialized.data() + 1;
  size_t data_len = serialized.size() - 1;

  // Type tags must match document_store_internal.h kTypeIndex* constants
  // and SerializeFilterValue() tag bytes above.
  switch (tag) {
    case internal::kTypeIndexMonostate:  // NULL
      return "NULL";
    case internal::kTypeIndexBool:  // bool
      return (data_len > 0 && data[0] != '\0') ? "true" : "false";
    case internal::kTypeIndexInt8: {  // int8_t
      if (data_len < sizeof(int8_t))
        return "";
      return std::to_string(static_cast<int8_t>(data[0]));
    }
    case internal::kTypeIndexUInt8: {  // uint8_t
      if (data_len < sizeof(uint8_t))
        return "";
      return std::to_string(static_cast<uint8_t>(data[0]));
    }
    case internal::kTypeIndexInt16: {  // int16_t
      if (data_len < sizeof(int16_t))
        return "";
      int16_t val = 0;
      std::memcpy(&val, data, sizeof(int16_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case internal::kTypeIndexUInt16: {  // uint16_t
      if (data_len < sizeof(uint16_t))
        return "";
      uint16_t val = 0;
      std::memcpy(&val, data, sizeof(uint16_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case internal::kTypeIndexInt32: {  // int32_t
      if (data_len < sizeof(int32_t))
        return "";
      int32_t val = 0;
      std::memcpy(&val, data, sizeof(int32_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case internal::kTypeIndexUInt32: {  // uint32_t
      if (data_len < sizeof(uint32_t))
        return "";
      uint32_t val = 0;
      std::memcpy(&val, data, sizeof(uint32_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case internal::kTypeIndexInt64: {  // int64_t
      if (data_len < sizeof(int64_t))
        return "";
      int64_t val = 0;
      std::memcpy(&val, data, sizeof(int64_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case internal::kTypeIndexUInt64: {  // uint64_t
      if (data_len < sizeof(uint64_t))
        return "";
      uint64_t val = 0;
      std::memcpy(&val, data, sizeof(uint64_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case internal::kTypeIndexTimeValue: {  // TimeValue (int64_t seconds)
      if (data_len < sizeof(int64_t))
        return "";
      int64_t val = 0;
      std::memcpy(&val, data, sizeof(int64_t));
      return std::to_string(mygram::utils::FromLittleEndian(val));
    }
    case internal::kTypeIndexString:  // string
      return std::string(data, data_len);
    case internal::kTypeIndexDouble: {  // double
      if (data_len < sizeof(double))
        return "";
      double val = 0.0;
      std::memcpy(&val, data, sizeof(double));
      val = mygram::utils::FromLittleEndianDouble(val);
      // Use to_chars for locale-independent formatting
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays)
      char buf[32];
      auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
      if (ec != std::errc())
        return "";
      return std::string(buf, ptr);
    }
    default:
      return "";
  }
}

namespace {

template <typename T>
std::optional<FilterValue> ParseFilterNumeric(std::string_view value) {
  auto parsed = mygram::utils::ParseNumeric<T>(value);
  if (!parsed) {
    return std::nullopt;
  }
  return FilterValue{*parsed};
}

}  // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
std::optional<FilterValue> ConvertFilterValue(std::string_view value, bool is_null, std::string_view filter_type,
                                              const std::string& datetime_timezone) {
  if (is_null) {
    return FilterValue{std::monostate{}};
  }

  if (filter_type == "tinyint") {
    return ParseFilterNumeric<int8_t>(value);
  }
  if (filter_type == "tinyint_unsigned") {
    return ParseFilterNumeric<uint8_t>(value);
  }
  if (filter_type == "smallint") {
    return ParseFilterNumeric<int16_t>(value);
  }
  if (filter_type == "smallint_unsigned") {
    return ParseFilterNumeric<uint16_t>(value);
  }
  if (filter_type == "int" || filter_type == "mediumint") {
    return ParseFilterNumeric<int32_t>(value);
  }
  if (filter_type == "int_unsigned" || filter_type == "mediumint_unsigned") {
    return ParseFilterNumeric<uint32_t>(value);
  }
  if (filter_type == "bigint") {
    return ParseFilterNumeric<int64_t>(value);
  }
  if (filter_type == "bigint_unsigned") {
    return ParseFilterNumeric<uint64_t>(value);
  }
  if (filter_type == "float" || filter_type == "double") {
    // from_chars also reads nan and inf, which query filter literals have always accepted;
    // a stored DOUBLE cannot be either, so ingest is unaffected.
    double parsed = 0.0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    const char* end = value.data() + value.size();
    auto [ptr, ec] = std::from_chars(value.data(), end, parsed);
    if (ec != std::errc() || ptr != end) {
      return std::nullopt;
    }
    return FilterValue{parsed};
  }
  if (filter_type == "string" || filter_type == "varchar" || filter_type == "text") {
    return FilterValue{std::string(value)};
  }
  if (filter_type == "boolean") {
    const std::string normalized = mygram::utils::ToLower(std::string(value));
    if (normalized == "1" || normalized == "true") {
      return FilterValue{true};
    }
    if (normalized == "0" || normalized == "false") {
      return FilterValue{false};
    }
    return std::nullopt;
  }
  if (filter_type == "datetime" || filter_type == "date") {
    auto epoch = mygram::utils::ParseDatetimeValue(std::string(value), datetime_timezone);
    if (!epoch) {
      return std::nullopt;
    }
    // Keep every calendar timestamp in the signed epoch domain. Mixing
    // int64_t before 1970 with uint64_t after it makes lexicographic sort
    // keys use different zero points and breaks paginated ordering.
    return FilterValue{*epoch};
  }
  if (filter_type == "timestamp") {
    // TIMESTAMP is selected in UTC (Connection pins @@session.time_zone) and
    // decoded from binlog as UTC. DATETIME alone uses datetime_timezone.
    auto epoch = mygram::utils::ParseDatetimeValue(std::string(value), "+00:00");
    if (!epoch) {
      return std::nullopt;
    }
    return FilterValue{*epoch};
  }
  if (filter_type == "time") {
    auto seconds = mygram::utils::DateTimeProcessor::TimeToSeconds(std::string(value));
    return seconds ? std::optional<FilterValue>{FilterValue{TimeValue{*seconds}}} : std::nullopt;
  }
  return std::nullopt;
}

}  // namespace mygramdb::storage
