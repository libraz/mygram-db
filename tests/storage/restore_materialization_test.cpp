/**
 * @file restore_materialization_test.cpp
 * @brief Encoded-to-resident expansion of the restore path.
 *
 * The restore guards decide whether a section can fit before decoding it, so
 * they scale the encoded section length by a fixed materialization factor.
 * These cases measure the real expansion for representative payload shapes and
 * fail if it drifts past what the guards assume. Ratios are asserted, never
 * absolute byte counts.
 *
 * Two ratios are reported per shape. The accounted ratio compares the encoded
 * length against MemoryUsage(), which is the quantity the restore budget is
 * denominated in and therefore the one the guard has to bound. The resident
 * ratio compares it against the process resident-set growth the decode causes,
 * as a check that MemoryUsage() is not far below what the machine actually
 * pays. The payload is built in a child process and read back from a file, so
 * the measuring process reaches the decode with a heap that no earlier
 * allocation has already grown.
 */

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "index/index.h"
#include "storage/document_store.h"
#include "storage/dump_format_internal.h"
#include "utils/memory_utils.h"

namespace {

using mygramdb::storage::DocumentStore;
using mygramdb::storage::FilterMap;

struct Shape {
  std::string name;
  size_t pk_len;
  size_t text_len;
  size_t filter_count;
  size_t doc_count;
};

std::string MakePrimaryKey(uint64_t index, size_t width) {
  std::string key = std::to_string(index);
  if (key.size() < width) {
    key.insert(key.begin(), width - key.size(), '0');
  }
  return key;
}

std::string MakeText(uint64_t index, size_t length) {
  if (length == 0) {
    return {};
  }
  std::string text;
  text.reserve(length);
  while (text.size() < length) {
    text += "doc" + std::to_string(index + text.size()) + " lorem ipsum dolor sit amet ";
  }
  text.resize(length);
  return text;
}

uint64_t CurrentRss() {
  auto info = mygram::utils::GetProcessMemoryInfo();
  return info ? info->rss_bytes : 0;
}

/// Sample the resident set while a decode runs so the transient peak is caught,
/// not only what survives to the end of the load.
class RssPeakSampler {
 public:
  explicit RssPeakSampler(uint64_t baseline) : baseline_(baseline), peak_(baseline) {
    worker_ = std::thread([this] {
      while (!stop_.load(std::memory_order_acquire)) {
        const uint64_t sample = CurrentRss();
        uint64_t observed = peak_.load(std::memory_order_relaxed);
        while (sample > observed && !peak_.compare_exchange_weak(observed, sample)) {
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
      }
    });
  }

  ~RssPeakSampler() {
    stop_.store(true, std::memory_order_release);
    worker_.join();
  }

  RssPeakSampler(const RssPeakSampler&) = delete;
  RssPeakSampler& operator=(const RssPeakSampler&) = delete;
  RssPeakSampler(RssPeakSampler&&) = delete;
  RssPeakSampler& operator=(RssPeakSampler&&) = delete;

  [[nodiscard]] uint64_t PeakDelta() const {
    const uint64_t peak = peak_.load(std::memory_order_relaxed);
    return peak > baseline_ ? peak - baseline_ : 0;
  }

 private:
  uint64_t baseline_;
  std::atomic<uint64_t> peak_;
  std::atomic<bool> stop_{false};
  std::thread worker_;
};

std::string ScratchPath(const std::string& tag) {
  return (std::filesystem::temp_directory_path() /
          ("mygramdb_materialization_" + tag + "_" + std::to_string(::getpid()) + ".bin"))
      .string();
}

/// Run @p build in a forked child so that whatever it allocates never enlarges
/// the measuring process's heap.
bool BuildPayloadInChild(const std::string& path, const std::function<void(std::ostream&)>& build) {
  const pid_t child = ::fork();
  if (child == 0) {
    std::ofstream out(path, std::ios::binary);
    build(out);
    out.flush();
    out.close();
    ::_exit(out.good() ? 0 : 1);
  }
  if (child < 0) {
    return false;
  }
  int status = 0;
  if (::waitpid(child, &status, 0) != child) {
    return false;
  }
  return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

struct Measurement {
  uint64_t encoded_bytes;
  uint64_t accounted_bytes;
  uint64_t resident_bytes;
  double accounted_ratio;
  double resident_ratio;
};

/// The restore budget, on both the write and the read side, is denominated in
/// MemoryUsage(). The guards therefore have to bound that quantity, which is
/// what the accounted ratio measures; the resident figure is reported alongside
/// so that a drift between the accounting and the machine's own view shows up.
constexpr double kMinimumFloorTightness = 0.15;

Measurement MeasureDocumentStore(const Shape& shape) {
  const std::string path = ScratchPath("docstore");
  const bool built = BuildPayloadInChild(path, [&shape](std::ostream& out) {
    DocumentStore source;
    for (size_t i = 0; i < shape.doc_count; ++i) {
      FilterMap filters;
      for (size_t f = 0; f < shape.filter_count; ++f) {
        filters.emplace("col" + std::to_string(f), static_cast<int64_t>(i + f));
      }
      const std::string text = MakeText(i, shape.text_len);
      if (!source.AddDocument(MakePrimaryKey(i, shape.pk_len), filters, text, text)) {
        return;
      }
    }
    (void)source.SaveToStream(out, "gtid:1-1");
  });
  EXPECT_TRUE(built);

  const auto encoded_bytes = static_cast<uint64_t>(std::filesystem::file_size(path));
  auto restored = std::make_unique<DocumentStore>();
  uint64_t accounted = 0;
  uint64_t resident = 0;
  {
    std::ifstream input(path, std::ios::binary);
    const uint64_t baseline = CurrentRss();
    RssPeakSampler sampler(baseline);
    auto loaded = restored->LoadFromStream(input, nullptr);
    EXPECT_TRUE(loaded.has_value());
    accounted = static_cast<uint64_t>(restored->MemoryUsage());
    resident = sampler.PeakDelta();
  }
  EXPECT_EQ(restored->Size(), shape.doc_count);
  std::filesystem::remove(path);

  const auto denominator = static_cast<double>(encoded_bytes);
  return {encoded_bytes, accounted, resident, static_cast<double>(accounted) / denominator,
          static_cast<double>(resident) / denominator};
}

Measurement MeasureIndex(const Shape& shape) {
  const std::string path = ScratchPath("index");
  const bool built = BuildPayloadInChild(path, [&shape](std::ostream& out) {
    mygramdb::index::Index source(2, 1, 100.0, true, true, "keep", true);
    for (size_t i = 0; i < shape.doc_count; ++i) {
      source.AddDocument(static_cast<mygramdb::storage::DocId>(i + 1), MakeText(i, shape.text_len));
    }
    (void)source.SaveToStream(out);
  });
  EXPECT_TRUE(built);

  const auto encoded_bytes = static_cast<uint64_t>(std::filesystem::file_size(path));
  auto restored = std::make_unique<mygramdb::index::Index>(2, 1, 100.0, true, true, "keep", true);
  uint64_t accounted = 0;
  uint64_t resident = 0;
  {
    std::ifstream input(path, std::ios::binary);
    const uint64_t baseline = CurrentRss();
    RssPeakSampler sampler(baseline);
    auto loaded = restored->LoadFromStream(input);
    EXPECT_TRUE(loaded.has_value());
    accounted = static_cast<uint64_t>(restored->MemoryUsage());
    resident = sampler.PeakDelta();
  }
  std::filesystem::remove(path);

  const auto denominator = static_cast<double>(encoded_bytes);
  return {encoded_bytes, accounted, resident, static_cast<double>(accounted) / denominator,
          static_cast<double>(resident) / denominator};
}

/// The document-store guard admits a section only if the smallest size it can
/// possibly decode into still fits the budget, so that figure has to stay a
/// genuine floor: never above what the decode actually costs, and never so far
/// below it that the guard stops refusing anything.
void ExpectFloorBoundsAccountedSize(const Shape& shape, const Measurement& measurement) {
  const uint64_t floor_bytes = mygramdb::storage::dump_internal::EstimateDocumentSectionResidentFloor(
      measurement.encoded_bytes, shape.doc_count);
  const double tightness = static_cast<double>(floor_bytes) / static_cast<double>(measurement.accounted_bytes);
  std::cout << std::left << std::setw(10) << "  floor" << std::setw(18) << shape.name << " bytes=" << std::setw(12)
            << floor_bytes << " tightness=" << std::fixed << std::setprecision(2) << tightness << "\n";
  EXPECT_LE(floor_bytes, measurement.accounted_bytes) << shape.name;
  EXPECT_GT(tightness, kMinimumFloorTightness) << shape.name;
}

void Report(const std::string& subject, const Shape& shape, const Measurement& measurement) {
  std::cout << std::left << std::setw(10) << subject << std::setw(18) << shape.name << " docs=" << std::setw(9)
            << shape.doc_count << " encoded=" << std::setw(12) << measurement.encoded_bytes
            << " accounted=" << std::setw(12) << measurement.accounted_bytes << " resident=" << std::setw(12)
            << measurement.resident_bytes << " accounted_ratio=" << std::fixed << std::setprecision(2)
            << measurement.accounted_ratio << " resident_ratio=" << measurement.resident_ratio << "\n";
}

}  // namespace

// The document counts straddle the open-addressing growth boundary: 262145
// entries sit just past the point where the tables double, so slot slack is
// near its maximum, while 458751 fills the same tables to their load limit.
// The guard has to hold at the wasteful end, not only the tidy one.

TEST(RestoreMaterializationTest, DocumentStoreShortPrimaryKeySparseTable) {
  const Shape shape{"short-pk-sparse", 8, 0, 0, 262145};
  const auto measurement = MeasureDocumentStore(shape);
  Report("docstore", shape, measurement);
  ExpectFloorBoundsAccountedSize(shape, measurement);
}

TEST(RestoreMaterializationTest, DocumentStoreShortPrimaryKeyDenseTable) {
  const Shape shape{"short-pk-dense", 8, 0, 0, 458751};
  const auto measurement = MeasureDocumentStore(shape);
  Report("docstore", shape, measurement);
  ExpectFloorBoundsAccountedSize(shape, measurement);
}

TEST(RestoreMaterializationTest, DocumentStoreLongPrimaryKey) {
  const Shape shape{"long-pk", 200, 0, 0, 262145};
  const auto measurement = MeasureDocumentStore(shape);
  Report("docstore", shape, measurement);
  ExpectFloorBoundsAccountedSize(shape, measurement);
}

TEST(RestoreMaterializationTest, DocumentStoreLongText) {
  const Shape shape{"long-text", 8, 400, 0, 262145};
  const auto measurement = MeasureDocumentStore(shape);
  Report("docstore", shape, measurement);
  ExpectFloorBoundsAccountedSize(shape, measurement);
}

TEST(RestoreMaterializationTest, DocumentStoreManyFilterColumns) {
  const Shape shape{"many-filters", 8, 0, 8, 262145};
  const auto measurement = MeasureDocumentStore(shape);
  Report("docstore", shape, measurement);
  ExpectFloorBoundsAccountedSize(shape, measurement);
}

TEST(RestoreMaterializationTest, IndexShortText) {
  const Shape shape{"short-text", 8, 40, 0, 200000};
  const auto measurement = MeasureIndex(shape);
  Report("index", shape, measurement);
  EXPECT_LT(measurement.accounted_ratio,
            static_cast<double>(mygramdb::storage::dump_internal::kIndexMaterializationFactor))
      << shape.name;
}

TEST(RestoreMaterializationTest, IndexLongText) {
  const Shape shape{"long-text", 8, 400, 0, 200000};
  const auto measurement = MeasureIndex(shape);
  Report("index", shape, measurement);
  EXPECT_LT(measurement.accounted_ratio,
            static_cast<double>(mygramdb::storage::dump_internal::kIndexMaterializationFactor))
      << shape.name;
}
