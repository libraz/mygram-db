/**
 * @file document_store_benchmark_test.cpp
 * @brief Cost of the document-store paths a large candidate set runs through.
 *
 * Verification, scoring and filter resolution all run once per query over a
 * candidate set that can be the whole table. Each case here measures the
 * production path against a straightforward baseline over the same store, so
 * the two figures are comparable on whatever machine the suite runs on.
 * Timings are reported rather than gated; the assertions cover agreement
 * between the paths and the cost shape the production path is chosen for.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "storage/document_store.h"

using mygramdb::DocId;
using mygramdb::storage::DocumentStore;
using mygramdb::storage::FilterMap;

namespace {

template <typename Body>
double TimeMs(Body&& body) {
  const auto start = std::chrono::steady_clock::now();
  body();
  const auto end = std::chrono::steady_clock::now();
  return std::chrono::duration<double, std::milli>(end - start).count();
}

std::string TextForDocument(size_t index) {
  // Wide enough that copying every candidate's text is measurable, and varied
  // so no small-string optimization or shared buffer hides the copy.
  std::string text = "document " + std::to_string(index) + " ";
  text.append(512, static_cast<char>('a' + (index % 26)));
  return text;
}

/// Populate @p store with @p count documents carrying text and filter columns.
std::vector<DocId> Populate(DocumentStore& store, size_t count) {
  std::vector<DocumentStore::DocumentItem> items;
  items.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    FilterMap filters;
    filters.emplace("status", static_cast<int32_t>(i % 3));
    filters.emplace("category", std::string("category") + std::to_string(i % 7));
    filters.emplace("enabled", static_cast<int32_t>(1));
    filters.emplace("author_id", static_cast<int64_t>(i));
    filters.emplace("published_at", static_cast<uint64_t>(i));
    items.push_back({std::to_string(i + 1), std::move(filters), TextForDocument(i), ""});
  }
  auto added = store.AddDocumentBatch(items);
  EXPECT_TRUE(added.has_value());
  return added.has_value() ? added.value() : std::vector<DocId>{};
}

class DocumentStoreBenchmarkTest : public ::testing::Test {};

/**
 * @brief Peak text copied while walking a whole-table candidate set.
 *
 * Retrieving the batch hands back every candidate's text at once, so a query
 * whose candidate set is the table duplicates the stored corpus for the
 * duration of the comparison. Visiting in bounded chunks holds one chunk at a
 * time instead. Peak bytes is the figure that matters here — it is what decides
 * whether a wide query can be served at all — so it is asserted, while the
 * timings are reported alongside it.
 */
TEST_F(DocumentStoreBenchmarkTest, PeakTextCopiedForAWholeTableCandidateSet) {
  constexpr size_t kDocuments = 50000;
  constexpr size_t kChunk = DocumentStore::kSelectedNormalizedTextChunkSize;

  DocumentStore store;
  const std::vector<DocId> candidates = Populate(store, kDocuments);
  ASSERT_EQ(candidates.size(), kDocuments);

  size_t batch_peak_bytes = 0;
  size_t batch_seen = 0;
  const double batch_ms = TimeMs([&] {
    const auto texts = store.GetNormalizedTextBatch(candidates);
    for (const auto& text : texts) {
      if (text.has_value()) {
        batch_peak_bytes += text->size();
        ++batch_seen;
      }
    }
  });

  // The visitor sees documents in the caller's order, so grouping by chunk
  // index reproduces exactly what one lock acquisition materializes.
  size_t visit_peak_bytes = 0;
  size_t visit_current_chunk_bytes = 0;
  size_t visit_seen = 0;
  const double visit_ms = TimeMs([&] {
    store.VisitNormalizedTextsFor(candidates, kChunk, [&](size_t index, DocId, const std::string* text) {
      if (index % kChunk == 0) {
        visit_peak_bytes = std::max(visit_peak_bytes, visit_current_chunk_bytes);
        visit_current_chunk_bytes = 0;
      }
      if (text != nullptr) {
        visit_current_chunk_bytes += text->size();
        ++visit_seen;
      }
      return true;
    });
    visit_peak_bytes = std::max(visit_peak_bytes, visit_current_chunk_bytes);
  });

  std::cout << "\nCandidate text retrieval: " << kDocuments << " candidates, chunk " << kChunk << "\n";
  std::cout << "  " << std::left << std::setw(34) << "peak bytes held at once" << std::right << std::setw(12)
            << batch_peak_bytes << "  ->" << std::setw(12) << visit_peak_bytes << std::endl;
  std::cout << "  " << std::left << std::setw(34) << "wall clock (ms)" << std::right << std::fixed
            << std::setprecision(2) << std::setw(12) << batch_ms << "  ->" << std::setw(12) << visit_ms << std::endl;

  EXPECT_EQ(batch_seen, visit_seen) << "the two retrieval paths returned different numbers of texts";
  EXPECT_EQ(batch_seen, kDocuments) << "the fixture stored no text, so neither path was exercised";
  // Peak is bounded by the chunk, not the candidate count, so the ratio tracks
  // kDocuments / kChunk rather than being a tuned constant.
  EXPECT_LT(visit_peak_bytes * (kDocuments / kChunk / 2), batch_peak_bytes)
      << "chunked visiting held nearly as much text as retrieving the whole batch";
}

/**
 * @brief Filter column resolution must not scale with the stored documents.
 *
 * Resolving a FILTER or ORDER BY column name happens once per query, before any
 * document is examined. If the resolution walks documents, every query pays a
 * table scan just to learn which column was meant. This measures the same
 * resolution against stores an order of magnitude apart in size.
 */
TEST_F(DocumentStoreBenchmarkTest, FilterColumnResolutionIsFlatAcrossStoreSize) {
  constexpr size_t kResolutions = 20000;

  const auto per_resolution_us = [kResolutions](size_t documents) {
    auto store = std::make_unique<DocumentStore>();
    Populate(*store, documents);

    size_t resolved = 0;
    const double ms = TimeMs([&] {
      for (size_t i = 0; i < kResolutions; ++i) {
        // Case-insensitive, so the answer cannot come from a plain hash hit.
        if (store->ResolveFilterColumnName("CaTeGoRy").has_value()) {
          ++resolved;
        }
      }
    });
    EXPECT_EQ(resolved, kResolutions) << "the column stopped resolving at " << documents << " documents";
    return (ms * 1000.0) / static_cast<double>(kResolutions);
  };

  const double small_us = per_resolution_us(10000);
  const double large_us = per_resolution_us(100000);

  std::cout << "\nFilter column resolution: " << kResolutions << " lookups per store size\n";
  std::cout << "  " << std::left << std::setw(34) << "per resolution us (10k -> 100k docs)" << std::right << std::fixed
            << std::setprecision(3) << std::setw(12) << small_us << "  ->" << std::setw(12) << large_us << std::endl;

  EXPECT_LT(large_us, small_us * 3.0)
      << "resolution cost grew with the document count, so a per-document scan is back on the query path";
}

/**
 * @brief Filter storage cost per document, at a size where it decides capacity.
 *
 * Filter columns are configured once and repeat across every row, so their
 * names are held per store rather than per document. This records the resulting
 * per-document cost and requires it to stay flat as the store grows, which is
 * what keeps a wide table's memory proportional to its values.
 */
TEST_F(DocumentStoreBenchmarkTest, FilterStorageCostPerDocumentIsFlat) {
  const auto bytes_per_document = [](size_t documents) {
    auto store = std::make_unique<DocumentStore>();
    store->SetStoreTexts(false);
    Populate(*store, documents);
    return static_cast<double>(store->MemoryUsage()) / static_cast<double>(documents);
  };

  const double small_bytes = bytes_per_document(20000);
  const double large_bytes = bytes_per_document(200000);

  std::cout << "\nFilter storage: 5 filter columns per document, text storage disabled\n";
  std::cout << "  " << std::left << std::setw(34) << "bytes per document (20k -> 200k)" << std::right << std::fixed
            << std::setprecision(1) << std::setw(12) << small_bytes << "  ->" << std::setw(12) << large_bytes
            << std::endl;

  EXPECT_LT(large_bytes, small_bytes * 1.5)
      << "per-document filter cost grew with the store, so column names are being held per document";
}

}  // namespace
