/**
 * @file optimize_memory_estimate_test.cpp
 * @brief Peak memory of a batched optimize against what the estimate promises.
 *
 * OPTIMIZE is admitted by comparing EstimateOptimizationMemory() against the
 * memory the machine has free, so an estimate below the real peak turns a
 * refusal into an OOM kill. These cases run a real batched optimize over a
 * synthetic index at several term-count and batch-size points, sample the
 * resident-set growth it causes, and require the estimate to stay above it.
 * Ratios are asserted, never absolute byte counts.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "index/index.h"
#include "utils/memory_utils.h"

namespace {

using mygram::utils::EstimateOptimizationMemory;

uint64_t CurrentRss() {
  auto info = mygram::utils::GetProcessMemoryInfo();
  return info ? info->rss_bytes : 0;
}

/// Sample the resident set while the batch loop runs, so the transient clone of
/// a batch is caught rather than only what survives the swap.
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

constexpr int kTokenNgramSize = 4;
constexpr size_t kTokenLength = 4;

/// Each token is exactly one n-gram, so the distinct-term count of the index is
/// the size of the token vocabulary and nothing else.
std::string MakeToken(size_t ordinal) {
  static const std::string kAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::string token(kTokenLength, 'a');
  for (size_t i = 0; i < kTokenLength; ++i) {
    token[i] = kAlphabet[ordinal % kAlphabet.size()];
    ordinal /= kAlphabet.size();
  }
  return token;
}

struct Corpus {
  size_t term_count;
  size_t document_count;
  size_t tokens_per_document;
  bool skewed;  ///< Concentrate postings on a few terms instead of spreading them evenly
};

std::unique_ptr<mygramdb::index::Index> BuildIndex(const Corpus& corpus) {
  auto index = std::make_unique<mygramdb::index::Index>(kTokenNgramSize, kTokenNgramSize, 100.0,
                                                        /*cross_boundary_ngrams=*/false, /*normalize_nfkc=*/false,
                                                        "keep", /*normalize_lower=*/false);
  std::vector<std::string> vocabulary;
  vocabulary.reserve(corpus.term_count);
  for (size_t i = 0; i < corpus.term_count; ++i) {
    vocabulary.push_back(MakeToken(i));
  }

  std::string text;
  for (size_t doc = 0; doc < corpus.document_count; ++doc) {
    text.clear();
    for (size_t slot = 0; slot < corpus.tokens_per_document; ++slot) {
      size_t term = 0;
      if (corpus.skewed) {
        // A tenth of the vocabulary carries most of the postings, the shape an
        // n-gram index actually takes on natural text.
        const size_t head = std::max<size_t>(1, corpus.term_count / 10);
        term = slot % 2 == 0 ? (doc + slot) % head : (doc * corpus.tokens_per_document + slot) % corpus.term_count;
      } else {
        term = (doc * corpus.tokens_per_document + slot) % corpus.term_count;
      }
      if (!text.empty()) {
        text.push_back(' ');
      }
      text += vocabulary[term];
    }
    index->AddDocument(static_cast<mygramdb::storage::DocId>(doc + 1), text);
  }
  return index;
}

struct Point {
  uint64_t index_memory;
  uint64_t index_memory_after;
  uint64_t term_count;
  size_t batch_size;
  uint64_t measured_peak;
  uint64_t resident_after;
  uint64_t estimated_peak;
};

Point MeasureOptimize(const Corpus& corpus, size_t batch_size) {
  auto index = BuildIndex(corpus);
  const uint64_t index_memory = index->MemoryUsage();
  const uint64_t term_count = index->GetStatistics().total_terms;
  const uint64_t estimated = EstimateOptimizationMemory(index_memory, batch_size, term_count);

  uint64_t peak = 0;
  uint64_t resident_after = 0;
  const uint64_t baseline = CurrentRss();
  {
    RssPeakSampler sampler(baseline);
    EXPECT_TRUE(index->OptimizeInBatches(corpus.document_count, batch_size));
    peak = sampler.PeakDelta();
  }
  const uint64_t after = CurrentRss();
  resident_after = after > baseline ? after - baseline : 0;
  // The estimate covers the index that is already resident as well as the
  // growth, so compare it against the same total.
  return {index_memory, index->MemoryUsage(), term_count, batch_size, index_memory + peak, resident_after, estimated};
}

void Report(const Corpus& corpus, const Point& point) {
  const auto headroom = static_cast<double>(point.estimated_peak) / static_cast<double>(point.measured_peak);
  std::cout << std::left << (corpus.skewed ? "skewed  " : "uniform ") << " terms=" << std::setw(8) << point.term_count
            << " batch=" << std::setw(7) << point.batch_size << " index=" << std::setw(11) << point.index_memory
            << " index_after=" << std::setw(11) << point.index_memory_after << " rss_peak_delta=" << std::setw(11)
            << (point.measured_peak - point.index_memory) << " rss_after_delta=" << std::setw(11)
            << point.resident_after << " measured_peak=" << std::setw(11) << point.measured_peak
            << " estimated=" << std::setw(11) << point.estimated_peak << " headroom=" << std::fixed
            << std::setprecision(2) << headroom << "\n";
}

/// The admission gate is only meaningful while the estimate sits above the peak
/// a run actually reaches, and only usable while it stays near it: an estimate
/// that drifts far above the peak refuses runs the machine could have served.
constexpr double kMaximumHeadroom = 4.0;

void ExpectEstimateCoversPeak(const Corpus& corpus, const Point& point) {
  Report(corpus, point);
  EXPECT_GE(point.estimated_peak, point.measured_peak) << "terms=" << point.term_count << " batch=" << point.batch_size;
  EXPECT_LT(static_cast<double>(point.estimated_peak) / static_cast<double>(point.measured_peak), kMaximumHeadroom)
      << "terms=" << point.term_count << " batch=" << point.batch_size;
}

const Corpus kUniformCorpus{4000, 60000, 20, false};
const Corpus kSkewedCorpus{4000, 60000, 20, true};

}  // namespace

TEST(OptimizeMemoryEstimateTest, WholeIndexInOneBatch) {
  ExpectEstimateCoversPeak(kUniformCorpus, MeasureOptimize(kUniformCorpus, 4000));
}

TEST(OptimizeMemoryEstimateTest, QuarterOfTheIndexPerBatch) {
  ExpectEstimateCoversPeak(kUniformCorpus, MeasureOptimize(kUniformCorpus, 1000));
}

TEST(OptimizeMemoryEstimateTest, SixteenthOfTheIndexPerBatch) {
  ExpectEstimateCoversPeak(kUniformCorpus, MeasureOptimize(kUniformCorpus, 250));
}

TEST(OptimizeMemoryEstimateTest, SmallBatchOverManyTerms) {
  ExpectEstimateCoversPeak(kUniformCorpus, MeasureOptimize(kUniformCorpus, 50));
}

TEST(OptimizeMemoryEstimateTest, SkewedPostingListsInOneBatch) {
  ExpectEstimateCoversPeak(kSkewedCorpus, MeasureOptimize(kSkewedCorpus, 4000));
}

TEST(OptimizeMemoryEstimateTest, SkewedPostingListsInSmallBatches) {
  ExpectEstimateCoversPeak(kSkewedCorpus, MeasureOptimize(kSkewedCorpus, 250));
}
