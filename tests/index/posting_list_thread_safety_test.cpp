/**
 * @file posting_list_thread_safety_test.cpp
 * @brief Thread safety tests for PostingList
 *
 * Tests the fix for the critical data race issue identified in the improvement report:
 * - PostingList::Clone() can be called concurrently with GetTopN(), GetAll(), etc.
 * - Roaring bitmap is not thread-safe and requires internal synchronization
 */

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "index/posting_list.h"

namespace mygramdb::index {

class PostingListThreadSafetyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a posting list with Roaring bitmap strategy
    posting_list_ = std::make_unique<PostingList>(0.01);  // Low threshold to force Roaring

    // Add enough documents to trigger Roaring bitmap
    std::vector<DocId> doc_ids;
    for (DocId i = 0; i < 10000; ++i) {
      doc_ids.push_back(i);
    }
    posting_list_->AddBatch(doc_ids);
    posting_list_->Optimize(10000);  // Should convert to Roaring bitmap

    ASSERT_EQ(posting_list_->GetStrategy(), PostingStrategy::kRoaringBitmap);
  }

  std::unique_ptr<PostingList> posting_list_;
};

/**
 * Test concurrent Clone() and GetTopN() operations
 * This tests the specific data race identified in the report
 */
TEST_F(PostingListThreadSafetyTest, ConcurrentCloneAndGetTopN) {
  constexpr int kNumThreads = 8;
  constexpr int kIterations = 1000;
  std::atomic<bool> start{false};
  std::atomic<int> errors{0};

  std::vector<std::thread> threads;

  // Half threads do Clone()
  for (int i = 0; i < kNumThreads / 2; ++i) {
    threads.emplace_back([&, this]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          auto cloned = posting_list_->Clone(10000);
          if (cloned->Size() != 10000) {
            errors.fetch_add(1);
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  // Other half do GetTopN()
  for (int i = 0; i < kNumThreads / 2; ++i) {
    threads.emplace_back([&, this]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          auto docs = posting_list_->GetTopN(100, false);
          if (docs.size() != 100) {
            errors.fetch_add(1);
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  start.store(true);

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(errors.load(), 0) << "Thread safety violation detected";
}

/**
 * Test concurrent Clone() and GetAll() operations
 */
TEST_F(PostingListThreadSafetyTest, ConcurrentCloneAndGetAll) {
  constexpr int kNumThreads = 8;
  constexpr int kIterations = 500;
  std::atomic<bool> start{false};
  std::atomic<int> errors{0};

  std::vector<std::thread> threads;

  // Half threads do Clone()
  for (int i = 0; i < kNumThreads / 2; ++i) {
    threads.emplace_back([&, this]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          auto cloned = posting_list_->Clone(10000);
          if (cloned->Size() != 10000) {
            errors.fetch_add(1);
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  // Other half do GetAll()
  for (int i = 0; i < kNumThreads / 2; ++i) {
    threads.emplace_back([&, this]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          auto docs = posting_list_->GetAll();
          if (docs.size() != 10000) {
            errors.fetch_add(1);
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  start.store(true);

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(errors.load(), 0) << "Thread safety violation detected";
}

/**
 * Test concurrent Clone(), Contains(), and Size() operations
 */
TEST_F(PostingListThreadSafetyTest, ConcurrentCloneAndContains) {
  constexpr int kNumThreads = 12;
  constexpr int kIterations = 1000;
  std::atomic<bool> start{false};
  std::atomic<int> errors{0};

  std::vector<std::thread> threads;

  // Clone threads
  for (int i = 0; i < kNumThreads / 3; ++i) {
    threads.emplace_back([&, this]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          auto cloned = posting_list_->Clone(10000);
          if (cloned->Size() != 10000) {
            errors.fetch_add(1);
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  // Contains threads
  for (int i = 0; i < kNumThreads / 3; ++i) {
    threads.emplace_back([&, this]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          DocId doc_id = j % 10000;
          if (!posting_list_->Contains(doc_id)) {
            errors.fetch_add(1);
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  // Size threads
  for (int i = 0; i < kNumThreads / 3; ++i) {
    threads.emplace_back([&, this]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          if (posting_list_->Size() != 10000) {
            errors.fetch_add(1);
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  start.store(true);

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(errors.load(), 0) << "Thread safety violation detected";
}

/**
 * Test concurrent Intersect() and Union() operations
 */
TEST_F(PostingListThreadSafetyTest, ConcurrentIntersectAndUnion) {
  // Create a second posting list
  auto posting_list2 = std::make_unique<PostingList>(0.01);
  std::vector<DocId> doc_ids2;
  for (DocId i = 5000; i < 15000; ++i) {
    doc_ids2.push_back(i);
  }
  posting_list2->AddBatch(doc_ids2);
  posting_list2->Optimize(15000);

  constexpr int kNumThreads = 8;
  constexpr int kIterations = 500;
  std::atomic<bool> start{false};
  std::atomic<int> errors{0};

  std::vector<std::thread> threads;

  // Intersect threads
  for (int i = 0; i < kNumThreads / 2; ++i) {
    threads.emplace_back([this, &start, &errors, &posting_list2]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          auto result = posting_list_->Intersect(*posting_list2);
          // Expected intersection: [5000, 10000)
          if (result->Size() != 5000) {
            errors.fetch_add(1);
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  // Union threads
  for (int i = 0; i < kNumThreads / 2; ++i) {
    threads.emplace_back([this, &start, &errors, &posting_list2]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          auto result = posting_list_->Union(*posting_list2);
          // Expected union: [0, 15000)
          if (result->Size() != 15000) {
            errors.fetch_add(1);
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  start.store(true);

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(errors.load(), 0) << "Thread safety violation detected";
}

/**
 * Test concurrent reads and writes
 * This is a stress test to ensure mutex protection works correctly
 */
TEST_F(PostingListThreadSafetyTest, ConcurrentReadsAndWrites) {
  constexpr int kNumThreads = 10;
  constexpr int kIterations = 200;
  std::atomic<bool> start{false};
  std::atomic<int> errors{0};

  std::vector<std::thread> threads;

  // Read threads (Clone, GetTopN, GetAll, Contains, Size)
  for (int i = 0; i < kNumThreads * 3 / 4; ++i) {
    threads.emplace_back([&, this, i]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          switch (i % 5) {
            case 0: {
              auto cloned = posting_list_->Clone(20000);
              break;
            }
            case 1: {
              auto docs = posting_list_->GetTopN(50, false);
              break;
            }
            case 2: {
              auto all = posting_list_->GetAll();
              break;
            }
            case 3: {
              (void)posting_list_->Contains(j % 20000);
              break;
            }
            case 4: {
              (void)posting_list_->Size();
              break;
            }
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  // Write threads (Add, Remove)
  for (int i = 0; i < kNumThreads / 4; ++i) {
    threads.emplace_back([this, &start, &errors]() {
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int j = 0; j < kIterations; ++j) {
        try {
          if (j % 2 == 0) {
            posting_list_->Add(10000 + j);
          } else {
            posting_list_->Remove(10000 + j - 1);
          }
        } catch (...) {
          errors.fetch_add(1);
        }
      }
    });
  }

  start.store(true);

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(errors.load(), 0) << "Thread safety violation detected";
}

}  // namespace mygramdb::index
