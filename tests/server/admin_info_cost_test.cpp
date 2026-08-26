/**
 * @file admin_info_cost_test.cpp
 * @brief What a TCP INFO costs, and what it blocks while it runs.
 *
 * INFO reports memory accounting that is derived by walking retained document
 * and index data, so its cost tracks the corpus rather than the request. A
 * monitoring agent polls it on a fixed interval, and the walk holds the
 * document store's shared lock, which excludes the writer that replication
 * apply needs. Both properties are measured here: the cost of repeated polls
 * against corpus size, and whether a writer waiting on the store makes progress
 * while a poll is in flight.
 *
 * Durations are machine-dependent, so the assertions compare repeated polls
 * against the first one rather than against any absolute figure.
 */

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/handlers/admin_handler.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"

namespace mygramdb::server {
namespace {

constexpr size_t kCorpusSize = 200000;
constexpr int kPolls = 20;

template <typename Body>
double TimeMs(Body&& body) {
  const auto start = std::chrono::steady_clock::now();
  body();
  const auto end = std::chrono::steady_clock::now();
  return std::chrono::duration<double, std::milli>(end - start).count();
}

}  // namespace

class AdminInfoCostTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::off);

    config_ = std::make_unique<config::Config>();
    stats_ = std::make_unique<ServerStats>();

    table_ = std::make_unique<TableContext>();
    table_->name = "bench";
    table_->index = std::make_unique<index::Index>(
        /*ngram_size=*/2, /*kanji_ngram_size=*/2,
        /*roaring_threshold=*/0.1, /*cross_boundary_ngrams=*/false,
        /*normalize_nfkc=*/true, /*normalize_width=*/"half", /*normalize_lower=*/true);
    table_->doc_store = std::make_unique<storage::DocumentStore>();
    for (size_t i = 0; i < kCorpusSize; ++i) {
      const std::string text = "record tokyo " + std::to_string(i);
      const std::string normalized = table_->index->NormalizeText(text);
      auto id = table_->doc_store->AddDocument("pk" + std::to_string(i), {}, normalized);
      ASSERT_TRUE(id.has_value());
      table_->index->AddDocument(*id, normalized);
    }

    std::unordered_map<std::string, TableContext*> tables;
    tables.emplace("bench", table_.get());
    catalog_ = std::make_unique<TableCatalog>(std::move(tables));

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = catalog_.get(),
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = "/tmp",
        .dump_load_in_progress = dump_load_in_progress_,
        .dump_save_in_progress = dump_save_in_progress_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
        .mysql_reconnecting = mysql_reconnecting_,
#ifdef USE_MYSQL
        .sync_manager = nullptr,
#endif
        .cache_manager = nullptr,
        .variable_manager = nullptr,
    });
  }

  static query::Query InfoQuery() {
    query::Query query;
    query.type = query::QueryType::INFO;
    return query;
  }

  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
  std::unique_ptr<TableContext> table_;
  std::unique_ptr<TableCatalog> catalog_;
  std::unique_ptr<HandlerContext> handler_ctx_;
  ConnectionContext conn_ctx_;
};

/**
 * @brief Repeated INFO polls must not repeat the memory walk.
 *
 * A monitoring agent polls INFO far more often than the accounting can
 * meaningfully change. The first poll pays for the walk; the ones that follow
 * within the snapshot's lifetime must be cheap.
 */
TEST_F(AdminInfoCostTest, RepeatedPollsDoNotRepeatTheMemoryWalk) {
  AdminHandler handler(*handler_ctx_);
  const auto query = InfoQuery();

  std::string first_response;
  const double first_ms = TimeMs([&] { first_response = handler.Handle(query, conn_ctx_); });
  ASSERT_NE(first_response.find("OK"), std::string::npos) << first_response;

  double subsequent_ms = 0.0;
  for (int i = 0; i < kPolls; ++i) {
    subsequent_ms += TimeMs([&] { (void)handler.Handle(query, conn_ctx_); });
  }
  const double per_poll_ms = subsequent_ms / kPolls;

  std::cout << "\nTCP INFO over " << kCorpusSize << " documents\n";
  std::cout << "  " << std::left << std::setw(28) << "first poll" << std::right << std::setw(10) << std::fixed
            << std::setprecision(3) << first_ms << " ms\n";
  std::cout << "  " << std::left << std::setw(28) << "later polls (mean of " << kPolls << ")" << std::right
            << std::setw(4) << per_poll_ms << " ms\n";
  std::cout << "  " << std::left << std::setw(28) << "total for " << (kPolls + 1) << " polls" << std::right
            << std::setw(4) << (first_ms + subsequent_ms) << " ms\n";

  // A repeated poll must cost a fraction of the walk it would otherwise redo.
  EXPECT_LT(per_poll_ms, first_ms / 4.0);
}

/**
 * @brief A writer must not wait behind an INFO poll.
 *
 * Replication apply takes the document store's exclusive lock. If INFO holds
 * the shared lock for the length of a corpus-sized walk, apply stalls for that
 * whole time, which is the failure this measures.
 */
TEST_F(AdminInfoCostTest, WriterProgressesWhileInfoIsInFlight) {
  AdminHandler handler(*handler_ctx_);
  const auto query = InfoQuery();

  // Prime whatever the first poll has to build so the measured window covers
  // the steady-state cost a poller actually imposes.
  (void)handler.Handle(query, conn_ctx_);

  std::atomic<bool> stop{false};
  std::atomic<size_t> writes{0};
  std::thread writer([&] {
    size_t i = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      const std::string pk = "live" + std::to_string(i++);
      if (table_->doc_store->AddDocument(pk, {}, "tokyo live record").has_value()) {
        writes.fetch_add(1, std::memory_order_relaxed);
      }
    }
  });

  // Let the writer establish a rate without a poller competing.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  const size_t before_idle = writes.load(std::memory_order_relaxed);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  const size_t idle_writes = writes.load(std::memory_order_relaxed) - before_idle;

  const size_t before_polling = writes.load(std::memory_order_relaxed);
  const auto poll_start = std::chrono::steady_clock::now();
  int polls = 0;
  while (std::chrono::steady_clock::now() - poll_start < std::chrono::milliseconds(100)) {
    (void)handler.Handle(query, conn_ctx_);
    ++polls;
  }
  const size_t polling_writes = writes.load(std::memory_order_relaxed) - before_polling;

  stop.store(true, std::memory_order_relaxed);
  writer.join();

  const double retained =
      idle_writes == 0 ? 0.0 : static_cast<double>(polling_writes) / static_cast<double>(idle_writes);
  std::cout << "\nWriter throughput over a 100 ms window\n";
  std::cout << "  " << std::left << std::setw(28) << "no poller" << std::right << std::setw(10) << idle_writes
            << " writes\n";
  std::cout << "  " << std::left << std::setw(28) << "INFO polled continuously" << std::right << std::setw(10)
            << polling_writes << " writes (" << polls << " polls)\n";
  std::cout << "  " << std::left << std::setw(28) << "throughput retained" << std::right << std::setw(9) << std::fixed
            << std::setprecision(2) << (retained * 100.0) << "%\n";

  ASSERT_GT(idle_writes, 0u);
  // A poller may cost the writer some throughput, but it must not take most of
  // it away.
  EXPECT_GT(retained, 0.5);
}

}  // namespace mygramdb::server
