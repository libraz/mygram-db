/**
 * @file operation_names.h
 * @brief String-view constants for blocked-operation names.
 *
 * These constants name the user-facing "operation" string passed to
 * `SyncOperationManager::CheckNoSyncInProgress` and similar conflict checks
 * (DUMP SAVE/LOAD, REPLICATION START, OPTIMIZE, ...). Centralising them
 * keeps the wording consistent across handlers and makes the values easy
 * to grep.
 *
 * The constants intentionally use `std::string_view` so call sites can pass
 * them directly to APIs that accept `std::string_view`. Callers that need a
 * `std::string` (e.g. CheckNoSyncInProgress's current overload) construct
 * one explicitly via `std::string(ops::kSaveDump)`.
 */

#pragma once

#include <string_view>

namespace mygramdb::server::ops {

inline constexpr std::string_view kSaveDump = "save dump";
inline constexpr std::string_view kLoadDump = "load dump";
inline constexpr std::string_view kStartReplication = "start replication";
inline constexpr std::string_view kOptimize = "optimize";

}  // namespace mygramdb::server::ops
