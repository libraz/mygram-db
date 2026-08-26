/**
 * @file dump_source_identity.h
 * @brief What a dump records about its MySQL source, and the rule every restore applies
 *
 * Startup restore and DUMP LOAD both decide whether a dump may be applied to
 * the running server. The source-identity part of that decision is expressed
 * here once, so the two paths cannot drift apart.
 */

#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace mygramdb::storage {

/**
 * @brief What a dump records about the MySQL server it was taken from
 *
 * Not every artifact has a place to record it: the V1 container has no field
 * for the UUID, and compatibility metadata version 1 predates it. Such a dump
 * leaves @ref recorded false, which means "unknown" — a different condition
 * from a dump that recorded an empty UUID because it was taken with no binlog
 * reader attached.
 */
struct DumpSourceIdentity {
  /// Whether the artifact carries the field at all.
  bool recorded = false;
  /// Value read from the artifact. Meaningful only when @ref recorded is true.
  std::string uuid;
};

/**
 * @brief Why a dump's recorded source is incompatible with the running server
 *
 * @see FindDumpSourceIdentityMismatch
 */
struct DumpSourceIdentityMismatch {
  /// Stable identifier for a structured log's "reason" field.
  std::string_view reason;
  /// Detail returned to the caller and surfaced in the error message.
  std::string_view detail;
};

/**
 * @brief Decide whether a dump may be restored onto the running MySQL source
 *
 * An unknown source is not a mismatched one. A dump written before the UUID
 * was recorded carries no claim about where it came from, so there is nothing
 * to disagree with and the dump is accepted; the host, port, database and GTID
 * checks the callers apply are unaffected. The same holds when the running
 * server's own UUID is unknown, which is the case with no binlog reader
 * attached.
 *
 * @param dump_source What the dump recorded
 * @param live_source_server_uuid UUID of the MySQL server the process follows,
 *                                empty when unknown
 * @return The reason to refuse, or std::nullopt to accept
 */
inline std::optional<DumpSourceIdentityMismatch> FindDumpSourceIdentityMismatch(
    const DumpSourceIdentity& dump_source, const std::string& live_source_server_uuid) {
  if (live_source_server_uuid.empty() || !dump_source.recorded) {
    return std::nullopt;
  }
  if (dump_source.uuid.empty()) {
    return DumpSourceIdentityMismatch{"missing_source_server_uuid",
                                      "dump does not record its MySQL source server UUID"};
  }
  if (dump_source.uuid != live_source_server_uuid) {
    return DumpSourceIdentityMismatch{"source_server_uuid_mismatch",
                                      "dump MySQL source server UUID does not match the running MySQL source"};
  }
  return std::nullopt;
}

}  // namespace mygramdb::storage
