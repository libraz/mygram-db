/**
 * @file log_field_names.h
 * @brief Canonical StructuredLog field-name and event-name constants.
 *
 * Phase 4A introduces this header to eliminate field-name drift across
 * server / cache logs (e.g. `path` vs `filepath`, `remote_addr` vs
 * `client_ip`, `client_fd` vs `connection_fd` vs `fd`).
 *
 * ## Usage
 *
 * All `StructuredLog().Field(...)` call sites in the server / cache layers
 * SHOULD use the constants defined here:
 *
 * @code
 *   using mygramdb::server::log_fields::kFieldFilepath;
 *   StructuredLog().Event("dump_save_failed").Field(kFieldFilepath, path).Error();
 * @endcode
 *
 * When adding a brand-new field, add it here first, with a comment that
 * documents the canonical type / unit (e.g. milliseconds, byte count). Do
 * NOT introduce ad-hoc field names directly at the call site; that drift
 * is exactly what this header is meant to prevent.
 *
 * ## Event-name policy
 *
 * Event names use the `<module>_<verb>_<outcome>` form, where outcome is
 * one of `failed`, `warning`, `succeeded`, `starting`, `completed`, etc.
 * Examples: `dump_save_failed`, `accept_failed`, `keepalive_warning`.
 *
 * The legacy catch-all events `server_error` / `server_warning` are being
 * phased out — do NOT add new call sites that emit those event names.
 * Existing call sites are being renamed to dedicated `<module>_<verb>_*`
 * events (tracked under TODOs in the relevant source files).
 */

#pragma once

#include <string>

namespace mygramdb::server::log_fields {

// ---------------------------------------------------------------------------
// Filesystem paths
// ---------------------------------------------------------------------------

/// Canonical filesystem path field. Replaces both `path` and `filepath`.
inline const std::string kFieldFilepath = "filepath";

// ---------------------------------------------------------------------------
// Network / connection identifiers
// ---------------------------------------------------------------------------

/// File descriptor (signed int64). Replaces `client_fd` and `connection_fd`.
inline const std::string kFieldFd = "fd";

/// Remote client IP address as a string. Replaces `remote_addr`.
inline const std::string kFieldClientIp = "client_ip";

/// Remote client port number (signed int64).
inline const std::string kFieldClientPort = "client_port";

// ---------------------------------------------------------------------------
// Replication / GTID
// ---------------------------------------------------------------------------

/// GTID position as a string. The protocol/response field names
/// `replication_gtid` and `current_gtid` (in `response_formatter.cpp` /
/// `http_server.cpp`) are deliberately NOT changed — those are part of the
/// stable client-facing wire format.
inline const std::string kFieldGtid = "gtid";

// ---------------------------------------------------------------------------
// Errors and operations
// ---------------------------------------------------------------------------

/// Free-form error message string. Pair with `kFieldErrorCode` when an
/// `Error` is available — prefer `StructuredLog::FieldError(err)`.
inline const std::string kFieldError = "error";

/// Numeric error code.
inline const std::string kFieldErrorCode = "error_code";

/// Logical operation name (e.g. `dump_save`, `accept`). Used to disambiguate
/// when the same event name applies to multiple operations.
inline const std::string kFieldOperation = "operation";

// ---------------------------------------------------------------------------
// Request / table / document context
// ---------------------------------------------------------------------------

/// Table name.
inline const std::string kFieldTable = "table";

/// Document identifier (signed int64).
inline const std::string kFieldDocId = "doc_id";

/// Request identifier (string).
inline const std::string kFieldRequestId = "request_id";

// ---------------------------------------------------------------------------
// Timing and counts
// ---------------------------------------------------------------------------

/// Duration in milliseconds (double or int64).
inline const std::string kFieldDurationMs = "duration_ms";

/// Generic count field (uint64).
inline const std::string kFieldCount = "count";

}  // namespace mygramdb::server::log_fields
