/**
 * @file safe_path.h
 * @brief Safe file path resolution with traversal protection
 *
 * Provides a unified utility for resolving and validating filesystem paths
 * against a base directory. Used by handlers (CONFIG VERIFY, DUMP *) to
 * prevent path traversal and arbitrary file access.
 */

#pragma once

#include <initializer_list>
#include <string>
#include <string_view>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygram::utils {

/**
 * @brief Resolve `input` to an absolute path guaranteed to be inside `base_dir`.
 *
 * The resolution rules are:
 *   1. If `input` is absolute, it is used as-is. Otherwise it is joined with
 *      `base_dir` (`base_dir + "/" + input`).
 *   2. The resolved path is canonicalized (`std::filesystem::canonical` if it
 *      exists, otherwise `std::filesystem::weakly_canonical`). `base_dir`
 *      itself is always canonicalized via `std::filesystem::canonical` and
 *      MUST exist.
 *   3. The canonicalized resolved path must be `lexically_relative` to the
 *      canonicalized `base_dir` and must not start with a `".."` segment.
 *      This collapses any symlink traversal (including symlinked parent
 *      directories) and rejects escapes to outside the base directory.
 *   4. If `allowed_extensions` is non-empty, the resolved file must have an
 *      extension (case-insensitive) that matches one of the entries
 *      (e.g. `{".yaml", ".yml"}`).
 *
 * Errors:
 *   - `ErrorCode::kInvalidArgument` for empty input, traversal attempts,
 *     resolution failures, or disallowed extensions.
 *
 * @param input Raw filepath (relative or absolute).
 * @param base_dir Base directory that the resolved path must reside within.
 * @param allowed_extensions Optional set of permitted extensions, case-insensitive.
 *                           Each entry must include the leading dot, e.g. ".yaml".
 *                           Empty list means any extension is allowed.
 * @param base_dir_label Optional human-readable label used when formatting the
 *                       traversal error message ("must be within <label>").
 *                       Defaults to "base directory" so existing callers do
 *                       not change. Pass e.g. "dump directory" for handlers
 *                       that need a domain-specific label.
 * @return Expected canonical (or weakly canonical) absolute path on success,
 *         or Error on failure.
 */
Expected<std::string, Error> ResolveSafePath(std::string_view input, std::string_view base_dir,
                                             std::initializer_list<std::string_view> allowed_extensions = {},
                                             std::string_view base_dir_label = "base directory");

}  // namespace mygram::utils
