/**
 * @file safe_path.cpp
 * @brief Safe path resolution implementation
 */

#include "utils/safe_path.h"

#include <filesystem>
#include <string>

#include "utils/string_utils.h"

namespace mygramdb::utils {

namespace {

/**
 * @brief Check whether `ext` (case-insensitive) is one of `allowed`.
 */
bool ExtensionAllowed(const std::string& ext, std::initializer_list<std::string_view> allowed) {
  if (allowed.size() == 0) {
    return true;
  }
  for (std::string_view a : allowed) {
    if (EqualsAsciiCaseInsensitive(ext, a)) {
      return true;
    }
  }
  return false;
}

}  // namespace

Expected<std::string, Error> ResolveSafePath(std::string_view input, std::string_view base_dir,
                                             std::initializer_list<std::string_view> allowed_extensions,
                                             std::string_view base_dir_label) {
  if (input.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Empty filepath"));
  }
  if (base_dir.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Empty base directory"));
  }

  for (char character : input) {
    const auto value = static_cast<unsigned char>(character);
    if (value < 0x20 || value == 0x7F) {
      return MakeUnexpected(
          MakeError(ErrorCode::kInvalidArgument, "Invalid filepath: control characters are not allowed"));
    }
  }

  std::string filepath{input};
  if (filepath[0] != '/') {
    std::string base{base_dir};
    filepath = base + "/" + filepath;
  }

  try {
    // Canonicalize base_dir first; it must exist.
    std::filesystem::path base_canonical = std::filesystem::canonical(std::string(base_dir));

    // For the resolved path, prefer canonical() (resolves all symlinks
    // including intermediate directories) when the file exists, otherwise
    // fall back to weakly_canonical() so that not-yet-existing targets
    // (e.g. DUMP SAVE outputs) can still be validated.
    std::filesystem::path resolved = std::filesystem::exists(filepath) ? std::filesystem::canonical(filepath)
                                                                       : std::filesystem::weakly_canonical(filepath);

    auto rel = resolved.lexically_relative(base_canonical);
    // rel == "." means resolved is base_dir itself (e.g. input "."): not a
    // descendant, so a caller that appends a suffix like ".tmp" for its
    // in-progress write would create that temp file as a *sibling* of
    // base_dir, outside the sandbox this function promises.
    if (rel.empty() || rel == std::filesystem::path(".") || *rel.begin() == std::filesystem::path("..")) {
      return MakeUnexpected(MakeError(
          ErrorCode::kInvalidArgument,
          "Invalid filepath: path must be within " + std::string(base_dir_label) + " (" + std::string(base_dir) + ")"));
    }

    if (allowed_extensions.size() > 0) {
      std::string ext = resolved.extension().string();
      if (ext.empty() || !ExtensionAllowed(ext, allowed_extensions)) {
        std::string allowed_list;
        bool first = true;
        for (std::string_view a : allowed_extensions) {
          if (!first) {
            allowed_list.append(", ");
          }
          allowed_list.append(a);
          first = false;
        }
        return MakeUnexpected(
            MakeError(ErrorCode::kInvalidArgument, "Disallowed file extension; allowed: " + allowed_list));
      }
    }

    return resolved.string();
  } catch (const std::exception& e) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, std::string("Invalid filepath: ") + e.what()));
  }
}

}  // namespace mygramdb::utils
