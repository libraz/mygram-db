/**
 * @file safe_path.cpp
 * @brief Safe path resolution implementation
 */

#include "utils/safe_path.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <string>

namespace mygram::utils {

namespace {

/**
 * @brief Lowercase an ASCII string in place (extension comparison helper).
 */
std::string ToLowerAscii(std::string_view s) {
  std::string result;
  result.reserve(s.size());
  for (char c : s) {
    result.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return result;
}

/**
 * @brief Check whether `ext` (case-insensitive) is one of `allowed`.
 */
bool ExtensionAllowed(const std::string& ext, std::initializer_list<std::string_view> allowed) {
  if (allowed.size() == 0) {
    return true;
  }
  std::string ext_lower = ToLowerAscii(ext);
  for (std::string_view a : allowed) {
    if (ext_lower == ToLowerAscii(a)) {
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

  // Validate extension up-front against the raw input. We lower-case both
  // sides for a case-insensitive match. This must happen before canonical()
  // resolves any symlinks so that the user-visible name is what we check.
  if (allowed_extensions.size() > 0) {
    std::filesystem::path input_path{std::string(input)};
    std::string ext = input_path.extension().string();
    if (ext.empty() || !ExtensionAllowed(ext, allowed_extensions)) {
      // Build a comma-separated list for the error message.
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
    if (rel.empty() || *rel.begin() == std::filesystem::path("..")) {
      return MakeUnexpected(MakeError(
          ErrorCode::kInvalidArgument,
          "Invalid filepath: path must be within " + std::string(base_dir_label) + " (" + std::string(base_dir) + ")"));
    }
    return resolved.string();
  } catch (const std::exception& e) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, std::string("Invalid filepath: ") + e.what()));
  }
}

}  // namespace mygram::utils
