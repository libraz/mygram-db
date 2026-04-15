/**
 * @file synonym_dictionary.cpp
 * @brief Synonym dictionary implementation
 */

#include "query/synonym_dictionary.h"

#include <algorithm>
#include <fstream>
#include <sstream>

#include "utils/binary_io.h"
#include "utils/structured_log.h"

namespace mygramdb::query {

SynonymDictionary::SynonymDictionary(SynonymDictionary&& other) noexcept {
  std::unique_lock lock(other.mutex_);
  groups_ = std::move(other.groups_);
  term_to_group_ = std::move(other.term_to_group_);
}

SynonymDictionary& SynonymDictionary::operator=(SynonymDictionary&& other) noexcept {
  if (this != &other) {
    std::unique_lock lock1(mutex_, std::defer_lock);
    std::unique_lock lock2(other.mutex_, std::defer_lock);
    std::lock(lock1, lock2);
    groups_ = std::move(other.groups_);
    term_to_group_ = std::move(other.term_to_group_);
  }
  return *this;
}

mygram::utils::Expected<void, mygram::utils::Error> SynonymDictionary::LoadFromFile(
    const std::string& filepath, std::function<std::string(std::string_view)> normalizer) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  std::ifstream file(filepath);
  if (!file.is_open()) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageReadError, "Cannot open synonym file: " + filepath));
  }

  std::unique_lock lock(mutex_);
  groups_.clear();
  term_to_group_.clear();

  std::string line;
  size_t line_num = 0;
  while (std::getline(file, line)) {
    ++line_num;

    // Skip empty lines and comments
    if (line.empty() || line[0] == '#') {
      continue;
    }

    // Collect raw tokens first (preserve original text for diagnostic logging)
    std::vector<std::string> raw_tokens;
    {
      std::istringstream iss(line);
      std::string token;
      while (std::getline(iss, token, '\t')) {
        if (!token.empty()) {
          raw_tokens.push_back(std::move(token));
        }
      }
    }

    // Single-token lines are legitimate (just a term with no synonyms) -- skip silently
    if (raw_tokens.size() < 2) {
      continue;
    }

    // Normalize
    std::vector<std::string> terms;
    terms.reserve(raw_tokens.size());
    for (const auto& raw : raw_tokens) {
      std::string normalized = normalizer(raw);
      if (!normalized.empty()) {
        terms.push_back(std::move(normalized));
      }
    }

    // Cap group size
    if (terms.size() > kMaxGroupSize) {
      terms.resize(kMaxGroupSize);
    }

    // Deduplicate
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());

    if (terms.size() < 2) {
      // All raw tokens normalized to the same value -- the group is effectively
      // a single term, so it's dropped. Warn so operators notice.
      std::string raw_preview;
      for (size_t i = 0; i < raw_tokens.size(); ++i) {
        if (i > 0) {
          raw_preview += " | ";
        }
        raw_preview += raw_tokens[i];
        if (raw_preview.size() > 180) {
          raw_preview += "...";
          break;
        }
      }
      mygram::utils::StructuredLog()
          .Event("synonym_group_collapsed")
          .Field("line_num", static_cast<uint64_t>(line_num))
          .Field("reason", "terms_collapsed_to_single_value_after_normalization")
          .Field("raw_terms", raw_preview)
          .Warn();
      continue;
    }

    // First-wins: skip terms that already belong to an existing group
    std::vector<std::string> new_terms;
    std::vector<std::string> conflicting_terms;
    new_terms.reserve(terms.size());
    for (const auto& term : terms) {
      if (term_to_group_.find(term) == term_to_group_.end()) {
        new_terms.push_back(term);
      } else {
        conflicting_terms.push_back(term);
      }
    }

    if (new_terms.size() < 2) {
      // Not enough non-conflicting terms to form a usable group.
      std::string conflict_preview;
      for (size_t i = 0; i < conflicting_terms.size(); ++i) {
        if (i > 0) {
          conflict_preview += " | ";
        }
        conflict_preview += conflicting_terms[i];
        if (conflict_preview.size() > 180) {
          conflict_preview += "...";
          break;
        }
      }
      mygram::utils::StructuredLog()
          .Event("synonym_group_term_conflict")
          .Field("line_num", static_cast<uint64_t>(line_num))
          .Field("reason", "terms_already_belong_to_earlier_group")
          .Field("conflicting_terms", conflict_preview)
          .Field("usable_terms_count", static_cast<uint64_t>(new_terms.size()))
          .Warn();
      continue;
    }

    size_t group_index = groups_.size();
    groups_.push_back(std::move(new_terms));
    for (const auto& term : groups_.back()) {
      term_to_group_[term] = group_index;
    }
  }

  return {};
}

std::vector<std::string> SynonymDictionary::Expand(const std::string& normalized_term) const {
  std::shared_lock lock(mutex_);
  auto it = term_to_group_.find(normalized_term);
  if (it == term_to_group_.end()) {
    return {normalized_term};
  }
  std::vector<std::string> result = groups_[it->second];
  if (std::find(result.begin(), result.end(), normalized_term) == result.end()) {
    result.insert(result.begin(), normalized_term);
  }
  return result;
}

bool SynonymDictionary::IsEmpty() const {
  std::shared_lock lock(mutex_);
  return groups_.empty();
}

size_t SynonymDictionary::GroupCount() const {
  std::shared_lock lock(mutex_);
  return groups_.size();
}

size_t SynonymDictionary::TermCount() const {
  std::shared_lock lock(mutex_);
  return term_to_group_.size();
}

void SynonymDictionary::ForEachTerm(const std::function<void(const std::string&)>& callback) const {
  std::shared_lock lock(mutex_);
  for (const auto& group : groups_) {
    for (const auto& term : group) {
      callback(term);
    }
  }
}

void SynonymDictionary::Clear() {
  std::unique_lock lock(mutex_);
  groups_.clear();
  term_to_group_.clear();
}

bool SynonymDictionary::SaveToStream(std::ostream& output_stream) const {
  std::shared_lock lock(mutex_);

  auto group_count = static_cast<uint32_t>(groups_.size());
  if (!mygram::utils::WriteBinary(output_stream, group_count)) {
    return false;
  }

  for (const auto& group : groups_) {
    auto term_count = static_cast<uint32_t>(group.size());
    if (!mygram::utils::WriteBinary(output_stream, term_count)) {
      return false;
    }
    for (const auto& term : group) {
      if (!mygram::utils::WriteString(output_stream, term)) {
        return false;
      }
    }
  }

  return output_stream.good();
}

bool SynonymDictionary::LoadFromStream(std::istream& input_stream) {
  std::unique_lock lock(mutex_);
  groups_.clear();
  term_to_group_.clear();

  uint32_t group_count = 0;
  if (!mygram::utils::ReadBinary(input_stream, group_count)) {
    return false;
  }

  // Safety limit
  constexpr uint32_t kMaxGroups = 1'000'000;
  if (group_count > kMaxGroups) {
    return false;
  }

  groups_.reserve(group_count);
  for (uint32_t g = 0; g < group_count; ++g) {
    uint32_t term_count = 0;
    if (!mygram::utils::ReadBinary(input_stream, term_count)) {
      return false;
    }

    if (term_count > kMaxGroupSize) {
      return false;
    }

    std::vector<std::string> group;
    group.reserve(term_count);
    for (uint32_t t = 0; t < term_count; ++t) {
      std::string term;
      if (!mygram::utils::ReadString(input_stream, term)) {
        return false;
      }

      group.push_back(std::move(term));
    }

    size_t group_index = groups_.size();
    groups_.push_back(std::move(group));
    for (const auto& term : groups_.back()) {
      term_to_group_[term] = group_index;
    }
  }

  return true;
}

}  // namespace mygramdb::query
