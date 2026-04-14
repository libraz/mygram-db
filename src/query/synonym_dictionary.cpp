/**
 * @file synonym_dictionary.cpp
 * @brief Synonym dictionary implementation
 */

#include "query/synonym_dictionary.h"

#include <algorithm>
#include <fstream>
#include <sstream>

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
    return MakeUnexpected(MakeError(ErrorCode::kIOError, "Cannot open synonym file: " + filepath));
  }

  std::unique_lock lock(mutex_);
  groups_.clear();
  term_to_group_.clear();

  std::string line;
  while (std::getline(file, line)) {
    // Skip empty lines and comments
    if (line.empty() || line[0] == '#') {
      continue;
    }

    // Split by tabs
    std::vector<std::string> terms;
    std::istringstream iss(line);
    std::string token;
    while (std::getline(iss, token, '\t')) {
      if (token.empty()) {
        continue;
      }
      // Normalize the term
      std::string normalized = normalizer(token);
      if (!normalized.empty()) {
        terms.push_back(std::move(normalized));
      }
    }

    // Need at least 2 terms for a synonym group
    if (terms.size() < 2) {
      continue;
    }

    // Cap group size
    if (terms.size() > kMaxGroupSize) {
      terms.resize(kMaxGroupSize);
    }

    // Deduplicate terms within the group
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());

    if (terms.size() < 2) {
      continue;  // All terms normalized to the same value
    }

    // For simplicity in Phase 1B, skip terms that already belong to a group
    std::vector<std::string> new_terms;
    for (const auto& term : terms) {
      if (term_to_group_.find(term) == term_to_group_.end()) {
        new_terms.push_back(term);
      }
    }

    if (new_terms.size() < 2) {
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

void SynonymDictionary::Clear() {
  std::unique_lock lock(mutex_);
  groups_.clear();
  term_to_group_.clear();
}

bool SynonymDictionary::SaveToStream(std::ostream& output_stream) const {
  std::shared_lock lock(mutex_);

  auto group_count = static_cast<uint32_t>(groups_.size());
  output_stream.write(reinterpret_cast<const char*>(&group_count), sizeof(group_count));

  for (const auto& group : groups_) {
    auto term_count = static_cast<uint32_t>(group.size());
    output_stream.write(reinterpret_cast<const char*>(&term_count), sizeof(term_count));
    for (const auto& term : group) {
      auto len = static_cast<uint32_t>(term.size());
      output_stream.write(reinterpret_cast<const char*>(&len), sizeof(len));
      output_stream.write(term.data(), static_cast<std::streamsize>(term.size()));
    }
  }

  return output_stream.good();
}

bool SynonymDictionary::LoadFromStream(std::istream& input_stream) {
  std::unique_lock lock(mutex_);
  groups_.clear();
  term_to_group_.clear();

  uint32_t group_count = 0;
  input_stream.read(reinterpret_cast<char*>(&group_count), sizeof(group_count));
  if (!input_stream.good()) {
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
    input_stream.read(reinterpret_cast<char*>(&term_count), sizeof(term_count));
    if (!input_stream.good()) {
      return false;
    }

    if (term_count > kMaxGroupSize) {
      return false;
    }

    std::vector<std::string> group;
    group.reserve(term_count);
    for (uint32_t t = 0; t < term_count; ++t) {
      uint32_t len = 0;
      input_stream.read(reinterpret_cast<char*>(&len), sizeof(len));
      if (!input_stream.good()) {
        return false;
      }

      constexpr uint32_t kMaxTermLength = 10'000;
      if (len > kMaxTermLength) {
        return false;
      }

      std::string term(len, '\0');
      input_stream.read(term.data(), static_cast<std::streamsize>(len));
      if (!input_stream.good()) {
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
