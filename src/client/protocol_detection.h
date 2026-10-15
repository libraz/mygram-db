/**
 * @file protocol_detection.h
 * @brief Response-frame completion detection for the MygramDB client
 *
 * The TCP protocol has no length prefix, and several replies carry blank
 * lines inside their bodies, so whether the bytes received so far form the
 * whole frame depends on which command was sent. BeginResponseFrame records
 * that context once per request (command verb, a SEARCH HIGHLIGHT clause,
 * the connection's DEBUG flag); IsResponseComplete then walks the reply line
 * by line and stops exactly at the frame's last CRLF. The one exception is the
 * trailing CRLF after an OK FACET reply whose header omits the distinct-value
 * total; the next frame skips it as a leading blank line. The grammar
 * follows spec/tcp-commands.md §8, §10 and §12, with the transport CRLF the
 * server appends to every response body (§1) included. An ERROR line ends
 * every frame.
 */

#pragma once

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <vector>

#include "client/wire_quoting.h"
#include "server/protocol_constants.h"
#include "utils/string_utils.h"

namespace mygramdb::client::detail {

namespace proto = mygramdb::server::protocol;

/**
 * @brief One request token as the server tokenizer splits it (spec/tcp-commands.md §2.1)
 *
 * @p text is the raw bytes of the token, without the surrounding quotes and
 * with escapes left undecoded; it views into the tokenized command.
 */
struct RequestToken {
  std::string_view text;
  bool quoted = false;
};

/**
 * @brief Split a request line the way the server tokenizer does
 *
 * Splits on ASCII and Unicode whitespace outside quotes; `"` and `'` open a
 * quoted region that a backslash escape cannot close, and opening a quote
 * flushes any pending unquoted token. Only token boundaries and the quoted
 * flag are recovered; escapes are not decoded.
 */
inline std::vector<RequestToken> TokenizeRequest(std::string_view command) {
  std::vector<RequestToken> tokens;
  size_t start = std::string_view::npos;
  const auto flush = [&](size_t end) {
    if (start != std::string_view::npos) {
      tokens.push_back({command.substr(start, end - start), false});
      start = std::string_view::npos;
    }
  };

  size_t pos = 0;
  while (pos < command.size()) {
    if (IsUnicodeWhitespace(command, pos)) {
      flush(pos);
      // IsUnicodeWhitespace matches ASCII (1 byte), U+00A0 (2 bytes) or a 3-byte code point.
      const auto lead = static_cast<unsigned char>(command[pos]);
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      pos += lead < 0x80 ? 1 : (lead == 0xC2 ? 2 : 3);
      continue;
    }
    const char quote = command[pos];
    if (quote == '"' || quote == '\'') {
      flush(pos);
      const size_t open = ++pos;
      while (pos < command.size() && command[pos] != quote) {
        pos += command[pos] == '\\' ? 2 : 1;
      }
      const size_t close = pos < command.size() ? pos : command.size();
      tokens.push_back({command.substr(open, close - open), true});
      pos = close + 1;
      continue;
    }
    if (start == std::string_view::npos) {
      start = pos;
    }
    ++pos;
  }
  flush(command.size());
  return tokens;
}

/**
 * @brief ASCII case-insensitive equality, matching the server's keyword comparison (spec §3)
 */
inline bool EqualsIgnoreAsciiCase(std::string_view lhs, std::string_view rhs) {
  return mygram::utils::EqualsAsciiCaseInsensitive(lhs, rhs);
}

/**
 * @brief Commands whose reply grammar cannot be told from the reply's first line alone
 */
enum class FrameCommand {
  kOther,
  kSearch,      ///< "OK RESULTS" may carry highlight rows and/or a DEBUG block
  kCount,       ///< "OK COUNT" may carry a DEBUG block
  kConfigHelp,  ///< "+OK" body carries exactly one internal blank line
};

/**
 * @brief Per-request framing context plus the parse position inside the reply
 *
 * Create one with BeginResponseFrame for every request and pass it to each
 * IsResponseComplete call for that request's reply.
 */
struct ResponseFrameState {
  FrameCommand command = FrameCommand::kOther;
  bool debug = false;      ///< The connection had DEBUG ON when the command was sent
  bool highlight = false;  ///< A SEARCH carried an unquoted HIGHLIGHT clause

  size_t line_start = 0;           ///< Offset of the first line not yet consumed
  size_t scan_offset = 0;          ///< Where the CRLF search resumes inside an unfinished line
  size_t frame_start = 0;          ///< Offset of the status line; blank lines before it are a prior reply's leftover
  bool header_seen = false;        ///< The status line has been consumed
  bool body_line_pending = false;  ///< The next line is body even if empty (bare "+OK")
  bool until_end_line = false;     ///< Lines are body until one reads exactly "END"
  size_t blank_lines_left = 0;     ///< Empty lines still to come; the last one ends the frame
};

/**
 * @brief Decode a request token's escapes the way the server tokenizer does
 *
 * Inside quotes `\n`, `\t`, `\r`, `\\`, `\"`, `\'` and `\xHH` decode, `\x`
 * without two hex digits is `x`, and any other escaped byte stands for
 * itself. An unquoted token is taken verbatim, backslashes included.
 */
inline std::string DecodeRequestToken(const RequestToken& token) {
  const std::string_view text = token.text;
  if (!token.quoted) {
    return std::string(text);
  }
  std::string decoded;
  decoded.reserve(text.size());
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] != '\\' || i + 1 >= text.size()) {
      decoded += text[i];
      continue;
    }
    const char escaped = text[++i];
    switch (escaped) {
      case 'n':
        decoded += '\n';
        break;
      case 't':
        decoded += '\t';
        break;
      case 'r':
        decoded += '\r';
        break;
      case 'x': {
        const int high = i + 2 < text.size() ? HexDigitValue(text[i + 1]) : -1;
        const int low = i + 2 < text.size() ? HexDigitValue(text[i + 2]) : -1;
        if (high >= 0 && low >= 0) {
          decoded += static_cast<char>((high << 4) | low);
          i += 2;
        } else {
          decoded += 'x';
        }
        break;
      }
      default:
        decoded += escaped;
        break;
    }
  }
  return decoded;
}

/**
 * @brief FILTER, SORT, LIMIT, OFFSET, HIGHLIGHT, FUZZY or FACET, which end even a Boolean search text
 */
inline bool IsNonExpressionClauseKeyword(std::string_view token) {
  return EqualsIgnoreAsciiCase(token, "FILTER") || EqualsIgnoreAsciiCase(token, "SORT") ||
         EqualsIgnoreAsciiCase(token, "LIMIT") || EqualsIgnoreAsciiCase(token, "OFFSET") ||
         EqualsIgnoreAsciiCase(token, "HIGHLIGHT") || EqualsIgnoreAsciiCase(token, "FUZZY") ||
         EqualsIgnoreAsciiCase(token, "FACET");
}

/**
 * @brief A non-expression clause keyword, or AND, OR or NOT
 */
inline bool IsClauseKeyword(std::string_view token) {
  return IsNonExpressionClauseKeyword(token) || EqualsIgnoreAsciiCase(token, "AND") ||
         EqualsIgnoreAsciiCase(token, "OR") || EqualsIgnoreAsciiCase(token, "NOT");
}

/**
 * @brief Find where the server ends a SEARCH's search text
 *
 * Mirrors ParseSearchTextTokens (query_parser_commands.cpp). The text is a
 * Boolean expression when it opens with NOT, or when a top-level OR, or a
 * parenthesized group after a top-level AND/NOT, comes before the first
 * clause keyword. A Boolean expression ends only at a top-level
 * non-expression clause keyword; a flat one also at AND, OR or NOT. Quoted
 * tokens are never keywords and their parentheses never count.
 *
 * @return Index of the first token after the search text
 */
inline size_t SkipSearchText(const std::vector<RequestToken>& tokens, size_t start) {
  const auto paren_delta = [](std::string_view text) {
    return static_cast<int>(std::count(text.begin(), text.end(), '(') - std::count(text.begin(), text.end(), ')'));
  };

  bool boolean_expression =
      start < tokens.size() && !tokens[start].quoted && EqualsIgnoreAsciiCase(tokens[start].text, "NOT");
  int depth = 0;
  bool seen_operator = false;
  for (size_t i = start; i < tokens.size() && !boolean_expression; ++i) {
    const std::string_view text = tokens[i].text;
    if (tokens[i].quoted) {
      continue;
    }
    if (depth == 0 && seen_operator && text.find('(') != std::string_view::npos) {
      boolean_expression = true;
    }
    depth += paren_delta(text);
    if (depth == 0 && (IsNonExpressionClauseKeyword(text) || EqualsIgnoreAsciiCase(text, "ORDER"))) {
      break;
    }
    if (depth == 0 && EqualsIgnoreAsciiCase(text, "OR")) {
      boolean_expression = true;
    }
    if (depth == 0 && (EqualsIgnoreAsciiCase(text, "AND") || EqualsIgnoreAsciiCase(text, "NOT"))) {
      seen_operator = true;
    }
  }

  depth = 0;
  size_t pos = start;
  for (; pos < tokens.size(); ++pos) {
    const std::string_view text = tokens[pos].text;
    if (tokens[pos].quoted) {
      continue;
    }
    depth += paren_delta(text);
    if (depth == 0 && (IsNonExpressionClauseKeyword(text) || (!boolean_expression && IsClauseKeyword(text)))) {
      break;
    }
  }
  return pos;
}

/**
 * @brief Skip the operands of a FILTER clause the way ParseFilterArguments consumes them
 *
 * A token holding the operator (`col=value`, or `col=` followed by the value)
 * is one or two tokens; otherwise the clause takes `col op value`.
 *
 * @param pos Index just after FILTER
 * @return Index just after the clause's operands
 */
inline size_t SkipFilterOperands(const std::vector<RequestToken>& tokens, size_t pos) {
  constexpr std::array<std::string_view, 7> kOperators = {">=", "<=", "!=", "<>", "=", ">", "<"};
  constexpr size_t kSeparateOperands = 3;
  const auto starts_with_operator = [](std::string_view value) {
    return !value.empty() && (value[0] == '=' || value[0] == '<' || value[0] == '>' || value[0] == '!');
  };
  if (pos >= tokens.size()) {
    return pos;
  }
  const std::string first = DecodeRequestToken(tokens[pos]);
  for (const auto op : kOperators) {
    const size_t at = first.find(op);
    if (at == std::string::npos) {
      continue;
    }
    const std::string_view value = std::string_view(first).substr(at + op.size());
    if (at > 0 && !value.empty() && !starts_with_operator(value)) {
      return pos + 1;
    }
    if (at > 0 && value.empty() && pos + 1 < tokens.size() &&
        !starts_with_operator(DecodeRequestToken(tokens[pos + 1]))) {
      return pos + 2;
    }
    break;
  }
  return pos + kSeparateOperands;
}

/**
 * @brief Skip the operands of a SORT clause: `[BY] col [ASC|DESC]` or `[BY] ASC|DESC`
 *
 * @param pos Index just after SORT
 * @return Index just after the clause's operands
 */
inline size_t SkipSortOperands(const std::vector<RequestToken>& tokens, size_t pos) {
  const auto token_is = [&tokens](size_t index, std::string_view word) {
    return index < tokens.size() && EqualsIgnoreAsciiCase(DecodeRequestToken(tokens[index]), word);
  };
  const auto is_order = [&token_is](size_t index) { return token_is(index, "ASC") || token_is(index, "DESC"); };
  if (token_is(pos, "BY")) {
    ++pos;
  }
  if (is_order(pos)) {
    return pos + 1;
  }
  ++pos;
  return is_order(pos) ? pos + 1 : pos;
}

/**
 * @brief Check whether a SEARCH carries a HIGHLIGHT clause
 *
 * Mirrors QueryParser::ParseSearch: after the search text each clause
 * consumes its operands (AND and NOT one term, AND NOT two, FILTER and SORT
 * as above, LIMIT and OFFSET one token, FUZZY an optional distance), and
 * only an unquoted HIGHLIGHT in clause position counts. Requests the server
 * rejects reply with ERROR, which ends the frame whatever this returns.
 */
inline bool SearchRequestsHighlight(const std::vector<RequestToken>& tokens) {
  constexpr size_t kSearchTextStart = 2;
  const auto keyword_at = [&tokens](size_t index, std::string_view keyword) {
    return index < tokens.size() && !tokens[index].quoted && EqualsIgnoreAsciiCase(tokens[index].text, keyword);
  };

  size_t pos = SkipSearchText(tokens, kSearchTextStart);
  while (pos < tokens.size()) {
    const size_t clause = pos++;
    if (keyword_at(clause, "HIGHLIGHT")) {
      return true;
    }
    if (keyword_at(clause, "AND")) {
      pos += keyword_at(pos, "NOT") ? 2 : 1;
    } else if (keyword_at(clause, "NOT") || keyword_at(clause, "LIMIT") || keyword_at(clause, "OFFSET")) {
      ++pos;
    } else if (keyword_at(clause, "FILTER")) {
      pos = SkipFilterOperands(tokens, pos);
    } else if (keyword_at(clause, "SORT")) {
      pos = SkipSortOperands(tokens, pos);
    } else if (keyword_at(clause, "FUZZY")) {
      if (pos < tokens.size() && !IsClauseKeyword(DecodeRequestToken(tokens[pos]))) {
        ++pos;
      }
    } else {
      return false;
    }
  }
  return false;
}

/**
 * @brief Record the framing context of a request before its reply is read
 *
 * @param command    The request line as sent, without its CRLF
 * @param debug_mode Whether the connection has DEBUG ON (spec §10.7)
 */
inline ResponseFrameState BeginResponseFrame(std::string_view command, bool debug_mode) {
  const auto tokens = TokenizeRequest(command);
  ResponseFrameState state;
  state.debug = debug_mode;
  if (tokens.empty()) {
    return state;
  }
  const std::string_view verb = tokens[0].text;
  if (EqualsIgnoreAsciiCase(verb, "SEARCH")) {
    state.command = FrameCommand::kSearch;
    state.highlight = SearchRequestsHighlight(tokens);
  } else if (EqualsIgnoreAsciiCase(verb, "COUNT")) {
    state.command = FrameCommand::kCount;
  } else if (EqualsIgnoreAsciiCase(verb, "CONFIG") && tokens.size() > 1 &&
             EqualsIgnoreAsciiCase(tokens[1].text, "HELP")) {
    state.command = FrameCommand::kConfigHelp;
  }
  return state;
}

/**
 * @brief Classify the status line and set up the rest of the frame
 *
 * @return true when the status line is the whole frame
 */
inline bool ConsumeResponseHeader(std::string_view line, ResponseFrameState& state) {
  const auto starts_with = [line](std::string_view prefix) { return line.substr(0, prefix.size()) == prefix; };
  constexpr std::string_view kTableBorderPrefix = "+-";

  if (starts_with(proto::kErrorPrefix)) {
    return true;
  }
  if (line == proto::kOkInfoPrefix || line == proto::kOkReplicationPrefix || line == proto::kOkCacheStatsPrefix ||
      line == proto::kOkDumpStatusPrefix || starts_with(proto::kOkDumpInfoPrefix)) {
    state.until_end_line = true;
  } else if (line == proto::kOkSyncStatusPrefix) {
    // SyncOperationManager's body already ends in "END\r\n", so the transport CRLF adds a blank line.
    state.until_end_line = true;
    state.blank_lines_left = 1;
  } else if (starts_with(proto::kOkFacetPrefix)) {
    // Rows, a blank line closing the body, then the transport CRLF. Without the
    // distinct-value total the frame ends at the first blank line; a CRLF after
    // it is skipped as the next reply's leading blank line.
    const bool has_total = line.find(' ', proto::kOkFacetWithSpacePrefix.size()) != std::string_view::npos;
    state.blank_lines_left = has_total ? 2 : 1;
  } else if (starts_with(kTableBorderPrefix)) {
    // Non-empty SHOW VARIABLES: a bare table with no status prefix (spec §10.10).
    state.blank_lines_left = 1;
  } else if (line == proto::kPlusOkPrefix) {
    // CONFIG body: at least one line, which may be empty when CONFIG SHOW renders an empty value.
    state.body_line_pending = true;
    state.blank_lines_left = state.command == FrameCommand::kConfigHelp ? 2 : 1;
  } else if (starts_with(proto::kPlusOkPrefix)) {
    state.blank_lines_left = 1;
  } else if (state.command == FrameCommand::kSearch && starts_with(proto::kOkResultsWithSpacePrefix)) {
    // A highlight header carries only the count; primary keys on it mean a plain reply.
    const std::string_view after_count = line.substr(proto::kOkResultsWithSpacePrefix.size());
    const bool highlight_rows =
        state.highlight && after_count.find_first_not_of("0123456789") == std::string_view::npos;
    state.blank_lines_left = (highlight_rows ? 1 : 0) + (state.debug ? 2 : 0);
  } else if (state.command == FrameCommand::kCount && starts_with(proto::kOkCountWithSpacePrefix)) {
    state.blank_lines_left = state.debug ? 2 : 0;
  }
  return !state.until_end_line && !state.body_line_pending && state.blank_lines_left == 0;
}

/**
 * @brief Check whether the accumulated reply bytes are one complete frame
 *
 * Call after every recv() with all bytes received so far for the request
 * @p state was begun for; already-consumed lines are not rescanned.
 *
 * @param response Accumulated reply bytes
 * @param state    Context from BeginResponseFrame, advanced in place
 * @return true when @p response ends exactly at the end of the frame
 */
inline bool IsResponseComplete(std::string_view response, ResponseFrameState& state) {
  constexpr std::string_view kCrlf = "\r\n";
  constexpr std::string_view kEndLine = "END";
  while (true) {
    const size_t line_end = response.find(kCrlf, std::max(state.line_start, state.scan_offset));
    if (line_end == std::string_view::npos) {
      state.scan_offset = response.empty() ? 0 : response.size() - 1;
      return false;
    }
    const std::string_view line = response.substr(state.line_start, line_end - state.line_start);
    state.line_start = line_end + kCrlf.size();

    if (!state.header_seen) {
      // No reply starts with a blank line, so one here is left over from the previous reply.
      if (line.empty()) {
        state.frame_start = state.line_start;
        continue;
      }
      state.header_seen = true;
      if (ConsumeResponseHeader(line, state)) {
        return true;
      }
      continue;
    }
    if (state.body_line_pending) {
      state.body_line_pending = false;
      continue;
    }
    if (state.until_end_line) {
      if (line == kEndLine) {
        state.until_end_line = false;
        if (state.blank_lines_left == 0) {
          return true;
        }
      }
      continue;
    }
    if (line.empty() && --state.blank_lines_left == 0) {
      return true;
    }
  }
}

}  // namespace mygramdb::client::detail
