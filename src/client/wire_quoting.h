/**
 * @file wire_quoting.h
 * @brief Shared quote/escape decision for client-side wire-protocol tokens
 *
 * One predicate (NeedsWireQuoting) and one encoder (QuoteWireToken) back
 * every string the client turns into wire-protocol text: search terms,
 * filter values, highlight tags, primary keys, CLI arguments. Quoting
 * defeats the server's clause-keyword and OR/AND/NOT recognition (spec/
 * tcp-commands.md §2: "the quoted flag suppresses clause-keyword
 * recognition"), so a bare token that happens to collide with a reserved
 * word -- "order", "or", a filter value equal to "AND" -- must go through
 * this same decision everywhere it can reach the wire, or the collision
 * reappears at whichever call site skipped it. ParseWireToken is the
 * matching decoder for the other direction: reading a quoted/escaped token
 * back out of a response (primary keys, header fields).
 */

#pragma once

#include <cctype>
#include <string>
#include <string_view>

namespace mygramdb::client::detail {

/**
 * @brief Check whether the byte at `pos` starts a Unicode whitespace code point
 *
 * Matches ASCII whitespace plus the non-ASCII code points spec/tcp-commands.md
 * §2 documents the server's tokenizer splitting on outside quotes: U+00A0,
 * U+1680, U+2000-U+200B, U+2028, U+2029, U+202F, U+205F, U+3000.
 */
// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
inline bool IsUnicodeWhitespace(std::string_view text, size_t pos) {
  if (pos >= text.size()) {
    return false;
  }
  const auto byte = static_cast<unsigned char>(text[pos]);
  if (std::isspace(byte) != 0) {
    return true;
  }
  if (byte == 0xC2 && pos + 1 < text.size() && static_cast<unsigned char>(text[pos + 1]) == 0xA0) {
    return true;
  }
  if (pos + 2 >= text.size()) {
    return false;
  }
  const auto byte2 = static_cast<unsigned char>(text[pos + 1]);
  const auto byte3 = static_cast<unsigned char>(text[pos + 2]);
  if (byte == 0xE1 && byte2 == 0x9A && byte3 == 0x80) {
    return true;
  }
  if (byte == 0xE2 && byte2 == 0x80 &&
      ((byte3 >= 0x80 && byte3 <= 0x8B) || byte3 == 0xA8 || byte3 == 0xA9 || byte3 == 0xAF)) {
    return true;
  }
  if (byte == 0xE2 && byte2 == 0x81 && byte3 == 0x9F) {
    return true;
  }
  return byte == 0xE3 && byte2 == 0x80 && byte3 == 0x80;
}
// NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

/**
 * @brief Check whether `token` case-insensitively matches a reserved clause keyword
 *
 * AND, OR, NOT, FILTER, SORT, LIMIT, OFFSET, HIGHLIGHT, FUZZY, FACET and
 * ORDER are recognized case-insensitively by both the top-level command
 * clause scanner (query_parser_commands.cpp: EqualsIgnoreCase/ToUpper) and
 * the boolean sub-expression tokenizer (query_ast.cpp: ToUpper), so a term
 * equal to one of them must be quoted regardless of which of those two
 * parsers ends up reading it.
 */
inline bool IsReservedKeyword(std::string_view token) {
  std::string upper(token);
  for (char& ch : upper) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return upper == "AND" || upper == "OR" || upper == "NOT" || upper == "FILTER" || upper == "SORT" ||
         upper == "LIMIT" || upper == "OFFSET" || upper == "HIGHLIGHT" || upper == "FUZZY" || upper == "FACET" ||
         upper == "ORDER";
}

/**
 * @brief Check whether `value` must be quoted to survive the wire tokenizer intact
 *
 * True when `value` is empty, is a reserved keyword (see IsReservedKeyword),
 * or contains a byte the server's tokenizer treats specially outside quotes:
 * ASCII or Unicode whitespace, a control character, a quote (`"` or `'`),
 * backslash, or a parenthesis.
 */
inline bool NeedsWireQuoting(std::string_view value) {
  if (value.empty()) {
    return true;
  }
  if (IsReservedKeyword(value)) {
    return true;
  }
  for (size_t i = 0; i < value.size(); ++i) {
    const auto ch = static_cast<unsigned char>(value[i]);
    if (std::isspace(ch) != 0 || std::iscntrl(ch) != 0 || ch == '"' || ch == '\'' || ch == '\\' || ch == '(' ||
        ch == ')') {
      return true;
    }
    if (IsUnicodeWhitespace(value, i)) {
      return true;
    }
  }
  return false;
}

/**
 * @brief Quote and escape `value` for the wire protocol, only if needed
 *
 * Returns `value` unchanged when NeedsWireQuoting is false. Otherwise wraps
 * it in double quotes, backslash-escaping `\` and `"`, the common control
 * characters as `\r`/`\n`/`\t`, and any other control byte as `\xHH` --
 * reversible by the server's own quoted-string reader (spec/tcp-commands.md
 * §2) and by ParseWireToken on responses the client reads back.
 */
inline std::string QuoteWireToken(std::string_view value) {
  if (!NeedsWireQuoting(value)) {
    return std::string(value);
  }

  constexpr char kHexDigits[] = "0123456789ABCDEF";
  std::string escaped;
  escaped.reserve(value.size() + 2);
  escaped += '"';
  for (unsigned char ch : value) {
    switch (ch) {
      case '\\':
        escaped += "\\\\";
        break;
      case '"':
        escaped += "\\\"";
        break;
      case '\r':
        escaped += "\\r";
        break;
      case '\n':
        escaped += "\\n";
        break;
      case '\t':
        escaped += "\\t";
        break;
      default:
        if (std::iscntrl(ch) != 0) {
          escaped += "\\x";
          escaped += kHexDigits[ch >> 4];
          escaped += kHexDigits[ch & 0x0F];
        } else {
          escaped += static_cast<char>(ch);
        }
        break;
    }
  }
  escaped += '"';
  return escaped;
}

/**
 * @brief Decode one hex digit; -1 if `ch` is not [0-9a-fA-F]
 */
inline int HexDigitValue(char ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  }
  if (ch >= 'a' && ch <= 'f') {
    return 10 + (ch - 'a');
  }
  if (ch >= 'A' && ch <= 'F') {
    return 10 + (ch - 'A');
  }
  return -1;
}

/**
 * @brief Read one whitespace-delimited or quoted token, inverse of QuoteWireToken
 *
 * Skips leading whitespace, then reads either a bare token (up to the next
 * whitespace) or, if the token opens with `"`, a quoted token -- unescaping
 * `\\`, `\"`, `\r`, `\n`, `\t` and `\xHH` back to their literal bytes.
 * Advances `pos` past what was consumed. Used to read a response the server
 * quoted per spec/tcp-commands.md §11.1 (primary keys, header fields) back
 * into the bytes QuoteWireToken would have produced from them.
 *
 * @return true and sets `value` on success; false on an unterminated quote
 *         or a truncated `\x` escape, or when nothing is left to read.
 */
inline bool ParseWireToken(std::string_view input, size_t& pos, std::string& value) {
  while (pos < input.size() && std::isspace(static_cast<unsigned char>(input[pos])) != 0) {
    ++pos;
  }
  if (pos >= input.size()) {
    return false;
  }

  value.clear();
  if (input[pos] != '"') {
    const size_t start = pos;
    while (pos < input.size() && std::isspace(static_cast<unsigned char>(input[pos])) == 0) {
      ++pos;
    }
    value.assign(input.substr(start, pos - start));
    return true;
  }

  ++pos;
  while (pos < input.size()) {
    const char ch = input[pos++];
    if (ch == '"') {
      return true;
    }
    if (ch != '\\') {
      value += ch;
      continue;
    }
    if (pos >= input.size()) {
      return false;
    }

    const char escaped = input[pos++];
    switch (escaped) {
      case 'n':
        value += '\n';
        break;
      case 'r':
        value += '\r';
        break;
      case 't':
        value += '\t';
        break;
      case '\\':
      case '"':
        value += escaped;
        break;
      case 'x': {
        if (pos + 1 >= input.size()) {
          return false;
        }
        const int high = HexDigitValue(input[pos]);
        const int low = HexDigitValue(input[pos + 1]);
        if (high < 0 || low < 0) {
          return false;
        }
        value += static_cast<char>((high << 4) | low);
        pos += 2;
        break;
      }
      default:
        value += escaped;
        break;
    }
  }

  return false;
}

}  // namespace mygramdb::client::detail
