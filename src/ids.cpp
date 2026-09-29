#include "airflow_control/ids.hpp"

namespace airflow_control {
namespace {

bool is_ascii_alphanumeric(char value) noexcept {
  return (value >= '0' && value <= '9') || (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z');
}

bool is_identifier_character(char value) noexcept {
  // Deliberately excludes ':' as well as every path and shell metacharacter: on
  // a filesystem that supports alternate data streams ':' is a separator that
  // would let an identifier name something other than itself.
  return is_ascii_alphanumeric(value) || value == '-' || value == '_' || value == '.';
}

/// Strict UTF-8 validation: rejects overlong encodings, surrogate code points,
/// code points above U+10FFFF, and truncated sequences. A permissive decoder
/// here would let two different byte strings decode to the same text, which
/// would make durable text non-canonical.
bool is_valid_utf8(std::string_view text) noexcept {
  std::size_t index = 0;
  while (index < text.size()) {
    const auto lead = static_cast<unsigned char>(text[index]);
    std::size_t continuation = 0;
    std::uint32_t code_point = 0;
    std::uint32_t minimum = 0;
    if (lead < 0x80) {
      continuation = 0;
      code_point = lead;
      minimum = 0;
    } else if ((lead & 0xE0u) == 0xC0u) {
      continuation = 1;
      code_point = lead & 0x1Fu;
      minimum = 0x80u;
    } else if ((lead & 0xF0u) == 0xE0u) {
      continuation = 2;
      code_point = lead & 0x0Fu;
      minimum = 0x800u;
    } else if ((lead & 0xF8u) == 0xF0u) {
      continuation = 3;
      code_point = lead & 0x07u;
      minimum = 0x10000u;
    } else {
      return false;
    }
    if (index + continuation >= text.size()) {
      return false;
    }
    for (std::size_t offset = 1; offset <= continuation; ++offset) {
      const auto next = static_cast<unsigned char>(text[index + offset]);
      if ((next & 0xC0u) != 0x80u) {
        return false;
      }
      code_point = (code_point << 6) | (next & 0x3Fu);
    }
    if (code_point < minimum) {
      return false;  // overlong
    }
    if (code_point > 0x10FFFFu) {
      return false;
    }
    if (code_point >= 0xD800u && code_point <= 0xDFFFu) {
      return false;  // surrogate
    }
    index += continuation + 1;
  }
  return true;
}

bool is_forbidden_text_code_point(std::uint32_t code_point) noexcept {
  if (code_point == 0x7Fu) {
    return true;  // DEL
  }
  if (code_point < 0x20u) {
    return true;  // C0 controls, including NUL, tab, and newline
  }
  if (code_point >= 0x80u && code_point <= 0x9Fu) {
    return true;  // C1 controls
  }
  return false;
}

/// Decodes the code point starting at index and advances index past it.
std::uint32_t decode_code_point(std::string_view text, std::size_t& index) noexcept {
  const auto lead = static_cast<unsigned char>(text[index]);
  std::size_t continuation = 0;
  std::uint32_t code_point = lead;
  if (lead >= 0xF0u) {
    continuation = 3;
    code_point = lead & 0x07u;
  } else if (lead >= 0xE0u) {
    continuation = 2;
    code_point = lead & 0x0Fu;
  } else if (lead >= 0xC0u) {
    continuation = 1;
    code_point = lead & 0x1Fu;
  }
  for (std::size_t offset = 1; offset <= continuation; ++offset) {
    code_point = (code_point << 6) | (static_cast<unsigned char>(text[index + offset]) & 0x3Fu);
  }
  index += continuation + 1;
  return code_point;
}

}  // namespace

Result<std::string> validate_identifier(std::string_view text, std::size_t max_length) {
  if (text.empty()) {
    return Status::failure(StatusCode::invalid_argument, "identifier is empty");
  }
  if (text.size() > max_length) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "identifier of " + std::to_string(text.size()) +
                               " bytes exceeds the limit of " + std::to_string(max_length));
  }
  if (text == "." || text == "..") {
    return Status::failure(StatusCode::path_invalid, "identifier cannot be a relative path element");
  }
  if (!is_ascii_alphanumeric(text.front())) {
    return Status::failure(StatusCode::invalid_argument,
                           "identifier must begin with an alphanumeric character");
  }
  if (text.back() == '.') {
    return Status::failure(StatusCode::invalid_argument,
                           "identifier cannot end with a dot");
  }
  for (const char value : text) {
    if (!is_identifier_character(value)) {
      return Status::failure(StatusCode::invalid_argument,
                             std::string("identifier contains a character that is not allowed: '") +
                                 value + "'");
    }
  }
  return std::string(text);
}

Result<std::string> validate_text(std::string_view text, std::size_t max_length) {
  if (text.size() > max_length) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "text of " + std::to_string(text.size()) +
                               " bytes exceeds the limit of " + std::to_string(max_length));
  }
  if (!is_valid_utf8(text)) {
    return Status::failure(StatusCode::invalid_argument, "text is not valid UTF-8");
  }
  std::size_t index = 0;
  while (index < text.size()) {
    const std::uint32_t code_point = decode_code_point(text, index);
    if (is_forbidden_text_code_point(code_point)) {
      return Status::failure(StatusCode::invalid_argument,
                             "text contains a control character, which is not allowed");
    }
  }
  return std::string(text);
}

Result<LogicalTick> tick_add(LogicalTick base, std::uint64_t delta) noexcept {
  if (delta > UINT64_MAX - base.value()) {
    return Status::failure(StatusCode::overflow, "logical tick overflow");
  }
  return LogicalTick::from(base.value() + delta);
}

Result<std::uint64_t> tick_age(LogicalTick now, LogicalTick earlier) noexcept {
  if (earlier > now) {
    return Status::failure(StatusCode::evidence_future,
                           "instant " + earlier.to_string() + " is later than " + now.to_string());
  }
  return now.value() - earlier.value();
}

}  // namespace airflow_control
