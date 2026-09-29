#include "detail/codec.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace airflow_control::detail {
namespace {

/// True for the C0 and C1 control ranges. Neither may appear in a text field:
/// they carry no meaning here and they are what makes a durable string unsafe to
/// print or to embed in a diagnostic.
constexpr bool is_control(std::uint32_t code_point) noexcept {
  return code_point < 0x20u || (code_point >= 0x80u && code_point <= 0x9Fu);
}

enum class TextVerdict { ok, malformed, control };

/// Strict UTF-8 validation.
///
/// Every sequence must be the shortest encoding of its code point: overlong
/// forms, surrogates, code points above U+10FFFF, and truncated sequences are
/// all refused rather than replaced, because a decoder that repairs input turns
/// corruption into a silently different value.
TextVerdict classify_text(const std::uint8_t* data, std::size_t size) noexcept {
  std::size_t index = 0;
  while (index < size) {
    const std::uint8_t lead = data[index];
    std::uint32_t code_point = 0;
    std::size_t length = 0;
    if (lead < 0x80u) {
      code_point = lead;
      length = 1;
    } else if (lead >= 0xC2u && lead <= 0xDFu) {
      code_point = lead & 0x1Fu;
      length = 2;
    } else if (lead >= 0xE0u && lead <= 0xEFu) {
      code_point = lead & 0x0Fu;
      length = 3;
    } else if (lead >= 0xF0u && lead <= 0xF4u) {
      code_point = lead & 0x07u;
      length = 4;
    } else {
      // A stray continuation byte, an overlong lead (C0/C1), or a lead that
      // cannot start a five- or six-byte form.
      return TextVerdict::malformed;
    }
    if (size - index < length) {
      return TextVerdict::malformed;
    }
    for (std::size_t offset = 1; offset < length; ++offset) {
      const std::uint8_t continuation = data[index + offset];
      if ((continuation & 0xC0u) != 0x80u) {
        return TextVerdict::malformed;
      }
      code_point = (code_point << 6) | (continuation & 0x3Fu);
    }
    // The shortest form of a code point is the only accepted form, which is
    // exactly the overlong check; the remaining checks reject the surrogate
    // range and the code points Unicode does not define.
    if (length > 1) {
      const std::uint32_t minimum = length == 2 ? 0x80u : (length == 3 ? 0x800u : 0x10000u);
      if (code_point < minimum) {
        return TextVerdict::malformed;
      }
    }
    if (code_point > 0x10FFFFu) {
      return TextVerdict::malformed;
    }
    if (code_point >= 0xD800u && code_point <= 0xDFFFu) {
      return TextVerdict::malformed;
    }
    if (is_control(code_point)) {
      return TextVerdict::control;
    }
    index += length;
  }
  return TextVerdict::ok;
}

}  // namespace

// ---------------------------------------------------------------------------
// ByteWriter
// ---------------------------------------------------------------------------

void ByteWriter::u8(std::uint8_t value) {
  data_.push_back(value);
}

void ByteWriter::u16(std::uint16_t value) {
  data_.push_back(static_cast<std::uint8_t>(value & 0xFFu));
  data_.push_back(static_cast<std::uint8_t>((value >> 8) & 0xFFu));
}

void ByteWriter::u32(std::uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    data_.push_back(static_cast<std::uint8_t>((value >> shift) & 0xFFu));
  }
}

void ByteWriter::u64(std::uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    data_.push_back(static_cast<std::uint8_t>((value >> shift) & 0xFFu));
  }
}

void ByteWriter::i64(std::int64_t value) {
  // Two's complement is the encoding the reader inverts, and it is the
  // representation C++20 guarantees for signed integers.
  u64(static_cast<std::uint64_t>(value));
}

void ByteWriter::bytes(const std::uint8_t* data, std::size_t length) {
  if (length == 0) {
    return;
  }
  data_.insert(data_.end(), data, data + length);
}

void ByteWriter::zeros(std::size_t length) {
  data_.insert(data_.end(), length, std::uint8_t{0});
}

void ByteWriter::blob(const std::vector<std::uint8_t>& value) {
  // The length field is 32 bits wide. A longer field cannot be expressed, and a
  // writer holding more than 4 GiB in one buffer cannot arise from any bound
  // this runtime enforces.
  u32(static_cast<std::uint32_t>(value.size()));
  bytes(value.data(), value.size());
}

void ByteWriter::text(const std::string& value) {
  u32(static_cast<std::uint32_t>(value.size()));
  bytes(reinterpret_cast<const std::uint8_t*>(value.data()), value.size());
}

// ---------------------------------------------------------------------------
// ByteReader
// ---------------------------------------------------------------------------

Result<const std::uint8_t*> ByteReader::take(std::size_t length) {
  if (length > remaining()) {
    return decode_failure("field");
  }
  const std::uint8_t* at = data_;
  if (at != nullptr) {
    at += offset_;
  }
  offset_ += length;
  return at;
}

Result<std::uint8_t> ByteReader::u8() {
  Result<const std::uint8_t*> field = take(1);
  if (!field.ok()) {
    return field.status();
  }
  return *field.value();
}

Result<std::uint16_t> ByteReader::u16() {
  Result<const std::uint8_t*> field = take(2);
  if (!field.ok()) {
    return field.status();
  }
  const std::uint8_t* at = field.value();
  const std::uint16_t low = static_cast<std::uint16_t>(at[0]);
  const std::uint16_t high = static_cast<std::uint16_t>(at[1]);
  return static_cast<std::uint16_t>(low | static_cast<std::uint16_t>(high << 8));
}

Result<std::uint32_t> ByteReader::u32() {
  Result<const std::uint8_t*> field = take(4);
  if (!field.ok()) {
    return field.status();
  }
  const std::uint8_t* at = field.value();
  std::uint32_t value = 0;
  for (int shift = 0; shift < 32; shift += 8) {
    value |= static_cast<std::uint32_t>(at[shift / 8]) << shift;
  }
  return value;
}

Result<std::uint64_t> ByteReader::u64() {
  Result<const std::uint8_t*> field = take(8);
  if (!field.ok()) {
    return field.status();
  }
  const std::uint8_t* at = field.value();
  std::uint64_t value = 0;
  for (int shift = 0; shift < 64; shift += 8) {
    value |= static_cast<std::uint64_t>(at[shift / 8]) << shift;
  }
  return value;
}

Result<std::int64_t> ByteReader::i64() {
  Result<std::uint64_t> bits = u64();
  if (!bits.ok()) {
    return bits.status();
  }
  return static_cast<std::int64_t>(bits.value());
}

Result<const std::uint8_t*> ByteReader::raw(std::size_t length) {
  return take(length);
}

Result<bool> ByteReader::present() {
  if (remaining() < 1) {
    return decode_failure("presence byte");
  }
  const std::uint8_t value = data_[offset_];
  if (value > 1) {
    // The byte is not consumed: a payload that is refused is refused whole.
    return Status::failure(StatusCode::invalid_argument,
                           "presence byte is neither 0 nor 1");
  }
  offset_ += 1;
  return value == 1;
}

Result<std::size_t> ByteReader::count(std::size_t max_count, const char* what) {
  Result<std::uint32_t> value = u32();
  if (!value.ok()) {
    return decode_failure(what);
  }
  if (value.value() > max_count) {
    return Status::failure(StatusCode::bounds_exceeded,
                           std::string(what) + " count " + std::to_string(value.value()) +
                               " exceeds the bound " + std::to_string(max_count));
  }
  return static_cast<std::size_t>(value.value());
}

Result<std::vector<std::uint8_t>> ByteReader::blob(std::size_t max_length) {
  Result<std::uint32_t> length = u32();
  if (!length.ok()) {
    return decode_failure("blob length");
  }
  if (length.value() > max_length) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "blob length " + std::to_string(length.value()) +
                               " exceeds the bound " + std::to_string(max_length));
  }
  Result<const std::uint8_t*> body = take(length.value());
  if (!body.ok()) {
    return decode_failure("blob");
  }
  return std::vector<std::uint8_t>(body.value(), body.value() + length.value());
}

Result<std::string> ByteReader::text(std::size_t max_length) {
  Result<std::uint32_t> length = u32();
  if (!length.ok()) {
    return decode_failure("text length");
  }
  if (length.value() > max_length) {
    return Status::failure(StatusCode::bounds_exceeded,
                           "text length " + std::to_string(length.value()) +
                               " exceeds the bound " + std::to_string(max_length));
  }
  Result<const std::uint8_t*> body = take(length.value());
  if (!body.ok()) {
    return decode_failure("text");
  }
  const std::size_t size = length.value();
  const TextVerdict verdict = classify_text(body.value(), size);
  if (verdict == TextVerdict::malformed) {
    return Status::failure(StatusCode::invalid_argument, "text is not valid UTF-8");
  }
  if (verdict == TextVerdict::control) {
    return Status::failure(StatusCode::invalid_argument,
                           "text contains a C0 or C1 control character");
  }
  if (size == 0) {
    return std::string();
  }
  return std::string(reinterpret_cast<const char*>(body.value()), size);
}

Status ByteReader::require_exhausted() const {
  if (exhausted()) {
    return Status::success();
  }
  return Status::failure(StatusCode::invalid_argument,
                         "trailing bytes after payload: " + std::to_string(remaining()) +
                             " byte(s) unconsumed");
}

Status decode_failure(const char* what) {
  return Status::failure(StatusCode::invalid_argument, std::string("truncated ") + what);
}

}  // namespace airflow_control::detail
