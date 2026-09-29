#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "airflow_control/ids.hpp"
#include "airflow_control/status.hpp"

namespace airflow_control::detail {

/// Little-endian byte writer.
///
/// The encoding is canonical: every integer is written little-endian with no
/// padding, every variable-length field is length-prefixed, and the writer
/// never emits a byte the reader would not consume.
class ByteWriter {
 public:
  void u8(std::uint8_t value);
  void u16(std::uint16_t value);
  void u32(std::uint32_t value);
  void u64(std::uint64_t value);
  void i64(std::int64_t value);

  void bytes(const std::uint8_t* data, std::size_t length);
  void zeros(std::size_t length);

  /// A length-prefixed byte string. The length must fit in 32 bits.
  void blob(const std::vector<std::uint8_t>& value);
  /// A length-prefixed text field. The caller has already validated the text.
  void text(const std::string& value);
  /// A length-prefixed, syntax-checked identifier.
  template <typename Id>
  void identifier(const Id& value) {
    text(value.str());
  }
  /// A presence byte followed by the payload when present.
  template <typename T, typename Fn>
  void optional(const std::optional<T>& value, Fn&& write_value) {
    if (value.has_value()) {
      u8(1);
      write_value(*value);
    } else {
      u8(0);
    }
  }

  [[nodiscard]] const std::vector<std::uint8_t>& data() const noexcept { return data_; }
  [[nodiscard]] std::size_t size() const noexcept { return data_.size(); }
  [[nodiscard]] std::vector<std::uint8_t> take() && noexcept { return std::move(data_); }

 private:
  std::vector<std::uint8_t> data_;
};

/// Little-endian byte reader with strict bounds.
class ByteReader {
 public:
  ByteReader(const std::uint8_t* data, std::size_t size) noexcept : data_(data), size_(size) {}

  [[nodiscard]] Result<std::uint8_t> u8();
  [[nodiscard]] Result<std::uint16_t> u16();
  [[nodiscard]] Result<std::uint32_t> u32();
  [[nodiscard]] Result<std::uint64_t> u64();
  [[nodiscard]] Result<std::int64_t> i64();

  [[nodiscard]] Result<std::vector<std::uint8_t>> blob(std::size_t max_length);
  [[nodiscard]] Result<std::string> text(std::size_t max_length);
  template <typename Id>
  [[nodiscard]] Result<Id> identifier() {
    Result<std::string> value = text(airflow_control::kMaxIdentifierLength);
    if (!value.ok()) {
      return value.status();
    }
    return Id::parse(value.value());
  }

  /// Reads exactly length bytes, or fails without consuming anything.
  [[nodiscard]] Result<const std::uint8_t*> raw(std::size_t length);

  /// Reads a boolean presence byte, rejecting any value other than 0 or 1.
  [[nodiscard]] Result<bool> present();

  [[nodiscard]] Result<std::size_t> count(std::size_t max_count, const char* what);

  [[nodiscard]] bool exhausted() const noexcept { return offset_ == size_; }
  [[nodiscard]] std::size_t remaining() const noexcept { return size_ - offset_; }
  [[nodiscard]] std::size_t offset() const noexcept { return offset_; }

  /// Fails unless every byte was consumed. A decode that leaves trailing bytes
  /// is refused rather than tolerated.
  [[nodiscard]] Status require_exhausted() const;

 private:
  [[nodiscard]] Result<const std::uint8_t*> take(std::size_t length);

  const std::uint8_t* data_ = nullptr;
  std::size_t size_ = 0;
  std::size_t offset_ = 0;
};

/// A missing-value status used across the decoders.
[[nodiscard]] Status decode_failure(const char* what);

}  // namespace airflow_control::detail
