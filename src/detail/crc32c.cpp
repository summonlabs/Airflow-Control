#include "detail/crc32c.hpp"

#include <array>
#include <cstddef>
#include <cstdint>

namespace airflow_control::detail {
namespace {

/// The Castagnoli polynomial in reflected form: the form the byte-at-a-time
/// algorithm consumes without a bit reversal per byte.
constexpr std::uint32_t kCastagnoliReflected = 0x82F63B78u;

/// The byte-at-a-time table.
///
/// It is built on first use rather than embedded in the image: a process that
/// never checksums anything pays nothing, and the language guarantees the
/// one-time construction is thread safe.
const std::array<std::uint32_t, 256>& crc_table() noexcept {
  static const std::array<std::uint32_t, 256> table = [] {
    std::array<std::uint32_t, 256> built{};
    for (std::uint32_t index = 0; index < 256; ++index) {
      std::uint32_t remainder = index;
      for (int bit = 0; bit < 8; ++bit) {
        remainder = (remainder & 1u) != 0 ? (remainder >> 1) ^ kCastagnoliReflected
                                          : remainder >> 1;
      }
      built[index] = remainder;
    }
    return built;
  }();
  return table;
}

}  // namespace

std::uint32_t crc32c_extend(std::uint32_t seed, const std::uint8_t* data,
                            std::size_t length) noexcept {
  const std::array<std::uint32_t, 256>& table = crc_table();
  // The reflected algorithm carries the complement inside the running value so
  // that continuing from a previous result is a plain continuation.
  std::uint32_t crc = seed ^ 0xFFFFFFFFu;
  for (std::size_t index = 0; index < length; ++index) {
    crc = table[(crc ^ data[index]) & 0xFFu] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

std::uint32_t crc32c(const std::uint8_t* data, std::size_t length) noexcept {
  return crc32c_extend(0, data, length);
}

std::uint32_t crc32c_byte(std::uint32_t seed, std::uint8_t value) noexcept {
  return crc32c_extend(seed, &value, 1);
}

}  // namespace airflow_control::detail
