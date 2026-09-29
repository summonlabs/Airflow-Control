#pragma once

#include <cstddef>
#include <cstdint>

namespace airflow_control::detail {

/// CRC-32C (Castagnoli) of a buffer.
[[nodiscard]] std::uint32_t crc32c(const std::uint8_t* data, std::size_t length) noexcept;

/// CRC-32C continued from a previous result.
[[nodiscard]] std::uint32_t crc32c_extend(std::uint32_t seed, const std::uint8_t* data,
                                          std::size_t length) noexcept;

/// CRC-32C of a single byte.
[[nodiscard]] std::uint32_t crc32c_byte(std::uint32_t seed, std::uint8_t value) noexcept;

}  // namespace airflow_control::detail
