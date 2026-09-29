#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "airflow_control/status.hpp"
#include "detail/records.hpp"

namespace airflow_control::detail {

/// Encodes the model into the canonical durable payload.
///
/// The encoding is canonical: the same model always produces the same bytes,
/// and every collection is written in a deterministic order that does not
/// depend on the order the engine happened to learn about it.
[[nodiscard]] Status encode_model(const ModelState& state, std::vector<std::uint8_t>& out);

/// Decodes a durable payload.
///
/// Every structure is bounded before it is allocated, every enum value is
/// checked against its declared set, every count is checked against the bound
/// that applies to it, and a decode that does not consume the payload exactly
/// is refused. A payload that fails any check is refused; it is never repaired.
[[nodiscard]] Result<ModelState> decode_model(const std::uint8_t* data, std::size_t size);

/// FNV-1a 64-bit digest. Used for request fingerprints and state digests.
[[nodiscard]] std::uint64_t fnv1a64(const std::uint8_t* data, std::size_t size) noexcept;
[[nodiscard]] std::uint64_t fnv1a64_extend(std::uint64_t seed, const std::uint8_t* data,
                                           std::size_t size) noexcept;
[[nodiscard]] std::uint64_t fnv1a64_text(std::uint64_t seed, const std::string& text) noexcept;

}  // namespace airflow_control::detail
