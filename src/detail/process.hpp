#pragma once

#include <cstdint>

namespace airflow_control::detail {

/// Identifier of the current operating-system process. Diagnostics only: it is
/// never used as an authority, never persisted, and never compared across
/// hosts.
[[nodiscard]] std::uint64_t current_process_id() noexcept;

}  // namespace airflow_control::detail
