#pragma once

#include <string>

#include "airflow_control/status.hpp"

namespace airflow_control::detail {

/// Resolves a caller-supplied store path into the canonical form used for
/// authority.
///
/// Two processes must never be able to obtain different locks for the same
/// logical store, so the path that names the lock is derived from the fully
/// resolved path: absolute, separators normalised, every existing component
/// resolved through reparse points, and a final component that is not a
/// directory. A path containing a NUL, a trailing separator, or a reserved
/// device name is refused.
[[nodiscard]] Result<std::string> canonical_store_path(const std::string& path);

/// The lock file path for a canonical store path.
[[nodiscard]] std::string lock_path_for(const std::string& canonical_path);

/// True when the canonical path exists as a regular file.
[[nodiscard]] bool regular_file_exists(const std::string& canonical_path);

/// Creates the parent directory if it is missing. Refuses when the parent is
/// not a directory.
[[nodiscard]] Status ensure_parent_directory(const std::string& canonical_path);

/// Removes a file if it exists. Reports success when it did not exist.
[[nodiscard]] Status remove_file_if_present(const std::string& path);

}  // namespace airflow_control::detail
