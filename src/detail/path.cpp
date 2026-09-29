#include "detail/path.hpp"

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

#include <climits>
#include <utility>
#else
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace airflow_control::detail {
namespace {

Status path_failure(const std::string& message) {
  return Status::failure(StatusCode::path_invalid, message);
}

#ifdef _WIN32

// ---------------------------------------------------------------------------
// Windows: every path is converted once to UTF-16 and every structural decision
// is taken on the wide form, because that is the form the operating system
// compares and opens.
// ---------------------------------------------------------------------------

using Native = std::wstring;

constexpr wchar_t kSeparator = L'\\';

bool is_separator(wchar_t value) noexcept { return value == L'\\' || value == L'/'; }

Result<std::string> from_native(const Native& path) {
  if (path.size() > static_cast<std::size_t>(INT_MAX)) {
    return path_failure("path is too long to convert");
  }
  const int length = static_cast<int>(path.size());
  const int needed =
      WideCharToMultiByte(CP_UTF8, 0, path.data(), length, nullptr, 0, nullptr, nullptr);
  if (needed <= 0) {
    return Status::failure(StatusCode::internal_error, "path cannot be encoded as UTF-8");
  }
  std::string narrow(static_cast<std::size_t>(needed), '\0');
  const int written =
      WideCharToMultiByte(CP_UTF8, 0, path.data(), length, narrow.data(), needed, nullptr, nullptr);
  if (written != needed) {
    return Status::failure(StatusCode::internal_error, "path cannot be encoded as UTF-8");
  }
  return narrow;
}

Result<Native> to_native(const std::string& path) {
  if (path.empty()) {
    return path_failure("path is empty");
  }
  if (path.find('\0') != std::string::npos) {
    return path_failure("path contains a NUL byte");
  }
  if (path.size() > static_cast<std::size_t>(INT_MAX)) {
    return path_failure("path is too long to convert");
  }
  const int length = static_cast<int>(path.size());
  const int needed =
      MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.data(), length, nullptr, 0);
  if (needed <= 0) {
    return path_failure("path is not valid UTF-8: " + path);
  }
  Native wide(static_cast<std::size_t>(needed), L'\0');
  const int written = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.data(), length,
                                          wide.data(), needed);
  if (written != needed) {
    return path_failure("path is not valid UTF-8: " + path);
  }
  return wide;
}

/// A path rendered for a diagnostic. The narrow form is only ever a message.
std::string describe(const Native& path) {
  Result<std::string> narrow = from_native(path);
  return narrow.ok() ? narrow.value() : std::string("<unprintable path>");
}

Status win32_failure(const char* what, const Native& path, DWORD error) {
  return path_failure(std::string(what) + " failed for " + describe(path) + " (win32 " +
                      std::to_string(static_cast<unsigned long>(error)) + ")");
}

/// Case-insensitive comparison against an ASCII literal, without a locale.
bool ascii_equals(const Native& value, std::size_t length, const wchar_t* expected) noexcept {
  for (std::size_t index = 0; index < length; ++index) {
    if (expected[index] == L'\0') {
      return false;
    }
    wchar_t left = value[index];
    wchar_t right = expected[index];
    if (left >= L'A' && left <= L'Z') {
      left = static_cast<wchar_t>(left - L'A' + L'a');
    }
    if (right >= L'A' && right <= L'Z') {
      right = static_cast<wchar_t>(right - L'A' + L'a');
    }
    if (left != right) {
      return false;
    }
  }
  return expected[length] == L'\0';
}

/// True for a name Win32 resolves to a device rather than to a file.
///
/// A store named after a device is not a file at all: it cannot be locked, and
/// it is the same object for every directory. Trailing dots and spaces are
/// ignored by the Win32 path parser, and "NUL.txt" is the device too, so both
/// the trimmed name and its stem are compared.
bool is_reserved_device_name(const Native& component) noexcept {
  static const wchar_t* const kReserved[] = {
      L"CON",  L"PRN",  L"AUX",  L"NUL",  L"COM1", L"COM2", L"COM3", L"COM4", L"COM5",
      L"COM6", L"COM7", L"COM8", L"COM9", L"LPT1", L"LPT2", L"LPT3", L"LPT4", L"LPT5",
      L"LPT6", L"LPT7", L"LPT8", L"LPT9"};
  std::size_t end = component.size();
  while (end > 0 && (component[end - 1] == L'.' || component[end - 1] == L' ')) {
    --end;
  }
  std::size_t stem_end = end;
  for (std::size_t index = 0; index < end; ++index) {
    if (component[index] == L'.') {
      stem_end = index;
      break;
    }
  }
  for (const wchar_t* reserved : kReserved) {
    if (ascii_equals(component, end, reserved) || ascii_equals(component, stem_end, reserved)) {
      return true;
    }
  }
  return false;
}

Status validate_structure(const Native& path) {
  // ':' introduces an alternate data stream, which is a different object from
  // the store file and would not be fenced by the store's lock. Only the drive
  // letter position may carry one.
  for (std::size_t index = 0; index < path.size(); ++index) {
    if (path[index] == L':' && index != 1) {
      return path_failure("path contains an alternate data stream separator: " + describe(path));
    }
  }
  if (is_separator(path.back())) {
    return path_failure("path ends with a directory separator: " + describe(path));
  }
  const std::size_t cut = path.find_last_of(L"\\/");
  const Native component = cut == Native::npos ? path : path.substr(cut + 1);
  if (component == L"." || component == L"..") {
    return path_failure("path ends with a traversal component: " + describe(path));
  }
  if (is_reserved_device_name(component)) {
    return path_failure("path names a reserved DOS device: " + describe(path));
  }
  return Status::success();
}

/// The device form "\\?\C:\x" carries no information a caller can use, and
/// its UNC spelling "\\?\UNC\server\share" has to lose the "UNC\" to name
/// the same share the caller named.
Native strip_device_prefix(Native path) {
  constexpr std::wstring_view kDevicePrefix = L"\\\\?\\";
  if (path.compare(0, kDevicePrefix.size(), kDevicePrefix) != 0) {
    return path;
  }
  path.erase(0, kDevicePrefix.size());
  constexpr std::wstring_view kUncPrefix = L"UNC\\";
  if (path.compare(0, kUncPrefix.size(), kUncPrefix) == 0) {
    path.erase(0, kUncPrefix.size());
    path.insert(0, L"\\\\");
  }
  return path;
}

Result<Native> resolve_handle(HANDLE handle, const Native& path) {
  const DWORD needed = GetFinalPathNameByHandleW(handle, nullptr, 0, FILE_NAME_NORMALIZED);
  if (needed == 0) {
    return win32_failure("resolving", path, GetLastError());
  }
  Native resolved(static_cast<std::size_t>(needed), L'\0');
  const DWORD written =
      GetFinalPathNameByHandleW(handle, resolved.data(), needed, FILE_NAME_NORMALIZED);
  if (written == 0 || written >= needed) {
    return win32_failure("resolving", path, GetLastError());
  }
  resolved.resize(static_cast<std::size_t>(written));
  return strip_device_prefix(std::move(resolved));
}

struct Probe {
  bool exists = false;
  bool is_directory = false;
  bool is_regular_file = false;
  Native resolved;
};

/// Asks the file system what a path really is.
///
/// The handle is opened without FILE_FLAG_OPEN_REPARSE_POINT, so a junction or
/// a symbolic link is followed and the reported attributes and resolved name
/// describe the object the link names.
Result<Probe> probe_path(const Native& path) {
  HANDLE handle = CreateFileW(path.c_str(), FILE_READ_ATTRIBUTES,
                              FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
                              OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    const DWORD error = GetLastError();
    if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND ||
        error == ERROR_INVALID_NAME || error == ERROR_BAD_PATHNAME) {
      // Absent is not the same as broken: the caller may be naming the file it
      // is about to create, and a stripped UNC root reports itself this way.
      return Probe{};
    }
    return win32_failure("inspecting", path, error);
  }
  BY_HANDLE_FILE_INFORMATION information{};
  if (GetFileInformationByHandle(handle, &information) == 0) {
    const DWORD error = GetLastError();
    CloseHandle(handle);
    return win32_failure("inspecting", path, error);
  }
  Result<Native> resolved = resolve_handle(handle, path);
  CloseHandle(handle);
  if (!resolved.ok()) {
    return resolved.status();
  }
  Probe probe;
  probe.exists = true;
  probe.is_directory = (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
  probe.is_regular_file = !probe.is_directory;
  probe.resolved = std::move(resolved).value();
  return probe;
}

/// Absolute form with separators normalised and "." and ".." resolved. The
/// result may still contain reparse points; that is what probe_path() is for.
Result<Native> absolute_path(const Native& path) {
  const DWORD needed = GetFullPathNameW(path.c_str(), 0, nullptr, nullptr);
  if (needed == 0) {
    return win32_failure("resolving", path, GetLastError());
  }
  Native absolute(static_cast<std::size_t>(needed), L'\0');
  const DWORD written = GetFullPathNameW(path.c_str(), needed, absolute.data(), nullptr);
  if (written == 0 || written >= needed) {
    return win32_failure("resolving", path, GetLastError());
  }
  absolute.resize(static_cast<std::size_t>(written));
  return absolute;
}

Native join(const Native& prefix, const Native& tail) {
  if (tail.empty()) {
    return prefix;
  }
  Native combined = prefix;
  if (combined.empty() || !is_separator(combined.back())) {
    combined.push_back(kSeparator);
  }
  combined += tail;
  return combined;
}

/// Removes the last component of an absolute path and reports what it was.
///
/// Returns false when only a root is left, which is what ends the walk: a root
/// cannot be shortened without naming a different object ("C:" is the current
/// directory on a drive, not the drive).
bool strip_last_component(Native& path, Native& component) {
  const std::size_t cut = path.find_last_of(L"\\/");
  if (cut == Native::npos || cut + 1 >= path.size()) {
    return false;
  }
  component = path.substr(cut + 1);
  const bool drive_root = cut == 2 && path[1] == L':';
  path.erase(drive_root ? cut + 1 : cut);
  return true;
}

/// The parent of a path, or an empty path when there is none to create.
Result<Native> parent_directory(const Native& path) {
  const std::size_t cut = path.find_last_of(L"\\/");
  if (cut == Native::npos) {
    return Native();
  }
  Native parent = path.substr(0, cut);
  if (parent.size() == 2 && parent[1] == L':') {
    parent.push_back(kSeparator);
  }
  return parent;
}

Status require_directory(const Probe& probe, const Native& path) {
  if (!probe.exists || !probe.is_directory) {
    return path_failure("path exists and is not a directory: " + describe(path));
  }
  return Status::success();
}

struct ExistingPrefix {
  Native resolved;
  Native tail;
  bool is_directory = false;
};

/// The deepest ancestor that exists, resolved through every reparse point, and
/// the components below it that do not exist yet.
Result<ExistingPrefix> deepest_existing(const Native& absolute) {
  Native candidate = absolute;
  Native tail;
  for (;;) {
    Result<Probe> probe = probe_path(candidate);
    if (!probe.ok()) {
      return probe.status();
    }
    if (probe.value().exists) {
      return ExistingPrefix{probe.value().resolved, tail, probe.value().is_directory};
    }
    Native component;
    if (!strip_last_component(candidate, component)) {
      return path_failure("path has no existing ancestor: " + describe(absolute));
    }
    tail = tail.empty() ? component : component + kSeparator + tail;
  }
}

Status create_directory_chain(const Native& parent) {
  Native candidate = parent;
  std::vector<Native> missing;
  Native resolved;
  for (;;) {
    Result<Probe> probe = probe_path(candidate);
    if (!probe.ok()) {
      return probe.status();
    }
    if (probe.value().exists) {
      Status directory = require_directory(probe.value(), candidate);
      if (!directory.ok()) {
        return directory;
      }
      resolved = probe.value().resolved;
      break;
    }
    Native component;
    if (!strip_last_component(candidate, component)) {
      return path_failure("parent path has no existing ancestor: " + describe(parent));
    }
    missing.push_back(component);
  }
  for (auto component = missing.rbegin(); component != missing.rend(); ++component) {
    resolved = join(resolved, *component);
    if (CreateDirectoryW(resolved.c_str(), nullptr) != 0) {
      continue;
    }
    const DWORD error = GetLastError();
    if (error != ERROR_ALREADY_EXISTS) {
      return Status::failure(StatusCode::store_io_error,
                             "could not create directory " + describe(resolved) + " (win32 " +
                                 std::to_string(static_cast<unsigned long>(error)) + ")");
    }
    // Another process created it between the probe and the call; it still has
    // to be a directory.
    Result<Probe> made = probe_path(resolved);
    if (!made.ok()) {
      return made.status();
    }
    Status directory = require_directory(made.value(), resolved);
    if (!directory.ok()) {
      return directory;
    }
  }
  return Status::success();
}

#else

// ---------------------------------------------------------------------------
// POSIX: the same contract, expressed with realpath(3), stat(2), mkdir(2), and
// unlink(2).
// ---------------------------------------------------------------------------

using Native = std::string;

constexpr char kSeparator = '/';

bool is_separator(char value) noexcept { return value == '/'; }

Result<std::string> from_native(const Native& path) { return path; }

Result<Native> to_native(const std::string& path) {
  if (path.empty()) {
    return path_failure("path is empty");
  }
  if (path.find('\0') != std::string::npos) {
    return path_failure("path contains a NUL byte");
  }
  return path;
}

std::string describe(const Native& path) { return path; }

Status errno_failure(const char* what, const Native& path, int error) {
  return Status::failure(StatusCode::path_invalid, std::string(what) + " failed for " + path +
                                                       " (errno " + std::to_string(error) + ")");
}

Status validate_structure(const Native& path) {
  if (is_separator(path.back())) {
    return path_failure("path ends with a directory separator: " + path);
  }
  const std::size_t cut = path.find_last_of('/');
  const Native component = cut == Native::npos ? path : path.substr(cut + 1);
  if (component == "." || component == "..") {
    return path_failure("path ends with a traversal component: " + path);
  }
  return Status::success();
}

struct Probe {
  bool exists = false;
  bool is_directory = false;
  bool is_regular_file = false;
  Native resolved;
};

Result<Probe> probe_path(const Native& path) {
  struct stat information {};
  if (::stat(path.c_str(), &information) != 0) {
    const int error = errno;
    if (error == ENOENT) {
      return Probe{};
    }
    if (error == ENOTDIR) {
      // A component of the path is a file, so nothing below it can exist.
      return path_failure("a path component is not a directory: " + path);
    }
    return errno_failure("inspecting", path, error);
  }
  Probe probe;
  probe.exists = true;
  probe.is_directory = S_ISDIR(information.st_mode) != 0;
  probe.is_regular_file = S_ISREG(information.st_mode) != 0;
  char* resolved = ::realpath(path.c_str(), nullptr);
  if (resolved == nullptr) {
    return errno_failure("resolving", path, errno);
  }
  probe.resolved = resolved;
  std::free(resolved);
  return probe;
}

/// Makes the path absolute. The existing prefix is resolved through realpath()
/// by probe_path(), so only the relative form has to be anchored here.
Result<Native> absolute_path(const Native& path) {
  if (!path.empty() && is_separator(path.front())) {
    return path;
  }
  char buffer[PATH_MAX];
  if (::getcwd(buffer, sizeof(buffer)) == nullptr) {
    return errno_failure("getcwd", path, errno);
  }
  return std::string(buffer) + kSeparator + path;
}

Native join(const Native& prefix, const Native& tail) {
  if (tail.empty()) {
    return prefix;
  }
  Native combined = prefix;
  if (combined.empty() || !is_separator(combined.back())) {
    combined.push_back(kSeparator);
  }
  combined += tail;
  return combined;
}

/// Removes the last component of an absolute path and reports what it was.
/// Returns false when only the root is left, which ends the walk.
bool strip_last_component(Native& path, Native& component) {
  const std::size_t cut = path.find_last_of('/');
  if (cut == Native::npos || cut + 1 >= path.size()) {
    return false;
  }
  component = path.substr(cut + 1);
  path.erase(cut == 0 ? cut + 1 : cut);
  return true;
}

Result<Native> parent_directory(const Native& path) {
  const std::size_t cut = path.find_last_of('/');
  if (cut == Native::npos || cut == 0) {
    return Native();
  }
  return path.substr(0, cut);
}

Status require_directory(const Probe& probe, const Native& path) {
  if (!probe.exists || !probe.is_directory) {
    return path_failure("path exists and is not a directory: " + path);
  }
  return Status::success();
}

struct ExistingPrefix {
  Native resolved;
  Native tail;
  bool is_directory = false;
};

Result<ExistingPrefix> deepest_existing(const Native& absolute) {
  Native candidate = absolute;
  Native tail;
  for (;;) {
    Result<Probe> probe = probe_path(candidate);
    if (!probe.ok()) {
      return probe.status();
    }
    if (probe.value().exists) {
      return ExistingPrefix{probe.value().resolved, tail, probe.value().is_directory};
    }
    Native component;
    if (!strip_last_component(candidate, component)) {
      return path_failure("path has no existing ancestor: " + absolute);
    }
    tail = tail.empty() ? component : component + kSeparator + tail;
  }
}

Status create_directory_chain(const Native& parent) {
  Native candidate = parent;
  std::vector<Native> missing;
  Native resolved;
  for (;;) {
    Result<Probe> probe = probe_path(candidate);
    if (!probe.ok()) {
      return probe.status();
    }
    if (probe.value().exists) {
      Status directory = require_directory(probe.value(), candidate);
      if (!directory.ok()) {
        return directory;
      }
      resolved = probe.value().resolved;
      break;
    }
    Native component;
    if (!strip_last_component(candidate, component)) {
      return path_failure("parent path has no existing ancestor: " + parent);
    }
    missing.push_back(component);
  }
  for (auto component = missing.rbegin(); component != missing.rend(); ++component) {
    resolved = join(resolved, *component);
    if (::mkdir(resolved.c_str(), 0777) == 0) {
      continue;
    }
    const int error = errno;
    if (error != EEXIST) {
      return Status::failure(StatusCode::store_io_error, "could not create directory " + resolved +
                                                             " (errno " + std::to_string(error) +
                                                             ")");
    }
    Result<Probe> made = probe_path(resolved);
    if (!made.ok()) {
      return made.status();
    }
    Status directory = require_directory(made.value(), resolved);
    if (!directory.ok()) {
      return directory;
    }
  }
  return Status::success();
}

#endif  // _WIN32

}  // namespace

Result<std::string> canonical_store_path(const std::string& path) {
  Result<Native> native = to_native(path);
  if (!native.ok()) {
    return native.status();
  }
  Status structure = validate_structure(native.value());
  if (!structure.ok()) {
    return structure;
  }
  Result<Native> absolute = absolute_path(native.value());
  if (!absolute.ok()) {
    return absolute.status();
  }
  Result<Probe> leaf = probe_path(absolute.value());
  if (!leaf.ok()) {
    return leaf.status();
  }
  if (leaf.value().exists) {
    if (leaf.value().is_directory) {
      return path_failure("path names a directory: " + path);
    }
    return from_native(leaf.value().resolved);
  }
  // The leaf does not exist yet. Its canonical form is the fully resolved
  // deepest existing ancestor with the missing components appended, so two
  // processes naming the same store through different spellings still agree on
  // the lock file even before the store is created.
  Result<ExistingPrefix> prefix = deepest_existing(absolute.value());
  if (!prefix.ok()) {
    return prefix.status();
  }
  if (!prefix.value().is_directory) {
    // Something that is not a directory would have to be traversed.
    return path_failure("a path component is not a directory: " + path);
  }
  return from_native(join(prefix.value().resolved, prefix.value().tail));
}

std::string lock_path_for(const std::string& canonical_path) {
  return canonical_path + ".lock";
}

bool regular_file_exists(const std::string& canonical_path) {
  Result<Native> native = to_native(canonical_path);
  if (!native.ok()) {
    return false;
  }
  Result<Probe> probe = probe_path(native.value());
  if (!probe.ok() || !probe.value().exists) {
    return false;
  }
  return probe.value().is_regular_file;
}

Status ensure_parent_directory(const std::string& canonical_path) {
  Result<Native> native = to_native(canonical_path);
  if (!native.ok()) {
    return native.status();
  }
  Result<Native> parent = parent_directory(native.value());
  if (!parent.ok()) {
    return parent.status();
  }
  if (parent.value().empty()) {
    return Status::success();
  }
  Result<Probe> existing = probe_path(parent.value());
  if (!existing.ok()) {
    return existing.status();
  }
  if (existing.value().exists) {
    return require_directory(existing.value(), parent.value());
  }
  return create_directory_chain(parent.value());
}

Status remove_file_if_present(const std::string& path) {
  Result<Native> native = to_native(path);
  if (!native.ok()) {
    return native.status();
  }
#ifdef _WIN32
  if (DeleteFileW(native.value().c_str()) != 0) {
    return Status::success();
  }
  const DWORD error = GetLastError();
  if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) {
    return Status::success();
  }
  return Status::failure(StatusCode::store_io_error,
                         "could not remove file " + path + " (win32 " +
                             std::to_string(static_cast<unsigned long>(error)) + ")");
#else
  if (::unlink(native.value().c_str()) == 0) {
    return Status::success();
  }
  const int error = errno;
  if (error == ENOENT) {
    return Status::success();
  }
  return Status::failure(StatusCode::store_io_error, "could not remove file " + path +
                                                         " (errno " + std::to_string(error) +
                                                         ")");
#endif
}

}  // namespace airflow_control::detail
