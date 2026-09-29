#include "detail/file_io.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

#include <climits>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace airflow_control::detail {
namespace {

Status not_open_failure() {
  return Status::failure(StatusCode::not_open, "file handle is not open");
}

#ifdef _WIN32

using NativeHandle = HANDLE;

constexpr std::size_t kMaxTransferBytes = static_cast<std::size_t>(0xFFFFFFFFull);

NativeHandle invalid_handle() noexcept { return INVALID_HANDLE_VALUE; }

/// UTF-8 to UTF-16. The file API is the only thing that ever sees the wide
/// form, so the conversion is local to this translation unit.
Result<std::wstring> widen_utf8(const std::string& path) {
  if (path.empty()) {
    return Status::failure(StatusCode::path_invalid, "path is empty");
  }
  if (path.find('\0') != std::string::npos) {
    return Status::failure(StatusCode::path_invalid, "path contains a NUL byte");
  }
  if (path.size() > static_cast<std::size_t>(INT_MAX)) {
    return Status::failure(StatusCode::path_invalid, "path is too long to convert");
  }
  const int length = static_cast<int>(path.size());
  const int needed =
      MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.data(), length, nullptr, 0);
  if (needed <= 0) {
    return Status::failure(StatusCode::path_invalid, "path is not valid UTF-8: " + path);
  }
  std::wstring wide(static_cast<std::size_t>(needed), L'\0');
  const int written = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.data(), length,
                                          wide.data(), needed);
  if (written != needed) {
    return Status::failure(StatusCode::path_invalid, "path is not valid UTF-8: " + path);
  }
  return wide;
}

std::string win32_detail(DWORD error) {
  return " (win32 " + std::to_string(static_cast<unsigned long>(error)) + ")";
}

Status io_failure(const char* what, const std::string& path, DWORD error) {
  return Status::failure(StatusCode::store_io_error,
                         std::string("could not ") + what + " " + path + win32_detail(error));
}

enum class OpenKind { existing, create_new, open_or_create };

Result<NativeHandle> open_store_handle(const std::string& path, OpenKind kind) {
  Result<std::wstring> wide = widen_utf8(path);
  if (!wide.ok()) {
    return wide.status();
  }
  DWORD disposition = OPEN_EXISTING;
  if (kind == OpenKind::create_new) {
    disposition = CREATE_NEW;
  } else if (kind == OpenKind::open_or_create) {
    disposition = OPEN_ALWAYS;
  }
  // FILE_FLAG_RANDOM_ACCESS tells the cache manager how the file is used: every
  // access names its own offset, so sequential read ahead is wasted work.
  HANDLE handle =
      CreateFileW(wide.value().c_str(), GENERIC_READ | GENERIC_WRITE,
                  FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, disposition,
                  FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS, nullptr);
  if (handle != INVALID_HANDLE_VALUE) {
    return handle;
  }
  const DWORD error = GetLastError();
  if (kind == OpenKind::create_new &&
      (error == ERROR_FILE_EXISTS || error == ERROR_ALREADY_EXISTS)) {
    return Status::failure(StatusCode::duplicate_identity, "store file already exists: " + path);
  }
  if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) {
    return Status::failure(StatusCode::store_missing,
                           "store file does not exist: " + path + win32_detail(error));
  }
  return Status::failure(StatusCode::store_io_error,
                         "could not open store file " + path + win32_detail(error));
}

Status close_handle(NativeHandle handle) noexcept {
  if (handle == invalid_handle()) {
    return Status::success();
  }
  if (CloseHandle(handle) == 0) {
    return Status::failure(StatusCode::store_io_error, "could not close file handle");
  }
  return Status::success();
}

Status read_from(NativeHandle handle, const std::string& path, std::uint64_t offset,
                 std::uint8_t* buffer, std::size_t length, std::size_t& bytes_read) {
  bytes_read = 0;
  if (length == 0) {
    return Status::success();
  }
  if (buffer == nullptr) {
    return Status::failure(StatusCode::invalid_argument, "read buffer is null");
  }
  if (offset > UINT64_MAX - length) {
    return Status::failure(StatusCode::out_of_range, "read offset overflows");
  }
  std::size_t total = 0;
  while (total < length) {
    // Every call carries its own offset, so the file pointer is never read and
    // the handle is never sought.
    const std::uint64_t position = offset + total;
    OVERLAPPED overlapped{};
    overlapped.Offset = static_cast<DWORD>(position & 0xFFFFFFFFull);
    overlapped.OffsetHigh = static_cast<DWORD>(position >> 32);
    const std::size_t remaining = length - total;
    const DWORD chunk =
        static_cast<DWORD>(remaining > kMaxTransferBytes ? kMaxTransferBytes : remaining);
    DWORD transferred = 0;
    if (ReadFile(handle, buffer + total, chunk, &transferred, &overlapped) == 0) {
      const DWORD error = GetLastError();
      if (error == ERROR_HANDLE_EOF) {
        break;
      }
      bytes_read = total;
      return io_failure("read", path, error);
    }
    total += transferred;
    if (transferred < chunk) {
      break;  // end of file: a short read is information for the caller
    }
  }
  bytes_read = total;
  return Status::success();
}

Status write_to(NativeHandle handle, const std::string& path, std::uint64_t offset,
                const std::uint8_t* buffer, std::size_t length) {
  if (length == 0) {
    return Status::success();
  }
  if (buffer == nullptr) {
    return Status::failure(StatusCode::invalid_argument, "write buffer is null");
  }
  if (offset > UINT64_MAX - length) {
    return Status::failure(StatusCode::out_of_range, "write offset overflows");
  }
  std::size_t total = 0;
  while (total < length) {
    const std::uint64_t position = offset + total;
    OVERLAPPED overlapped{};
    overlapped.Offset = static_cast<DWORD>(position & 0xFFFFFFFFull);
    overlapped.OffsetHigh = static_cast<DWORD>(position >> 32);
    const std::size_t remaining = length - total;
    const DWORD chunk =
        static_cast<DWORD>(remaining > kMaxTransferBytes ? kMaxTransferBytes : remaining);
    DWORD transferred = 0;
    if (WriteFile(handle, buffer + total, chunk, &transferred, &overlapped) == 0) {
      return io_failure("write", path, GetLastError());
    }
    if (transferred == 0) {
      return Status::failure(StatusCode::store_io_error,
                             "write made no progress on " + path);
    }
    total += transferred;
  }
  return Status::success();
}

Status flush_handle(NativeHandle handle, const std::string& path) {
  if (FlushFileBuffers(handle) == 0) {
    return io_failure("flush", path, GetLastError());
  }
  return Status::success();
}

Status truncate_handle(NativeHandle handle, const std::string& path, std::uint64_t size) {
  if (size > 0x7FFFFFFFFFFFFFFFull) {
    return Status::failure(StatusCode::out_of_range, "file size is out of range: " + path);
  }
  // SetEndOfFile cuts at the current file position, which is the one place a
  // position has to be established.
  LARGE_INTEGER target{};
  target.QuadPart = static_cast<LONGLONG>(size);
  if (SetFilePointerEx(handle, target, nullptr, FILE_BEGIN) == 0) {
    return io_failure("seek", path, GetLastError());
  }
  if (SetEndOfFile(handle) == 0) {
    return io_failure("truncate", path, GetLastError());
  }
  return Status::success();
}

Result<std::uint64_t> size_of_handle(NativeHandle handle, const std::string& path) {
  LARGE_INTEGER extent{};
  if (GetFileSizeEx(handle, &extent) == 0) {
    return io_failure("size", path, GetLastError());
  }
  return static_cast<std::uint64_t>(extent.QuadPart);
}

Status lock_handle(NativeHandle handle, const std::string& path) {
  // The lock covers the first byte of the lock file and is held for as long as
  // the handle is open; the operating system drops it when the process dies,
  // which is what makes it a writer fence rather than a convention.
  OVERLAPPED overlapped{};
  if (LockFileEx(handle, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0,
                 &overlapped) == 0) {
    const DWORD error = GetLastError();
    if (error == ERROR_LOCK_VIOLATION || error == ERROR_SHARING_VIOLATION) {
      return Status::failure(StatusCode::busy,
                             "store lock is held by another process: " + path);
    }
    return io_failure("lock", path, error);
  }
  return Status::success();
}

Status unlock_handle(NativeHandle handle, const std::string& path) {
  OVERLAPPED overlapped{};
  if (UnlockFileEx(handle, 0, 1, 0, &overlapped) == 0) {
    return io_failure("unlock", path, GetLastError());
  }
  return Status::success();
}

#else

using NativeHandle = int;

constexpr std::size_t kMaxTransferBytes = static_cast<std::size_t>(0x7FFFFFFFull);

NativeHandle invalid_handle() noexcept { return -1; }

Result<std::string> widen_utf8(const std::string& path) {
  if (path.empty()) {
    return Status::failure(StatusCode::path_invalid, "path is empty");
  }
  if (path.find('\0') != std::string::npos) {
    return Status::failure(StatusCode::path_invalid, "path contains a NUL byte");
  }
  return path;
}

std::string errno_detail(int error) { return " (errno " + std::to_string(error) + ")"; }

Status io_failure(const char* what, const std::string& path, int error) {
  return Status::failure(StatusCode::store_io_error,
                         std::string("could not ") + what + " " + path + errno_detail(error));
}

enum class OpenKind { existing, create_new, open_or_create };

Result<NativeHandle> open_store_handle(const std::string& path, OpenKind kind) {
  int flags = O_RDWR | O_CLOEXEC;
  if (kind == OpenKind::create_new) {
    flags |= O_CREAT | O_EXCL;
  } else if (kind == OpenKind::open_or_create) {
    flags |= O_CREAT;
  }
  const int descriptor = ::open(path.c_str(), flags, 0600);
  if (descriptor >= 0) {
    return descriptor;
  }
  const int error = errno;
  if (kind == OpenKind::create_new && error == EEXIST) {
    return Status::failure(StatusCode::duplicate_identity, "store file already exists: " + path);
  }
  if (error == ENOENT) {
    return Status::failure(StatusCode::store_missing,
                           "store file does not exist: " + path + errno_detail(error));
  }
  return Status::failure(StatusCode::store_io_error,
                         "could not open store file " + path + errno_detail(error));
}

Status close_handle(NativeHandle descriptor) noexcept {
  if (descriptor < 0) {
    return Status::success();
  }
  if (::close(descriptor) != 0) {
    return Status::failure(StatusCode::store_io_error, "could not close file descriptor");
  }
  return Status::success();
}

Status read_from(NativeHandle descriptor, const std::string& path, std::uint64_t offset,
                 std::uint8_t* buffer, std::size_t length, std::size_t& bytes_read) {
  bytes_read = 0;
  if (length == 0) {
    return Status::success();
  }
  if (buffer == nullptr) {
    return Status::failure(StatusCode::invalid_argument, "read buffer is null");
  }
  if (offset > static_cast<std::uint64_t>(INT64_MAX) ||
      offset + length > static_cast<std::uint64_t>(INT64_MAX)) {
    return Status::failure(StatusCode::out_of_range, "read offset overflows");
  }
  std::size_t total = 0;
  while (total < length) {
    const std::size_t remaining = length - total;
    const std::size_t chunk = remaining > kMaxTransferBytes ? kMaxTransferBytes : remaining;
    const ssize_t transferred =
        ::pread(descriptor, buffer + total, chunk, static_cast<off_t>(offset + total));
    if (transferred < 0) {
      if (errno == EINTR) {
        continue;
      }
      bytes_read = total;
      return io_failure("read", path, errno);
    }
    if (transferred == 0) {
      break;  // end of file
    }
    total += static_cast<std::size_t>(transferred);
  }
  bytes_read = total;
  return Status::success();
}

Status write_to(NativeHandle descriptor, const std::string& path, std::uint64_t offset,
                const std::uint8_t* buffer, std::size_t length) {
  if (length == 0) {
    return Status::success();
  }
  if (buffer == nullptr) {
    return Status::failure(StatusCode::invalid_argument, "write buffer is null");
  }
  if (offset > static_cast<std::uint64_t>(INT64_MAX) ||
      offset + length > static_cast<std::uint64_t>(INT64_MAX)) {
    return Status::failure(StatusCode::out_of_range, "write offset overflows");
  }
  std::size_t total = 0;
  while (total < length) {
    const std::size_t remaining = length - total;
    const std::size_t chunk = remaining > kMaxTransferBytes ? kMaxTransferBytes : remaining;
    const ssize_t transferred =
        ::pwrite(descriptor, buffer + total, chunk, static_cast<off_t>(offset + total));
    if (transferred < 0) {
      if (errno == EINTR) {
        continue;
      }
      return io_failure("write", path, errno);
    }
    if (transferred == 0) {
      return Status::failure(StatusCode::store_io_error, "write made no progress on " + path);
    }
    total += static_cast<std::size_t>(transferred);
  }
  return Status::success();
}

Status flush_handle(NativeHandle descriptor, const std::string& path) {
  if (::fsync(descriptor) != 0) {
    return io_failure("flush", path, errno);
  }
  return Status::success();
}

Status truncate_handle(NativeHandle descriptor, const std::string& path, std::uint64_t size) {
  if (size > static_cast<std::uint64_t>(INT64_MAX)) {
    return Status::failure(StatusCode::out_of_range, "file size is out of range: " + path);
  }
  if (::ftruncate(descriptor, static_cast<off_t>(size)) != 0) {
    return io_failure("truncate", path, errno);
  }
  return Status::success();
}

Result<std::uint64_t> size_of_handle(NativeHandle descriptor, const std::string& path) {
  struct stat information {};
  if (::fstat(descriptor, &information) != 0) {
    return io_failure("size", path, errno);
  }
  return static_cast<std::uint64_t>(information.st_size);
}

Status lock_handle(NativeHandle descriptor, const std::string& path) {
  if (::flock(descriptor, LOCK_EX | LOCK_NB) != 0) {
    const int error = errno;
    if (error == EWOULDBLOCK || error == EAGAIN) {
      return Status::failure(StatusCode::busy, "store lock is held by another process: " + path);
    }
    return io_failure("lock", path, error);
  }
  return Status::success();
}

Status unlock_handle(NativeHandle descriptor, const std::string& path) {
  if (::flock(descriptor, LOCK_UN) != 0) {
    return io_failure("unlock", path, errno);
  }
  return Status::success();
}

#endif  // _WIN32

}  // namespace

struct FileHandle::Impl {
  Impl(NativeHandle native, std::string name) : handle(native), path(std::move(name)) {}

  NativeHandle handle = invalid_handle();
  std::string path;
};

FileHandle::~FileHandle() {
  if (impl_ == nullptr) {
    return;
  }
  // A destructor reports nothing; the status of a close is only observable
  // through close().
  close_handle(impl_->handle);
  delete impl_;
  impl_ = nullptr;
}

FileHandle::FileHandle(FileHandle&& other) noexcept : impl_(other.impl_) {
  other.impl_ = nullptr;
}

FileHandle& FileHandle::operator=(FileHandle&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  if (impl_ != nullptr) {
    close_handle(impl_->handle);
    delete impl_;
  }
  impl_ = other.impl_;
  other.impl_ = nullptr;
  return *this;
}

Result<FileHandle> FileHandle::open_existing(const std::string& path) {
  Result<NativeHandle> handle = open_store_handle(path, OpenKind::existing);
  if (!handle.ok()) {
    return handle.status();
  }
  FileHandle file;
  file.impl_ = new Impl(handle.value(), path);
  return std::move(file);
}

Result<FileHandle> FileHandle::create_new(const std::string& path) {
  Result<NativeHandle> handle = open_store_handle(path, OpenKind::create_new);
  if (!handle.ok()) {
    return handle.status();
  }
  FileHandle file;
  file.impl_ = new Impl(handle.value(), path);
  return std::move(file);
}

Result<FileHandle> FileHandle::open_or_create(const std::string& path) {
  Result<NativeHandle> handle = open_store_handle(path, OpenKind::open_or_create);
  if (!handle.ok()) {
    return handle.status();
  }
  FileHandle file;
  file.impl_ = new Impl(handle.value(), path);
  return std::move(file);
}

bool FileHandle::valid() const noexcept {
  return impl_ != nullptr && impl_->handle != invalid_handle();
}

Status FileHandle::read_at(std::uint64_t offset, std::uint8_t* buffer, std::size_t length,
                           std::size_t& bytes_read) {
  bytes_read = 0;
  if (!valid()) {
    return not_open_failure();
  }
  return read_from(impl_->handle, impl_->path, offset, buffer, length, bytes_read);
}

Status FileHandle::write_at(std::uint64_t offset, const std::uint8_t* buffer, std::size_t length) {
  if (!valid()) {
    return not_open_failure();
  }
  return write_to(impl_->handle, impl_->path, offset, buffer, length);
}

Status FileHandle::flush() {
  if (!valid()) {
    return not_open_failure();
  }
  return flush_handle(impl_->handle, impl_->path);
}

Status FileHandle::truncate(std::uint64_t size) {
  if (!valid()) {
    return not_open_failure();
  }
  return truncate_handle(impl_->handle, impl_->path, size);
}

Result<std::uint64_t> FileHandle::size() const {
  if (!valid()) {
    return not_open_failure();
  }
  return size_of_handle(impl_->handle, impl_->path);
}

Status FileHandle::close() {
  if (impl_ == nullptr || impl_->handle == invalid_handle()) {
    return Status::success();
  }
  const NativeHandle handle = impl_->handle;
  impl_->handle = invalid_handle();
  return close_handle(handle);
}

struct FileLock::Impl {
  Impl(NativeHandle native, std::string name, bool is_locked)
      : handle(native), path(std::move(name)), locked(is_locked) {}

  NativeHandle handle = invalid_handle();
  std::string path;
  bool locked = false;
};

FileLock::~FileLock() {
  if (impl_ == nullptr) {
    return;
  }
  // Closing the handle releases the lock even when the explicit unlock fails,
  // so the fence is never left behind by a destructor.
  if (impl_->locked) {
    unlock_handle(impl_->handle, impl_->path);
  }
  close_handle(impl_->handle);
  delete impl_;
  impl_ = nullptr;
}

FileLock::FileLock(FileLock&& other) noexcept : impl_(other.impl_) {
  other.impl_ = nullptr;
}

FileLock& FileLock::operator=(FileLock&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  if (impl_ != nullptr) {
    if (impl_->locked) {
      unlock_handle(impl_->handle, impl_->path);
    }
    close_handle(impl_->handle);
    delete impl_;
  }
  impl_ = other.impl_;
  other.impl_ = nullptr;
  return *this;
}

Result<FileLock> FileLock::acquire(const std::string& lock_path) {
  Result<NativeHandle> handle = open_store_handle(lock_path, OpenKind::open_or_create);
  if (!handle.ok()) {
    return handle.status();
  }
  Status locked = lock_handle(handle.value(), lock_path);
  if (!locked.ok()) {
    close_handle(handle.value());
    return locked;
  }
  FileLock lock;
  lock.impl_ = new Impl(handle.value(), lock_path, true);
  return std::move(lock);
}

bool FileLock::held() const noexcept {
  return impl_ != nullptr && impl_->locked;
}

Status FileLock::release() {
  if (impl_ == nullptr) {
    return Status::success();
  }
  Status unlocked = Status::success();
  if (impl_->locked) {
    unlocked = unlock_handle(impl_->handle, impl_->path);
    impl_->locked = false;
  }
  const Status closed = close_handle(impl_->handle);
  impl_->handle = invalid_handle();
  if (!unlocked.ok()) {
    return unlocked;
  }
  return closed;
}

}  // namespace airflow_control::detail
