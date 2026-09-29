#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "airflow_control/status.hpp"

namespace airflow_control::detail {

/// A positioned file handle. Handles are move-only and close on destruction.
class FileHandle {
 public:
  FileHandle() = default;
  ~FileHandle();

  FileHandle(FileHandle&& other) noexcept;
  FileHandle& operator=(FileHandle&& other) noexcept;
  FileHandle(const FileHandle&) = delete;
  FileHandle& operator=(const FileHandle&) = delete;

  static Result<FileHandle> open_existing(const std::string& path);
  static Result<FileHandle> create_new(const std::string& path);
  static Result<FileHandle> open_or_create(const std::string& path);

  [[nodiscard]] bool valid() const noexcept;

  Status read_at(std::uint64_t offset, std::uint8_t* buffer, std::size_t length,
                 std::size_t& bytes_read);
  Status write_at(std::uint64_t offset, const std::uint8_t* buffer, std::size_t length);
  Status flush();
  Status truncate(std::uint64_t size);
  [[nodiscard]] Result<std::uint64_t> size() const;
  Status close();

 private:
  struct Impl;
  Impl* impl_ = nullptr;
};

/// An exclusive, cross-process advisory lock backed by the operating system.
///
/// The lock is released by the operating system when the holding process dies,
/// which is what makes it usable as a writer fence.
class FileLock {
 public:
  FileLock() = default;
  ~FileLock();

  FileLock(FileLock&& other) noexcept;
  FileLock& operator=(FileLock&& other) noexcept;
  FileLock(const FileLock&) = delete;
  FileLock& operator=(const FileLock&) = delete;

  static Result<FileLock> acquire(const std::string& lock_path);

  [[nodiscard]] bool held() const noexcept;
  Status release();

 private:
  struct Impl;
  Impl* impl_ = nullptr;
};

}  // namespace airflow_control::detail
