#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "airflow_control/status.hpp"

namespace airflow_control::detail {

/// Bytes of the fixed file header at offset zero.
inline constexpr std::uint64_t kFileHeaderBytes = 512;
/// Bytes of the fixed head record that begins every slot.
inline constexpr std::uint64_t kHeadRecordBytes = 128;
/// Slots in the file. Fixed by the format: one holds the current generation
/// while the other is being replaced.
inline constexpr unsigned kSlotCount = 2;
/// Exit code used when a test terminates the process at an armed crash point.
inline constexpr int kCrashExitCode = 97;

/// The decoded head record of one slot.
///
/// \c valid is false when the slot holds no committed generation: the head was
/// never written, or it was written but does not verify. The remaining fields
/// are then zero, so an unverified head cannot be mistaken for data.
struct SlotHead {
  std::uint64_t generation = 0;
  std::uint64_t payload_length = 0;
  std::uint32_t payload_crc32c = 0;
  std::uint64_t writer_incarnation = 0;
  std::uint64_t writer_process_id = 0;
  bool valid = false;
};

/// A dual-slot, crash-consistent container for opaque payload bytes.
///
/// Layout: a 512-byte file header, then slot 0, then slot 1, each slot being
/// \c slot_capacity_bytes long and beginning with a 128-byte head record.
///
/// The commit point is the head write: a generation is published when its head
/// record verifies, and the payload is written and flushed before the head that
/// describes it. A process that dies at any earlier moment leaves the previous
/// generation intact, because the bytes it was writing are not reachable from
/// any head that verifies.
///
/// The container moves bytes; it knows nothing about what they mean.
class StoreFile {
 public:
  StoreFile() = default;
  ~StoreFile();

  StoreFile(StoreFile&& other) noexcept;
  StoreFile& operator=(StoreFile&& other) noexcept;
  StoreFile(const StoreFile&) = delete;
  StoreFile& operator=(const StoreFile&) = delete;

  /// Creates a store file. An existing file is never replaced.
  [[nodiscard]] static Result<StoreFile> create(const std::string& canonical_path,
                                                std::uint64_t slot_capacity_bytes);

  /// Opens an existing store file. The file is verified before it is used.
  [[nodiscard]] static Result<StoreFile> open(const std::string& canonical_path);

  /// The head of one slot. A slot that was never written, or whose head does
  /// not verify, reads as \c valid == false rather than as an error.
  [[nodiscard]] Result<SlotHead> read_head(unsigned slot) const;

  /// The payload described by a head, verified against its checksum.
  [[nodiscard]] Result<std::vector<std::uint8_t>> read_payload(const SlotHead& head) const;

  /// Publishes a generation into a slot.
  ///
  /// The payload is written and flushed before the head record that makes it
  /// reachable, so a crash at any stage leaves either the previous generation or
  /// this one, never a mixture.
  Status write_slot(unsigned slot, std::uint64_t generation,
                    const std::vector<std::uint8_t>& payload, std::uint64_t writer_incarnation);

  /// Forces everything written so far to stable storage.
  Status flush();

  [[nodiscard]] Result<std::uint64_t> slot_capacity() const;
  [[nodiscard]] Result<std::uint64_t> file_size() const;

 private:
  struct Impl;
  Impl* impl_ = nullptr;
};

}  // namespace airflow_control::detail
