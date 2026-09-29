#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "airflow_control/ids.hpp"
#include "airflow_control/status.hpp"

namespace airflow_control {

/// How an existing store file should be treated.
enum class OpenMode : std::uint32_t {
  /// The store must already exist and verify.
  open_existing = 0,
  /// The store is opened if it verifies, and created if it does not exist.
  open_or_create = 1,
  /// The store is created; an existing file is refused rather than replaced.
  create_new = 2,
};

/// Default size reserved for one generation slot.
inline constexpr std::uint64_t kDefaultSlotCapacityBytes = 8ull * 1024ull * 1024ull;
/// Smallest slot capacity a store may be created with.
inline constexpr std::uint64_t kMinSlotCapacityBytes = 64ull * 1024ull;
/// Largest slot capacity a store may be created with.
inline constexpr std::uint64_t kMaxSlotCapacityBytes = 256ull * 1024ull * 1024ull;
/// Largest payload the format's length fields can describe.
inline constexpr std::uint64_t kMaxPayloadBytes = kMaxSlotCapacityBytes;

struct StoreOptions {
  /// Bytes reserved for one generation slot. Fixed at creation.
  std::uint64_t slot_capacity_bytes = kDefaultSlotCapacityBytes;
  /// When set, an adopted generation below min_generation is refused with
  /// rollback_detected instead of being adopted.
  bool fence_min_generation = false;
  StoreGeneration min_generation = StoreGeneration::from(0);
};

/// What the store did when it was opened, and what it currently holds.
struct StoreAudit {
  std::string path;
  std::uint64_t slot_capacity_bytes = 0;
  std::uint64_t payload_bytes = 0;
  std::uint32_t valid_head_records = 0;
  std::uint64_t superseded_generations = 0;
  bool rollback_observed = false;
  StoreGeneration generation = StoreGeneration::from(0);
  IncarnationId last_writer = IncarnationId::from(0);
  std::uint64_t publications = 0;
};

/// A crash-consistent, integrity-checked, dual-slot durable store.
///
/// The layout, the commit point, and the recovery rule are documented in
/// docs/artifact-format.md. The store knows nothing about the model it carries:
/// it moves opaque payload bytes and guarantees that a reader sees exactly one
/// complete generation.
///
/// Cross-process write authority is a real operating-system lock held for the
/// store's lifetime. The lock is acquired once at open, released at close, and
/// never reacquired on any call path.
class DurableStore {
 public:
  DurableStore();
  ~DurableStore();

  DurableStore(DurableStore&& other) noexcept;
  DurableStore& operator=(DurableStore&& other) noexcept;
  DurableStore(const DurableStore&) = delete;
  DurableStore& operator=(const DurableStore&) = delete;

  [[nodiscard]] static Result<DurableStore> open(const std::string& path, OpenMode mode,
                                                 const StoreOptions& options);

  [[nodiscard]] bool is_open() const noexcept;
  [[nodiscard]] bool owns_file() const noexcept;

  /// Releases the operating-system lock and closes the file. Safe to call more
  /// than once.
  Status close();

  [[nodiscard]] const std::string& canonical_path() const noexcept;
  [[nodiscard]] StoreGeneration generation() const noexcept;
  [[nodiscard]] const std::vector<std::uint8_t>& payload() const noexcept;
  [[nodiscard]] StoreAudit audit() const;

  /// Publishes a payload as the next generation.
  ///
  /// The commit point is the head write for the new slot. Everything before it
  /// is staging. The store re-reads both heads first: a head that advanced
  /// beyond the generation this instance adopted means another writer touched
  /// the file, and publication is refused with store_fenced rather than
  /// overwriting a generation this writer never saw.
  Status publish(const std::vector<std::uint8_t>& payload, IncarnationId writer);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace airflow_control
