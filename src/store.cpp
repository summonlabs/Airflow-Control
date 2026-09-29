#include "airflow_control/store.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "detail/crc32c.hpp"
#include "detail/file_io.hpp"
#include "detail/path.hpp"
#include "detail/process.hpp"
#include "detail/store_file.hpp"

namespace airflow_control {
namespace {

/// True when the head area of a slot holds any non-zero byte.
///
/// A slot whose head area is entirely zero was never written. A slot whose head
/// area holds bytes that do not verify is a torn publication, which is exactly
/// the signature of a process that died between writing the staging payload and
/// committing its head.
[[nodiscard]] bool slot_has_bytes(const std::string& canonical_path, std::uint64_t offset,
                                  std::uint64_t slot_capacity) {
  Result<detail::FileHandle> handle = detail::FileHandle::open_existing(canonical_path);
  if (!handle.ok()) {
    return false;
  }
  std::vector<std::uint8_t> buffer(detail::kHeadRecordBytes, 0);
  std::size_t read = 0;
  Status status = handle.value().read_at(offset, buffer.data(), buffer.size(), read);
  if (!status.ok()) {
    return false;
  }
  (void)slot_capacity;
  for (std::size_t index = 0; index < read; ++index) {
    if (buffer[index] != 0) {
      return true;
    }
  }
  return false;
}

}  // namespace

struct DurableStore::Impl {
  StoreOptions options;
  std::string canonical_path;
  detail::FileLock lock;
  detail::StoreFile file;
  std::vector<std::uint8_t> payload;
  StoreGeneration generation = StoreGeneration::from(0);
  IncarnationId last_writer = IncarnationId::from(0);
  std::uint64_t slot_capacity = 0;
  std::uint32_t valid_heads = 0;
  bool rollback_observed = false;
  bool open = false;
  bool owns_file = false;
  std::uint64_t publications = 0;
  std::uint64_t superseded_generations = 0;
  unsigned current_slot = 0;

  [[nodiscard]] std::uint64_t slot_offset(unsigned slot) const {
    return detail::kFileHeaderBytes + static_cast<std::uint64_t>(slot) * slot_capacity;
  }
};

DurableStore::DurableStore() = default;
DurableStore::~DurableStore() = default;
DurableStore::DurableStore(DurableStore&& other) noexcept = default;
DurableStore& DurableStore::operator=(DurableStore&& other) noexcept = default;

Result<DurableStore> DurableStore::open(const std::string& path, OpenMode mode,
                                        const StoreOptions& options) {
  if (options.slot_capacity_bytes < kMinSlotCapacityBytes ||
      options.slot_capacity_bytes > kMaxSlotCapacityBytes) {
    return Status::failure(StatusCode::out_of_range,
                           "slot capacity " + std::to_string(options.slot_capacity_bytes) +
                               " is outside the permitted range " +
                               std::to_string(kMinSlotCapacityBytes) + " .. " +
                               std::to_string(kMaxSlotCapacityBytes));
  }
  Result<std::string> canonical = detail::canonical_store_path(path);
  if (!canonical.ok()) {
    return canonical.status();
  }
  Status parent = detail::ensure_parent_directory(canonical.value());
  if (!parent.ok()) {
    return parent;
  }

  auto impl = std::make_unique<Impl>();
  impl->options = options;
  impl->canonical_path = canonical.value();

  // The lock is taken before the file is touched, so two processes racing to
  // create the same store cannot both believe they created it.
  Result<detail::FileLock> lock =
      detail::FileLock::acquire(detail::lock_path_for(impl->canonical_path));
  if (!lock.ok()) {
    return lock.status();
  }
  impl->lock = std::move(lock).value();

  const bool exists = detail::regular_file_exists(impl->canonical_path);
  switch (mode) {
    case OpenMode::open_existing:
      if (!exists) {
        return Status::failure(StatusCode::store_missing,
                               "store " + impl->canonical_path + " does not exist");
      }
      break;
    case OpenMode::create_new:
      if (exists) {
        return Status::failure(StatusCode::duplicate_identity,
                               "store " + impl->canonical_path + " already exists");
      }
      break;
    case OpenMode::open_or_create:
      break;
  }

  if (!exists) {
    Result<detail::StoreFile> created =
        detail::StoreFile::create(impl->canonical_path, options.slot_capacity_bytes);
    if (!created.ok()) {
      return created.status();
    }
    impl->file = std::move(created).value();
    impl->owns_file = true;
    impl->slot_capacity = options.slot_capacity_bytes;
  } else {
    Result<detail::StoreFile> opened = detail::StoreFile::open(impl->canonical_path);
    if (!opened.ok()) {
      return opened.status();
    }
    impl->file = std::move(opened).value();
    Result<std::uint64_t> capacity = impl->file.slot_capacity();
    if (!capacity.ok()) {
      return capacity.status();
    }
    impl->slot_capacity = capacity.value();
  }

  // Adopt the valid head with the largest generation. Generations are never
  // stitched together: a slot is adopted whole or not at all.
  std::optional<detail::SlotHead> adopted;
  unsigned adopted_slot = 0;
  for (unsigned slot = 0; slot < detail::kSlotCount; ++slot) {
    Result<detail::SlotHead> head = impl->file.read_head(slot);
    if (!head.ok()) {
      return head.status();
    }
    if (!head.value().valid) {
      continue;
    }
    Result<std::vector<std::uint8_t>> candidate = impl->file.read_payload(head.value());
    if (!candidate.ok()) {
      // A head that verifies but whose payload does not is corruption, not a
      // stale generation: refusing is the only honest answer.
      return candidate.status();
    }
    ++impl->valid_heads;
    if (!adopted.has_value() || head.value().generation > adopted->generation) {
      adopted = head.value();
      adopted_slot = slot;
      impl->payload = std::move(candidate).value();
    }
  }

  if (!adopted.has_value()) {
    // No slot verifies. For a file that was just created that is correct; for
    // an existing file it means there is no complete generation to adopt.
    bool any_bytes = false;
    for (unsigned slot = 0; slot < detail::kSlotCount && !any_bytes; ++slot) {
      any_bytes = slot_has_bytes(impl->canonical_path, impl->slot_offset(slot), impl->slot_capacity);
    }
    if (any_bytes) {
      return Status::failure(StatusCode::store_corrupt,
                             "store " + impl->canonical_path +
                                 " holds no complete generation; refusing rather than repairing");
    }
  } else {
    impl->generation = StoreGeneration::from(adopted->generation);
    impl->last_writer = IncarnationId::from(adopted->writer_incarnation);
    impl->current_slot = adopted_slot;
    if (impl->valid_heads == 1) {
      const unsigned other = (adopted_slot + 1u) % detail::kSlotCount;
      if (slot_has_bytes(impl->canonical_path, impl->slot_offset(other), impl->slot_capacity)) {
        // The other slot holds bytes that do not verify. That is a publication
        // that did not commit, so this open rolled back to the previous whole
        // generation.
        impl->rollback_observed = true;
      }
    }
  }

  if (options.fence_min_generation && impl->generation < options.min_generation) {
    return Status::failure(StatusCode::rollback_detected,
                           "store generation " + impl->generation.to_string() +
                               " is below the required minimum " +
                               options.min_generation.to_string());
  }

  impl->open = true;
  DurableStore store;
  store.impl_ = std::move(impl);
  return store;
}

bool DurableStore::is_open() const noexcept { return impl_ != nullptr && impl_->open; }
bool DurableStore::owns_file() const noexcept { return impl_ != nullptr && impl_->owns_file; }

const std::string& DurableStore::canonical_path() const noexcept {
  static const std::string kEmpty;
  return impl_ == nullptr ? kEmpty : impl_->canonical_path;
}

StoreGeneration DurableStore::generation() const noexcept {
  return impl_ == nullptr ? StoreGeneration::from(0) : impl_->generation;
}

const std::vector<std::uint8_t>& DurableStore::payload() const noexcept {
  static const std::vector<std::uint8_t> kEmpty;
  return impl_ == nullptr ? kEmpty : impl_->payload;
}

StoreAudit DurableStore::audit() const {
  StoreAudit result;
  if (impl_ == nullptr) {
    return result;
  }
  result.path = impl_->canonical_path;
  result.slot_capacity_bytes = impl_->slot_capacity;
  result.payload_bytes = static_cast<std::uint64_t>(impl_->payload.size());
  result.valid_head_records = impl_->valid_heads;
  result.superseded_generations = impl_->superseded_generations;
  result.rollback_observed = impl_->rollback_observed;
  result.generation = impl_->generation;
  result.last_writer = impl_->last_writer;
  result.publications = impl_->publications;
  return result;
}

Status DurableStore::publish(const std::vector<std::uint8_t>& payload, IncarnationId writer) {
  if (impl_ == nullptr || !impl_->open) {
    return Status::failure(StatusCode::not_open, "the store is not open");
  }
  if (payload.size() > kMaxPayloadBytes) {
    return Status::failure(StatusCode::store_capacity_exceeded,
                           "payload of " + std::to_string(payload.size()) +
                               " bytes exceeds the format maximum");
  }

  // Re-read both heads first. A head that advanced beyond the generation this
  // writer adopted means another writer touched the file, and this publication
  // is fenced rather than allowed to overwrite a generation it never saw.
  std::optional<detail::SlotHead> newest;
  unsigned newest_slot = 0;
  std::uint32_t valid = 0;
  for (unsigned slot = 0; slot < detail::kSlotCount; ++slot) {
    Result<detail::SlotHead> head = impl_->file.read_head(slot);
    if (!head.ok()) {
      return head.status();
    }
    if (!head.value().valid) {
      continue;
    }
    ++valid;
    if (!newest.has_value() || head.value().generation > newest->generation) {
      newest = head.value();
      newest_slot = slot;
    }
  }
  const std::uint64_t observed = newest.has_value() ? newest->generation : 0;
  if (observed != impl_->generation.value()) {
    return Status::failure(StatusCode::store_fenced,
                           "the store advanced to generation " + std::to_string(observed) +
                               " outside this writer, which adopted generation " +
                               impl_->generation.to_string());
  }
  if (valid == 0 && impl_->generation.value() != 0) {
    return Status::failure(StatusCode::store_corrupt,
                           "every head became unreadable under this writer");
  }

  const unsigned target_slot = newest.has_value()
                                   ? (newest_slot + 1u) % detail::kSlotCount
                                   : 0u;
  const std::uint64_t next_generation = observed + 1;
  Status written = impl_->file.write_slot(target_slot, next_generation, payload, writer.value());
  if (!written.ok()) {
    return written;
  }
  Status flushed = impl_->file.flush();
  if (!flushed.ok()) {
    return flushed;
  }

  // Read back what was committed. Durability is claimed only after the store
  // can prove the committed generation is the one that was just written.
  Result<detail::SlotHead> verify_head = impl_->file.read_head(target_slot);
  if (!verify_head.ok()) {
    return verify_head.status();
  }
  if (!verify_head.value().valid || verify_head.value().generation != next_generation) {
    return Status::failure(StatusCode::store_io_error,
                           "the committed head does not describe the generation just written");
  }
  Result<std::vector<std::uint8_t>> verify_payload = impl_->file.read_payload(verify_head.value());
  if (!verify_payload.ok()) {
    return verify_payload.status();
  }
  if (verify_payload.value() != payload) {
    return Status::failure(StatusCode::store_io_error,
                           "the committed payload does not match what was published");
  }

  if (impl_->generation.value() != 0) {
    ++impl_->superseded_generations;
  }
  ++impl_->publications;
  impl_->generation = StoreGeneration::from(next_generation);
  impl_->last_writer = writer;
  impl_->current_slot = target_slot;
  impl_->valid_heads = std::max(impl_->valid_heads, 1u);
  // rollback_observed is deliberately NOT cleared here. A rollback is something
  // this opener detected, and a caller that asks the store what happened must
  // still be told after the writer has moved on; clearing it would make the
  // evidence disappear exactly when it became reachable.
  impl_->payload = payload;
  return Status::success();
}

Status DurableStore::close() {
  if (impl_ == nullptr) {
    return Status::success();
  }
  Status result = Status::success();
  if (impl_->file.slot_capacity().ok()) {
    Status closed_file = impl_->file.flush();
    if (!closed_file.ok()) {
      result = closed_file;
    }
  }
  impl_.reset();
  return result;
}

}  // namespace airflow_control
