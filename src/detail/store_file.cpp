#include "detail/store_file.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "detail/codec.hpp"
#include "detail/crash_point.hpp"
#include "detail/crc32c.hpp"
#include "detail/file_io.hpp"
#include "detail/process.hpp"

namespace airflow_control::detail {
namespace {

// The layout. Write offsets are implicit in the order the fields are appended
// below; read offsets are explicit, and the two are held together by the size
// check at the end of each builder.
constexpr char kFileMagic[8] = {'A', 'F', 'C', 'T', 'L', 'S', 'T', '1'};
constexpr char kHeadMagic[8] = {'A', 'F', 'C', 'L', 'H', 'E', 'A', 'D'};

constexpr std::uint32_t kFormatVersion = 1;
constexpr std::uint32_t kHeaderMarker = 0x41464331u;  // "AFC1"
constexpr std::uint32_t kHeadMarker = 0x41464831u;    // "AFH1"

constexpr std::size_t kHeaderVersionOffset = 8;
constexpr std::size_t kHeaderLengthOffset = 12;
constexpr std::size_t kHeaderSlotCapacityOffset = 16;
constexpr std::size_t kHeaderPayloadCapacityOffset = 24;
constexpr std::size_t kHeaderReservedOffset = 40;
constexpr std::size_t kHeaderCrcOffset = 504;
constexpr std::size_t kHeaderMarkerOffset = 508;

constexpr std::size_t kHeadGenerationOffset = 8;
constexpr std::size_t kHeadPayloadLengthOffset = 16;
constexpr std::size_t kHeadPayloadCrcOffset = 24;
constexpr std::size_t kHeadVersionOffset = 28;
constexpr std::size_t kHeadIncarnationOffset = 32;
constexpr std::size_t kHeadProcessIdOffset = 40;
constexpr std::size_t kHeadReservedOffset = 48;
constexpr std::size_t kHeadCrcOffset = 120;
constexpr std::size_t kHeadMarkerOffset = 124;

Status not_open_failure() {
  return Status::failure(StatusCode::not_open, "store file is not open");
}

Status corrupt_failure(const std::string& message) {
  return Status::failure(StatusCode::store_corrupt, message);
}

std::uint32_t load_u32(const std::uint8_t* data) noexcept {
  std::uint32_t value = 0;
  for (int shift = 0; shift < 32; shift += 8) {
    value |= static_cast<std::uint32_t>(data[shift / 8]) << shift;
  }
  return value;
}

std::uint64_t load_u64(const std::uint8_t* data) noexcept {
  std::uint64_t value = 0;
  for (int shift = 0; shift < 64; shift += 8) {
    value |= static_cast<std::uint64_t>(data[shift / 8]) << shift;
  }
  return value;
}

bool all_zero(const std::uint8_t* data, std::size_t length) noexcept {
  for (std::size_t index = 0; index < length; ++index) {
    if (data[index] != 0) {
      return false;
    }
  }
  return true;
}

std::uint64_t slot_offset(std::uint64_t slot_capacity_bytes, unsigned slot) noexcept {
  return kFileHeaderBytes + static_cast<std::uint64_t>(slot) * slot_capacity_bytes;
}

Result<std::vector<std::uint8_t>> build_file_header(std::uint64_t slot_capacity_bytes) {
  ByteWriter writer;
  writer.bytes(reinterpret_cast<const std::uint8_t*>(kFileMagic), sizeof(kFileMagic));
  writer.u32(kFormatVersion);
  writer.u32(static_cast<std::uint32_t>(kFileHeaderBytes));
  writer.u64(slot_capacity_bytes);
  writer.u64(slot_capacity_bytes - kHeadRecordBytes);
  writer.u64(0);  // creation_store_serial: diagnostic only, and never read back
  writer.zeros(kHeaderCrcOffset - writer.size());
  writer.u32(crc32c(writer.data().data(), writer.size()));
  writer.u32(kHeaderMarker);
  if (writer.size() != kFileHeaderBytes) {
    return Status::failure(StatusCode::internal_error, "store file header is not 512 bytes");
  }
  return std::move(writer).take();
}

Result<std::vector<std::uint8_t>> build_head_record(std::uint64_t generation,
                                                    std::uint64_t payload_length,
                                                    std::uint32_t payload_crc32c,
                                                    std::uint64_t writer_incarnation,
                                                    std::uint64_t writer_process_id) {
  ByteWriter writer;
  writer.bytes(reinterpret_cast<const std::uint8_t*>(kHeadMagic), sizeof(kHeadMagic));
  writer.u64(generation);
  writer.u64(payload_length);
  writer.u32(payload_crc32c);
  writer.u32(kFormatVersion);
  writer.u64(writer_incarnation);
  writer.u64(writer_process_id);
  writer.zeros(kHeadCrcOffset - writer.size());
  writer.u32(crc32c(writer.data().data(), writer.size()));
  writer.u32(kHeadMarker);
  if (writer.size() != kHeadRecordBytes) {
    return Status::failure(StatusCode::internal_error, "store head record is not 128 bytes");
  }
  return std::move(writer).take();
}

/// Verifies everything the file header claims about itself and about the
/// layout. A header that fails any check makes the whole file unusable, which
/// is why the checks are gathered here rather than spread over the readers.
Status verify_file_header(const std::uint8_t* header, const std::string& path,
                          std::uint64_t& slot_capacity_bytes,
                          std::uint64_t& payload_capacity_bytes) {
  if (std::memcmp(header, kFileMagic, sizeof(kFileMagic)) != 0) {
    return corrupt_failure("file is not an Airflow Control store: " + path);
  }
  if (load_u32(header + kHeaderMarkerOffset) != kHeaderMarker) {
    return corrupt_failure("store file header marker is wrong: " + path);
  }
  const std::uint32_t version = load_u32(header + kHeaderVersionOffset);
  if (version != kFormatVersion) {
    return Status::failure(StatusCode::store_unsupported_version,
                           "store format version " + std::to_string(version) +
                               " is not supported: " + path);
  }
  if (load_u32(header + kHeaderLengthOffset) != kFileHeaderBytes) {
    return corrupt_failure("store file header length is wrong: " + path);
  }
  if (crc32c(header, kHeaderCrcOffset) != load_u32(header + kHeaderCrcOffset)) {
    return corrupt_failure("store file header checksum does not match: " + path);
  }
  if (!all_zero(header + kHeaderReservedOffset, kHeaderCrcOffset - kHeaderReservedOffset)) {
    return corrupt_failure("store file header reserved bytes are not zero: " + path);
  }
  const std::uint64_t capacity = load_u64(header + kHeaderSlotCapacityOffset);
  const std::uint64_t payload_capacity = load_u64(header + kHeaderPayloadCapacityOffset);
  if (capacity <= kHeadRecordBytes || payload_capacity != capacity - kHeadRecordBytes) {
    return corrupt_failure("store slot capacity is inconsistent: " + path);
  }
  slot_capacity_bytes = capacity;
  payload_capacity_bytes = payload_capacity;
  return Status::success();
}

/// The slot that currently holds a head record.
///
/// A head does not name its slot, so the generation is identified by the fields
/// that describe it; matching all of them identifies one publication.
Result<unsigned> locate_slot(const StoreFile& file, const SlotHead& head) {
  for (unsigned slot = 0; slot < kSlotCount; ++slot) {
    Result<SlotHead> candidate = file.read_head(slot);
    if (!candidate.ok()) {
      return candidate.status();
    }
    const SlotHead& found = candidate.value();
    if (!found.valid) {
      continue;
    }
    if (found.generation == head.generation && found.payload_length == head.payload_length &&
        found.payload_crc32c == head.payload_crc32c &&
        found.writer_incarnation == head.writer_incarnation &&
        found.writer_process_id == head.writer_process_id) {
      return slot;
    }
  }
  return corrupt_failure("no slot holds the requested head record");
}

}  // namespace

struct StoreFile::Impl {
  FileHandle handle;
  std::uint64_t slot_capacity = 0;
  std::uint64_t payload_capacity = 0;
};

StoreFile::~StoreFile() {
  delete impl_;
  impl_ = nullptr;
}

StoreFile::StoreFile(StoreFile&& other) noexcept : impl_(other.impl_) {
  other.impl_ = nullptr;
}

StoreFile& StoreFile::operator=(StoreFile&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  delete impl_;
  impl_ = other.impl_;
  other.impl_ = nullptr;
  return *this;
}

Result<StoreFile> StoreFile::create(const std::string& canonical_path,
                                    std::uint64_t slot_capacity_bytes) {
  if (slot_capacity_bytes <= kHeadRecordBytes) {
    return Status::failure(StatusCode::invalid_argument,
                           "slot capacity " + std::to_string(slot_capacity_bytes) +
                               " must exceed the " + std::to_string(kHeadRecordBytes) +
                               "-byte head record");
  }
  if (slot_capacity_bytes > (UINT64_MAX - kFileHeaderBytes) / kSlotCount) {
    return Status::failure(StatusCode::overflow,
                           "slot capacity " + std::to_string(slot_capacity_bytes) +
                               " does not fit the store layout");
  }
  Result<FileHandle> handle = FileHandle::create_new(canonical_path);
  if (!handle.ok()) {
    return handle.status();
  }
  // The file reaches its final extent before anything is written into it, so
  // publishing a generation never has to extend the file at the moment it is
  // trying to commit.
  const std::uint64_t total_bytes = kFileHeaderBytes + slot_capacity_bytes * kSlotCount;
  Status sized = handle.value().truncate(total_bytes);
  if (!sized.ok()) {
    return sized;
  }
  Result<std::vector<std::uint8_t>> header = build_file_header(slot_capacity_bytes);
  if (!header.ok()) {
    return header.status();
  }
  Status written = handle.value().write_at(0, header.value().data(), header.value().size());
  if (!written.ok()) {
    return written;
  }
  reach_crash_point(CrashPoint::after_header_write);
  Status flushed = handle.value().flush();
  if (!flushed.ok()) {
    return flushed;
  }
  StoreFile file;
  file.impl_ = new Impl();
  file.impl_->handle = std::move(handle).value();
  file.impl_->slot_capacity = slot_capacity_bytes;
  file.impl_->payload_capacity = slot_capacity_bytes - kHeadRecordBytes;
  return std::move(file);
}

Result<StoreFile> StoreFile::open(const std::string& canonical_path) {
  Result<FileHandle> handle = FileHandle::open_existing(canonical_path);
  if (!handle.ok()) {
    return handle.status();
  }
  std::array<std::uint8_t, static_cast<std::size_t>(kFileHeaderBytes)> header{};
  std::size_t bytes_read = 0;
  Status read = handle.value().read_at(0, header.data(), header.size(), bytes_read);
  if (!read.ok()) {
    return read;
  }
  if (bytes_read != header.size()) {
    return corrupt_failure("store file is smaller than its header: " + canonical_path);
  }
  std::uint64_t slot_capacity_bytes = 0;
  std::uint64_t payload_capacity_bytes = 0;
  Status verified = verify_file_header(header.data(), canonical_path, slot_capacity_bytes,
                                        payload_capacity_bytes);
  if (!verified.ok()) {
    return verified;
  }
  StoreFile file;
  file.impl_ = new Impl();
  file.impl_->handle = std::move(handle).value();
  file.impl_->slot_capacity = slot_capacity_bytes;
  file.impl_->payload_capacity = payload_capacity_bytes;
  return std::move(file);
}

Result<SlotHead> StoreFile::read_head(unsigned slot) const {
  if (impl_ == nullptr) {
    return not_open_failure();
  }
  if (slot >= kSlotCount) {
    return Status::failure(StatusCode::invalid_argument,
                           "slot index " + std::to_string(slot) + " is out of range");
  }
  SlotHead head;  // invalid until every check below has passed
  std::array<std::uint8_t, static_cast<std::size_t>(kHeadRecordBytes)> bytes{};
  std::size_t bytes_read = 0;
  Status read = impl_->handle.read_at(slot_offset(impl_->slot_capacity, slot), bytes.data(),
                                      bytes.size(), bytes_read);
  if (!read.ok()) {
    return read;
  }
  if (bytes_read < bytes.size()) {
    return head;  // the slot has never been written
  }
  if (std::memcmp(bytes.data(), kHeadMagic, sizeof(kHeadMagic)) != 0) {
    return head;
  }
  if (load_u32(bytes.data() + kHeadMarkerOffset) != kHeadMarker) {
    return head;
  }
  if (load_u32(bytes.data() + kHeadVersionOffset) != kFormatVersion) {
    return head;
  }
  if (crc32c(bytes.data(), kHeadCrcOffset) != load_u32(bytes.data() + kHeadCrcOffset)) {
    return head;
  }
  if (!all_zero(bytes.data() + kHeadReservedOffset, kHeadCrcOffset - kHeadReservedOffset)) {
    return head;
  }
  const std::uint64_t payload_length = load_u64(bytes.data() + kHeadPayloadLengthOffset);
  if (payload_length > impl_->payload_capacity) {
    return head;
  }
  head.generation = load_u64(bytes.data() + kHeadGenerationOffset);
  head.payload_length = payload_length;
  head.payload_crc32c = load_u32(bytes.data() + kHeadPayloadCrcOffset);
  head.writer_incarnation = load_u64(bytes.data() + kHeadIncarnationOffset);
  head.writer_process_id = load_u64(bytes.data() + kHeadProcessIdOffset);
  head.valid = true;
  return head;
}

Result<std::vector<std::uint8_t>> StoreFile::read_payload(const SlotHead& head) const {
  if (impl_ == nullptr) {
    return not_open_failure();
  }
  if (!head.valid) {
    return Status::failure(StatusCode::invalid_argument,
                           "payload requested for a head record that does not verify");
  }
  if (head.payload_length > impl_->payload_capacity) {
    return corrupt_failure("head payload length exceeds the slot capacity");
  }
  Result<unsigned> slot = locate_slot(*this, head);
  if (!slot.ok()) {
    return slot.status();
  }
  const std::uint64_t base =
      slot_offset(impl_->slot_capacity, slot.value()) + kHeadRecordBytes;
  Result<std::uint64_t> total = impl_->handle.size();
  if (!total.ok()) {
    return total.status();
  }
  if (total.value() < base || head.payload_length > total.value() - base) {
    return corrupt_failure("store payload extends past the end of the file");
  }
  if constexpr (sizeof(std::size_t) < sizeof(std::uint64_t)) {
    if (head.payload_length > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
      return corrupt_failure("store payload length does not fit this platform");
    }
  }
  std::vector<std::uint8_t> payload(static_cast<std::size_t>(head.payload_length));
  if (!payload.empty()) {
    std::size_t bytes_read = 0;
    Status read = impl_->handle.read_at(base, payload.data(), payload.size(), bytes_read);
    if (!read.ok()) {
      return read;
    }
    if (bytes_read != payload.size()) {
      return corrupt_failure("store payload is truncated");
    }
  }
  if (crc32c(payload.data(), payload.size()) != head.payload_crc32c) {
    return corrupt_failure("store payload checksum does not match its head record");
  }
  return payload;
}

Status StoreFile::write_slot(unsigned slot, std::uint64_t generation,
                             const std::vector<std::uint8_t>& payload,
                             std::uint64_t writer_incarnation) {
  if (impl_ == nullptr) {
    return not_open_failure();
  }
  if (slot >= kSlotCount) {
    return Status::failure(StatusCode::invalid_argument,
                           "slot index " + std::to_string(slot) + " is out of range");
  }
  if (payload.size() > impl_->payload_capacity) {
    return Status::failure(StatusCode::store_capacity_exceeded,
                           "payload of " + std::to_string(payload.size()) +
                               " bytes exceeds the slot capacity of " +
                               std::to_string(impl_->payload_capacity) + " bytes");
  }
  const std::uint64_t base = slot_offset(impl_->slot_capacity, slot);
  // Staging: the payload is written and forced to stable storage before the
  // head record that makes it reachable.
  Status written = impl_->handle.write_at(base + kHeadRecordBytes, payload.data(), payload.size());
  if (!written.ok()) {
    return written;
  }
  reach_crash_point(CrashPoint::after_slot_write);
  Status flushed = impl_->handle.flush();
  if (!flushed.ok()) {
    return flushed;
  }
  reach_crash_point(CrashPoint::after_slot_flush);
  // Commit: one checksummed 128-byte record. Nothing before this point is
  // reachable from any head that verifies.
  Result<std::vector<std::uint8_t>> head =
      build_head_record(generation, payload.size(), crc32c(payload.data(), payload.size()),
                        writer_incarnation, current_process_id());
  if (!head.ok()) {
    return head.status();
  }
  Status head_written = impl_->handle.write_at(base, head.value().data(), head.value().size());
  if (!head_written.ok()) {
    return head_written;
  }
  reach_crash_point(CrashPoint::after_head_write);
  Status head_flushed = impl_->handle.flush();
  if (!head_flushed.ok()) {
    return head_flushed;
  }
  reach_crash_point(CrashPoint::after_head_flush);
  return Status::success();
}

Status StoreFile::flush() {
  if (impl_ == nullptr) {
    return not_open_failure();
  }
  return impl_->handle.flush();
}

Result<std::uint64_t> StoreFile::slot_capacity() const {
  if (impl_ == nullptr) {
    return not_open_failure();
  }
  return impl_->slot_capacity;
}

Result<std::uint64_t> StoreFile::file_size() const {
  if (impl_ == nullptr) {
    return not_open_failure();
  }
  return impl_->handle.size();
}

}  // namespace airflow_control::detail
