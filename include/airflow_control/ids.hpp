#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "airflow_control/status.hpp"

namespace airflow_control {

/// Longest identifier this runtime accepts, in bytes.
inline constexpr std::size_t kMaxIdentifierLength = 96;

/// Longest bounded free-text field this runtime accepts, in bytes.
inline constexpr std::size_t kMaxTextLength = 256;

/// Validates a bounded free-text field: valid UTF-8, no NUL, no other C0 or C1
/// control character, and at most max_length bytes.
[[nodiscard]] Result<std::string> validate_text(std::string_view text, std::size_t max_length);

/// Validates an identifier: printable ASCII, no separator, no traversal, no
/// character that a filesystem or a shell would reinterpret.
[[nodiscard]] Result<std::string> validate_identifier(std::string_view text, std::size_t max_length);

/// A strongly typed identifier.
///
/// Identifiers are opaque to this runtime: they name objects owned here and are
/// never parsed for structure. They are deliberately restricted to a character
/// set that cannot express a path, a traversal, a device name, or a vendor
/// string, because identifiers appear in durable state and in diagnostics but
/// never in a protocol.
///
/// A BasicId has no default constructor: an absent identity is
/// std::optional<Id>, never a zero-valued or empty id.
template <typename Tag, std::size_t MaxLength = kMaxIdentifierLength>
class BasicId {
 public:
  using tag_type = Tag;

  BasicId() = delete;

  [[nodiscard]] static Result<BasicId> parse(std::string_view text) {
    Result<std::string> validated = validate_identifier(text, MaxLength);
    if (!validated.ok()) {
      return validated.status();
    }
    return BasicId(std::move(validated).value());
  }

  [[nodiscard]] const std::string& str() const noexcept { return value_; }
  [[nodiscard]] std::size_t length() const noexcept { return value_.size(); }
  [[nodiscard]] std::size_t hash() const noexcept { return std::hash<std::string>{}(value_); }

  friend bool operator==(const BasicId& lhs, const BasicId& rhs) noexcept = default;
  friend std::strong_ordering operator<=>(const BasicId& lhs, const BasicId& rhs) noexcept = default;

 private:
  explicit BasicId(std::string value) : value_(std::move(value)) {}

  std::string value_;
};

/// A monotonically ordered ordinal with a distinct type per meaning.
///
/// Generations, revisions, epochs, sequences, ordinals, and identifiers are
/// materially different concepts and are never interchangeable.
template <typename Tag>
class Ordinal {
 public:
  using tag_type = Tag;
  using value_type = std::uint64_t;

  Ordinal() = delete;

  [[nodiscard]] static Ordinal from(std::uint64_t value) noexcept { return Ordinal(value); }

  [[nodiscard]] std::uint64_t value() const noexcept { return value_; }
  [[nodiscard]] bool is_zero() const noexcept { return value_ == 0; }

  /// The next ordinal, or an overflow status when the value is already maximal.
  [[nodiscard]] Result<Ordinal> next() const noexcept {
    if (value_ == UINT64_MAX) {
      return Status::failure(StatusCode::overflow, "ordinal exhausted");
    }
    return Ordinal(value_ + 1);
  }

  friend bool operator==(const Ordinal& lhs, const Ordinal& rhs) noexcept = default;
  friend std::strong_ordering operator<=>(const Ordinal& lhs, const Ordinal& rhs) noexcept = default;

  [[nodiscard]] std::string to_string() const { return std::to_string(value_); }

 private:
  explicit Ordinal(std::uint64_t value) noexcept : value_(value) {}

  std::uint64_t value_;
};

// ---------------------------------------------------------------------------
// Identifier tags and types
// ---------------------------------------------------------------------------

/// A room, as named by the facility topology that owns rooms.
struct RoomIdTag {};
/// A row of racks, as named by the facility topology that owns rows.
struct RowIdTag {};
/// A rack, as named by the facility topology that owns racks.
struct RackIdTag {};
/// An airflow device: the actuator this runtime is allowed to request a setpoint
/// from. Fan motor control, PID behaviour, and device protocol live elsewhere.
struct AirflowDeviceIdTag {};
/// A containment element relevant to airflow control.
struct ContainmentIdTag {};
/// A controlled-space/reference-space pressure relationship.
struct PressureRelationshipIdTag {};
/// A room, row, or rack airflow obligation.
struct ObligationIdTag {};
/// A protected external obligation that must be satisfied before control.
struct InterlockIdTag {};
/// A permission grant issued by the authority that owns airflow permissions.
struct GrantIdTag {};
/// A scoped maintenance override for one device generation.
struct OverrideIdTag {};
/// A safety-directed permit issued by the authority that owns emergency policy.
struct SafetyPermitIdTag {};
/// A fan/setpoint policy revision owned by the layer that derives airflow limits.
struct PolicyIdTag {};
/// A provenance reference: the system that produced a value.
struct SourceIdTag {};
/// A human or system actor that requested a change.
struct ActorIdTag {};
/// A caller-supplied idempotency key.
struct IdempotencyKeyTag {};
/// An opaque reference to a space or measurement point owned by the facility
/// topology layer. Never interpreted here.
struct SpaceRefIdTag {};

using RoomId = BasicId<RoomIdTag>;
using RowId = BasicId<RowIdTag>;
using RackId = BasicId<RackIdTag>;
using AirflowDeviceId = BasicId<AirflowDeviceIdTag>;
using ContainmentId = BasicId<ContainmentIdTag>;
using PressureRelationshipId = BasicId<PressureRelationshipIdTag>;
using ObligationId = BasicId<ObligationIdTag>;
using InterlockId = BasicId<InterlockIdTag>;
using GrantId = BasicId<GrantIdTag>;
using OverrideId = BasicId<OverrideIdTag>;
using SafetyPermitId = BasicId<SafetyPermitIdTag>;
using PolicyId = BasicId<PolicyIdTag>;
using SourceId = BasicId<SourceIdTag>;
using ActorId = BasicId<ActorIdTag>;
using IdempotencyKey = BasicId<IdempotencyKeyTag>;
using SpaceRefId = BasicId<SpaceRefIdTag>;

// ---------------------------------------------------------------------------
// Ordinal tags and types
// ---------------------------------------------------------------------------

struct AuthorityEpochTag {};
struct DeviceGenerationTag {};
struct EvidenceGenerationTag {};
struct PolicyGenerationTag {};
struct StateRevisionTag {};
struct StoreGenerationTag {};
struct AttemptIdTag {};
struct ObservationIdTag {};
struct EvidenceSequenceTag {};
struct AdapterSequenceTag {};
struct AuditSequenceTag {};
struct EffectSequenceTag {};
struct AttemptOrdinalTag {};
struct CommandIdTag {};
struct IncarnationIdTag {};
struct LogicalTickTag {};

/// The authority epoch an operation is bound to. Adopted explicitly and
/// monotonic; adopting an older epoch is refused.
using AuthorityEpoch = Ordinal<AuthorityEpochTag>;
/// The generation of a device. Bumped when a device is replaced or
/// reconstructed; distinct from the state revision.
using DeviceGeneration = Ordinal<DeviceGenerationTag>;
/// The generation of the evidence set a decision was planned against.
using EvidenceGeneration = Ordinal<EvidenceGenerationTag>;
/// The generation of the fan/setpoint policy in force.
using PolicyGeneration = Ordinal<PolicyGenerationTag>;
/// The revision of a mutable object, bumped by every accepted mutation.
using StateRevision = Ordinal<StateRevisionTag>;
/// The monotonic fence of the durable publication.
using StoreGeneration = Ordinal<StoreGenerationTag>;
/// The identity of one control attempt.
using AttemptId = Ordinal<AttemptIdTag>;
/// The identity of one accepted observation.
using ObservationId = Ordinal<ObservationIdTag>;
/// The order a source assigns to its own evidence for one subject.
using EvidenceSequence = Ordinal<EvidenceSequenceTag>;
/// The order an adapter assigns to its own operations.
using AdapterSequence = Ordinal<AdapterSequenceTag>;
/// The order of an audit entry.
using AuditSequence = Ordinal<AuditSequenceTag>;
/// The order of an established effect.
using EffectSequence = Ordinal<EffectSequenceTag>;
/// The ordinal of an attempt within one device's attempt history.
using AttemptOrdinal = Ordinal<AttemptOrdinalTag>;
/// The identity of one adapter command.
using CommandId = Ordinal<CommandIdTag>;
/// The identity of one engine incarnation: a writer that was thought to be dead
/// can be told apart from its successor.
using IncarnationId = Ordinal<IncarnationIdTag>;

/// A logical instant supplied by the caller.
///
/// This library never reads the system clock. Control decisions are taken
/// against logical ticks only, so a decision is reproducible.
class LogicalTick {
 public:
  using tag_type = LogicalTickTag;
  using value_type = std::uint64_t;

  LogicalTick() = delete;

  [[nodiscard]] static LogicalTick from(std::uint64_t value) noexcept { return LogicalTick(value); }
  [[nodiscard]] std::uint64_t value() const noexcept { return value_; }

  friend bool operator==(const LogicalTick& lhs, const LogicalTick& rhs) noexcept = default;
  friend std::strong_ordering operator<=>(const LogicalTick& lhs, const LogicalTick& rhs) noexcept = default;

  [[nodiscard]] std::string to_string() const { return std::to_string(value_); }

 private:
  explicit LogicalTick(std::uint64_t value) noexcept : value_(value) {}

  std::uint64_t value_;
};

/// base + delta, or an overflow status.
[[nodiscard]] Result<LogicalTick> tick_add(LogicalTick base, std::uint64_t delta) noexcept;

/// now - earlier, or evidence_future when earlier is later than now.
[[nodiscard]] Result<std::uint64_t> tick_age(LogicalTick now, LogicalTick earlier) noexcept;

}  // namespace airflow_control

namespace std {

template <typename Tag, size_t MaxLength>
struct hash<airflow_control::BasicId<Tag, MaxLength>> {
  size_t operator()(const airflow_control::BasicId<Tag, MaxLength>& id) const noexcept {
    return id.hash();
  }
};

template <typename Tag>
struct hash<airflow_control::Ordinal<Tag>> {
  size_t operator()(const airflow_control::Ordinal<Tag>& ordinal) const noexcept {
    return static_cast<size_t>(ordinal.value());
  }
};

template <>
struct hash<airflow_control::LogicalTick> {
  size_t operator()(const airflow_control::LogicalTick& tick) const noexcept {
    return static_cast<size_t>(tick.value());
  }
};

}  // namespace std
