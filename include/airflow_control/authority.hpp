#pragma once

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>

#include "airflow_control/ids.hpp"
#include "airflow_control/model.hpp"
#include "airflow_control/status.hpp"

namespace airflow_control {

/// An action an actor must be permitted to perform.
///
/// The action is derived from the intent by a pure function, so a caller cannot
/// choose the permission it is checked against.
enum class ControlAction : std::uint32_t {
  apply_setpoint = 0,
  raise_airflow = 1,
  lower_airflow = 2,
  change_pressure_target = 3,
  service_containment = 4,
  clear_breach = 5,
  emergency_purge = 6,
  release_hold = 7,
  set_lifecycle = 8,
  set_policy = 9,
};

inline constexpr std::uint32_t kControlActionCount = 10;

[[nodiscard]] const char* to_string(ControlAction action) noexcept;
[[nodiscard]] std::optional<ControlAction> parse_control_action(std::string_view token) noexcept;

/// The action a control intent requires. Pure and total.
[[nodiscard]] ControlAction required_action(ControlIntent intent) noexcept;

/// A bounded set of control actions.
class ActionSet {
 public:
  ActionSet() = default;

  [[nodiscard]] static ActionSet none() noexcept { return ActionSet(0); }
  [[nodiscard]] static ActionSet all() noexcept;
  [[nodiscard]] static Result<ActionSet> from_mask(std::uint32_t mask) noexcept;
  [[nodiscard]] static ActionSet of(std::initializer_list<ControlAction> actions) noexcept;

  [[nodiscard]] bool contains(ControlAction action) const noexcept;
  [[nodiscard]] bool empty() const noexcept { return mask_ == 0; }
  [[nodiscard]] std::uint32_t mask() const noexcept { return mask_; }

  friend bool operator==(const ActionSet& lhs, const ActionSet& rhs) noexcept = default;

  [[nodiscard]] std::string to_string() const;

 private:
  explicit ActionSet(std::uint32_t mask) noexcept : mask_(mask) {}

  std::uint32_t mask_ = 0;
};

/// A permission grant issued by the authority that owns airflow permissions.
///
/// A grant is scoped by epoch, by device generation, by room, and by action. It
/// expires at a logical tick. This runtime never issues one, never widens one,
/// and never infers one from the absence of a refusal.
struct PermissionGrant {
  GrantId id;
  SourceId issuer;
  AuthorityEpoch epoch;
  std::optional<AirflowDeviceId> device;
  std::optional<DeviceGeneration> device_generation;
  std::optional<RoomId> room;
  ActionSet actions;
  LogicalTick issued_at;
  std::optional<LogicalTick> expires_at;
  bool revoked = false;
};

/// A permit for emergency ventilation, issued by the authority that owns
/// emergency policy. Distinct from a PermissionGrant: it authorises a
/// safety-directed request whose containment and pressure evidence may be
/// unknown, and it is recorded as such on the attempt.
struct SafetyPermit {
  SafetyPermitId id;
  SourceId issuer;
  AuthorityEpoch epoch;
  AirflowDeviceId device;
  LogicalTick issued_at;
  std::optional<LogicalTick> expires_at;
  std::string reason;
};

/// A protected external obligation that must be satisfied before control.
///
/// An airflow change can depend on a condition this runtime does not own: a
/// fire panel, a leak detector, a door contact, a maintenance lockout. The
/// owning system declares the interlock and reports its state; this runtime
/// never invents one, and fails closed while a protected interlock is open or
/// cannot be established.
enum class InterlockClass : std::uint32_t {
  protected_obligation = 0,
  advisory = 1,
};

enum class InterlockState : std::uint32_t {
  satisfied = 0,
  open = 1,
  unknown = 2,
};

[[nodiscard]] const char* to_string(InterlockClass klass) noexcept;
[[nodiscard]] const char* to_string(InterlockState state) noexcept;
[[nodiscard]] std::optional<InterlockClass> parse_interlock_class(std::string_view token) noexcept;
[[nodiscard]] std::optional<InterlockState> parse_interlock_state(std::string_view token) noexcept;

/// A declared interlock.
///
/// A protected obligation that was required by the model but never declared is
/// treated as unknown, not as satisfied: absence of a declaration is never
/// permission.
struct Interlock {
  InterlockId id;
  RoomId room;
  std::optional<RowId> row;
  std::optional<AirflowDeviceId> device;
  InterlockClass klass = InterlockClass::protected_obligation;
  InterlockState state = InterlockState::unknown;
  EvidenceSequence sequence;
  AuthorityEpoch epoch;
  LogicalTick declared_at;
  std::optional<LogicalTick> reported_at;
};

/// Proof, produced only by the engine, that every precondition of one control
/// command was validated against a specific state.
///
/// An adapter receives an AdapterCommand, and an AdapterCommand can only be
/// constructed from this token. A caller therefore cannot hand an adapter a
/// command that skipped precondition validation.
class ActuationAuthorization {
 public:
  ActuationAuthorization(const ActuationAuthorization&) = default;
  ActuationAuthorization& operator=(const ActuationAuthorization&) = default;
  ActuationAuthorization(ActuationAuthorization&&) = default;
  ActuationAuthorization& operator=(ActuationAuthorization&&) = default;
  ~ActuationAuthorization() = default;

  [[nodiscard]] CommandId command() const noexcept { return command_; }
  [[nodiscard]] AttemptId attempt() const noexcept { return attempt_; }
  [[nodiscard]] const AirflowDeviceId& device() const noexcept { return device_; }
  [[nodiscard]] DeviceGeneration device_generation() const noexcept { return device_generation_; }
  [[nodiscard]] AuthorityEpoch epoch() const noexcept { return epoch_; }
  [[nodiscard]] StateRevision planned_revision() const noexcept { return planned_revision_; }
  [[nodiscard]] PolicyGeneration policy_generation() const noexcept { return policy_generation_; }
  [[nodiscard]] EvidenceGeneration evidence_generation() const noexcept { return evidence_generation_; }
  [[nodiscard]] LogicalTick issued_at() const noexcept { return issued_at_; }

 private:
  friend class AirflowControlEngine;

  ActuationAuthorization(CommandId command, AttemptId attempt, AirflowDeviceId device,
                         DeviceGeneration device_generation, AuthorityEpoch epoch,
                         StateRevision planned_revision, PolicyGeneration policy_generation,
                         EvidenceGeneration evidence_generation, LogicalTick issued_at)
      : command_(command),
        attempt_(attempt),
        device_(std::move(device)),
        device_generation_(device_generation),
        epoch_(epoch),
        planned_revision_(planned_revision),
        policy_generation_(policy_generation),
        evidence_generation_(evidence_generation),
        issued_at_(issued_at) {}

  CommandId command_;
  AttemptId attempt_;
  AirflowDeviceId device_;
  DeviceGeneration device_generation_;
  AuthorityEpoch epoch_;
  StateRevision planned_revision_;
  PolicyGeneration policy_generation_;
  EvidenceGeneration evidence_generation_;
  LogicalTick issued_at_;
};

}  // namespace airflow_control
