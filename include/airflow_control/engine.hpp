#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "airflow_control/adapter.hpp"
#include "airflow_control/attempt.hpp"
#include "airflow_control/audit.hpp"
#include "airflow_control/authority.hpp"
#include "airflow_control/evidence.hpp"
#include "airflow_control/ids.hpp"
#include "airflow_control/lifecycle.hpp"
#include "airflow_control/model.hpp"
#include "airflow_control/status.hpp"
#include "airflow_control/store.hpp"

namespace airflow_control {

/// Configuration of one engine instance. Every bound is explicit.
struct EngineOptions {
  struct EvidenceBounds {
    /// How old a reading may be, in logical ticks, and still count as fresh.
    LogicalTick max_age_ticks = LogicalTick::from(1000);
    /// How many distinct sources are retained per pressure relationship.
    std::size_t max_relationship_sources = 4;
    /// How far an observed fan setpoint may differ from the commanded one and
    /// still establish the effect. Zero means exact, which is the strictest
    /// answer and the honest default; a deployment that knows its device
    /// settles sets the tolerance it can defend.
    std::uint32_t setpoint_tolerance_basis_points = 0;
    /// How far an observed airflow may differ from the commanded airflow and
    /// still establish the effect. Zero means exact.
    std::int64_t airflow_tolerance_cubic_metres_per_hour = 0;
  } evidence;

  /// How many accepted attempts are retained for idempotent replay.
  ///
  /// A fresh store adopts this value verbatim. A store that already holds a
  /// larger persisted window keeps it, so reopening with a smaller bound never
  /// loses a replay guarantee the store was written under; the effective value
  /// is reported by canonical_state() and by the store audit.
  std::size_t idempotency_window = 256;
  /// Size of the bounded audit ring. Also a floor against the persisted value.
  std::size_t audit_capacity = 256;
  /// Size of the bounded attempt journal, raised to at least the effective
  /// idempotency window, because a retained key must always name an attempt the
  /// journal still holds.
  std::size_t attempt_journal_capacity = 512;

  StoreOptions store;
};

/// The adjudicated state of a pressure relationship.
enum class PressureState : std::uint32_t {
  /// Fresh good evidence agrees the differential is inside the band.
  satisfied = 0,
  /// Fresh good evidence agrees the differential is outside the band.
  violated = 1,
  /// Fresh good evidence disagrees with itself about the band.
  conflicted = 2,
  /// There is no fresh good evidence at all.
  unknown = 3,
};

[[nodiscard]] const char* to_string(PressureState state) noexcept;

/// The adjudicated state of an airflow obligation.
enum class ObligationState : std::uint32_t {
  satisfied = 0,
  unsatisfied = 1,
  unknown = 2,
};

[[nodiscard]] const char* to_string(ObligationState state) noexcept;

// ---------------------------------------------------------------------------
// Mutation requests
// ---------------------------------------------------------------------------

struct RegisterDeviceRequest {
  AirflowDeviceId device;
  DeviceGeneration generation;
  RoomId room;
  std::optional<RowId> row;
  ActorId actor;
  LogicalTick requested_at;
};

struct SetLifecycleRequest {
  AirflowDeviceId device;
  DeviceGeneration generation;
  StateRevision expected_revision;
  DeviceLifecycle target = DeviceLifecycle::provisioned;
  AuthorityEpoch epoch;
  ActorId actor;
  LogicalTick requested_at;
};

struct SetFanPolicyRequest {
  AirflowDeviceId device;
  DeviceGeneration generation;
  StateRevision expected_revision;
  FanPolicy policy;
  ActorId actor;
  LogicalTick requested_at;
};

struct DefineRelationshipRequest {
  PressureRelationshipId id;
  RoomId room;
  SpaceRefId controlled_space;
  SpaceRefId reference_space;
  PressurePolarity polarity = PressurePolarity::neutral;
  Pressure lower;
  Pressure upper;
  Pressure tolerance;
  EvidenceGeneration evidence_generation;
  std::optional<StateRevision> expected_revision;
  ActorId actor;
  LogicalTick requested_at;
};

struct DefineContainmentRequest {
  ContainmentId id;
  RoomId room;
  std::optional<RowId> row;
  ContainmentKind kind = ContainmentKind::aisle_containment;
  std::optional<StateRevision> expected_revision;
  ActorId actor;
  LogicalTick requested_at;
};

struct ReportContainmentRequest {
  ContainmentId element;
  ContainmentState state = ContainmentState::unknown;
  Quality quality = Quality::good;
  SourceId source;
  EvidenceSequence sequence;
  EvidenceGeneration evidence_generation;
  LogicalTick measured_at;
};

struct DeclareObligationRequest {
  ObligationId id;
  ObligationScope scope = ObligationScope::room;
  RoomId room;
  std::optional<RowId> row;
  std::optional<RackId> rack;
  ObligationClass klass = ObligationClass::protected_obligation;
  ObligationBinding binding = ObligationBinding::device_sum;
  std::optional<SpaceRefId> metered_point;
  std::vector<AirflowDeviceId> devices;
  Airflow minimum_airflow;
  Airflow target_airflow;
  SourceId source;
  EvidenceGeneration evidence_generation;
  std::optional<StateRevision> expected_revision;
  ActorId actor;
  LogicalTick requested_at;
};

struct DeclareInterlockRequest {
  InterlockId id;
  RoomId room;
  std::optional<RowId> row;
  std::optional<AirflowDeviceId> device;
  InterlockClass klass = InterlockClass::protected_obligation;
  AuthorityEpoch epoch;
  ActorId actor;
  LogicalTick declared_at;
};

struct ReportInterlockRequest {
  InterlockId id;
  InterlockState state = InterlockState::unknown;
  EvidenceSequence sequence;
  AuthorityEpoch epoch;
  LogicalTick reported_at;
};

struct MaintenanceOverride {
  OverrideId id;
  AirflowDeviceId device;
  DeviceGeneration device_generation;
  AuthorityEpoch epoch;
  LogicalTick issued_at;
  LogicalTick expires_at;
  std::string reason;
  bool revoked = false;
};

/// One control request. This is the whole input to evaluate and issue.
struct ControlRequest {
  IdempotencyKey key;
  AirflowDeviceId device;
  DeviceGeneration device_generation;
  AuthorityEpoch epoch;
  std::optional<StateRevision> expected_revision;
  ControlIntent intent = ControlIntent::hold_setpoint;
  /// Absent only for release_to_policy, which resolves to the policy default.
  std::optional<SetpointRequest> setpoint;
  ActorId actor;
  LogicalTick requested_at;
  /// Required for a safety-directed intent.
  std::optional<SafetyPermitId> safety_permit;
  /// Names the exact unresolved attempt this request supersedes. Absent means
  /// the caller is not asking to supersede anything.
  std::optional<AttemptId> supersede;
  /// The pressure relationship this request is about.
  ///
  /// Required for restore_pressure_relationship, and refused for an intent that
  /// is not about a pressure relationship: the caller states which
  /// relationship it means rather than leaving the runtime to guess from
  /// topology it does not own.
  std::optional<PressureRelationshipId> relationship;
};

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

/// The adjudicated answer to "may this transition be attempted".
struct Decision {
  bool eligible = false;
  StatusCode code = StatusCode::ok;
  std::string message;
  RequestClass request_class = RequestClass::protective;
  bool replayed = false;
  std::optional<AttemptId> replayed_attempt;
  // The generation counters start at zero, which is this runtime's encoding of
  // "no generation was bound". A refusal that never reached the binding checks
  // therefore reports zero rather than a plausible-looking generation.
  StateRevision planned_revision = StateRevision::from(0);
  DeviceGeneration device_generation = DeviceGeneration::from(0);
  AuthorityEpoch epoch = AuthorityEpoch::from(0);
  PolicyGeneration policy_generation = PolicyGeneration::from(0);
  EvidenceGeneration evidence_generation = EvidenceGeneration::from(0);
  std::optional<SetpointRequest> resolved_setpoint;
  std::optional<AttemptId> unresolved_attempt;
  EffectState current_effect = EffectState::unverified;
  /// True when the refusal is due to a precondition the caller can repair
  /// without changing authority.
  std::vector<CheckTrace> trace;
};

struct VerificationRequest {
  AttemptId attempt;
  /// When null, only evidence already accepted is considered.
  AirflowAdapter* adapter = nullptr;
  SourceId source;
  SpaceRefId fan_point;
  SpaceRefId pressure_point;
  EvidenceSequence fan_sequence;
  EvidenceSequence pressure_sequence;
  LogicalTick at;
  /// The relationship whose differential is read when the attempt requires
  /// pressure proof.
  std::optional<PressureRelationshipId> relationship;
};

struct ResolveAttemptRequest {
  AttemptId attempt;
  AttemptState target = AttemptState::resolved_without_effect;
  ActorId actor;
  LogicalTick at;
  std::string reason;
};

// ---------------------------------------------------------------------------
// Inspection views
// ---------------------------------------------------------------------------

struct DeviceView {
  AirflowDeviceId id;
  DeviceGeneration generation;
  StateRevision revision;
  DeviceLifecycle lifecycle = DeviceLifecycle::provisioned;
  RoomId room;
  std::optional<RowId> row;
  /// Absent when the device has no fan policy, which is a device for which no
  /// safe operating envelope is known rather than one with a zero envelope.
  std::optional<PolicyId> policy_id;
  std::optional<PolicyGeneration> policy_generation;
  std::optional<OperatingEnvelope> envelope;
  std::optional<AttemptId> unresolved_attempt;
  EffectState effect = EffectState::unverified;
  std::optional<SetpointBasisPoints> observed_fan_percent;
  std::optional<Airflow> observed_airflow;
  FreshnessVerdict fan_freshness = FreshnessVerdict::stale;
  FreshnessVerdict airflow_freshness = FreshnessVerdict::stale;
  bool has_fan_observation = false;
  bool has_airflow_observation = false;
};

struct RelationshipView {
  PressureRelationshipId id;
  RoomId room;
  PressurePolarity polarity = PressurePolarity::neutral;
  Pressure lower;
  Pressure upper;
  Pressure tolerance;
  EvidenceGeneration evidence_generation;
  StateRevision revision;
  PressureState state = PressureState::unknown;
  std::optional<Pressure> adjudicated;
  std::size_t contributing_sources = 0;
  std::size_t conflicting_sources = 0;
};

struct ContainmentView {
  ContainmentId id;
  RoomId room;
  std::optional<RowId> row;
  ContainmentKind kind = ContainmentKind::other;
  ContainmentState state = ContainmentState::unknown;
  Quality quality = Quality::unknown;
  SourceId source;
  EvidenceSequence sequence;
  EvidenceGeneration evidence_generation;
  LogicalTick measured_at;
  bool has_report = false;
};

struct ObligationView {
  ObligationId id;
  ObligationScope scope = ObligationScope::room;
  RoomId room;
  std::optional<RowId> row;
  std::optional<RackId> rack;
  ObligationClass klass = ObligationClass::protected_obligation;
  ObligationBinding binding = ObligationBinding::device_sum;
  Airflow minimum_airflow;
  Airflow target_airflow;
  EvidenceGeneration evidence_generation;
  StateRevision revision;
  ObligationState state = ObligationState::unknown;
  std::optional<Airflow> observed_airflow;
};

struct InterlockView {
  InterlockId id;
  RoomId room;
  InterlockClass klass = InterlockClass::protected_obligation;
  InterlockState state = InterlockState::unknown;
  EvidenceSequence sequence;
  AuthorityEpoch epoch;
  bool has_report = false;
};

struct AttemptView {
  AttemptRecord record;
  bool superseded_by_known_attempt = false;
};

struct HistoryView {
  std::vector<AuditEntry> entries;
  std::uint64_t dropped = 0;
};

// ---------------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------------

/// Vendor-neutral airflow-oriented control semantics.
///
/// An engine instance is not internally synchronized: one instance is used by
/// one thread at a time. The only lock in this library is the operating-system
/// file lock of a durable store, acquired once at open and released at close.
/// It is never reacquired on any call path, so it is never held across a
/// callback that could re-enter this engine, and no adapter call ever happens
/// while an internal mutex is held, because there is no internal mutex.
class AirflowControlEngine {
 public:
  AirflowControlEngine();
  ~AirflowControlEngine();

  AirflowControlEngine(AirflowControlEngine&& other) noexcept;
  AirflowControlEngine& operator=(AirflowControlEngine&& other) noexcept;
  AirflowControlEngine(const AirflowControlEngine&) = delete;
  AirflowControlEngine& operator=(const AirflowControlEngine&) = delete;

  /// Opens a durable engine. Recovery runs before the engine is returned.
  [[nodiscard]] static Result<AirflowControlEngine> open(const std::string& path, OpenMode mode,
                                                         const EngineOptions& options);

  /// An engine with no durable state. Nothing survives close, and the store
  /// audit reports that no publication happened.
  [[nodiscard]] static Result<AirflowControlEngine> open_in_memory(const EngineOptions& options);

  [[nodiscard]] bool is_open() const noexcept;
  [[nodiscard]] bool is_durable() const noexcept;
  Status close();

  [[nodiscard]] const EngineOptions& options() const noexcept;

  // -- lifecycle of the control plane --------------------------------------

  /// Adopts an authority epoch. Epochs are monotonic: adopting an older one is
  /// refused, because that would silently re-authorize withdrawn grants.
  Status adopt_epoch(AuthorityEpoch epoch, const ActorId& actor, LogicalTick at);

  /// Advances the logical clock. The clock never moves backwards.
  Status advance_tick(LogicalTick tick);
  [[nodiscard]] LogicalTick current_tick() const noexcept;
  [[nodiscard]] AuthorityEpoch current_epoch() const noexcept;

  // -- model registration ---------------------------------------------------

  Status register_device(const RegisterDeviceRequest& request);
  Status set_device_lifecycle(const SetLifecycleRequest& request);
  Status set_fan_policy(const SetFanPolicyRequest& request);
  Status define_pressure_relationship(const DefineRelationshipRequest& request);
  Status define_containment_element(const DefineContainmentRequest& request);
  Status report_containment(const ReportContainmentRequest& request);
  Status declare_obligation(const DeclareObligationRequest& request);
  Status declare_interlock(const DeclareInterlockRequest& request);
  Status report_interlock(const ReportInterlockRequest& request);
  Status add_grant(const PermissionGrant& grant, const ActorId& actor, LogicalTick at);
  Status revoke_grant(const GrantId& grant, AuthorityEpoch epoch, const ActorId& actor, LogicalTick at);
  Status add_maintenance_override(const MaintenanceOverride& override_entry, const ActorId& actor,
                                  LogicalTick at);
  Status revoke_maintenance_override(const OverrideId& override_id, const ActorId& actor, LogicalTick at);
  Status add_safety_permit(const SafetyPermit& permit, const ActorId& actor, LogicalTick at);

  // -- evidence -------------------------------------------------------------

  /// Accepts a reading into the evidence set.
  ///
  /// The subject must be known to the model: a reading for an undeclared
  /// device, relationship, or metered point is refused with not_found rather
  /// than retained, which is what bounds the evidence set.
  Result<Observation> observe(const ObservationDraft& draft, LogicalTick accepted_at);

  // -- control --------------------------------------------------------------

  Result<Decision> evaluate(const ControlRequest& request);
  Result<AttemptRecord> issue(const ControlRequest& request, AirflowAdapter& adapter);
  Result<VerifiedEffect> verify(const VerificationRequest& request);
  Status resolve_attempt(const ResolveAttemptRequest& request);

  // -- inspection -----------------------------------------------------------

  [[nodiscard]] std::vector<DeviceView> devices() const;
  [[nodiscard]] Result<DeviceView> device(const AirflowDeviceId& id) const;
  [[nodiscard]] std::vector<RelationshipView> relationships() const;
  [[nodiscard]] Result<RelationshipView> relationship(const PressureRelationshipId& id) const;
  [[nodiscard]] std::vector<ContainmentView> containment() const;
  [[nodiscard]] std::vector<ObligationView> obligations() const;
  [[nodiscard]] std::vector<InterlockView> interlocks() const;
  [[nodiscard]] std::vector<PermissionGrant> grants() const;
  [[nodiscard]] std::vector<MaintenanceOverride> overrides() const;
  [[nodiscard]] std::vector<SafetyPermit> safety_permits() const;
  [[nodiscard]] std::vector<AttemptView> attempts() const;
  [[nodiscard]] Result<AttemptRecord> attempt(AttemptId id) const;
  [[nodiscard]] std::vector<Observation> observations(const AirflowDeviceId& device) const;
  [[nodiscard]] HistoryView history(std::size_t limit) const;
  [[nodiscard]] std::uint64_t dropped_audit_entries() const noexcept;
  [[nodiscard]] StoreAudit store_audit() const;

  /// The authoritative model rendered as deterministic text.
  ///
  /// Two engines driven through the same logical event sequence produce
  /// byte-identical text regardless of when they ran, which process ran them,
  /// or how many times they published.
  [[nodiscard]] std::string canonical_state() const;

  /// A digest of canonical_state(). Deterministic for the same reason.
  [[nodiscard]] std::string state_digest() const;

 private:
  static ActuationAuthorization make_authorization(CommandId command, AttemptId attempt,
                                                   AirflowDeviceId device,
                                                   DeviceGeneration device_generation,
                                                   AuthorityEpoch epoch, StateRevision planned_revision,
                                                   PolicyGeneration policy_generation,
                                                   EvidenceGeneration evidence_generation,
                                                   LogicalTick issued_at);

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace airflow_control
