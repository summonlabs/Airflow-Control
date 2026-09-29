#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "airflow_control/adapter.hpp"
#include "airflow_control/ids.hpp"
#include "airflow_control/model.hpp"
#include "airflow_control/status.hpp"

namespace airflow_control {

/// The durable state of one control attempt.
///
/// The states below are the complete set. In particular there is no state that
/// means "probably applied": an attempt either has an established effect, a
/// definite refusal, or it remains unresolved and blocks further control.
enum class AttemptState : std::uint32_t {
  /// The durable record exists and the command has not left for the adapter.
  accepted = 0,
  /// The command was handed to the adapter; no outcome has been recorded.
  dispatched = 1,
  /// The adapter accepted responsibility. Not an effect.
  acknowledged = 2,
  /// The adapter stated it did not act.
  refused = 3,
  /// The adapter could not be reached.
  unavailable = 4,
  /// The adapter reported a fault.
  faulted = 5,
  /// The adapter answered without stating whether it acted.
  indeterminate = 6,
  /// Fresh evidence established the requested condition.
  effect_established = 7,
  /// Fresh evidence established the opposite condition.
  effect_contradicted = 8,
  /// Verification ran but the evidence could not decide.
  effect_indeterminate = 9,
  /// The process died at or after the durable command boundary. A command may
  /// have reached the device. Recovery adopts the attempt in this state and
  /// never re-sends it.
  recovery_required = 10,
  /// An operator or the owning authority resolved the attempt without an
  /// established effect, with a recorded reason.
  resolved_without_effect = 11,
  /// The attempt was explicitly superseded under the documented supersession
  /// rule. It is never overwritten silently.
  superseded = 12,
};

[[nodiscard]] const char* to_string(AttemptState state) noexcept;
[[nodiscard]] std::optional<AttemptState> parse_attempt_state(std::string_view token) noexcept;

/// True when the attempt blocks new control on the same device.
///
/// accepted, dispatched, acknowledged, indeterminate, effect_indeterminate, and
/// recovery_required are unresolved: the physical result of a command that may
/// have reached the device is not known.
[[nodiscard]] bool is_unresolved(AttemptState state) noexcept;

/// True when the adapter stated something definite about the physical outcome.
[[nodiscard]] bool is_definite(AttemptState state) noexcept;

/// The durable record of one control attempt.
///
/// The record is written and durably published before the command leaves for
/// the adapter. That publication is the command-attempt boundary: a process
/// that dies after it leaves behind a record that says a command may have
/// reached the device.
struct AttemptRecord {
  AttemptId id;
  AttemptOrdinal ordinal;
  IdempotencyKey key;
  AirflowDeviceId device;
  DeviceGeneration device_generation;
  AuthorityEpoch epoch;
  StateRevision planned_revision;
  PolicyGeneration policy_generation;
  EvidenceGeneration evidence_generation;
  ControlIntent intent = ControlIntent::hold_setpoint;
  RequestClass request_class = RequestClass::protective;
  SetpointRequest setpoint;
  ActorId actor;
  LogicalTick accepted_at;
  std::optional<LogicalTick> dispatched_at;
  AttemptState state = AttemptState::accepted;
  CommandId command;
  AdapterSequence adapter_sequence;
  AdapterDisposition disposition = AdapterDisposition::indeterminate;
  std::string detail;
  std::optional<SafetyPermitId> safety_permit;
  std::optional<AttemptId> supersedes;
  std::optional<AttemptId> superseded_by;
  std::optional<ObservationId> fan_observation;
  std::optional<ObservationId> pressure_observation;
  std::optional<EffectSequence> effect_sequence;
  std::optional<LogicalTick> resolved_at;
  std::string resolution_reason;
};

/// What evidence established about a commanded effect.
enum class EffectState : std::uint32_t {
  unverified = 0,
  effective = 1,
  contradicted = 2,
  indeterminate = 3,
};

[[nodiscard]] const char* to_string(EffectState state) noexcept;

/// The result of attempting to establish the physical effect of one attempt.
struct VerifiedEffect {
  AttemptId attempt = AttemptId::from(0);
  EffectState state = EffectState::unverified;
  EffectSequence sequence = EffectSequence::from(0);
  LogicalTick verified_at = LogicalTick::from(0);
  StatusCode code = StatusCode::ok;
  std::string message;
  std::optional<ObservationId> fan_observation;
  std::optional<ObservationId> pressure_observation;
  std::optional<SetpointBasisPoints> observed_fan_percent;
  std::optional<Airflow> observed_airflow;
  std::optional<Pressure> observed_differential;
  std::vector<CheckTrace> trace;
};

/// The outcome of comparing two setpoint requests for supersession safety.
enum class DeliveryComparison : std::uint32_t {
  /// Both requests name the same delivery. Supersession changes nothing.
  equal = 0,
  /// The candidate delivers strictly more airflow.
  increases = 1,
  /// The candidate delivers strictly less airflow.
  decreases = 2,
  /// The two requests are of different kinds and cannot be ordered.
  incomparable = 3,
};

/// Orders two setpoint requests by the airflow they deliver.
///
/// A percent request and an airflow request are not comparable here, because
/// this runtime does not own the fan curve that would relate them. That is a
/// refusal, never an assumption.
[[nodiscard]] DeliveryComparison compare_delivery(const SetpointRequest& candidate,
                                                  const SetpointRequest& incumbent) noexcept;

/// The document supersession rule, as a predicate.
///
/// A newer command may supersede an unresolved attempt only when the caller
/// names that exact attempt, the device generation matches, and the new command
/// either moves strictly more airflow or is a safety-directed request carrying
/// a valid permit. Anything else is refused with supersession_unsupported.
[[nodiscard]] bool supersession_permitted(RequestClass candidate_class, bool has_safety_permit,
                                          DeliveryComparison comparison) noexcept;

}  // namespace airflow_control
