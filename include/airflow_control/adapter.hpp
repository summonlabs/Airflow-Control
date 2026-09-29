#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "airflow_control/authority.hpp"
#include "airflow_control/evidence.hpp"
#include "airflow_control/ids.hpp"
#include "airflow_control/model.hpp"
#include "airflow_control/status.hpp"

namespace airflow_control {

/// What an adapter says happened to a command.
///
/// A disposition is not an effect. accepted means the adapter took
/// responsibility for the command; it does not mean the airflow changed. Only
/// fresh evidence establishes an effect.
enum class AdapterDisposition : std::uint32_t {
  accepted = 0,
  refused = 1,
  unavailable = 2,
  fault = 3,
  indeterminate = 4,
};

[[nodiscard]] const char* to_string(AdapterDisposition disposition) noexcept;
[[nodiscard]] std::optional<AdapterDisposition> parse_adapter_disposition(std::string_view token) noexcept;

/// True when the disposition states something definite about the outcome.
///
/// refused, unavailable, and fault are definite: the adapter has said it did
/// not, or could not, act. accepted and indeterminate are not definite about
/// the physical result.
[[nodiscard]] bool is_definite_refusal(AdapterDisposition disposition) noexcept;

/// Capability bits an adapter declares.
enum class AdapterCapability : std::uint32_t {
  set_fan_percent = 1u << 0,
  set_airflow = 1u << 1,
  read_pressure = 1u << 2,
  read_airflow = 1u << 3,
  read_fan_percent = 1u << 4,
  observe_containment = 1u << 5,
};

/// An adapter's self-description.
///
/// synthetic is an honesty flag, not a capability: a synthetic adapter drives
/// no hardware, and the tooling reports that fact wherever results are printed.
struct AdapterDescriptor {
  std::string vendor;
  std::string model;
  std::string protocol;
  std::uint32_t capabilities = 0;
  bool synthetic = true;
};

[[nodiscard]] bool declares(const AdapterDescriptor& descriptor, AdapterCapability capability) noexcept;
[[nodiscard]] std::string describe_capabilities(std::uint32_t capabilities);

/// An already-validated control command on its way to an adapter.
///
/// The only constructor takes an ActuationAuthorization, which only the engine
/// can produce. A caller therefore cannot synthesise a command that skipped
/// precondition validation, and cannot widen what the engine decided.
class AdapterCommand {
 public:
  AdapterCommand(const ActuationAuthorization& authorization, ControlIntent intent, RequestClass klass,
                 SetpointRequest setpoint)
      : id_(authorization.command()),
        attempt_(authorization.attempt()),
        device_(authorization.device()),
        device_generation_(authorization.device_generation()),
        epoch_(authorization.epoch()),
        planned_revision_(authorization.planned_revision()),
        policy_generation_(authorization.policy_generation()),
        evidence_generation_(authorization.evidence_generation()),
        issued_at_(authorization.issued_at()),
        intent_(intent),
        request_class_(klass),
        setpoint_(std::move(setpoint)) {}

  [[nodiscard]] CommandId id() const noexcept { return id_; }
  [[nodiscard]] AttemptId attempt() const noexcept { return attempt_; }
  [[nodiscard]] const AirflowDeviceId& device() const noexcept { return device_; }
  [[nodiscard]] DeviceGeneration device_generation() const noexcept { return device_generation_; }
  [[nodiscard]] AuthorityEpoch epoch() const noexcept { return epoch_; }
  [[nodiscard]] StateRevision planned_revision() const noexcept { return planned_revision_; }
  [[nodiscard]] PolicyGeneration policy_generation() const noexcept { return policy_generation_; }
  [[nodiscard]] EvidenceGeneration evidence_generation() const noexcept { return evidence_generation_; }
  [[nodiscard]] LogicalTick issued_at() const noexcept { return issued_at_; }
  [[nodiscard]] ControlIntent intent() const noexcept { return intent_; }
  [[nodiscard]] RequestClass request_class() const noexcept { return request_class_; }
  [[nodiscard]] const SetpointRequest& setpoint() const noexcept { return setpoint_; }

 private:
  CommandId id_;
  AttemptId attempt_;
  AirflowDeviceId device_;
  DeviceGeneration device_generation_;
  AuthorityEpoch epoch_;
  StateRevision planned_revision_;
  PolicyGeneration policy_generation_;
  EvidenceGeneration evidence_generation_;
  LogicalTick issued_at_;
  ControlIntent intent_;
  RequestClass request_class_;
  SetpointRequest setpoint_;
};

/// What an adapter answered.
struct AdapterOutcome {
  CommandId command;
  AttemptId attempt;
  AirflowDeviceId device;
  DeviceGeneration device_generation;
  AdapterDisposition disposition = AdapterDisposition::indeterminate;
  AdapterSequence sequence;
  LogicalTick acknowledged_at;
  std::string detail;
  std::optional<ObservationDraft> reading;
};

/// A request for one reading from one source.
struct AdapterReadRequest {
  AirflowDeviceId device;
  DeviceGeneration device_generation;
  ObservationKind kind = ObservationKind::airflow;
  std::optional<PressureRelationshipId> relationship;
  SpaceRefId point;
  SourceId source;
  EvidenceSequence sequence;
  LogicalTick at;
  EvidenceGeneration evidence_generation;
};

/// The whole vendor boundary.
///
/// An adapter receives an already-authorised command and answers with a
/// disposition and, optionally, a draft reading. It never receives a raw
/// request, never sees the engine's state, and its answer is never attributed
/// to a command it was not given.
class AirflowAdapter {
 public:
  AirflowAdapter() = default;
  AirflowAdapter(const AirflowAdapter&) = delete;
  AirflowAdapter& operator=(const AirflowAdapter&) = delete;
  AirflowAdapter(AirflowAdapter&&) = delete;
  AirflowAdapter& operator=(AirflowAdapter&&) = delete;
  virtual ~AirflowAdapter() = default;

  [[nodiscard]] virtual AdapterDescriptor describe() const = 0;
  virtual AdapterOutcome execute(const AdapterCommand& command) = 0;
  virtual Result<ObservationDraft> read(const AdapterReadRequest& request) = 0;
};

/// Checks that an outcome answers the command it was handed.
///
/// An outcome that names a different command, attempt, device, or device
/// generation is fenced and its answer is discarded.
[[nodiscard]] Status verify_outcome_echo(const AdapterCommand& command, const AdapterOutcome& outcome);

}  // namespace airflow_control
