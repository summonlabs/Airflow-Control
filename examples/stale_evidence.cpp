// Example: evidence that is fresh but is not evidence about the command.
//
// A reading that was already in hand when a command was issued is fresh, but it
// is not evidence about that command: verification binds evidence to the
// attempt, so the pre-command pressure observation cannot verify the restore.
// The same attempt becomes effective once a reading taken after the command
// exists. Finally, evidence that has aged past the configured maximum age is not
// silently treated as satisfied: a transition that requires pressure proof is
// refused with pressure_unknown.
//
// Everything here is SYNTHETIC: the observations are synthetic inputs and no
// hardware is read.

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>

#include "airflow_control/engine.hpp"
#include "airflow_control/synthetic_adapter.hpp"

using namespace airflow_control;

namespace {

int failures = 0;

void expect(bool condition, const std::string& description) {
  if (!condition) {
    std::cout << "EXPECTATION FAILED: " << description << "\n";
    failures += 1;
  } else {
    std::cout << "ok: " << description << "\n";
  }
}

const AirflowDeviceId kDevice = AirflowDeviceId::parse("room-d-fan-1").value();
const RoomId kRoom = RoomId::parse("room-d").value();
const PressureRelationshipId kRelationship =
    PressureRelationshipId::parse("room-d-pressure").value();
const ActorId kActor = ActorId::parse("example-operator").value();
const SourceId kField = SourceId::parse("field-instrumentation").value();
const SourceId kPressureField = SourceId::parse("pressure-instrumentation").value();

/// Deliberately short, so that "past the maximum age" is a few ticks away
/// rather than a large number that hides the boundary.
constexpr std::uint64_t kMaxAgeTicks = 50;

EngineOptions example_options() {
  EngineOptions options;
  options.evidence.max_age_ticks = LogicalTick::from(kMaxAgeTicks);
  options.audit_capacity = 128;
  options.idempotency_window = 32;
  options.attempt_journal_capacity = 64;
  return options;
}

std::string store_path(const std::string& directory) {
  std::error_code error;
  std::filesystem::create_directories(directory, error);
  return directory + "/airflow-stale-evidence.airflowstore";
}

Status build_model(AirflowControlEngine& engine) {
  Status status = engine.adopt_epoch(AuthorityEpoch::from(1), kActor, LogicalTick::from(0));
  if (!status.ok()) {
    return status;
  }
  status = engine.register_device(RegisterDeviceRequest{
      .device = kDevice,
      .generation = DeviceGeneration::from(1),
      .room = kRoom,
      .row = std::nullopt,
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
  if (!status.ok()) {
    return status;
  }
  status = engine.add_grant(
      PermissionGrant{
          .id = GrantId::parse("room-d-grant").value(),
          .issuer = SourceId::parse("authority-airflow").value(),
          .epoch = AuthorityEpoch::from(1),
          .device = kDevice,
          .device_generation = DeviceGeneration::from(1),
          .room = kRoom,
          .actions = ActionSet::all(),
          .issued_at = LogicalTick::from(0),
          .expires_at = std::nullopt,
          .revoked = false,
      },
      kActor, LogicalTick::from(0));
  if (!status.ok()) {
    return status;
  }
  status = engine.set_device_lifecycle(SetLifecycleRequest{
      .device = kDevice,
      .generation = DeviceGeneration::from(1),
      .expected_revision = StateRevision::from(1),
      .target = DeviceLifecycle::active,
      .epoch = AuthorityEpoch::from(1),
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
  if (!status.ok()) {
    return status;
  }
  status = engine.set_fan_policy(SetFanPolicyRequest{
      .device = kDevice,
      .generation = DeviceGeneration::from(1),
      .expected_revision = StateRevision::from(2),
      .policy =
          FanPolicy{
              .id = PolicyId::parse("room-d-policy").value(),
              .generation = PolicyGeneration::from(1),
              .envelope =
                  OperatingEnvelope{
                      .min_fan_percent = SetpointBasisPoints::create(0).value(),
                      .max_fan_percent = SetpointBasisPoints::create(10000).value(),
                      .default_fan_percent = SetpointBasisPoints::create(6000).value(),
                      .min_airflow = Airflow::from_cubic_metres_per_hour(0),
                      .max_airflow = Airflow::from_cubic_metres_per_hour(100000),
                      .max_step = SlewBasisPoints::create(10000).value(),
                      .source = SourceId::parse("authority-airflow").value(),
                      .evidence_generation = EvidenceGeneration::from(1),
                  },
          },
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
  if (!status.ok()) {
    return status;
  }
  return engine.define_pressure_relationship(DefineRelationshipRequest{
      .id = kRelationship,
      .room = kRoom,
      .controlled_space = SpaceRefId::parse("space-d-controlled").value(),
      .reference_space = SpaceRefId::parse("space-d-reference").value(),
      .polarity = PressurePolarity::negative,
      .lower = Pressure::from_millipascals(-8000),
      .upper = Pressure::from_millipascals(-4000),
      .tolerance = Pressure::from_millipascals(500),
      .evidence_generation = EvidenceGeneration::from(1),
      .expected_revision = std::nullopt,
      .actor = kActor,
      .requested_at = LogicalTick::from(0),
  });
}

ControlRequest restore_request(const char* key, LogicalTick at) {
  return ControlRequest{
      .key = IdempotencyKey::parse(key).value(),
      .device = kDevice,
      .device_generation = DeviceGeneration::from(1),
      .epoch = AuthorityEpoch::from(1),
      .expected_revision = std::nullopt,
      .intent = ControlIntent::restore_pressure_relationship,
      .setpoint = SetpointPercent{SetpointBasisPoints::create(6000).value()},
      .actor = kActor,
      .requested_at = at,
      .safety_permit = std::nullopt,
      .supersede = std::nullopt,
      .relationship = kRelationship,
  };
}

VerificationRequest verification(AttemptId attempt, LogicalTick at) {
  return VerificationRequest{
      .attempt = attempt,
      .adapter = nullptr,
      .source = kField,
      .fan_point = SpaceRefId::parse("space-d-fan-point").value(),
      .pressure_point = SpaceRefId::parse("space-d-pressure-point").value(),
      .fan_sequence = EvidenceSequence::from(1),
      .pressure_sequence = EvidenceSequence::from(1),
      .at = at,
      .relationship = kRelationship,
  };
}

Result<Observation> record_pressure(AirflowControlEngine& engine, Pressure differential,
                                    EvidenceSequence sequence, LogicalTick measured_at) {
  return engine.observe(
      ObservationDraft{
          .payload = PressureReading{differential},
          .device = kDevice,
          .relationship = kRelationship,
          .point = SpaceRefId::parse("space-d-pressure-point").value(),
          .source = kPressureField,
          .sequence = sequence,
          .measured_at = measured_at,
          .device_generation = DeviceGeneration::from(1),
          .evidence_generation = EvidenceGeneration::from(1),
          .quality = Quality::good,
      },
      measured_at);
}

Result<Observation> record_fan(AirflowControlEngine& engine, SetpointBasisPoints percent,
                               EvidenceSequence sequence, LogicalTick measured_at) {
  return engine.observe(
      ObservationDraft{
          .payload = FanReading{percent},
          .device = kDevice,
          .relationship = std::nullopt,
          .point = SpaceRefId::parse("space-d-fan-point").value(),
          .source = kField,
          .sequence = sequence,
          .measured_at = measured_at,
          .device_generation = DeviceGeneration::from(1),
          .evidence_generation = EvidenceGeneration::from(1),
          .quality = Quality::good,
      },
      measured_at);
}

/// The first trace entry with the given outcome, or nothing.
std::optional<CheckTrace> trace_entry(const VerifiedEffect& effect, StatusCode code) {
  for (const CheckTrace& entry : effect.trace) {
    if (entry.outcome == code) {
      return entry;
    }
  }
  return std::nullopt;
}

AdapterDescriptor synthetic_descriptor() {
  AdapterDescriptor descriptor;
  descriptor.vendor = "Airflow Control";
  descriptor.model = "synthetic-stale-evidence-plant";
  descriptor.protocol = "in-process";
  descriptor.capabilities = static_cast<std::uint32_t>(AdapterCapability::set_fan_percent);
  descriptor.synthetic = true;
  return descriptor;
}

}  // namespace

int main(int argc, char** argv) {
  const std::string directory = argc > 1 ? argv[1] : "example-stale-evidence";
  const std::string store = store_path(directory);
  std::remove(store.c_str());
  std::remove((store + ".lock").c_str());
  std::cout << "store: " << store << "\n";
  std::cout << "adapter=SYNTHETIC (no hardware is driven)\n";

  auto opened = AirflowControlEngine::open(store, OpenMode::create_new, example_options());
  if (!opened.ok()) {
    std::cout << "open failed: " << opened.status().to_string() << "\n";
    return 2;
  }
  AirflowControlEngine& engine = opened.value();
  const Status built = build_model(engine);
  if (!built.ok()) {
    std::cout << "setup failed: " << built.to_string() << "\n";
    return 2;
  }

  // Step 1: evidence that is already in hand before the command. It is fresh at
  // the verification instant, and it still is not evidence about the command.
  const LogicalTick measured = LogicalTick::from(1);
  if (!engine.advance_tick(measured).ok()) {
    return 2;
  }
  const auto early_fan = record_fan(engine, SetpointBasisPoints::create(6000).value(),
                                    EvidenceSequence::from(1), measured);
  const auto early_pressure =
      record_pressure(engine, Pressure::from_millipascals(-6000), EvidenceSequence::from(1),
                      measured);
  if (!early_fan.ok() || !early_pressure.ok()) {
    std::cout << "initial evidence refused\n";
    return 2;
  }
  std::cout << "step 1 accepted pre-command evidence: fan and pressure measured at tick "
            << measured.to_string() << "\n";

  AdapterDescriptor descriptor = synthetic_descriptor();
  SyntheticAirflowAdapter adapter(descriptor);
  adapter.seed_device(kDevice, DeviceGeneration::from(1),
                      SyntheticPlantState{
                          .fan_percent = SetpointBasisPoints::create(6000).value(),
                          .airflow = Airflow::from_cubic_metres_per_hour(6000),
                          .differential = Pressure::from_millipascals(-6000),
                      });
  adapter.set_apply_on_execute(false);

  const LogicalTick command_at = LogicalTick::from(3);
  if (!engine.advance_tick(command_at).ok()) {
    return 2;
  }
  const auto issued = engine.issue(restore_request("restore-stale-1", command_at), adapter);
  if (!issued.ok()) {
    std::cout << "issue failed: " << issued.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 2 attempt=" << issued.value().id.to_string()
            << " dispatched-at=" << issued.value().dispatched_at->to_string()
            << " attempt-state=" << to_string(issued.value().state) << "\n";

  const LogicalTick verify_at = LogicalTick::from(5);
  if (!engine.advance_tick(verify_at).ok()) {
    return 2;
  }
  const auto stale = engine.verify(verification(issued.value().id, verify_at));
  if (!stale.ok()) {
    std::cout << "verify failed: " << stale.status().to_string() << "\n";
    return 2;
  }
  const std::optional<CheckTrace> stale_entry =
      trace_entry(stale.value(), StatusCode::evidence_stale);
  std::cout << "step 3 effect=" << to_string(stale.value().state)
            << " not-effective=" << (stale.value().state != EffectState::effective ? "true" : "false")
            << "\n";
  if (stale_entry.has_value()) {
    std::cout << "step 3 trace: " << stale_entry->check << " "
              << to_string(stale_entry->outcome) << " " << stale_entry->detail << "\n";
  }
  std::cout << "step 3 detail: " << stale.value().message << "\n";
  expect(stale.value().state != EffectState::effective,
         "a pre-command pressure observation does not verify the command: the effect is not "
         "effective");
  expect(stale.value().state == EffectState::indeterminate,
         "verification reports not-effective: the available evidence does not decide the effect");
  expect(stale_entry.has_value(),
         "the trace names the reason as evidence_stale: the reading predates the command");
  expect(stale_entry.has_value() && std::string(to_string(stale_entry->outcome)) == "evidence_stale",
         "the trace outcome token is byte-for-byte evidence_stale");

  // Step 4: the same attempt becomes effective once a reading taken after the
  // command exists.
  if (!engine.advance_tick(verify_at).ok()) {
    return 2;
  }
  const auto fresh_fan = record_fan(engine, SetpointBasisPoints::create(6000).value(),
                                    EvidenceSequence::from(2), verify_at);
  const auto fresh_pressure = record_pressure(engine, Pressure::from_millipascals(-6000),
                                              EvidenceSequence::from(2), verify_at);
  if (!fresh_fan.ok() || !fresh_pressure.ok()) {
    std::cout << "post-command evidence refused\n";
    return 2;
  }
  const auto effective = engine.verify(verification(issued.value().id, verify_at));
  if (!effective.ok()) {
    std::cout << "verify failed: " << effective.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 4 effect=" << to_string(effective.value().state)
            << " observed-fan-percent="
            << (effective.value().observed_fan_percent.has_value()
                    ? effective.value().observed_fan_percent->to_string()
                    : std::string("-"))
            << " observed-differential="
            << (effective.value().observed_differential.has_value()
                    ? effective.value().observed_differential->to_string()
                    : std::string("-"))
            << "\n";
  expect(effective.value().state == EffectState::effective,
         "a fresh post-command observation of the same attempt makes it effective");
  const auto record = engine.attempt(issued.value().id);
  expect(record.ok() && record.value().state == AttemptState::effect_established,
         "the attempt is closed as effect_established once fresh evidence exists");

  // Step 5: evidence that ages past the configured maximum age is not silently
  // treated as satisfied.
  const LogicalTick aged = LogicalTick::from(verify_at.value() + kMaxAgeTicks + 1);
  if (!engine.advance_tick(aged).ok()) {
    return 2;
  }
  const auto aged_request = restore_request("restore-stale-2", aged);
  const auto refused = engine.evaluate(aged_request);
  if (!refused.ok()) {
    std::cout << "evaluate failed: " << refused.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 5 age-ticks=" << (aged.value() - verify_at.value())
            << " max-age-ticks=" << kMaxAgeTicks
            << " eligible=" << (refused.value().eligible ? "true" : "false")
            << " refusal-token=" << to_string(refused.value().code) << "\n";
  std::cout << "step 5 detail: " << refused.value().message << "\n";
  expect(!refused.value().eligible && refused.value().code == StatusCode::pressure_unknown,
         "a transition requiring pressure proof is refused with pressure_unknown once the "
         "evidence is older than the configured maximum age");
  expect(std::string(to_string(refused.value().code)) == "pressure_unknown",
         "the refusal token is byte-for-byte pressure_unknown");

  const Status closed = engine.close();
  expect(closed.ok(), "the durable store closed cleanly");
  if (failures != 0) {
    std::cout << failures << " expectation(s) failed\n";
    return 1;
  }
  std::cout << "stale evidence was never promoted to proof\n";
  return 0;
}
