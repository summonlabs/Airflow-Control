// Example: recovering a violated pressure relationship, and what the device
// latch looks like when a second command is acknowledged but the plant does not
// move.
//
// Everything here is SYNTHETIC: the adapter is a deterministic simulator that
// drives no hardware, and no value below is a measurement of a real room.

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

const AirflowDeviceId kDevice = AirflowDeviceId::parse("room-a-fan-1").value();
const RoomId kRoom = RoomId::parse("room-a").value();
const PressureRelationshipId kRelationship =
    PressureRelationshipId::parse("room-a-pressure").value();
const ActorId kActor = ActorId::parse("example-operator").value();
const SourceId kField = SourceId::parse("field-instrumentation").value();

EngineOptions example_options() {
  EngineOptions options;
  options.evidence.max_age_ticks = LogicalTick::from(1000);
  options.audit_capacity = 128;
  options.idempotency_window = 32;
  options.attempt_journal_capacity = 64;
  return options;
}

/// Creates the store directory when it does not exist and returns the store
/// path inside it.
std::string store_path(const std::string& directory) {
  std::error_code error;
  std::filesystem::create_directories(directory, error);
  return directory + "/airflow-pressure-recovery.airflowstore";
}

/// The bootstrap order the runtime requires: authority, then the device, then
/// the grant, because commissioning a device is itself an authority-bearing
/// act, then the lifecycle, the fan policy, and the pressure relationship.
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
          .id = GrantId::parse("room-a-grant").value(),
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
              .id = PolicyId::parse("room-a-policy").value(),
              .generation = PolicyGeneration::from(1),
              .envelope =
                  OperatingEnvelope{
                      .min_fan_percent = SetpointBasisPoints::create(0).value(),
                      .max_fan_percent = SetpointBasisPoints::create(10000).value(),
                      .default_fan_percent = SetpointBasisPoints::create(5000).value(),
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
      .controlled_space = SpaceRefId::parse("space-a-controlled").value(),
      .reference_space = SpaceRefId::parse("space-a-reference").value(),
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

/// A restore request for the negative-pressure relationship. The same shape is
/// used for every attempt, so the only difference between them is their key.
ControlRequest restore_request(const char* key, SetpointBasisPoints setpoint, LogicalTick at) {
  return ControlRequest{
      .key = IdempotencyKey::parse(key).value(),
      .device = kDevice,
      .device_generation = DeviceGeneration::from(1),
      .epoch = AuthorityEpoch::from(1),
      .expected_revision = std::nullopt,
      .intent = ControlIntent::restore_pressure_relationship,
      .setpoint = SetpointPercent{setpoint},
      .actor = kActor,
      .requested_at = at,
      .safety_permit = std::nullopt,
      .supersede = std::nullopt,
      .relationship = kRelationship,
  };
}

VerificationRequest verification(AttemptId attempt, AirflowAdapter& adapter, LogicalTick at,
                                 EvidenceSequence fan_sequence, EvidenceSequence pressure_sequence) {
  return VerificationRequest{
      .attempt = attempt,
      .adapter = &adapter,
      .source = kField,
      .fan_point = SpaceRefId::parse("space-a-fan-point").value(),
      .pressure_point = SpaceRefId::parse("space-a-pressure-point").value(),
      .fan_sequence = fan_sequence,
      .pressure_sequence = pressure_sequence,
      .at = at,
      .relationship = kRelationship,
  };
}

AdapterDescriptor synthetic_descriptor() {
  AdapterDescriptor descriptor;
  descriptor.vendor = "Airflow Control";
  descriptor.model = "synthetic-room-plant";
  descriptor.protocol = "in-process";
  descriptor.capabilities = static_cast<std::uint32_t>(AdapterCapability::set_fan_percent) |
                            static_cast<std::uint32_t>(AdapterCapability::read_pressure) |
                            static_cast<std::uint32_t>(AdapterCapability::read_fan_percent);
  descriptor.synthetic = true;
  return descriptor;
}

}  // namespace

int main(int argc, char** argv) {
  const std::string directory = argc > 1 ? argv[1] : "example-pressure-recovery";
  const std::string store = store_path(directory);
  // The example starts from nothing so that it can be run repeatedly.
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

  // Step 1: the relationship starts violated.
  if (!engine.advance_tick(LogicalTick::from(1)).ok()) {
    return 2;
  }
  const auto initial = engine.observe(
      ObservationDraft{
          .payload = PressureReading{Pressure::from_millipascals(1500)},
          .device = kDevice,
          .relationship = kRelationship,
          .point = SpaceRefId::parse("space-a-pressure-point").value(),
          .source = kField,
          .sequence = EvidenceSequence::from(1),
          .measured_at = LogicalTick::from(1),
          .device_generation = DeviceGeneration::from(1),
          .evidence_generation = EvidenceGeneration::from(1),
          .quality = Quality::good,
      },
      LogicalTick::from(1));
  if (!initial.ok()) {
    std::cout << "initial observation refused: " << initial.status().to_string() << "\n";
    return 2;
  }
  const auto before = engine.relationship(kRelationship);
  if (!before.ok()) {
    std::cout << "relationship lookup failed: " << before.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 1 pressure-relationship-state=" << to_string(before.value().state)
            << " adjudicated=" << (before.value().adjudicated.has_value()
                                       ? before.value().adjudicated->to_string()
                                       : std::string("-"))
            << "\n";
  expect(before.value().state == PressureState::violated,
         "the negative-pressure relationship starts violated: a positive differential cannot "
         "satisfy a negative band");

  // Step 2: a protective restore command is evaluated and issued through the
  // synthetic plant, which is seeded so the plant is coherent and scripted so
  // the post-command differential lands inside the band.
  const OperatingEnvelope& envelope = engine.device(kDevice).value().envelope.value();
  expect(envelope.max_step.basis_points() == 10000,
         "the example's envelope was installed with the slew bound it declares");

  AdapterDescriptor descriptor = synthetic_descriptor();
  SyntheticAirflowAdapter adapter(descriptor);
  adapter.seed_device(kDevice, DeviceGeneration::from(1),
                      SyntheticPlantState{
                          .fan_percent = SetpointBasisPoints::create(5000).value(),
                          .airflow = Airflow::from_cubic_metres_per_hour(5000),
                          .differential = Pressure::from_millipascals(1500),
                      });
  adapter.set_pending_pressure_response(kRelationship, Pressure::from_millipascals(-6000));

  if (!engine.advance_tick(LogicalTick::from(2)).ok()) {
    return 2;
  }
  const ControlRequest first = restore_request(
      "restore-pressure-1", SetpointBasisPoints::create(6000).value(), LogicalTick::from(2));
  const auto evaluated = engine.evaluate(first);
  if (!evaluated.ok()) {
    std::cout << "evaluate failed: " << evaluated.status().to_string() << "\n";
    return 2;
  }
  expect(evaluated.value().eligible,
         "the protective restore request is eligible while the relationship is violated, "
         "because a protective request repairs rather than depends on the violation");
  const auto issued = engine.issue(first, adapter);
  if (!issued.ok()) {
    std::cout << "issue failed: " << issued.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 2 attempt=" << issued.value().id.to_string()
            << " disposition=" << to_string(issued.value().disposition)
            << " attempt-state=" << to_string(issued.value().state) << "\n";
  expect(issued.value().state == AttemptState::acknowledged,
         "the adapter's acknowledgement is recorded as an acknowledgement, not as an effect");

  // Step 3: a fresh observation taken after the command establishes the effect.
  if (!engine.advance_tick(LogicalTick::from(3)).ok()) {
    return 2;
  }
  const auto verified =
      engine.verify(verification(issued.value().id, adapter, LogicalTick::from(3),
                                EvidenceSequence::from(1), EvidenceSequence::from(2)));
  if (!verified.ok()) {
    std::cout << "verify failed: " << verified.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 3 outcome=" << to_string(verified.value().state)
            << " effect-token=" << to_string(verified.value().state)
            << " observed-fan-percent="
            << (verified.value().observed_fan_percent.has_value()
                    ? verified.value().observed_fan_percent->to_string()
                    : std::string("-"))
            << " observed-differential="
            << (verified.value().observed_differential.has_value()
                    ? verified.value().observed_differential->to_string()
                    : std::string("-"))
            << "\n";
  expect(verified.value().state == EffectState::effective,
         "fresh post-command pressure evidence establishes the restore as effective");
  const auto first_record = engine.attempt(issued.value().id);
  expect(first_record.ok() && first_record.value().state == AttemptState::effect_established,
         "the first attempt is closed as effect_established");

  // Step 4: a second attempt whose adapter acknowledges but never moves the
  // plant. Nothing about the acknowledgement is an effect.
  adapter.set_apply_on_execute(false);
  if (!engine.advance_tick(LogicalTick::from(4)).ok()) {
    return 2;
  }
  const ControlRequest second = restore_request(
      "restore-pressure-2", SetpointBasisPoints::create(6500).value(), LogicalTick::from(4));
  const auto second_issued = engine.issue(second, adapter);
  if (!second_issued.ok()) {
    std::cout << "second issue failed: " << second_issued.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 4 attempt=" << second_issued.value().id.to_string()
            << " disposition=" << to_string(second_issued.value().disposition)
            << " attempt-state=" << to_string(second_issued.value().state)
            << " applied-commands=" << adapter.applied_commands() << "\n";
  expect(second_issued.value().state == AttemptState::acknowledged,
         "the second command is acknowledged while the plant does not move");

  // Step 5: the next command for the same device is refused while that attempt
  // has no established effect.
  const std::uint64_t execute_calls_before = adapter.execute_calls();
  const ControlRequest third = restore_request(
      "restore-pressure-3", SetpointBasisPoints::create(6500).value(), LogicalTick::from(4));
  const auto refused = engine.issue(third, adapter);
  const StatusCode refusal_code =
      refused.ok() ? StatusCode::ok : refused.status().code();
  std::cout << "step 5 next-command=" << (refused.ok() ? "accepted" : "refused")
            << " status-token=" << to_string(refusal_code) << "\n";
  if (!refused.ok()) {
    std::cout << "step 5 detail: " << refused.status().message() << "\n";
  }
  expect(!refused.ok() && refusal_code == StatusCode::attempt_unresolved,
         "the next command for the device is refused with attempt_unresolved");
  expect(adapter.execute_calls() == execute_calls_before,
         "the refused command never reached the adapter");

  // Step 6: fresh evidence about the unmoved plant contradicts the second
  // attempt.
  if (!engine.advance_tick(LogicalTick::from(5)).ok()) {
    return 2;
  }
  const auto contradicted =
      engine.verify(verification(second_issued.value().id, adapter, LogicalTick::from(5),
                                EvidenceSequence::from(2), EvidenceSequence::from(3)));
  if (!contradicted.ok()) {
    std::cout << "second verify failed: " << contradicted.status().to_string() << "\n";
    return 2;
  }
  std::cout << "step 6 outcome=" << to_string(contradicted.value().state)
            << " effect-token=" << to_string(contradicted.value().state)
            << " observed-fan-percent="
            << (contradicted.value().observed_fan_percent.has_value()
                    ? contradicted.value().observed_fan_percent->to_string()
                    : std::string("-"))
            << "\n";
  std::cout << "step 6 detail: " << contradicted.value().message << "\n";
  expect(contradicted.value().state == EffectState::contradicted,
         "fresh evidence establishes that the plant did not move, so the effect is contradicted");
  const auto second_record = engine.attempt(second_issued.value().id);
  expect(second_record.ok() && second_record.value().state == AttemptState::effect_contradicted,
         "the second attempt is closed as effect_contradicted");

  std::cout << "outcome-1 effect=effective status-token=" << to_string(StatusCode::ok) << "\n";
  std::cout << "outcome-2 effect=contradicted status-token=" << to_string(refusal_code) << "\n";

  const Status closed = engine.close();
  expect(closed.ok(), "the durable store closed cleanly");
  if (failures != 0) {
    std::cout << failures << " expectation(s) failed\n";
    return 1;
  }
  std::cout << "a restore was proven effective by fresh pressure evidence, and an "
               "acknowledgement without movement was not\n";
  return 0;
}
